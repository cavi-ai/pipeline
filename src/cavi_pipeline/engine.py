"""Workflow runs as LangGraph threads: run the ready steps, then wait for the next event."""

import logging
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Annotated, TypedDict

from cavi_mcp_core.files import inside
from cavi_mcp_core.lock import FileLock
from cavi_mcp_core.server import ANTICIPATED
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, Send, interrupt
from langsmith import tracing_context

from . import ledger
from .reservations import Registry
from .spec import MAX_NODES

logger = logging.getLogger(__name__)

# Two supersteps per step that finishes in one event, and the schedule and wait around them.
RECURSION_LIMIT = 4 * MAX_NODES + 8


def _merge(left: dict, right: dict) -> dict:
    return {**left, **right}


class RunState(TypedDict):
    run: str
    graph: str
    run_dir: str
    cancelled: bool
    nodes: Annotated[dict, _merge]


def _entry(**fields) -> dict:
    entry = {"status": "waiting", "attempt": 0, "evidence": None, "receipt": None,
             "reason": None, "note": None, "blocked": None}
    entry.update(fields)
    return entry


def _ready(graph, nodes) -> list[str]:
    done = {node_id for node_id, entry in nodes.items() if entry["status"] == "done"}
    return [node_id for node_id in graph.order()
            if nodes[node_id]["status"] == "waiting"
            and done.issuperset(graph.nodes[node_id].depends + graph.nodes[node_id].after)]


def _why_not_ready(graph, nodes, node_id) -> str:
    status = nodes[node_id]["status"]
    if status != "waiting":
        return f"{node_id} is {status}"
    node = graph.nodes[node_id]
    waiting = [name for name in node.depends + node.after if nodes[name]["status"] != "done"]
    return f"{node_id} waits on {', '.join(waiting)}"


class Engine:
    """Runs of the given graphs, checkpointed in ``store`` with run directories under ``runs``.

    ``hooks`` maps every check and run hook a graph names to a callable taking the run
    directory; a hook refuses by raising ValueError.
    """

    def __init__(self, graphs, hooks, *, store, runs):
        missing = sorted({name for graph in graphs.values() for name in graph.hooks()} - set(hooks))
        if missing:
            raise ValueError(f"The graphs name hooks that were not given: {', '.join(missing)}")
        self.graphs, self.hooks = dict(graphs), dict(hooks)
        self.runs_root = Path(runs).resolve(strict=True)
        store = Path(store)
        store.parent.mkdir(parents=True, exist_ok=True)
        self._lock_dir = store.parent / f"{store.name}.locks"
        self._lock_dir.mkdir(exist_ok=True)
        self._db = sqlite3.connect(store, check_same_thread=False, timeout=30)
        self._saver = SqliteSaver(self._db)
        self._saver.setup()
        self.registry = Registry(store)
        self._locks, self._guard = {}, threading.Lock()
        builder = StateGraph(RunState)
        builder.add_node("schedule", self._schedule)
        builder.add_node("step", self._step)
        builder.add_node("wait", self._wait)
        builder.add_edge(START, "schedule")
        builder.add_edge("step", "schedule")
        builder.add_edge("wait", "schedule")
        self._app = builder.compile(checkpointer=self._saver)

    def close(self):
        self._db.close()

    # Graph nodes. Everything an event needs is decided before it is resumed.

    def _schedule(self, state):
        if state["cancelled"]:
            return Command(goto=END)
        graph, nodes = self.graphs[state["graph"]], state["nodes"]
        sends = [
            Send("step", {"run": state["run"], "graph": state["graph"], "run_dir": state["run_dir"],
                          "node": node_id, "attempt": nodes[node_id]["attempt"],
                          "upstream": {name: nodes[name]["receipt"]
                                       for name in graph.nodes[node_id].depends}})
            for node_id in _ready(graph, nodes)
            if graph.nodes[node_id].kind == "step" and not nodes[node_id]["blocked"]
        ]
        return Command(goto=sends or "wait")

    def _step(self, task):
        node = self.graphs[task["graph"]].nodes[task["node"]]
        run, run_dir, attempt = task["run"], Path(task["run_dir"]), task["attempt"] + 1
        try:
            self.registry.reserve(run, node.id, node.resources)
        except ValueError as exc:
            return {"nodes": {node.id: _entry(attempt=task["attempt"], blocked=str(exc))}}
        try:
            self.hooks[node.run](run_dir)
            recorded = self._evidence(node, run_dir, task["upstream"])
        except ANTICIPATED as exc:
            self.registry.release(run, node.id)
            return {"nodes": {node.id: _entry(status="failed", attempt=attempt, reason=str(exc))}}
        except Exception as exc:
            logger.exception("Step %s of run %s raised", node.id, run)
            return {"nodes": {node.id: _entry(
                status="uncertain", attempt=attempt,
                reason=f"{type(exc).__name__}: the outcome is unknown; inspect, then retry")}}
        self.registry.release(run, node.id)
        return {"nodes": {node.id: _entry(status="done", attempt=attempt, evidence=recorded,
                                          receipt=ledger.receipt(recorded))}}

    def _wait(self, state):
        event = interrupt({"run": state["run"]})
        update = {node_id: {**entry, "blocked": None}
                  for node_id, entry in state["nodes"].items() if entry["blocked"]}
        update.update(event.get("nodes", {}))
        return {"nodes": update, "cancelled": event.get("cancelled", False)}

    # Plumbing.

    def _config(self, run):
        return {"configurable": {"thread_id": run}, "recursion_limit": RECURSION_LIMIT,
                "max_concurrency": 1}

    def _invoke(self, value, run):
        with tracing_context(enabled=False):
            self._app.invoke(value, self._config(run), durability="sync")

    def _lock(self, run):
        with self._guard:
            if run not in self._locks:
                self._locks[run] = FileLock(self._lock_dir / f"{run}.lock", follow=False)
            return self._locks[run]

    def _graph(self, values):
        graph = self.graphs.get(values["graph"])
        if graph is None or set(graph.nodes) != set(values["nodes"]):
            raise ValueError(f"Graph {values['graph']} changed since run {values['run']} started; "
                             "start a new run")
        return graph

    def _node(self, graph, node_id):
        if node_id not in graph.nodes:
            raise ValueError(f"Graph {graph.id} has no node {node_id}")
        return graph.nodes[node_id]

    def _evidence(self, node, run_dir, upstream):
        recorded = ledger.evidence(node, run_dir, upstream)
        if node.check:
            self.hooks[node.check](run_dir)
        return recorded

    def _snapshot(self, run):
        snapshot = self._app.get_state(self._config(run))
        if not snapshot.values:
            raise ValueError(f"Run {run} has no state in this store")
        return snapshot

    def _apply(self, run, revision, decide):
        self.registry.run(run)
        with self._lock(run):
            snapshot = self._snapshot(run)
            if snapshot.next and not snapshot.interrupts:
                self._invoke(None, run)
                snapshot = self._snapshot(run)
            values = snapshot.values
            current = snapshot.config["configurable"]["checkpoint_id"]
            if revision != current:
                raise ValueError(f"Revision {revision} is not the current revision {current}; "
                                 "read workflow_status and act on what it shows")
            if values["cancelled"]:
                raise ValueError(f"Run {run} is cancelled")
            graph, run_dir = self._graph(values), Path(values["run_dir"])
            resets = {node_id: _entry(attempt=values["nodes"][node_id]["attempt"],
                                      reason=f"stale: {why}")
                      for node_id, why in ledger.stale(graph, values["nodes"], run_dir).items()}
            event = decide(graph, run_dir, {**values["nodes"], **resets})
            event["nodes"] = {**resets, **event.get("nodes", {})}
            self._invoke(Command(resume=event), run)
        return self.status(run)

    # Actions.

    def catalog(self) -> dict:
        return {
            "graphs": [{"id": graph.id, "nodes": [node.describe() for node in graph.nodes.values()]}
                       for graph in self.graphs.values()],
            "runs": [{"run": row["run"], "graph": row["graph"], "run_dir": row["run_dir"],
                      "state": row["state"]} for row in self.registry.runs()],
        }

    def start(self, graph_id, run_dir) -> dict:
        graph = self.graphs.get(graph_id)
        if graph is None:
            raise ValueError(f"Unknown graph {graph_id}; workflow_list names the graphs")
        directory = inside(self.runs_root, run_dir, strict=True)
        if not directory.is_dir():
            raise ValueError(f"{run_dir} is not a directory in the runs workspace")
        run = f"{graph_id}-{uuid.uuid4().hex[:8]}"
        self.registry.add_run(run, graph_id, str(directory))
        initial = {"run": run, "graph": graph_id, "run_dir": str(directory), "cancelled": False,
                   "nodes": {node_id: _entry() for node_id in graph.nodes}}
        with self._lock(run):
            self._invoke(initial, run)
        return self.status(run)

    def status(self, run) -> dict:
        self.registry.run(run)
        snapshot = self._snapshot(run)
        values = snapshot.values
        graph, nodes = self._graph(values), values["nodes"]
        stale = {} if values["cancelled"] else ledger.stale(graph, nodes, Path(values["run_dir"]))
        ready = _ready(graph, {**nodes, **{node_id: _entry() for node_id in stale}})
        held = self.registry.held(run)
        rows = []
        for node_id in graph.order():
            entry = nodes[node_id]
            row = {"id": node_id, "kind": graph.nodes[node_id].kind, "attempt": entry["attempt"],
                   "status": ("stale" if node_id in stale
                              else "ready" if node_id in ready else entry["status"])}
            for key in ("reason", "note", "blocked"):
                if entry[key]:
                    row[key] = entry[key]
            if node_id in stale:
                row["reason"] = stale[node_id]
            if node_id in held:
                row["holds"] = held[node_id]
            rows.append(row)
        return {
            "run": run, "graph": graph.id, "run_dir": values["run_dir"],
            "revision": snapshot.config["configurable"]["checkpoint_id"],
            "cancelled": values["cancelled"],
            "complete": not stale and all(entry["status"] == "done" for entry in nodes.values()),
            "nodes": rows, "ready": ready, "stale": sorted(stale),
            "tasks": [graph.nodes[node_id].describe() for node_id in ready
                      if graph.nodes[node_id].kind != "step"],
        }

    def begin(self, run, node_id, revision) -> dict:
        def decide(graph, run_dir, nodes):
            node = self._node(graph, node_id)
            if node.kind != "agent":
                raise ValueError(f"{node_id} is a {node.kind} node; only an agent node begins")
            if node_id not in _ready(graph, nodes):
                raise ValueError(f"{node_id} is not ready: {_why_not_ready(graph, nodes, node_id)}")
            self.registry.reserve(run, node_id, node.resources)
            return {"nodes": {node_id: _entry(status="running",
                                              attempt=nodes[node_id]["attempt"] + 1)}}
        return self._apply(run, revision, decide)

    def record(self, run, node_id, revision, note=None) -> dict:
        def decide(graph, run_dir, nodes):
            node, entry = self._node(graph, node_id), nodes.get(node_id)
            if node.kind != "agent" or entry["status"] != "running":
                raise ValueError(f"{node_id} is {entry['status']}; an agent node records after "
                                 "workflow_begin")
            waiting = [name for name in node.depends + node.after
                       if nodes[name]["status"] != "done"]
            if waiting:
                raise ValueError(f"{node_id} needs {', '.join(waiting)} done again first")
            recorded = self._evidence(node, run_dir,
                                      {name: nodes[name]["receipt"] for name in node.depends})
            self.registry.release(run, node_id)
            return {"nodes": {node_id: _entry(status="done", attempt=entry["attempt"],
                                              evidence=recorded, receipt=ledger.receipt(recorded),
                                              note=note)}}
        return self._apply(run, revision, decide)

    def fail(self, run, node_id, revision, reason, uncertain=False) -> dict:
        def decide(graph, run_dir, nodes):
            node, entry = self._node(graph, node_id), nodes.get(node_id)
            if node.kind != "agent" or entry["status"] != "running":
                raise ValueError(f"{node_id} is {entry['status']}; only a running agent node fails")
            if not uncertain:
                self.registry.release(run, node_id)
            return {"nodes": {node_id: _entry(status="uncertain" if uncertain else "failed",
                                              attempt=entry["attempt"], reason=reason)}}
        return self._apply(run, revision, decide)

    def retry(self, run, node_id, revision) -> dict:
        def decide(graph, run_dir, nodes):
            node, entry = self._node(graph, node_id), nodes.get(node_id)
            waiting_step = node.kind == "step" and entry["status"] == "waiting"
            if entry["status"] not in ("failed", "uncertain") and not waiting_step:
                raise ValueError(f"{node_id} is {entry['status']}; retry takes a failed or "
                                 "uncertain node, or a step that is waiting")
            self.registry.release(run, node_id)
            return {"nodes": {node_id: _entry(attempt=entry["attempt"])}}
        return self._apply(run, revision, decide)

    def approve(self, run, node_id, revision, approved, reason=None) -> dict:
        def decide(graph, run_dir, nodes):
            node = self._node(graph, node_id)
            if node.kind != "approval":
                raise ValueError(f"{node_id} is a {node.kind} node, not an approval")
            if node_id not in _ready(graph, nodes):
                raise ValueError(f"{node_id} is not ready: {_why_not_ready(graph, nodes, node_id)}")
            attempt = nodes[node_id]["attempt"] + 1
            if not approved:
                if not reason:
                    raise ValueError("A rejection needs a reason")
                return {"nodes": {node_id: _entry(status="failed", attempt=attempt, reason=reason)}}
            recorded = ledger.evidence(node, run_dir,
                                       {name: nodes[name]["receipt"] for name in node.depends})
            return {"nodes": {node_id: _entry(status="done", attempt=attempt, evidence=recorded,
                                              receipt=ledger.receipt(recorded), note=reason)}}
        return self._apply(run, revision, decide)

    def cancel(self, run, revision, reason) -> dict:
        def decide(graph, run_dir, nodes):
            self.registry.cancel_run(run)
            return {"cancelled": True,
                    "nodes": {node_id: {**entry, "status": "cancelled", "reason": reason}
                              for node_id, entry in nodes.items() if entry["status"] != "done"}}
        return self._apply(run, revision, decide)
