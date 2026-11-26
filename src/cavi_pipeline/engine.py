"""Workflow runs as LangGraph threads: run the ready steps, then wait for the next event."""

import hashlib
import json
import logging
import os
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Annotated, TypedDict

from cavi_mcp_core.files import inside, parse_json, read_bytes
from cavi_mcp_core.lock import FileLock
from cavi_mcp_core.server import ANTICIPATED
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import START, StateGraph
from langgraph.types import Command, Send, interrupt
from langsmith import tracing_context

from . import ledger
from .reservations import Registry
from .spec import GRAPH_LIMIT, MAX_NODES, parse

logger = logging.getLogger(__name__)

# Two supersteps per step that finishes in one event, and the schedule and wait around them.
RECURSION_LIMIT = 4 * MAX_NODES + 8
DETAIL_LIMIT = 1024 * 1024
GRAPH_FILE = "graph.json"
ACTIVE = ("running", "uncertain")
_CURRENT_REVISION = object()
_KEEP = object()


def _merge(left: dict, right: dict) -> dict:
    return {**left, **right}


class RunState(TypedDict):
    run: str
    graph: str
    run_dir: str
    cancelled: bool
    nodes: Annotated[dict, _merge]
    level: str | None
    sealed: dict
    graph_sha256: str | None


def _entry(**fields) -> dict:
    entry = {"status": "waiting", "attempt": 0, "evidence": None, "receipt": None,
             "reason": None, "note": None, "blocked": None, "detail": None}
    entry.update(fields)
    return entry


def _detail(value):
    """A copy of a server detail: a JSON object of at most 1 MiB, or None."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("A node detail must be a JSON object")
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"A node detail must be JSON: {exc}") from None
    if len(encoded.encode()) > DETAIL_LIMIT:
        raise ValueError("A node detail must be at most 1 MiB of JSON")
    return json.loads(encoded)


def _ready(graph, nodes, excluded=()) -> list[str]:
    done = {node_id for node_id, entry in nodes.items() if entry["status"] == "done"}
    return [node_id for node_id in graph.order()
            if node_id not in excluded and nodes[node_id]["status"] == "waiting"
            and done.issuperset(graph.nodes[node_id].depends + graph.nodes[node_id].after)]


def _why_not_ready(graph, nodes, node_id, excluded) -> str:
    status = nodes[node_id]["status"]
    if status != "waiting":
        return f"{node_id} is {status}"
    if node_id in excluded:
        return f"{node_id} {excluded[node_id]}"
    node = graph.nodes[node_id]
    waiting = [name for name in node.depends + node.after if nodes[name]["status"] != "done"]
    return f"{node_id} waits on {', '.join(waiting)}"


def _writable(node, dispatch, action):
    if node.kind == "worker" and not dispatch:
        raise ValueError(f"{node.id} is a worker node; the server's own tools dispatch it")
    if node.kind not in ("agent", "worker"):
        raise ValueError(f"{node.id} is a {node.kind} node; only an agent node {action}")


class Engine:
    """Runs of the given graphs, checkpointed in ``store`` with run directories under ``runs``.

    ``hooks`` maps every check and run hook a graph names to a callable taking the directory
    node paths resolve against; a hook refuses by raising ValueError.

    With ``shared``, node paths resolve against ``runs`` and every run shares them: a run
    starts in a new directory that holds only its own records, its inputs are sealed when it
    starts, and a running node holds its output paths against every other run in the store.
    """

    def __init__(self, graphs, hooks, *, store, runs, shared=False):
        self.graphs, self.hooks, self.shared = dict(graphs), dict(hooks), shared
        for graph in self.graphs.values():
            self._admit(graph)
        self.runs_root = Path(runs).resolve(strict=True)
        store = Path(store)
        store.parent.mkdir(parents=True, exist_ok=True)
        self._store_dir = store.parent.resolve(strict=True)
        self._lock_dir = store.parent / f"{store.name}.locks"
        self._lock_dir.mkdir(exist_ok=True)
        self._db = sqlite3.connect(store, check_same_thread=False, timeout=30)
        self._saver = SqliteSaver(self._db)
        self._saver.setup()
        self.registry = Registry(store)
        self._locks, self._guard, self._documents = {}, threading.Lock(), {}
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
            # A cancelled run still takes the outcomes of the work it left unresolved.
            return Command(goto="wait")
        graph, nodes = self._graph_of(state), state["nodes"]
        excluded = self._excluded(graph, state, nodes)
        sends = [
            Send("step", {"run": state["run"], "graph": state["graph"], "run_dir": state["run_dir"],
                          "graph_sha256": state.get("graph_sha256"), "node": node_id,
                          "attempt": nodes[node_id]["attempt"],
                          "upstream": {name: nodes[name]["receipt"]
                                       for name in graph.nodes[node_id].depends}})
            for node_id in _ready(graph, nodes, excluded)
            if graph.nodes[node_id].kind == "step" and not nodes[node_id]["blocked"]
        ]
        return Command(goto=sends or "wait")

    def _step(self, task):
        node = self._graph_of(task).nodes[task["node"]]
        run, root, attempt = task["run"], self._root(task), task["attempt"] + 1
        try:
            self.registry.reserve(run, node.id, node.resources, self._outputs(node))
        except ValueError as exc:
            return {"nodes": {node.id: _entry(attempt=task["attempt"], blocked=str(exc))}}
        try:
            self.hooks[node.run](root)
            recorded = self._evidence(node, root, task["upstream"])
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
        result = {"nodes": update}
        for key in ("cancelled", "level"):
            if key in event:
                result[key] = event[key]
        return result

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

    def _admit(self, graph):
        missing = sorted(graph.hooks() - set(self.hooks))
        if missing:
            raise ValueError(f"The graphs name hooks that were not given: {', '.join(missing)}")
        return graph

    def _root(self, values) -> Path:
        return self.runs_root if self.shared else Path(values["run_dir"])

    def _outputs(self, node) -> tuple[str, ...]:
        return node.output_paths if self.shared else ()

    def _graph_of(self, values):
        """The graph a run follows: a server graph, or the copy written when the run started."""
        sha256 = values.get("graph_sha256")
        if sha256 is None:
            graph = self.graphs.get(values["graph"])
            if graph is None:
                raise ValueError(f"Graph {values['graph']} changed since run {values['run']} "
                                 "started; start a new run")
            return graph
        path = Path(values["run_dir"]) / GRAPH_FILE
        try:
            raw = read_bytes(path, GRAPH_LIMIT)
        except (OSError, ValueError) as exc:
            raise ValueError(f"The graph of run {values['run']} is unreadable: {exc}") from None
        if hashlib.sha256(raw).hexdigest() != sha256:
            raise ValueError(f"{path} changed since run {values['run']} started; "
                             "start a new run")
        if sha256 not in self._documents:
            self._documents[sha256] = self._admit(parse(parse_json(raw)))
        return self._documents[sha256]

    def _graph(self, values):
        graph = self._graph_of(values)
        if (set(graph.nodes) != set(values["nodes"])
                or (graph.levels and values.get("level") not in graph.levels)):
            raise ValueError(f"Graph {values['graph']} changed since run {values['run']} started; "
                             "start a new run")
        return graph

    def _node(self, graph, node_id):
        if node_id not in graph.nodes:
            raise ValueError(f"Graph {graph.id} has no node {node_id}")
        return graph.nodes[node_id]

    def _excluded(self, graph, values, nodes) -> dict[str, str]:
        """Waiting nodes that cannot become ready in this run, each with the reason."""
        level = values.get("level")
        found = {node_id: f"is above the run level {level}" for node_id in graph.above(level)}
        sealed = values.get("sealed")
        if sealed:
            for node_id, why in ledger.changed(graph, nodes, self._root(values), sealed).items():
                found.setdefault(node_id, why)
        return found

    def _protect(self, graph, directory):
        """Refuse shared paths in run or store records, non-files and outputs linked to inputs."""
        fences = [(directory, "the run directory"), (self._store_dir, "the store directory")]
        for row in self.registry.runs():
            other = Path(row["run_dir"])
            if other != directory:
                if directory.is_relative_to(other) or other.is_relative_to(directory):
                    raise ValueError(f"{directory} overlaps the directory of run {row['run']}")
                fences.append((other, f"the directory of run {row['run']}"))
        if directory.is_relative_to(self._store_dir) or self._store_dir.is_relative_to(directory):
            raise ValueError(f"{directory} overlaps the store directory")
        found = []
        for node in graph.nodes.values():
            for role, paths in (("input", node.inputs), ("output", node.output_paths)):
                for relative in paths:
                    path = inside(self.runs_root, relative, strict=True)
                    for fence, name in fences:
                        if path.is_relative_to(fence) or fence.is_relative_to(path):
                            raise ValueError(f"node {node.id} {role} {relative} overlaps {name}")
                    if path.exists() and not path.is_file():
                        raise ValueError(f"node {node.id} {role} {relative} is not a regular file")
                    found.append((node.id, role, relative, path))
        identities = {(info.st_dev, info.st_ino) for _, role, _, path in found
                      if role == "input" and path.exists() for info in (path.stat(),)}
        for node_id, role, relative, path in found:
            if role == "output" and path.exists():
                info = path.stat()
                if (info.st_dev, info.st_ino) in identities:
                    raise ValueError(f"node {node_id} output {relative} is a link to an input")

    def _evidence(self, node, root, upstream):
        recorded = ledger.evidence(node, root, upstream)
        if node.check:
            self.hooks[node.check](root)
        return recorded

    def _snapshot(self, run):
        snapshot = self._app.get_state(self._config(run))
        if not snapshot.values:
            raise ValueError(f"Run {run} has no state in this store")
        return snapshot

    def _apply(self, run, revision, decide, *, max_events=1, cancelled=False):
        """Apply events under the run lock, checking the revision before the first write.

        ``decide(graph, root, nodes, values)`` returns the event; with ``cancelled`` it also
        runs on a cancelled run, where a failure it records shows as cancelled.
        """
        self.registry.run(run)
        with self._lock(run):
            snapshot = self._snapshot(run)
            if snapshot.next and not snapshot.interrupts:
                self._invoke(None, run)
                snapshot = self._snapshot(run)
            values = snapshot.values
            current = snapshot.config["configurable"]["checkpoint_id"]
            if revision is not _CURRENT_REVISION and revision != current:
                raise ValueError(f"Revision {revision} is not the current revision {current}; "
                                 "read workflow_status and act on what it shows")
            if values["cancelled"] and not cancelled:
                raise ValueError(f"Run {run} is cancelled")
            graph, root = self._graph(values), self._root(values)
            for index in range(max_events):
                if index:
                    values = self._snapshot(run).values
                nodes = values["nodes"]
                resets = {} if values["cancelled"] else {
                    node_id: _entry(attempt=nodes[node_id]["attempt"], reason=f"stale: {why}")
                    for node_id, why in ledger.stale(graph, nodes, root).items()}
                event = decide(graph, root, {**nodes, **resets}, values)
                if values["cancelled"]:
                    event["nodes"] = {
                        node_id: {**entry, "status": "cancelled"} if entry["status"] == "failed"
                        else entry for node_id, entry in event.get("nodes", {}).items()}
                # After the initial event, continue only for new work the caller requested.
                # Otherwise a step that invalidates its own prerequisites could run repeatedly.
                if not (event.get("nodes") or event.get("cancelled") or "level" in event
                        or (index == 0 and resets)):
                    break
                event["nodes"] = {**resets, **event.get("nodes", {})}
                self._invoke(Command(resume=event), run)
            return self._status(run)

    # Actions.

    def catalog(self) -> dict:
        return {
            "graphs": [{"id": graph.id, "nodes": [node.describe() for node in graph.nodes.values()],
                        **({"levels": list(graph.levels)} if graph.levels else {})}
                       for graph in self.graphs.values()],
            "runs": [{"run": row["run"], "graph": row["graph"], "run_dir": row["run_dir"],
                      "state": row["state"]} for row in self.registry.runs()],
        }

    def start(self, graph_id, run_dir, level=None, *, document=None) -> dict:
        """Start a run of a server graph, or of ``document`` on a shared-workspace engine.

        A document is a graph value; the run keeps a read-only copy in its directory and
        refuses to continue once that copy changes.
        """
        raw = None
        if document is not None:
            if not self.shared:
                raise ValueError("A graph document needs an engine with a shared workspace")
            graph = self._admit(parse(document))
            raw = (json.dumps(document, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
            if len(raw) > GRAPH_LIMIT:
                raise ValueError("A graph document must be at most 1 MiB")
            graph_id = graph.id
        else:
            graph = self.graphs.get(graph_id)
            if graph is None:
                raise ValueError(f"Unknown graph {graph_id}; workflow_list names the graphs")
        if graph.levels:
            level = graph.levels[0] if level is None else level
            if level not in graph.levels:
                raise ValueError(f"Choose a level of graph {graph.id}: {', '.join(graph.levels)}")
        elif level is not None:
            raise ValueError(f"Graph {graph.id} has no levels")
        directory = inside(self.runs_root, run_dir, strict=True)
        sealed = {}
        if self.shared:
            if directory.exists() or directory.is_symlink():
                raise ValueError(f"{run_dir} exists; a shared-workspace run starts in a new "
                                 "directory")
            self._protect(graph, directory)
            sealed = ledger.seal(graph, self.runs_root)
        elif not directory.is_dir():
            raise ValueError(f"{run_dir} is not a directory in the runs workspace")
        run = f"{graph_id}-{uuid.uuid4().hex[:8]}"
        initial = {"run": run, "graph": graph_id, "run_dir": str(directory), "cancelled": False,
                   "nodes": {node_id: _entry() for node_id in graph.nodes}, "level": level,
                   "sealed": sealed,
                   "graph_sha256": None if raw is None else hashlib.sha256(raw).hexdigest()}
        with self._lock(run):
            if self.shared:
                try:
                    directory.mkdir(parents=True, mode=0o700)
                except FileExistsError:
                    raise ValueError(f"{run_dir} exists; a shared-workspace run starts in a new "
                                     "directory") from None
                if raw is not None:
                    self._write_graph(directory / GRAPH_FILE, raw)
            self.registry.add_run(run, graph_id, str(directory))
            self._invoke(initial, run)
            return self._status(run)

    @staticmethod
    def _write_graph(path, raw):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                             | getattr(os, "O_NOFOLLOW", 0), 0o444)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

    def adopt(self, graph_id, run_dir) -> str:
        """The live run in ``run_dir``, started when the directory has none."""
        directory = str(inside(self.runs_root, run_dir, strict=True))
        live = self.registry.live(directory)
        if live is None:
            try:
                return self.start(graph_id, run_dir)["run"]
            except ValueError:
                # Another process may have started it between the lookup and the start.
                live = self.registry.live(directory)
                if live is None:
                    raise
        if live["graph"] != graph_id:
            raise ValueError(f"Run {live['run']} in {run_dir} is a {live['graph']} run")
        return live["run"]

    def check(self, run, node_id) -> dict:
        """Settle ``node_id`` and every node before it by their checks against the files now.

        Raises the reason ``node_id`` is not done; see ``require``.
        """
        return self._settle(run, node_id, include=True)

    def require(self, run, node_id) -> dict:
        """Settle every node ``node_id`` depends on and raise the first one that is not done.

        Eligible agents can be recorded done without workflow_begin. Running and uncertain
        attempts keep their state. A done agent that no longer passes waits again with the
        check's reason. Ready steps run through the scheduler. Returns the run status.
        """
        return self._settle(run, node_id, include=False)

    def _settle(self, run, node_id, include):
        wanted, checked, retried = set(), set(), set()
        reasons = {}

        def decide(graph, root, nodes, values):
            self._node(graph, node_id)
            wanted.update(graph.ancestors(node_id))
            if include:
                wanted.add(node_id)
            excluded = self._excluded(graph, values, nodes)
            merged, update = dict(nodes), {}
            for current in graph.order():
                spec, entry = graph.nodes[current], merged[current]
                if (current not in wanted or current in checked or current in excluded
                        or spec.kind != "agent" or entry["status"] in ACTIVE
                        or any(merged[name]["status"] != "done"
                               for name in spec.depends + spec.after)):
                    continue
                checked.add(current)
                upstream = {name: merged[name]["receipt"] for name in spec.depends}
                try:
                    recorded = self._evidence(spec, root, upstream)
                except ValueError as exc:
                    reason = reasons[current] = str(exc)
                    if entry["status"] == "failed" or entry["reason"] == reason:
                        continue
                    next_entry = _entry(attempt=entry["attempt"], reason=reason)
                else:
                    receipt = ledger.receipt(recorded)
                    if entry["status"] == "done" and entry["receipt"] == receipt:
                        continue
                    self.registry.release(run, current)
                    next_entry = _entry(
                        status="done", attempt=entry["attempt"] + 1, evidence=recorded,
                        receipt=receipt)
                update[current] = merged[current] = next_entry
                if entry["status"] == "done":
                    # The scheduler must see invalidation in the same event as this change.
                    change = ("was recorded again" if next_entry["status"] == "done"
                              else "is no longer done")
                    for dependent in graph.descendants(current):
                        if merged[dependent]["status"] == "done":
                            update[dependent] = merged[dependent] = _entry(
                                attempt=merged[dependent]["attempt"],
                                reason=f"stale: {current} {change}")

            if not update:
                for current in _ready(graph, merged, excluded):
                    entry, spec = merged[current], graph.nodes[current]
                    if (current not in wanted or current in retried or spec.kind != "step"
                            or not entry["blocked"]):
                        continue
                    if self.registry.conflict(run, current, spec.resources,
                                              self._outputs(spec)) is None:
                        retried.add(current)
                        update[current] = {**entry, "blocked": None}
            return {"nodes": update}

        # Steps can unblock another layer of agents on each scheduler pass.
        status = self._apply(run, _CURRENT_REVISION, decide, max_events=MAX_NODES + 1)
        for row in status["nodes"]:
            name, kind, state = row["id"], row["kind"], row["status"]
            if name not in wanted or state == "done":
                continue
            if name in reasons:
                raise ValueError(reasons[name])
            if blocked := row.get("blocked"):
                raise ValueError(f"{name} is blocked: {blocked}")
            if kind == "approval" and state in ("waiting", "ready"):
                raise ValueError(f"{name} is waiting; approval nodes are not settled by a check")
            detail = f": {row['reason']}" if row.get("reason") else ""
            raise ValueError(f"{name} is {state}{detail}")
        return status

    def status(self, run) -> dict:
        self.registry.run(run)
        # Read the checkpoint, evidence and reservations between writes to this run.
        with self._lock(run):
            return self._status(run)

    def _status(self, run) -> dict:
        snapshot = self._snapshot(run)
        values = snapshot.values
        graph, nodes, root = self._graph(values), values["nodes"], self._root(values)
        stale = {} if values["cancelled"] else ledger.stale(graph, nodes, root)
        view = {**nodes, **{node_id: _entry() for node_id in stale}}
        excluded = {} if values["cancelled"] else self._excluded(graph, values, view)
        deferred = graph.above(values.get("level"))
        ready = _ready(graph, view, excluded)
        held = self.registry.held(run)
        rows = []
        for node_id in graph.order():
            entry, spec = nodes[node_id], graph.nodes[node_id]
            status = entry["status"]
            if node_id in stale:
                status = "stale"
            elif node_id in ready:
                status = "ready"
            elif status == "waiting" and node_id in deferred:
                status = "deferred"
            elif status in ("waiting", "failed") and node_id in excluded:
                status = "stale"
            row = {"id": node_id, "kind": spec.kind, "attempt": entry["attempt"],
                   "status": status}
            if spec.level is not None:
                row["level"] = spec.level
            for key in ("reason", "note", "blocked", "detail"):
                if entry.get(key):
                    row[key] = entry[key]
            if node_id in stale:
                row["reason"] = stale[node_id]
            elif status == "stale":
                row["reason"] = excluded[node_id]
            elif node_id in excluded and node_id not in deferred:
                row["changed"] = excluded[node_id]
            if status == "ready" and spec.kind != "step":
                conflict = self.registry.conflict(run, node_id, spec.resources,
                                                  self._outputs(spec))
                if conflict:
                    row["blocked"] = conflict
            if node_id in held:
                row["holds"] = held[node_id]
            rows.append(row)
        status = {
            "run": run, "graph": graph.id, "run_dir": values["run_dir"],
            "revision": snapshot.config["configurable"]["checkpoint_id"],
            "cancelled": values["cancelled"],
            "complete": not stale and all(entry["status"] == "done"
                                          for node_id, entry in nodes.items()
                                          if node_id not in deferred),
            "nodes": rows, "ready": ready, "stale": sorted(stale),
            "tasks": [graph.nodes[node_id].describe() for node_id in ready
                      if graph.nodes[node_id].kind != "step"],
        }
        if graph.levels:
            status["level"] = values["level"]
        return status

    def evidence(self, run) -> dict:
        """The inputs the run sealed when it started and the evidence each done node recorded."""
        self.registry.run(run)
        with self._lock(run):
            values = self._snapshot(run).values
        return {"sealed": values.get("sealed") or {},
                "nodes": {node_id: entry["evidence"] for node_id, entry in values["nodes"].items()
                          if entry["status"] == "done"}}

    def begin(self, run, node_id, revision, *, dispatch=False, detail=None) -> dict:
        """Mark a ready agent node running; a worker node needs ``dispatch`` from server code."""
        detail = _detail(detail)

        def decide(graph, root, nodes, values):
            node = self._node(graph, node_id)
            _writable(node, dispatch, "begins")
            excluded = self._excluded(graph, values, nodes)
            if node_id not in _ready(graph, nodes, excluded):
                reason = _why_not_ready(graph, nodes, node_id, excluded)
                raise ValueError(f"{node_id} is not ready: {reason}")
            if self.shared:
                self._protect(graph, Path(values["run_dir"]))
            self.registry.reserve(run, node_id, node.resources, self._outputs(node))
            return {"nodes": {node_id: _entry(status="running",
                                              attempt=nodes[node_id]["attempt"] + 1,
                                              detail=detail)}}
        return self._apply(run, revision, decide)

    def record(self, run, node_id, revision, note=None, *, dispatch=False, detail=_KEEP) -> dict:
        def decide(graph, root, nodes, values):
            node, entry = self._node(graph, node_id), nodes.get(node_id)
            if node.kind == "worker":
                _writable(node, dispatch, "records")
            if node.kind not in ("agent", "worker") or entry["status"] != "running":
                raise ValueError(f"{node_id} is {entry['status']}; an agent node records after "
                                 "workflow_begin")
            waiting = [name for name in node.depends + node.after
                       if nodes[name]["status"] != "done"]
            if waiting:
                raise ValueError(f"{node_id} needs {', '.join(waiting)} done again first")
            moved = self._excluded(graph, values, nodes).get(node_id)
            if moved:
                raise ValueError(f"{node_id} cannot record: {moved}")
            if self.shared:
                self._protect(graph, Path(values["run_dir"]))
            recorded = self._evidence(node, root,
                                      {name: nodes[name]["receipt"] for name in node.depends})
            self.registry.release(run, node_id)
            return {"nodes": {node_id: _entry(
                status="done", attempt=entry["attempt"], evidence=recorded,
                receipt=ledger.receipt(recorded), note=note,
                detail=entry.get("detail") if detail is _KEEP else _detail(detail))}}
        return self._apply(run, revision, decide)

    def fail(self, run, node_id, revision, reason, uncertain=False, *, dispatch=False,
             detail=_KEEP) -> dict:
        """Mark a running or uncertain node failed, or uncertain when its outcome is unknown.

        Failing an uncertain node records that its outcome is now known and releases it; this
        is the one write a cancelled run takes.
        """
        def decide(graph, root, nodes, values):
            node, entry = self._node(graph, node_id), nodes.get(node_id)
            if node.kind == "worker":
                _writable(node, dispatch, "fails")
            if node.kind not in ("agent", "worker") or entry["status"] not in ACTIVE:
                raise ValueError(f"{node_id} is {entry['status']}; only a running or uncertain "
                                 "agent node fails")
            if not uncertain:
                self.registry.release(run, node_id)
            return {"nodes": {node_id: _entry(
                status="uncertain" if uncertain else "failed", attempt=entry["attempt"],
                reason=reason,
                detail=entry.get("detail") if detail is _KEEP else _detail(detail))}}
        return self._apply(run, revision, decide, cancelled=True)

    def retry(self, run, node_id, revision, *, dispatch=False, detail=None) -> dict:
        detail = _detail(detail)

        def decide(graph, root, nodes, values):
            node, entry = self._node(graph, node_id), nodes.get(node_id)
            if node.kind == "worker":
                _writable(node, dispatch, "retries")
            waiting_step = node.kind == "step" and entry["status"] == "waiting"
            if entry["status"] not in ("failed", "uncertain") and not waiting_step:
                raise ValueError(f"{node_id} is {entry['status']}; retry takes a failed or "
                                 "uncertain node, or a step that is waiting")
            self.registry.release(run, node_id)
            return {"nodes": {node_id: _entry(attempt=entry["attempt"], detail=detail)}}
        return self._apply(run, revision, decide)

    def annotate(self, run, node_id, revision, detail) -> dict:
        """Replace the server detail of a running agent or worker node."""
        detail = _detail(detail)

        def decide(graph, root, nodes, values):
            node, entry = self._node(graph, node_id), nodes.get(node_id)
            if node.kind not in ("agent", "worker") or entry["status"] != "running":
                raise ValueError(f"{node_id} is {entry['status']}; only a running node takes "
                                 "a detail")
            if entry.get("detail") == detail:
                return {"nodes": {}}
            return {"nodes": {node_id: {**entry, "detail": detail}}}
        return self._apply(run, revision, decide)

    def approve(self, run, node_id, revision, approved, reason=None) -> dict:
        def decide(graph, root, nodes, values):
            node = self._node(graph, node_id)
            if node.kind != "approval":
                raise ValueError(f"{node_id} is a {node.kind} node, not an approval")
            excluded = self._excluded(graph, values, nodes)
            if node_id not in _ready(graph, nodes, excluded):
                reason_text = _why_not_ready(graph, nodes, node_id, excluded)
                raise ValueError(f"{node_id} is not ready: {reason_text}")
            attempt = nodes[node_id]["attempt"] + 1
            if not approved:
                if not reason:
                    raise ValueError("A rejection needs a reason")
                return {"nodes": {node_id: _entry(status="failed", attempt=attempt, reason=reason)}}
            recorded = ledger.evidence(node, root,
                                       {name: nodes[name]["receipt"] for name in node.depends})
            return {"nodes": {node_id: _entry(status="done", attempt=attempt, evidence=recorded,
                                              receipt=ledger.receipt(recorded), note=reason)}}
        return self._apply(run, revision, decide)

    def escalate(self, run, level, revision) -> dict:
        """Raise the run level; nodes up to the new level become eligible."""
        def decide(graph, root, nodes, values):
            if not graph.levels:
                raise ValueError(f"Graph {graph.id} has no levels")
            if level not in graph.levels:
                raise ValueError(f"Choose a level of graph {graph.id}: {', '.join(graph.levels)}")
            if graph.levels.index(level) < graph.levels.index(values["level"]):
                raise ValueError(f"Run {run} is at level {values['level']}; escalation keeps or "
                                 "raises the level")
            return {"level": level} if level != values["level"] else {}
        return self._apply(run, revision, decide)

    def cancel(self, run, revision, reason, *, hold=False) -> dict:
        """End a run. With ``hold``, running and uncertain nodes stay uncertain and keep their
        reservations until a failure records their outcome."""
        def decide(graph, root, nodes, values):
            kept = [node_id for node_id, entry in nodes.items()
                    if hold and entry["status"] in ACTIVE]
            self.registry.cancel_run(run, keep=kept)
            update = {}
            for node_id, entry in nodes.items():
                if node_id in kept:
                    if entry["status"] == "running":
                        update[node_id] = {**entry, "status": "uncertain",
                                           "reason": f"cancelled while running: {reason}"}
                elif entry["status"] != "done":
                    update[node_id] = {**entry, "status": "cancelled", "reason": reason}
            return {"cancelled": True, "nodes": update}
        return self._apply(run, revision, decide)
