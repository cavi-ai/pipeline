"""Graph files: the nodes of a workflow, what each depends on and what it produces."""

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from cavi_mcp_core.files import parse_json, read_bytes

KINDS = ("step", "agent", "approval")
IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
GRAPH_LIMIT = 1024 * 1024
MAX_NODES = 32
TASK_LIMIT = 4096
FIELDS = {"id", "kind", "depends", "after", "inputs", "outputs", "resources", "check", "run",
          "task"}


@dataclass(frozen=True)
class Node:
    id: str
    kind: str
    depends: tuple[str, ...] = ()
    after: tuple[str, ...] = ()
    inputs: tuple[str, ...] = ()
    outputs: tuple[tuple[str, str], ...] = ()
    resources: tuple[str, ...] = ()
    check: str | None = None
    run: str | None = None
    task: str = ""

    @property
    def output_paths(self) -> tuple[str, ...]:
        return tuple(path for _, path in self.outputs)

    def describe(self) -> dict:
        return {
            "id": self.id, "kind": self.kind, "depends": list(self.depends),
            "after": list(self.after), "inputs": list(self.inputs),
            "outputs": dict(self.outputs), "resources": list(self.resources), "task": self.task,
        }


@dataclass(frozen=True)
class Graph:
    id: str
    nodes: dict[str, Node]

    def hooks(self) -> set[str]:
        """The check and run hook names the graph needs."""
        return {name for node in self.nodes.values() for name in (node.check, node.run) if name}

    def descendants(self, node_id: str) -> set[str]:
        """Nodes that depend on ``node_id``, directly or through other dependencies."""
        found, pending = set(), [node_id]
        while pending:
            current = pending.pop()
            for node in self.nodes.values():
                if current in node.depends and node.id not in found:
                    found.add(node.id)
                    pending.append(node.id)
        return found

    def order(self) -> list[str]:
        """Node ids with every dependency and ``after`` node before the node itself."""
        placed, ordered = set(), []
        while len(ordered) < len(self.nodes):
            for node in self.nodes.values():
                if node.id not in placed and placed.issuperset(node.depends + node.after):
                    placed.add(node.id)
                    ordered.append(node.id)
        return ordered


def _identifier(value, what):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{what} must be an identifier (a letter, then letters, digits, - or _)")
    return value


def _names(value, what):
    if not isinstance(value, list) or len(set(map(str, value))) != len(value):
        raise ValueError(f"{what} must be a list of distinct names")
    return tuple(_identifier(item, what) for item in value)


def _path(value, what):
    if not isinstance(value, str) or not value or any(ord(c) < 32 for c in value):
        raise ValueError(f"{what} must be a nonempty relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or str(path) != value:
        raise ValueError(f"{what} must be a normalized relative path without ..: {value}")
    return value


def _node(value, index) -> Node:
    if not isinstance(value, dict):
        raise ValueError(f"nodes[{index}] must be an object")
    node_id = _identifier(value.get("id"), f"nodes[{index}].id")
    where = f"node {node_id}"
    unknown = sorted(set(value) - FIELDS)
    if unknown:
        raise ValueError(f"{where} has unknown fields: {', '.join(unknown)}")
    kind = value.get("kind")
    if kind not in KINDS:
        raise ValueError(f"{where} kind must be one of {', '.join(KINDS)}")
    outputs = value.get("outputs", {})
    if not isinstance(outputs, dict):
        raise ValueError(f"{where} outputs must map names to paths")
    outputs = tuple((_identifier(name, f"{where} output name"), _path(path, f"{where} output"))
                    for name, path in outputs.items())
    inputs = value.get("inputs", [])
    if not isinstance(inputs, list):
        raise ValueError(f"{where} inputs must be a list of paths")
    inputs = tuple(_path(path, f"{where} input") for path in inputs)
    task = value.get("task", "")
    if not isinstance(task, str) or len(task) > TASK_LIMIT:
        raise ValueError(f"{where} task must be text of at most {TASK_LIMIT} characters")
    hooks = {key: value.get(key) for key in ("check", "run")}
    for key, name in hooks.items():
        if name is not None:
            _identifier(name, f"{where} {key}")
    if kind == "step" and hooks["run"] is None:
        raise ValueError(f"{where} is a step and needs a run hook")
    if kind != "step" and hooks["run"] is not None:
        raise ValueError(f"{where} is {kind}; only a step has a run hook")
    if kind == "approval" and (outputs or hooks["check"] is not None):
        raise ValueError(f"{where} is an approval and has no outputs or check")
    if set(inputs) & {path for _, path in outputs}:
        raise ValueError(f"{where} lists one path as both input and output")
    return Node(
        id=node_id, kind=kind,
        depends=_names(value.get("depends", []), f"{where} depends"),
        after=_names(value.get("after", []), f"{where} after"),
        inputs=inputs, outputs=outputs,
        resources=_names(value.get("resources", []), f"{where} resources"),
        check=hooks["check"], run=hooks["run"], task=task,
    )


def parse(value) -> Graph:
    """A validated graph from its JSON value."""
    if not isinstance(value, dict) or set(value) - {"version", "id", "description", "nodes"}:
        raise ValueError("A graph has version, id, an optional description and nodes")
    if value.get("version") != 1:
        raise ValueError("Graph version must be 1")
    graph_id = _identifier(value.get("id"), "Graph id")
    raw = value.get("nodes")
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_NODES:
        raise ValueError(f"Graph {graph_id} needs 1 to {MAX_NODES} nodes")
    nodes = {}
    for index, item in enumerate(raw):
        node = _node(item, index)
        if node.id in nodes:
            raise ValueError(f"Graph {graph_id} repeats node {node.id}")
        nodes[node.id] = node
    owners = {}
    for node in nodes.values():
        for name in node.depends + node.after:
            if name not in nodes or name == node.id:
                raise ValueError(f"node {node.id} names {name}, which is not another node")
        for path in node.output_paths:
            if path in owners:
                raise ValueError(f"{path} is an output of both {owners[path]} and {node.id}")
            owners[path] = node.id
    for node in nodes.values():
        for path in node.inputs:
            if path in owners:
                raise ValueError(f"node {node.id} input {path} is the output of {owners[path]}; "
                                 "depend on that node instead")
    graph = Graph(graph_id, nodes)
    placed = set()
    while len(placed) < len(nodes):
        free = [n.id for n in nodes.values()
                if n.id not in placed and placed.issuperset(n.depends + n.after)]
        if not free:
            raise ValueError(f"Graph {graph_id} has a cycle through "
                             f"{', '.join(sorted(set(nodes) - placed))}")
        placed.update(free)
    return graph


def load(path) -> Graph:
    """The graph in a JSON file of at most 1 MiB."""
    return parse(parse_json(read_bytes(path, GRAPH_LIMIT)))


def load_directory(directory) -> dict[str, Graph]:
    """Every ``*.json`` graph in ``directory``, by graph id; the file name must be the id."""
    graphs = {}
    for path in sorted(Path(directory).glob("*.json")):
        graph = load(path)
        if graph.id != path.stem:
            raise ValueError(f"{path.name} holds graph {graph.id}; name the file {graph.id}.json")
        graphs[graph.id] = graph
    if not graphs:
        raise ValueError(f"No graph files in {directory}")
    return graphs
