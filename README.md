# CAVI Pipeline

Workflow engine for CAVI pipeline MCP servers, built on LangGraph. A pipeline server declares its workflows as graph files and mounts the engine; the engine adds the `workflow_*` tools, checkpoints every run in SQLite, tracks the evidence of each finished node, and holds shared resources across runs.

Requires Python 3.15 or newer.

## Mount it in a server

```python
from pathlib import Path

from cavi_mcp_core.server import Server
from cavi_pipeline import mount

server = Server("my-pipeline", package="my-pipeline")


def cites_sources(run_dir: Path) -> None:
    """A check hook: raise ValueError to refuse the record."""


def render(run_dir: Path) -> None:
    """A run hook: does the work of a step node."""


engine = mount(
    server,
    graphs=Path(__file__).parent / "graphs",
    hooks={"cites_sources": cites_sources, "render": render},
    store=Path("~/.local/state/my-pipeline/pipeline.sqlite").expanduser(),
    runs=Path("~/my-pipeline/runs").expanduser(),
)
```

`mount` refuses a graph that names a hook the server did not give.

## Graph files

One JSON file per graph, named after its id, at most 1 MiB and 32 nodes.

```json
{
  "version": 1,
  "id": "cut",
  "nodes": [
    {"id": "research", "kind": "agent", "inputs": ["source.md"],
     "outputs": {"notes": "research.json"}, "check": "cites_sources",
     "task": "Read source.md and write research.json."},
    {"id": "render", "kind": "step", "depends": ["research"],
     "outputs": {"frames": "frames.bin"}, "resources": ["gpu"], "run": "render"},
    {"id": "review", "kind": "approval", "depends": ["render"]}
  ]
}
```

| Field | Meaning |
|---|---|
| `kind` | `agent` (a model does the task), `step` (the engine calls the `run` hook), `approval` (a person decides) |
| `depends` | Nodes that must be done first; a change to their evidence makes this node stale |
| `after` | Nodes that must be done first, without staleness |
| `inputs` | Files in the run directory the node reads; each must exist when it is recorded |
| `outputs` | Name to file in the run directory; each file has one owning node and must exist when recorded |
| `resources` | Names held while the node runs; one holder at a time across every run in the store |
| `check` | Hook that validates the outputs before a record is accepted |
| `run` | Hook a step node runs |
| `task` | Instructions shown when the node is ready |

Paths are relative to the run directory. An input may not be another node's output; depend on that node instead.

## Tools

| Tool | Effect |
|---|---|
| `workflow_list` | Graphs, their nodes, and the runs in the store |
| `workflow_start` | Start a run of a graph in an existing directory under the runs workspace |
| `workflow_status` | Every node, ready nodes and their tasks, stale nodes, the current revision |
| `workflow_begin` | Reserve a ready agent node's resources and mark it running |
| `workflow_record` | Mark a running agent node done once its outputs exist and its check passes |
| `workflow_fail` | Mark a running agent node failed, or uncertain when its outcome is unknown |
| `workflow_retry` | Return a failed or uncertain node to waiting, or run a waiting step again |
| `workflow_approve` | Approve or reject a ready approval node |
| `workflow_cancel` | End a run and release its resources |

Every write takes the revision from the latest status and refuses an older one.

## Runs

- A recorded node keeps the SHA-256 of its inputs and outputs and the receipts of its dependencies. When any of them changes, the node and everything depending on it show as stale, and the next write returns them to waiting.
- Step nodes run as soon as they are ready. A step that raises ValueError, RuntimeError or OSError fails with that message; any other exception leaves it uncertain, holding its resources until it is retried.
- A ready node whose resource another node holds shows `blocked` with the holder, and starts on a later write once the resource is free.
- A run directory holds one live run. Runs survive a server restart; a new engine on the same store continues them.

## Skill

`skills/pipeline-workflows` teaches an agent the tool loop. The repository is a Claude Code and Codex plugin that ships it.

## License

MIT
