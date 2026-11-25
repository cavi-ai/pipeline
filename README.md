# CAVI Pipeline

Workflow engine for CAVI pipeline MCP servers, built on LangGraph. A pipeline server declares its workflows as graph files and mounts the engine; the engine adds the `workflow_*` tools, checkpoints every run in SQLite, tracks the evidence of each finished node, and holds shared resources across runs.

## Install

Requires Python 3.15 or newer. Download the wheel from [GitHub Releases](https://github.com/cavi-ai/pipeline/releases) into your project directory, then run:

```sh
uv venv --python 3.15
uv pip install ./cavi_pipeline-*.whl
```

## Mount it in a server

Create a graph directory and a workspace for the example run.

```sh
mkdir -p graphs runs/demo
printf 'The demo has one source.\n' > runs/demo/source.md
```

Save this as `server.py`.

```python
import json
from pathlib import Path

from cavi_mcp_core.server import Server
from cavi_pipeline import mount


def read_claims(run_dir: Path) -> list[str]:
    notes = json.loads((run_dir / "research.json").read_text(encoding="utf-8"))
    claims = notes.get("claims") if isinstance(notes, dict) else None
    if not isinstance(claims, list) or not claims or not all(
        isinstance(claim, str) and claim.strip() for claim in claims
    ):
        raise ValueError("research.json needs a nonempty list of claims")
    return claims


def write_report(run_dir: Path) -> None:
    report = "\n".join(read_claims(run_dir)) + "\n"
    (run_dir / "report.txt").write_text(report, encoding="utf-8")


root = Path(__file__).resolve().parent
server = Server("my-pipeline", version="1")
mount(
    server,
    graphs=root / "graphs",
    hooks={"read_claims": read_claims, "write_report": write_report},
    store=root / "pipeline.sqlite",
    runs=root / "runs",
)

if __name__ == "__main__":
    server.run(transport="stdio")
```

Save this as `graphs/cut.json`.

```json
{
  "version": 1,
  "id": "cut",
  "nodes": [
    {"id": "research", "kind": "agent", "inputs": ["source.md"],
     "outputs": {"notes": "research.json"}, "check": "read_claims",
     "task": "Read source.md and write a JSON object with a claims list to research.json."},
    {"id": "report", "kind": "step", "depends": ["research"],
     "outputs": {"report": "report.txt"}, "run": "write_report"},
    {"id": "review", "kind": "approval", "depends": ["report"]}
  ]
}
```

Configure your MCP client to launch `uv run --no-project python server.py` over stdio from this directory.

1. Call `workflow_start` with `graph` set to `cut` and `run_dir` set to `demo`.
2. Call `workflow_begin` for `research` using the returned run ID and revision. Read `runs/demo/source.md` and write `{"claims": ["The demo has one source."]}` to `runs/demo/research.json`.
3. Call `workflow_record` for `research` using the revision from `workflow_begin`. The engine writes `runs/demo/report.txt` and offers `review`.
4. Read the report and call `workflow_approve` for `review` with decision `approve` and the latest revision. The run reports `complete: true`.

## Graph files

One JSON file per graph, named after its id, at most 1 MiB and 32 nodes. Pass every hook named in a graph to `mount`.

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
