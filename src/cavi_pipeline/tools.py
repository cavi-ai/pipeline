"""The workflow tools a pipeline MCP server mounts."""

import asyncio
from typing import Annotated, Any, Literal

from cavi_mcp_core.server import DESTRUCTIVE, READ_ONLY, WRITES
from pydantic import Field

from .engine import Engine
from .spec import load_directory

Text = Annotated[str, Field(min_length=1, max_length=4096)]


def mount(server, *, graphs, hooks, store, runs, shared=False) -> Engine:
    """Add the workflow tools to ``server`` and return the engine behind them.

    ``graphs`` is a directory of graph files, ``hooks`` maps every hook name they use to a
    callable taking the directory node paths resolve against, ``store`` is the SQLite file
    that keeps the runs, ``runs`` the directory holding run directories, and ``shared`` makes
    node paths relative to ``runs`` instead of each run directory.
    """
    engine = Engine(load_directory(graphs), hooks, store=store, runs=runs, shared=shared)
    register(server, lambda: engine)
    return engine


def register(server, engine) -> None:
    """Add the workflow tools to ``server``; each call asks ``engine()`` for the Engine.

    Use it when the store or runs directory is only known once a tool is called; an error
    ``engine()`` raises is the tool's error.
    """

    def call(method, *args):
        return asyncio.to_thread(lambda: getattr(engine(), method)(*args))

    @server.tool(annotations=READ_ONLY)
    async def workflow_list() -> dict[str, Any]:
        """List the workflow graphs with their nodes, and the runs this server keeps."""
        return await call("catalog")

    @server.tool(annotations=WRITES)
    async def workflow_start(graph: str, run_dir: str,
                             level: str | None = None) -> dict[str, Any]:
        """Start a run of a graph in a directory under the runs workspace.

        A graph with levels runs the nodes up to ``level``, its lowest level by default.
        Returns the run status: ready nodes, the tasks of ready agent and approval nodes,
        and the revision every later write must pass.
        """
        return await call("start", graph, run_dir, level)

    @server.tool(annotations=READ_ONLY)
    async def workflow_status(run: str) -> dict[str, Any]:
        """Show every node of a run, what is ready, what went stale, and the current revision."""
        return await call("status", run)

    @server.tool(annotations=WRITES)
    async def workflow_begin(run: str, node: str, revision: str) -> dict[str, Any]:
        """Begin a ready agent node: reserve its resources and mark it running."""
        return await call("begin", run, node, revision)

    @server.tool(annotations=WRITES)
    async def workflow_record(run: str, node: str, revision: str,
                              note: Text | None = None) -> dict[str, Any]:
        """Record a running agent node as done once its outputs exist and its check passes.

        A refused record leaves the node running and names what to fix.
        """
        return await call("record", run, node, revision, note)

    @server.tool(annotations=WRITES)
    async def workflow_fail(run: str, node: str, revision: str, reason: Text,
                            uncertain: bool = False) -> dict[str, Any]:
        """Mark a running agent node failed; uncertain=true when its outcome is unknown.

        An uncertain node keeps its resources until workflow_retry, or until workflow_fail
        records what its outcome turned out to be.
        """
        return await call("fail", run, node, revision, reason, uncertain)

    @server.tool(annotations=WRITES)
    async def workflow_retry(run: str, node: str, revision: str) -> dict[str, Any]:
        """Return a failed or uncertain node to waiting, or run a waiting step again."""
        return await call("retry", run, node, revision)

    @server.tool(annotations=WRITES)
    async def workflow_approve(run: str, node: str, revision: str,
                               decision: Literal["approve", "reject"],
                               reason: Text | None = None) -> dict[str, Any]:
        """Approve or reject a ready approval node; a rejection needs a reason."""
        return await call("approve", run, node, revision, decision == "approve", reason)

    @server.tool(annotations=WRITES)
    async def workflow_escalate(run: str, level: str, revision: str) -> dict[str, Any]:
        """Raise the level of a run so nodes up to ``level`` can run; it never lowers."""
        return await call("escalate", run, level, revision)

    @server.tool(annotations=DESTRUCTIVE)
    async def workflow_cancel(run: str, revision: str, reason: Text) -> dict[str, Any]:
        """Cancel a run and release every resource it holds; a cancelled run takes no writes."""
        return await call("cancel", run, revision, reason)
