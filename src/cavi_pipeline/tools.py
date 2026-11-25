"""The workflow tools a pipeline MCP server mounts."""

import asyncio
from typing import Annotated, Any, Literal

from cavi_mcp_core.server import DESTRUCTIVE, READ_ONLY, WRITES
from pydantic import Field

from .engine import Engine
from .spec import load_directory

Text = Annotated[str, Field(min_length=1, max_length=4096)]


def mount(server, *, graphs, hooks, store, runs) -> Engine:
    """Add the workflow tools to ``server`` and return the engine behind them.

    ``graphs`` is a directory of graph files, ``hooks`` maps every hook name they use to a
    callable taking the run directory, ``store`` is the SQLite file that keeps the runs and
    ``runs`` the directory holding run directories.
    """
    engine = Engine(load_directory(graphs), hooks, store=store, runs=runs)

    def call(fn, *args, **kwargs):
        return asyncio.to_thread(fn, *args, **kwargs)

    @server.tool(annotations=READ_ONLY)
    async def workflow_list() -> dict[str, Any]:
        """List the workflow graphs with their nodes, and the runs this server keeps."""
        return await call(engine.catalog)

    @server.tool(annotations=WRITES)
    async def workflow_start(graph: str, run_dir: str) -> dict[str, Any]:
        """Start a run of a graph in an existing directory under the runs workspace.

        Returns the run status: ready nodes, the tasks of ready agent and approval nodes,
        and the revision every later write must pass.
        """
        return await call(engine.start, graph, run_dir)

    @server.tool(annotations=READ_ONLY)
    async def workflow_status(run: str) -> dict[str, Any]:
        """Show every node of a run, what is ready, what went stale, and the current revision."""
        return await call(engine.status, run)

    @server.tool(annotations=WRITES)
    async def workflow_begin(run: str, node: str, revision: str) -> dict[str, Any]:
        """Begin a ready agent node: reserve its resources and mark it running."""
        return await call(engine.begin, run, node, revision)

    @server.tool(annotations=WRITES)
    async def workflow_record(run: str, node: str, revision: str,
                              note: Text | None = None) -> dict[str, Any]:
        """Record a running agent node as done once its outputs exist and its check passes.

        A refused record leaves the node running and names what to fix.
        """
        return await call(engine.record, run, node, revision, note)

    @server.tool(annotations=WRITES)
    async def workflow_fail(run: str, node: str, revision: str, reason: Text,
                            uncertain: bool = False) -> dict[str, Any]:
        """Mark a running agent node failed; uncertain=true when its outcome is unknown.

        An uncertain node keeps its resources until workflow_retry.
        """
        return await call(engine.fail, run, node, revision, reason, uncertain)

    @server.tool(annotations=WRITES)
    async def workflow_retry(run: str, node: str, revision: str) -> dict[str, Any]:
        """Return a failed or uncertain node to waiting, or run a waiting step again."""
        return await call(engine.retry, run, node, revision)

    @server.tool(annotations=WRITES)
    async def workflow_approve(run: str, node: str, revision: str,
                               decision: Literal["approve", "reject"],
                               reason: Text | None = None) -> dict[str, Any]:
        """Approve or reject a ready approval node; a rejection needs a reason."""
        return await call(engine.approve, run, node, revision, decision == "approve", reason)

    @server.tool(annotations=DESTRUCTIVE)
    async def workflow_cancel(run: str, revision: str, reason: Text) -> dict[str, Any]:
        """Cancel a run and release every resource it holds; a cancelled run takes no writes."""
        return await call(engine.cancel, run, revision, reason)

    return engine
