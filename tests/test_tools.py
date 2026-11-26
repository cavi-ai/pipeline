import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from cavi_mcp_core.server import Server
from mcp import Client

from cavi_pipeline import Engine, mount, register

FIXTURES = Path(__file__).parent / "fixtures" / "graphs"


def noop(run_dir):
    return None


async def session(server, calls):
    results = []
    async with Client(server) as client:
        listed = await client.list_tools()
        results.append(listed.tools)
        for name, arguments in calls:
            results.append(await client.call_tool(name, arguments))
    return results


def view(result):
    return json.loads(result.content[0].text)


class ToolTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "runs" / "one").mkdir(parents=True)
        (root / "runs" / "one" / "source.md").write_text("Line one.\n")
        self.server = Server("demo", version="1")
        self.engine = mount(self.server, graphs=FIXTURES,
                            hooks={name: noop for name in ("cites_sources", "render", "assemble",
                                                           "publish")},
                            store=root / "pipeline.sqlite", runs=root / "runs")
        self.addCleanup(self.engine.close)

    def test_tools_drive_a_run_over_mcp(self):
        tools, listed, started = asyncio.run(session(self.server, [
            ("workflow_list", {}), ("workflow_start", {"graph": "cut", "run_dir": "one"})]))
        names = {tool.name: tool for tool in tools}
        self.assertEqual(set(names), {
            "workflow_list", "workflow_start", "workflow_status", "workflow_begin",
            "workflow_record", "workflow_fail", "workflow_retry", "workflow_approve",
            "workflow_escalate", "workflow_cancel"})
        self.assertTrue(names["workflow_status"].annotations.read_only_hint)
        self.assertTrue(names["workflow_cancel"].annotations.destructive_hint)
        self.assertEqual(view(listed)["graphs"][0]["id"], "cut")
        run = view(started)
        self.assertEqual(run["ready"], ["research", "score"])

        _, begun, refused, unknown = asyncio.run(session(self.server, [
            ("workflow_begin", {"run": run["run"], "node": "research",
                                "revision": run["revision"]}),
            ("workflow_record", {"run": run["run"], "node": "research",
                                 "revision": run["revision"]}),
            ("workflow_begin", {"run": run["run"], "node": "research", "revision": "x",
                                "extra": 1}),
        ]))
        self.assertFalse(begun.is_error)
        self.assertEqual(view(begun)["nodes"][0]["status"], "running")
        self.assertTrue(refused.is_error)
        self.assertIn("not the current revision", refused.content[0].text)
        self.assertTrue(unknown.is_error)

    def test_register_asks_for_the_engine_on_each_call(self):
        server = Server("later", version="1")

        def engine():
            raise ValueError("Configure the runs workspace first")

        register(server, engine)
        tools, refused = asyncio.run(session(server, [("workflow_list", {})]))
        self.assertIn("workflow_status", {tool.name for tool in tools})
        self.assertTrue(refused.is_error)
        self.assertIn("Configure the runs workspace first", refused.content[0].text)

    def test_cancel_holds_a_dispatched_worker_until_server_confirms_it_stopped(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        runs = root / "runs"
        runs.mkdir()
        engine = Engine({}, {}, store=root / "state" / "pipeline.sqlite", runs=runs, shared=True)
        self.addCleanup(engine.close)
        server = Server("dispatch", version="1")
        register(server, lambda: engine)
        document = {"version": 1, "id": "render", "nodes": [
            {"id": "render", "kind": "worker", "outputs": {"asset": "render.json"}}]}
        active = engine.start(None, "first", document=document)
        waiting = engine.start(None, "second", document=document)
        active = engine.begin(active["run"], "render", active["revision"], dispatch=True)

        _, cancelled = asyncio.run(session(server, [("workflow_cancel", {
            "run": active["run"], "revision": active["revision"], "reason": "Brief withdrawn",
        })]))
        self.assertFalse(cancelled.is_error)
        cancelled = view(cancelled)
        self.assertTrue(cancelled["cancelled"])
        self.assertEqual(cancelled["nodes"][0]["status"], "uncertain")
        self.assertEqual(cancelled["nodes"][0]["holds"], ["output:render.json"])
        with self.assertRaisesRegex(ValueError, "held by run"):
            engine.begin(waiting["run"], "render", waiting["revision"], dispatch=True)

        engine.fail(active["run"], "render", cancelled["revision"], "Worker exited", dispatch=True)
        waiting = engine.begin(waiting["run"], "render", waiting["revision"], dispatch=True)
        self.assertEqual(waiting["nodes"][0]["status"], "running")


if __name__ == "__main__":
    unittest.main()
