import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from cavi_mcp_core.server import Server
from mcp import Client

from cavi_pipeline import mount

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
            "workflow_cancel"})
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


if __name__ == "__main__":
    unittest.main()
