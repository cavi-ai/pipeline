import json
import os
import tempfile
import unittest
from pathlib import Path

from cavi_pipeline import Engine, load_directory

FIXTURES = Path(__file__).parent / "fixtures" / "graphs"
OUTPUTS = {"outline": "outline.json", "palette": "palette.json", "compose": "compose.json",
           "audit": "audit.json"}


def level_graph():
    return {"version": 1, "id": "staged", "levels": ["draft", "standard", "final"], "nodes": [
        {"id": "outline", "kind": "agent", "inputs": ["brief.json"],
         "outputs": {"outline": "outline.json"}, "resources": ["studio"]},
        {"id": "palette", "kind": "agent", "inputs": ["brief.json"],
         "outputs": {"palette": "palette.json"}},
        {"id": "compose", "kind": "agent", "depends": ["outline", "palette"],
         "outputs": {"compose": "compose.json"}, "level": "standard"},
        {"id": "audit", "kind": "worker", "depends": ["compose"],
         "outputs": {"audit": "audit.json"}, "level": "final"}]}


def one_node(output, inputs=("brief.json",)):
    return {"version": 1, "id": "export", "nodes": [
        {"id": "producer", "kind": "agent", "inputs": list(inputs),
         "outputs": {"asset": output}}]}


def row(view, node_id):
    return next(item for item in view["nodes"] if item["id"] == node_id)


class SharedWorkspaceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        (self.workspace / "brief.json").write_text('{"seed": 1}')
        self.engine = Engine({}, {}, store=self.root / "state" / "workflow.sqlite",
                             runs=self.workspace, shared=True)
        self.addCleanup(self.engine.close)

    def start(self, run_dir="runs/one", level="standard", value=None):
        return self.engine.start(None, run_dir, level, document=value or level_graph())

    def finish(self, view, node_id):
        dispatch = {"dispatch": True} if node_id == "audit" else {}
        view = self.engine.begin(view["run"], node_id, view["revision"], **dispatch)
        (self.workspace / OUTPUTS[node_id]).write_text(json.dumps({"node": node_id}))
        return self.engine.record(view["run"], node_id, view["revision"], **dispatch)

    def test_a_run_uses_workspace_paths_and_keeps_its_graph_read_only(self):
        view = self.start()
        directory = self.workspace / "runs" / "one"
        copy = directory / "graph.json"
        self.assertEqual(view["run_dir"], str(directory))
        self.assertEqual(json.loads(copy.read_text())["id"], "staged")
        self.assertFalse(copy.stat().st_mode & 0o222)
        view = self.finish(view, "outline")
        self.assertEqual(row(view, "outline")["status"], "done")
        evidence = self.engine.evidence(view["run"])
        self.assertIn("outline.json", evidence["nodes"]["outline"]["files"])
        self.assertIn("brief.json", evidence["sealed"])

        os.chmod(copy, 0o644)
        copy.write_text(json.dumps(one_node("other.json")))
        with self.assertRaisesRegex(ValueError, "changed since run"):
            self.engine.status(view["run"])

    def test_inputs_are_sealed_when_the_run_starts(self):
        view = self.start()
        (self.workspace / "brief.json").write_text('{"seed": 2}')
        status = self.engine.status(view["run"])
        self.assertEqual(row(status, "outline")["status"], "stale")
        self.assertIn("brief.json changed since the run started", row(status, "outline")["reason"])
        self.assertEqual(status["ready"], [])
        self.assertEqual(status["revision"], view["revision"])
        with self.assertRaisesRegex(ValueError, "not ready"):
            self.engine.begin(view["run"], "outline", status["revision"])

        second = self.start("runs/two")
        second = self.engine.begin(second["run"], "palette", second["revision"])
        (self.workspace / "brief.json").write_text('{"seed": 3}')
        (self.workspace / "palette.json").write_text("{}")
        status = self.engine.status(second["run"])
        self.assertEqual(row(status, "palette")["status"], "running")
        self.assertIn("changed since the run started", row(status, "palette")["changed"])
        with self.assertRaisesRegex(ValueError, "cannot record: input brief.json changed"):
            self.engine.record(second["run"], "palette", status["revision"])

    def test_output_paths_inside_or_around_a_held_one_wait_across_runs(self):
        first = self.start("runs/one", level=None, value=one_node("exports/report.pdf"))
        second = self.start("runs/two", level=None, value=one_node("EXPORTS"))
        first = self.engine.begin(first["run"], "producer", first["revision"])
        status = self.engine.status(second["run"])
        self.assertEqual(row(status, "producer")["status"], "ready")
        self.assertIn("overlaps exports/report.pdf", row(status, "producer")["blocked"])
        with self.assertRaisesRegex(ValueError, "overlaps exports/report.pdf"):
            self.engine.begin(second["run"], "producer", status["revision"])

        self.engine.fail(first["run"], "producer", first["revision"], "Exited without output")
        second = self.engine.begin(second["run"], "producer", status["revision"])
        self.assertEqual(row(second, "producer")["status"], "running")
        self.assertEqual(row(second, "producer")["holds"], ["output:exports"])

    def test_start_refuses_paths_a_shared_run_must_not_touch(self):
        self.start()
        (self.workspace / "alias.json").hardlink_to(self.workspace / "brief.json")
        cases = {
            "overlaps the directory of run": ("runs/new", one_node("runs/one/outline.json")),
            "is a link to an input": ("runs/alias", one_node("alias.json")),
            "input missing.json does not exist": ("runs/missing",
                                                  one_node("x.json", ["missing.json"])),
            "exists; a shared-workspace run starts in a new directory": ("runs/one",
                                                                         one_node("y.json")),
        }
        for message, (run_dir, value) in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.start(run_dir, level=None, value=value)
        with self.assertRaisesRegex(ValueError, "overlaps the directory of run"):
            self.start("runs/one/nested", level=None, value=one_node("z.json"))
        self.assertEqual(sorted(path.name for path in (self.workspace / "runs").iterdir()),
                         ["one"])

    def test_levels_defer_nodes_until_the_run_is_escalated(self):
        view = self.start(level="draft")
        self.assertEqual(view["level"], "draft")
        view = self.finish(self.finish(view, "outline"), "palette")
        self.assertEqual(row(view, "compose")["status"], "deferred")
        self.assertEqual(view["ready"], [])
        self.assertTrue(view["complete"])

        view = self.engine.escalate(view["run"], "standard", view["revision"])
        self.assertEqual(view["ready"], ["compose"])
        self.assertFalse(view["complete"])
        self.assertEqual(row(view, "audit")["status"], "deferred")
        with self.assertRaisesRegex(ValueError, "keeps or raises the level"):
            self.engine.escalate(view["run"], "draft", view["revision"])
        same = self.engine.escalate(view["run"], "standard", view["revision"])
        self.assertEqual(same["revision"], view["revision"])

    def test_worker_nodes_take_writes_only_from_server_code(self):
        view = self.start(level="final")
        for node_id in ("outline", "palette", "compose"):
            view = self.finish(view, node_id)
        self.assertEqual(view["ready"], ["audit"])
        with self.assertRaisesRegex(ValueError, "server's own tools dispatch it"):
            self.engine.begin(view["run"], "audit", view["revision"])
        view = self.engine.begin(view["run"], "audit", view["revision"], dispatch=True,
                                 detail={"phase": "sealed"})
        view = self.engine.annotate(view["run"], "audit", view["revision"],
                                    {"phase": "dispatched"})
        self.assertEqual(row(view, "audit")["detail"], {"phase": "dispatched"})
        with self.assertRaisesRegex(ValueError, "server's own tools dispatch it"):
            self.engine.record(view["run"], "audit", view["revision"])

        (self.workspace / "audit.json").write_text("{}")
        view = self.engine.record(view["run"], "audit", view["revision"], dispatch=True)
        self.assertEqual(row(view, "audit")["status"], "done")
        self.assertEqual(row(view, "audit")["detail"], {"phase": "dispatched"})
        self.assertTrue(view["complete"])

    def test_a_known_failure_resolves_an_uncertain_node(self):
        view = self.start()
        view = self.engine.begin(view["run"], "outline", view["revision"])
        view = self.engine.fail(view["run"], "outline", view["revision"], "Connection lost",
                                uncertain=True)
        self.assertEqual(row(view, "outline")["holds"], ["output:outline.json", "studio"])
        view = self.engine.fail(view["run"], "outline", view["revision"], "Producer wrote nothing")
        self.assertEqual(row(view, "outline")["status"], "failed")
        self.assertNotIn("holds", row(view, "outline"))
        view = self.engine.retry(view["run"], "outline", view["revision"])
        self.assertIn("outline", view["ready"])

    def test_a_held_cancel_keeps_unresolved_work_until_its_outcome_is_known(self):
        first, second = self.start("runs/one"), self.start("runs/two")
        first = self.engine.begin(first["run"], "outline", first["revision"])
        first = self.engine.cancel(first["run"], first["revision"], "Brief withdrawn", hold=True)
        self.assertEqual(row(first, "outline")["status"], "uncertain")
        self.assertEqual(row(first, "palette")["status"], "cancelled")
        with self.assertRaisesRegex(ValueError, "studio is held by run"):
            self.engine.begin(second["run"], "outline", second["revision"])
        with self.assertRaisesRegex(ValueError, "is cancelled"):
            self.engine.retry(first["run"], "outline", first["revision"])

        first = self.engine.fail(first["run"], "outline", first["revision"], "Producer stopped")
        self.assertEqual(row(first, "outline")["status"], "cancelled")
        self.assertNotIn("holds", row(first, "outline"))
        second = self.engine.begin(second["run"], "outline", second["revision"])
        self.assertEqual(row(second, "outline")["status"], "running")

    def test_a_graph_document_needs_a_shared_workspace(self):
        runs = self.root / "runs"
        (runs / "one").mkdir(parents=True)
        engine = Engine(load_directory(FIXTURES), {name: print for name in (
            "cites_sources", "render", "assemble", "publish")},
            store=self.root / "other.sqlite", runs=runs)
        self.addCleanup(engine.close)
        with self.assertRaisesRegex(ValueError, "needs an engine with a shared workspace"):
            engine.start(None, "one", document=level_graph())


if __name__ == "__main__":
    unittest.main()
