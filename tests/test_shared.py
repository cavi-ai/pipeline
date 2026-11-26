import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_run_directory_cannot_overlap_a_reserved_output(self):
        producer = self.start(level=None, value=one_node("exports/report.json"))
        producer = self.engine.begin(producer["run"], "producer", producer["revision"])
        for directory in ("exports", "exports/report.json", "exports/report.json/child"):
            with self.subTest(directory), self.assertRaisesRegex(ValueError, "held by run"):
                self.start(directory, level=None, value=one_node("other.json"))
        self.assertFalse((self.workspace / "exports").exists())
        self.engine.fail(producer["run"], "producer", producer["revision"], "Producer stopped")
        self.start("exports/report.json", level=None, value=one_node("other.json"))
        self.assertTrue((self.workspace / "exports/report.json").is_dir())

    def test_reserve_refuses_a_run_directory_created_after_path_validation(self):
        producer = self.start(level=None, value=one_node("exports/report.json"))
        second_engine = Engine({}, {}, store=self.root / "state" / "workflow.sqlite",
                               runs=self.workspace, shared=True)
        self.addCleanup(second_engine.close)
        protect = self.engine._protect

        def create_after_validation(graph, directory):
            protect(graph, directory)
            second_engine.start(None, "exports/report.json", document=one_node("other.json"))

        with (patch.object(self.engine, "_protect", side_effect=create_after_validation),
              self.assertRaisesRegex(ValueError, "overlaps the directory of run")):
            self.engine.begin(producer["run"], "producer", producer["revision"])
        status = self.engine.status(producer["run"])
        self.assertEqual(status["revision"], producer["revision"])
        self.assertNotIn("holds", row(status, "producer"))

    def test_check_and_require_defer_until_a_shared_output_is_released(self):
        document = one_node("outline.json")
        document["nodes"].append({"id": "consumer", "kind": "agent", "depends": ["producer"]})
        consumer = self.start("runs/consumer", level=None, value=document)
        producer = self.start("runs/producer", level=None, value=one_node("outline.json"))
        producer = self.engine.begin(producer["run"], "producer", producer["revision"])
        output = self.workspace / "outline.json"
        output.write_text('{"draft": true}')

        for check, node in ((self.engine.check, "producer"), (self.engine.require, "consumer")):
            with self.subTest(check=check.__name__), self.assertRaisesRegex(ValueError, "held"):
                check(consumer["run"], node)
            after = self.engine.status(consumer["run"])
            self.assertEqual(after["revision"], consumer["revision"])
            self.assertEqual(row(after, "consumer")["status"], "waiting")

        output.write_text('{"draft": false}')
        self.engine.record(producer["run"], "producer", producer["revision"])
        settled = self.engine.require(consumer["run"], "consumer")
        self.assertEqual(row(settled, "producer")["status"], "done")
        self.assertEqual(row(settled, "consumer")["status"], "ready")
        self.assertNotIn("holds", row(settled, "producer"))

    def test_settled_output_stays_reserved_while_its_dependent_step_runs(self):
        writer = self.start("runs/writer", level=None, value=one_node("outline.json"))
        document = one_node("outline.json")
        document["nodes"].append({"id": "render", "kind": "step", "depends": ["producer"],
                                  "run": "render", "outputs": {"asset": "render.json"}})

        def render(root):
            with self.assertRaisesRegex(ValueError, "held by run"):
                self.engine.begin(writer["run"], "producer", writer["revision"])
            (root / "render.json").write_text((root / "outline.json").read_text())

        self.engine.hooks["render"] = render
        consumer = self.start(level=None, value=document)
        (self.workspace / "outline.json").write_text('{"draft": false}')
        settled = self.engine.check(consumer["run"], "render")
        self.assertEqual(row(settled, "render")["status"], "done")
        self.assertEqual((self.workspace / "render.json").read_text(), '{"draft": false}')
        self.assertNotIn("holds", row(settled, "producer"))
        writer = self.engine.begin(writer["run"], "producer", writer["revision"])
        self.assertEqual(row(writer, "producer")["status"], "running")

    def test_step_revalidates_shared_paths_after_waiting_for_a_dependency(self):
        document = {"version": 1, "id": "render", "nodes": [
            {"id": "review", "kind": "approval"},
            {"id": "render", "kind": "step", "depends": ["review"], "run": "render",
             "inputs": ["brief.json"], "outputs": {"asset": "render.json"}},
        ]}

        def render(root):
            (root / "render.json").write_text('{"rendered": true}')

        self.engine.hooks["render"] = render
        started = self.start(level=None, value=document)
        brief = self.workspace / "brief.json"
        (self.workspace / "render.json").hardlink_to(brief)
        status = self.engine.approve(started["run"], "review", started["revision"], True)
        self.assertEqual(brief.read_text(), '{"seed": 1}')
        self.assertEqual(row(status, "render")["status"], "failed")
        self.assertIn("is a link to an input", row(status, "render")["reason"])

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

    def test_invalid_worker_detail_keeps_the_checkpoint_and_reservations(self):
        document = one_node("outline.json")
        document["nodes"][0].update(kind="worker", resources=["studio"])
        competitor = self.start("runs/competitor", level=None, value=one_node("outline.json"))
        (self.workspace / "outline.json").write_text("{}")
        for method, args, detail in (
            (self.engine.record, (), {"invalid": object()}),
            (self.engine.fail, ("Producer stopped",), {"oversized": "a" * 1024 * 1024}),
        ):
            with self.subTest(method=method.__name__):
                producer = self.start(f"runs/{method.__name__}", level=None, value=document)
                producer = self.engine.begin(producer["run"], "producer", producer["revision"],
                                             dispatch=True, detail={"job": "one"})
                with self.assertRaisesRegex(ValueError, "node detail"):
                    method(producer["run"], "producer", producer["revision"], *args,
                           dispatch=True, detail=detail)
                self.assertEqual(self.engine.status(producer["run"]), producer)
                with self.assertRaisesRegex(ValueError, "held by run"):
                    self.engine.begin(competitor["run"], "producer", competitor["revision"])
                finished = method(producer["run"], "producer", producer["revision"], *args,
                                  dispatch=True)
                self.assertEqual(row(finished, "producer")["detail"], {"job": "one"})
                self.assertNotIn("holds", row(finished, "producer"))

    def test_confirming_an_uncertain_step_stopped_releases_held_cancel_reservations(self):
        document = one_node("outline.json")
        document["nodes"][0].update(kind="step", run="render", resources=["studio"])

        def render(root):
            raise KeyError("missing renderer result")

        self.engine.hooks["render"] = render
        with self.assertLogs("cavi_pipeline.engine", "ERROR"):
            stopped = self.start(level=None, value=document)
        stopped = self.engine.cancel(stopped["run"], stopped["revision"], "Cancel render",
                                     hold=True)
        next_run = self.start("runs/next", level=None, value=one_node("outline.json"))
        with self.assertRaisesRegex(ValueError, "held by run"):
            self.engine.begin(next_run["run"], "producer", next_run["revision"])

        stopped = self.engine.fail(stopped["run"], "producer", stopped["revision"],
                                   "Confirmed the render stopped")
        self.assertEqual(row(stopped, "producer")["status"], "cancelled")
        self.assertNotIn("holds", row(stopped, "producer"))
        started = self.engine.begin(next_run["run"], "producer", next_run["revision"])
        self.assertEqual(row(started, "producer")["status"], "running")

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
