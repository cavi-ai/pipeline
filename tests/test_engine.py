import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path
from threading import Event

from cavi_pipeline import Engine, load_directory

FIXTURES = Path(__file__).parent / "fixtures" / "graphs"
CHAIN = ("research", "outline", "script", "picture", "layout")


class Harness:
    """A real engine on a temporary store, with hooks that write real files."""

    def __init__(self, root):
        self.root = Path(root)
        self.runs = self.root / "runs"
        self.runs.mkdir()
        self.store = self.root / "state" / "pipeline.sqlite"
        self.graphs = load_directory(FIXTURES)
        self.raises = {}
        self.engine = self.open()

    def open(self):
        return Engine(self.graphs, {
            "cites_sources": self.cites_sources, "render": self.render,
            "assemble": self.assemble, "publish": self.publish,
        }, store=self.store, runs=self.runs)

    def reopen(self):
        self.engine.close()
        self.engine = self.open()

    def cites_sources(self, run_dir):
        if not json.loads((run_dir / "research.json").read_text()).get("claims"):
            raise ValueError("research.json needs at least one claim")

    def render(self, run_dir):
        if "render" in self.raises:
            raise self.raises["render"]
        (run_dir / "frames.bin").write_bytes(b"frames:" + (run_dir / "layout.json").read_bytes())

    def assemble(self, run_dir):
        picture = (run_dir / "frames.bin").read_bytes()
        (run_dir / "final.bin").write_bytes(picture + (run_dir / "score.wav").read_bytes())

    def publish(self, run_dir):
        (run_dir / "published.json").write_text('{"published": true}')

    def start(self, name="one"):
        directory = self.runs / name
        directory.mkdir(exist_ok=True)
        (directory / "source.md").write_text("Line one.\n")
        view = self.engine.start("cut", name)
        return view["run"], view

    def output(self, run, node_id):
        relative = self.graphs["cut"].nodes[node_id].output_paths[0]
        return Path(self.engine.status(run)["run_dir"]) / relative

    def write(self, run, node_id, content=None):
        default = '{"claims": ["Line one."]}' if node_id == "research" else f'"{node_id}"'
        self.output(run, node_id).write_text(content or default)

    def finish(self, run, node_id, content=None):
        view = self.engine.begin(run, node_id, self.engine.status(run)["revision"])
        self.write(run, node_id, content)
        return self.engine.record(run, node_id, view["revision"])

    def approve(self, run, approved=True, reason=None):
        return self.engine.approve(run, "review", self.engine.status(run)["revision"], approved,
                                   reason)


def statuses(view):
    return {row["id"]: row["status"] for row in view["nodes"]}


def row(view, node_id):
    return next(item for item in view["nodes"] if item["id"] == node_id)


class EngineTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.h = Harness(directory.name)
        self.addCleanup(lambda: self.h.engine.close())

    def test_start_offers_the_nodes_without_dependencies(self):
        run, view = self.h.start()
        self.assertEqual(view["ready"], ["research", "score"])
        self.assertEqual([task["id"] for task in view["tasks"]], ["research", "score"])
        self.assertEqual(statuses(view)["render"], "waiting")
        self.assertFalse(view["complete"])
        self.assertEqual(self.h.engine.status(run)["revision"], view["revision"])

    def test_writes_need_the_current_revision(self):
        run, first = self.h.start()
        self.h.engine.begin(run, "score", first["revision"])
        with self.assertRaisesRegex(ValueError, "not the current revision"):
            self.h.engine.begin(run, "research", first["revision"])
        self.assertEqual(statuses(self.h.engine.status(run))["research"], "ready")

    def test_status_waits_for_an_in_progress_write_to_the_same_run(self):
        run, _ = self.h.start()
        for node_id in CHAIN[:-1]:
            self.h.finish(run, node_id)
        view = self.h.engine.begin(run, "layout", self.h.engine.status(run)["revision"])
        self.h.write(run, "layout")

        rendering, finish_render, reading = Event(), Event(), Event()

        def render(run_dir):
            rendering.set()
            if not finish_render.wait(timeout=10):
                raise RuntimeError("Timed out waiting to finish the test render")
            self.h.render(run_dir)

        reader = self.h.open()
        self.addCleanup(reader.close)
        self.h.engine.hooks["render"] = render

        def status():
            reading.set()
            return reader.status(run)

        with ThreadPoolExecutor(max_workers=2) as pool:
            write = pool.submit(self.h.engine.record, run, "layout", view["revision"])
            try:
                self.assertTrue(rendering.wait(timeout=5))
                read = pool.submit(status)
                self.assertTrue(reading.wait(timeout=5))
                with self.assertRaises(TimeoutError):
                    read.result(timeout=0.25)
            finally:
                finish_render.set()
            written, observed = write.result(timeout=5), read.result(timeout=5)

        self.assertEqual(observed["revision"], written["revision"])
        self.assertEqual(row(observed, "render")["status"], "done")
        self.assertNotIn("holds", row(observed, "render"))

    def test_record_needs_the_outputs_and_a_passing_check(self):
        run, view = self.h.start()
        view = self.h.engine.begin(run, "research", view["revision"])
        with self.assertRaisesRegex(ValueError, "output research.json does not exist"):
            self.h.engine.record(run, "research", view["revision"])
        self.h.write(run, "research", '{"claims": []}')
        with self.assertRaisesRegex(ValueError, "needs at least one claim"):
            self.h.engine.record(run, "research", view["revision"])
        unchanged = self.h.engine.status(run)
        self.assertEqual(unchanged["revision"], view["revision"])
        self.assertEqual(statuses(unchanged)["research"], "running")
        self.h.write(run, "research")
        done = self.h.engine.record(run, "research", view["revision"])
        self.assertEqual(statuses(done)["research"], "done")
        self.assertEqual(done["ready"], ["outline", "score"])

    def test_begin_refuses_a_node_that_is_not_ready(self):
        run, view = self.h.start()
        with self.assertRaisesRegex(ValueError, "outline waits on research"):
            self.h.engine.begin(run, "outline", view["revision"])
        with self.assertRaisesRegex(ValueError, "only an agent node begins"):
            self.h.engine.begin(run, "render", view["revision"])

    def test_both_chains_meet_at_assembly_and_finish_after_approval(self):
        run, view = self.h.start()
        self.h.engine.begin(run, "score", view["revision"])
        for node_id in CHAIN:
            view = self.h.finish(run, node_id)
        render = row(view, "render")
        self.assertEqual(render["status"], "ready")
        self.assertEqual(render["blocked"], f"gpu is held by run {run} node score")
        self.assertEqual(row(view, "score")["holds"], ["gpu"])

        self.h.write(run, "score", "tone")
        view = self.h.engine.record(run, "score", view["revision"])
        self.assertEqual(statuses(view)["render"], "done")
        self.assertEqual(statuses(view)["assemble"], "done")
        self.assertEqual(view["ready"], ["review"])
        self.assertEqual(self.h.output(run, "assemble").read_bytes(), b'frames:"layout"tone')

        with self.assertRaisesRegex(ValueError, "needs a reason"):
            self.h.approve(run, approved=False)
        view = self.h.approve(run, approved=False, reason="Shorter opening")
        self.assertEqual(row(view, "review")["status"], "failed")
        self.assertEqual(row(view, "review")["reason"], "Shorter opening")
        view = self.h.engine.retry(run, "review", view["revision"])
        self.assertEqual(view["ready"], ["review"])
        view = self.h.approve(run)
        self.assertTrue(view["complete"])
        self.assertTrue(self.h.output(run, "publish").is_file())
        self.assertEqual(row(view, "review")["attempt"], 2)

    def complete(self, run):
        for node_id in (*CHAIN, "score"):
            self.h.finish(run, node_id)
        return self.h.approve(run)

    def test_a_changed_output_makes_it_and_its_descendants_stale(self):
        run, _ = self.h.start()
        self.assertTrue(self.complete(run)["complete"])
        score = self.h.output(run, "score")
        os.utime(score, ns=(score.stat().st_atime_ns, score.stat().st_mtime_ns + 10**9))
        self.assertEqual(self.h.engine.status(run)["stale"], [])

        self.h.write(run, "research", '{"claims": ["Line one.", "Line two."]}')
        view = self.h.engine.status(run)
        self.assertEqual(view["stale"], sorted({*CHAIN, "render", "assemble", "review", "publish"}))
        self.assertEqual(row(view, "research")["reason"], "research.json changed")
        self.assertEqual(statuses(view)["score"], "done")
        self.assertEqual(view["ready"], ["research"])
        self.assertFalse(view["complete"])

        for node_id in CHAIN:
            view = self.h.finish(run, node_id)
        self.assertEqual(row(view, "render")["attempt"], 2)
        self.assertEqual(view["ready"], ["review"])
        self.assertTrue(self.h.approve(run)["complete"])

    def test_a_dependent_waits_until_its_stale_dependency_is_recorded_again(self):
        run, _ = self.h.start()
        self.h.finish(run, "research")
        self.h.finish(run, "outline")
        self.h.write(run, "research", '{"claims": ["Line two."]}')
        view = self.h.finish(run, "research")
        self.assertEqual(statuses(view)["outline"], "ready")
        self.assertEqual(row(view, "outline")["reason"], "stale: research is no longer done")

    def test_an_uncertain_node_keeps_its_resources_until_retried(self):
        first, view = self.h.start("one")
        view = self.h.engine.begin(first, "score", view["revision"])
        view = self.h.engine.fail(first, "score", view["revision"], "Render host stopped answering",
                                  uncertain=True)
        self.assertEqual(row(view, "score")["status"], "uncertain")
        self.assertEqual(row(view, "score")["holds"], ["gpu"])

        second, other = self.h.start("two")
        with self.assertRaisesRegex(ValueError, f"gpu is held by run {first} node score"):
            self.h.engine.begin(second, "score", other["revision"])
        view = self.h.engine.retry(first, "score", view["revision"])
        self.assertNotIn("holds", row(view, "score"))
        other = self.h.engine.begin(second, "score", other["revision"])
        self.assertEqual(row(other, "score")["holds"], ["gpu"])

    def test_a_known_failure_releases_its_resources(self):
        run, view = self.h.start()
        view = self.h.engine.begin(run, "score", view["revision"])
        view = self.h.engine.fail(run, "score", view["revision"], "No tempo in the brief")
        self.assertEqual(row(view, "score")["status"], "failed")
        self.assertNotIn("holds", row(view, "score"))

    def test_step_failures_and_retries(self):
        run, _ = self.h.start()
        self.h.raises["render"] = ValueError("layout has no frames")
        for node_id in CHAIN:
            view = self.h.finish(run, node_id)
        self.assertEqual(row(view, "render")["status"], "failed")
        self.assertEqual(row(view, "render")["reason"], "layout has no frames")
        self.assertNotIn("holds", row(view, "render"))

        self.h.raises["render"] = KeyError("frame")
        with self.assertLogs("cavi_pipeline.engine", "ERROR"):
            view = self.h.engine.retry(run, "render", view["revision"])
        self.assertEqual(row(view, "render")["status"], "uncertain")
        self.assertEqual(row(view, "render")["holds"], ["gpu"])

        del self.h.raises["render"]
        view = self.h.engine.retry(run, "render", view["revision"])
        self.assertEqual(row(view, "render")["status"], "done")
        self.assertEqual(row(view, "render")["attempt"], 3)
        self.assertEqual(statuses(view)["assemble"], "waiting")

    def test_a_new_engine_on_the_same_store_resumes_the_run(self):
        run, _ = self.h.start()
        before = self.h.finish(run, "research")
        self.h.reopen()
        after = self.h.engine.status(run)
        self.assertEqual(after["revision"], before["revision"])
        self.assertEqual(statuses(after)["research"], "done")
        self.assertEqual(statuses(self.h.finish(run, "outline"))["outline"], "done")

    def test_cancel_releases_resources_and_ends_the_run(self):
        run, view = self.h.start()
        with self.assertRaisesRegex(ValueError, f"Run {run} is already live"):
            self.h.engine.start("cut", "one")
        view = self.h.engine.begin(run, "score", view["revision"])
        view = self.h.engine.cancel(run, view["revision"], "Brief withdrawn")
        self.assertTrue(view["cancelled"])
        self.assertEqual(row(view, "score")["status"], "cancelled")
        self.assertNotIn("holds", row(view, "score"))
        with self.assertRaisesRegex(ValueError, "is cancelled"):
            self.h.engine.begin(run, "research", view["revision"])
        self.assertNotEqual(self.h.engine.start("cut", "one")["run"], run)

    def test_refuses_run_directories_outside_the_runs_workspace(self):
        for value in ("../outside", "/etc", "missing"):
            with self.subTest(value), self.assertRaises(ValueError):
                self.h.engine.start("cut", value)

    def test_refuses_graphs_whose_hooks_were_not_given(self):
        with self.assertRaisesRegex(ValueError, "not given: assemble, publish, render"):
            Engine(self.h.graphs, {"cites_sources": print}, store=self.h.root / "other.sqlite",
                   runs=self.h.runs)


if __name__ == "__main__":
    unittest.main()
