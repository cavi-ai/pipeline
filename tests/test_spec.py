import copy
import json
import tempfile
import unittest
from pathlib import Path

from cavi_pipeline import spec

FIXTURES = Path(__file__).parent / "fixtures" / "graphs"
CUT = json.loads((FIXTURES / "cut.json").read_text())


def variant(change):
    value = copy.deepcopy(CUT)
    change(value)
    return value


def node(value, node_id):
    return next(item for item in value["nodes"] if item["id"] == node_id)


class SpecTests(unittest.TestCase):
    def test_loads_the_cut_graph_in_dependency_order(self):
        graph = spec.load(FIXTURES / "cut.json")
        order = graph.order()
        self.assertEqual(len(order), 10)
        for node_id, item in graph.nodes.items():
            for name in item.depends + item.after:
                self.assertLess(order.index(name), order.index(node_id))
        self.assertEqual(graph.hooks(), {"cites_sources", "render", "assemble", "publish"})
        self.assertEqual(graph.descendants("score"), {"assemble", "review", "publish"})

    def test_refuses_invalid_graphs(self):
        cases = {
            "cycle through": lambda v: node(v, "research").update(depends=["publish"]),
            "which is not another node": lambda v: node(v, "outline").update(depends=["missing"]),
            "is an output of both": lambda v: node(v, "outline").update(
                outputs={"notes": "research.json"}),
            "needs a run hook": lambda v: node(v, "render").pop("run"),
            "only a step has a run hook": lambda v: node(v, "outline").update(run="render"),
            "has no outputs or check": lambda v: node(v, "review").update(
                outputs={"x": "x.json"}),
            "unknown fields: retries": lambda v: node(v, "outline").update(retries=3),
            "without ..": lambda v: node(v, "outline").update(outputs={"o": "../outline.json"}),
            "depend on that node instead": lambda v: node(v, "outline").update(
                inputs=["research.json"]),
            "repeats node": lambda v: v["nodes"].append(copy.deepcopy(node(v, "outline"))),
            "kind must be one of": lambda v: node(v, "outline").update(kind="robot"),
            "version must be 1": lambda v: v.update(version=2),
            "has a level but the graph has no levels": lambda v: node(v, "outline").update(
                level="full"),
            "level must be one of the graph levels": lambda v: (
                v.update(levels=["draft", "full"]), node(v, "outline").update(level="final")),
            "levels must list 2 to 8 names": lambda v: v.update(levels=["only"]),
        }
        for message, change in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                spec.parse(variant(change))

    def test_levels_default_to_the_lowest_and_name_the_nodes_above(self):
        def change(value):
            value["levels"] = ["draft", "preview", "full"]
            node(value, "publish").update(level="full")
            node(value, "review").update(level="preview")

        graph = spec.parse(variant(change))
        self.assertEqual(graph.nodes["research"].level, "draft")
        self.assertEqual(graph.above("draft"), {"review", "publish"})
        self.assertEqual(graph.above("preview"), {"publish"})
        self.assertEqual(graph.above("full"), set())

    def test_order_rejects_unplaceable_programmatic_graphs(self):
        cases = (
            ({"first": spec.Node("first", "agent", depends=("second",)),
              "second": spec.Node("second", "agent", after=("first",))}, "cycle through"),
            ({"first": spec.Node("first", "agent", depends=("missing",))}, "missing"),
        )
        for nodes, message in cases:
            with self.subTest(nodes=nodes), self.assertRaisesRegex(ValueError, message):
                spec.Graph("invalid", nodes).order()

    def test_refuses_more_than_the_node_limit(self):
        value = {"version": 1, "id": "wide",
                 "nodes": [{"id": f"n{index}", "kind": "agent"} for index in range(33)]}
        with self.assertRaisesRegex(ValueError, "1 to 32 nodes"):
            spec.parse(value)

    def test_graph_file_name_must_match_its_id(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "other.json").write_text(json.dumps(CUT))
            with self.assertRaisesRegex(ValueError, "name the file cut.json"):
                spec.load_directory(directory)


if __name__ == "__main__":
    unittest.main()
