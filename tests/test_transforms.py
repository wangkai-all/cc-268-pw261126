"""Unit tests for the transformation primitives: multiplicity, fan-out,
dedupe, drops and aggregate semantics."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pointwise.model import Row, Relation, make_rid  # noqa: E402
from pointwise.transforms import (  # noqa: E402
    AggSpec, Dedupe, Filter, GroupAggregate, Join, Project,
)


def rel(name, rows, cols=None):
    return Relation(name, [Row(make_rid(name, i), r) for i, r in enumerate(rows)],
                    cols or list(rows[0].keys()))


class FilterTests(unittest.TestCase):
    def test_keep_and_drop_with_reasons(self):
        src = rel("orders", [{"v": 1}, {"v": 2}, {"v": 3}])
        res = Filter("f", "orders", "v", ">", 1).run({"orders": src})
        self.assertEqual([r.data["v"] for r in res.relation.rows], [2, 3])
        self.assertEqual(len(res.edges), 2)
        self.assertEqual(len(res.drops), 1)
        self.assertIn("不满足", res.drops[0].reason)
        self.assertTrue(all(d.rule for d in res.drops))

    def test_numeric_vs_string_semantics(self):
        src = rel("t", [{"v": 10}, {"v": 2}])
        res = Filter("f", "t", "v", "<", 9).run({"t": src})
        self.assertEqual([r.data["v"] for r in res.relation.rows], [2])


class ProjectTests(unittest.TestCase):
    def test_one_to_one_never_drops(self):
        src = rel("t", [{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        res = Project("p", "t", {"x": "a"}).run({"t": src})
        self.assertEqual(len(res.relation.rows), 2)
        self.assertEqual(len(res.edges), 2)
        self.assertEqual(res.relation.rows[0].data, {"x": 1})
        self.assertEqual(res.drops, [])


class DedupeTests(unittest.TestCase):
    def test_first_kept_duplicates_dropped_with_reason(self):
        src = rel("t", [{"k": "a", "v": 1}, {"k": "a", "v": 1}, {"k": "b", "v": 2}])
        res = Dedupe("d", "t", ["k", "v"]).run({"t": src})
        self.assertEqual(len(res.relation.rows), 2)
        self.assertEqual(len(res.drops), 1)
        self.assertIn("重复", res.drops[0].reason)
        self.assertEqual(res.drops[0].in_rid, "t#1")


class JoinTests(unittest.TestCase):
    def test_fan_out_preserves_every_participation(self):
        left = rel("l", [{"id": 1, "v": "L1"}, {"id": 2, "v": "L2"}])
        right = rel("r", [{"id": 1, "w": "R1"}, {"id": 1, "w": "R2"}])
        res = Join("j", "l", "r", "id", "id").run({"l": left, "r": right})
        # one-to-many: L1 appears twice, L2 unmatched under inner join
        self.assertEqual(len(res.relation.rows), 2)
        out_rid = res.relation.rows[0].rid
        pair_edges = [e for e in res.edges if e.out_rid == out_rid]
        self.assertEqual(len(pair_edges), 2)  # both sides evidenced
        # right row R1 contributes once, R2 once, left row L1 twice
        in_counts = {}
        for e in res.edges:
            in_counts[e.in_rid] = in_counts.get(e.in_rid, 0) + 1
        self.assertEqual(in_counts["l#0"], 2)
        self.assertEqual(in_counts["r#0"], 1)
        self.assertEqual(in_counts["r#1"], 1)
        self.assertTrue(any(d.in_rid == "l#1" for d in res.drops))

    def test_left_join_keeps_unmatched_with_left_lineage_only(self):
        left = rel("l", [{"id": 1}, {"id": 9}])
        right = rel("r", [{"id": 1, "w": "x"}])
        res = Join("j", "l", "r", "id", "id", how="left").run({"l": left, "r": right})
        self.assertEqual(len(res.relation.rows), 2)
        orphan = [r for r in res.relation.rows if r.data["id"] == 9][0]
        edges = [e for e in res.edges if e.out_rid == orphan.rid]
        self.assertEqual([e.in_rid for e in edges], ["l#1"])


class GroupTests(unittest.TestCase):
    def test_aggregates_and_edge_multiplicity_not_collapsed(self):
        src = rel("t", [
            {"g": "a", "amt": 10}, {"g": "a", "amt": -10}, {"g": "a", "amt": 0},
            {"g": "b", "amt": 5},
        ])
        agg = GroupAggregate("s", "t", ["g"], [
            AggSpec("total", "sum", "amt"),
            AggSpec("n", "count"),
            AggSpec("distinct", "count_distinct", "amt"),
        ])
        res = agg.run({"t": src})
        groups = {r.data["g"]: r.data for r in res.relation.rows}
        self.assertEqual(groups["a"]["total"], 0)
        self.assertEqual(groups["a"]["n"], 3)
        self.assertEqual(groups["b"]["distinct"], 1)
        a_rid = next(r.rid for r in res.relation.rows if r.data["g"] == "a")
        edge_ins = {e.in_rid: e.n for e in res.edges if e.out_rid == a_rid}
        self.assertEqual(sum(edge_ins.values()), 3)  # multiplicity preserved

    def test_duplicate_input_rid_counts_with_n(self):
        # same rid can reach a group twice through different paths
        src = Relation("t", [Row("x#0", {"g": "a", "amt": 7}),
                             Row("x#0", {"g": "a", "amt": 7})], ["g", "amt"])
        agg = GroupAggregate("s", "t", ["g"], [AggSpec("total", "sum", "amt")])
        res = agg.run({"t": src})
        edge = res.edges[0]
        self.assertEqual(edge.in_rid, "x#0")
        self.assertEqual(edge.n, 2)  # not collapsed to n=1

    def test_empty_input_has_no_groups_not_zero_group(self):
        src = Relation("t", [], ["g", "amt"])
        agg = GroupAggregate("s", "t", ["g"], [AggSpec("total", "sum", "amt")])
        res = agg.run({"t": src})
        self.assertEqual(res.relation.rows, [])


if __name__ == "__main__":
    unittest.main()
