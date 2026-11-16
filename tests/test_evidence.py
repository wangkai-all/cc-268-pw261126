"""Tests for evidence expansion, multiplicity along paths, zero semantics
and referential-integrity verification."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pointwise.engine import Store, build  # noqa: E402
from pointwise.evidence import EvidenceStore  # noqa: E402

REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO, "examples"))
import pipeline as demo  # noqa: E402


class EvidenceTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = Store(os.path.join(self.tmp, "store"))
        sources = {k: os.path.join(REPO, p) for k, p in demo.SOURCE_FILES.items()}
        with self.store.lock():
            self.result = build(self.store, demo.PIPELINE, sources, note="test")
        self.ev = self.store.open_current()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)


class ExpansionTests(EvidenceTestBase):
    def test_every_terminal_row_expands_to_sources(self):
        for stage in ("region_summary", "channel_summary"):
            for row in self.ev.rows_of(stage):
                part = self.ev.source_participation(row.rid)
                self.assertTrue(part, f"{row.rid} has no source participation")
                # every participant is a source rid
                for rid in part:
                    self.assertIn(self.ev.stage_of(rid), self.ev.manifest["source_stages"])

    def test_join_fanout_multiplicity_is_multiplied_not_erased(self):
        # 华东 group joins 3 orders to the single customer C1: the customer
        # participates 3 times even though it is one physical row.
        group = next(r for r in self.ev.rows_of("region_summary")
                     if r.data["region"] == "华东")
        part = self.ev.source_participation(group.rid)
        self.assertEqual(part["customers#0"], 3)
        self.assertEqual(part["orders#0"], 1)
        self.assertEqual(part["orders#11"], 1)

    def test_paths_enumeration_respects_limit(self):
        group = self.ev.rows_of("region_summary")[0]
        small = self.ev.paths(group.rid, limit=2)
        self.assertTrue(small["truncated"])
        self.assertLessEqual(len(small["paths"]), 2)

    def test_expand_includes_rules(self):
        group = self.ev.rows_of("region_summary")[0]
        exp = self.ev.expand(group.rid)
        self.assertTrue(exp["rules"])
        self.assertIn("分组汇总", exp["rules"][0]["description"])

    def test_verify_referential_integrity_clean(self):
        report = self.ev.verify()
        self.assertTrue(report["ok"], report["issues"])
        self.assertEqual(report["drops"], 3)  # refund, duplicate, orphan join


class ZeroTests(EvidenceTestBase):
    def _group(self, region):
        return next(r for r in self.ev.rows_of("region_summary")
                    if r.data["region"] == region)

    def test_cancelled_zero_distinguished_from_all_zero(self):
        east = self._group("华东")
        explanation = self.ev.explain_zero("region_summary", east.rid, "total_amount")
        self.assertEqual(explanation["kind"], "cancelled")
        self.assertEqual(explanation["positive_sum"], 2000)
        self.assertEqual(explanation["negative_sum"], -2000)
        self.assertIn("冲销", explanation["message"])

    def test_north_includes_a_zero_value_row_but_is_not_zero(self):
        north = self._group("华北")
        explanation = self.ev.explain_zero("region_summary", north.rid, "total_amount")
        self.assertEqual(explanation["kind"], "nonzero")
        # O6 amount=0 is still evidenced as a participating row
        entries = {(e["rid"], e["value"]) for e in explanation["entries"]}
        self.assertTrue(any(v == 0 for _, v in entries))

    def test_missing_group_is_reported_as_no_data(self):
        explanation = self.ev.explain_zero(
            "region_summary", "region_summary#g000000000000", "total_amount")
        self.assertEqual(explanation["kind"], "no_group")
        self.assertIn("根本没有数据", explanation["message"])

    def test_retraction_can_turn_a_group_into_no_group(self):
        with self.store.lock():
            self.store.retract("orders#9")  # only 海外 order
            build(self.store, demo.PIPELINE,
                  {k: os.path.join(REPO, p) for k, p in demo.SOURCE_FILES.items()},
                  note="retract overseas")
        ev2 = self.store.open_current()
        regions = {r.data["region"] for r in ev2.rows_of("region_summary")}
        self.assertNotIn("海外", regions)
        # the old version still carries the group — immutable history
        self.assertIn("海外", {r.data["region"] for r in self.ev.rows_of("region_summary")})


class SizingTests(EvidenceTestBase):
    def test_stats_report_compact_vs_materialised_evidence(self):
        with open(os.path.join(self.ev.version_dir, "stats.json"), encoding="utf-8") as fh:
            import json
            stats = json.load(fh)
        e = stats["evidence"]
        self.assertGreater(e["edge_records"], 0)
        self.assertGreater(e["estimated_full_path_bytes"], e["edge_bytes"])
        self.assertIsNotNone(e["saving_ratio"])
        self.assertGreaterEqual(e["max_stage_depth"], 3)
        self.assertIn("按需展开", e["estimate_note"])

    def test_multi_way_aggregate_keeps_every_participation(self):
        # The same joined rows feed two independent aggregates. Neither path
        # may erase or halve the other's source participation.
        joined_n = len(self.ev.rows_of("joined"))
        for stage in ("region_summary", "channel_summary"):
            parts = [self.ev.source_participation(r.rid)
                     for r in self.ev.rows_of(stage)]
            order_hits = sum(v for p in parts for k, v in p.items()
                             if k.startswith("orders#"))
            customer_hits = sum(v for p in parts for k, v in p.items()
                                if k.startswith("customers#"))
            # each joined row has one order edge and one customer edge
            self.assertEqual(order_hits, joined_n)
            self.assertEqual(customer_hits, joined_n)
        # one physical source row is independently visible in both aggregates
        east = next(r for r in self.ev.rows_of("region_summary")
                    if r.data["region"] == "华东")
        online = next(r for r in self.ev.rows_of("channel_summary")
                      if r.data["channel"] == "线上")
        self.assertEqual(self.ev.source_participation(east.rid)["orders#0"], 1)
        self.assertEqual(self.ev.source_participation(online.rid)["orders#0"], 1)


if __name__ == "__main__":
    unittest.main()
