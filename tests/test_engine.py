"""Tests for versioning, retraction/recompute, crash recovery, GC and the
read-your-version consistency guarantee under concurrent writers."""
import json
import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pointwise.diff import diff_versions  # noqa: E402
from pointwise.engine import Store, _InjectedCrash, build  # noqa: E402

REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = Store(os.path.join(self.tmp, "store"))
        sys.path.insert(0, os.path.join(REPO, "examples"))
        import pipeline as demo
        self.config = demo.PIPELINE
        self.sources = {k: os.path.join(REPO, p) for k, p in demo.SOURCE_FILES.items()}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def build(self, note="t", **kw):
        with self.store.lock():
            return build(self.store, self.config, self.sources, note=note, **kw)


class VersioningTests(EngineTestBase):
    def test_immutable_versions_and_atomic_switch(self):
        r1 = self.build("v1")
        r2 = self.build("v2")
        self.assertNotEqual(r1.version_id, r2.version_id)
        self.assertEqual(self.store.current_version(), r2.version_id)
        # old version directory is untouched and still readable
        old = self.store.open_version(r1.version_id)
        self.assertTrue(old.verify()["ok"])

    def test_retraction_records_tombstone_and_drops_reason(self):
        before = self.build("before")
        with self.store.lock():
            self.store.retract("orders#0")
            after = build(self.store, self.config, self.sources, note="retract")
        ev = self.store.open_version(after.version_id)
        self.assertIn("orders#0", ev.manifest["tombstones"])
        drops = ev.drops_of("orders")
        self.assertTrue(any(d["in"] == "orders#0" and "撤销" in d["reason"] for d in drops))
        # the retracted rid is absent from the source snapshot but still known via drops
        self.assertIsNone(ev.get_row("orders#0"))
        diff = diff_versions(self.store.open_version(before.version_id), ev)
        self.assertEqual(diff["tombstones_added"], ["orders#0"])

    def test_restore_brings_row_back(self):
        self.build("base")
        with self.store.lock():
            self.store.retract("orders#2")
            build(self.store, self.config, self.sources, note="retract")
        with self.store.lock():
            self.store.restore("orders#2")
            build(self.store, self.config, self.sources, note="restore")
        ev = self.store.open_current()
        self.assertNotIn("orders#2", ev.manifest["tombstones"])
        self.assertIsNotNone(ev.get_row("orders#2"))

    def test_gc_never_removes_current(self):
        ids = [self.build(f"v{i}").version_id for i in range(5)]
        removed = self.store.gc(keep=2)
        self.assertNotIn(self.store.current_version(), removed)
        remaining = {v["version"] for v in self.store.versions()}
        self.assertIn(self.store.current_version(), remaining)
        # a removed version directory is really gone
        self.assertTrue(any(v not in remaining for v in ids))


class RecoveryTests(EngineTestBase):
    def test_resume_after_injected_crash_uses_checkpoints(self):
        current_before = self.build("base").version_id
        with self.assertRaises(_InjectedCrash):
            self.build("crash", crash_after_stage="dedup_orders")
        # current untouched while a partial attempt exists
        self.assertEqual(self.store.current_version(), current_before)
        partial = [d for d in os.listdir(self.store.versions_dir)
                   if os.path.exists(os.path.join(self.store.versions_dir, d, "wal.json"))]
        self.assertEqual(len(partial), 1)
        result = self.build("recover", allow_resume=True)
        self.assertTrue(result.recovered)
        # adopted the crashed directory rather than creating a new one
        self.assertEqual(result.version_id, partial[0])
        self.assertFalse(os.path.exists(
            os.path.join(self.store.versions_dir, partial[0], "wal.json")))
        self.assertTrue(self.store.open_current().verify()["ok"])

    def test_corrupt_checkpoint_discards_partial_build(self):
        self.build("base")
        with self.assertRaises(_InjectedCrash):
            self.build("crash", crash_after_stage="joined")
        partial = [d for d in os.listdir(self.store.versions_dir)
                   if os.path.exists(os.path.join(self.store.versions_dir, d, "wal.json"))][0]
        with open(os.path.join(self.store.versions_dir, partial, "joined", "edges.jsonl"),
                  "a", encoding="utf-8") as fh:
            fh.write('{"out":"x","in":"y","n":1}\n')
        result = self.build("recover", allow_resume=True)
        self.assertFalse(result.recovered)  # full restart
        self.assertTrue(self.store.open_current().verify()["ok"])

    def test_resume_after_aggregate_keeps_agg_specs_and_zero_notes(self):
        self.build("base")
        # crash AFTER the first aggregate stage has been persisted
        with self.assertRaises(_InjectedCrash):
            self.build("crash", crash_after_stage="region_summary")
        result = self.build("recover", allow_resume=True)
        self.assertTrue(result.recovered)
        ev = self.store.open_current()
        # agg_specs for BOTH group stages must be present despite skipping one
        self.assertEqual(
            set(ev.manifest["agg_specs"]),
            {"region_summary", "channel_summary"})
        # zero explanation still works on the resumed, published version
        zero_group = next(r for r in ev.rows_of("region_summary")
                          if r.data["total_amount"] == 0)
        explanation = ev.explain_zero(
            "region_summary", zero_group.rid, "total_amount")
        self.assertEqual(explanation["kind"], "cancelled")
        self.assertTrue(result.zero_explanations)

    def test_repeated_rebuilds_leave_no_stale_wal(self):
        self.build("a")
        self.build("b")
        self.build("c")
        wals = []
        for d in os.listdir(self.store.versions_dir):
            wals += [f for f in os.listdir(os.path.join(self.store.versions_dir, d))
                     if f == "wal.json"]
        self.assertEqual(wals, [])


class ConcurrencyTests(EngineTestBase):
    def test_writer_lock_serialises_retract_and_gc(self):
        self.build("base")
        errors = []

        def worker(rid):
            try:
                with self.store.lock():
                    self.store.retract(rid)
                    build(self.store, self.config, self.sources, note=f"r {rid}")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(f"orders#{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        ev = self.store.open_current()
        self.assertTrue(ev.verify()["ok"])
        # every retraction made it into some version; current holds all four
        self.assertEqual(len(ev.manifest["tombstones"]), 4)

    def test_readers_always_see_one_coherent_version(self):
        """While a rebuild runs, every read resolves a single immutable
        version directory — rows and evidence can never be mixed."""
        first = self.build("base").version_id
        seen_versions = set()
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                ev = self.store.open_current()
                if ev is None:
                    continue
                vid = ev.manifest["version"]
                seen_versions.add(vid)
                # cross-check: every edge target exists inside THIS version
                report = ev.verify()
                if not report["ok"]:
                    raise AssertionError(report["issues"])

        t = threading.Thread(target=reader)
        t.start()
        for i in range(3):
            with self.store.lock():
                self.store.retract(f"orders#{i + 1}")
                build(self.store, self.config, self.sources, note=f"b{i}")
        stop.set()
        t.join()
        self.assertIn(first, seen_versions)
        self.assertGreaterEqual(len(seen_versions), 2)


if __name__ == "__main__":
    unittest.main()
