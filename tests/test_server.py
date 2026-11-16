"""End-to-end tests of the HTTP API and the real web page assets."""
import json
import os
import shutil
import sys
import tempfile
import threading
import unittest
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pointwise.engine import Store, build  # noqa: E402
from pointwise.server import make_handler  # noqa: E402

REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(REPO, "examples"))
import pipeline as demo  # noqa: E402


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.store = Store(os.path.join(cls.tmp, "store"))
        cls.sources = {k: os.path.join(REPO, p) for k, p in demo.SOURCE_FILES.items()}
        with cls.store.lock():
            build(cls.store, demo.PIPELINE, cls.sources, note="seed")
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0),
                                        make_handler(cls.store, demo.PIPELINE, cls.sources))
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def get(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    @staticmethod
    def q(**params):
        return "?" + urllib.parse.urlencode(params)

    def post(self, path, payload):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_health_and_versions(self):
        self.assertTrue(self.get("/api/health")["ok"])
        versions = self.get("/api/versions")["versions"]
        self.assertTrue(any(v["is_current"] for v in versions))

    def test_web_assets_are_served(self):
        for path, needle in (("/", "Pointwise"), ("/app.js", "showLineage"),
                             ("/styles.css", "--accent")):
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as resp:
                self.assertIn(needle, resp.read().decode("utf-8"))

    def test_rows_filter_and_group(self):
        data = self.get("/api/rows?stage=joined&filter_col=amount&filter_op=%3C&filter_val=0")
        self.assertTrue(all(r["data"]["amount"] < 0 for r in data["rows"]))
        grp = self.get("/api/rows?stage=joined&group_by=region")
        self.assertIsNotNone(grp["groups"])
        self.assertTrue(all(g["count"] > 0 for g in grp["groups"]))

    def test_expand_paths_and_sources_agree(self):
        rows = self.get("/api/rows?stage=region_summary")["rows"]
        rid = rows[0]["rid"]
        exp = self.get("/api/expand" + self.q(rid=rid))
        self.assertTrue(exp["rules"])
        paths = self.get("/api/paths" + self.q(rid=rid))
        self.assertGreater(paths["path_count"], 0)
        participation = self.get("/api/sources" + self.q(rid=rid))["participation"]
        # path endpoints must reach the same source rids (full rids, not stages)
        path_leaves = {p[-1] for p in paths["paths"]}
        self.assertTrue(set(participation).intersection(path_leaves))

    def test_explain_zero_and_drops_filters(self):
        rows = self.get("/api/rows?stage=region_summary")["rows"]
        zero_rid = next(r["rid"] for r in rows if r["data"]["total_amount"] == 0)
        explanation = self.get("/api/explain-zero" + self.q(
            stage="region_summary", rid=zero_rid, alias="total_amount"))
        self.assertEqual(explanation["kind"], "cancelled")
        drops = self.get("/api/drops?reason=" + urllib.parse.quote("重复"))
        self.assertTrue(all("重复" in d["reason"] for d in drops["drops"]))

    def test_retract_via_api_creates_new_current_version(self):
        before = self.get("/api/health")["current"]
        out = self.post("/api/retract", {"rid": "orders#3"})
        self.assertNotEqual(before, out["version"])
        self.assertTrue(out["stats"]["verification"]["ok"])
        # restore so other tests keep a clean seed
        self.post("/api/restore", {"rid": "orders#3"})

    def test_diff_endpoint(self):
        self.post("/api/rebuild", {"note": "diff setup"})
        versions = [v["version"] for v in self.get("/api/versions")["versions"]]
        self.assertGreaterEqual(len(versions), 2)
        diff = self.get(f"/api/diff?from={versions[0]}&to={versions[-1]}")
        self.assertIn("summary", diff)

    def test_release_switch_and_gc(self):
        versions = [v["version"] for v in self.get("/api/versions")["versions"]]
        target = versions[0]
        self.post("/api/release", {"version": target})
        self.assertEqual(self.get("/api/health")["current"], target)
        # switch back to the newest so subsequent assertions see full data
        self.post("/api/release", {"version": versions[-1]})
        out = self.post("/api/gc", {"keep": 5})
        self.assertIn("removed", out)


if __name__ == "__main__":
    unittest.main()
