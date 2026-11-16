"""Standard-library HTTP server exposing evidence queries and actions.

All reads target ONE immutable version per request (the symlink is
resolved a single time), so a concurrent rebuild can never mix rows from
a new version with evidence from an old one.
"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .diff import diff_versions
from .engine import Store, build
from .evidence import EvidenceStore

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")


def make_handler(store: Store, config: dict, source_files: dict[str, str]):
    class Handler(BaseHTTPRequestHandler):
        server_version = "Pointwise/1.0"

        def log_message(self, fmt, *args):  # quieter logs
            pass

        # -------------------------------------------------------------- #
        def _send(self, obj, status=200, ctype="application/json; charset=utf-8"):
            body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _static(self, name: str, ctype: str):
            path = os.path.join(WEB_DIR, name)
            if not os.path.exists(path):
                self._send({"error": "not found"}, 404)
                return
            with open(path, "rb") as fh:
                body = fh.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _version(self, q: dict) -> EvidenceStore | None:
            vid = (q.get("version") or [None])[0]
            if vid:
                return store.open_version(vid)
            return store.open_current()

        # -------------------------------------------------------------- #
        def do_GET(self):
            parsed = urlparse(self.path)
            path, q = parsed.path, parse_qs(parsed.query)
            try:
                if path == "/" or path == "/index.html":
                    return self._static("index.html", "text/html; charset=utf-8")
                if path == "/app.js":
                    return self._static("app.js", "application/javascript; charset=utf-8")
                if path == "/styles.css":
                    return self._static("styles.css", "text/css; charset=utf-8")
                if path == "/api/health":
                    return self._send({"ok": True, "current": store.current_version()})
                if path == "/api/versions":
                    return self._send({"versions": store.versions()})
                if path == "/api/stats":
                    ev = self._version(q)
                    if ev is None:
                        return self._send({"error": "no version yet"}, 404)
                    with open(os.path.join(ev.version_dir, "stats.json"), encoding="utf-8") as fh:
                        return self._send(json.load(fh))
                if path == "/api/stages":
                    ev = self._version(q)
                    if ev is None:
                        return self._send({"error": "no version yet"}, 404)
                    return self._send({
                        "stages": [{"name": s,
                                    "inputs": ev.upstream_stages(s),
                                    "rules": ev.rules_of(s)}
                                   for s in ev.manifest.get("transform_stages", ev.stages)],
                        "sources": ev.manifest["source_stages"],
                        "tombstones": ev.manifest.get("tombstones", []),
                        "version": ev.manifest["version"],
                    })
                if path == "/api/rows":
                    return self._rows(q)
                if path == "/api/drops":
                    return self._drops(q)
                if path == "/api/expand":
                    ev = self._version(q)
                    rid = q["rid"][0]
                    return self._send(ev.expand(rid))
                if path == "/api/paths":
                    ev = self._version(q)
                    limit = int(q.get("limit", ["500"])[0])
                    return self._send(ev.paths(q["rid"][0], limit=limit))
                if path == "/api/sources":
                    ev = self._version(q)
                    rid = q["rid"][0]
                    return self._send({"rid": rid,
                                       "participation": ev.source_participation(rid)})
                if path == "/api/explain-zero":
                    ev = self._version(q)
                    return self._send(ev.explain_zero(q["stage"][0], q["rid"][0], q["alias"][0]))
                if path == "/api/verify":
                    ev = self._version(q)
                    return self._send(ev.verify())
                if path == "/api/diff":
                    old = store.open_version(q["from"][0])
                    new = store.open_version(q["to"][0])
                    return self._send(diff_versions(old, new))
                return self._send({"error": "unknown route", "path": path}, 404)
            except FileNotFoundError as exc:
                return self._send({"error": str(exc)}, 404)
            except KeyError as exc:
                return self._send({"error": f"缺少必填参数: {exc.args[0]}"}, 400)
            except Exception as exc:  # noqa: BLE001 - API boundary
                return self._send({"error": f"{type(exc).__name__}: {exc}"}, 500)

        def do_POST(self):
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/api/retract":
                    payload = self._body()
                    with store.lock():
                        store.retract(payload["rid"])
                        result = build(store, config, source_files,
                                       note=payload.get("note", f"撤销 {payload['rid']} 后重算"))
                    return self._send({"ok": True, "version": result.version_id,
                                       "stats": result.stats})
                if parsed.path == "/api/restore":
                    payload = self._body()
                    with store.lock():
                        store.restore(payload["rid"])
                        result = build(store, config, source_files,
                                       note=payload.get("note", f"恢复 {payload['rid']} 后重算"))
                    return self._send({"ok": True, "version": result.version_id,
                                       "stats": result.stats})
                if parsed.path == "/api/rebuild":
                    payload = self._body()
                    with store.lock():
                        result = build(store, config, source_files,
                                       note=payload.get("note", "手动重算"))
                    return self._send({"ok": True, "version": result.version_id,
                                       "recovered": result.recovered})
                if parsed.path == "/api/release":
                    # switch current pointer to an existing immutable version
                    payload = self._body()
                    target = payload["version"]
                    if not os.path.isdir(os.path.join(store.versions_dir, target)):
                        return self._send({"error": "unknown version"}, 404)
                    with store.lock():
                        tmp = os.path.join(store.root, ".current.tmp")
                        if os.path.lexists(tmp):
                            os.unlink(tmp)
                        os.symlink(os.path.join("versions", target), tmp)
                        os.replace(tmp, store.current_link)
                    return self._send({"ok": True, "current": target})
                if parsed.path == "/api/gc":
                    payload = self._body()
                    with store.lock():
                        removed = store.gc(keep=int(payload.get("keep", 3)))
                    return self._send({"ok": True, "removed": removed})
                return self._send({"error": "unknown route"}, 404)
            except Exception as exc:  # noqa: BLE001 - API boundary
                return self._send({"error": f"{type(exc).__name__}: {exc}"}, 500)

        # -------------------------------------------------------------- #
        def _rows(self, q: dict) -> None:
            ev = self._version(q)
            if ev is None:
                return self._send({"error": "no version yet"}, 404)
            stage = q["stage"][0]
            rows = ev.rows_of(stage)
            flt_col = (q.get("filter_col") or [None])[0]
            flt_op = (q.get("filter_op") or ["contains"])[0]
            flt_val = (q.get("filter_val") or [None])[0]
            group_by = (q.get("group_by") or [None])[0]
            page = int((q.get("page") or ["1"])[0])
            page_size = int((q.get("page_size") or ["100"])[0])

            def keep(row):
                if not flt_col or flt_val is None or flt_val == "":
                    return True
                actual = row.data.get(flt_col)
                if actual is None:
                    return False
                if flt_op == "contains":
                    return str(flt_val).lower() in str(actual).lower()
                if flt_op == "==":
                    return str(actual) == str(flt_val)
                if flt_op == "!=":
                    return str(actual) != str(flt_val)
                if flt_op == ">":
                    return actual is not None and _num(actual) > _num(flt_val)
                if flt_op == "<":
                    return actual is not None and _num(actual) < _num(flt_val)
                return True

            filtered = [r for r in rows if keep(r)]
            groups = None
            if group_by:
                groups = {}
                for r in filtered:
                    groups.setdefault(str(r.data.get(group_by)), 0)
                    groups[str(r.data.get(group_by))] += 1
                groups = [{"key": k, "count": v} for k, v in sorted(groups.items())]
            start = (page - 1) * page_size
            page_rows = filtered[start:start + page_size]
            return self._send({
                "stage": stage,
                "columns": ev.rows_of(stage)[0].data.keys() if rows else [],
                "total": len(filtered),
                "page": page,
                "page_size": page_size,
                "groups": groups,
                "rows": [{"rid": r.rid, "data": r.data} for r in page_rows],
            })

        def _drops(self, q: dict) -> None:
            ev = self._version(q)
            if ev is None:
                return self._send({"error": "no version yet"}, 404)
            stage = (q.get("stage") or [None])[0]
            stages = [stage] if stage else ev.stages
            reason_q = (q.get("reason") or [""])[0].lower()
            rule_q = (q.get("rule") or [""])[0].lower()
            out = []
            for s in stages:
                for d in ev.drops_of(s):
                    if reason_q and reason_q not in d["reason"].lower():
                        continue
                    if rule_q and rule_q not in d["rule"].lower():
                        continue
                    row = ev.get_row(d["in"])
                    out.append({**d, "stage": s,
                                "data": row.data if row else None})
            return self._send({"drops": out, "total": len(out)})

    return Handler


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def serve(store: Store, config: dict, source_files: dict, host: str, port: int) -> None:
    httpd = ThreadingHTTPServer((host, port), make_handler(store, config, source_files))
    print(f"Pointwise web UI: http://{host}:{port}/  (Ctrl+C 停止)")
    httpd.serve_forever()
