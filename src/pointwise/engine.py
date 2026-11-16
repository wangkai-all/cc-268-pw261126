"""Build engine: execute pipelines, persist evidence, recover, release.

Consistency model
-----------------
* Every released version lives in an *immutable* directory; evidence and
  result rows of a version are written together before publication.
* Publication is one atomic ``os.replace`` of the ``current`` symlink.
  Readers resolve the symlink once per request, so they can never see
  new results next to old evidence.
* A single ``flock`` serialises writers (source deletion / rebuild /
  evidence GC).  GC only removes versions that are not ``current``.
* Interrupted builds recover from per-stage checkpoints (checksummed);
  a corrupt checkpoint discards the whole partial build and restarts.
"""
from __future__ import annotations

import fcntl
import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from typing import Any

from . import FORMAT_VERSION, __version__
from .evidence import EvidenceStore
from .io_utils import (
    atomic_write_text,
    fsync_dir,
    load_source,
    sha256_file,
    write_json,
    write_jsonl,
)
from .model import Row, Relation, content_hash
from .pipeline import build_transform, pipeline_fingerprint, source_relation, stage_inputs
from .transforms import GroupAggregate


@dataclass
class BuildResult:
    version_id: str
    recovered: bool
    stages_completed: int
    stages_total: int
    zero_explanations: list[dict[str, Any]]
    stats: dict[str, Any]


class WriteLock:
    """Process-exclusive advisory lock for all mutating operations."""

    def __init__(self, store: "Store"):
        self.store = store
        self.fh = None

    def __enter__(self):
        self.fh = open(os.path.join(self.store.root, ".lock"), "w")
        fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
        self.fh.close()
        self.fh = None


class Store:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.versions_dir = os.path.join(self.root, "versions")
        self.sources_dir = os.path.join(self.root, "sources")
        self.tombstones_path = os.path.join(self.root, "tombstones.json")
        self.current_link = os.path.join(self.root, "current")
        os.makedirs(self.versions_dir, exist_ok=True)
        os.makedirs(self.sources_dir, exist_ok=True)

    # ------------------------------------------------------------------ #
    # tombstones = user-initiated retraction of a source row
    # ------------------------------------------------------------------ #
    def read_tombstones(self) -> list[str]:
        if not os.path.exists(self.tombstones_path):
            return []
        with open(self.tombstones_path, encoding="utf-8") as fh:
            return json.load(fh).get("retracted", [])

    def write_tombstones(self, rids: list[str]) -> None:
        write_json(self.tombstones_path, {"retracted": sorted(set(rids))})

    def retract(self, rid: str) -> None:
        rids = self.read_tombstones()
        if rid not in rids:
            rids.append(rid)
        self.write_tombstones(rids)

    def restore(self, rid: str) -> None:
        rids = [r for r in self.read_tombstones() if r != rid]
        self.write_tombstones(rids)

    # ------------------------------------------------------------------ #
    def current_version(self) -> str | None:
        if not os.path.islink(self.current_link):
            return None
        target = os.readlink(self.current_link)
        return os.path.basename(os.path.normpath(target))

    def open_current(self) -> EvidenceStore | None:
        vid = self.current_version()
        if vid is None:
            return None
        return EvidenceStore(os.path.join(self.versions_dir, vid))

    def open_version(self, vid: str) -> EvidenceStore:
        return EvidenceStore(os.path.join(self.versions_dir, vid))

    def versions(self) -> list[dict[str, Any]]:
        out = []
        for name in sorted(os.listdir(self.versions_dir)):
            manifest_path = os.path.join(self.versions_dir, name, "manifest.json")
            if not os.path.exists(manifest_path):
                continue
            with open(manifest_path, encoding="utf-8") as fh:
                m = json.load(fh)
            out.append(
                {
                    "version": name,
                    "created_at": m.get("created_at"),
                    "note": m.get("note", ""),
                    "source_hash": m.get("source_hash"),
                    "pipeline_hash": m.get("pipeline_hash"),
                    "is_current": name == self.current_version(),
                }
            )
        return out

    # ------------------------------------------------------------------ #
    def lock(self) -> WriteLock:
        return WriteLock(self)

    def gc(self, keep: int = 3) -> list[str]:
        """Delete old immutable versions (never the current one)."""
        removed = []
        current = self.current_version()
        alive = [v for v in self.versions()]
        alive.sort(key=lambda v: v["created_at"])
        keepers = {v["version"] for v in alive[-keep:]}
        if current:
            keepers.add(current)
        for v in alive:
            if v["version"] not in keepers:
                shutil.rmtree(os.path.join(self.versions_dir, v["version"]))
                removed.append(v["version"])
        return removed


# --------------------------------------------------------------------------- #
# build
# --------------------------------------------------------------------------- #


def build(
    store: Store,
    config: dict[str, Any],
    source_files: dict[str, str],
    note: str = "",
    crash_after_stage: str | None = None,
    allow_resume: bool = False,
) -> BuildResult:
    """Execute a full build end to end (caller must hold the write lock).

    ``crash_after_stage`` is a test/demo hook: the process exits (via an
    exception that leaves WAL + partial files on disk) right after the
    named stage is persisted, before publication.

    ``allow_resume=True`` adopts an earlier crashed attempt with the same
    content id and continues from its checkpoints. A normal (non-recovery)
    build instead purges such leftovers first — it must never silently
    reuse another invocation's partial work.
    """
    stages_spec = config["stages"]
    tombstones = set(store.read_tombstones())

    # 1. load + snapshot sources ------------------------------------------------
    source_meta: dict[str, Any] = {}
    relations: dict[str, Relation] = {}
    for sname, path in source_files.items():
        rows, columns = load_source(path, sname)
        stored = os.path.join(store.sources_dir, f"{sname}.jsonl")
        write_jsonl(stored, [r.to_json() for r in rows])
        retracted_here = sorted(r.rid for r in rows if r.rid in tombstones)
        source_meta[sname] = {
            "file": path,
            "columns": columns,
            "rows": len(rows),
            "hash": sha256_file(stored),
            "retracted": retracted_here,
        }

    source_hash = content_hash(source_meta)
    pipe_hash = content_hash(pipeline_fingerprint(config))
    content_id = content_hash({"s": source_hash, "p": pipe_hash})[:12]
    # uuid suffix guarantees a fresh directory even for identical content built
    # twice in the same process/second; recovery still matches by content_id.
    attempt = uuid.uuid4().hex[:8]
    version_id = f"v{int(time.time())}-{attempt}-{content_id}"
    version_dir = os.path.join(store.versions_dir, version_id)

    # 2. WAL describes the intended build ---------------------------------------
    wal = {
        "content_id": content_id,
        "version_id": version_id,
        "version_dir": version_dir,
        "config": config,
        "source_meta": source_meta,
        "tombstones": sorted(tombstones),
        "note": note,
        "started_at": time.time(),
        "state": "running",
        "completed_stages": [],
        "checksums": {},
        "crash_after_stage": crash_after_stage,
    }

    # 3. resume from a compatible previous attempt ------------------------------
    # A crashed attempt keeps its WAL inside its own version directory. The
    # recovering process scans versions/ for an unfinished attempt with the
    # same content id and adopts it — genuine continuation from checkpoints.
    # A regular build explicitly purges those leftovers instead.
    if not allow_resume:
        _purge_running_attempts(store, content_id)
    recovered, _, loaded, adopt_dir = _resume(store, wal)
    if recovered:
        version_id = os.path.basename(adopt_dir)
        version_dir = adopt_dir
        wal["version_id"] = version_id
        wal["version_dir"] = version_dir
    relations.update(loaded)
    wal_path = os.path.join(version_dir, "wal.json")

    # persist sources as immutable leaves inside the version itself (only on a
    # fresh build — resumed builds already have them on disk, checksum-verified)
    if not recovered:
        os.makedirs(version_dir, exist_ok=True)
        for sname, path in source_files.items():
            rows, _ = load_source(path, sname)
            sdir = os.path.join(version_dir, sname)
            os.makedirs(sdir, exist_ok=True)
            write_jsonl(os.path.join(sdir, "rows.jsonl"),
                        [r.to_json() for r in rows if r.rid not in tombstones])
            write_jsonl(
                os.path.join(sdir, "drops.jsonl"),
                [{"in": r.rid, "rule": f"{sname}/retraction",
                  "reason": "来源行已被用户撤销（墓碑），不参与本次重算"}
                 for r in rows if r.rid in tombstones],
            )
            write_json(os.path.join(sdir, "rules.json"),
                       [{"rule_id": f"{sname}/source", "stage": sname, "type": "source",
                         "description": f"来源快照：{path}；撤销行以墓碑形式记录在 drops.jsonl"}])
            relations[sname] = source_relation(
                sname, [r for r in rows if r.rid not in tombstones],
                source_meta[sname]["columns"])
        wal["checksums"].update(
            {sname: _stage_checksums(version_dir, sname) for sname in source_files})
        write_json(wal_path, wal)
    else:
        # record that this process is continuing the adopted attempt
        write_json(wal_path, wal)

    stage_input_map: dict[str, list[str]] = {}
    agg_specs: dict[str, list[dict[str, Any]]] = {}
    zero_notes: list[dict[str, Any]] = []

    # 4. execute stages ---------------------------------------------------------
    for spec in stages_spec:
        stage = spec["stage"]
        stage_input_map[stage] = stage_inputs(spec)
        # aggregate specs come from the pipeline definition, so a resumed
        # build that skips an already-persisted group stage still records them
        if spec["type"] == "group":
            agg_specs[stage] = [
                {"alias": a["alias"], "func": a["func"], "column": a.get("column")}
                for a in spec["aggregates"]
            ]
        if stage in wal["completed_stages"]:
            continue
        transform = build_transform(spec)
        result = transform.run(relations)
        _persist_stage(version_dir, stage, spec, result, transform)
        relations[stage] = result.relation
        wal["completed_stages"].append(stage)
        wal.setdefault("checksums", {})[stage] = _stage_checksums(version_dir, stage)
        write_json(wal_path, wal)
        if crash_after_stage == stage:
            raise _InjectedCrash(f"injected crash after stage {stage!r}")

    # zero-value notes are derived after every stage relation is in memory,
    # so they survive a resume that skipped the aggregate stage itself
    zero_notes = []
    for spec in stages_spec:
        if spec["type"] != "group":
            continue
        transform = build_transform(spec)
        # re-run only to reconstruct the group edges; rows are identical by
        # construction (deterministic transforms over the same inputs)
        rerun = transform.run(relations)
        zero_notes.extend(_zero_notes(transform, relations[spec["input"]], rerun))

    # 5. manifest first, then stats (the Evidence reader needs the manifest)
    manifest = {
        "format_version": FORMAT_VERSION,
        "engine_version": __version__,
        "version": version_id,
        "created_at": time.time(),
        "note": note,
        "stages": list(source_files) + [s["stage"] for s in stages_spec],
        "transform_stages": [s["stage"] for s in stages_spec],
        "stage_inputs": stage_input_map,
        "source_stages": list(source_files),
        "sources": source_meta,
        "tombstones": sorted(tombstones),
        "source_hash": source_hash,
        "pipeline_hash": pipe_hash,
        "agg_specs": agg_specs,
        "stats_path": "stats.json",
    }
    write_json(os.path.join(version_dir, "manifest.json"), manifest)
    stats = _compute_stats(store, version_id, version_dir, stages_spec, source_meta, zero_notes)
    write_json(os.path.join(version_dir, "stats.json"), stats)
    fsync_dir(version_dir)

    # 6. atomic publication -----------------------------------------------------
    _publish(store, version_id, version_dir)
    wal["state"] = "published"
    write_json(wal_path, wal)
    try:
        os.unlink(wal_path)
    except FileNotFoundError:
        pass

    return BuildResult(
        version_id=version_id,
        recovered=recovered,
        stages_completed=len(wal["completed_stages"]),
        stages_total=len(stages_spec),
        zero_explanations=zero_notes,
        stats=stats,
    )


class _InjectedCrash(RuntimeError):
    pass


def _persist_stage(version_dir: str, stage: str, spec: dict[str, Any], result, transform=None) -> None:
    base = os.path.join(version_dir, stage)
    os.makedirs(base, exist_ok=True)
    write_jsonl(os.path.join(base, "rows.jsonl"), [r.to_json() for r in result.relation.rows])
    write_jsonl(os.path.join(base, "edges.jsonl"), [e.to_json() for e in result.edges])
    write_jsonl(os.path.join(base, "drops.jsonl"), [d.to_json() for d in result.drops])
    rule = transform if transform is not None else build_transform(spec)
    write_json(
        os.path.join(base, "rules.json"),
        [{"rule_id": rule.rule_id, "stage": stage,
          "type": spec["type"], "description": rule.describe()}],
    )


def _zero_notes(transform: GroupAggregate, input_relation, result) -> list[dict[str, Any]]:
    notes = []
    sum_aliases = [a.alias for a in transform.aggregates if a.func == "sum"]
    if not sum_aliases:
        return notes
    in_index = input_relation.row_by_rid()
    for row in result.relation.rows:
        group_edges = [e for e in result.edges if e.out_rid == row.rid]
        for alias in sum_aliases:
            if row.data.get(alias) != 0:
                continue
            col = next(a.column for a in transform.aggregates if a.alias == alias)
            pos = neg = zero = 0
            for e in group_edges:
                v = in_index[e.in_rid].data.get(col)
                if v is None:
                    continue
                if v > 0:
                    pos += 1
                elif v < 0:
                    neg += 1
                else:
                    zero += 1
            if pos and neg:
                kind = "cancelled"
                reason = "冲销：正数与负数相加为 0"
            else:
                kind = "all_zero"
                reason = "参与行该列本身全为 0（有数据，非空组）"
            notes.append(
                {"stage": transform.stage, "group_rid": row.rid, "alias": alias,
                 "zero_kind": kind, "reason": reason,
                 "positive_rows": pos, "negative_rows": neg, "zero_rows": zero}
            )
    return notes


def _stage_checksums(version_dir: str, stage: str) -> dict[str, str]:
    return {
        fname: sha256_file(os.path.join(version_dir, stage, fname))
        for fname in ("rows.jsonl", "edges.jsonl", "drops.jsonl", "rules.json")
        if os.path.exists(os.path.join(version_dir, stage, fname))
    }


def _purge_running_attempts(store: Store, content_id: str) -> None:
    """Remove unfinished attempts matching this content (fresh-build policy)."""
    if not os.path.isdir(store.versions_dir):
        return
    for name in os.listdir(store.versions_dir):
        vdir = os.path.join(store.versions_dir, name)
        wal_file = os.path.join(vdir, "wal.json")
        if not os.path.exists(wal_file):
            continue
        try:
            with open(wal_file, encoding="utf-8") as fh:
                candidate = json.load(fh)
        except (json.JSONDecode, OSError):
            shutil.rmtree(vdir, ignore_errors=True)
            continue
        if candidate.get("state") == "running" and candidate.get("content_id") == content_id:
            shutil.rmtree(vdir, ignore_errors=True)


def _resume(store: Store, wal: dict[str, Any]) -> tuple[bool, int, dict, str]:
    """Find an interrupted attempt with the same content id and adopt it.

    Returns ``(recovered, start_index, loaded_relations, version_dir)``.
    Checksum mismatch on any persisted file means a torn write: the whole
    partial attempt is discarded and the caller starts from scratch.
    """
    content_id = wal["content_id"]
    candidate_dir = ""
    old = None
    if os.path.isdir(store.versions_dir):
        for name in sorted(os.listdir(store.versions_dir)):
            vdir = os.path.join(store.versions_dir, name)
            wal_file = os.path.join(vdir, "wal.json")
            if not os.path.exists(wal_file):
                continue
            try:
                with open(wal_file, encoding="utf-8") as fh:
                    candidate = json.load(fh)
            except (json.JSONDecode, OSError):
                continue
            if candidate.get("state") == "running" and candidate.get("content_id") == content_id:
                old, candidate_dir = candidate, vdir
                break
    if old is None:
        return False, 0, {}, ""
    if old.get("config") != wal["config"] or old.get("tombstones") != wal["tombstones"]:
        shutil.rmtree(candidate_dir, ignore_errors=True)
        return False, 0, {}, ""

    loaded: dict[str, Relation] = {}
    persisted = list(old.get("completed_stages", [])) + list(wal["source_meta"].keys())
    for stage in persisted:
        base = os.path.join(candidate_dir, stage)
        rows_path = os.path.join(base, "rows.jsonl")
        try:
            with open(rows_path, encoding="utf-8") as fh:
                payload = [json.loads(line) for line in fh if line.strip()]
        except (json.JSONDecode, OSError):
            shutil.rmtree(candidate_dir, ignore_errors=True)
            return False, 0, {}, ""
        # checksum guard against torn/partial writes of any stage file
        expected = old.get("checksums", {}).get(stage, {})
        if expected != _stage_checksums(candidate_dir, stage):
            shutil.rmtree(candidate_dir, ignore_errors=True)
            return False, 0, {}, ""
        rows = [Row(r["rid"], r["data"]) for r in payload]
        cols = list(payload[0]["data"].keys()) if payload else []
        loaded[stage] = Relation(name=stage, rows=rows, columns=cols)
    wal["completed_stages"] = list(old.get("completed_stages", []))
    wal["checksums"] = dict(old.get("checksums", {}))
    return True, 0, loaded, candidate_dir


def _publish(store: Store, version_id: str, version_dir: str) -> None:
    link = store.current_link
    tmp = os.path.join(store.root, ".current.tmp")
    target = os.path.join("versions", version_id)
    if os.path.lexists(tmp):
        os.unlink(tmp)
    os.symlink(target, tmp)
    os.replace(tmp, link)  # atomic on POSIX
    fsync_dir(store.root)


# --------------------------------------------------------------------------- #
# evidence sizing / explainability stats
# --------------------------------------------------------------------------- #


def _dir_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def _compute_stats(
    store: Store,
    version_id: str,
    version_dir: str,
    stages_spec: list[dict[str, Any]],
    source_meta: dict[str, Any],
    zero_notes: list[dict[str, Any]],
) -> dict[str, Any]:
    ev = EvidenceStore(version_dir)
    stage_stats = []
    edge_records = drop_records = 0
    edge_bytes = drop_bytes = 0
    all_sources = sum(m["rows"] for m in source_meta.values())
    active_sources = sum(m["rows"] - len(m["retracted"]) for m in source_meta.values())
    retracted = sum(len(m["retracted"]) for m in source_meta.values())

    # Full-path materialisation estimate.
    # If we stored every complete source path for every output row at every
    # stage, the number of (row, path-entry) pairs equals the sum over every
    # stage row of its total source-participation count (edge multiplicity
    # multiplied along each path). We compute that exactly with a memoised
    # bottom-up count, then multiply by the average bytes of one edge record.
    def source_count(rid: str, memo: dict[str, int]) -> int:
        if rid in memo:
            return memo[rid]
        stage = ev.stage_of(rid)
        edges = ev.edges_from(stage, rid)
        if not edges:
            memo[rid] = 1 if stage in source_meta else 0
            return memo[rid]
        memo[rid] = sum(e.get("n", 1) * source_count(e["in"], memo) for e in edges)
        return memo[rid]

    memo: dict[str, int] = {}
    total_path_entries = 0
    total_stage_rows = 0
    for stage in ev.manifest.get("transform_stages", ev.stages):
        for row in ev.rows_of(stage):
            total_path_entries += source_count(row.rid, memo)
            total_stage_rows += 1

    for stage in ev.stages:
        base = os.path.join(version_dir, stage)
        e_path = os.path.join(base, "edges.jsonl")
        d_path = os.path.join(base, "drops.jsonl")
        e_bytes = os.path.getsize(e_path) if os.path.exists(e_path) else 0
        d_bytes = os.path.getsize(d_path) if os.path.exists(d_path) else 0
        n_edges = _load_count(e_path)
        n_drops = _load_count(d_path)
        edge_records += n_edges
        drop_records += n_drops
        edge_bytes += e_bytes
        drop_bytes += d_bytes
        stage_stats.append(
            {
                "stage": stage,
                "rows": len(ev.rows_of(stage)),
                "edges": n_edges,
                "drops": n_drops,
                "edge_bytes": e_bytes,
                "drop_bytes": d_bytes,
                "rules": ev.rules_of(stage),
            }
        )

    # depth = longest source-to-terminal chain in the stage DAG
    depth = {s: 1 for s in source_meta}
    for spec in stages_spec:
        depth[spec["stage"]] = 1 + max(depth.get(x, 0) for x in stage_inputs(spec))
    max_depth = max(depth.values())

    total_bytes = _dir_bytes(version_dir)
    avg_edge_size = edge_bytes / edge_records if edge_records else 0
    estimated_full_paths = int(total_path_entries * avg_edge_size)
    coverage_note = (
        f"精确统计全部 {total_stage_rows} 个变换阶段输出行的来源参与条目共 "
        f"{total_path_entries} 条（边重数沿路径相乘，扇出/重复不丢次数），"
        f"× 边记录平均 {avg_edge_size:.1f} 字节；完整路径不物化，查询时按需展开"
        "（paths 接口单次上限可配，防极端扇出撑爆响应）"
    )
    saving_ratio = (
        round(1 - (edge_bytes + drop_bytes) / estimated_full_paths, 3)
        if estimated_full_paths > 0
        else None
    )

    verification = ev.verify()
    return {
        "version": version_id,
        "sources": {"total": all_sources, "active": active_sources, "retracted": retracted},
        "evidence": {
            "edge_records": edge_records,
            "drop_records": drop_records,
            "edge_bytes": edge_bytes,
            "drop_bytes": drop_bytes,
            "total_version_bytes": total_bytes,
            "materialised_path_entries": total_path_entries,
            "estimated_full_path_bytes": estimated_full_paths,
            "saving_ratio": saving_ratio,
            "max_stage_depth": max_depth,
            "coverage": "全部输出行均可递归展开到原始来源行；引用完整性已逐边校验",
            "estimate_note": coverage_note,
        },
        "explainability_scope": {
            "row_level": "每个结果行 -> 直接输入行（含重数 n）-> 递归到来源行的完整路径",
            "rules": "每阶段规则（类型 + 自然语言说明）随证据同目录保存",
            "drops": "每条被丢弃/冲销/未匹配的行都有 rule_id 与原因，可按阶段筛选",
            "zero": "零值区分 cancelled（冲销）/ all_zero（输入本身为零）/ no_group（无此分组）",
            "retraction": "来源撤销通过墓碑重算，旧版本完整保留可对比",
        },
        "zero_explanations": zero_notes,
        "verification": verification,
        "stages": stage_stats,
    }


def _load_count(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, encoding="utf-8") as fh:
        return sum(1 for line in fh if line.strip())
