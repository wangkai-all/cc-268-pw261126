"""Row-level evidence: storage layout, on-demand expansion and verification.

On-disk evidence per stage (all JSONL/JSON, human-inspectable):

    versions/<v>/<stage>/
        rows.jsonl     {"rid": ..., "data": {...}}
        edges.jsonl    {"out": out_rid, "in": in_rid, "n": multiplicity}
        drops.jsonl    {"in": in_rid, "rule": rule_id, "reason": ...}
        rules.json     [{"rule_id", "stage", "type", "description"}]

Only the compact edge set is materialised. Full source paths are expanded
on demand by walking edges, so a row can always be opened to see *which
original rows and which rules* produced it without paying full-path
storage for every row.
"""
from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import dataclass
from typing import Any

from .model import Row, stage_of_rid


@dataclass
class StageBundle:
    stage: str
    rows: list[Row]
    row_index: dict[str, Row]
    out_edges: dict[str, list[dict[str, Any]]]
    in_edges: dict[str, list[dict[str, Any]]]
    drops: list[dict[str, Any]]
    rules: list[dict[str, Any]]


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class EvidenceStore:
    """Read access to one immutable released version."""

    def __init__(self, version_dir: str):
        self.version_dir = version_dir
        self._bundles: dict[str, StageBundle] = {}
        with open(os.path.join(version_dir, "manifest.json"), "r", encoding="utf-8") as fh:
            self.manifest = json.load(fh)
        self.stages: list[str] = list(self.manifest["stages"])

    # ------------------------------------------------------------------ #
    def _load(self, stage: str) -> StageBundle:
        if stage not in self._bundles:
            base = os.path.join(self.version_dir, stage)
            rows = [Row(r["rid"], r["data"]) for r in _read_jsonl(os.path.join(base, "rows.jsonl"))]
            edges = _read_jsonl(os.path.join(base, "edges.jsonl"))
            out_edges: dict[str, list[dict[str, Any]]] = {}
            in_edges: dict[str, list[dict[str, Any]]] = {}
            for e in edges:
                out_edges.setdefault(e["out"], []).append(e)
                in_edges.setdefault(e["in"], []).append(e)
            rules_path = os.path.join(base, "rules.json")
            if os.path.exists(rules_path):
                with open(rules_path, encoding="utf-8") as rfh:
                    rules = json.load(rfh)
            else:
                rules = []
            self._bundles[stage] = StageBundle(
                stage=stage,
                rows=rows,
                row_index={r.rid: r for r in rows},
                out_edges=out_edges,
                in_edges=in_edges,
                drops=_read_jsonl(os.path.join(base, "drops.jsonl")),
                rules=rules,
            )
        return self._bundles[stage]

    # ------------------------------------------------------------------ #
    def stage_of(self, rid: str) -> str:
        return stage_of_rid(rid)

    def get_row(self, rid: str) -> Row | None:
        bundle = self._load(self.stage_of(rid))
        return bundle.row_index.get(rid)

    def edges_from(self, stage: str, rid: str) -> list[dict[str, Any]]:
        return self._load(stage).out_edges.get(rid, [])

    def edges_to(self, stage: str, rid: str) -> list[dict[str, Any]]:
        return self._load(stage).in_edges.get(rid, [])

    def drops_of(self, stage: str) -> list[dict[str, Any]]:
        return self._load(stage).drops

    def rules_of(self, stage: str) -> list[dict[str, Any]]:
        return self._load(stage).rules

    def rows_of(self, stage: str) -> list[Row]:
        return self._load(stage).rows

    def upstream_stages(self, stage: str) -> list[str]:
        return [s for s in self.manifest["stage_inputs"].get(stage, []) if s in self.stages]

    # ------------------------------------------------------------------ #
    def expand(self, rid: str) -> dict[str, Any]:
        """Expand one result row one level: its direct input rows + rules."""
        stage = self.stage_of(rid)
        row = self.get_row(rid)
        if row is None:
            return {"rid": rid, "found": False, "inputs": [], "rules": [], "drops": []}
        edges = self.edges_from(stage, rid)
        inputs = []
        for e in edges:
            in_row = self.get_row(e["in"])
            inputs.append(
                {
                    "rid": e["in"],
                    "stage": self.stage_of(e["in"]),
                    "n": e.get("n", 1),
                    "data": in_row.data if in_row else None,
                    "missing": in_row is None,
                }
            )
        return {
            "rid": rid,
            "stage": stage,
            "found": True,
            "data": row.data,
            "inputs": inputs,
            "rules": self.rules_of(stage),
        }

    def source_participation(self, rid: str) -> dict[str, int]:
        """Map source rid -> number of participations along ALL paths.

        Multiplicity (edge ``n`` and the number of distinct paths) is
        multiplied, so joins that fan out and repeated aggregations never
        erase how many times a source row contributed.
        """
        counts: Counter[str] = Counter()

        def walk(cur: str, mult: int) -> None:
            stage = self.stage_of(cur)
            edges = self.edges_from(stage, cur)
            if not edges:
                if stage in self.manifest.get("source_stages", []):
                    counts[cur] += mult
                return
            for e in edges:
                walk(e["in"], mult * e.get("n", 1))

        walk(rid, 1)
        return dict(sorted(counts.items()))

    def paths(self, rid: str, limit: int = 500) -> dict[str, Any]:
        """Enumerate complete source paths on demand (capped for safety)."""
        found: list[list[str]] = []
        truncated = False

        def walk(cur: str, chain: list[str]) -> None:
            nonlocal truncated
            if len(found) >= limit:
                truncated = True
                return
            stage = self.stage_of(cur)
            edges = self.edges_from(stage, cur)
            if not edges:
                found.append(list(chain))
                return
            for e in edges:
                if truncated:
                    return
                for _ in range(e.get("n", 1)):
                    walk(e["in"], chain + [e["in"]])

        walk(rid, [rid])
        return {"rid": rid, "path_count": len(found), "truncated": truncated, "paths": found}

    def explain_zero(self, group_stage: str, group_rid: str, agg_alias: str) -> dict[str, Any]:
        """Explain an aggregate value, distinguishing the three zero cases."""
        bundle = self._load(group_stage)
        group_row = bundle.row_index.get(group_rid)
        if group_row is None:
            return {
                "kind": "no_group",
                "message": "不存在该分组：根本没有数据进入该键（与值为 0 不同）",
            }
        spec = next((a for a in self.manifest["agg_specs"].get(group_stage, [])
                     if a["alias"] == agg_alias), None)
        value = group_row.data.get(agg_alias)
        edges = bundle.out_edges.get(group_rid, [])
        if not edges:
            return {
                "kind": "no_input",
                "value": value,
                "message": "分组存在但没有任何输入行参与",
            }
        if spec is None or spec["func"] != "sum":
            return {"kind": "not_sum", "value": value, "message": f"聚合 {agg_alias} 非求和，值为 {value}"}

        col = spec["column"]
        entries = []
        positives = negatives = zeros = 0
        pos_sum = neg_sum = 0
        for e in edges:
            in_row = self.get_row(e["in"])
            v = in_row.data.get(col) if in_row else None
            entries.append({"rid": e["in"], "n": e.get("n", 1), "value": v})
            if v is None:
                continue
            if v > 0:
                positives += 1
                pos_sum += v * e.get("n", 1)
            elif v < 0:
                negatives += 1
                neg_sum += v * e.get("n", 1)
            else:
                zeros += 1
        if value != 0:
            kind = "nonzero"
        elif positives and negatives:
            kind = "cancelled"
        elif positives or negatives:
            kind = "nonzero"
        else:
            kind = "all_zero"
        messages = {
            "cancelled": f"值为 0 是冲销结果：正向 {pos_sum} 与负向 {neg_sum} 相互抵消",
            "all_zero": "值为 0 是因为参与行的该列本身全为 0（存在数据，并非无数据）",
            "nonzero": f"当前值为 {value}",
        }
        return {
            "kind": kind,
            "value": value,
            "column": col,
            "message": messages[kind],
            "positive_sum": pos_sum,
            "negative_sum": neg_sum,
            "zero_value_rows": zeros,
            "entries": entries,
            "source_participation": self.source_participation(group_rid),
        }

    # ------------------------------------------------------------------ #
    def verify(self) -> dict[str, Any]:
        """Referential-integrity check of the whole evidence graph."""
        issues: list[str] = []
        known: set[str] = set()
        # tombstones are intentionally absent from source rows but legally
        # referenced by the source-stage retraction drops
        known.update(self.manifest.get("tombstones", []))
        total_edges = total_drops = 0
        for stage in self.stages:
            bundle = self._load(stage)
            for row in bundle.rows:
                if row.rid in known:
                    issues.append(f"重复 rid: {row.rid}")
                known.add(row.rid)
        for stage in self.stages:
            bundle = self._load(stage)
            for e in bundle.out_edges.values():
                for edge in e:
                    total_edges += 1
                    if edge["out"] not in known:
                        issues.append(f"{stage}: 边引用未知输出 {edge['out']}")
                    if edge["in"] not in known:
                        issues.append(f"{stage}: 边引用未知输入 {edge['in']}")
                    if edge.get("n", 1) < 1:
                        issues.append(f"{stage}: 边重数非法 {edge}")
            for d in bundle.drops:
                total_drops += 1
                if d["in"] not in known:
                    issues.append(f"{stage}: 丢弃记录引用未知行 {d['in']}")
                if not d.get("reason"):
                    issues.append(f"{stage}: 丢弃记录缺少原因 {d}")
        return {
            "ok": not issues,
            "issues": issues,
            "stages": len(self.stages),
            "edges": total_edges,
            "drops": total_drops,
            "known_rids": len(known),
        }
