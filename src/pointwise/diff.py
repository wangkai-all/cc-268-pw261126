"""Diff two released versions stage by stage.

Aggregate groups use key-hash rids that stay stable across versions, so
groups are matched directly by rid. Positional stage rows (filter/project/
dedupe/join outputs, rids like ``stage#3``) are matched by *content hash*
instead, so deleting a row in the middle reads as a removal rather than as
a change to every row that shifted position. Every difference is expressed
in terms of source rids.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .evidence import EvidenceStore
from .model import content_hash


def _is_group_rid(rid: str) -> bool:
    return "#g" in rid


@dataclass
class StageDiff:
    stage: str
    added: list[dict[str, Any]]
    removed: list[dict[str, Any]]
    changed: list[dict[str, Any]]

    def to_json(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "added": self.added,
            "removed": self.removed,
            "changed": self.changed,
        }


def _entry(row, store: EvidenceStore) -> dict[str, Any]:
    return {"rid": row.rid, "data": row.data,
            "sources": store.source_participation(row.rid)}


def diff_versions(old: EvidenceStore, new: EvidenceStore) -> dict[str, Any]:
    stages = list(new.manifest.get("transform_stages", new.stages))
    stage_diffs: list[StageDiff] = []
    for stage in stages:
        old_rows = {r.rid: r for r in old.rows_of(stage)} if stage in old.stages else {}
        new_rows = {r.rid: r for r in new.rows_of(stage)}
        added, removed, changed = [], [], []

        if any(_is_group_rid(r) for r in (list(old_rows) + list(new_rows))):
            # aggregate stage: stable key-hash rids -> direct rid matching
            for rid, row in new_rows.items():
                if rid not in old_rows:
                    added.append(_entry(row, new))
                elif content_hash(old_rows[rid].data) != content_hash(row.data):
                    changed.append({
                        "rid": rid,
                        "before": old_rows[rid].data,
                        "after": row.data,
                        "sources_before": old.source_participation(rid),
                        "sources_after": new.source_participation(rid),
                    })
            for rid, row in old_rows.items():
                if rid not in new_rows:
                    removed.append(_entry(row, old))
        else:
            # positional stage: match by content, multiplicity-aware.
            # Duplicate content rows are kept as separate entries.
            old_bag: dict[str, list] = {}
            new_bag: dict[str, list] = {}
            for r in old_rows.values():
                old_bag.setdefault(content_hash(r.data), []).append(r)
            for r in new_rows.values():
                new_bag.setdefault(content_hash(r.data), []).append(r)
            for h, rows in new_bag.items():
                old_h = old_bag.get(h, [])
                if len(rows) > len(old_h):
                    for r in rows[len(old_h):]:
                        added.append(_entry(r, new))
            for h, rows in old_bag.items():
                new_h = new_bag.get(h, [])
                if len(rows) > len(new_h):
                    for r in rows[len(new_h):]:
                        removed.append(_entry(r, old))
        stage_diffs.append(StageDiff(stage, added, removed, changed))
    return {
        "old_version": old.manifest["version"],
        "new_version": new.manifest["version"],
        "tombstones_added": sorted(
            set(new.manifest.get("tombstones", [])) - set(old.manifest.get("tombstones", []))
        ),
        "tombstones_removed": sorted(
            set(old.manifest.get("tombstones", [])) - set(new.manifest.get("tombstones", []))
        ),
        "stages": [d.to_json() for d in stage_diffs],
        "summary": {
            "added": sum(len(d.added) for d in stage_diffs),
            "removed": sum(len(d.removed) for d in stage_diffs),
            "changed": sum(len(d.changed) for d in stage_diffs),
        },
    }
