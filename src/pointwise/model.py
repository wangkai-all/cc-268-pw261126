"""Core data model: row ids, rows and relations."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


def make_rid(source: str, seq: int) -> str:
    """A source row id, e.g. ``orders#0``."""
    return f"{source}#{seq}"


def stage_of_rid(rid: str) -> str:
    """Stage/source name prefix; the suffix may be a seq or a group hash."""
    return rid.rsplit("#", 1)[0]


def parse_rid(rid: str) -> tuple[str, int]:
    source, seq = rid.rsplit("#", 1)
    return source, int(seq)


def stage_rid(stage: str, seq: int) -> str:
    return f"{stage}#{seq}"


def group_rid(stage: str, key: tuple[Any, ...]) -> str:
    """Stable aggregate-group rid derived from the group key.

    Hash-based (not positional) so the same business key keeps the same
    rid across versions even when other groups appear or disappear.
    """
    digest = hashlib.sha1(
        json.dumps(key, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    ).hexdigest()[:12]
    return f"{stage}#g{digest}"


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def content_hash(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


@dataclass
class Row:
    rid: str
    data: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {"rid": self.rid, "data": self.data}


@dataclass
class Relation:
    """A named bag of rows with an ordered column list.

    ``rows`` is a list (order is deterministic) and may contain duplicate
    business values — multiplicity is always preserved.
    """

    name: str
    rows: list[Row] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    kind: str = "stage"  # "source" | "stage"

    def row_by_rid(self) -> dict[str, Row]:
        return {r.rid: r for r in self.rows}

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "columns": list(self.columns),
            "rows": [r.to_json() for r in self.rows],
        }
