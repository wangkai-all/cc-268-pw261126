"""Executable transformation primitives.

Each transform takes one or more input relations and returns an output
relation together with *edges* (out-rid -> in-rid, multiplicity kept) and
*drops* (input rids that did not pass, with a human-readable reason).

Only a small fixed vocabulary is implemented on purpose — this is not a
SQL parser.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .model import Row, Relation, group_rid, stage_rid


@dataclass
class Edge:
    """One lineage edge.  ``n`` records how many times ``in_rid``
    participated (fan-out / duplicate multiplicity)."""

    out_rid: str
    in_rid: str
    n: int = 1

    def to_json(self) -> dict[str, Any]:
        return {"out": self.out_rid, "in": self.in_rid, "n": self.n}


@dataclass
class Drop:
    in_rid: str
    rule: str
    reason: str

    def to_json(self) -> dict[str, Any]:
        return {"in": self.in_rid, "rule": self.rule, "reason": self.reason}


@dataclass
class TransformResult:
    relation: Relation
    edges: list[Edge] = field(default_factory=list)
    drops: list[Drop] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# filter
# --------------------------------------------------------------------------- #

_FILTER_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a is not None and b is not None and a > b,
    ">=": lambda a, b: a is not None and b is not None and a >= b,
    "<": lambda a, b: a is not None and b is not None and a < b,
    "<=": lambda a, b: a is not None and b is not None and a <= b,
    "contains": lambda a, b: b is not None and a is not None and b in a,
    "not_contains": lambda a, b: b is None or a is None or b not in a,
}


@dataclass
class Filter:
    stage: str
    input: str
    column: str
    op: str
    value: Any
    rule_id: str = ""

    def describe(self) -> str:
        return f"筛选：{self.column} {self.op} {self.value!r}（保留通过的行，其余逐条记录丢弃原因）"

    def run(self, inputs: dict[str, Relation]) -> TransformResult:
        rid = self.rule_id or f"{self.stage}/filter"
        pred = _FILTER_OPS[self.op]
        out = Relation(name=self.stage, columns=list(inputs[self.input].columns))
        edges: list[Edge] = []
        drops: list[Drop] = []
        seq = 0
        for row in inputs[self.input].rows:
            if pred(row.data.get(self.column), self.value):
                new_rid = stage_rid(self.stage, seq)
                seq += 1
                out.rows.append(Row(new_rid, dict(row.data)))
                edges.append(Edge(new_rid, row.rid))
            else:
                drops.append(
                    Drop(
                        row.rid,
                        rid,
                        f"{self.column}={row.data.get(self.column)!r} 不满足 {self.column} {self.op} {self.value!r}",
                    )
                )
        return TransformResult(out, edges, drops)


# --------------------------------------------------------------------------- #
# project (rename / keep columns; 1:1, never drops)
# --------------------------------------------------------------------------- #


@dataclass
class Project:
    stage: str
    input: str
    columns: dict[str, str]  # new_name -> old_name
    rule_id: str = ""

    def describe(self) -> str:
        return "投影：保留/重命名列 " + ", ".join(f"{old}→{new}" for new, old in self.columns.items())

    def run(self, inputs: dict[str, Relation]) -> TransformResult:
        rid = self.rule_id or f"{self.stage}/project"
        src = inputs[self.input]
        out = Relation(name=self.stage, columns=list(self.columns))
        edges: list[Edge] = []
        for seq, row in enumerate(src.rows):
            new_rid = stage_rid(self.stage, seq)
            out.rows.append(Row(new_rid, {new: row.data.get(old) for new, old in self.columns.items()}))
            edges.append(Edge(new_rid, row.rid))
        return TransformResult(out, edges, [])


# --------------------------------------------------------------------------- #
# dedupe (keeps first occurrence; every suppressed row is a drop with a reason)
# --------------------------------------------------------------------------- #


@dataclass
class Dedupe:
    stage: str
    input: str
    keys: list[str]
    rule_id: str = ""

    def describe(self) -> str:
        return f"去重：按 {', '.join(self.keys)} 保留首次出现，后续重复行记入丢弃清单"

    def run(self, inputs: dict[str, Relation]) -> TransformResult:
        rid = self.rule_id or f"{self.stage}/dedupe"
        src = inputs[self.input]
        out = Relation(name=self.stage, columns=list(src.columns))
        edges: list[Edge] = []
        drops: list[Drop] = []
        seen: dict[tuple, str] = {}
        seq = 0
        for row in src.rows:
            key = tuple(row.data.get(k) for k in self.keys)
            if key in seen:
                drops.append(Drop(row.rid, rid, f"键 {dict(zip(self.keys, key))} 与保留行 {seen[key]} 重复"))
            else:
                new_rid = stage_rid(self.stage, seq)
                seq += 1
                seen[key] = new_rid
                out.rows.append(Row(new_rid, dict(row.data)))
                edges.append(Edge(new_rid, row.rid))
        return TransformResult(out, edges, drops)


# --------------------------------------------------------------------------- #
# join (every match is its own output row + pair of edges — fan-out preserved)
# --------------------------------------------------------------------------- #


@dataclass
class Join:
    stage: str
    left: str
    right: str
    left_on: str
    right_on: str
    how: str = "inner"  # "inner" | "left"
    rule_id: str = ""

    def describe(self) -> str:
        kind = "内连接" if self.how == "inner" else "左连接"
        return (
            f"{kind}：{self.left}.{self.left_on} = {self.right}.{self.right_on}；"
            "每个匹配生成一行并同时连接左右两条来源（一对多会产生多行，参与次数不合并）"
        )

    def run(self, inputs: dict[str, Relation]) -> TransformResult:
        rid = self.rule_id or f"{self.stage}/join"
        left_rel, right_rel = inputs[self.left], inputs[self.right]
        index: dict[Any, list[Row]] = {}
        for r in right_rel.rows:
            index.setdefault(r.data.get(self.right_on), []).append(r)

        out_cols = list(left_rel.columns)
        for c in right_rel.columns:
            if c == self.right_on:
                continue
            out_cols.append(c if c not in left_rel.columns else f"r_{c}")

        out = Relation(name=self.stage, columns=out_cols)
        edges: list[Edge] = []
        drops: list[Drop] = []
        matched_right: set[str] = set()
        seq = 0
        for lrow in left_rel.rows:
            matches = index.get(lrow.data.get(self.left_on), [])
            if not matches:
                if self.how == "inner":
                    drops.append(
                        Drop(lrow.rid, rid, f"左行键 {lrow.data.get(self.left_on)!r} 在右表无匹配")
                    )
                    continue
                # left join: emit null-padded row, lineage only to the left
                new_rid = stage_rid(self.stage, seq)
                seq += 1
                data = dict(lrow.data)
                for c in out_cols:
                    data.setdefault(c, None)
                out.rows.append(Row(new_rid, data))
                edges.append(Edge(new_rid, lrow.rid))
                drops.append(
                    Drop(lrow.rid, rid, f"左行键 {lrow.data.get(self.left_on)!r} 在右表无匹配（左连接补空保留）")
                )
                continue
            for rrow in matches:
                new_rid = stage_rid(self.stage, seq)
                seq += 1
                data = dict(lrow.data)
                for c in right_rel.columns:
                    if c == self.right_on:
                        continue
                    target = c if c not in lrow.data else f"r_{c}"
                    data[target] = rrow.data.get(c)
                out.rows.append(Row(new_rid, data))
                # two edges per match — both sides are evidence, multiplicity intact
                edges.append(Edge(new_rid, lrow.rid))
                edges.append(Edge(new_rid, rrow.rid))
                matched_right.add(rrow.rid)
        if self.how == "inner":
            for rrow in right_rel.rows:
                if rrow.rid not in matched_right:
                    drops.append(
                        Drop(rrow.rid, rid, f"右行键 {rrow.data.get(self.right_on)!r} 在左表无匹配")
                    )
        return TransformResult(out, edges, drops)


# --------------------------------------------------------------------------- #
# group + aggregate
# --------------------------------------------------------------------------- #


@dataclass
class AggSpec:
    alias: str
    func: str  # sum | count | count_distinct | min | max | avg
    column: str | None = None  # None only for count(*)


@dataclass
class GroupAggregate:
    stage: str
    input: str
    keys: list[str]
    aggregates: list[AggSpec]
    rule_id: str = ""

    def describe(self) -> str:
        aggs = ", ".join(
            f"{a.func}({a.column or '*'})→{a.alias}" for a in self.aggregates
        )
        return f"分组汇总：按 {', '.join(self.keys) or '（全表）'} 分组，计算 {aggs}"

    def run(self, inputs: dict[str, Relation]) -> TransformResult:
        rid = self.rule_id or f"{self.stage}/group"
        src = inputs[self.input]
        out_cols = list(self.keys) + [a.alias for a in self.aggregates]
        groups: dict[tuple, list[Row]] = {}
        for row in src.rows:
            key = tuple(row.data.get(k) for k in self.keys)
            groups.setdefault(key, []).append(row)

        out = Relation(name=self.stage, columns=out_cols)
        edges: list[Edge] = []
        for key in sorted(groups, key=lambda k: repr(k)):
            rows = groups[key]
            new_rid = group_rid(self.stage, key)
            data = {k: v for k, v in zip(self.keys, key)}
            for spec in self.aggregates:
                data[spec.alias] = _apply_agg(spec, rows)
            out.rows.append(Row(new_rid, data))
            # one edge per DISTINCT input rid, n = its occurrence count —
            # the same rid contributing k times is never collapsed to 1
            counts: dict[str, int] = {}
            for r in rows:
                counts[r.rid] = counts.get(r.rid, 0) + 1
            for in_rid, n in sorted(counts.items()):
                edges.append(Edge(new_rid, in_rid, n))
        return TransformResult(out, edges, [])


def _apply_agg(spec: AggSpec, rows: list[Row]) -> Any:
    if spec.func == "count":
        if spec.column is None:
            return len(rows)
        return sum(1 for r in rows if r.data.get(spec.column) is not None)
    vals = [r.data.get(spec.column) for r in rows]
    vals = [v for v in vals if v is not None]
    if spec.func == "count_distinct":
        return len(set(vals))
    if not vals:
        return None
    if spec.func == "sum":
        return sum(vals)
    if spec.func == "min":
        return min(vals)
    if spec.func == "max":
        return max(vals)
    if spec.func == "avg":
        return sum(vals) / len(vals)
    raise ValueError(f"unknown aggregate {spec.func}")
