"""Declarative pipeline definitions -> executable transform objects."""
from __future__ import annotations

from typing import Any

from .model import Relation, canonical_json
from .transforms import AggSpec, Dedupe, Filter, GroupAggregate, Join, Project


def build_transform(spec: dict[str, Any]):
    t = spec["type"]
    if t == "filter":
        return Filter(
            stage=spec["stage"], input=spec["input"], column=spec["column"],
            op=spec["op"], value=spec.get("value"),
            rule_id=f"{spec['stage']}/filter",
        )
    if t == "project":
        return Project(
            stage=spec["stage"], input=spec["input"], columns=spec["columns"],
            rule_id=f"{spec['stage']}/project",
        )
    if t == "dedupe":
        return Dedupe(
            stage=spec["stage"], input=spec["input"], keys=spec["keys"],
            rule_id=f"{spec['stage']}/dedupe",
        )
    if t == "join":
        return Join(
            stage=spec["stage"], left=spec["left"], right=spec["right"],
            left_on=spec["left_on"], right_on=spec["right_on"],
            how=spec.get("how", "inner"), rule_id=f"{spec['stage']}/join",
        )
    if t == "group":
        return GroupAggregate(
            stage=spec["stage"], input=spec["input"], keys=spec.get("keys", []),
            aggregates=[AggSpec(**a) for a in spec["aggregates"]],
            rule_id=f"{spec['stage']}/group",
        )
    raise ValueError(f"unknown transform type: {t}")


def stage_inputs(spec: dict[str, Any]) -> list[str]:
    t = spec["type"]
    if t in ("filter", "project", "dedupe", "group"):
        return [spec["input"]]
    if t == "join":
        return [spec["left"], spec["right"]]
    raise ValueError(t)


def source_relation(name: str, rows, columns: list[str]) -> Relation:
    return Relation(name=name, rows=rows, columns=list(columns), kind="source")


def pipeline_fingerprint(config: dict[str, Any]) -> str:
    """Stable hash of pipeline definition (sources excluded; they hash separately)."""
    return canonical_json(config["stages"])
