"""File helpers: source loading, atomic writes, fsync and checksums."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import tempfile
from typing import Any

from .model import Row, make_rid


def load_source(path: str, name: str) -> Row:
    """Load a CSV or JSONL file as a source relation (rows keep order)."""
    lower = path.lower()
    if lower.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as fh:
            records = [json.loads(line) for line in fh if line.strip()]
    elif lower.endswith(".csv"):
        with open(path, "r", encoding="utf-8", newline="") as fh:
            records = list(csv.DictReader(fh))
    else:
        raise ValueError(f"unsupported source format: {path} (need .csv/.jsonl)")
    columns = list(records[0].keys()) if records else []
    rows = [Row(make_rid(name, i), _coerce(rec)) for i, rec in enumerate(records)]
    return rows, columns


def _coerce(record: dict[str, str]) -> dict[str, Any]:
    """Best-effort numeric coercion for CSV strings; JSONL passes through."""
    out: dict[str, Any] = {}
    for k, v in record.items():
        out[k] = _scalar(v)
    return out


def _scalar(v: Any) -> Any:
    if not isinstance(v, str):
        return v
    if v == "":
        return None
    try:
        iv = int(v)
        return iv
    except ValueError:
        pass
    try:
        fv = float(v)
        return fv
    except ValueError:
        pass
    return v


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_text(path: str, text: str) -> None:
    """Write + fsync into a sibling temp file then os.replace (crash-safe)."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_jsonl(path: str, records: list[dict[str, Any]]) -> None:
    buf = io.StringIO()
    for rec in records:
        buf.write(json.dumps(rec, ensure_ascii=False, default=str))
        buf.write("\n")
    atomic_write_text(path, buf.getvalue())


def write_json(path: str, obj: Any) -> None:
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
