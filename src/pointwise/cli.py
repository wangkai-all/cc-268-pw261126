"""Command line interface for the pointwise engine."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys

from .diff import diff_versions
from .engine import Store, _InjectedCrash, build
from .evidence import EvidenceStore


def _load_project(path: str):
    spec = importlib.util.spec_from_file_location("pw_project", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    project_dir = os.path.dirname(os.path.abspath(path))
    cwd = os.getcwd()

    def resolve(p: str) -> str:
        if os.path.isabs(p):
            return p
        for base in (cwd, project_dir):
            candidate = os.path.normpath(os.path.join(base, p))
            if os.path.exists(candidate):
                return candidate
        return os.path.normpath(os.path.join(cwd, p))

    sources = {k: resolve(p) for k, p in module.SOURCE_FILES.items()}
    return module.PIPELINE, sources


def _store(args) -> Store:
    return Store(args.store)


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pw", description="Pointwise 行级证据变换引擎")
    parser.add_argument("--store", default=os.environ.get("PW_STORE", "data/store"))
    parser.add_argument("--project", default="examples/pipeline.py")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("build", help="执行变换并发布新版本")
    p = sub.add_parser("crash-build", help="模拟在某阶段后崩溃（演示恢复）")
    p.add_argument("--after", required=True)
    sub.add_parser("recover", help="从中断处恢复构建（等价于重跑 build）")
    sub.add_parser("status", help="当前版本与证据完整性")
    sub.add_parser("versions", help="列出所有版本")
    p = sub.add_parser("release", help="把 current 切换到指定不可变版本")
    p.add_argument("version")
    p = sub.add_parser("gc", help="清理旧版本证据（保留 current 与最近 N 个）")
    p.add_argument("--keep", type=int, default=3)
    p = sub.add_parser("retract", help="撤销一条来源记录并重算")
    p.add_argument("rid")
    p = sub.add_parser("restore", help="恢复一条被撤销的来源记录并重算")
    p.add_argument("rid")

    p = sub.add_parser("rows", help="查看某阶段输出（支持筛选/分组）")
    p.add_argument("stage")
    p.add_argument("--filter-col"), p.add_argument("--filter-op", default="contains"),
    p.add_argument("--filter-val"), p.add_argument("--group-by")
    p = sub.add_parser("drops", help="查看被丢弃的行及原因")
    p.add_argument("--stage"), p.add_argument("--rule"), p.add_argument("--reason")
    p = sub.add_parser("expand", help="展开一行的直接输入与规则")
    p.add_argument("rid")
    p = sub.add_parser("paths", help="按需枚举到来源行的完整路径")
    p.add_argument("rid"), p.add_argument("--limit", type=int, default=500)
    p = sub.add_parser("zero", help="解释聚合零值：冲销 / 本零 / 无数据")
    p.add_argument("stage"), p.add_argument("rid"), p.add_argument("alias")
    p = sub.add_parser("verify", help="校验证据引用完整性")
    p = sub.add_parser("diff", help="对比两个版本")
    p.add_argument("frm"), p.add_argument("to")
    p = sub.add_parser("serve", help="启动网页")
    p.add_argument("--host", default="127.0.0.1"), p.add_argument("--port", type=int, default=8000)

    args = parser.parse_args(argv)
    config, sources = _load_project(args.project)
    store = _store(args)

    if args.cmd == "build":
        with store.lock():
            result = build(store, config, sources, note="手动构建")
        _print({"version": result.version_id, "recovered": result.recovered,
                "stages": f"{result.stages_completed}/{result.stages_total}",
                "verification": result.stats["verification"],
                "evidence": result.stats["evidence"],
                "zero_explanations": result.zero_explanations})
        return 0

    if args.cmd == "crash-build":
        with store.lock():
            try:
                build(store, config, sources, note="崩溃演练", crash_after_stage=args.after)
            except _InjectedCrash as exc:
                print(f"[模拟崩溃] {exc}；WAL 与部分阶段证据已落盘，current 未切换")
                return 0
        return 0

    if args.cmd == "recover":
        with store.lock():
            result = build(store, config, sources, note="恢复后构建", allow_resume=True)
        print(f"[恢复完成] recovered={result.recovered} version={result.version_id} "
              f"stages={result.stages_completed}/{result.stages_total}")
        return 0

    if args.cmd == "retract" or args.cmd == "restore":
        with store.lock():
            (store.retract if args.cmd == "retract" else store.restore)(args.rid)
            result = build(store, config, sources,
                           note=f"{'撤销' if args.cmd == 'retract' else '恢复'} {args.rid}")
        _print({"version": result.version_id, "tombstones": store.read_tombstones()})
        return 0

    if args.cmd == "status":
        ev = store.open_current()
        if ev is None:
            print("尚无已发布版本"); return 1
        with open(os.path.join(ev.version_dir, "stats.json"), encoding="utf-8") as fh:
            _print(json.load(fh))
        return 0

    if args.cmd == "versions":
        _print(store.versions()); return 0

    if args.cmd == "release":
        target = os.path.join(store.versions_dir, args.version)
        if not os.path.isdir(target):
            print(f"版本不存在: {args.version}"); return 2
        with store.lock():
            tmp = os.path.join(store.root, ".current.tmp")
            if os.path.lexists(tmp):
                os.unlink(tmp)
            os.symlink(os.path.join("versions", args.version), tmp)
            os.replace(tmp, store.current_link)
        print(f"current -> {args.version}")
        return 0

    if args.cmd == "gc":
        with store.lock():
            removed = store.gc(keep=args.keep)
        _print({"removed": removed}); return 0

    ev = store.open_current()
    if ev is None:
        print("尚无已发布版本，请先运行 build"); return 1

    if args.cmd == "rows":
        rows = ev.rows_of(args.stage)
        if args.filter_col:
            def keep(r):
                v = r.data.get(args.filter_col)
                q = args.filter_val
                op = args.filter_op
                if v is None:
                    return False
                if op == "contains":
                    return str(q).lower() in str(v).lower()
                if op == "==":
                    return str(v) == str(q)
                if op == "!=":
                    return str(v) != str(q)
                if op in (">", "<", ">=", "<="):
                    try:
                        lv, rv = float(v), float(q)
                    except (TypeError, ValueError):
                        return False
                    return {"<": lv < rv, ">": lv > rv,
                            "<=": lv <= rv, ">=": lv >= rv}[op]
                raise ValueError(f"unsupported filter op {op}")
            rows = [r for r in rows if keep(r)]
        out = [{"rid": r.rid, "data": r.data} for r in rows]
        if args.group_by:
            groups: dict[str, int] = {}
            for r in rows:
                key = str(r.data.get(args.group_by))
                groups[key] = groups.get(key, 0) + 1
            _print({"groups": dict(sorted(groups.items())), "rows": out})
        else:
            _print(out)
        return 0

    if args.cmd == "drops":
        stages = [args.stage] if args.stage else ev.stages
        out = []
        for s in stages:
            for d in ev.drops_of(s):
                if args.rule and args.rule not in d["rule"]:
                    continue
                if args.reason and args.reason not in d["reason"]:
                    continue
                out.append({"stage": s, **d})
        _print(out); return 0

    if args.cmd == "expand":
        _print(ev.expand(args.rid)); return 0
    if args.cmd == "paths":
        _print(ev.paths(args.rid, limit=args.limit)); return 0
    if args.cmd == "zero":
        _print(ev.explain_zero(args.stage, args.rid, args.alias)); return 0
    if args.cmd == "verify":
        _print(ev.verify()); return 0
    if args.cmd == "diff":
        _print(diff_versions(store.open_version(args.frm), store.open_version(args.to)))
        return 0
    if args.cmd == "serve":
        from .server import serve
        serve(store, config, sources, args.host, args.port)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
