# Pointwise — 带行级证据的数据变换引擎

一个从零构建、纯 Python 标准库实现的可执行数据变换系统。每个结果行都能点开，
看清**由哪些原始行、经过哪些规则**产生；支持筛选、关联、分组、撤销一条来源记录
并重算；所有发布都是不可变版本，证据与结果同生共死，不会出现"结果新、证据旧"。

## 它解决的问题

| 要求 | 本项目的做法 |
|---|---|
| 点开结果行知道由哪些原始行和规则产生 | 每阶段存紧凑边 `out→in`（含重数 n）和规则说明，按需递归展开到来源行 |
| 关联重复/多路汇总不能抹掉同一来源的参与次数 | join 每个匹配一条边（一对多扇出多行）；聚合边带 `n`；路径重数沿链相乘 |
| 聚合为零要解释是冲销还是本来没数据 | 三态区分：`cancelled`（正负冲销）/ `all_zero`（参与行本身为零）/ `no_group`（无此分组） |
| 全量存路径爆空间，摘要又怕漏 | 只物化紧凑边，全路径查询时按需展开；stats 同时给出实测边字节数与全路径估算字节数 |
| 来源删除、结果重算、证据清理并行时保持一致 | 不可变版本目录 + `os.replace` 原子切换 current 符号链接 + `flock` 写锁；读者一次请求只读一个版本 |
| 重建中断后恢复 | WAL 放在版本目录内，逐阶段 checksum checkpoint；显式恢复时按 content_id 采纳残留尝试，checksum 损坏则整盘丢弃重建 |

## 快速开始

```bash
# 无需安装依赖（Python 3.10+，仅标准库）
./pw build                 # 执行 examples 里的 5 阶段管道并发布
./pw status                # 看证据规模、零值解释、完整性校验
./pw rows region_summary   # 看末端聚合结果
./pw drops                 # 每条被丢弃/未匹配/去重的行及原因
./pw expand 'region_summary#g8b45…'   # 展开一行的直接输入与规则
./pw paths 'joined#0'                  # 枚举到来源行的完整路径
./pw zero region_summary '<gid>' total_amount   # 解释零值
./pw serve --port 8000     # 真实网页：筛选/分组/展开/撤销/版本对比
```

### 撤销与版本

```bash
./pw retract orders#3      # 墓碑撤销一条来源行 → 原子发布新版本（旧版保留）
./pw versions              # 列出所有不可变版本
./pw diff <old> <new>      # 按内容哈希/稳定分组 rid 对比两版
./pw release <version>     # 把 current 切回任意历史版本
./pw restore orders#3      # 恢复撤销并重算
./pw gc --keep 3           # 清理旧版本（current 永不删除）
```

### 崩溃恢复演练

```bash
./pw crash-build --after dedup_orders   # 模拟在该阶段落盘后崩溃（current 不动）
./pw recover                            # 从 checkpoint 续建，输出 recovered=True
```

## 五个真实可执行的变换原语

不是 SQL 解析器，而是一组固定、可执行、各自带证据语义的原语（`src/pointwise/transforms.py`）：

1. **filter** — 不通过的行逐条进 drops，带规则 id 与自然语言原因；
2. **project** — 列保留/重命名，1:1，不丢行；
3. **dedupe** — 按业务键保留首次出现，重复行进 drops 并指明与哪个保留行重复；
4. **join**（inner/left）— 每个匹配产生一行、同时连左右两条边；一对多自然扇出，参与次数不合并；
5. **group + aggregate**（sum/count/count_distinct/min/max/avg）— 分组 rid 由分组键哈希得到，跨版本稳定；同一 rid 参与 k 次记为边 `n=k`。

## 证据存储方案与空间取舍

每个版本目录 `data/store/versions/<vid>/`：

```
<source>/  rows.jsonl + drops.jsonl(墓碑撤销) + rules.json
<stage>/   rows.jsonl + edges.jsonl + drops.jsonl + rules.json
manifest.json   阶段拓扑、来源哈希、墓碑、聚合定义
stats.json      证据实测大小、全路径估算、零值解释、完整性校验
wal.json        仅构建中存在，发布后删除
```

- **物化的是边不是路径**：一条边约几十字节，与扇出深度无关；全路径在查询时沿边递归。
- **空间数字实测**：`./pw status` 的 `evidence` 段同时报告 `edge_bytes`（实际占用）、
  `materialised_path_entries`（所有输出行的来源参与条目总数，重数相乘后的精确值）、
  `estimated_full_path_bytes`（若全量物化路径的估算）与节省比例。
- **防漏机制**：发布前 `verify()` 对每条边/丢弃记录做引用完整性校验，断链即不通过；
  网页和 stats 都展示校验结果。

## 一致性模型（为什么不会"结果新、证据旧"）

- 版本目录**不可变**：一次构建的结果行、边、丢弃、规则写在同一个新目录里，写完才发布。
- 发布是**一次** `os.replace` 原子替换 `current` 符号链接，不存在中间态。
- 读者（HTTP 每请求）只解析一次 current，拿到一个版本目录后所有查询都落在该目录。
- 写者（撤销/重算/GC）由一把 `flock` 串行化；GC 只删非 current 的旧版本。
- 测试 `tests/test_engine.py::ConcurrencyTests` 在 6 路并发写 + 持续读取（5000+ 次校验）下验证零断链。

## 测试

```bash
python3 -m unittest discover -s tests -v   # 36 个测试，覆盖原语/证据/版本/恢复/并发/HTTP
```

## 目录结构

```
src/pointwise/
  model.py       行/关系/稳定 rid
  transforms.py  五个变换原语（边 + 丢弃记录）
  evidence.py    证据读取、按需展开、路径枚举、零值解释、完整性校验
  engine.py      执行、WAL/checkpoint、恢复、原子发布、证据统计、GC
  diff.py        版本对比（位置型按内容哈希，聚合按稳定 rid）
  server.py      标准库 HTTP API
  web/           原生 HTML/CSS/JS 网页（无构建、无外部依赖）
examples/        演示数据（含冲销、零值行、孤儿键、重复行）与管道定义
tests/           unittest 测试套件
```
