"""Validate incremental A/B evidence; never pool separately reported percentiles."""
import csv
import json
from pathlib import Path
from statistics import median

from run import COUNTS, parse
from hash_move_summary import LATENCIES

ROOT = Path(__file__).resolve().parent
STAGES = {"read": ("C", "move", "direct"),
          "reserve": ("A", "direct", "reserve")}
NAMES = {"move": "前版：已消除复制", "direct": "专用解码", "reserve": "专用解码＋参数预分配"}


def report_lines():
    lines = ["", "## 增量优化：专用全量解码与写参数预分配", "",
             "两项分开归因：第一项用 C 比较上一轮的消除复制版与专用解码版；第三项用 A（50% READ / 50% UPDATE）比较专用解码版与再加参数预分配版。第二组不是与原 main 比较。", "",
             "均为 256 连接、Uniform、原有 100M key 范围、10 × 128B。每组独立 1M 预热＋5M 测量，100K 为全客户端合计目标，不限速不设 target。每轮基线→候选版，交替三轮；每版重启，GC/defrag、客户端 Prometheus/Grafana 保持开启。A 用 HMSET 更新全部字段，无 INSERT，无 reload；A 的读写量分别按实际成功计数统计。", "",
             "所有延迟来自 YCSB operation；Intended-operation 另列于 CSV。汇总是三轮各指标的中位数，不是合并请求得到的百分位；各列独立取中位数，因此读写 QPS 中位数之和不一定等于总 QPS 中位数，逐轮计数则可加和。测量 JVM 独立启动，包含自身 JIT/GC；短期调度、缓存和后台回收波动仍可能影响结果。基线总在每轮先运行，不能据此声称排除了顺序效应。", "",
             "实现保持拥有型返回值、完整编码校验、内存 admission、磁盘格式和 mutex 不变。专用路径只处理 compact HGETALL/HKEYS/HVALS（包括共用 Set 路径），grouped 继续原路径。HSET/HMSET/HREPLACE 的参数仍为请求持有的 string_view；预分配仅避免 vector 扩容，不改写入算法或整值读写方式。普通 SET/GET 代码未改，本轮没有单独测试其性能。", "",
             "局部分配计数（不是服务端总分配或耗时）：10×128B 全读由 12 次/2810B 降至 11 次/2090B，少一个 HashEntry 数组及 10 个 field digest 计算；两组写参数数组由 10 次/992B 降至 2 次/320B。证据：[计数输出](verified-update-matrix/hash-direct-allocations.log)、[计数程序](verified-update-matrix/hash_direct_allocations.cpp)。"]
    all_rows = []
    stage_count = 0
    for stage, (workload, baseline, candidate) in STAGES.items():
        prefix = f"hash-direct-{stage}"
        path = ROOT / f"{prefix}-results.json"
        if not path.exists():
            continue
        stage_count += 1
        cells = json.loads(path.read_text())
        env = json.loads((ROOT / f"{prefix}-environment.json").read_text())
        assert (env["baseline"], env["candidate"], env["workload"]) == (baseline, candidate, workload)
        if stage == "reserve":
            read_env = json.loads((ROOT / "hash-direct-read-environment.json").read_text())
            assert env["binaries"]["direct"] == read_env["binaries"]["direct"]
        rows, seen = [], set()
        for cell in cells:
            identity = (cell["round"], cell["variant"], cell["target_ops_sec"])
            assert identity not in seen
            seen.add(identity)
            assert cell["binary_sha256"] == env["binaries"][cell["variant"]]["sha256"]
            assert cell["workers"] == 256 and cell["workload"] == workload
            suffix = f"-target{cell['target_ops_sec']}" if cell["target_ops_sec"] else ""
            stem = f"{cell['label']}-hmset-{workload.lower()}-c256{suffix}"
            for phase in ("warmup", "measured"):
                recorded = json.loads((ROOT / f"{stem}-{phase}.json").read_text())
                checked = parse(ROOT / f"{stem}-{phase}.log", COUNTS[phase])
                assert all(recorded[k] == v for k, v in checked.items())
                assert recorded["exit_code"] == 0 and recorded["failed"] == 0
                if phase == "measured":
                    assert all(cell[k] == v for k, v in recorded.items())
            ops = ("READ",) if workload == "C" else ("READ", "UPDATE")
            assert sum(cell["metrics"][op]["Operations"] for op in ops) == COUNTS["measured"]
            for op in ops:
                for histogram in (op, "Intended-" + op):
                    v = cell["metrics"][histogram]
                    assert v["Operations"] == cell["metrics"][op]["Return=OK"]
                    rows.append({"stage": stage, "workload": workload, "round": cell["round"],
                                 "variant": cell["variant"], "target": cell["target_ops_sec"],
                                 "workers": 256, "histogram": histogram, "total_qps": cell["success_qps"],
                                 "op_qps": v["Operations"] * 1000 / cell["runtime_ms"],
                                 "count": v["Operations"], "failed": cell["failed"],
                                 **{label: v[key] for label, key in LATENCIES.items()},
                                 "log": f"{stem}-measured.log", "sha256": cell["sha256"],
                                 "binary_sha256": cell["binary_sha256"]})
        if len(cells) == 12:
            assert seen == {(r, v, t) for r in (1, 2, 3) for v in (baseline, candidate) for t in (100000, 0)}
        all_rows += rows
        ordinary = [r for r in rows if not r["histogram"].startswith("Intended-")]
        lines += ["", f"### {'第一项：C 专用解码' if stage == 'read' else '第三项：A 写参数预分配'}", "",
                  f"完成 {len(cells)}/12 个测量单元；[二进制 SHA256、源码差异及环境](verified-update-matrix/{prefix}-environment.json)，[原始结果](verified-update-matrix/{prefix}-results.json)。", "",
                  "| 限速 | 版本 | 操作 | 轮数 | 总 QPS | 操作 QPS | 平均 ms | p99 ms | p999 ms | p9999 ms |",
                  "|---|---|---|---:|---:|---:|---:|---:|---:|---:|"]
        for target in (100000, 0):
            for variant in (baseline, candidate):
                for op in (("READ",) if workload == "C" else ("READ", "UPDATE")):
                    group = [r for r in ordinary if r["target"] == target and r["variant"] == variant and r["histogram"] == op]
                    if not group:
                        continue
                    nums = [f"{median(r[f] for r in group):,.0f}" for f in ("total_qps", "op_qps")]
                    nums += [f"{median(r[f] for r in group)/1000:.3f}" for f in ("avg_us", "p99_us", "p999_us", "p9999_us")]
                    lines.append(f"| {'100K' if target else '不限速'} | {NAMES[variant]} | {op} | {len(group)} | " + " | ".join(nums) + " |")
        lines += ["", "逐轮记录（单位 ms）：", "",
                  "| 轮次 | 限速 | 版本 | 操作 | 操作 QPS | 平均 | p99 | p999 | p9999 | 失败 |",
                  "|---:|---|---|---|---:|---:|---:|---:|---:|---:|"]
        for r in ordinary:
            nums = [f"{r['op_qps']:,.0f}"]
            nums += [f"{r[f]/1000:.3f}" for f in ("avg_us", "p99_us", "p999_us", "p9999_us")]
            lines.append(f"| {r['round']} | {'100K' if r['target'] else '不限速'} | {NAMES[r['variant']]} | {r['histogram']} | " + " | ".join(nums) + f" | {r['failed']} |")
        if len(cells) == 12:
            unlimited = {(c["round"], c["variant"]): c for c in cells if c["target_ops_sec"] == 0}
            changes = [unlimited[(r, candidate)]["success_qps"] / unlimited[(r, baseline)]["success_qps"] - 1 for r in (1, 2, 3)]
            ratio = median(unlimited[(r, candidate)]["success_qps"] for r in (1, 2, 3)) / median(unlimited[(r, baseline)]["success_qps"] for r in (1, 2, 3)) - 1
            lines += ["", f"不限速总 QPS 三轮中位数变化 {ratio:+.2%}；逐轮配对变化为 "
                      + "、".join(f"{v:+.2%}" for v in changes) + "。分配减少是确定的代码变化，吞吐和极端长尾的稳定收益仍需按逐轮一致性判断，不能只挑最好的一轮。"]
            op = "READ" if stage == "read" else "UPDATE"
            tails = {v: median(r["p9999_us"] for r in ordinary
                               if r["target"] == 0 and r["variant"] == v and r["histogram"] == op)
                     for v in (baseline, candidate)}
            lines += ["", f"本轮不限速 {op} 的 p9999 中位数为 {tails[baseline]/1000:.3f} → {tails[candidate]/1000:.3f} ms；"
                      "不能把这项改动称为极端长尾优化。100K 下也保留了所有尖峰：只读前版有 38.719 ms，A 的预分配版 UPDATE 有 49.983 ms。它们的来源尚未归因，本实验不证明分配就是主要瓶颈。"]
        print(f"validated {prefix}: {len(cells)}/12 cells, {len(rows)} histogram rows")
    if not stage_count:
        return []
    for name, count in (("hash-direct-unit-tests.log", 65),
                        ("hash-direct-functional-tests.log", 6),
                        ("hash-direct-oom-tests.log", 3)):
        assert f"[  PASSED  ] {count} tests." in (ROOT / name).read_text()
    with (ROOT / "hash-direct-summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(all_rows[0]))
        writer.writeheader()
        writer.writerows(all_rows)
    lines += ["", "[全部百分位与 Intended 指标 CSV](verified-update-matrix/hash-direct-summary.csv)；[交替运行脚本](verified-update-matrix/hash_direct_ab.py)。", "",
              "正确性：专用解码版通过 65 项 codec/旁表单测及 9 项端到端回归，覆盖二进制/空 field/value、compact/grouped 全量读、WATCH/EXEC、写入与重启、超大值、Set 共用路径和真实 OOM。", "",
              "[单测](verified-update-matrix/hash-direct-unit-tests.log)、[功能回归](verified-update-matrix/hash-direct-functional-tests.log)、[OOM 回归](verified-update-matrix/hash-direct-oom-tests.log)。", ""]
    if (ROOT / "hash-reserve-functional-tests.log").exists():
        for name, count in (("hash-reserve-functional-tests.log", 6),
                            ("hash-reserve-oom-tests.log", 3)):
            assert f"[  PASSED  ] {count} tests." in (ROOT / name).read_text()
        lines += ["参数预分配版再次通过相同的 9 项端到端回归：[功能](verified-update-matrix/hash-reserve-functional-tests.log)、[OOM](verified-update-matrix/hash-reserve-oom-tests.log)。", ""]
    return lines
