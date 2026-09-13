"""Validate each A/B log, preserve all runs, and summarize per-run medians."""
import csv
import json
from pathlib import Path
from statistics import median

from run import parse, COUNTS

ROOT = Path(__file__).resolve().parent
LATENCIES = {
    "avg_us": "AverageLatency(us)", "p50_us": "50thPercentileLatency(us)",
    "p95_us": "95thPercentileLatency(us)", "p99_us": "99thPercentileLatency(us)",
    "p999_us": "99.9PercentileLatency(us)", "p9999_us": "99.99PercentileLatency(us)",
    "max_us": "MaxLatency(us)",
}


def report_lines():
    path = ROOT / "hash-move-ab-results.json"
    if not path.exists():
        return []
    cells = json.loads(path.read_text())
    environment = json.loads((ROOT / "hash-move-ab-environment.json").read_text())
    rows = []
    seen = set()
    for cell in cells:
        identity = (cell["round"], cell["variant"], cell["target_ops_sec"])
        assert identity not in seen
        seen.add(identity)
        suffix = f"-target{cell['target_ops_sec']}" if cell["target_ops_sec"] else ""
        stem = f"{cell['label']}-hmset-c-c256{suffix}"
        for phase in ("warmup", "measured"):
            recorded = json.loads((ROOT / f"{stem}-{phase}.json").read_text())
            checked = parse(ROOT / f"{stem}-{phase}.log", COUNTS[phase])
            assert all(recorded[k] == v for k, v in checked.items())
            assert recorded["exit_code"] == 0 and recorded["failed"] == 0
            if phase == "measured":
                assert all(cell[k] == v for k, v in recorded.items())
        assert cell["binary_sha256"] == environment["binaries"][cell["variant"]]["sha256"]
        for histogram in ("READ", "Intended-READ"):
            v = cell["metrics"][histogram]
            assert v["Operations"] == COUNTS["measured"]
            rows.append({
                "round": cell["round"], "variant": cell["variant"],
                "target": cell["target_ops_sec"], "workers": cell["workers"],
                "histogram": histogram, "qps": cell["success_qps"],
                "count": v["Operations"], "failed": cell["failed"],
                **{label: v[key] for label, key in LATENCIES.items()},
                "log": f"{stem}-measured.log", "sha256": cell["sha256"],
                "binary_sha256": cell["binary_sha256"],
            })
    with (ROOT / "hash-move-ab-summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = ["", "## Hash 全量读取：消除第二次复制的 A/B 复测", "",
             f"完成 {len(cells)}/12 组测量。256 连接，Workload C，Uniform，原有 100M key、10 × 128B；每组 1M 预热 + 5M 测量，GC/defrag 和客户端监控保持开启。仅 READ，没有重新 load 或写入。", "",
             "基线是上述 main commit 的原二进制；修改版只改变 Hash/Set 共用的全量读取结果组装：移动已经解码的字符串，并按实际 capacity 预留内存。未改变磁盘格式、Hash 写路径、普通 SET/GET 或 mutex。两版二进制 SHA256 和完整源码差异见 [A/B 环境证据](verified-update-matrix/hash-move-ab-environment.json)。", "",
             "每轮依次运行基线和修改版；每版重新启动后分别运行 100K 限速、不限速，两档各有独立预热。共交替三轮，测量期间没有编译或正确性测试。测量 JVM 与预热 JVM 分开，仍包含测量 JVM 自身的 JIT/GC；有限时长结果不等于稳定容量上限。", "",
             "### 三轮汇总", "",
             "以下每个数是三轮对应指标的中位数；百分位不是将三轮请求合并后计算的百分位。延迟全部为 YCSB READ operation，单位 ms。Intended-READ 另存 CSV，不混入本表。", "",
             "| 限速 | 版本 | 轮数 | QPS | 平均 ms | p99 ms | p999 ms | p9999 ms |",
             "|---|---|---:|---:|---:|---:|---:|---:|"]
    ordinary = [r for r in rows if r["histogram"] == "READ"]
    for target in (100000, 0):
        for variant in ("baseline", "move"):
            group = [r for r in ordinary if r["target"] == target and r["variant"] == variant]
            if not group:
                continue
            nums = [f"{median(r['qps'] for r in group):,.0f}"]
            nums += [f"{median(r[field] for r in group)/1000:.3f}"
                     for field in ("avg_us", "p99_us", "p999_us", "p9999_us")]
            lines.append(f"| {'100K' if target else '不限速'} | {'基线' if variant == 'baseline' else '消除复制'} | {len(group)} | " + " | ".join(nums) + " |")
    lines += ["", "### 逐轮结果", "",
              "| 轮次 | 限速 | 版本 | QPS | 平均 ms | p99 ms | p999 ms | p9999 ms | 失败 |",
              "|---:|---|---|---:|---:|---:|---:|---:|---:|"]
    for r in ordinary:
        nums = [f"{r['qps']:,.0f}"]
        nums += [f"{r[field]/1000:.3f}" for field in ("avg_us", "p99_us", "p999_us", "p9999_us")]
        lines.append(f"| {r['round']} | {'100K' if r['target'] else '不限速'} | {'基线' if r['variant'] == 'baseline' else '消除复制'} | " + " | ".join(nums) + f" | {r['failed']} |")
    if len(cells) == 12:
        expected = {(round_number, variant, target) for round_number in (1, 2, 3)
                    for variant in ("baseline", "move") for target in (100000, 0)}
        assert seen == expected
        unlimited = {variant: [r for r in ordinary if r["target"] == 0 and r["variant"] == variant]
                     for variant in ("baseline", "move")}
        ratio = median(r["qps"] for r in unlimited["move"]) / median(r["qps"] for r in unlimited["baseline"]) - 1
        changes = []
        for round_number in (1, 2, 3):
            pair = {variant: next(r for r in unlimited[variant] if r["round"] == round_number)
                    for variant in unlimited}
            changes.append(pair["move"]["qps"] / pair["baseline"]["qps"] - 1)
        lines += ["", "### 结论与正确性核验", "",
                  f"不限速 QPS 的三轮中位数变化为 {ratio:+.2%}，逐轮配对变化为 "
                  + "、".join(f"{change:+.2%}" for change in changes) + "。中位数长尾较低，但逐轮存在反向波动；当前样本不足以把中位数改善认定为稳定收益。100K 限速下吞吐受目标速率约束，延迟变化较小。", "",
                  "本轮未修改的基线在 100K 下 p9999 为 2.585 / 5.383 / 2.679 ms，上轮单次测量为 25.599 ms。不能把跨轮环境/JVM/调度波动全部归功于消除复制，也不能据此宣称极端尖峰已解决。", "",
                  "正确性最终通过 9 项针对性测试：compact/grouped 全量重复读、空/二进制/长字段、WATCH/EXEC、原有 Hash 写入与重启恢复、超大字段和共用 Set 路径，以及真实内存限制下的 OOM。普通 SET/GET 的功能检查通过，但本次未另外测它们的性能。", "",
                  "OOM 测试用临时服务器的预算由 96M 收紧到 64M，使全量读工作集确实触发拒绝；原 96M 仍容得下读取，不能用来断言必须 OOM。该调整只属于测试夹具，不改变 YCSB 服务端配置。", "",
                  "[6 项功能测试](verified-update-matrix/hash-move-functional-tests.log)；[3 项 Hash/OOM 回归](verified-update-matrix/hash-move-oom-tests.log)；[原 96M 夹具断言记录](verified-update-matrix/hash-move-oom-fixture-96m.log)。", ""]
    lines += ["", "[全部指标 CSV](verified-update-matrix/hash-move-ab-summary.csv)；[A/B 运行脚本](verified-update-matrix/hash_move_ab.py)；[原始结果索引](verified-update-matrix/hash-move-ab-results.json)。", ""]
    print(f"validated hash-move A/B: {len(cells)}/12 measured cells, {len(rows)} histogram rows, zero failures")
    return lines
