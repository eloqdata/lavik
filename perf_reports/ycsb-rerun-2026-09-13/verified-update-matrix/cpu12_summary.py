"""Validate clean-load/CPU12 receipts and compare historical 16-worker runs."""
import csv
import json
from pathlib import Path
from statistics import median

from run import COUNTS, parse
from hash_move_summary import LATENCIES

ROOT = Path(__file__).resolve().parent


def rows_for(cells, configuration):
    rows = []
    for cell in cells:
        for op in (("READ",) if cell["workload"] == "C" else ("READ", "UPDATE")):
            for histogram in (op, "Intended-" + op):
                v = cell["metrics"][histogram]
                assert v["Operations"] == cell["metrics"][op]["Return=OK"]
                rows.append({"configuration": configuration, "round": cell["round"],
                             "workload": cell["workload"], "target": cell["target_ops_sec"],
                             "histogram": histogram, "total_qps": cell["success_qps"],
                             "op_qps": v["Operations"] * 1000 / cell["runtime_ms"],
                             "count": v["Operations"], "failed": cell["failed"],
                             **{label: v[key] for label, key in LATENCIES.items()},
                             "label": cell["label"], "sha256": cell["sha256"],
                             "binary_sha256": cell["binary_sha256"]})
    return rows


def host_deltas(before, after):
    def irqs(raw):
        return {line.split(":", 1)[0].strip(): [int(x) for x in line.split(":", 1)[1].split()[:16]]
                for line in raw.splitlines() if "mlx5_comp" in line}
    left, right = irqs(before["/proc/interrupts"]), irqs(after["/proc/interrupts"])
    assert left.keys() == right.keys() and len(left) == 16
    worker = sum(right[i][cpu] - left[i][cpu] for i in left for cpu in range(12))
    house = sum(right[i][cpu] - left[i][cpu] for i in left for cpu in range(12, 16))
    def softnet(raw, column):
        return sum(int(line.split()[column], 16) for line in raw.splitlines())
    def cpu_times(raw):
        return {int(parts[0][3:]): [int(v) for v in parts[1:9]]
                for line in raw.splitlines()
                if (parts := line.split()) and parts[0].startswith("cpu") and parts[0][3:].isdigit()}
    first, last = cpu_times(before["/proc/stat"]), cpu_times(after["/proc/stat"])
    deltas = {cpu: [b - a for a, b in zip(first[cpu], last[cpu])] for cpu in first}
    def cpu_pct(cpus, indices):
        total = sum(sum(deltas[c]) for c in cpus)
        return 100 * sum(deltas[c][i] for c in cpus for i in indices) / total
    return {"nic_worker_irqs": worker, "nic_housekeeping_irqs": house,
            "softnet_dropped": softnet(after["/proc/net/softnet_stat"], 1) - softnet(before["/proc/net/softnet_stat"], 1),
            "softnet_time_squeeze": softnet(after["/proc/net/softnet_stat"], 2) - softnet(before["/proc/net/softnet_stat"], 2),
            "housekeeping_busy_pct": 100 - cpu_pct(range(12, 16), (3, 4)),
            "housekeeping_irq_softirq_pct": cpu_pct(range(12, 16), (5, 6)),
            "worker_busy_pct": 100 - cpu_pct(range(12), (3, 4))}


def report_lines():
    if not (ROOT / "cpu12-results.json").exists():
        return []
    env = json.loads((ROOT / "cpu12-environment.json").read_text())
    clear = json.loads((ROOT / "cpu12-clear-db0.json").read_text())
    load = json.loads((ROOT / "cpu12-load.json").read_text())
    assert clear["after_restart"]["dbsize"] == 0
    checked = parse(ROOT / "cpu12-load.log", 100_000_000)
    assert all(load[k] == v for k, v in checked.items())
    assert load["exit_code"] == 0 and load["failed"] == 0 and load["server"]["dbsize"] == 100_000_000
    cells = json.loads((ROOT / "cpu12-results.json").read_text())
    seen, host = set(), []
    for cell in cells:
        ident = (cell["round"], cell["workload"], cell["target_ops_sec"])
        assert ident not in seen
        seen.add(ident)
        assert cell["binary_sha256"] == env["sha256"] and cell["workers"] == 256
        suffix = f"-target{cell['target_ops_sec']}" if cell["target_ops_sec"] else ""
        stem = f"{cell['label']}-hmset-{cell['workload'].lower()}-c256{suffix}"
        for phase in ("warmup", "measured"):
            recorded = json.loads((ROOT / f"{stem}-{phase}.json").read_text())
            checked = parse(ROOT / f"{stem}-{phase}.log", COUNTS[phase])
            assert all(recorded[k] == v for k, v in checked.items())
            assert recorded["exit_code"] == 0 and recorded["failed"] == 0
            if phase == "measured":
                assert all(cell[k] == v for k, v in recorded.items())
        host_stem = f"{cell['label']}-{cell['workload'].lower()}-target{cell['target_ops_sec']}-measured"
        before = json.loads((ROOT / f"{host_stem}.host-before.json").read_text())
        after = json.loads((ROOT / f"{host_stem}.host-after.json").read_text())
        host.append({"round": cell["round"], "workload": cell["workload"], "target": cell["target_ops_sec"],
                     **host_deltas(before, after)})
    if len(cells) == 12:
        assert seen == {(r, w, t) for r in (1, 2, 3) for w in ("C", "A") for t in (100000, 0)}
    old_c = [c for c in json.loads((ROOT / "hash-direct-read-results.json").read_text()) if c["variant"] == "direct"]
    old_a = [c for c in json.loads((ROOT / "hash-direct-reserve-results.json").read_text()) if c["variant"] == "reserve"]
    rows = rows_for(old_c + old_a, "16-worker historical") + rows_for(cells, "12-worker clean reload")
    for name, data in (("cpu12-summary.csv", rows), ("cpu12-host-deltas.csv", host)):
        with (ROOT / name).open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(data[0]))
            writer.writeheader()
            writer.writerows(data)
    lines = ["", "## 最新 main：清库重灌与 12＋4 CPU 分配", "",
             f"已完成 {len(cells)}/12 组测量。代码基于 `{env['commit']}`，二进制 SHA256 `{env['sha256']}`。合并时保留上游取消逻辑 Hash 总大小限制的修复；66 项 codec/旁表单测（含超过 1 GiB 的实际编解码）和 9 项端到端回归通过。", "",
             f"只清空 Keylane DB0 的 {clear['before']['dbsize']:,} 条旧测试记录：使用 FLUSHDB SYNC，等待同步回收，重启确认 DBSIZE=0，再从 .5 load 100M 条、每条 10×128B。没有格式化/丢弃原始设备，不改变 RAID/分区或 Aerospike 数据；这是逻辑清库，不是物理安全擦除。100M 次 load 全部成功，吞吐 {load['success_qps']:,.0f} inserts/s，完成后 DBSIZE=100M。", "",
             "### 放置与复现", "",
             "服务器 16 个逻辑 CPU 对应 8 个物理核心：0–11 是前 6 个完整物理核心，12–15 是后 2 个。Keylane 使用 taskset -c 0-11、--threads 12、--pin-workers，运行在独立的 keylane-bench.slice；system.slice、user.slice、init.scope 的运行时 AllowedCPUs 均为 12-15。可迁移任务的 affinity、全局 unbound workqueue 掩码也移到 12-15。", "",
             "将 enP46392s1 的 17 个 mlx5 MSI IRQ（含 16 个 completion 队列）轮流分配到 CPU 12–15。irqbalance 原本停用；RPS/XPS 保留原值。固定 per-CPU 内核线程及 managed NVMe IRQ 未宣称全部迁移，这不是 boot-time 完全隔离。GC/defrag 与 .5 的 Prometheus/Grafana 保持开启；客户端 CPU 放置、256 workers、Uniform、无 scan index、全字段读写不变。", "",
             "当前常驻服务为 keylane-ycsb-cpu12.service。启动/验证使用 cpu12_run.py start/verify；重灌命令为 cpu12_run.py clear-load --expected-old-count 100605038，带旧计数和单次清理凭据保护，不能直接重复执行。测量使用 cpu12_run.py run。恢复原放置前需停止该服务，再以 sudo 运行 cpu12_affinity.py restore；原始策略和任务 affinity 已保存，重启会清除这些 runtime 设置。旧 16-worker runner 不能在 user.slice=12-15 下直接使用。", "",
             "每轮依次 C、A，每种负载依次 100K 与不限速；每组独立 1M 预热＋5M 测量。三轮连续使用同一个 12-worker 服务，A 只更新、不插入。", "",
             "### 与上次 16-worker 结果并列", "",
             "这不是只改变 IRQ 的严格 A/B：最新 main、清库重灌、worker 数、后台任务/网卡 IRQ 放置、重启顺序均有变化。16-worker 的 C 是上次专用解码版（未加不参与只读的写参数预分配），A 是两项改动版。只能比较这组整体配置效果，不能将变化独立归因于 IRQ 或某项代码。", "",
             "下表是逐轮指标的中位数，非合并 HDR 百分位；所有延迟为 YCSB operation、单位 ms，Intended 指标另存 CSV。各列独立取中位数，读写 QPS 中位数之和不必等于总 QPS 中位数。", "",
             "| Workload | 限速 | 操作 | 配置 | 轮数 | 总 QPS | 操作 QPS | 平均 ms | p99 ms | p999 ms | p9999 ms |",
             "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    ordinary = [r for r in rows if not r["histogram"].startswith("Intended-")]
    for workload in ("C", "A"):
        for target in (100000, 0):
            for op in (("READ",) if workload == "C" else ("READ", "UPDATE")):
                for config in ("16-worker historical", "12-worker clean reload"):
                    group = [r for r in ordinary if (r["workload"], r["target"], r["histogram"], r["configuration"]) == (workload, target, op, config)]
                    if not group:
                        continue
                    nums = [f"{median(r[f] for r in group):,.0f}" for f in ("total_qps", "op_qps")]
                    nums += [f"{median(r[f] for r in group)/1000:.3f}" for f in ("avg_us", "p99_us", "p999_us", "p9999_us")]
                    lines.append(f"| {workload} | {'100K' if target else '不限速'} | {op} | {'原16' if config.startswith('16') else '新12＋4'} | {len(group)} | " + " | ".join(nums) + " |")
    lines += ["", "### 新配置逐轮数据", "",
              "| 轮 | Workload | 限速 | 操作 | 操作 QPS | p99 ms | p999 ms | p9999 ms | 失败 |",
              "|---:|---|---|---|---:|---:|---:|---:|---:|"]
    for r in ordinary:
        if not r["configuration"].startswith("12"):
            continue
        lines.append(f"| {r['round']} | {r['workload']} | {'100K' if r['target'] else '不限速'} | {r['histogram']} | {r['op_qps']:,.0f} | {r['p99_us']/1000:.3f} | {r['p999_us']/1000:.3f} | {r['p9999_us']/1000:.3f} | {r['failed']} |")
    lines += ["", "### 放置效果核验", "",
              "| 轮 | Workload | 限速 | NIC IRQ 增量：CPU 0–11 | CPU 12–15 | softnet dropped | time-squeeze | 后4 CPU 忙碌% | 后4 IRQ/softirq% |",
              "|---:|---|---|---:|---:|---:|---:|---:|---:|"]
    for r in host:
        lines.append(f"| {r['round']} | {r['workload']} | {'100K' if r['target'] else '不限速'} | {r['nic_worker_irqs']} | {r['nic_housekeeping_irqs']} | {r['softnet_dropped']} | {r['softnet_time_squeeze']} | {r['housekeeping_busy_pct']:.1f} | {r['housekeeping_irq_softirq_pct']:.1f} |")
    if len(cells) == 12:
        def metric(config, workload, target, op, field):
            return median(r[field] for r in ordinary
                          if (r["configuration"], r["workload"], r["target"], r["histogram"])
                          == (config, workload, target, op))
        old, new = "16-worker historical", "12-worker clean reload"
        c_gain = metric(new, "C", 0, "READ", "total_qps") / metric(old, "C", 0, "READ", "total_qps") - 1
        a_gain = metric(new, "A", 0, "READ", "total_qps") / metric(old, "A", 0, "READ", "total_qps") - 1
        lines += ["", "### 本轮结论", "",
                  f"与上述历史中位数相比，不限速 C 总吞吐 {c_gain:+.2%}，A 总吞吐 {a_gain:+.2%}。100K 的 C READ p9999 为 "
                  f"{metric(old, 'C', 100000, 'READ', 'p9999_us')/1000:.3f} → {metric(new, 'C', 100000, 'READ', 'p9999_us')/1000:.3f} ms，"
                  f"A UPDATE 为 {metric(old, 'A', 100000, 'UPDATE', 'p9999_us')/1000:.3f} → {metric(new, 'A', 100000, 'UPDATE', 'p9999_us')/1000:.3f} ms。", "",
                  f"不限速 C READ p9999 却从 {metric(old, 'C', 0, 'READ', 'p9999_us')/1000:.3f} 升至 {metric(new, 'C', 0, 'READ', 'p9999_us')/1000:.3f} ms；"
                  "不能宣称该配置全面改善极端长尾。固定 100K 和饱和压力的取舍必须分开看，历史对照也无法分离 CPU 放置、代码和数据状态的影响。", "",
                  f"12 个测量窗口中，网卡 completion IRQ 落在 CPU 0–11 的总增量为 {sum(r['nic_worker_irqs'] for r in host)}，"
                  f"softnet dropped 总增量为 {sum(r['softnet_dropped'] for r in host)}。不限速窗口仍有 time-squeeze；"
                  "这证实网卡 IRQ 放置生效，但不是 YCSB 尖峰来源的分段归因。"]
    lines += ["", "CPU 百分比来自 /proc/stat 两次快照差值，在对应 CPU 集合内加权平均；忙碌为 100% 减 idle/iowait。time-squeeze 表示一次 softirq 处理用完预算，不等于丢包，也不能仅凭它判定 YCSB 尖峰来源。以上 IRQ/softnet 是服务端计数，延迟仍完全取客户端 YCSB。", "",
              "[环境与二进制](verified-update-matrix/cpu12-environment.json)；[原始放置](verified-update-matrix/cpu12-affinity-original.json)；[应用凭据](verified-update-matrix/cpu12-affinity-applied.json)；[清库凭据](verified-update-matrix/cpu12-clear-db0.json)；[load 结果](verified-update-matrix/cpu12-load.json)。", "",
              "[完整指标 CSV](verified-update-matrix/cpu12-summary.csv)；[逐轮结果](verified-update-matrix/cpu12-results.json)；[放置脚本](verified-update-matrix/cpu12_affinity.py)；[重灌/测量脚本](verified-update-matrix/cpu12_run.py)。", ""]
    print(f"validated CPU12: {len(cells)}/12 cells, {len(host)} host counter windows, 100M clean load")
    return lines
