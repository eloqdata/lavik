#!/usr/bin/env python3
"""Validate raw YCSB counts and regenerate the single human-facing report."""
import csv
import json
from pathlib import Path
import subprocess
from run import parse, COUNTS, WORKLOADS, WORKERS

ROOT = Path(__file__).resolve().parent
MODES = ("hmset", "hreplace", "aerospike")


def capture_metadata():
    commands = {
        "server_cpu": ["lscpu"], "server_uname": ["uname", "-a"],
        "server_memory": ["free", "-b"],
        "raid": ["sudo", "-n", "mdadm", "--detail", "/dev/md0"],
        "partitions": ["sudo", "-n", "parted", "-s", "/dev/md0", "unit", "s", "print"],
        "aerospike_config": ["cat", "/mnt/dev/aerospike-md0.conf"],
        "keylane_sha256": ["sha256sum", "/mnt/dev/keylane/build/keylane"],
        "keylane_commit": ["git", "-C", "/mnt/dev/keylane", "rev-parse", "HEAD"],
        "client": ["ssh", "172.16.0.5", "lscpu; free -b; uname -a; java -version; sha256sum /mnt/dev/YCSB-hreplace-dist/lib/redis-binding-0.18.0-SNAPSHOT.jar /mnt/dev/YCSB-aerospike-dist/lib/aerospike-binding-0.18.0-SNAPSHOT.jar"],
    }
    info = {k: subprocess.run(v, capture_output=True, text=True).stdout for k, v in commands.items()}
    (ROOT / "environment.json").write_text(json.dumps(info, indent=2) + "\n")


def main():
    cells = {}
    rows = []
    for mode in MODES:
        for workload in WORKLOADS:
            for workers in WORKERS:
                name = f"{mode}-{workload.lower()}-c{workers}-measured"
                receipt_path = ROOT / f"{name}.json"
                if not receipt_path.exists():
                    continue
                cell = json.loads(receipt_path.read_text())
                checked = parse(ROOT / f"{name}.log", COUNTS["measured"])
                assert all(cell[key] == value for key, value in checked.items())
                assert cell["exit_code"] == 0
                cells[(mode, workload, workers)] = cell
                for operation in ("READ", "UPDATE", "INSERT", "READ-FAILED", "UPDATE-FAILED", "INSERT-FAILED"):
                    fields = cell["metrics"].get(operation)
                    if not fields or not fields.get("Operations", 0):
                        continue
                    row = {"database": mode, "workload": workload, "workers": workers,
                           "operation": operation, "runtime_ms": cell["runtime_ms"],
                           "count": fields["Operations"],
                           "operation_qps": fields["Operations"] * 1000 / cell["runtime_ms"],
                           "overall_success_qps": cell["success_qps"], "failed": cell["failed"],
                           "log": name + ".log", "sha256": cell["sha256"]}
                    for label, key in (("avg_us", "AverageLatency(us)"), ("min_us", "MinLatency(us)"),
                                       ("max_us", "MaxLatency(us)"), ("p50_us", "50thPercentileLatency(us)"),
                                       ("p95_us", "95thPercentileLatency(us)"), ("p99_us", "99thPercentileLatency(us)"),
                                       ("p999_us", "99.9PercentileLatency(us)"),
                                       ("p9999_us", "99.99PercentileLatency(us)")):
                        row[label] = fields[key]
                    rows.append(row)
    if rows:
        with (ROOT / "summary.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    expected = len(MODES) * len(WORKLOADS) * len(WORKERS)
    failures = sum(cell["failed"] for cell in cells.values())
    lines = ["# YCSB：Aerospike、Keylane HMSET、Keylane HREPLACE", "",
             "本页使用 2026-09-13 串行复测的原始 YCSB 客户端结果。此前 100K 操作短测不作为本页性能依据。", "",
             f"完成 **{len(cells)}/{expected} 组**；每组独立 1M 操作预热，随后独立 JVM 执行 5M 次测量操作。累计测量失败 **{failures:,}** 次。", "",
             "## Workload 与测量口径", "",
             "| 名称 | READ | UPDATE | INSERT |", "|---|---:|---:|---:|",
             "| A | 50% | 50% | 0 |", "| B | 95% | 5% | 0 |", "| C | 100% | 0 | 0 |",
             "| D（Uniform 变体） | 95% | 0 | 5% |", "",
             "按最终要求仅报告 256 workers 的 A/B/C/D；不再测其他并发或纯 UPDATE。标准 YCSB D 默认 latest，这里按要求使用 Uniform。", "",
             "- READ/UPDATE 访问原来加载的前 100,000,000 个 key，Uniform、hashed key order、10 个 field × 128 字节。历史 D 留下的少量额外 key 不进入读取范围。",
             "- D 的插入编号使用未用过的范围：HMSET/Aero warmup 从 200M 开始、measured 从 210M 开始；HREPLACE 配置复测从 220M/230M 开始。`insertstart=0,insertcount=100M` 保持相同 Uniform 读取范围；仅 D 的 `recordcount` 用于控制插入起点。不会把稀疏新范围当成已加载的读 key。",
             "- `readallfields=true`、`writeallfields=true`；读取整条记录，UPDATE 发送全部十个 field。YCSB 默认单字段更新与本次参数不同。",
             "- HMSET 与 KEYLANE.HREPLACE 使用同一 Jedis 3.9.0 binding，`redis.scanindex=none`。HMSET 合并字段，HREPLACE 替换已存在 Hash。Aero binding UPDATE 使用 REPLACE_ONLY（已检查实际 jar 字节码）。",
             "- C 是 HGETALL/读全部 bins；HREPLACE 列的 C/D 只是一轮相同读/插入路径的复测，不执行 HREPLACE（该选项仅影响 UPDATE）。",
             "- 表中并发是 YCSB workers；Redis 每个 worker 一条连接，Aero 客户端可能维护额外连接。",
             "- 每个时刻只运行一个 YCSB JVM、一个数据库服务。Keylane GC/defrag 开启；Aerospike 使用原生回收。客户端 Prometheus/Grafana 保持启用。",
             "- 延迟全部来自 YCSB operation HDR，单位毫秒；p999 = p99.9。成功和失败分开，QPS 使用实际成功次数 / 同一运行时间。服务端 metrics 只用于状态核验。",
             "- 这是一次有限操作数复测，预热 JVM 与测量 JVM 分开，测量包含新 JVM 的 JIT 过程；不代表持续稳态容量或多次重复的置信区间。", "",
             "## 测试环境", "",
             "- 服务端 `.4`：AMD EPYC 9V74，16 vCPU / 8 核（SMT），约 125 GiB 内存；客户端 `.5`：AMD EPYC 9V45，16 vCPU / 16 核。以本轮 lscpu 为准。",
             "- 六块 NVMe → `/dev/md0` RAID0，512 KiB chunk。Keylane io_uring 使用 `/dev/md0p1`，Aero CE 8.1.2.4 使用 `/dev/md0p2` 原始块设备。",
             "- 两者共享同组物理 NVMe，但分区容量不同：p1 约 9.47 TiB，Keylane 持久化标签实际可用约 5.24 TiB（之前初始化时确定）；p2 约 1.003 TiB。阵列/分区起点保存在环境证据中。",
             "- Keylane commit `a968d002c839828102a44316d43eb7dd63f03f09`，16 pinned workers，默认 busy-poll 20us，flush 128KiB，defrag 未暂停。",
             "- Aerospike：RF=1，indexes-memory-budget=64G，flush-size=128K，max-write-cache=8G，原配置与全部硬件信息见环境证据。", "",
             "## QPS 与 p99 / p99.9 / p99.99", "",
             "每格为 **成功 QPS；p99 / p99.9 / p99.99（ms）**。TOTAL 只列吞吐，不拼接读写百分位。", "",
             "| Workload | 并发 | 操作 | Aerospike | Keylane HMSET | Keylane HREPLACE |",
             "|---|---:|---|---:|---:|---:|"]
    for workload in WORKLOADS:
        for workers in WORKERS:
            operations = ["TOTAL"] + [op for op, fraction in zip(("READ", "UPDATE", "INSERT"), WORKLOADS[workload]) if fraction]
            for operation in operations:
                entries = []
                for mode in ("aerospike", "hmset", "hreplace"):
                    cell = cells.get((mode, workload, workers))
                    if not cell:
                        entries.append("待测")
                    elif operation == "TOTAL":
                        suffix = f"（失败 {cell['failed']:,}）" if cell["failed"] else ""
                        entries.append(f"{cell['success_qps']:,.0f}{suffix}")
                    else:
                        values = cell["metrics"][operation]
                        qps = values["Operations"] * 1000 / cell["runtime_ms"]
                        entries.append(f"{qps:,.0f}；{values['99thPercentileLatency(us)']/1000:.3f} / {values['99.9PercentileLatency(us)']/1000:.3f} / {values['99.99PercentileLatency(us)']/1000:.3f}")
                lines.append(f"| {workload} | {workers} | {operation} | " + " | ".join(entries) + " |")
    rate_cells = []
    rate_rows = []
    for mode in ("hmset", "aerospike"):
        name = f"{mode}-c-c256-target100000-measured"
        path = ROOT / f"{name}.json"
        if not path.exists():
            continue
        cell = json.loads(path.read_text())
        checked = parse(ROOT / f"{name}.log", COUNTS["measured"])
        assert all(cell[key] == value for key, value in checked.items())
        assert cell["exit_code"] == 0 and cell["failed"] == 0
        rate_cells.append((mode, cell))
        for histogram in ("READ", "Intended-READ"):
            values = cell["metrics"][histogram]
            assert values["Operations"] == COUNTS["measured"]
            rate_rows.append({"database": mode, "histogram": histogram,
                              "actual_qps": cell["success_qps"], "target_qps": 100000,
                              "runtime_ms": cell["runtime_ms"], "log": name + ".log",
                              "sha256": cell["sha256"], **values})
    if rate_rows:
        with (ROOT / "rate-limit-summary.csv").open("w", newline="") as output:
            keys = list(dict.fromkeys(k for row in rate_rows for k in row))
            writer = csv.DictWriter(output, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rate_rows)
        lines += ["", "## 只读限速 100K ops/s（256 workers）", "",
                  f"完成 {len(rate_cells)}/2 个数据库。使用 YCSB `-target 100000`，这是所有 worker 合计的目标速率。每库 1M 操作预热、5M 操作测量，读最初的 100M key；未做写入。", "",
                  "以下对比原始 C 的不限速结果与本轮 100K 限速结果，全部使用 YCSB READ operation 延迟，单位 ms；不限速数据来自上面的单轮矩阵。", "",
                  "| 数据库 | 目标 ops/s | 实际 QPS | 平均 ms | p99 ms | p99.9 ms | p99.99 ms | 失败 |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for mode, limited in rate_cells:
            for label, cell in (("不限速", cells[(mode, "C", 256)]), ("100,000", limited)):
                values = cell["metrics"]["READ"]
                db = "Keylane" if mode == "hmset" else "Aerospike"
                lines.append(f"| {db} | {label} | {cell['success_qps']:,.0f} | {values['AverageLatency(us)']/1000:.3f} | {values['99thPercentileLatency(us)']/1000:.3f} | {values['99.9PercentileLatency(us)']/1000:.3f} | {values['99.99PercentileLatency(us)']/1000:.3f} | {cell['failed']} |")
        lines += ["", "限速时额外设置 `measurement.interval=both`，同时记录从计划发起时刻算起的 `Intended-READ`；它包含客户端错过发起计划的延迟，不能与 READ 或服务端指标混为一谈。", "",
                  "| 数据库 | Intended p99 ms | Intended p99.9 ms | Intended p99.99 ms |",
                  "|---|---:|---:|---:|"]
        for mode, cell in rate_cells:
            v = cell["metrics"]["Intended-READ"]
            db = "Keylane" if mode == "hmset" else "Aerospike"
            lines.append(f"| {db} | {v['99thPercentileLatency(us)']/1000:.3f} | {v['99.9PercentileLatency(us)']/1000:.3f} | {v['99.99PercentileLatency(us)']/1000:.3f} |")
        lines += ["", "[限速逐项 CSV](verified-update-matrix/rate-limit-summary.csv) 包含两种延迟口径、所有百分位及原始日志校验值；[运行脚本](verified-update-matrix/rate_limit.py)。", ""]
    from hash_move_summary import report_lines
    lines += report_lines()
    from hash_direct_summary import report_lines as direct_report_lines
    lines += direct_report_lines()
    from cpu12_summary import report_lines as cpu12_report_lines
    lines += cpu12_report_lines()
    lines += ["", "## 原始证据与复现", "",
              "[逐操作 CSV](verified-update-matrix/summary.csv) 包含平均、最小、最大、p50/p95/p99/p99.9/p99.99、成功次数、失败次数、源日志 SHA256。",
              "[环境信息](verified-update-matrix/environment.json)；[串行运行脚本](verified-update-matrix/run.py)；[汇总与计数核验脚本](verified-update-matrix/summarize.py)。", "",
              "`verified-update-matrix/` 保留每组 warmup/measured 的原始 log、完整客户端命令、开始结束 UTC、解析 JSON 和前后服务端状态快照。", "",
              "脚本使用现有两份数据，依次停止/启动本轮数据库，并复用验证通过的已完成日志；不会清盘或 load。D 会插入新 key。若移走 D 的结果重测，必须分配新的插入编号范围，不能重复使用本轮范围。执行前确认没有其他压测。", "",
              "## 对之前结果的更正", "",
              "之前 `md0-matrix/` 的 100K 操作仅耗时约 0.4–1.3 秒，包含新 JVM 启动影响，旧脚本没有保证两库串行。其 D 在多次独立 JVM 中重复从同一编号插入，Aero 返回 CREATE_ONLY 冲突；Redis HMSET upsert 则掩盖了重复插入。旧数据保留审计，不用于当前比较。",
              "之前 perf 选择了 sudo 包装进程，空采样不能据此归因于 perf 权限限制；需要选择实际 keylane PID 重新验证。", ""]
    (ROOT.parent / "README.md").write_text("\n".join(lines))
    print(f"validated {len(cells)}/{expected} cells; {len(rows)} operation rows; {failures} failures")


if __name__ == "__main__":
    if not (ROOT / "environment.json").exists():
        capture_metadata()
    main()
