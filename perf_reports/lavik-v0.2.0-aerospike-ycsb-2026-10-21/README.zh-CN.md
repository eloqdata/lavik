# Lavik v0.2.0 与 Aerospike：YCSB 吞吐与尾延迟

[English](README.md) | **简体中文**

在新建 Azure 环境中重新执行完整 **A/B/C/D × 100K/不限速** 矩阵，共 **16 个 300 秒正式窗口**。每种数据库独立装载 **1 亿条记录，每条 10 × 128 B 字段**；正式测量与预热均零错误。

不限速总吞吐相对 Aerospike 的实测差异：A **+9.06%**, B **+23.30%**, C **+21.67%**, D **+24.08%**.

吞吐与尾延迟存在取舍：不限速的七个操作单元中，Lavik 有 **4 个 p99.9 高于 Aerospike**；最大相对差异在 **A / READ**，为 **4.399 ms 对 2.225 ms（1.98×）**。这是单次窗口观察值。

两者使用不同 CPU 与宿主策略：Aerospike 保持原始宿主策略，Lavik 使用 12 个 worker 与 4 个用于系统任务与压测网卡 IRQ 策略的 CPU。表中操作单元格为 **操作 QPS；p99 / p99.9（ms）**。

## 100K 限速

![100K 限速](ycsb-100k.svg)

| Workload | Operation | Aerospike | Lavik |
| --- | --- | --- | --- |
| A | Total QPS | 99,916 | 99,959 |
| A | READ | 49,954; 0.700 / 0.797 | 49,974; 0.695 / 0.837 |
| A | UPDATE | 49,962; 0.646 / 0.741 | 49,985; 0.655 / 0.797 |
| B | Total QPS | 99,932 | 99,976 |
| B | READ | 94,931; 0.731 / 0.861 | 94,980; 0.704 / 0.920 |
| B | UPDATE | 5,001; 0.681 / 0.805 | 4,996; 0.674 / 0.803 |
| C | Total QPS | 99,909 | 99,968 |
| C | READ | 99,909; 0.724 / 0.845 | 99,968; 0.745 / 0.873 |
| D-Uniform | Total QPS | 99,934 | 99,973 |
| D-Uniform | READ | 94,937; 0.709 / 0.840 | 94,971; 0.695 / 0.786 |
| D-Uniform | INSERT | 4,997; 0.662 / 0.785 | 5,002; 0.676 / 0.768 |

## 不限速

![不限速](ycsb-unlimited.svg)

| Workload | Operation | Aerospike | Lavik |
| --- | --- | --- | --- |
| A | Total QPS | 436,866 | 476,454 |
| A | READ | 218,424; 1.164 / 2.225 | 238,229; 1.140 / 4.399 |
| A | UPDATE | 218,442; 1.037 / 1.900 | 238,225; 0.903 / 2.179 |
| B | Total QPS | 396,293 | 488,622 |
| B | READ | 376,491; 1.178 / 2.095 | 464,203; 0.895 / 1.902 |
| B | UPDATE | 19,802; 1.061 / 1.835 | 24,418; 0.831 / 1.716 |
| C | Total QPS | 396,447 | 482,358 |
| C | READ | 396,447; 1.105 / 1.919 | 482,358; 0.933 / 1.754 |
| D-Uniform | Total QPS | 397,541 | 493,262 |
| D-Uniform | READ | 377,664; 1.150 / 2.004 | 468,616; 0.928 / 2.305 |
| D-Uniform | INSERT | 19,876; 1.040 / 1.797 | 24,646; 0.865 / 2.229 |

## 环境与测量方法

| Item | Configuration |
| --- | --- |
| Server | Azure Standard_L16aos_v4; Japan East zone 2; AMD EPYC 9V74; 8 physical / 16 logical CPUs; 128 GiB nominal RAM |
| Client | Standard_F32als_v6; Java 17; YCSB pinned to CPUs 0–15 |
| OS | Ubuntu 24.04; Linux 6.17.0-1022-azure on both VMs |
| Storage | 6 × 1,919,850,381,312 B local NVMe; RAID0, 512 KiB chunk; two fully zeroed 1,099,494,850,560 B raw partitions |
| Database | Lavik v0.2.0 standard x86_64; Aerospike CE 8.1.2.4-4 |
| YCSB | eloqdata/YCSB a47bf01c4f197d2ff2eecbbebd9743da5a1cae47; KEYLANE.HREPLACE renamed LAVIK.HREPLACE |
| A / B / C / D | 50% read + 50% update / 95% read + 5% update / 100% read / 95% read + 5% insert |
| Final records | Aerospike 107,563,558; Lavik 108,996,321 |

- 每种数据库独立装载 1 亿条记录，干净停机并恢复后再正式测量；恢复前后对比分散的八条记录及所有字段 SHA-256。初始字段净载荷为 119.21 GiB，不含 key 和元数据。
- 先 Aerospike、后 Lavik；负载顺序 C/A/B/D，每种先 100K 再不限速。每格单独运行 100 万次操作预热 JVM，再启动新的 256-worker 测量 JVM。操作上限为 20 亿，由 300 秒计时器结束；初始 JIT 和爬升包含在窗口内。
- 均匀随机读取；D 只读取原始 1 亿条，100K 预热/测量的插入起点为 20 亿/40 亿，不限速为 60 亿/80 亿，互不重叠。D 的最终记录数随实测吞吐变化。
- 每次读写所有十个字段。Lavik 使用 HGETALL / LAVIK.HREPLACE / HMSET；Aerospike 使用全 bin 读取 / REPLACE_ONLY / CREATE_ONLY。客户端超时 10,000 ms；Aerospike Java client 3.1.2、Jedis 3.9.0 与旧报告一致。
- Aerospike 只改必要的集群、监听、namespace、复制与设备设置。Lavik 使用 io_uring、停机 checkpoint、metrics 和 12+4 宿主策略；全程检查 defrag 启用，结束后恢复 CPU/IRQ 策略。
- 每阶段核对客户端成功数、服务端命令计数及记录数变化。验证 Lavik 网卡完成 IRQ 在阶段内不落到 worker CPU。保留精确命令、生效配置、计数和日志。
- QPS 按成功操作数除以实际 YCSB 运行时长计算；各操作分位数来自完整窗口普通 HDR 直方图，不平均分段分位数。intended 直方图保留在日志中。相同 worker 数不代表相同 TCP 连接数。
- 每格仅一次测量，不估计置信区间。CPU 分配、缓存、刷盘和持久化策略不同，成功响应不代表相同同步持久性；本次位于 Japan East，旧测试位于 Japan West；区域、物理放置和运行时段也有差异。

- Aerospike CE 8.1.2.4-4: 2026-10-21T15:00:19Z – 2026-10-21T15:41:22Z (UTC).
- Lavik v0.2.0: 2026-10-21T15:46:26Z – 2026-10-21T16:27:24Z (UTC).

[Point data](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-aerospike-ycsb-2026-10-21/metadata/results.csv) · [Provenance](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-aerospike-ycsb-2026-10-21/metadata/provenance.json) · [Validation](https://github.com/eloqdata/lavik-agent/blob/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-aerospike-ycsb-2026-10-21/metadata/validation.json) · [原始日志与测量凭据](https://github.com/eloqdata/lavik-agent/tree/bb061b97e9836281e71c219f21570d742313fb8c/perf_reports/lavik-v0.2.0-aerospike-ycsb-2026-10-21)

## 运行命令

以下 Bash 命令对应本次参数；路径、地址、网卡 PCI 身份需按部署环境调整。开始前准备两个独立、已全量写零的空白 RAID0 裸分区，分别由 `aerospike-device` 和 `lavik-device` 指向。两库顺序运行，每库只灌入一次。宿主策略在 Aerospike 阶段保持原样，完成后才应用 Lavik 的隔离设置；测试完成后恢复原值。

### Aerospike 配置

保存为 `/opt/bench020/ycsb/aerospike.conf`：

```conf
# Single-node YCSB device namespace. Performance settings use server defaults.
service {
    cluster-name ycsb-default-20261021
}
logging {
    console {
        context any info
    }
}
network {
    service {
        address 10.96.1.4
        port 3000
    }
    heartbeat {
        address 127.0.0.1
        mode mesh
        port 3002
    }
    fabric {
        address 127.0.0.1
        port 3001
    }
    admin {
        address 127.0.0.1
        port 3003
    }
}
namespace ycsb {
    replication-factor 1
    storage-engine device {
        device /opt/bench020/ycsb/aerospike-device
    }
}
```

### 数据库启动

先执行 Aerospike 命令。完成其全部测试后停止 Aerospike，应用下一节的 CPU/IRQ 隔离，再执行 Lavik 命令。启动前确保配置中的日志与运行目录已创建。

```bash
sudo mkdir -p /opt/aerospike
sudo cp -a /opt/bench020/aerospike-package/opt/aerospike/usr /opt/aerospike/

sudo systemd-run \
  --unit aerospike-ycsb-default-20261021.service \
  --collect \
  -p TimeoutStopSec=300 \
  -p StandardOutput=append:/opt/bench020/ycsb/aerospike-server.stdout \
  -p StandardError=inherit \
  -p SendSIGKILL=no \
  -p LimitNOFILE=100000 /opt/bench020/aerospike-package/usr/bin/asd \
  --config-file /opt/bench020/ycsb/aerospike.conf \
  --foreground
```

```bash
sudo systemd-run \
  --unit lavik-ycsb-tuned-20261021.service \
  --collect \
  -p TimeoutStopSec=300 \
  -p StandardOutput=append:/opt/bench020/ycsb/hreplace-server.stdout \
  -p StandardError=inherit \
  -p SendSIGKILL=no \
  --slice=lavik-bench.slice \
  -p AllowedCPUs=0-11 \
  -p CPUAffinity=0-11 \
  -p LimitNOFILE=20000 /opt/bench020/downloads/lavik-v0.2.0-linux-x86_64/lavik \
  --bind 10.96.1.4 \
  --port 16379 \
  --metrics-port 19100 \
  --threads 12 \
  --shutdown-checkpoint \
  --data-file /opt/bench020/ycsb/lavik-device \
  --log-dir /opt/bench020/ycsb/lavik-server-logs
```

### Lavik CPU/IRQ 隔离

irqbalance 在本机未运行。以下操作与前述服务启动参数共同实现 12+4 隔离。

```bash
# Server, only after Aerospike has stopped; dedicated 16-logical-CPU test host.
sudo systemctl set-property --runtime system.slice AllowedCPUs=12-15
sudo systemctl set-property --runtime user.slice AllowedCPUs=12-15
sudo systemctl set-property --runtime init.scope AllowedCPUs=12-15
printf 'f000\n' | sudo tee /sys/devices/virtual/workqueue/cpumask

# This PCI identity must match the benchmark NIC on the current boot.
sudo python3 - <<'PYCODE'
from pathlib import Path
import os
vfs = list(Path('/sys/class/net/eth1').glob('lower_*'))
assert len(vfs) == 1
pci = (vfs[0].resolve() / 'device').resolve().name
irqs = [line.split(':', 1)[0].strip()
        for line in Path('/proc/interrupts').read_text().splitlines() if pci in line]
assert len(irqs) == 17, irqs
for i, irq in enumerate(irqs):
    Path('/proc/irq', irq, 'smp_affinity_list').write_text(str(12 + i % 4) + '\n')
# Migrate existing userspace threads; leave kernel threads to their own policy.
for stat in Path('/proc').glob('[0-9]*/task/[0-9]*/stat'):
    try:
        raw = stat.read_text()
        comm = raw[raw.index('(')+1:raw.rindex(')')]
        flags = int(raw[raw.rindex(')')+2:].split()[6])
        if flags & 0x04000000 or comm.startswith('kworker/'):
            continue
        os.sched_setaffinity(int(stat.parent.name), {12, 13, 14, 15})
    except (OSError, ProcessLookupError):
        pass
PYCODE
```

### YCSB 灌入、预热和正式测量

```bash
# On the client: Java 17 and the custom YCSB distribution are installed.
# Run the complete load/recovery/matrix for Aerospike first, then for Lavik.
DB=aerospike                       # use DB=lavik for the second database
if [ "$DB" = aerospike ]; then
  YCSB_ROOT=/opt/bench020/ycsb-aerospike-dist
  BINDING=aerospike
  DB_ARGS=(-p as.host=10.96.1.4 -p as.port=3000 -p as.namespace=ycsb -p as.timeout=10000)
else
  YCSB_ROOT=/opt/bench020/ycsb-redis-dist
  BINDING=redis
  DB_ARGS=(-p redis.host=10.96.1.4 -p redis.port=16379 -p redis.scanindex=none
           -p redis.timeout=10000 -p redis.cluster=false)
fi
mkdir -p results
COMMON=(-s -threads 256 -P "$YCSB_ROOT/workloads/workloada"
  -p fieldcount=10 -p fieldlength=128 -p fieldlengthdistribution=constant
  -p readallfields=true -p writeallfields=true -p insertorder=hashed
  -p measurementtype=hdrhistogram -p measurement.interval=both
  -p hdrhistogram.percentiles=50,95,99,99.9,99.99 -p status.interval=10)

# Fresh load: run once on an empty database.
taskset -c 0-15 /usr/bin/python3 "$YCSB_ROOT/bin/ycsb" load "$BINDING" "${COMMON[@]}" "${DB_ARGS[@]}" \
  -p recordcount=100000000 -p insertstart=0 -p insertcount=100000000 \
  > "results/$DB-load.log" 2>&1
```

灌入完成后核验恰好 100,000,000 次成功 INSERT、服务端记录数及字段长度，干净停止再用同一启动命令恢复，核验样本内容一致后运行下方矩阵。Aerospike 使用 `sudo systemctl stop aerospike-ycsb-default-20261021.service`，Lavik 使用 `sudo systemctl stop lavik-ycsb-tuned-20261021.service`；不要重建或清零分区。恢复就绪需确认 Aerospike 全部 4096 个主分区可用，Lavik 恢复完成且 DBSIZE 正确。

```bash
# Run after load/recovery verification; retain this client shell's variables.
if [ "$DB" = lavik ]; then
  DB_ARGS+=(-p redis.updatecommand=lavik.hreplace)
fi
for workload in C A B D; do
  case "$workload" in
    A) READ=0.5;  UPDATE=0.5;  INSERT=0 ;;
    B) READ=0.95; UPDATE=0.05; INSERT=0 ;;
    C) READ=1;    UPDATE=0;    INSERT=0 ;;
    D) READ=0.95; UPDATE=0;    INSERT=0.05 ;;
  esac
  for target in 100000 0; do
    RATE=()
    if [ "$target" -gt 0 ]; then RATE=(-target "$target"); fi
    for phase in warmup measured; do
      LIMIT=(-p operationcount=1000000)
      if [ "$phase" = measured ]; then
        LIMIT=(-p operationcount=2000000000 -p maxexecutiontime=300)
      fi
      RECORDS=(-p recordcount=100000000)
      if [ "$workload" = D ]; then
        if [ "$target" -gt 0 ]; then start=2000000000; else start=6000000000; fi
        if [ "$phase" = measured ]; then start=$((start + 2000000000)); fi
        RECORDS=(-p "recordcount=$start" -p insertstart=0 -p insertcount=100000000)
      fi
      taskset -c 0-15 /usr/bin/python3 "$YCSB_ROOT/bin/ycsb" run "$BINDING" \
        "${COMMON[@]}" "${DB_ARGS[@]}" "${RATE[@]}" "${LIMIT[@]}" "${RECORDS[@]}" \
        -p requestdistribution=uniform -p "readproportion=$READ" \
        -p "updateproportion=$UPDATE" -p "insertproportion=$INSERT" \
        -p scanproportion=0 -p readmodifywriteproportion=0 \
        > "results/$DB-$workload-$target-$phase.log" 2>&1
    done
  done
done
```
