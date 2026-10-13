# Lavik v0.1.0 and Aerospike: YCSB throughput and tail latency

**English** | [简体中文](README.zh-CN.md)

Both databases were freshly loaded with **100 million records, each containing 10 × 128 B fields**. The complete **A/B/C/D × 100K/unlimited** matrix contains 16 measured windows of 300 seconds each; all warmup and measured phases have zero errors. **Lavik defrag stays enabled throughout.**

Aerospike uses default performance settings and original host policy. Lavik uses 12 workers with 4 CPUs for system/IRQ isolation and io_uring on a raw RAID0 partition. CPU allocation and database settings differ between these deployments.

Measured unlimited total-throughput differences relative to Aerospike: A **+4.36%**, B **+25.13%**, C **+28.76%**, D **+25.56%**.

Each figure shows total QPS above and per-operation p99.9 latency (ms) below, both as grouped bars. Color identifies the database; solid bars are READ and hatched bars are writes. C has no writes. Axis ranges differ between rate settings. Operation cells in the tables show **operation QPS; p99 / p99.9 (ms)**.

## 100K target

![100K target: throughput and p99.9 latency](ycsb-100k.png)

| Workload | Operation | Aerospike | Lavik |
|---|---|---:|---:|
| A | Total QPS | 99,883 | 99,966 |
| A | READ | 49,932; 0.342 / 0.437 | 49,987; 0.372 / 0.530 |
| A | UPDATE | 49,951; 0.292 / 0.385 | 49,979; 0.337 / 0.474 |
| B | Total QPS | 99,827 | 99,967 |
| B | READ | 94,829; 0.374 / 0.498 | 94,968; 0.482 / 0.856 |
| B | UPDATE | 4,998; 0.321 / 0.426 | 4,999; 0.410 / 0.653 |
| C | Total QPS | 99,902 | 99,970 |
| C | READ | 99,902; 0.379 / 0.481 | 99,970; 0.406 / 0.495 |
| D-Uniform | Total QPS | 99,901 | 99,961 |
| D-Uniform | READ | 94,906; 0.351 / 0.442 | 94,962; 0.368 / 0.488 |
| D-Uniform | INSERT | 4,995; 0.311 / 0.404 | 4,999; 0.354 / 0.479 |

## Unlimited

![Unlimited: throughput and p99.9 latency](ycsb-unlimited.png)

| Workload | Operation | Aerospike | Lavik |
|---|---|---:|---:|
| A | Total QPS | 507,401 | 529,501 |
| A | READ | 253,704; 1.407 / 2.953 | 264,758; 2.105 / 5.083 |
| A | UPDATE | 253,697; 1.196 / 2.255 | 264,743; 1.532 / 2.831 |
| B | Total QPS | 461,404 | 577,373 |
| B | READ | 438,351; 1.274 / 2.313 | 548,492; 0.776 / 1.818 |
| B | UPDATE | 23,053; 1.087 / 1.853 | 28,881; 0.702 / 1.578 |
| C | Total QPS | 461,251 | 593,911 |
| C | READ | 461,251; 1.253 / 1.982 | 593,911; 0.707 / 1.556 |
| D-Uniform | Total QPS | 460,167 | 577,808 |
| D-Uniform | READ | 437,177; 1.246 / 2.055 | 548,915; 0.790 / 1.824 |
| D-Uniform | INSERT | 22,991; 1.077 / 1.754 | 28,893; 0.724 / 1.589 |

## Hardware and storage

- **Server**：Azure Standard_L16aos_v4, Japan West zone 2; AMD EPYC 9V74; 16 logical CPUs / 8 physical cores; 125.8 GiB Linux RAM
- **Client**：Azure Standard_F32als_v6; AMD EPYC 9V74; 32 physical cores, 62.8 GiB RAM; Java 17; YCSB process restricted to CPUs 0–15
- **OS**：Ubuntu 24.04; Linux 6.17.0-1022-azure on both machines; no reboot
- **Network**：One client 10.84.0.5 → server 10.84.1.4; kernel TCP over accelerated mlx5
- **Disks**：Six local NVMe, each 1,919,850,381,312 B; Linux RAID0, 512 KiB chunk
- **Partitions**：Two distinct raw partitions; each 1,099,494,850,560 B; 24 MiB alignment; no filesystem; fully zeroed before load
- **Monitoring**：Per-phase effective configuration, process, IRQ, storage and client counters; Lavik metrics enabled

## Data and workloads

- **A**：READ: 50%；UPDATE: 50%；INSERT: 0；Read distribution: Uniform
- **B**：READ: 95%；UPDATE: 5%；INSERT: 0；Read distribution: Uniform
- **C**：READ: 100%；UPDATE: 0；INSERT: 0；Read distribution: Uniform
- **D-Uniform**：READ: 95%；UPDATE: 0；INSERT: 5%；Read distribution: Fixed original 100M keys

Both databases start with 100,000,000 records. After the matrix, Aerospike has 108,497,069 records and Lavik has 110,271,711 records.

## Database and host configuration

- **Version**：Aerospike: Community Edition 8.1.2.4-4；Lavik: v0.1.0 standard x86_64 release
- **Storage**：Aerospike: `storage-engine device`, raw RAID0 partition；Lavik: `--storage=uring`, raw RAID0 partition
- **CPU**：Aerospike: Original host policy, CPUs 0–15; defaults `service-threads=80`, `auto-pin=none`；Lavik: 12 pinned workers on CPUs 0–11; system tasks, workqueue and NIC IRQs on CPUs 12–15
- **systemd**：Aerospike: Default system.slice, no affinity override；Lavik: lavik-bench.slice, AllowedCPUs/CPUAffinity=0–11
- **Flush**：Aerospike: Defaults: 1 MiB, 1000 ms；Lavik: 128 KiB write chunks, 100 ms
- **Cache**：Aerospike: Defaults: max-write-cache=64 MiB, post-write-cache=256 MiB, read-page-cache=false；Lavik: Release defaults
- **Index**：Aerospike: Default indexes-memory-budget=0；Lavik: Release defaults
- **GC / defrag**：Aerospike: Defaults；Lavik: Enabled; 8 active jobs/device; block and record sleep=0
- **READ**：Aerospike: All 10 bins；Lavik: HGETALL, all 10 fields
- **UPDATE**：Aerospike: REPLACE_ONLY, all 10 bins；Lavik: LAVIK.HREPLACE, all 10 fields
- **INSERT**：Aerospike: CREATE_ONLY；Lavik: HMSET, redis.scanindex=none
- **Binding**：Aerospike: YCSB Aerospike, Java client 3.1.2；Lavik: YCSB Redis, Jedis 3.9.0
- **Replication / TTL**：Aerospike: One node, RF=1, namespace ycsb; default TTL 0；Lavik: One instance DB0, no replica, no TTL

Lavik is the unmodified standard release package: `v0.1.0 / feb61224e414c5653fd09560e20c9d214db1e3ac`; SHA-256 `7bf8d204a137678cee0e1a621b02661181373fbbfe1366bc1c201942b9dcf420`. Aerospike SHA-256 `6e1b2bd6deebd5816f4f369ac1db4151217ed0be3b66578bdd6f15c24e6af8d0`.

## Measurement method and limits

- Each database starts with 100M freshly loaded records. Workloads run in C/A/B/D order, 100K then unlimited for each workload; Aerospike first, then Lavik. Databases share the RAID0 sequentially, never concurrently. There are no clears or restarts between measured cases.
- Each case uses 256 YCSB workers, a separate one-million-operation warmup JVM and a fresh 300-second measurement JVM. `-target 100000` is the total rate across threads and operations. The measurement operation cap is 2 billion, so the timer controls termination. Initial JIT/ramp-up stays in the measured window.
- `fieldcount=10`, `fieldlength=128`, constant field lengths; `readallfields=true`, `writeallfields=true`; hashed key names; uniform reads; no SCAN or read-modify-write; 10,000 ms client timeouts. Raw field payload is 128 GB (119.21 GiB), excluding keys, field names, indexes and record metadata.
- D reads only the original 100M keys. Warmup/measured inserts use disjoint starting IDs: 2B/4B at 100K, 6B/8B unlimited. Equal starting sizes do not imply equal final sizes because D insert counts depend on throughput.
- Every load reconciles exactly 100M successful INSERTs with the server count. Each database is stopped cleanly and recovered after loading. Eight distributed records, all ten field lengths and their SHA-256 values are compared before/after recovery. Every phase reconciles client successes with database command counters and record-count deltas; errors must remain zero.
- Aerospike configuration changes only required cluster/listener/namespace/RF/device settings. CPU scheduling, IRQs, threads, caches, flushing, index budget and reclamation retain defaults. Effective values are recorded before/after every phase. Lavik's separate 12+4 CPU/IRQ policy is applied only after Aerospike stops, and restored afterward. Completion IRQ destinations and per-CPU interrupt deltas verify that benchmark NIC completion IRQs do not execute on Lavik worker CPUs during phases.
- Lavik defrag is enabled throughout load, recovery, warmup and measurement, with 8 active jobs/device and no additional sleep. `CONFIG GET defrag-paused` and `DEFRAG STATUS` are captured per phase. Tomb Raider and active expiration use release defaults in this YCSB group. Periodic 100 ms flushing is not a per-write synchronous durability guarantee.
- CPU numbers are logical CPUs: workers 0–11 occupy six physical cores and system/IRQ CPUs 12–15 occupy two separate physical cores (SMT2). Source-based routing sends replies from 10.84.1.4 through eth1/table 101; the management interface is not the benchmark reply path.
- QPS = successful operations / actual YCSB RunTime. Total QPS sums successful reads and writes. Each operation's p99/p99.9 comes from its complete-window ordinary HDR histogram, not an average of interval percentiles; intended histograms remain in raw logs. Equal YCSB worker counts do not imply equal TCP connection counts.
- The client uses [eloqdata/YCSB](https://github.com/eloqdata/YCSB) commit `a47bf01c4f197d2ff2eecbbebd9743da5a1cae47`. In RedisClient.java and RedisClientTest.java, replace `KEYLANE.HREPLACE` with `LAVIK.HREPLACE` and `keylane.hreplace` with `lavik.hreplace` before building. The bindings use Aerospike Java client 3.1.2 and Jedis 3.9.0.
- One five-minute measurement per cell; run-to-run variability and confidence intervals are not estimated. Product order is fixed. CPU allocation, host policies, caches and persistence differ between the two requested deployments, so the result is not a single-variable comparison.

- Aerospike: 2026-10-13T09:54:41Z – 2026-10-13T10:35:42Z (UTC), 8 measured windows.
- Lavik: 2026-10-13T10:39:09Z – 2026-10-13T11:20:04Z (UTC), 8 measured windows.

## Run commands

These Bash commands use the measured settings; adapt paths, addresses and NIC PCI identity to the deployment. Start with two independent, fully zeroed empty raw RAID0 partitions, exposed as `aerospike-device` and `lavik-device`. Run databases sequentially and load each once. Preserve the original host policy for Aerospike, then apply Lavik isolation; restore the original values afterward.

### Aerospike configuration

Save as `/mnt/dev/benchmark/release010/ycsb/aerospike.conf`:

```conf
# Single-node YCSB device namespace. Performance settings use server defaults.
service {
    cluster-name ycsb-default-20261013
}
logging {
    console {
        context any info
    }
}
network {
    service {
        address 10.84.1.4
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
        device /mnt/dev/benchmark/release010/ycsb/aerospike-device
    }
}
```

### Start the databases

Start Aerospike first. After its complete matrix, stop Aerospike, apply the CPU/IRQ isolation below, then start Lavik. Create the required log and runtime directories before starting.

```bash
sudo systemd-run \
  --unit aerospike-ycsb-default-20261013.service \
  --collect \
  -p TimeoutStopSec=300 \
  -p StandardOutput=append:/mnt/dev/benchmark/release010/ycsb/aerospike-server.stdout \
  -p StandardError=inherit \
  -p SendSIGKILL=no \
  -p LimitNOFILE=100000 /mnt/dev/benchmark/release010/aerospike-package/usr/bin/asd \
  --config-file /mnt/dev/benchmark/release010/ycsb/aerospike.conf \
  --foreground
```

```bash
sudo systemd-run \
  --unit lavik-ycsb-tuned-20261013.service \
  --collect \
  -p TimeoutStopSec=300 \
  -p StandardOutput=append:/mnt/dev/benchmark/release010/ycsb/hreplace-server.stdout \
  -p StandardError=inherit \
  -p SendSIGKILL=no \
  --slice=lavik-bench.slice \
  -p AllowedCPUs=0-11 \
  -p CPUAffinity=0-11 \
  -p LimitNOFILE=20000 /mnt/dev/benchmark/release010/downloads/lavik-v0.1.0-linux-x86_64/lavik \
  --bind 10.84.1.4 \
  --port 16379 \
  --metrics-port 19100 \
  --threads 12 \
  --pin-workers \
  --shutdown-checkpoint \
  --network kernel \
  --storage uring \
  --data-file /mnt/dev/benchmark/release010/ycsb/lavik-device \
  --flush-max-ms 100 \
  --defrag-max-active-per-device 8 \
  --defrag-sleep-ms 0 \
  --defrag-record-sleep-us 0 \
  --log-dir /mnt/dev/benchmark/release010/ycsb/lavik-server-logs
```

### Lavik CPU/IRQ isolation

irqbalance was inactive on this host. These commands combine with the service settings above to implement 12+4 isolation.

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
pci = '67bf:00:02.0'
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

### YCSB load, warmup and measurement

```bash
# On the client: Java 17 and the custom YCSB distribution are installed.
# Run the complete load/recovery/matrix for Aerospike first, then for Lavik.
DB=aerospike                       # use DB=lavik for the second database
if [ "$DB" = aerospike ]; then
  YCSB_ROOT=/opt/benchmark/release010/ycsb-aerospike-dist
  BINDING=aerospike
  DB_ARGS=(-p as.host=10.84.1.4 -p as.port=3000 -p as.namespace=ycsb -p as.timeout=10000)
else
  YCSB_ROOT=/opt/benchmark/ycsb-pr318-dist
  BINDING=redis
  DB_ARGS=(-p redis.host=10.84.1.4 -p redis.port=16379 -p redis.scanindex=none
           -p redis.timeout=10000 -p redis.cluster=false)
fi
mkdir -p results
COMMON=(-s -threads 256 -P "$YCSB_ROOT/workloads/workloada"
  -p fieldcount=10 -p fieldlength=128 -p fieldlengthdistribution=constant
  -p readallfields=true -p writeallfields=true -p insertorder=hashed
  -p measurementtype=hdrhistogram -p measurement.interval=both
  -p hdrhistogram.percentiles=50,95,99,99.9,99.99 -p status.interval=10)

# Fresh load: run once on an empty database.
taskset -c 0-15 "$YCSB_ROOT/bin/ycsb" load "$BINDING" "${COMMON[@]}" "${DB_ARGS[@]}" \
  -p recordcount=100000000 -p insertstart=0 -p insertcount=100000000 \
  > "results/$DB-load.log" 2>&1
```

After loading, verify exactly 100,000,000 successful INSERTs, server record counts and field lengths. Stop cleanly, restart with the same launch command, and confirm recovered samples before running the matrix below. Stop with `sudo systemctl stop aerospike-ycsb-default-20261013.service` or `sudo systemctl stop lavik-ycsb-tuned-20261013.service`; do not recreate or zero the partitions. Wait until all 4096 Aerospike master partitions are ready, or Lavik recovery is complete with the expected DBSIZE.

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
      taskset -c 0-15 "$YCSB_ROOT/bin/ycsb" run "$BINDING" \
        "${COMMON[@]}" "${DB_ARGS[@]}" "${RATE[@]}" "${LIMIT[@]}" "${RECORDS[@]}" \
        -p requestdistribution=uniform -p "readproportion=$READ" \
        -p "updateproportion=$UPDATE" -p "insertproportion=$INSERT" \
        -p scanproportion=0 -p readmodifywriteproportion=0 \
        > "results/$DB-$workload-$target-$phase.log" 2>&1
    done
  done
done
```
