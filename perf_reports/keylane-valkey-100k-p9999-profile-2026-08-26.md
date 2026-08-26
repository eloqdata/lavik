# Keylane 100K QPS read p99.99 diagnosis

Date: 2026-08-26 UTC
Latest confirmation: 2026-08-26 03:00 UTC

## Technical summary

The five-minute normal Release run no longer has a 0.8 ms p99.99. With Keylane workers on CPUs 0-11 and the NIC IRQs plus local tmux/Codex processes on CPUs 12-15, 29,999,999 measured GETs placed p99.99 in the `0.455-0.479 ms` histogram bucket. Throughput was 99,926.06 requests/s, average latency was 0.192 ms, and p99 was 0.327 ms.

This does not mean the prior `0.703-0.807 ms` result was calculated incorrectly. A 30-second Release run immediately after isolating the tools produced that bucket, and a subsequent 30-second run produced `0.471-0.495 ms`. The five-minute result contains 3,000 observations in the upper 0.01% and places p99.99 below 0.5 ms, demonstrating that `0.7-0.8 ms` is neither the current steady value nor a service floor.

The trace build identified and removed one concrete server-side source of tail latency. Before local tool isolation, individual workers showed request-queue p99.99 values of 200-750 us. BPF scheduler tracing attributed 93 of 95 wake-to-run delays above 100 us to the unrestricted tmux server or Codex processes. After pinning those processes away from worker CPUs, all 12 workers measured request- and reply-queue p99.99 at or below 30 us in the same trace build. Source-side wake batching was already fast and was not the cause.

The implication is twofold:

- Do not treat the earlier 200-750 us per-worker queue tail as a Celer cross-core algorithm delay; it was predominantly target-worker preemption by unrelated local processes.
- Do not attribute the entire remaining client p99.99 to cross-core routing. The measured internal queue tail is now much smaller than the end-to-end tail, so the next profile must split network ingress/egress, command execution, SPDK I/O, and client rate-limiter scheduling in the same requests.

## Five minutes with trace disabled holds p99.99 below 0.5 ms

The confirmation used the normal Release binary with no trace strings, no server journal entries during the measurement, and the same 100K-QPS workload as the previous formal measurements.

| Metric | Latest result |
|---|---:|
| Completed requests | 29,999,999 |
| Throughput | 99,926.06 requests/s |
| Average latency | 0.192 ms |
| p50 | 0.191 ms |
| p95 | 0.271 ms |
| p99 | 0.327 ms |
| p99.9 | `0.375-0.399 ms` |
| p99.99 | `0.455-0.479 ms` |
| Maximum | 6.799 ms |

The p99.99 rank is `ceil(29,999,999 * 0.9999) = 29,997,000`. The cumulative count was 29,996,484 at or below 0.455 ms and 29,998,242 at or below 0.479 ms, which bounds the percentile to that bucket. The exact latency is not recoverable from the histogram.

The two immediately preceding 30-second Release runs placed p99.99 in `0.703-0.807 ms` and `0.471-0.495 ms`. Their variation explains the apparent contradiction; the longer run is the stronger estimate of the current steady p99.99. Its larger 6.799 ms maximum does not contradict the improved p99.99 because only a very small number of requests reached that extreme.

## Unlimited throughput reaches 316K QPS with sub-millisecond p99.99

Removing `--rps 100000` while retaining the same Release binary, key dataset, 80 connections, eight client threads, pipeline depth one, five-second warmup, and five-minute measurement produced:

| Metric | 100K rate limit | Unlimited |
|---|---:|---:|
| Throughput | 99,926.06 QPS | 316,412.41 QPS |
| Average | 0.192 ms | 0.233 ms |
| p99 | 0.327 ms | 0.423 ms |
| p99.9 | `0.375-0.399 ms` | `0.487-0.519 ms` |
| p99.99 | `0.455-0.479 ms` | `0.599-0.631 ms` |
| Maximum | 6.799 ms | 7.367 ms |

The unlimited client completed 94,923,721 requests. Keylane averaged 1,192.82% CPU, or 11.93 of its 12 assigned logical CPUs, while the remote Valkey benchmark averaged 531%, or 5.31 CPUs. The server was therefore the first CPU bottleneck, and approximately 316K QPS is the measured ceiling for this exact 12-worker, pipeline-one workload. Saturation raised average latency by about 21% and p99.99 by about 32%, but p99.99 remained below 0.7 ms.

## SPDK is 6.8% faster than io_uring raw block in the affinity-controlled read test

The io_uring backend directly opened the same two raw NVMe block devices without XFS or RAID and recovered the existing 200,000,000-key dataset. It used the same 12 Keylane worker CPUs, four mlx5 IRQ/housekeeping CPUs, client configuration, key dataset, pipeline depth, and five-minute unlimited workload.

| Metric | SPDK | io_uring raw block |
|---|---:|---:|
| Throughput | 316,412.41 QPS | 296,266.47 QPS |
| Average | 0.233 ms | 0.251 ms |
| p99 | 0.423 ms | 0.439 ms |
| p99.9 | `0.487-0.519 ms` | `0.511-0.535 ms` |
| p99.99 | `0.599-0.631 ms` | `0.615-0.647 ms` |
| Maximum | 7.367 ms | 8.159 ms |

Raw block was 6.37% below SPDK in throughput, not 6.8%; its average latency was 7.73% higher and its p99.99 midpoint was about 2.6% higher. Linux assigned NVMe managed completion queues to CPUs 0-15 and did not permit manual affinity changes. During the raw run, worker CPUs 0-11 received about 7.2-7.7 million NVMe interrupts each while mlx5 remained entirely on CPUs 12-15. That kernel block/interrupt work is an inherent part of this raw io_uring result.

The raw run used a dedicated Release block-device build and the SPDK run used its Release SPDK build. They are separate build artifacts from the same workspace, so the comparison is operationally representative but not a byte-identical-source compiler A/B. Both had trace disabled.

## Isolating local tools removed the abnormal worker queue tail

The direct cross-core trace separated three intervals:

| Interval | Average | p99.9 | Observed p99.99 behavior |
|---|---:|---:|---:|
| Source worker wake batching | 0.4-0.6 us | <= 8-10 us | <= 15-20 us |
| Request post to target drain | 2.4-3.0 us | <= 20-30 us | 200-750 us on occasional workers before isolation |
| Reply post to source drain | similar to request path | <= 20-30 us | same scheduling-sensitive pattern before isolation |

The source-side `FlushWakes()` stage is therefore not the slow stage. The long interval was between publishing work and the target worker running again.

BPF scheduler tracing then measured worker wake-to-run latency and recorded the task occupying the target CPU at each event above 100 us:

| CPU owner at slow wake | Events |
|---|---:|
| `tmux: server` | 81 |
| `codex-main` | 12 |
| `containerd` | 1 |
| `sqlx-sqlite-worker` | 1 |
| **Total** | **95** |

Of those events, 82 were in the 512-1000 us bucket and 10 were in the 1-2 ms bucket. The delays did not stay attached to a particular Keylane worker; the unrestricted processes could run on any CPU from 0 through 11 and preempt whichever worker was there.

After moving tmux/Codex to CPUs 12-15 and repeating the same trace workload:

- all 12 request-queue p99.99 values were at or below 30 us;
- all 12 reply-queue p99.99 values were at or below 30 us;
- queue averages were 2.49-2.91 us;
- the trace-build client p99.99 was in `0.407-0.503 ms`.

The trace-build client number should not be compared directly with the normal Release number because instrumentation changes instruction count and timing. The valid A/B conclusion is the disappearance of the 200-750 us internal queue p99.99 within the same trace build.

## IRQ isolation helps, but moving Keylane support threads onto IRQ CPUs hurts

The active CPU layout is:

| Work | CPUs |
|---|---|
| 12 pinned Keylane workers | 0-11 |
| mlx5 completion IRQs | 12-15, round-robin |
| mlx5 async IRQ | 15 |
| local tmux/Codex processes | 12-15 |
| Keylane main and `dpdk-intr` threads | left in the Keylane service CPU set, 0-11 |

An A/B/A Release experiment tested moving the Keylane main and `dpdk-intr` threads to CPUs 12-15:

| Layout | Client p99.99 |
|---|---:|
| Original | `0.703-0.807 ms` |
| Keylane main + `dpdk-intr` moved to 12-15 | `1.303-1.407 ms` |
| Restored | `0.703-0.807 ms` |

The IRQ CPUs are therefore not a good home for those Keylane threads on this host. They remain relatively idle on average, but colocating additional Keylane work with network interrupt handling made the tail worse.

In the clean no-log trace, worker CPUs 0-11 received no mlx5 interrupt increments. CPUs 12-15 handled about 9-11% softirq each and remained about 87-90% idle. Steal time was zero. Worker CPUs were approximately 70% user, 8% system, and 20% I/O wait during the read load.

## Reproducible tuning procedure

The measured improvement depends on keeping unrelated runnable tasks and network interrupts off the 12 Keylane worker CPUs. The following is the known-good procedure for this specific 16-logical-CPU host; CPU numbers must be recalculated on machines with a different topology.

### 1. Confirm the CPU topology

```bash
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
```

This host has eight physical cores and 16 SMT threads. CPUs 0-11 are assigned to Keylane and CPUs 12-15 to housekeeping and mlx5 IRQs. This is a logical-CPU partition, not complete physical-core isolation.

### 2. Move all mlx5 IRQs to CPUs 12-15

Stop or configure `irqbalance` first if it is installed, otherwise it can overwrite the manual affinity:

```bash
sudo systemctl disable --now irqbalance
```

Pin the mlx5 async IRQ to CPU 15 and distribute completion IRQs round-robin over CPUs 12-15:

```bash
async_irq=$(awk '/mlx5_async/{gsub(":", "", $1); print $1; exit}' /proc/interrupts)
printf '15\n' | sudo tee "/proc/irq/${async_irq}/smp_affinity_list"

i=0
while read -r irq; do
  cpu=$((12 + i % 4))
  printf '%s\n' "$cpu" | sudo tee "/proc/irq/${irq}/smp_affinity_list"
  i=$((i + 1))
done < <(awk '/mlx5_comp/{gsub(":", "", $1); print $1}' /proc/interrupts)
```

Verify the effective mapping and confirm under load that CPUs 0-11 do not receive mlx5 interrupt increments:

```bash
for irq in $(awk '/mlx5/{gsub(":", "", $1); print $1}' /proc/interrupts); do
  printf 'IRQ %s: ' "$irq"
  cat "/proc/irq/${irq}/smp_affinity_list"
done
```

The measured mapping was IRQ 58 on CPU 15 and IRQs 59-74 round-robin on CPUs 12-15.

### 3. Start Keylane inside CPUs 0-11

The five-minute result used this effective command and CPU constraint:

```bash
sudo systemd-run \
  --unit=keylane-spdk-12c-irq4.service \
  --property=AllowedCPUs=0-11 \
  /usr/bin/taskset -c 0-11 \
  /mnt/dev/keylane/bld-spdk-rebase-release/keylane \
    --bind=10.0.0.4 --port=6379 --metrics-port=9100 \
    --threads=12 --pin-workers \
    --busy-poll-us=20 \
    --foreground-budget-us=1000 \
    --spdk-max-completions-per-poll=8 \
    --data-file=spdk://fe8d:00:00.0/1 \
    --data-file=spdk://9913:00:00.0/1 \
    --logtostderr
```

The three tuning flags above equal the Release defaults and are shown explicitly to make the tested configuration unambiguous. Do not separately move the Keylane main or `dpdk-intr` thread to CPUs 12-15; that change worsened p99.99 from `0.703-0.807 ms` to `1.303-1.407 ms` in the A/B/A test.

### 4. Keep tmux, Codex, and benchmark launchers off worker CPUs

For an immediate process-level correction, apply affinity to every thread of each existing process:

```bash
sudo taskset -apc 12-15 PID
```

This must be repeated for each process and after restarts. The preferred persistent containment is the user slice:

```bash
# Trial until reboot
sudo systemctl set-property --runtime user-1000.slice AllowedCPUs=12-15

# Persist across reboot after validating the trial
sudo systemctl set-property user-1000.slice AllowedCPUs=12-15
```

Check both the cgroup constraint and current thread affinities:

```bash
systemctl show user-1000.slice -p AllowedCPUs -p EffectiveCPUs
ps -eLo pid,tid,psr,comm | rg 'tmux|codex'
```

Launch local control processes such as the remote benchmark SSH client through the same housekeeping set:

```bash
taskset -c 12-15 ssh 10.0.0.5 'COMMAND'
```

### 5. Validate before accepting a result

```bash
systemctl is-active keylane-spdk-12c-irq4.service
systemctl show keylane-spdk-12c-irq4.service -p AllowedCPUs -p EffectiveCPUs
redis-cli -h 10.0.0.4 -p 6379 PING
redis-cli -h 10.0.0.4 -p 6379 DBSIZE
```

For a clean Release latency run, also verify that trace markers are absent and inspect the journal over the exact load window:

```bash
strings /mnt/dev/keylane/bld-spdk-rebase-release/keylane | \
  rg 'cross-core-latency|wake-batch'
journalctl -u keylane-spdk-12c-irq4.service \
  --since 'START UTC' --until 'END UTC' --no-pager
```

The `strings` command should return no matches. The five-minute accepted run also had no journal entries during its measurement window.

### 6. Keep the settings that failed A/B out of the production recipe

- Do not use a fixed `busy-poll-us=100`; it consumed more CPU and worsened the tail.
- Do not raise `spdk-max-completions-per-poll` to 32 at this load; batches did not exceed eight and no repeatable benefit was measured.
- Do not lower `foreground-budget-us` to 100; it did not beat run-to-run variation.
- Do not place Keylane main/DPDK support threads on the four IRQ CPUs.
- Do not compare trace-build client latency directly with Release latency; use trace builds only to compare internal stages within the same instrumented binary.

## Scope and metric definitions

- Keylane endpoint: `10.0.0.4:6379`
- Keylane commit: `5ad8c1ff2fd0`
- Celer commit: `0a70d22`, including the SPSC cached-head, empty-poll fast rejects, and optional cross-core tracing used by the diagnostic build
- Data: 200,000,000 keys, approximately 500 GB, on two SPDK NVMe namespaces
- Client host: `10.0.0.5`
- Client: Valkey benchmark commit `382a134959b7`
- Workload: GET, 80 connections, 8 client threads, pipeline depth 1
- Rate: 100,000 requests/s globally
- Duration: 5-second warmup plus 300-second measurement for the latest run; earlier A/B runs used 30 seconds
- Key selection: 4,000,000-key CSV permutation over the populated key range
- Client p99.99: the smallest histogram upper bound whose cumulative count reaches `ceil(N * 0.9999)`; reported as an interval between adjacent printed bounds
- Per-worker p99.99: Celer histogram percentile within the trace reporting window, not the same population as the client percentile

The benchmark command was:

```bash
taskset -c 12-15 ssh 10.0.0.5 \
  "/tmp/keylane-valkey-profile-20260826/valkey-benchmark \
  -h 10.0.0.4 -p 6379 -c 80 --threads 8 -P 1 \
  --warmup 5 --duration 300 --rps 100000 --precision 3 \
  --dataset /tmp/keylane-valkey-profile-20260826/keys.csv \
  GET '__field:key__'"
```

At the end of the confirmation run, Keylane returned `PONG`, `DBSIZE` remained 200,000,000, and the service was active.

## Earlier profile and negative tuning results

The original CPU profile captured 74,932 samples without loss. Its largest self-time entries were:

| Function | Self CPU |
|---|---:|
| `celer::Worker::PollStorage()` | 20.33% |
| `celer::Worker::RunOnce(bool)` | 19.33% |
| `celer::Worker::DrainCrossCore()` | 9.40% |
| `nvme_pcie_qpair_process_completions` | 5.50% |
| `spdk_nvme_qpair_process_completions` | 5.38% |
| TCP/NAPI softirq path rooted at `_raw_spin_unlock_irqrestore` | 3.41% |
| scheduler path rooted at `finish_task_switch` | 1.16% |

The process recorded 2,679,718 context switches in 36 seconds. The benchmark issued one physical read for every GET: 3,500,612 GETs produced 3,500,612 storage reads. Uniform random keys were remote from the connection worker about 93.3% of the time, so most commands required both a request and reply cross-worker hop.

The following changes did not produce a repeatable improvement:

- Raising `spdk-max-completions-per-poll` from 8 to 32: observed batches stayed at or below 8, and A/B/A attributed the apparent first improvement to run variance.
- Raising `busy-poll-us` from 20 to 100: reduced wakeups but consumed 13.82 cores and worsened client p99.99 to about 3.2-3.5 ms; storage polling rounds were occasionally descheduled for 0.8-3.6 ms.
- Lowering `foreground-budget-us` from 1000 to 100: did not beat the surrounding A/B variation and was restored to 1000.
- Moving Keylane main and `dpdk-intr` threads onto CPUs 12-15: worsened p99.99 in the A/B/A experiment above.

These negative results constrain the next optimization: the bottleneck is not simply “poll more,” “poll more completions,” or “move every non-worker thread to the IRQ CPUs.”

## Isolation commands and persistence

The diagnostic process-level pinning used:

```bash
sudo taskset -apc 12-15 PID
```

This updates every existing thread under that process, but it must be applied separately to each current process and does not survive process restart. For durable isolation of user processes, constrain the user slice instead:

```bash
# Until reboot
sudo systemctl set-property --runtime user-1000.slice AllowedCPUs=12-15

# Persistent across reboot; omit --runtime
sudo systemctl set-property user-1000.slice AllowedCPUs=12-15
```

Before applying the slice rule permanently, verify that the benchmark launcher and any operational agents are intended to share CPUs 12-15 with the NIC IRQs. Remote benchmark execution was also launched through `taskset -c 12-15` so the local SSH process did not land on worker CPUs.

## Methodology and robustness checks

The diagnosis used four layers of evidence:

1. Normal Release A/B/A runs established the end-to-end client result without tracing overhead.
2. Compile-time Celer tracing split wake batching, request queueing, reply queueing, and SPDK I/O. The macro is off in the normal Release build.
3. A no-log measurement window verified that the 200-750 us queue tail persisted without `spdlog` output; the journal contained no application messages during the load.
4. BPF scheduler tracing connected the internal queue delay to the CPU owner delaying the target worker. Repeating the trace after tool isolation removed the per-worker p99.99 anomaly.

The host has 16 logical CPUs but only 8 physical cores, with SMT sibling pairs `0/1`, `2/3`, and so on, and one NUMA node. The kernel command line does not use `isolcpus`, `nohz_full`, or `rcu_nocbs`. IRQ affinity and `taskset` reduce identified interference but do not provide full kernel-level CPU isolation.

## Limitations

- The five-minute run has 3,000 observations in the top 0.01%, making it more stable than a 30-second result, but repeated long runs are still needed to quantify run-to-run confidence.
- Valkey benchmark reports histogram buckets rather than exact samples, so p99.99 is an interval.
- Trace-build end-to-end latency is not directly comparable to normal Release because timestamping and histogram updates add work.
- Process-level pinning is temporary and does not automatically cover restarted or newly created processes.
- The BPF owner counts establish a strong scheduling association for this host and run; they do not prove that every remaining end-to-end tail event has the same cause.
- CPU topology is SMT-heavy. A clean physical-core partition would require a different worker/IRQ layout or disabling SMT and should be benchmarked separately.

## Recommended next measurements

1. Repeat the five-minute Release trial several times and report the median and worst p99.99 bucket, rather than selecting one run.
2. Add low-overhead sampled request correlation across network ingress, cross-core request, SPDK completion, cross-core reply, and socket write to locate the remaining 0.5-0.8 ms end-to-end tail.
3. Make user-slice CPU isolation durable, then repeat the Release series after restarting tmux/Codex to verify that the containment survives.
4. Test a physical-core-aware layout because the current “12 worker CPUs” necessarily shares physical cores on an 8-core/16-thread machine.
5. Keep the default 20 us busy poll, completion batch 8, and foreground budget 1000 until a controlled A/B shows otherwise.

## Raw evidence index

- Original profile and first IRQ isolation: `perf_runs/valkey-keylane-100k-profile-20260826/`, `perf_runs/valkey-keylane-100k-12w-irq4-20260826/`
- Keylane main/DPDK thread A/B/A: `perf_runs/valkey-keylane-100k-release12-housekeeping-ab-20260826/`
- Direct wake-stage trace: `perf_runs/valkey-keylane-100k-wake-trace-20260826/`
- Clean and no-log controls: `perf_runs/valkey-keylane-100k-wake-trace-clean-20260826/`, `perf_runs/valkey-keylane-100k-wake-trace-nolog-20260826/`
- Worker scheduler histogram: `perf_runs/valkey-keylane-100k-sched-wakeup-trace-20260826/bpftrace.log`
- Slow-wake CPU-owner trace: `perf_runs/valkey-keylane-100k-sched-slow-owner2-20260826/bpftrace.log`
- Trace build after tool isolation: `perf_runs/valkey-keylane-100k-isolated-tools-clean-20260826/`
- Formal Release after tool isolation: `perf_runs/valkey-keylane-100k-release12-tools-isolated-20260826/client.log`
- Latest formal Release confirmation: `perf_runs/valkey-keylane-100k-release12-tools-isolated-confirm-20260826/result-summary.md`
- Five-minute trace-off Release run: `perf_runs/valkey-keylane-100k-release12-tools-isolated-5min-20260826/client.log` and `result-summary.md`
- Five-minute unlimited Release run: `perf_runs/valkey-keylane-unlimited-release12-tools-isolated-5min-20260826/client.log`, `server-pidstat.log`, and `result-summary.md`
- Five-minute io_uring raw-block run: `perf_runs/valkey-keylane-iouring-raw-unlimited-12c-irq4-5min-20260826/client.log`, `server-pidstat.log`, `mpstat.log`, IRQ snapshots, and `result-summary.md`

The 02:47 UTC confirmation output was captured interactively; its command, summary, and rank/count evidence are preserved in its confirmation summary rather than as a full progress log. The five-minute run retains the full client output.

## Further questions

- How much of the remaining p99.99 is client-side rate-limiter scheduling versus network/server latency?
- Does a physical-core-aware 8-worker layout have a lower tail than 12 SMT workers at the same 100K QPS?
- Can low-rate sampling preserve request-stage correlation without moving the Release latency distribution?
