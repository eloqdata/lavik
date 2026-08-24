# Network IRQ Affinity Tuning for Tail Latency

Keylane pins worker threads, but Linux network interrupts are scheduled
independently. If a NIC completion IRQ runs on a Keylane worker CPU, it can
interrupt the worker while the worker is processing a request or polling SPDK.
This interference can increase average latency and lower tail percentiles.
Reserve physical cores for the NIC and bind its IRQs away from Keylane workers
when latency matters.

IRQ isolation is not a universal fix for extreme tails. On the reference host,
it improved average GET latency by 8.3% and p99.9 by 4.9%, while p99.99 remained
within normal run-to-run variation. Always validate the percentiles required by
the deployment.

## Reserve a scheduler housekeeping CPU

Before attempting strict IRQ isolation, leave one logical CPU outside the
Keylane worker affinity set. The spare CPU gives Linux a low-load destination
for movable system work such as SSH sessions, monitoring agents, command-line
tools, and unbound kernel workqueues. This is scheduler headroom, not explicit
migration: it is not necessary to find and pin every system process for this
first experiment.

On the 16-vCPU Azure reference VM, the tested configuration used 15 workers on
CPUs 0-14 and left CPU15 out of Keylane's allowed CPU set:

```sh
taskset -c 0-14 ./bld-spdk/keylane --threads=15 OTHER_OPTIONS
```

Both parts are required. `taskset` prevents the process from running on CPU15,
and `--threads=15` prevents Keylane from creating a sixteenth worker. Keylane
then pins worker `i` to allowed CPU `i`. Linux CFS load balancing naturally
prefers the otherwise idle CPU15 for movable housekeeping tasks.

Verify the process and worker placement after startup:

```sh
keylane_pid=$(pgrep -xo keylane)
taskset -pc "$keylane_pid"
ps -T -p "$keylane_pid" -o pid,tid,psr,stat,comm
```

This does **not** strictly isolate CPUs 0-14. Per-CPU kernel threads can still
run there, and a NIC IRQ mapped to a worker CPU can still interrupt its worker.
Strict isolation requires a separate cpuset/cgroup or boot-time CPU-isolation
configuration in addition to deliberate IRQ, RPS, XPS, and workqueue placement.
Use the one-spare-CPU setup first because it is reversible and showed a large
tail-latency benefit without changing global kernel policy.

On 2026-08-24, with `htop` stopped, the same binary and 400M-key SPDK dataset,
the original per-queue mlx5 IRQ distribution, and a remote memtier client fixed
at 400k GET/s, the 60-second A/B was:

| Server placement | GET/s | Average | p99 | p99.9 | p99.99 | runnable off-CPU >=2 ms | >=4 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 workers, CPUs 0-15 | 399,339 | 0.323 ms | 0.647 ms | 3.167 ms | 4.447 ms | 540 | 10 |
| 15 workers, CPUs 0-14 | 399,359 | 0.335 ms | 0.583 ms | 0.791 ms | 1.639 ms | 5 | 1 |

Leaving CPU15 available reduced p99.99 by 63.1% and runnable worker off-CPU
events of at least 2 ms by 99.1%, while throughput was unchanged. Average
latency increased 3.6% because each remaining worker handled more traffic. The
only >=4 ms runnable interruption in the 15-worker run was a 6.35 ms
`ksoftirqd/12` interval, illustrating that this setup is not strict isolation.

Do not concentrate every NIC completion IRQ onto the single spare CPU without
first validating packet capacity. On this VM, moving all 16 mlx5 completion
IRQs to CPU15 limited the same test to about 216k GET/s. Restoring the original
one-queue-per-CPU IRQ mapping recovered 400k GET/s while retaining the benefit
of scheduler headroom on CPU15.

A 300-second, fixed-100k GET/s follow-up compared scheduler headroom with
strict worker/IRQ separation. The 12-worker configuration ran Keylane on CPUs
0-11 and distributed the 16 mlx5 completion IRQs round-robin over CPUs 12-15.
The 15-worker configuration ran Keylane on CPUs 0-14 and retained the original
one-queue-per-CPU IRQ mapping. All other server and client settings were the
same:

| Placement at 100k GET/s | GET/s | Average | p99 | p99.9 | p99.99 | runnable off-CPU >=1 ms | >=2 ms | >=4 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 15 workers, IRQs distributed on CPUs 0-15 | 99,843 | 0.326 ms | 0.655 ms | 1.215 ms | 1.807 ms | 20 | 6 | 0 |
| 12 workers, IRQs isolated on CPUs 12-15 | 99,845 | 0.334 ms | 0.735 ms | 1.263 ms | 1.943 ms | 3 | 2 | 1 |

Strict IRQ separation reduced runnable scheduling interruptions, proving that
the placement worked, but p99.99 regressed 7.5% and average latency regressed
2.6%. At this load, the benefit from eliminating IRQ interference did not
offset the cost of reducing worker parallelism and changing the SPDK qpair
owner layout. Prefer 15 workers with the original distributed IRQ mapping on
this 16-vCPU VM unless a different workload demonstrates a repeatable benefit
from strict IRQ isolation.

## 1. Inspect physical CPU topology

Logical CPU numbers do not necessarily identify separate physical cores. Find
SMT siblings before selecting worker and IRQ CPU sets:

```sh
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE

for cpu_id in /sys/devices/system/cpu/cpu[0-9]*; do
  printf '%s ' "${cpu_id##*/}"
  cat "$cpu_id/topology/thread_siblings_list"
done
```

Reserve complete physical cores. For example, if CPUs 14 and 15 are SMT
siblings, together they are one physical core, not two.

Keep Keylane and its NIC IRQ set in the same NUMA node when possible. For
multi-socket systems, also check the NIC's NUMA node:

```sh
numactl --hardware
cat /sys/class/net/NET_DEVICE/device/numa_node
```

## 2. Identify the network interface and IRQs

Find the interface carrying client traffic and its driver:

```sh
ip route get CLIENT_IP
ethtool -i NET_DEVICE
ethtool -l NET_DEVICE
```

MSI-X IRQ numbers can change after a reboot or driver reload. Discover them
from sysfs instead of copying numbers from another machine:

```sh
find -L /sys/class/net/NET_DEVICE/device/msi_irqs \
  -mindepth 1 -maxdepth 1 -printf '%f\n' | sort -n

grep -E 'NET_DRIVER|NET_DEVICE' /proc/interrupts
```

Some virtual NICs do not expose `msi_irqs` through the interface's sysfs
device. In that case, select the IRQs by the exact driver queue names in
`/proc/interrupts`, and verify their counters under network load.

Check whether another service may overwrite manual affinity:

```sh
systemctl is-active irqbalance
systemctl is-enabled irqbalance
```

On a dedicated Keylane host, either configure irqbalance to exclude the
reserved CPUs or disable it before applying manual affinity. Do not leave
irqbalance active with an unrestricted policy and assume the manual mapping is
persistent.

Also inspect RPS and XPS. Non-zero masks can move packet processing away from
the hardware IRQ CPU and must be included in the experiment:

```sh
for queue_path in /sys/class/net/NET_DEVICE/queues/rx-*; do
  printf '%s rps=' "${queue_path##*/}"
  cat "$queue_path/rps_cpus"
done

for queue_path in /sys/class/net/NET_DEVICE/queues/tx-*; do
  printf '%s xps=' "${queue_path##*/}"
  cat "$queue_path/xps_cpus"
done
```

Do not change RPS/XPS blindly on a shared host. Record their original values
and treat any change as a separate A/B variable.

## 3. Bind NIC IRQs to reserved CPUs

The following Bash example discovers all MSI IRQs for one NIC and distributes
them round-robin across CPUs 12-15. Replace the interface and CPU list with the
values selected from the host topology:

```bash
net_device=eth0
irq_cpus=(12 13 14 15)

mapfile -t irq_ids < <(
  find -L "/sys/class/net/$net_device/device/msi_irqs" \
    -mindepth 1 -maxdepth 1 -printf '%f\n' | sort -n
)

if (( ${#irq_ids[@]} == 0 )); then
  echo "no MSI IRQs found for $net_device" >&2
  exit 1
fi

for index in "${!irq_ids[@]}"; do
  irq_id=${irq_ids[index]}
  target_cpu=${irq_cpus[index % ${#irq_cpus[@]}]}
  printf '%s\n' "$target_cpu" |
    sudo tee "/proc/irq/$irq_id/smp_affinity_list" >/dev/null
done
```

Start Keylane with a disjoint CPU set. With 12 workers on CPUs 0-11:

```sh
taskset -c 0-11 ./bld-spdk/keylane --threads=12 OTHER_OPTIONS
```

Keylane pins worker `i` to allowed CPU `i` by default. The number of allowed
CPUs must be at least the worker count.

## 4. Verify the effective placement under load

Check both the configured and effective affinity:

```sh
for irq_id in "${irq_ids[@]}"; do
  printf 'irq=%s configured=' "$irq_id"
  cat "/proc/irq/$irq_id/smp_affinity_list"
  printf ' effective='
  cat "/proc/irq/$irq_id/effective_affinity_list"
done
```

For managed MSI-X interrupts, `effective_affinity_list` may retain the old CPU
until the next interrupt. Generate normal client traffic, then confirm that
IRQ counters grow only on the reserved CPUs:

```sh
grep -E 'NET_DRIVER|NET_DEVICE' /proc/interrupts
mpstat -P 12,13,14,15 1
ethtool -S NET_DEVICE | grep -Ei 'drop|miss|discard|error'
```

Compare `/proc/net/softnet_stat` before and after the test as well. Growth in
its dropped or time-squeeze fields means the network CPU set is too small or
packet processing is not keeping up.

The required headroom depends on packet rate, response size, offload settings,
and the latency objective. Low average softirq utilization is necessary but
not sufficient: reject a configuration if p99.9 or p99.99 regresses even when
the reserved CPUs appear mostly idle.

## 5. Benchmark without confounding variables

Use an A/B/A sequence:

1. Run at least three identical trials with the original IRQ placement.
2. Change only IRQ affinity and repeat the same trials.
3. Restore the original placement and run confirmation trials.

Keep worker count, client connections, QPS, key distribution, value size,
storage devices, allocator policy, defrag state, and monitoring unchanged.
Report medians and individual p99.9/p99.99 runs instead of selecting the best
run. Do not avoid normal allocator purge intervals if purge is part of the
production configuration.

When deciding how many cores to reserve, first change only IRQ placement while
leaving the worker count fixed. Increasing workers at the same time also
changes cross-core traffic, SPDK qpair count, and scheduler behavior, so it is
not an IRQ-only comparison.

## 6. One or two physical cores for IRQs

One reserved physical core is often enough at moderate network rates, but it
is not automatically lower latency than two cores:

- one core reduces IRQ migration and can free another core for other work;
- concentrating all queues can serialize bursts and worsen extreme tails;
- adding Keylane workers on the freed core changes more than IRQ capacity.

Test in two stages. First, keep the Keylane worker count unchanged and move all
NIC IRQs from two reserved physical cores onto the SMT siblings of one physical
core. If tail latency remains stable and the IRQ core has ample headroom, test
additional Keylane workers separately.

On the 2026-08-11 reference run, four logical IRQ CPUs (two physical cores)
each used only 5.03--5.78% softirq and remained at least 92.3% idle at 100k
GET/s. A combined follow-up used 14 workers and concentrated all IRQs on the
two SMT siblings of one physical core. Although those IRQ CPUs remained about
72% idle and the NIC had no drops, p99.9 regressed from a 0.991 ms median to
1.135 ms and p99.99 from 1.687 ms to 4.127 ms. That A/B also changed worker and
SPDK qpair count, so it cannot assign the regression to IRQ concentration
alone, but it rejects that combined configuration for low tail latency.

## 7. Make affinity persistent

`smp_affinity_list` is runtime state. Rebooting, reloading the NIC driver, or
changing queue count can renumber IRQs and discard the mapping. Apply affinity
from a boot-time service after the network device appears. The service should:

1. discover current IRQ numbers from sysfs;
2. validate that every configured CPU is online and belongs to the intended
   physical core set;
3. write `smp_affinity_list`;
4. log both configured and effective mappings;
5. fail visibly when the interface exposes no matching IRQs.

Never persist benchmark-host IRQ numbers as universal configuration.
