# Keylane 12-worker CPU and IRQ affinity

Date: 2026-08-26 UTC

## Validated result

On this 16-logical-CPU, eight-physical-core host, the validated layout is:

| Work | CPUs |
|---|---|
| Keylane service cgroup and 12 pinned workers | 0-11 |
| mlx5 network IRQs | 12-15 |
| io_uring raw-block NVMe managed IRQs | one queue per logical CPU, 0-15; not manually movable |
| tmux, Codex, benchmark SSH launcher and other user housekeeping | 12-15 |

With the SPDK backend, this layout sustained 316,412 QPS at pipeline one. At 100K QPS, a five-minute Release run measured p99.9 `0.375-0.399 ms` and p99.99 `0.455-0.479 ms`.

BPF tracing showed why the user-process affinity matters: before isolation, tmux/Codex accounted for 93 of 95 worker wake-to-run delays above 100 us. After isolation, all 12 workers' request/reply queue p99.99 values fell to at most 30 us.

## 1. Inspect topology and current IRQs

```bash
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
rg 'mlx5|nvme' /proc/interrupts
```

CPU numbers in this document are specific to the tested host. Recalculate them on a different topology.

## 2. Keep irqbalance from undoing manual affinity

If `irqbalance` is installed, either configure its banned CPU mask or stop it:

```bash
sudo systemctl disable --now irqbalance
```

The tested host did not have an active irqbalance service.

## 3. Put mlx5 IRQs on CPUs 12-15

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

The measured SPDK-run mapping was async IRQ 58 on CPU 15 and completion IRQs 59-74 round-robin on CPUs 12-15.

## 4. Account for managed NVMe IRQs in raw io_uring

SPDK does not use kernel NVMe IRQs. After rebinding the controllers to the Linux `nvme` driver, this kernel creates one managed MSI-X I/O queue per logical CPU. Linux rejects writes to those queues' `smp_affinity_list`; each completion queue follows its assigned submission CPU. They therefore cannot all be moved to CPUs 12-15 with the mlx5 interrupts.

Inspect the actual managed mapping rather than assuming it is configurable:

```bash
for irq in $(awk '/nvme/{gsub(":", "", $1); print $1}' /proc/interrupts | sort -nu); do
  printf 'IRQ %s: ' "$irq"
  cat "/proc/irq/${irq}/effective_affinity_list"
done
```

The raw-block test retained the same Keylane, network-IRQ, tool, and client affinity as SPDK. Its NVMe completion IRQs ran on worker CPUs 0-11 by kernel design and are part of the measured io_uring backend cost. During the five-minute run, CPUs 0-11 each received about 7.2-7.7 million NVMe interrupts; CPUs 12-15 received none apart from 19 setup/admin events on CPU 12.

## 5. Start Keylane in CPUs 0-11

SPDK example:

```bash
sudo systemd-run \
  --unit=keylane-spdk-12c-irq4.service \
  --property=AllowedCPUs=0-11 \
  --property=LimitMEMLOCK=infinity \
  /usr/bin/taskset -c 0-11 \
  /mnt/dev/keylane/bld-spdk-rebase-release/keylane \
    --bind=10.0.0.4 --port=6379 --metrics-port=9100 \
    --threads=12 --pin-workers \
    --busy-poll-us=20 --foreground-budget-us=1000 \
    --spdk-max-completions-per-poll=8 \
    --data-file=spdk://fe8d:00:00.0/1 \
    --data-file=spdk://9913:00:00.0/1 \
    --logtostderr
```

Raw io_uring uses the same CPU and runtime settings but passes stable Linux block-device paths:

```bash
sudo systemd-run \
  --unit=keylane-iouring-raw-12c-irq4.service \
  --property=AllowedCPUs=0-11 \
  --property=LimitMEMLOCK=infinity \
  /usr/bin/taskset -c 0-11 \
  /mnt/dev/keylane/bld-block-device-fix-release/keylane \
    --bind=10.0.0.4 --port=6379 --metrics-port=9100 \
    --threads=12 --pin-workers \
    --busy-poll-us=20 --foreground-budget-us=1000 \
    --data-file=/dev/disk/by-id/DEVICE0 \
    --data-file=/dev/disk/by-id/DEVICE1 \
    --logtostderr
```

Do not move the Keylane main or `dpdk-intr` thread to CPUs 12-15. That change worsened Release p99.99 from `0.703-0.807 ms` to `1.303-1.407 ms` in an A/B/A test.

## 6. Isolate local user processes

Immediate, process-level method for each existing PID:

```bash
sudo taskset -apc 12-15 PID
```

Preferred cgroup method:

```bash
# Until reboot
sudo systemctl set-property --runtime user-1000.slice AllowedCPUs=12-15

# Persistent after the trial is validated
sudo systemctl set-property user-1000.slice AllowedCPUs=12-15
```

Process-level `taskset` must be repeated for every process and after restarts. The user-slice rule covers future processes in that slice.

Launch local SSH control processes on the housekeeping set as well:

```bash
taskset -c 12-15 ssh 10.0.0.5 'COMMAND'
```

## 7. Verify the effective state

```bash
systemctl show KEYLANE.service -p AllowedCPUs -p EffectiveCPUs
ps -eLo pid,tid,psr,comm | rg 'keylane|tmux|codex'

for irq in $(awk '/mlx5|nvme/{gsub(":", "", $1); print $1}' /proc/interrupts | sort -nu); do
  printf 'IRQ %s: ' "$irq"
  cat "/proc/irq/${irq}/smp_affinity_list"
done
```

Compare `/proc/interrupts` snapshots before and after load. Both accepted backends had zero mlx5 IRQ increments on CPUs 0-11. NVMe IRQ increments on worker CPUs are expected for io_uring raw block and must be reported rather than treated as an affinity failure.

## Settings retained after A/B

- `busy-poll-us=20`
- `foreground-budget-us=1000`
- SPDK `spdk-max-completions-per-poll=8`
- trace disabled for formal latency tests

Fixed `busy-poll-us=100`, SPDK completion batch 32, foreground budget 100, and moving Keylane support threads to IRQ CPUs did not improve the controlled tests and are not part of this recipe.

Detailed evidence and raw-run paths are in [the p99.99 diagnosis](keylane-valkey-100k-p9999-profile-2026-08-26.md).

## Raw-block validation result

With the layout above, io_uring raw block sustained 296,266.47 QPS for five minutes at pipeline one. It measured p99.9 `0.511-0.535 ms` and p99.99 `0.615-0.647 ms`. SPDK under the same user-visible CPU layout sustained 316,412.41 QPS with p99.99 `0.599-0.631 ms`; raw block was 6.37% slower in throughput.

Raw evidence is under `perf_runs/valkey-keylane-iouring-raw-unlimited-12c-irq4-5min-20260826/`.
