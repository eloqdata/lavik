<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Running Lavik with SPDK

Lavik can access one or more local NVMe namespaces directly through SPDK.
Networking remains on celer/io_uring; only the storage backend changes. This
guide is hardware-independent; benchmark-host details belong in
[`PERF_SESSION_HANDOFF.md`](../../PERF_SESSION_HANDOFF.md).

Binding an NVMe controller to a userspace driver removes every namespace on
that controller from the kernel. Use a dedicated data controller, never an OS
or workspace disk.

> **Data safety:** verify that none of the selected controller's namespaces is
> mounted, used as swap, part of LVM/RAID, or needed by another service.
> Binding does not erase a disk, but starting Lavik on an unlabeled namespace
> initializes it as Lavik storage. Always pass an explicit `PCI_ALLOWED`
> allowlist to SPDK's setup script.

## 1. Fetch the complete source tree

SPDK is pinned as a nested celer submodule, so initialize recursively:

```sh
git clone --recurse-submodules REPOSITORY_URL lavik
cd lavik
git submodule update --init --recursive
```

SPDK provides a distribution-aware dependency installer. Review it before
running it on a managed host:

```sh
sudo celer/third_party/spdk/scripts/pkgdep.sh
```

Lavik additionally requires CMake 3.20 or newer, Ninja, a C++23 compiler,
pkg-config, OpenSSL development files, and NUMA development files.

## 2. Build the SPDK variant

```sh
cmake -S . -B bld-spdk -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON \
  -DLAVIK_ENABLE_OPT=ON \
  -DLAVIK_WITH_SPDK=ON
cmake --build bld-spdk -j "$(nproc)"
```

`LAVIK_WITH_SPDK=ON` statically links the pinned SPDK/DPDK storage stack. A
normal io_uring build uses `-DLAVIK_WITH_SPDK=OFF` in a separate build
directory.

## 3. Select controllers and namespaces safely

Inventory the machine before changing any driver:

```sh
lsblk -d -o NAME,SIZE,MODEL,SERIAL,TRAN
lsblk -o NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS
sudo nvme list
lspci -Dnnk | grep -A3 -i 'non-volatile memory'
swapon --show
```

Record all three identities for every selected disk:

- model and serial number;
- PCI address (BDF), such as `0000:01:00.0`;
- namespace ID (NSID), commonly `1`, as reported by `nvme list-ns` or
  `nvme id-ns`.

The Lavik URI is `spdk://PCI_BDF/NSID`; for example:

```text
spdk://0000:01:00.0/1
```

Do not derive identity from `/dev/nvmeXnY`: kernel enumeration numbers can
change after rebinding or rebooting.

## 4. Configure IOMMU, hugepages, and vfio-pci

Production hosts should enable Intel VT-d or AMD-Vi/IOMMU in firmware and in
the kernel. Load `vfio-pci`, then bind only an explicit allowlist. Set these
example values for the host being configured:

```sh
SPDK_BDFS='0000:01:00.0'
SPDK_HUGEMEM_MB=4096
SPDK_TARGET_USER="$(id -un)"

sudo modprobe vfio-pci
sudo env \
  PCI_ALLOWED="$SPDK_BDFS" \
  DRIVER_OVERRIDE=vfio-pci \
  TARGET_USER="$SPDK_TARGET_USER" \
  HUGEMEM="$SPDK_HUGEMEM_MB" \
  celer/third_party/spdk/scripts/setup.sh
celer/third_party/spdk/scripts/setup.sh status
```

Size hugepage memory for all simultaneously running SPDK processes. A useful
starting point is:

```text
sum(registered-buffer-mb-per-worker × worker-count per process)
+ expected adaptive overflow high-water memory
+ SPDK/DPDK overhead
+ safety margin
```

Round upward generously, verify free hugepages after startup, and account for
NUMA placement. `scripts/setup.sh --help` documents `HUGENODE`, `HUGEPGSZ`,
`NRHUGE`, and persistent hugepage options. On NUMA hosts, keep the NVMe PCI
device, hugepages, and Lavik worker CPUs on the same node when possible.

After setup, use `lspci -Dnnk` to confirm that only the selected controllers
use `vfio-pci`. Their kernel `/dev/nvme*` namespaces should no longer appear.
Run Lavik as `SPDK_TARGET_USER`, and configure an adequate locked-memory
limit (commonly `LimitMEMLOCK=infinity` in systemd or `ulimit -l unlimited` in
an administrative launch wrapper).

For an isolated development VM without an IOMMU only, unsafe no-IOMMU mode may
be required:

```sh
sudo sh -c 'echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode'
```

Do not use that mode on production or multi-tenant systems; it removes DMA
isolation.

## 5. Start and validate one Lavik instance

Choose deployment-specific addresses, ports, CPUs, and the URI discovered
above. Current defaults include the production tuning used by this baseline,
so only deployment-specific values are required:

```sh
LAVIK_BIND_IP=127.0.0.1
LAVIK_CPUS=0-7
LAVIK_SPDK_URI='spdk://0000:01:00.0/1'

taskset -c "$LAVIK_CPUS" ./bld-spdk/lavik \
  --bind="$LAVIK_BIND_IP" \
  --data-file="$LAVIK_SPDK_URI"
```

Redis traffic uses the default port 6379. Prometheus metrics are disabled by
default; add `--metrics-port=9100` only when a metrics collector is needed.

Lavik pins workers by default. It reads the inherited affinity mask and maps
worker 0 to the first allowed CPU, worker 1 to the second, and so on. Thus
`taskset -c 8-15 ...` automatically creates eight workers and maps them to CPUs
8 through 15 rather than
allowing all eight workers to migrate across that set. Startup fails when the
allowed CPU count is smaller than the worker count. Use `--no-pin-workers` only
when operating-system scheduling is intentionally preferred.

Use `taskset`, cpusets, or the service manager to define the process CPU set.
Do not copy a benchmark host's CPU list without checking NUMA topology. SPDK
poll-mode workers are particularly sensitive to migration and preemption while
I/O is outstanding.

For latency-sensitive deployments, also reserve physical cores for the network
device's completion IRQs and keep them disjoint from the Lavik worker set.
See [Network IRQ Affinity Tuning for Tail Latency](../operations/irq-affinity-tuning.md).

`--spdk-max-completions-per-poll=8` bounds one event-loop poll's completion
work, preventing a completion burst from monopolizing a worker. The budget is
shared fairly across all controller qpairs owned by that worker.
`--spdk-foreground-pre-poll-us=5` lets newly arrived network and cross-worker
foreground work run for a small bounded slice before the storage completion
poll. Both values are tunable; `0` restores unbounded completion draining or
disables the pre-poll slice, respectively.

The default maximum storage submission is 128 KiB. Tomb raider runs once every
24 hours by default, so it cannot overlap a five-minute benchmark started from
a fresh process. Defrag remains enabled with its default runtime controls.
Payload CRC32C verification is mandatory on reads.

`--registered-buffer-mb-per-worker` is a budget **per worker**. With 256 MiB and eight
workers, a process can reserve roughly 2 GiB of fixed storage buffers, before
adaptive overflow and other memory. The overflow read pool grows to the
observed per-worker concurrency high-water mark when fixed read buffers are
busy, then reuses those DMA buffers instead of allocating on every later miss.
Within each worker budget, `--storage-write-buffers-per-worker` reserves the
8 MiB storage write buffers and defaults to four. Replication backlog chunks
are ordinary process memory and do not consume registered or DMA buffers.
`--storage-read-buffer-kb` controls the registered read
payload size and defaults to 1024 KiB (each slot also has 4 KiB of headroom and
tailroom). The remaining budget determines the registered read-slot count.
Startup fails if the budget cannot fit the configured storage write pool.

On io_uring, buffer registration itself is probed at runtime. Kernels before
5.12 normally charge it to the process `RLIMIT_MEMLOCK`; if registration fails,
Lavik logs the requested per-worker bytes plus the runtime soft/hard limits
and keeps the same fixed-size reusable pools on the unregistered-I/O path.
Linux 5.12+ with native io_uring workers uses cgroup memory accounting instead,
so Lavik does not incorrectly cap those machines from `ulimit -l`. SPDK uses
its DMA-addressability check and fails startup if a pool buffer is not DMA
addressable; no SPDK source modification is required.

A finite purge delay is recommended for recovery-heavy deployments. A 60-second
delay retains recently freed pages for reuse while allowing recovery's arena
high-water memory to return to the OS. `-1` can improve extreme tail latency by
avoiding recommit faults, but it may retain tens of GiB after a large recovery
and provides no proactive response to Linux or cgroup memory pressure.

Wait for `storage recovery complete` before sending traffic, then validate:

```sh
redis-cli -h "$LAVIK_BIND_IP" -p "$LAVIK_PORT" PING
redis-cli -h "$LAVIK_BIND_IP" -p "$LAVIK_PORT" DBSIZE
redis-cli -h "$LAVIK_BIND_IP" -p "$LAVIK_PORT" DEFRAG STATUS
curl "http://$LAVIK_BIND_IP:$LAVIK_METRICS_PORT/metrics"
```

## 6. Use multiple NVMe devices

SPDK supports multiple NVMe controllers and multiple namespaces. Allow every
required controller BDF, and repeat `--data-file` for every namespace:

```sh
SPDK_BDFS='0000:01:00.0 0000:02:00.0'
sudo env \
  PCI_ALLOWED="$SPDK_BDFS" \
  DRIVER_OVERRIDE=vfio-pci \
  TARGET_USER="$(id -un)" \
  HUGEMEM=8192 \
  celer/third_party/spdk/scripts/setup.sh

./bld-spdk/lavik \
  --port=6379 \
  --metrics-port=9100 \
  --threads=8 \
  --registered-buffer-mb-per-worker=256 \
  --data-file=spdk://0000:01:00.0/1 \
  --data-file=spdk://0000:02:00.0/1
```

Namespaces on one controller share that controller's hardware resources and
failure domain; namespaces on distinct controllers have separate PCI paths.
Lavik treats each namespace URI as one storage device in either case.

At startup Lavik reads the negotiated I/O queue count from every physical
controller, then assigns controller qpairs to workers deterministically. One
worker uses one qpair for all configured namespaces on the same controller;
that qpair is submitted and polled only by that worker. Controller owner counts
are weighted by usable namespace capacity and capped by the controller's
reported qpair count. Startup fails before recovery when the available qpairs
cannot cover every worker (and every configured controller).

The SPDK backend intentionally has stronger storage affinity than the io_uring
backend. A worker opens and allocates only from namespaces whose controller it
owns. If all of those namespaces are full, the allocation reports `FULL`; it
does not fall back to a controller for which the worker has no qpair. Existing
blocks recovered after a worker-topology change remain owned by a worker with a
qpair for their source controller. Normal overwrite and defrag paths relocate
live data onto the current key owner's local controller over time. The
io_uring build retains its all-device allocation fallback.

All paths in one process must include the complete persisted Lavik storage
set. To add fresh namespaces, stop Lavik and restart it with every existing
URI plus the zero-label new URIs. The fixed metadata on new namespaces is
initialized automatically; existing data remains in place. Expansion is safe
to retry after interruption. A foreign initialized namespace is rejected, so
clear its Lavik label before intentionally reusing it as a new member.
Argument order does not define persistent device identity after initialization.
Online addition and device removal are not implemented.

To expand an existing SPDK set, stop Lavik before changing driver ownership.
If the new namespace contains an old Lavik storage set, temporarily expose
it through the kernel, verify its PCI-to-device mapping, and clear only its
label before binding it to VFIO. Never clear an existing member being kept.

```sh
# Example only: resolve these names and BDFs on the current host.
readlink -f /sys/class/block/nvme1n1/device/device
lsblk -o NAME,PATH,SIZE,MODEL,SERIAL,MOUNTPOINTS /dev/nvme1n1
findmnt -rn -S /dev/nvme1n1
sudo fuser -v /dev/nvme1n1
sudo blkdiscard -z -f --offset 0 --length 4096 /dev/nvme1n1

SPDK_BDFS='0000:01:00.0 0000:02:00.0'
sudo env \
  PCI_ALLOWED="$SPDK_BDFS" \
  DRIVER_OVERRIDE=vfio-pci \
  TARGET_USER="$(id -un)" \
  HUGEMEM=8192 \
  celer/third_party/spdk/scripts/setup.sh

./bld-spdk/lavik \
  --port=6379 \
  --metrics-port=9100 \
  --threads=8 \
  --registered-buffer-mb-per-worker=256 \
  --data-file=spdk://0000:01:00.0/1 \
  --data-file=spdk://0000:02:00.0/1
```

Wait for both device lines, the expansion log, and recovery completion. Every
later start must repeat both URIs. The operation initializes only fixed
metadata on the new namespace; it does not rewrite records on existing
namespaces.

`--defrag-max-active-per-device=N` and runtime `DEFRAG MAX-ACTIVE N` apply to
each device independently. With two devices and `N=1`, at most one relocation
may run on each device, for two total.

## 7. Run separate instances for an A/B test

Separate instances need different Redis and metrics ports and must never open
the same namespace. Keep all performance-relevant options equal. For a clean
backend comparison, disable tomb-raider, start with defrag paused, verify equal
logical datasets, and drive only one instance at a time. Idle workers park when
they have no requests or background work.

Configure Prometheus with both metrics endpoints and select both instances in
the provisioned Grafana dashboard. Its comparison queries retain the
`instance` label when calculating histogram quantiles; aggregating buckets
across servers produces an invalid A/B latency curve.

## 8. Restore controllers to the kernel

Stop every process using the selected namespaces before resetting their
drivers:

```sh
SPDK_BDFS='0000:01:00.0 0000:02:00.0'
sudo env PCI_ALLOWED="$SPDK_BDFS" \
  celer/third_party/spdk/scripts/setup.sh reset
```

Verify with `lspci -Dnnk`, `nvme list`, and `lsblk` that the kernel `nvme`
driver and the expected model/serial numbers returned. Re-discover device names
rather than assuming the previous `/dev/nvmeXnY` numbering.

For storage-set layout and recovery rules, see
[Multi-Device Storage](../operations/multi-device-storage.md).
