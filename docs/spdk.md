# Running Keylane with SPDK

Keylane can access one or more local NVMe namespaces directly through SPDK.
Networking remains on celer/io_uring; only the storage backend changes. This
guide is hardware-independent; benchmark-host details belong in
[`PERF_SESSION_HANDOFF.md`](../PERF_SESSION_HANDOFF.md).

Binding an NVMe controller to a userspace driver removes every namespace on
that controller from the kernel. Use a dedicated data controller, never an OS
or workspace disk.

> **Data safety:** verify that none of the selected controller's namespaces is
> mounted, used as swap, part of LVM/RAID, or needed by another service.
> Binding does not erase a disk, but starting Keylane on an unlabeled namespace
> initializes it as Keylane storage. Always pass an explicit `PCI_ALLOWED`
> allowlist to SPDK's setup script.

## 1. Fetch the complete source tree

SPDK is pinned as a nested celer submodule, so initialize recursively:

```sh
git clone --recurse-submodules REPOSITORY_URL keylane
cd keylane
git submodule update --init --recursive
```

SPDK provides a distribution-aware dependency installer. Review it before
running it on a managed host:

```sh
sudo celer/third_party/spdk/scripts/pkgdep.sh
```

Keylane additionally requires CMake 3.20 or newer, Ninja, a C++23 compiler,
pkg-config, OpenSSL development files, and NUMA development files.

## 2. Build the SPDK variant

```sh
cmake -S . -B bld-spdk -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON \
  -DKEYLANE_ENABLE_OPT=ON \
  -DKEYLANE_WITH_SPDK=ON
cmake --build bld-spdk -j "$(nproc)"
```

`KEYLANE_WITH_SPDK=ON` statically links the pinned SPDK/DPDK storage stack. A
normal io_uring build uses `-DKEYLANE_WITH_SPDK=OFF` in a separate build
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

The Keylane URI is `spdk://PCI_BDF/NSID`; for example:

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
sum(registered-buffer-mb × worker-count per process)
+ expected adaptive overflow high-water memory
+ SPDK/DPDK overhead
+ safety margin
```

Round upward generously, verify free hugepages after startup, and account for
NUMA placement. `scripts/setup.sh --help` documents `HUGENODE`, `HUGEPGSZ`,
`NRHUGE`, and persistent hugepage options. On NUMA hosts, keep the NVMe PCI
device, hugepages, and Keylane worker CPUs on the same node when possible.

After setup, use `lspci -Dnnk` to confirm that only the selected controllers
use `vfio-pci`. Their kernel `/dev/nvme*` namespaces should no longer appear.
Run Keylane as `SPDK_TARGET_USER`, and configure an adequate locked-memory
limit (commonly `LimitMEMLOCK=infinity` in systemd or `ulimit -l unlimited` in
an administrative launch wrapper).

For an isolated development VM without an IOMMU only, unsafe no-IOMMU mode may
be required:

```sh
sudo sh -c 'echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode'
```

Do not use that mode on production or multi-tenant systems; it removes DMA
isolation.

## 5. Start and validate one Keylane instance

Choose deployment-specific addresses, ports, CPUs, and the URI discovered
above. This baseline disables maintenance so it is also suitable for isolated
latency tests:

```sh
KEYLANE_BIND_IP=127.0.0.1
KEYLANE_PORT=6379
KEYLANE_METRICS_PORT=9100
KEYLANE_THREADS=8
KEYLANE_CPUS=0-7
KEYLANE_SPDK_URI='spdk://0000:01:00.0/1'

taskset -c "$KEYLANE_CPUS" ./bld-spdk/keylane \
  --bind="$KEYLANE_BIND_IP" \
  --port="$KEYLANE_PORT" \
  --metrics-port="$KEYLANE_METRICS_PORT" \
  --threads="$KEYLANE_THREADS" \
  --recv-buffers=1024 \
  --registered-buffer-mb=256 \
  --busy-poll-us=20 \
  --background-budget-us=10 \
  --background-warrant-percent=1 \
  --spdk-max-completions-per-poll=8 \
  --spdk-foreground-pre-poll-us=5 \
  --mimalloc-purge-delay-ms=60000 \
  --flush-max-ms=1000 \
  --flush-size-kb=128 \
  --disable-read-crc \
  --tomb-raider-interval-ms=0 \
  --defrag-paused \
  --defrag-max-active-per-device=1 \
  --data-file="$KEYLANE_SPDK_URI"
```

Keylane pins workers by default. It reads the inherited affinity mask and maps
worker 0 to the first allowed CPU, worker 1 to the second, and so on. Thus
`taskset -c 8-15 ... --threads=8` maps workers to CPUs 8 through 15 rather than
allowing all eight workers to migrate across that set. Startup fails when the
allowed CPU count is smaller than the worker count. Use `--no-pin-workers` only
when operating-system scheduling is intentionally preferred.

Use `taskset`, cpusets, or the service manager to define the process CPU set.
Do not copy a benchmark host's CPU list without checking NUMA topology. SPDK
poll-mode workers are particularly sensitive to migration and preemption while
I/O is outstanding.

`--spdk-max-completions-per-poll=8` bounds one event-loop poll's completion
work, preventing a completion burst from monopolizing a worker. The budget is
shared fairly across all open SPDK namespaces on that worker.
`--spdk-foreground-pre-poll-us=5` lets newly arrived network and cross-worker
foreground work run for a small bounded slice before the storage completion
poll. Both values are tunable; `0` restores unbounded completion draining or
disables the pre-poll slice, respectively.

`--registered-buffer-mb` is a budget **per worker**. With 256 MiB and eight
workers, a process can reserve roughly 2 GiB of fixed storage buffers, before
adaptive overflow and other memory. The overflow read pool grows to the
observed per-worker concurrency high-water mark when fixed read buffers are
busy, then reuses those DMA buffers instead of allocating on every later miss.

A finite purge delay is recommended for recovery-heavy deployments. A 60-second
delay retains recently freed pages for reuse while allowing recovery's arena
high-water memory to return to the OS. `-1` can improve extreme tail latency by
avoiding recommit faults, but it may retain tens of GiB after a large recovery
and provides no proactive response to Linux or cgroup memory pressure.

Wait for `storage recovery complete` before sending traffic, then validate:

```sh
redis-cli -h "$KEYLANE_BIND_IP" -p "$KEYLANE_PORT" PING
redis-cli -h "$KEYLANE_BIND_IP" -p "$KEYLANE_PORT" DBSIZE
redis-cli -h "$KEYLANE_BIND_IP" -p "$KEYLANE_PORT" DEFRAG STATUS
curl "http://$KEYLANE_BIND_IP:$KEYLANE_METRICS_PORT/metrics"
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

./bld-spdk/keylane \
  --port=6379 \
  --metrics-port=9100 \
  --threads=8 \
  --registered-buffer-mb=256 \
  --data-file=spdk://0000:01:00.0/1 \
  --data-file=spdk://0000:02:00.0/1
```

Namespaces on one controller share that controller's hardware resources and
failure domain; namespaces on distinct controllers have separate PCI paths.
Keylane treats each namespace URI as one storage device in either case.

All paths in one process must be the complete members of the same persisted
Keylane storage set. Fresh devices must be initialized together. An existing
single-device set cannot be extended merely by adding an argument, and online
device addition/removal is not implemented. Argument order does not define
persistent device identity after initialization.

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
[Multi-Device Storage](multi-device-storage.md).
