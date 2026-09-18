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

# SPDK storage with kernel TCP

Use this guide to run a standard Lavik release package on dedicated NVMe
namespaces with `--storage=spdk`. Network traffic continues
through Linux; no NIC rebinding or DPDK network setup is required.

## Package and setup helper

Download the standard package for your architecture from
[Releases](https://github.com/eloqdata/lavik/releases), verify its published
SHA-256 file, and extract it. The `-minimal` package does not include SPDK.
See [building and packaging](building-and-packaging.md#downloadable-release-package)
for CPU, Linux 6.1+, glibc, and runtime-library requirements. On Ubuntu 24.04,
the standard package needs `libnuma1` and `libuuid1`.

The binary archive does not bundle SPDK's host setup script. Obtain the helper
from the source revision matching the package; this does not require compiling
Lavik. For v0.1.0-beta.1:

```bash
git clone --branch v0.1.0-beta.1 --depth 1 https://github.com/eloqdata/lavik.git lavik-spdk-setup
cd lavik-spdk-setup
git submodule update --init bycorf
git -C bycorf submodule update --init third_party/spdk
LAVIK_SPDK_SETUP="$PWD/bycorf/third_party/spdk/scripts/setup.sh"
```

The commands below use Bash, `sudo`, `pciutils`, and the standard Linux
`util-linux` tools. Run them on the host that owns the NVMe controllers.

## Identify dedicated controllers

Binding to VFIO removes the controller's namespaces from Linux block-device
access. Select only controllers dedicated to this Lavik instance. Check all
namespaces and partitions for mounted filesystems, swap, RAID/LVM membership,
and open users. Do not bind the OS/workspace drive or a shared controller.

```bash
lsblk -o NAME,SIZE,MODEL,SERIAL,FSTYPE,MOUNTPOINTS
for controller in /sys/class/nvme/nvme*; do
  printf '%s serial=%s pci=%s\n' \
    "${controller##*/}" "$(cat "$controller/serial")" \
    "$(basename "$(readlink -f "$controller/device")")"
done
```

Record serial numbers, full PCI addresses (including domain), and namespace
IDs before binding. Kernel names such as `/dev/nvme1n1` can change after reset.
A Lavik path has the form `spdk://DOMAIN:BUS:DEVICE.FUNCTION/NAMESPACE_ID`.
The examples below use namespace 1; replace it if your namespace ID differs.

Set an explicit allowlist using your inventory. The addresses below are
examples, not a device-discovery mechanism:

```bash
LAVIK_NVME_PCI='0000:01:00.0 0000:02:00.0'
```

Use existing complete Lavik storage sets without clearing their contents.
For a new set, provision empty dedicated media according to
[multi-device storage](multi-device-storage.md). The benchmark erased its
allowlisted scratch devices before each independent load; that is a test
preparation step, not part of normal startup or recovery.

## Reserve memory and bind devices

An IOMMU-enabled host with VFIO is the normal configuration. Before changing
anything, save the original settings. The commands assume 2 MiB hugepages,
as used in the benchmark:

```bash
sudo modprobe vfio-pci
LAVIK_PREVIOUS_HUGEPAGES=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
LAVIK_PREVIOUS_NOIOMMU=$(cat /sys/module/vfio/parameters/enable_unsafe_noiommu_mode)
sudo env PCI_ALLOWED="${LAVIK_NVME_PCI:?Set the dedicated controller allowlist}" \
  DRIVER_OVERRIDE=vfio-pci HUGEMEM=8192 \
  "$LAVIK_SPDK_SETUP" config
for bdf in $LAVIK_NVME_PCI; do
  readlink -f "/sys/bus/pci/devices/$bdf/driver"
done
```

Each selected controller should now use `vfio-pci`. `HUGEMEM=8192` requests
8 GiB of hugepage memory. Lavik's `BYCORF_DPDK_MEMORY_MB=8192` below selects
that EAL memory budget; neither setting limits total Lavik RSS. Hugepage
allocation can fail on a fragmented or memory-constrained host. Allocate
before starting other large services and inspect the helper's error output.
Never run `setup.sh config` with an empty allowlist: its default device scope
is broader than this guide's selected controllers.

The benchmark VM did not expose an IOMMU and temporarily used VFIO's unsafe
no-IOMMU mode. This removes DMA isolation and is specific to that dedicated
benchmark environment. If reproducing in an equally isolated VM that requires
it, enable it **after saving the previous value and before binding**:

```bash
printf '1\n' | sudo tee /sys/module/vfio/parameters/enable_unsafe_noiommu_mode
```

Do not enable this on a shared host as a general workaround. Prefer exposing
an IOMMU. If config failed before that VM-specific step, inspect the current
driver state and rerun the same allowlisted config after enabling the mode.

## Start Lavik and check the configuration

Set the extracted binary's absolute path and adjust the bind address and CPU
list to your machine. The default worker count follows the CPU affinity, so
`taskset -c 0-15` selects 16 workers. The example omits `v0.1.0-beta.1`
defaults: kernel TCP, loopback address, port 6379, pinned workers, and disabled
metrics:

```bash
LAVIK_BINARY='/opt/lavik-v0.1.0-beta.1-linux-x86_64/lavik'
LAVIK_EAL_ARGS=''
LAVIK_DATA_ARGS=()
for bdf in $LAVIK_NVME_PCI; do
  LAVIK_EAL_ARGS+="-a $bdf "
  LAVIK_DATA_ARGS+=("--data-file=spdk://$bdf/1")
done
sudo prlimit --memlock=unlimited:unlimited --nofile=65535:65535 \
  env BYCORF_EAL_ARGS="$LAVIK_EAL_ARGS" BYCORF_DPDK_MEMORY_MB=8192 \
  taskset -c 0-15 "$LAVIK_BINARY" \
  --storage=spdk \
  --spdk-max-completions-per-poll=16 \
  "${LAVIK_DATA_ARGS[@]}"
```

Both the EAL allowlist and the `--data-file` list must cover the intended
controllers/storage set. The latter uses SPDK URIs, never `/dev/nvme...`
paths. The completion cap of 16 overrides the default of 8 to match the
benchmark; foreground pre-poll keeps its default of 5 µs. The benchmark
report records its other explicit settings.

Use another terminal with a Redis-compatible CLI to inspect the running
server. Query the same bind address and port used at launch:

```bash
redis-cli -h 127.0.0.1 -p 6379 PING
redis-cli -h 127.0.0.1 -p 6379 CONFIG GET spdk-max-completions-per-poll
```

Expect `PONG` and `16`, respectively. The foreground pre-poll option
keeps its startup default; verify `spdk_foreground_pre_poll_us=5` in the startup
log rather than through `CONFIG GET`. Inspect startup logs
for SPDK initialization and the selected namespaces before loading data.

## Stop and restore the host

Stop the client, send `SIGTERM` to this Lavik process, and wait for it to exit
before rebinding. Use the same allowlist; never reset a controller still owned
by a running process. On a dedicated host, restore the saved settings once
no other process needs the reserved hugepages or VFIO mode:

```bash
sudo env PCI_ALLOWED="${LAVIK_NVME_PCI:?Set the dedicated controller allowlist}" \
  DRIVER_OVERRIDE=vfio-pci "$LAVIK_SPDK_SETUP" reset
printf '%s\n' "$LAVIK_PREVIOUS_HUGEPAGES" | \
  sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
if [ "$LAVIK_PREVIOUS_NOIOMMU" = Y ]; then
  printf '1\n' | sudo tee /sys/module/vfio/parameters/enable_unsafe_noiommu_mode
else
  printf '0\n' | sudo tee /sys/module/vfio/parameters/enable_unsafe_noiommu_mode
fi
for bdf in $LAVIK_NVME_PCI; do
  readlink -f "/sys/bus/pci/devices/$bdf/driver"
done
lsblk -o NAME,SIZE,SERIAL,FSTYPE,MOUNTPOINTS
```

The selected controllers should use `nvme` again. Verify by serial number
because namespace device names may have changed. Resetting driver ownership
does not erase data. Subsequent Lavik recovery still requires the complete
original storage set.
