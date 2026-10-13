# Reproduce the 2-vCPU experiment

## Offline audit and figures

From this report directory, use Python 3.11+:

```sh
python3 verify_results.py
python3 -m pip install matplotlib==3.11.2
python3 build_assets.py
shasum -a 256 -c SHA256SUMS
```

The verifier independently reconciles all 70 formal rows with the original
memtier command, JSON and printed text, checks request count / duration against
QPS, validates percentiles, confirms zero GET misses and connection errors,
checks CPU affinity, clean exits, dataset cardinality and sampled value lengths,
and verifies that SPDK device bindings and the hugepage reservation were restored.
No cloud access or server binaries are required for this audit.

## Acquisition layout

The exact acquisition scripts are archived under `evidence/`. They use
`/opt/bench2c` on two dedicated Ubuntu 24.04 machines:

| Host | VM | CPU placement | Purpose |
|---|---|---|---|
| Server | Standard_L2aos_v5, eastus zone 3 | CPUs 0–1 | All three products, sequentially |
| Client | Standard_F16as_v7, eastus zone 3 | CPUs 0–15 | Builds first, then memtier only |

`bootstrap.sh` installs the dependencies on both hosts. `build-client.sh` builds
Redis, Valkey and memtier from the recorded versions. `prepare-server.sh`
downloads the checksum-pinned Lavik release and SPDK setup helper, copies peer
binaries from the client, and inventories the dedicated local NVMe. The full
binary and source archive checksums are in `binaries.json` and the archived
checksum text files. The SPDK helper is used to bind/unbind the device; it is
not a separately built SPDK library linked into Lavik.

The original host addresses, device serial, PCI address, namespace and capacity
remain in `evidence/config.json` and the scripts as acquisition evidence.
**These are host-specific destructive scripts, not a portable one-command
installer.** Before another acquisition, provision new disposable hosts, inspect
their CPU topology and storage, and replace the explicit device allowlist only
after identifying an unused local NVMe. Keep the mounted-device, partition,
holder, process-use, swap, serial, capacity and PCI guards. Never select a disk
from its `/dev/nvme*` number alone. `spdk_run.py` discards the allowlisted namespace.

Supply a new SSH key between the server's `azureuser` and the client at
`/opt/bench2c/client-control`, with a corresponding verified known-hosts file.
Private keys are deliberately absent from this report. Copy `client_wrapper.py`
to the client and the other acquisition Python scripts to the server. The exact
commands and versions are in the evidence; VM bootstrap and private SSH setup
are deployment prerequisites, not part of the timed experiment.

Before all products, set transparent hugepages to `never` and
`vm.overcommit_memory=1`. Record the old policy first. Keep every product within
server CPUs 0–1; no cgroup memory restriction is used. NIC IRQs share those CPUs.
Leave the 2 MiB hugepage pool and VFIO no-IOMMU setting in their original state:
`spdk_run.py` temporarily reserves 2 GiB and enables VFIO's no-IOMMU mode when
needed, then restores both before Redis/Valkey. Lavik uses SPDK storage and
kernel TCP networking. Do not add separate benchmark or build activity while
measurements run.

After installing the correct paths and credentials and writing `PREPARED`, run
`run_all.py` as root with `RLIMIT_NOFILE=65535` and unlimited locked memory. The
recorded systemd service used the equivalent of:

```sh
cd /opt/bench2c
ulimit -n 65535
ulimit -l unlimited
python3 run_all.py > suite.log 2>&1
```

The runner is deliberately one-shot and refuses an existing `EXPERIMENT.json`.
For a interrupted run, preserve its evidence before deciding what to resume;
do not delete partial results to make a rerun look fresh.

## Workload

Three independent fresh datasets contain exactly 1,000,000 decimal keys
(`1` through `1000000`, no prefix), with 1,024-byte values. There is no expiry.
Every point uses 16 client threads, pipeline 1 and no rate limit. Total
connections are 16, 32, 80, 160, 320, 640 and 1280, hence 1, 2, 5, 10, 20, 40 and
80 connections per client thread. The memtier default correlated seeds match
the earlier memory-peer scenario; `--distinct-client-seed` is not set.

Order: Lavik with 2 pinned data workers; Redis with 1 then 2 I/O threads; Valkey
with 1 then 2 I/O threads. Each configuration first receives a 10-second GET
warmup, followed by seven 30-second GET points and seven 30-second overwriting
SET points. Values and key counts are checked before and after each configuration.

For Redis and Valkey, a manual `SAVE` creates the reset baseline after the initial
fill, outside all formal measurement windows. Both I/O configurations load this
same baseline. Automatic snapshots and AOF remain disabled (`save ""`,
`appendonly no`); measured peer writes are memory-only. Lavik defrag stays enabled.

## Evidence collection

`evidence/results/` contains complete client JSON/text, command arrays,
server commands and logs, enclosing host/INFO snapshots, client CPU snapshots,
dataset validation, driver transitions and clean exits. `results/data/` (RDB
files), server/client binaries, private credentials and cloud access tokens are
excluded. The SHA-256 manifest covers the archived evidence and report inputs;
figures and the CSV are reproducible from those files. Preserve this archive
before deallocating or deleting machines, because the local NVMe is temporary.
