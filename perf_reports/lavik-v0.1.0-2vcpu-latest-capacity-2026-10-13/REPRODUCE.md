# Reproduce the four-curve 2-vCPU comparison

## Offline

From this report directory, use Python 3.11+:

```sh
python3 verify_results.py
python3 -m pip install matplotlib==3.11.2
python3 build_assets.py
shasum -a 256 -c SHA256SUMS
```

The verifier reconciles all 84 formal results with original commands, memtier
JSON/text, counts and durations, percentile values, client exit codes, zero
GET misses and connection errors, command-error deltas, runtime versions,
CPU affinity, exact key counts and sampled lengths, clean server exits, and
both SPDK driver/hugepage restoration records. Readiness PINGs may receive
LOADING during baseline RDB restore before warmup; the counters must not rise
during measured windows. Figures retain one best observed peer I/O setting per
command across the entire concurrency sweep; no per-point switching is used.

## Fresh acquisition

Use two new dedicated Ubuntu 24.04 VMs in the same East US availability zone 3:
Standard_L2aos_v5 for all servers sequentially, Standard_F16as_v7 for the client.
The server has two SMT vCPUs of one physical core. Every server process and
thread is confined to CPUs 0–1, including competition with NIC interrupts.
The client uses all 16 physical cores, pinned to CPUs 0–15.

The exact scripts are archived in `evidence/`, with `/opt/bench2c` as the root
on both hosts. Run `bootstrap.sh` on both, then `build-client.sh` on the client.
It builds pinned Redis 8.10.2, Valkey 9.1.2 and memtier 2.5.1. Before server
preparation, supply a private inter-host SSH key at `/opt/bench2c/client-control`
and a verified known-hosts file at `/opt/bench2c/client-known-hosts`; neither
credential is included in the archive. Copy `client_wrapper.py` to the client,
and the acquisition scripts plus `experiment-plan.json` to the server.

`prepare-server.sh` downloads the checksum-pinned standard Lavik v0.1.0 release
and the SPDK setup helper, inventories the local NVMe, verifies client build
completion, and copies peer binaries. Versions, revisions and binary hashes
are recorded in `binaries.json`; the latest-stable release selection as checked
on 2026-10-13 is in `versions.json`.

**These are host-specific, destructive scratch-device scripts.** The recorded
server's local data disk is `/dev/nvme0n1`, while its OS is on `/dev/nvme1n1`.
This is the reverse of the previous allocation. Do not select by Linux device
number. Before adapting the scripts, inventory the new disposable namespace
and explicitly replace the recorded serial, PCI address, namespace ID and
capacity. Keep all guards for partitions, mounted filesystems, swap, holders,
open users and exact device identity. `spdk_run.py` discards this allowlisted
scratch namespace separately before each Lavik dataset; it must never be
pointed at an existing database or shared/OS device.

`start.sh` checks preparation, records the initial host policy, sets THP to
`never` and overcommit to `1`, then starts `run_all.py` with unlimited locked
memory and a 65,535 file limit. No cgroup memory limit is added. SPDK temporarily
reserves 2 GiB of hugepages and, on this dedicated VM without an exposed IOMMU,
uses VFIO no-IOMMU mode. Both device ownership and those temporary settings are
restored after each Lavik run and before memory peers.

The runner is one-shot: an existing `results/EXPERIMENT.json` stops a fresh
launch. Preserve failed or interrupted evidence before deciding how to resume.
Do not remove partial evidence to disguise a restart.

## Workload and order

1. Lavik v0.1.0, 1,000,000 keys, two pinned workers.
2. Lavik v0.1.0, a separate fresh 100,000,000-key dataset, two pinned workers.
3. Redis 8.10.2, 1,000,000 keys, I/O threads 1 then 2.
4. Valkey 9.1.2, 1,000,000 keys, I/O threads 1 then 2.

Keys are decimal `1..N` without a prefix, values are 1,024 B, and there is no TTL.
The fill uses partitioned key streams and checks exactly N successful requests
and N stored keys. GET and overwriting SET use uniformly random keys across
the complete declared range, with **`--distinct-client-seed` for every timed
run, for all four curves**. This differs from the old correlated-seed batch;
do not combine their results into one curve or attribute changes solely to
software versions.

Each configuration has a 10-second GET warmup, then seven GET and seven SET
points at total connections 16, 32, 80, 160, 320, 640 and 1280. Each point lasts
30 seconds, uses 16 memtier threads and pipeline 1, and has no rate limit.
Every point is acquired once. No builds run during acquisition.

Lavik defrag stays enabled. Redis/Valkey use `save ""`, `appendonly no`, and
`maxmemory 0`. One manual SAVE after each product's initial fill, outside all
formal windows, creates its baseline RDB. Both I/O-thread configurations reload
that baseline. The peer timed writes remain memory-only.

The fourth curve demonstrates a larger Lavik capacity on the same hardware.
It is not an equal-dataset peer comparison: 100M 1-KiB values represent 95.37 GiB
of logical value payload, excluding keys and overhead, versus 0.954 GiB for 1M.
It does not establish the maximum database capacity or node-loss durability.

## Evidence collection

After COMPLETE, `collect_evidence.py` verifies that all server processes have
exited and SPDK ownership is restored, then archives an explicit file allowlist
and `results/` excluding generated RDBs. Collect the client build log and final
inventory too. Verify the archive's SHA-256 before deleting cloud resources.
Private keys, cloud credentials, generated databases and executable downloads
are excluded from the committed report. The local NVMe is temporary storage;
archive the evidence before deallocation or deletion.
