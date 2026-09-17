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

# Keylane SPDK: 1B values through 1024 bytes exceed 1M GET/s on 16 workers

**English** | [简体中文](README.zh-CN.md) | [All reports](../README.md)

> Historical benchmark of Keylane, the project now named Lavik. Product names,
> versions, commands, and measurements describe the original test; this is not a
> measurement of the current Lavik release.
> Restored from the [2026-09-15 archive](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports).

Date: 2026-08-31 UTC

## Technical summary

Keylane served **1,000,000,000** existing keys at over one million random
GET/s with fixed values from **128 bytes through 1024 bytes**.  The best
128-byte run reached **1,058,414 GET/s**, the all-256-byte run reached
**1,043,567 GET/s**, the all-512-byte run reached **1,031,819 GET/s**, and
the all-1024-byte run reached **1,007,197 GET/s**.  The next tested size,
2048 bytes, reached 843,047 GET/s.  Thus 1024 bytes is the largest value
size verified above one million GET/s in this configuration.  All runs used
16 pinned Keylane workers, six raw SPDK NVMe namespaces, and `dfly_bench`
with 16 client threads, 640 connections, pipeline depth one, and no rate
limit.  All 64,000,000 measured requests hit and no errors were reported.

This is a closed-loop throughput result, not a claim about an open-loop
latency SLA.  The persistent keyspace was exactly one billion keys before the
read run, and the read key distribution covered the complete `0` through
`999999999` range uniformly.

## Headline measurements

| Dataset and server setting | Key range sampled uniformly | Connections | Measured GETs | QPS | Average latency | p99 | Hits |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1B keys, 128 B values, SPDK completion cap 8 | 1B | 640 | 64M | 973,095 | 655.210 us | 4.932 ms | 100% |
| 1B keys, 128 B values, SPDK completion cap 16 | 1B | 640 | 64M | **1,058,414** | 602.071 us | **894.884 us** | 100% |
| 1B keys, 256 B values, SPDK completion cap 16 | 1B | 640 | 64M | **1,043,567** | 610.576 us | 905.363 us | 100% |
| 1B keys, 512 B values, SPDK completion cap 16 | 1B | 640 | 64M | **1,031,819** | 617.648 us | 937.584 us | 100% |
| 1B keys, 1024 B values, SPDK completion cap 16 | 1B | 640 | 64M | **1,007,197** | 632.995 us | 964.339 us | 100% |
| 1B keys, 2048 B values, SPDK completion cap 16 | 1B | 640 | 64M | 843,047 | 755.227 us | 1.175 ms | 100% |
| Same data, completion cap 8 | 500M | 640 | 64M | 972,010 | 656.067 us | 4.908 ms | 100% |
| Same data, completion cap 8 | 20M | 640 | 64M | 980,248 | 650.307 us | 4.916 ms | 100% |
| Same data, completion cap 8 | 1B | 704 | 64,000,640 | 930,681 | 753.769 us | 6.961 ms | 100% |

The 500M and 20M reads are sensitivity checks after the clean full-dataset
load.  They do not reach one million, so the result above is not attributable
to narrowing the random key range.

The cap-8 to cap-16 comparison is operationally useful but not a clean
single-variable causal A/B: the cap-16 process restarted and rebuilt the
index.  Before that restart, `perf` found incremental `RehashStep` work in
GET requests.  The defensible operational conclusion is therefore to use cap
16 *after recovery and index maintenance have settled*, not that changing the
cap alone explains the whole delta.

## Hardware and software

### Server

- Azure VM size: `Standard_L16aos_v4`.
- CPU: AMD EPYC 9V74, 16 logical CPUs: one socket, eight cores, two hardware
  threads per core.
- RAM: 135,066,603,520 bytes (125.79 GiB).
- Storage: six Microsoft NVMe Direct Disk v2 namespaces, each
  1,919,850,381,312 bytes; aggregate raw data capacity 10.48 TiB.  The SPDK
  BDFs were `5361:00:00.0`, `6d30:00:00.0`, `32a1:00:00.0`,
  `78f5:00:00.0`, `9093:00:00.0`, and `c7c0:00:00.0`.
- Keylane executable SHA-256:
  `be1f71c6c8c11e6130685ca0c0e83ad8478bb30b27cff1b61742376959ce32bf`.
- Source checkout at test time: Keylane `07d4115ab6e8a66e8b2dc81a5a7abf42f6eb78a6`,
  Celer `6437653f87887924c5e7ea1e83defb85d8e1f9f1`.

### Client

- Azure VM size: `Standard_F16als_v7`.
- CPU: AMD EPYC 9V45, 16 logical CPUs: one socket, 16 cores, one hardware
  thread per core.
- Client: Dragonfly `dfly_bench` v1.40.1 x86_64 release binary, SHA-256
  `68fbf912ddd469e621025e35b5b42cb658ed0daef476bf725a9d1e37cf0a538f`.

## Dataset and resource use

The 128-byte dataset was made reproducible by clearing the database, then
issuing one billion sequential SETs.  Keys are decimal strings with **no
prefix**.  `DBSIZE` returned `1000000000`; `STRLEN 0`, `STRLEN 500000000`,
and `STRLEN 999999999` each returned `128`.

The initial 128-byte fill took 18m17.511s, or 957,870 SET/s overall.  The
256-byte result was produced by overwriting the same complete key range,
without changing its key count; it took 18m14.970s, or 965,823 SET/s overall.
For the 256-byte run, `DBSIZE` remained `1000000000` and the same three
`STRLEN` checks each returned `256`.  The fill aggregates are lower than the
central steady portion because `dfly_bench` drains uneven sequential
connection ranges near completion and the server showed several transient
write slow periods.  The 512-byte result was produced by one further
overwrite of the same one-billion-key range.  It took 19m11.976s, or 910,384
SET/s overall; `DBSIZE` again remained `1000000000`, and the same three
`STRLEN` probes each returned `512` before the GET test.

The 1024-byte overwrite took 20m2.139s (877,563 SET/s) and the 2048-byte
overwrite took 26m7.184s (685,017 SET/s).  Each retained exactly one billion
keys and each of the `0`, `500000000`, and `999999999` probes had the stated
length before its GET run.  The 1024-byte read result is only 0.72% above the
one-million threshold; 2048 bytes is a measured failure of that threshold,
not an extrapolation.

Immediately before the restart used for the cap-16 run, Keylane reported:

| Metric | Value |
|---|---:|
| `used_memory` | 49.06 GiB |
| RSS | 50.62 GiB |
| Peak RSS | 51.89 GiB |
| Configured max memory | 100.63 GiB |

For the 128-byte dataset, logical key and value bytes total about 127.5 GiB.
Including the 104-byte record header, inline key bytes, and eight-byte record
alignment, the live record allocation is approximately 230 GiB before block
headers and direct-I/O flush padding.  The same calculation for 256-byte
values is approximately 349 GiB.  Keylane's reported in-memory index size did
not materially change with the value size: the post-256-byte run reported
`used_memory=49.06 GiB` and RSS `50.25 GiB`.

Raw devices do not have a filesystem `df` counter; these are live-data
calculations, not secure-erasure claims.  `FLUSHDB` is logical deletion, so
prior experimental records can remain physically present and increase
recovery scan time until reclamation occurs.

## Reproduction

Bind the six dedicated namespaces to `vfio-pci` and start Keylane with all
16 workers pinned.  `--defrag-paused` was used to keep reclamation work out
of this read measurement.

```bash
taskset -c 0-15 /mnt/dev/keylane-spdk-main \
  --bind=172.16.0.4 --port=6379 --metrics-port=9100 \
  --threads=16 --pin-workers --busy-poll-us=20 \
  --foreground-budget-us=1000 --background-budget-us=10 \
  --background-warrant-percent=1 --defrag-paused \
  --spdk-max-completions-per-poll=16 --spdk-foreground-pre-poll-us=5 \
  --data-file=spdk://5361:00:00.0/1 \
  --data-file=spdk://6d30:00:00.0/1 \
  --data-file=spdk://32a1:00:00.0/1 \
  --data-file=spdk://78f5:00:00.0/1 \
  --data-file=spdk://9093:00:00.0/1 \
  --data-file=spdk://c7c0:00:00.0/1 \
  --logtostderr
```

`spdk-max-completions-per-poll` is a hot runtime setting.  The following
applies the value to every worker without a restart; startup configuration
must also specify it if the setting is to survive a restart.

```bash
valkey-cli -h 172.16.0.4 -p 6379 \
  CONFIG SET spdk-max-completions-per-poll 16
```

Create the clean dataset from the client.  `dfly_bench` treats the maximum
as exclusive for this key generator, so `1000000000` creates exactly the
desired inclusive numeric range.

```bash
valkey-cli -h 172.16.0.4 -p 6379 FLUSHDB

ulimit -n 8192
taskset -c 0-15 /tmp/dfly_bench-x86_64 \
  --h=172.16.0.4 --p=6379 \
  --proactor_threads=16 --c=40 --n=1562500 \
  --ratio=1:0 --pipeline=1 --qps=0 \
  --key_prefix= --key_minimum=0 --key_maximum=1000000000 \
  --key_dist=S --d=128
```

Validate both population and fixed value length before reading:

```bash
valkey-cli -h 172.16.0.4 -p 6379 DBSIZE
valkey-cli -h 172.16.0.4 -p 6379 STRLEN 0
valkey-cli -h 172.16.0.4 -p 6379 STRLEN 500000000
valkey-cli -h 172.16.0.4 -p 6379 STRLEN 999999999
```

Run the measured GET workload:

```bash
ulimit -n 8192
taskset -c 0-15 /tmp/dfly_bench-x86_64 \
  --h=172.16.0.4 --p=6379 \
  --proactor_threads=16 --c=40 --n=100000 \
  --ratio=0:1 --pipeline=1 --qps=0 \
  --key_prefix= --key_minimum=0 --key_maximum=1000000000 \
  --key_dist=U --tcp_nodelay=true
```

This produces 640 client connections and 64,000,000 GETs.  Raise the client
open-file limit first: the default limit of 1024 is insufficient for 1,024
connections and produces client-side `EMFILE`, not a Keylane server error.

To reproduce the 256-byte, 512-byte, 1024-byte, or 2048-byte variants, run
the same sequential SET command again with `--d=` set to that size, validate
`DBSIZE` and the three `STRLEN` probes, then run the same GET command.  No
`FLUSHDB` is needed for the value-size overwrite.

## CPU hotspot and limitation

In a 60-second cap-8 GET sample, `perf` attributed 14.46% of user-mode
samples to `ScanHashMap::FindWithoutStep` and 2.24% to `RehashStep`.
`RehashStep` runs from the mutable GET lookup path while an incremental
expansion is unfinished.  That maintenance can make a post-load benchmark
slower and its effect was not independently removed in the cap-8/cap-16
comparison.

The formal code optimization is to drain this incremental rehash from
owner-worker background maintenance (and before declaring recovery ready),
while retaining a bounded fallback in mutation paths for liveness.  A GET
should not have to migrate a hash bucket.  This report does not claim that
change has been implemented or measured.

## Recommended operating point

For this dedicated benchmark host, use 16 pinned workers, 640 client
connections, pipeline one, and `spdk-max-completions-per-poll=16`.  Do not
claim that a narrower 20M or 500M working set was required: the successful
result read uniformly across all one billion keys.

Before treating this as a release gate, repeat the cap-8 and cap-16 variants
from equally settled post-recovery states, alternate their order, and retain
the same 64M-request window.  That is required to separate the completion
budget effect from incremental-rehash and recovery state.
