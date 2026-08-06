# Partition Replication Status

This document records the first working Keylane-to-Keylane replication design
as of 2026-08-06. It is a static asynchronous replication MVP, not yet a
production high-availability system and not Redis PSYNC compatible.

## Data model

Keylane uses all 16384 Redis hash slots as independent logical partitions. A
worker owns partition `p` when `p % current_worker_count == worker_id`. Every
owned partition contains 16 independent `ScanHashMap` indexes, one for each
logical database. Physical writes remain one 8 MiB append stream per worker;
partitions do not create independent files, active blocks, or write IOPS.

Every persistent record carries:

- `db_id` and durable `db_epoch`;
- partition `replication_epoch`;
- partition `mutation_sequence`.

`replication_epoch` fences records left by an older copy of a partition.
`db_epoch` fences all records invalidated by `FLUSHDB`. Recovery accepts only
the newest replication epoch for a partition and the current durable DB epoch.

## Copy state machine

The source processes partitions in ascending order. Each partition passes
through these logical states:

```text
RESET target replication epoch
  -> SNAPSHOT partition maps for nonempty DBs
  -> CATCHUP mutations newer than the snapshot fence
  -> SYNCED
```

`BeginPartitionReplication` captures the source mutation-sequence fence and
starts retaining subsequent SET, DEL, and FLUSHDB events. Snapshot records are
revalidated against the current index while holding the key lock before their
values are loaded. The target applies snapshot and delta records idempotently
using `(replication_epoch, db_epoch, mutation_sequence)`.

SYNCED partitions continue forwarding new mutations while later partitions are
still in SNAPSHOT. The coordinator drains ready partitions after each copied
partition and between snapshot batches. A partition retains at most 65536 delta
records; if the consumer falls behind that bound, only that partition is reset
and copied again.

After all 16384 partitions reach SYNCED, the same ready queue carries steady
state mutations. Writes are acknowledged to Redis clients before the replica
acknowledges them, so replication is asynchronous.

## FLUSHDB

Block zero contains a checksummed metadata page with the 16 durable DB epochs.
`FLUSHDB` closes a command-layer gate for the selected DB, waits for active DB
operations to drain, persists the incremented epoch with direct IO and
`fdatasync`, and then clears that DB's indexes on every worker. Old records are
ignored during recovery and reclaimed by normal defrag.

When source delta capture is active, FLUSHDB emits an epoch event for every
partition. The replica installs the newer DB epoch before accepting later
records. Other logical databases are unaffected. `FLUSHALL` is not implemented.

## Internal transport and configuration

The private Celer RPC transport has an explicit 16-byte little-endian header,
a 16 MiB frame limit, and asynchronous handlers. Apply batches are limited to
12 MiB. Cross-core asynchronous submissions move coroutine closures into the
target worker's coroutine frame so captured strings and vectors remain alive
across IO suspension.

Example static topology:

```text
replica: keylane --replication-port 17512 --replica-read-only ...
source : keylane --replicate-to 127.0.0.1:17512 ...
```

The receiver routes each partition RPC to the partition's current owner, so
source and target may use different worker counts.

## Verification completed

The following manual two-process tests passed:

- complete baseline of all 16384 partitions with source=2 workers and target=3;
- values in DB 0 and DB 1 copied correctly;
- steady-state SET, DEL, and read-only rejection;
- DB 0 FLUSHDB left DB 1 unchanged, and DB 1 FLUSHDB behaved independently;
- a surviving DB 2 value recovered after the target changed from 3 to 4 workers;
- a write to slot 25 after that partition completed appeared on the target
  while the remaining baseline partitions were still being copied;
- the cross-core record path passed ASAN beyond the partition that previously
  reproduced the coroutine-lifetime crash.

## Known gaps

This implementation must not yet be presented as completed production HA:

- replication progress and retained offsets are not durable;
- reconnect currently starts a fresh 16384-partition baseline;
- there is one statically configured target and no dynamic role command;
- there is no synchronous acknowledgement, `WAIT`, or replica durability policy;
- there is no election, promotion, quorum, fencing, or split-brain prevention;
- the private RPC has no authentication, TLS, rate limit, or traffic isolation;
- fault injection and automated multi-process replication tests are still needed;
- Redis PSYNC, replication backlog, RDB import/export, and Cluster bus protocols
  are not implemented.

## Index-memory follow-up

`RecordLocation` was reduced from 64 to 56 bytes by removing the redundant
in-memory `db_epoch` and narrowing `block_owner` to 16 bits. `record_offset`
remains 32 bits: it is an offset inside an 8 MiB block, which does not fit in 16
bits, and using a 23-bit implementation-defined bit-field would add masking to
the GET path for no whole-structure saving by itself.

This is still too large for small values. A current map entry is approximately
112 bytes before allocator and bucket overhead: 20-byte digest, 32-byte
`std::string`, and 56-byte location plus alignment. A later memory-focused
change should measure and address the whole entry rather than only reorder the
location fields. Candidate work is a 64-bit hash with full-key collision
checking, arena/inline key storage, and splitting recovery-only version fields
from the compact online location. The native replication correctness fields
must not be truncated without an explicit wrap/recovery design.

The partition snapshot/delta machinery is intended to remain the native
Keylane-to-Keylane replication path. Redis interoperability should be a
separate adapter: first RDB import/export, then Redis PSYNC/FULLRESYNC plus the
command stream, and only later Redis Cluster bus membership and failover.
