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

# Logical Databases and SELECT

Lavik supports the Redis logical database range `0..15`. A new connection
starts in DB 0.

## Connection state

The selected database is connection state. `RedisService::Serve` keeps
`selected_db` in the long-lived connection coroutine frame. `SELECT n` is
handled locally and changes that value only after its arguments have been
validated.

Each parsed `CommandRequest` receives a copy of the connection's current
`db_id`. Cross-core routing carries that explicit request field to the key
owner. Command coroutines do not consult thread-local or worker-local selected
database state, so suspending, resuming, or sending work to another worker
cannot change a request's database.

Requests on one connection are executed sequentially. Therefore a successful
`SELECT` affects subsequent pipelined commands in protocol order. It never
affects another connection.

## Indexing and partitioning

Logical databases are orthogonal to the 16384 Redis hash-slot partitions:

```text
partition = RedisSlot(user_key)
key_owner = partition % current_worker_count
partition_index[partition][db_id][(SHA1(user_key), user_key)]
```

The same user key in two databases routes to the same worker but has a
different hash table, generation history, and value. Each worker stores only
the partitions it owns; every partition has 16 lazily allocated maps. These
maps still share the worker's one physical append stream, so 16384 partitions
do not create 16384 write buffers or physical storage streams. Intent locks
remain per worker and DB. Keeping DB out of the partition calculation means
changing DB does not alter key distribution. The complete key participates in
equality, so two distinct keys with the same SHA-1 digest remain separate
records.

The primary index uses `ScanHashMap`, a Lavik-specific C++ adaptation of
Valkey's cache-line bucket hash table. Entries have stable addresses, expansion
is incremental, and the reverse-bit cursor does not allocate server-side scan
state.

## Persistent records

Every `RecordHeader` stores the one-byte `db_id`. It reuses the former `flags`
byte, so the record header does not grow. Recovery validates `db_id`, recomputes
the key digest, and inserts it into the selected DB's map. Defrag preserves
`db_id` when relocating a record.

The storage format version intentionally remains 1 during development. Data
written before `db_id` was added to record identity must be cleared.

## Database-scoped operations

`DBSIZE` sums only the selected database's owner-local counters across all
workers. `SCAN cursor [MATCH pattern] [COUNT count]` scans only the selected
database. Its unsigned 64-bit cursor is composed as follows:

```text
bits 63..50: Redis partition id (14 bits)
bits 49..0:  local reverse-bit cursor bits 63..14
```

A cursor of 0 begins and ends an iteration. Partitions are visited in ascending
order and each partition's map is scanned independently. The local cursor's low
14 bits are omitted from the external cursor and restored as zero on decode;
this is lossless while a partition map has at most 2^50 buckets because the
reverse-bit algorithm leaves those bits zero. A request visits at most 64
partitions, so a sparse database may legally return an empty key array with a
nonzero cursor instead of traversing all 16384 empty maps in one latency spike.
No global merge, cursor registry, or per-client scan state is required, and any
number of clients may hold cursors concurrently.

As in Redis/Valkey, SCAN is weakly consistent while writes are concurrent. It
may return duplicates; a key inserted after its bucket has passed may not be
returned. A key present for the complete iteration is not missed when the table
expands. Callers requiring a set must deduplicate results.

Deleted entries currently remain as tombstones in the index. SCAN filters them
and limits empty/tombstone bucket work per call to keep latency bounded. Shrink
and tombstone reclamation are intentionally deferred until an epoch-aware
reclamation policy exists.

`FLUSHDB` invalidates only the selected DB by durably advancing its per-DB
epoch before clearing its in-memory partition indexes. `FLUSHALL` is not yet
implemented; it should advance all DB epochs.

The selected DB must never be inferred from the worker executing one of these
operations.
