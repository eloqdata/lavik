# Logical Databases and SELECT

Keylane supports the Redis logical database range `0..15`. A new connection
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

Logical databases are orthogonal to the 1024 storage partitions:

```text
key_owner = StorageShardForKey(user_key) % current_worker_count
index[db_id][(SHA1(user_key), user_key)]
```

The same user key in two databases routes to the same worker but has a
different hash table, intent-lock table, generation history, and value. A DB's
rehash, scan, or flush does not traverse or resize another DB's index. The 16
maps allocate buckets only when used; they still share the worker's one physical
append stream, so this does not multiply write buffers or storage IOPS. Keeping
DB out of the partition calculation also means changing DB does not alter key
distribution. The complete key participates in equality, so two distinct keys
with the same SHA-1 digest remain separate records.

The primary index uses `ScanHashMap`, a Keylane-specific C++ adaptation of
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
bits 63..54: worker id (10 bits, up to 1024 workers)
bits 53..0:  owner-local reverse-bit hash-table cursor
```

A cursor of 0 begins and ends an iteration. Workers are visited in ascending
order, so a request contacts only the worker represented by its cursor unless
that worker's local scan finishes before the COUNT hint is satisfied. No global
merge, cursor registry, or per-client scan state is required, and any number of
clients may hold cursors concurrently.

As in Redis/Valkey, SCAN is weakly consistent while writes are concurrent. It
may return duplicates; a key inserted after its bucket has passed may not be
returned. A key present for the complete iteration is not missed when the table
expands. Callers requiring a set must deduplicate results.

Deleted entries currently remain as tombstones in the index. SCAN filters them
and limits empty/tombstone bucket work per call to keep latency bounded. Shrink
and tombstone reclamation are intentionally deferred until an epoch-aware
reclamation policy exists.

Future database-wide invalidation commands must use the `db_id` carried by the
request:

- `FLUSHDB` invalidates only that DB, preferably through a per-DB epoch;
- `FLUSHALL` advances every DB epoch or the node-wide epoch.

The selected DB must never be inferred from the worker executing one of these
operations.
