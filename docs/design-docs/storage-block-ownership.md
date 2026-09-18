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

# Worker-Count-Independent Storage Ownership

## Goals

Lavik's persistent layout must not require the process to restart with the
same worker count. The design keeps the shared-nothing fast path when the
topology is unchanged without creating one physical append stream per logical
partition.

The two ownership domains are deliberately independent:

```text
key_owner   = StorageShardForKey(key) % current_worker_count
block_owner = the current worker that exclusively manages one physical block
```

There are 16384 logical storage partitions, exactly matching Redis hash slots.
Partitions are used for key routing, independent indexes, SCAN, and replication.
They are not physical append streams. Each worker has one active 8 MiB append
block, so active write-buffer count scales with workers rather than with 16384.

## Persistent provenance

Every block header stores:

- `writer_id`: the worker that created the block;
- `layout_worker_count`: the worker count under which it was created;
- `allocation_epoch`: the incarnation of the physical block;
- committed length, record count, and maximum LSN.

`layout_worker_count` is provenance, not a startup constraint. Any current
worker count from 1 through 16384 is accepted. The format version intentionally
remains 1 during development; data must be cleared when incompatible layouts
are introduced.

After a scale-down, a persisted `writer_id` can be greater than or equal to
the current worker count. That is valid. The recovered in-memory
`block_owner`, however, is always an ID in the current topology and must be in
`[0, current_worker_count)`.

A record location in memory additionally stores `block_owner`. This is rebuilt
at recovery and is not persisted in each record.

## Recovery

Physical blocks are scanned in parallel. Ownership is rebuilt as follows:

```text
if block.layout_worker_count == current_worker_count:
    block_owner = block.writer_id
else:
    block_owner = SplitMix64(block_id, allocation_epoch) % current_worker_count

key_owner = StorageShardForKey(record.key) % current_worker_count
```

Keeping `writer_id` when the topology matches preserves the local fast path.
Hashing blocks when it differs spreads old IO and defrag work across all current
workers instead of concentrating it on the old worker-id range.

Recovery sends block metadata to `block_owner` and record metadata to
`key_owner`. Each key owner resolves logical mutation sequences and physical
LSNs in its local index. A second accounting phase sends only the winning
index locations to their block owners to rebuild exact `live_bytes`.

No data is rewritten during startup.

## Owner-only block state

Each worker has a sparse map containing only the blocks it currently owns:

```text
WorkerStore
  owned partitions          each has 16 logical-DB key indexes
  active append block       new physical writes owned by this worker
  owned BlockState map      blocks whose lifecycle this worker manages
  flush queue
  defrag queue
```

Only the block owner reads or writes `BlockState`, including `live_bytes`,
`pins`, flush flags, defrag flags, and buffer ownership. Other workers interact
with it through cross-core tasks. The local-owner path executes inline.

This single-writer rule avoids a global atomic block table and avoids false
sharing between workers updating adjacent block entries. `BlockState` objects
are stable heap allocations behind the sparse owner map, so references remain
valid while owner coroutines suspend.

## Read path and pinning

An incoming request deterministically reaches `key_owner` and looks up its
local index. The index location contains the current `block_owner`.

```text
block_owner == key_owner:
    pin and read locally

block_owner != key_owner:
    SubmitTaskTo(block_owner, pin + read)
```

The block owner validates `allocation_epoch`, increments its ordinary
owner-local `pins`, submits the read on its io_uring, and decrements `pins` only
after the read CQE. The returned `ReadBufferLease` may move across workers; its
existing remote-release path returns the registered slot to the storage worker
that owns it.

The key shared lock remains held until the read completes. Defrag relocation
needs the same key's exclusive lock, so it cannot invalidate a location while a
reader is acquiring or using the pin.

Pins protect physical release/reuse. Defrag may scan immutable block contents
while reads are active, but the block cannot enter final release until all
current index references have been removed and `pins == 0`.

## Write and invalidation

New records are appended to the key owner's single active block. Consequently,
new locations always have `block_owner == key_owner`.

When a write replaces an old location, the key index is changed under its
exclusive key lock and the old owner receives:

```text
MarkRecordDead(block_id, allocation_epoch, record_bytes)
```

The epoch is mandatory: it prevents a delayed operation from changing the
accounting of a reused physical block. The current implementation waits for the
owner task to complete, making accounting exact before the write operation
returns. Local invalidations execute inline with no message or suspension.

Updates therefore rehome old data naturally. Foreign references exist only for
unchanged records from layouts written with a different worker count.

## Defrag and release

Only the block owner queues, runs, and completes defrag for a block.

For every physical record, the owner computes the current key owner and invokes
`RelocateIfCurrent` there. The key owner takes the exclusive key lock and checks
that its index still points to the exact `(block_id, record_offset,
allocation_epoch)`. Stale versions are ignored. Current versions are appended
to that key owner's active block, the index is switched, and the source owner is
notified that the old bytes are dead.

Final release is owner-only:

```text
defragging
  -> live_bytes == 0
  -> freeing (reject new pins)
  -> wait for pins == 0
  -> zero and fdatasync the block header
  -> erase owner BlockState
  -> enqueue block_id on the global free queue
```

`allocation_epoch` protects all read, invalidation, and relocation operations
against block reuse.

## Locality convergence

After a worker-count change, key ownership and old block ownership may differ.
Foreign reads temporarily execute on the block owner. Foreground updates and
background defrag rewrite live records through their current key owners, so new
locations become local without an offline full-data rewrite. Once old-layout
blocks are reclaimed, the steady-state path is local again.

## Known scaling costs to measure

This storage layout no longer depends on worker count. At very large worker
counts, two independent startup/runtime structures have quadratic control
storage that should be measured on the target machine:

- recovery currently creates per-target batches from every scanning worker,
  giving an all-to-all `O(worker_count^2)` control structure;
- celer currently allocates an `N x N` matrix of bounded SPSC cross-core lanes.

These do not affect storage correctness and may be acceptable on a 1000-core
machine with correspondingly large memory. Keep the contention-free SPSC design
unless measurements show that its memory, initialization, or TLB footprint is a
problem; only then consider sparse lanes or per-target MPSC inboxes.

## Verified transition

The implementation has been exercised with one data file through this sequence:

```text
2 workers: write and durable shutdown
3 workers: recover, read foreign blocks, update/delete, defrag, shutdown
1 worker : recover again and verify values and DBSIZE
```

The test covers topology-changing recovery, remote owner reads, invalidation,
defrag relocation, tombstones, and repeated durable restart.
