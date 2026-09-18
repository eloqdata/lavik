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

# Recovery Metadata Layout

## Implemented scope

Lavik reserves a capacity-derived prefix in every configured file or raw
block device. The prefix contains:

- the immutable device label;
- 16 database epochs;
- 16,384 partition replication epochs;
- one recovery scan bit per physical 8 MiB block.

The epoch and bitmap pages use independent A/B slots. There is no metadata
journal, tree, checkpoint, or persistent worker table in this implementation.
Each path's persisted capacity is fixed after initialization, so its complete
bitmap size is known before data allocation starts. Different devices may have
different capacities and therefore different bitmap lengths and data-prefix
boundaries.

The format version remains 1 during development. Existing files must be
cleared when this layout changes.

## Fixed physical layout

All metadata I/O uses checksummed 4 KiB pages:

```text
offset 0
  DeviceLabel

offset 4 KiB
  epoch page 0 slot A
  epoch page 0 slot B
  ...
  epoch page N slot A
  epoch page N slot B

then
  scan-bitmap page 0 slot A
  scan-bitmap page 0 slot B
  ...

then, rounded up to an 8 MiB boundary
  data block 0
  data block 1
  ...
```

`DataBlockBegin(capacity_blocks)` computes the first physical local block that
may hold records. Block IDs continue to contain the real local block number, so
the reserved prefix never needs a separate address translation.

Each metadata page header records its type, logical page index, payload length,
generation, and CRC32C. An update writes the inactive A/B slot and calls
`fdatasync`; only then is that slot published as current in memory. Recovery
selects the valid slot with the highest generation. A zero/zero pair is an
uninitialized page whose logical contents are zero. A nonzero pair with no
valid slot is corruption and startup fails.

At the 1 PiB per-device limit:

```text
1 PiB / 8 MiB                         = 2^27 physical blocks
one scan bit per block                = 16 MiB payload
16 DB + 16,384 partition uint64 epochs = 128.125 KiB payload
```

A/B copies and page headers make the fixed prefix about 32.6 MiB, which rounds
to five 8 MiB blocks. Smaller configured files reserve fewer bitmap pages.

## Epoch metadata

The logical epoch array is laid out as:

```text
[0, 16)       database epochs
[16, 16400)   partition replication epochs, indexed by Redis slot
```

Epoch zero is not used. A fresh metadata area initializes every value to one.
The partition owner decides the next replication epoch and sends the update
directly to every device owner. Each device owner serializes the update with
the other metadata operations for its device, patches its owner-local page
image, writes the inactive A/B slot, and calls `fdatasync`. There is no global
metadata-page owner and worker zero has no special role in this path. The
logical epoch is published only after every device owner succeeds.
If any mirrored write or sync fails, further record writes are stopped until
restart; continuing with an old in-memory epoch would be unsafe when another
device may already contain the new valid page.

Startup reads every device, selects each device's newest valid A/B page, and
takes a component-wise maximum. This is safe because these epochs only increase
and startup requires the complete device set.

`FLUSHDB` persists the new DB epoch before dropping the in-memory DB indexes.
Recovery ignores records with an older DB epoch, so no per-key disk rewrite is
required. After the indexes are cleared, Lavik marks their old locations dead.
If this makes the worker's active append block completely dead, `FLUSHDB` seals
that block immediately and queues it for flush; a mixed active block remains
open so live records from other logical databases are not disturbed. Once the
sealed block is durable, the normal defrag path can return it to the ready pool.

`ResetReplicaPartition` locks the owning worker's append stream, persists the
new partition epoch, publishes it to the partition, and then removes the old
partition contents. Holding the writer lock across the metadata commit prevents
a command from receiving success for an old-epoch record after the new epoch is
durable. Recovery rejects every record whose partition epoch differs from the
persisted epoch.

## Recovery scan bitmap

The scan bitmap is conservative:

- bit 0 means the block has never been activated, or was durably returned to a
  future cold-free pool; recovery skips all I/O for it;
- bit 1 means recovery must inspect the block header;
- a bit-1 block with a zero header is a valid false positive and becomes a
  reusable ready block.

A false positive only costs one 4 KiB header read. A false negative could hide
committed data, so block activation uses strict ordering:

```text
device owner chooses a batch of block IDs
device owner sets their in-memory bits
device owner writes inactive bitmap A/B pages
fdatasync(device)
publish the block IDs to the owner-local ready pool
hand one block ID to a worker
allow record writes
```

Only the device owner can modify its ordinary bitmap vector or A/B page state.
The bitmap is therefore not atomic and allocation never performs a contended
cross-core CAS on a block bit.

The current activation batch is 256 blocks. It amortizes a metadata durability
operation across later standby requests, but it does not allocate 8 MiB memory
buffers for those blocks. They are only IDs in the device owner's ready vector.

Defrag uses the reverse safe ordering:

```text
remove live index references and wait for pins
write a zero block header
fdatasync(device)
return the ID to the device owner's ready pool
```

The bit intentionally remains one on this warm-reuse path. Reassignment then
needs no bitmap write. Consequently, the current bitmap skips pristine space
but may continue scanning historically used, now-free blocks. Batched warm-to-
cold bit clearing can be added later without changing the allocation ordering.

If a bitmap metadata write fails, the device allocator is frozen until restart.
It does not advance to later blocks because the durable state of the failed I/O
is ambiguous. Restart resolves the newest valid A/B page.

## Runtime ownership and queues

Each device has one runtime allocator owner:

```text
owner = persistent_device_id % current_worker_count
```

Ownership is not persistent and therefore changes safely with worker count.
The owner maintains ordinary owner-local containers for:

- the pristine cursor;
- allocation epoch counter;
- ready blocks;
- cold-free blocks;
- bitmap bytes;
- the device's epoch-page image and bitmap/epoch A/B generations.

Another worker requests a block by submitting a coroutine to that owner. There
is no NxN free-lane matrix and no MPMC ready queue.

Partition replication control remains on
`partition_id % current_worker_count`. That partition owner computes the next
epoch, broadcasts the persistence request to all device owners, waits for all
of them, and then installs the new in-memory value. Sharing a 4 KiB epoch page
between many partitions is safe because each device owner serializes all page
read-modify-write operations for its own device.

Each storage worker holds at most one active append block and one standby block
ID. At 75% active-block occupancy it starts a standby request. Promotion is
local. If the active block fills before the standby arrives, only the writing
coroutine waits; it releases the worker store-state mutex for ordinary commands so
unrelated work can continue. Partition reset deliberately keeps the mutex while
waiting because its epoch transition must remain serialized.

The foreground allocator preserves a small per-device defrag reserve. Device
selection retains worker affinity where possible and falls back to other
devices when needed. If no foreground block is immediately available, an
ordinary write waits in its coroutine while a flush or defrag pass is active,
then retries every device. A monotonically increasing reclaim generation closes
the completion race: `ResourceExhausted` is returned only after a stable
observation with no flush/defrag in progress and no newly returned block.
Defrag's own reserve allocation never waits for another defrag, avoiding a
self-deadlock when the protected reserve is genuinely exhausted. An I/O failure
still stops the writer and is reported as `FailedPrecondition`, rather than
being mistaken for capacity exhaustion.

The reserve and scheduler are per device, not per worker. Every device protects
eight ready blocks from foreground allocation and has an independent ready
queue with at most eight active defrag permits. A candidate is queued according
to its source block's device. Releasing a permit wakes work only for that
device, so workers do not continuously contend for reserved block IDs and one
device cannot consume another device's recovery capacity. Actual concurrency
is also naturally capped by the worker count because a worker runs at most one
defrag pass at a time. A candidate that temporarily encounters
`ResourceExhausted` is returned to the queue instead of being lost.

## Startup sequence

Recovery runs in this order:

1. Validate all device labels and reconstruct persistent device ordering.
2. Compute the fixed metadata prefix from the persisted capacity.
3. Load A/B epoch pages from every device and install canonical DB/partition
   epochs.
4. Load each device's A/B scan bitmap.
5. Distribute each device's physical data-block range across the current
   workers using a global linear ordinal; device capacities need not match.
6. Skip bit-0 blocks; inspect bit-1 headers; fully read valid committed blocks.
7. Reject records with stale DB or partition epochs and collect keyed
   candidates plus transaction commit evidence.
8. Finalize the committed-transaction set and select each key's winning
   single-record version, rebuilding shielding facts from older versions.
9. Install winning indexes and positively charge their records and external
   extents to the owning blocks.
10. Cross one global accounting barrier after every worker's exact
    `live_bytes_` charges have been installed. Release recovery-only candidate
    state after that barrier.
11. Rebuild allocation high-watermarks and per-device ready pools from the
    resulting zero/nonzero live-byte state.

The scan assignment depends on the current worker count, while block identity
and metadata do not. Restarting with a different number of workers or a
different command-line device order therefore does not rewrite data.


## Crash invariants

- A data block is never writable before its scan bit is durable.
- A DB or partition epoch is never published before its metadata page is
  durable on every configured device.
- A defragged block is never reusable before its old header is durably zero.
- An unused standby may leave a durable bit with a zero header; recovery safely
  returns it to the ready pool.
- Metadata A/B generations make a torn page update fall back to the previous
  valid slot.

## Deliberate limitations

- Corrupt A/B metadata currently fails startup; full-scan reconstruction is not
  implemented.
- Bitmap bit clearing and a bounded warm pool are not implemented yet.
- The bitmap does not avoid record scanning on a nearly full device. Fast
  recovery at high occupancy will eventually require a partitioned persistent
  index checkpoint.
- Device addition requires a restart with the complete old set and zero-label
  new paths. Online addition and device removal are not implemented.
