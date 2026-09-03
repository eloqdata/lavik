# Shutdown index checkpoints

## Role and authority

A shutdown index checkpoint is an optional, one-use cache of the in-memory
top-level indexes. It reduces restart disk traffic; it does not replace
ordinary records or change command durability. Checkpoint loading validates
the serialized index structure, while the referenced ordinary record body is
validated lazily when it is read. A structurally valid checkpoint can therefore
restore a location whose later value read reports media corruption even when a
cold body scan would have reported that corruption during startup.
`shutdown-checkpoint yes` enables creation at clean shutdown and use at the
next startup. The CLI spelling is `--shutdown-checkpoint`; the default is
disabled.

Checkpoint construction starts only after request admission has stopped and
every worker has sealed and flushed its record streams, drained storage
maintenance, and reached the shutdown barrier. Worker 0 then runs transaction
cleaning to a fixed point regardless of the online cleaner cooldown: committed
transaction-tagged winners are relocated to durable ordinary records and the
old transaction generations are retired before any index shard is frozen.
There is no online checkpoint flow. The build is therefore O(current index
entries), including reading complete keys that are not retained inline; it
does not scan obsolete record versions.

## Durable representation and publication

Four values at the end of mirrored epoch metadata form the checkpoint root:

- the monotonically increasing published generation;
- the highest consumed generation;
- the expected checkpoint block count;
- the expected checkpoint index-entry count.

These fields and the checkpoint block kinds directly extend development
storage format version 1. There is no compatibility decoder; existing media
must be cleared when moving between incompatible development layouts. Each
root copy participates in the existing per-page A/B generation and CRC32C
protocol on every device.

Each device also has a checkpoint bitmap parallel to its allocation bitmap,
with one bit per physical block and the same independently checksummed A/B page
scheme. It is a discovery index, not allocation authority: a set bit only asks
the loader to inspect that block, and the allocation bitmap plus the block's
own header still determine whether it is a matching candidate. The bitmap adds
fixed metadata space but does not reserve checkpoint data blocks or require a
contiguous block range. Its raw payload is `ceil(capacity_blocks / 8)` bytes;
the A/B copies consume twice that amount plus page headers (32 MiB at the
per-device 1 PiB limit).

Each worker serializes three chunk kinds into ordinary 8 MiB checkpoint blocks.
Its single capacity chunk contains the exact entry count for every owned
partition and logical-database index, including empty indexes. The complete
directory fits in one block even with one worker. Startup uses these counts to
allocate final index bucket tables before it installs keys; the directory is
also a completeness check independent of how index entries happen to be split
across blocks.
Index chunks contain the complete key, database and partition epoch, physical
record location, logical type and size, expiry and shielding state, and any
extent manifest. Their fixed entry header packs owner, database, type and flags
into one metadata word, derives entry bounds from the chunk and field lengths,
and stores expiry only when present. It never stores the process-random key
digest; startup recomputes that digest from the complete key.

Block-accounting chunks contain one entry for every live ordinary or extent
block owned by the shard, not one entry per key. Each entry stores the block
identity, owner, aggregate live bytes, and the extra extent identity needed to
validate a manifest against its physical header. The dense runtime block table
is the authority for ordinary-block aggregates. While performing the required
index serialization pass, the builder aggregates the sparse extent manifests
because extent identity deliberately does not occupy every runtime
`BlockState`, and an extent's recovery owner can differ from its key-index
shard. The restored extent entry therefore resolves its physical owner only
after the block-header scan.
Capacity, index, and accounting chunks share the checkpoint block kind,
generation, bitmap, publication lifecycle, and payload CRC; an explicit
chunk-kind field selects their entry layout.

Publication order is:

```text
write and synchronize every checkpoint capacity, index, and accounting block
write and synchronize the checkpoint bitmap on every device
publish the generation and expected counts through the root on every device
```

Until the final step completes, new blocks are unpublished acceleration state.
A partial or failed build never changes the published generation. Its bitmap
bits may remain as false positives, but the old root generation cannot select
the new block headers. Insufficient foreground space, an index entry larger
than one checkpoint block, transaction cleanup that cannot quiesce or relocate
its winners, or I/O failure causes this shutdown's checkpoint attempt to fail
without changing the durability of ordinary or transaction records. A tagged
winner observed by shard serialization is an invariant check for incomplete
cleanup, not a representation supported by the checkpoint. Checkpoint
allocation does not consume the defrag reserve and does not wait for online
reclamation.

## Startup consumption and fallback

Preparation compares root copies from every device. A usable root must be
identical across the storage set, name an unconsumed generation, contain
plausible expected counts, and have valid checkpoint-bitmap pages. Before
reading the checkpoint, worker 0 persists `consumed_generation = generation`
to every device. If this startup later crashes, another startup cannot reuse a
snapshot that predates the failed process's runtime writes.

The loader requires the same worker count that created the checkpoint, then
all recovery workers walk disjoint, topology-aware stripes of its set bits.
SPDK workers touch only devices for which they own a qpair; io_uring workers
stripe the complete storage set. A false positive is ignored unless the
allocated block has a self-validating checkpoint header for the selected
generation. For matching blocks a 12 KiB prefix read validates and classifies
the block header and chunk header. Scanners then read the capacity chunks in
full and rendezvous before any key is installed. Startup requires exactly one
capacity chunk per worker, exactly one declaration per partition and database,
and a declared sum equal to the root entry count. Worker 0 dispatches one
owner-local allocation pass per worker; every nonempty `ScanHashMap` receives
the same final power-of-two bucket count and 75-percent target it would have
after normal growth.

After that allocation barrier, each scanner validates block identity and
allocation epoch, generation, shard, bounds, entry counts, and CRC32C payload
checksums, then submits the complete decoded index or accounting block to its
owner. Each scanner double-buffers checkpoint reads: after a block completes
I/O it submits the next block before decoding and installing the current one.
I/O, decoding, and index construction therefore proceed concurrently both
within and across scanners. A scanner holds at most two 8 MiB buffers and one
decoded batch, so temporary entry memory remains bounded by worker count rather
than dataset size. Barriers reduce the per-scanner block, index-entry,
accounting-entry, capacity, and shard results. Block and index-entry totals
must exactly match the root, every worker must have all three chunk kinds
represented, and each installed index size must equal its capacity declaration.
The published total block count makes a missing chunk detectable without
adding another root field. If a block or the final completeness check fails,
startup
disables ordinary-body skipping and performs the full record scan. Entries
from already validated checkpoint blocks remain installed and participate in
the normal winner merge; the full scan supplies every missing key.

After the one load attempt, startup writes an all-zero checkpoint bitmap before
the parallel storage scan. On success, every matching index block is already
resident; its allocation bit is also cleared and its block ID enters the
allocator's cold-free pool. Stale, partial, or invalid checkpoint blocks not
selected by the bitmap and root are recognized during the normal header scan
and reclaimed through the same allocation-bitmap lifecycle. The consumed root
makes interruption of either clearing operation safe. A checkpoint-bitmap
clear I/O failure is reported but does not block authoritative record recovery;
the consumed root still prevents reuse.

With a valid checkpoint, recovery still scans the allocation bitmap and reads
the two header pages of every remaining allocated block. It reads transaction
block bodies to reconstruct durable commit evidence and extent headers for
identity validation, but skips the 8 MiB bodies of ordinary record blocks. Its
disk work is:

```text
O(allocated block headers + checkpoint bytes + transaction block bytes)
```

Recovery still walks the rebuilt winner indexes once to charge live roots and
extents to physical block owners after a cold scan. A successful checkpoint
load instead restores the persisted block-accounting table after the
block-header scan establishes runtime owners. Each absolute value is installed
once; a duplicate table entry or a mismatch with the scanned block identity is
a checkpoint failure. This removes both the second winner-index walk and the
per-key hash aggregation formerly performed while decoding the checkpoint.
The speedup still does not make startup independent of index-entry or allocated
block count: every checkpoint key must be decoded and every allocated block
header must be scanned.

Capacity chunks directly replace the earlier two-kind version-1 checkpoint
layout. An earlier checkpoint is rejected before any key body is installed and
falls back to the authoritative record scan; ordinary record media is
unchanged.

## Source map

| Claim | Repository source |
|---|---|
| Configuration and default | `include/keylane/server.h`, `app/keylane.cpp`, `src/config.cpp`, `src/redis/server.cpp` |
| Durable generation, root, bitmap, and block kind | `include/keylane/storage/format.h`, `src/storage/format.cpp` |
| Shutdown barriers, transaction promotion, shard construction, bitmap/root publication, and best-effort failure | `src/storage/engine/flush.cpp`, `src/storage/engine/tx_cleaner.cpp`, `src/storage/engine/checkpoint.cpp` |
| Startup consumption, validation, fallback, bitmap retirement, and ordinary-body skipping | `src/storage/engine/init.cpp`, `src/storage/engine/checkpoint.cpp`, `src/storage/engine/recovery.cpp` |
