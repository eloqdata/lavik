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
- the expected checkpoint index-block count;
- the expected checkpoint entry count.

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

Each worker serializes its frozen indexes into one or more 8 MiB checkpoint
index blocks. An entry contains the complete key, database and partition
epoch, physical record location, logical type and size, expiry and shielding
state, and any extent manifest. It never stores the process-random key digest;
startup recomputes that digest from the complete key. Each index block carries
its generation, shard, entry count, payload size, and payload checksum in its
existing block header.

Publication order is:

```text
write and synchronize every checkpoint index block
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
generation. For matching blocks each scanner validates block identity and
allocation epoch, generation, shard, bounds, entry counts, and CRC32C payload
checksums, then submits the complete decoded block to its index-owning worker.
I/O, decoding, and index construction therefore proceed concurrently. Each
scanner holds at most one 8 MiB block and its decoded batch, so temporary entry
memory is bounded by worker count rather than dataset size. A barrier reduces
the per-scanner block, entry, and shard results; totals must exactly match the
root and every worker shard must be represented. If a block or the final
completeness check fails, startup disables ordinary-body skipping and performs
the full record scan. Entries from already validated checkpoint blocks remain
installed and participate in the normal winner merge; the full scan supplies
every missing key.

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
extents to physical block owners. The speedup comes from replacing full
ordinary-block reads and obsolete-version decoding with a compact sequential
representation of current index entries; it does not make startup independent
of allocated-block count.

## Source map

| Claim | Repository source |
|---|---|
| Configuration and default | `include/keylane/server.h`, `app/keylane.cpp`, `src/config.cpp`, `src/redis/server.cpp` |
| Durable generation, root, bitmap, and block kind | `include/keylane/storage/format.h`, `src/storage/format.cpp` |
| Shutdown barriers, transaction promotion, shard construction, bitmap/root publication, and best-effort failure | `src/storage/engine/flush.cpp`, `src/storage/engine/tx_cleaner.cpp`, `src/storage/engine/checkpoint.cpp` |
| Startup consumption, validation, fallback, bitmap retirement, and ordinary-body skipping | `src/storage/engine/init.cpp`, `src/storage/engine/checkpoint.cpp`, `src/storage/engine/recovery.cpp` |
