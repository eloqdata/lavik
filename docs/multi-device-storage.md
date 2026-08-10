# Multi-Device Storage

Keylane can use multiple local files or raw block devices for online data.
Object storage remains a backup/restore target rather than part of the active
write path.

## Configuration

Repeat `--data-file` once per existing regular file or block device. Keylane
does not create, extend, truncate, or preallocate storage paths during startup.
Regular files should be provisioned explicitly by the deployment layer, for
example with `fallocate`.

```text
fallocate -l 1T /var/lib/keylane/data-0
fallocate -l 2T /var/lib/keylane/data-1
keylane \
  --data-file /var/lib/keylane/data-0 \
  --data-file /var/lib/keylane/data-1
```

A fresh regular file must be an 8 MiB multiple. A fresh raw block device uses
all complete 8 MiB blocks and ignores a tail smaller than one block. Capacity
cannot exceed 1 PiB per device. Initialized paths use the capacity persisted in
their device label: a smaller backing object is rejected, while newly added
tail capacity is ignored until online expansion has an explicit design.

Every device must have room for fixed metadata, an eight-block defrag reserve,
and at least one foreground data block. With the current metadata layout this
makes 80 MiB the minimum size of every device.

## Persistent identity and block IDs

The first 4 KiB page contains a checksummed device label: storage-set ID,
persistent device ID, device count, capacity, and block size. Device IDs are
dense in `[0, device_count)`.

A 64-bit block ID is encoded as:

```text
63                         27 26                         0
+----------------------------+----------------------------+
|       device_id (37)       |    local_block_id (27)     |
+----------------------------+----------------------------+
```

Changing `--data-file` argument order does not change identity. Startup requires
the complete persisted set and rejects a missing, duplicate, foreign, or empty
device.

The label is followed by capacity-derived fixed A/B metadata pages for database
epochs, partition epochs, and the recovery scan bitmap. Data begins at the next
8 MiB boundary; it is not hard-coded to local block one. See
[Recovery Metadata Layout](recovery-metadata-design.md).

## Per-device allocator ownership

Each device has one allocator owner chosen from the current workers:

```text
allocator_owner = device_id % worker_count
```

The owner exclusively maintains the device's ready/cold vectors, pristine
cursor, allocation epoch, bitmap, epoch-page image, and metadata generations.
These are ordinary owner-local structures. A non-owner requests an ID or epoch
page update through a cross-worker task; there is no shared MPMC free queue,
bitmap CAS, global metadata-page owner, or special worker-zero writer.

Fresh block IDs are activated in batches of 256. The owner makes their bitmap
bits durable before adding them to its ready pool. Defrag returns a durably
zeroed block to the same owner and keeps its bit set for cheap warm reuse.

## Worker write affinity

Each worker has at most one active block plus one prefetched standby ID.

- Every device protects its last eight allocatable blocks for defrag. Device
  weight is its remaining foreground data-block count after that reserve.
- With at least as many workers as usable devices, every device gets one home
  worker and remaining workers are apportioned by weight.
- With more usable devices than workers, devices are greedily grouped to keep
  aggregate group weights balanced. A worker chooses within its group by the
  lowest allocated-blocks/weight ratio.
- Allocation falls back to other devices when the preferred device is full.

The standby request starts when an active block reaches 75% occupancy. This
keeps the usual rollover off the latency-critical path without reserving an
8 MiB memory buffer per standby or per device. Weighting is computed at startup
and allocation counters are worker-local; the hot path does not read a shared
global free-space counter.

Defrag scheduling is also per device. A device has its own ready queue and a
runtime-configurable active-job limit, capped at eight to match its eight-block
reserve. `DEFRAG MAX-ACTIVE N` therefore applies independently to every device,
not across the whole process. A source block is queued on the device that
contains it, so a busy or full device does not consume another device's
permits. The reserve is capacity, not eight fixed block identities: defrag
consumes a ready destination, returns the cleaned source, and the protected
free space rotates over time.

## Recovery and worker-count changes

DB epochs and all 16,384 partition epochs are mirrored on every device. Recovery
loads them before it considers data records. The scan bitmap skips pristine
blocks, while set bits lead to a header check and, for valid headers, a full
block read.

Physical scan work is redistributed across the current workers. Recovered
blocks keep their old writer provenance only for validation and are assigned a
current runtime owner when necessary. New writes use the current worker/device
affinity. No data rewrite is required when worker count changes.

Online device addition/removal is not implemented. During development, changing
device membership requires clearing and reinitializing the storage set.
