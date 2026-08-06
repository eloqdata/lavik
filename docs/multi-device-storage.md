# Multi-Device Storage

Keylane can use multiple local files or raw block devices for online data.
Object storage remains a backup/restore target rather than part of the active
write path.

## Configuration

Repeat `--data-file` once per file or block device. Every configured device
currently uses the same fixed `--data-file-size-mb` capacity.

```text
keylane \
  --data-file /dev/nvme0n1 \
  --data-file /dev/nvme1n1 \
  --data-file-size-mb 1048576
```

Capacity must be an 8 MiB multiple and cannot exceed 1 PiB per device. For a
raw block device, Keylane verifies that its actual size is sufficient.

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

- With at least as many workers as devices, worker `w` first tries
  `w % device_count`.
- With more devices than workers, a worker round-robins its interleaved subset
  (`w`, `w + worker_count`, ...).
- Allocation falls back to other devices when the preferred device is full.

The standby request starts when an active block reaches 75% occupancy. This
keeps the usual rollover off the latency-critical path without reserving an
8 MiB memory buffer per standby or per device.

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
