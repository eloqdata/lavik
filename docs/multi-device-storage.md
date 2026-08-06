# Multi-Device Storage

Keylane can use multiple local files or raw block devices for its online data
path. Object storage is outside this path and is intended for backup and
restore.

## Configuration

Repeat `--data-file` once per file or block device. Every configured device
currently uses the same `--data-file-size-mb` capacity.

```text
keylane \
  --data-file /dev/nvme0n1 \
  --data-file /dev/nvme1n1 \
  --data-file-size-mb 1048576
```

The configured size must be a multiple of 8 MiB and must not exceed 1 PiB per
device. For a raw block device, Keylane verifies that its actual capacity is at
least the configured size.

## Persistent identity and block IDs

The first 4 KiB page of local block zero contains a checksummed device label.
The label records the storage-set ID, persistent device ID, expected number of
devices, capacity, and block size. The remaining metadata in local block zero
includes a mirrored copy of database epochs. Data starts at local block one.

A 64-bit block ID is encoded as:

```text
63                         27 26                         0
+----------------------------+----------------------------+
|       device_id (37)       |    local_block_id (27)     |
+----------------------------+----------------------------+
```

Device IDs are dense in the range `[0, device_count)`. Consequently, changing
the order of `--data-file` arguments does not change block identity, and block
lookup uses a direct array index rather than a hash-table lookup.

At startup Keylane requires the complete persisted device set. A missing,
duplicate, foreign, or empty device causes startup to fail before recovery.
Online device addition and removal are not implemented yet; initialize a new
set after clearing data when changing device membership during development.

## Block allocation

Each worker has a sticky home device:

- With at least as many workers as devices, worker `w` uses
  `w % device_count`.
- With more devices than workers, each worker round-robins only among its own
  interleaved subset (`w`, `w + worker_count`, ...).
- If that subset is full, allocation falls back to the other devices.

This keeps the common write path on one device per worker while retaining the
capacity of the whole set. It can improve NUMA, page-cache metadata, and block
queue locality. Changing a registered-file index in io_uring is already cheap,
so it is not the main source of the expected gain.

Fresh blocks have per-device atomic allocation cursors. Reclaimed blocks return
to a per-device MPMC free queue and are retried in the same home-device order as
fresh blocks. The small defrag reserve is distributed across those queues, so
the steady-state reuse path preserves device affinity without a global free-list
counter or queue.

## Recovery and metadata

Recovery scans every data block on every device and validates that the block ID
stored in its header matches its physical device and local offset. Recovery is
independent of the current worker count.

Database epochs are mirrored on every device. Startup takes the component-wise
maximum valid epoch and rewrites that canonical metadata to all devices. This
keeps `FLUSHDB` monotonic across a partial mirrored update, while the persisted
device-count check prevents an omitted device from being silently ignored.
