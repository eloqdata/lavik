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
makes 80 MiB the minimum size of every device. A fresh set represents an absent
durable Function catalog as the canonical empty catalog, so it does not spend
that last block during startup. Catalog commits use ordinary foreground
capacity; when it is exhausted, the Function mutation fails without changing
the current catalog.

## Add devices without clearing existing data

Device addition is an offline operation. The safe sequence is:

1. Stop Keylane cleanly. Do not change or clear any existing member.
2. Provision new paths whose first 4 KiB Keylane label page is zero. Include
   every existing member and every new path in the next launch.
3. Start Keylane and wait for `expanded storage set from OLD to NEW devices`
   followed by normal recovery completion before sending traffic.
4. Keep using the complete expanded path list on every later restart.

For a new regular file, allocate its final size before startup:

```sh
fallocate -l 1T /var/lib/keylane/data-1

keylane \
  --data-file /var/lib/keylane/data-0 \
  --data-file /var/lib/keylane/data-1
```

For a raw block device, first verify the exact device and that it is neither
mounted nor in use. Prefer a genuinely empty or fully sanitized device. A
device that has never held Keylane data already has a zero label. Clearing only
the 4 KiB label of a former Keylane member makes it eligible as a new member,
but does not provide secure erasure or a crash-safe stale-media sanitization
guarantee:

```sh
lsblk -o NAME,PATH,SIZE,MODEL,SERIAL,MOUNTPOINTS /dev/nvme1n1
findmnt -rn -S /dev/nvme1n1
sudo fuser -v /dev/nvme1n1
sudo blkdiscard -z -f --offset 0 --length 4096 /dev/nvme1n1

keylane \
  --data-file /dev/nvme0n1 \
  --data-file /dev/nvme1n1
```

The label reset is destructive to the selected new member's old Keylane
storage set. It must never target an existing member being preserved. Keylane
rewrites the new path's fixed metadata prefix, but it does not securely erase
the data region or prove every crash window between block activation and first
flush. Use fully sanitized media when stale-data isolation is required, and
retain the original data set or backup until expansion succeeds.

Expansion preserves old device IDs and assigns new IDs after them. Command-line
order is irrelevant, foreign initialized devices are rejected, and an empty
path cannot replace a missing old member. If startup is interrupted during
expansion, restart with the same complete path list; the transition is safe to
retry.

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
the complete persisted set and rejects a missing, duplicate, or foreign device.
While Keylane is stopped, empty devices may be appended to the complete
existing set by adding more `--data-file` arguments. Existing data and device
IDs are preserved; new device IDs are appended independently of argument order.

The label is followed by capacity-derived fixed A/B metadata pages for database
epochs, partition epochs, the recovery scan bitmap, and a process-global
system-state root. The root is mirrored on every configured device and points
to the Function catalog, full-sync eligibility, and promotion base manifest.
Data begins at the next 8 MiB boundary; it is not hard-coded to local block
one. See the current
[storage and recovery architecture](../architecture/04-storage-and-recovery.md).

This build writes the current storage format version 1 and does not detect or
preserve compatibility with pre-deployment layouts that previously reused
that version. Such media is unsupported. Moving between incompatible
development layouts requires a backup plus reset/restore, or a full sync from
a compatible source; replacing only the binary is not sufficient.

## Per-device allocator ownership

Each device has one allocator owner chosen from the current workers:

```text
allocator_owner = device_id % worker_count
```

The owner exclusively maintains the device's ready/cold vectors, pristine
cursor, allocation epoch, bitmap, epoch-page image, and metadata generations.
These allocator structures are ordinary owner-local state. A non-owner
requests an ID or epoch-page update through a cross-worker task; there is no
shared MPMC free queue or bitmap CAS. Separately, worker zero is the unique
writer for the process-global system-state manifest so catalog and promotion
updates cannot overwrite one another.

Fresh block IDs are activated in batches of 256. The owner makes their bitmap
bits durable before adding them to its ready pool. Reclaimed blocks follow a
cold-reuse lifecycle: clear and persist the allocation bit before adding the
ID to `cold_free`; before reuse, zero and synchronize the stale 8 KiB header,
then set and persist the allocation bit before publishing the ID as ready.

## Worker write affinity

Each worker has one ordinary active append block and may have one transaction
append block for each live transaction generation. The ordinary stream retains
at most one prefetched standby ID; transaction streams allocate per generation
on demand and do not reserve standby capacity.

- Every device protects its last eight allocatable blocks for defrag. Device
  weight is its remaining foreground data-block count after that reserve.
- With at least as many workers as usable devices, every device gets one home
  worker and remaining workers are apportioned by weight.
- With more usable devices than workers, devices are greedily grouped to keep
  aggregate group weights balanced. A worker chooses within its group by the
  lowest allocated-blocks/weight ratio.
- Allocation falls back to other devices when the preferred device is full.

The standby request starts immediately after an ordinary active block is
installed, and starts again after rollover consumes that standby. Appends do
not calculate an occupancy threshold. The request uses the same stateful
allocation gate as rollover: if rollover catches the prefetch, it waits for the
task and then consumes the published ID, without relying on an edge-triggered
notification. An unused reservation is returned after failure or during
shutdown. No 8 MiB staging buffer is attached until the ID becomes active.

If no standby is available, rollover allocates inline from the chosen device's
ready pool. When that pool falls within 32 blocks of the defrag reserve, the
device owner starts a background 256-block refill. This normally keeps bitmap
persistence out of the inline allocation path. Weighting is computed at
startup and allocation counters are worker-local; the hot path does not read a
shared global free-space counter.

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

Device addition is an offline operation: stop Keylane, provision zero-label
devices, and restart with the complete old set plus the new paths. Keylane
initializes each new device's fixed metadata, mirrors the current epochs and
highest system-state root common to the old members, then publishes the larger
member count. Normal recovery accepts only the highest valid system-state
generation whose exact root is present on every configured device. A torn root
update therefore falls back to the prior common generation; a set with no
common valid generation fails startup. Interrupted expansion is safe to retry
with the same complete path list. Removing a device and adding devices while
the server is running are not implemented.
