#include "engine_impl.h"

namespace keylane::storage {

Task<StatusOr<DiskValue>> StorageEngine::Impl::Get(std::uint8_t db_id,
                                                   std::string_view key,
                                                   ReadLatencyTrace* trace) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  co_return co_await GetLocked(db_id, key, digest, trace);
}

Task<StatusOr<DiskValue>> StorageEngine::Impl::GetLocked(std::uint8_t db_id,
                                                         std::string_view key,
                                                         const Digest& digest,
                                                         ReadLatencyTrace* trace) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  if (found == nullptr || found->value.kind == RecordKind::kTombstone) {
    if (trace != nullptr) {
      trace->lookup_done_ns = ReadTraceNowNanos();
    }
    co_return Status(StatusCode::kNotFound, "key not found");
  }
  const std::uint64_t now_ms = UnixTimeMillis();
  if (IsExpired(found->value, now_ms)) {
    QueueExpiredCandidate(store, partition.id, db_id, *found);
    if (trace != nullptr) {
      trace->lookup_done_ns = ReadTraceNowNanos();
    }
    co_return Status(StatusCode::kNotFound, "key not found");
  }
  if (trace != nullptr) {
    trace->hit = true;
    trace->lookup_done_ns = ReadTraceNowNanos();
  }

  auto loaded =
      co_await LoadValue(store, db_id, key, digest, found->value, trace);
  if (!loaded.ok()) {
    co_return loaded.status();
  }

  co_return EncodeDiskValue(std::move(*loaded));
}

Task<StatusOr<std::uint64_t>> StorageEngine::Impl::StringLength(
    std::uint8_t db_id, std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  co_return co_await StringLengthLocked(db_id, key, digest);
}

Task<StatusOr<std::uint64_t>> StorageEngine::Impl::StringLengthLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto* found = partition.indexes[db_id].Find(digest, key);
  if (found == nullptr || found->value.kind != RecordKind::kValue ||
      IsExpired(found->value, UnixTimeMillis())) {
    if (found != nullptr && IsExpired(found->value, UnixTimeMillis())) {
      QueueExpiredCandidate(store, partition.id, db_id, *found);
    }
    co_return Status(StatusCode::kNotFound, "key not found");
  }
  if (found->value.value_type != ValueType::kString) {
    co_return Status(StatusCode::kInvalidArgument,
                     "WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  co_return found->value.logical_size;
}

Task<ExpirationInfo> StorageEngine::Impl::GetExpiration(std::uint8_t db_id,
                                                        std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  co_return co_await GetExpirationLocked(db_id, key, digest);
}

Task<ExpirationInfo> StorageEngine::Impl::GetExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto* found = partition.indexes[db_id].Find(digest, key);
  if (found == nullptr || found->value.kind != RecordKind::kValue) {
    co_return ExpirationInfo{};
  }
  if (IsExpired(found->value, UnixTimeMillis())) {
    QueueExpiredCandidate(store, partition.id, db_id, *found);
    co_return ExpirationInfo{};
  }
  co_return ExpirationInfo{
      .exists = true,
      .expire_at_ms = found->value.expire_at_ms,
  };
}

bool StorageEngine::Impl::KeyLive(std::uint8_t db_id, std::string_view key,
                                  const Digest& digest) const {
  assert(db_id < kLogicalDatabaseCount);
  const WorkerStore& store = CurrentStore();
  auto& partition =
      const_cast<Impl*>(this)->PartitionForKey(const_cast<WorkerStore&>(store),
                                               key);
  const auto* found = partition.indexes[db_id].Find(digest, key);
  return found != nullptr && found->value.kind == RecordKind::kValue &&
         !IsExpired(found->value, UnixTimeMillis());
}

Task<bool> StorageEngine::Impl::Exists(std::uint8_t db_id, std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  co_return co_await ExistsLocked(db_id, key, digest);
}

Task<bool> StorageEngine::Impl::ExistsLocked(std::uint8_t db_id,
                                             std::string_view key,
                                             const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  if (found == nullptr || found->value.kind != RecordKind::kValue) {
    co_return false;
  }
  if (IsExpired(found->value, UnixTimeMillis())) {
    QueueExpiredCandidate(store, partition.id, db_id, *found);
    co_return false;
  }
  co_return true;
}

std::size_t StorageEngine::Impl::DirectGetValueLimit() const noexcept {
  const RegisteredBufferPoolOptions& buffers = options_.buffers;
  // Keep the direct-from-disk framing path conservative: with the defaults,
  // values through 1 MiB - 8 KiB avoid the value copy. Larger values retain
  // the materializing memmove path below.
  const std::size_t framing_reserve =
      buffers.read_headroom_bytes + buffers.read_tailroom_bytes;
  return buffers.read_payload_bytes > framing_reserve
             ? buffers.read_payload_bytes - framing_reserve
             : 0;
}

StatusOr<DiskValue> StorageEngine::Impl::EncodeDiskValue(LoadedValue loaded) {
  ReadBufferLease lease = std::move(loaded.lease);
  const std::size_t value_offset = loaded.value_offset;
  const std::size_t value_bytes = loaded.value_bytes;
  std::span<std::byte> buffer = lease.bytes();
  char length[32];
  auto [end, error] =
      std::to_chars(length, length + sizeof(length), value_bytes);
  if (error != std::errc{}) {
    return Status(StatusCode::kInternal, "bulk length formatting failed");
  }
  const std::size_t digits = static_cast<std::size_t>(end - length);
  const std::size_t prefix_bytes = digits + 3;
  if (value_offset < prefix_bytes || value_offset > buffer.size() ||
      value_bytes > buffer.size() - value_offset ||
      buffer.size() - value_offset - value_bytes < 2) {
    return Status(StatusCode::kInternal,
                  "value lacks RESP framing headroom or tailroom");
  }
  std::byte* prefix = buffer.data() + value_offset - prefix_bytes;
  prefix[0] = std::byte{'$'};
  std::memcpy(prefix + 1, length, digits);
  prefix[digits + 1] = std::byte{'\r'};
  prefix[digits + 2] = std::byte{'\n'};
  buffer[value_offset + value_bytes] = std::byte{'\r'};
  buffer[value_offset + value_bytes + 1] = std::byte{'\n'};
  const std::size_t network_offset = value_offset - prefix_bytes;
  return DiskValue(std::move(lease), network_offset,
                   prefix_bytes + value_bytes + 2);
}

Task<StatusOr<StorageEngine::Impl::LoadedValue>> StorageEngine::Impl::LoadValue(
    WorkerStore& key_store, std::uint8_t db_id, std::string_view key,
    const Digest& digest, RecordLocation location, ReadLatencyTrace* trace) {
  assert(location.block_owner < worker_count_);
  // An external value's manifest is already decoded in this index entry, so
  // the record's own block holds nothing worth reading. Assemble here and
  // let each extent go straight to its block's owner, rather than hopping to
  // the record's owner first and having it acquire the output buffer from
  // its pool and hand the lease back across workers.
  if (location.external) {
    co_return co_await LoadExternalValueLocal(key_store, location, trace);
  }
  if (location.block_owner == key_store.worker->id()) {
    co_return co_await LoadValueLocal(key_store, db_id, key, digest, location,
                                      trace);
  }
  const unsigned owner = location.block_owner;
  std::string owned_key(key);
  co_return co_await celer::SubmitTaskTo(
      owner,
      [this, owner, db_id, key = std::move(owned_key), digest, location,
       trace]() mutable -> Task<StatusOr<LoadedValue>> {
        co_return co_await LoadValueLocal(*stores_[owner], db_id, key, digest,
                                          location, trace);
      });
}

Task<Status> StorageEngine::Impl::ReadExtentInto(WorkerStore& store,
                                                 ExtentRef ref,
                                                 std::uint32_t extent_index,
                                                 std::byte* destination) {
  BlockState* state = FindBlockState(store, ref.block_id);
  if (state == nullptr || !state->allocated || state->freeing ||
      state->kind != BlockKind::kValueExtent ||
      state->allocation_epoch != ref.allocation_epoch) {
    co_return Status(StatusCode::kInternal,
                     "stale or missing external extent");
  }
  ++state->pins;
  struct ExtentPin {
    BlockState* state;
    ~ExtentPin() { --state->pins; }
  } pin{state};
  const std::size_t read_bytes =
      AlignDirect(kBlockHeaderBytes + ref.payload_bytes);
  auto temp_acquired = co_await store.buffers.AcquireReadBuffer(read_bytes);
  if (!temp_acquired.ok()) {
    co_return temp_acquired.status();
  }
  ReadBufferLease temp = std::move(*temp_acquired);
  FixedBuffer io = temp.io_buffer();
  io.size = read_bytes;
  const auto [file_id, block_offset] = FileOffset(ref.block_id);
  auto read = co_await ReadStorageBuffer(*store.worker,
                                         store.files[file_id], io,
                                         temp.registered(), block_offset);
  if (!read.ok()) {
    co_return read.status();
  }
  if (*read != read_bytes) {
    co_return Status(StatusCode::kInternal, "short extent block read");
  }
  BlockHeader header{};
  if (!DecodeBlockHeaderPages(
          std::span<const std::byte, kBlockHeaderBytes>(
              io.data, kBlockHeaderBytes),
          &header) ||
      header.kind != BlockKind::kValueExtent ||
      header.block_id != ref.block_id ||
      header.allocation_epoch != ref.allocation_epoch ||
      header.extent_index != extent_index ||
      header.extent_payload_bytes != ref.payload_bytes ||
      header.extent_payload_checksum != ref.payload_checksum) {
    co_return Status(StatusCode::kInternal,
                     "extent header does not match manifest");
  }
  const auto payload = std::span<const std::byte>(
      io.data + kBlockHeaderBytes, ref.payload_bytes);
  if (options_.verify_read_crc && Crc32c(payload) != ref.payload_checksum) {
    co_return Status(StatusCode::kInternal,
                     "extent payload checksum mismatch");
  }
  std::memcpy(destination, payload.data(), payload.size());
  co_return Status::Ok();
}

Task<StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadExternalValueLocal(WorkerStore& store,
                                            const RecordLocation& location,
                                            ReadLatencyTrace* trace) {
  if (!location.external || location.extents == nullptr ||
      location.logical_size > kMaxStringBytes) {
    co_return Status(StatusCode::kInternal,
                     "external value has no valid extent manifest");
  }
  if (trace != nullptr) {
    trace->buffer_acquire_start_ns = ReadTraceNowNanos();
  }
  auto acquired = co_await store.buffers.AcquireReadBuffer(
      static_cast<std::size_t>(location.logical_size));
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease output = std::move(*acquired);
  FixedBuffer destination = output.io_buffer();
  if (destination.size < location.logical_size) {
    co_return Status(StatusCode::kOutOfRange,
                     "external value exceeds read buffer capacity");
  }
  if (trace != nullptr) {
    trace->buffer_acquired_ns = ReadTraceNowNanos();
    trace->heap_read_buffer = !output.registered();
    trace->disk_read = true;
  }
  if (trace != nullptr) {
    trace->io_submit_ns = ReadTraceNowNanos();
  }
  std::size_t output_offset = 0;
  for (std::size_t index = 0; index < location.extents->size(); ++index) {
    const ExtentRef& ref = location.extents->at(index);
    if (output_offset + ref.payload_bytes > location.logical_size) {
      co_return Status(StatusCode::kInternal,
                       "extent header does not match manifest");
    }
    // The manifest is held by the record's owner, but each extent block has
    // its own owner, and after a worker-count change the two are unrelated.
    // Hop to the block's owner exactly as LoadValue does for records.
    const std::uint16_t owner = BlockOwner(ref.block_id);
    if (owner >= worker_count_) {
      co_return Status(StatusCode::kInternal,
                       "stale or missing external extent");
    }
    std::byte* target = destination.data + output_offset;
    // if/else, not ?:, to keep the two co_awaits in separate full
    // expressions (GCC coroutine frame-slot aliasing).
    Status read = Status::Ok();
    if (owner == store.worker->id()) {
      read = co_await ReadExtentInto(store, ref,
                                     static_cast<std::uint32_t>(index),
                                     target);
    } else {
      read = co_await celer::SubmitTaskTo(
          owner, [this, owner, ref, index, target]() -> Task<Status> {
            co_return co_await ReadExtentInto(
                *stores_[owner], ref, static_cast<std::uint32_t>(index),
                target);
          });
    }
    if (!read.ok()) {
      co_return read;
    }
    output_offset += ref.payload_bytes;
  }
  if (output_offset != location.logical_size) {
    co_return Status(StatusCode::kInternal,
                     "external value length does not match manifest");
  }
  if (trace != nullptr) {
    trace->io_complete_ns = ReadTraceNowNanos();
    trace->decode_done_ns = trace->io_complete_ns;
  }
  const std::size_t value_offset = static_cast<std::size_t>(
      destination.data - output.bytes().data());
  co_return LoadedValue{std::move(output), value_offset, output_offset};
}

Task<StatusOr<StorageEngine::Impl::LoadedValue>> StorageEngine::Impl::LoadValueLocal(
    WorkerStore& store, std::uint8_t db_id, std::string_view key,
    const Digest& digest, RecordLocation location, ReadLatencyTrace* trace) {
  if (location.external) {
    co_return co_await LoadExternalValueLocal(store, location, trace);
  }
  // TODO: Coalesce concurrent reads of the same aligned disk page, like
  // the reference engine tiering::OpManager::pending_reads_. Key the in-flight table by
  // (file_id, aligned offset, aligned length), submit one read, and fan the
  // decoded result out to all waiting coroutines. In-flight operations must
  // retain values/leases, never flat_hash_map iterators or element pointers.
  const auto [file_id, block_offset] = FileOffset(location.block_id);
  const std::uint64_t absolute_offset =
      block_offset + location.record_offset;
  const std::uint64_t direct_io_mask =
      static_cast<std::uint64_t>(direct_io_alignment_ - 1);
  const std::uint64_t aligned_offset = absolute_offset & ~direct_io_mask;
  const std::size_t record_headroom =
      static_cast<std::size_t>(absolute_offset - aligned_offset);
  const std::size_t record_span =
      record_headroom + location.total_disk_bytes;
  const std::size_t read_bytes =
      (record_span + direct_io_alignment_ - 1) & ~direct_io_mask;

  // Acquire the output buffer before resolving any block state. This is the
  // only suspension the staged-copy path would otherwise have, and it used
  // to sit between reading the staging pointer and using it, so that path
  // needed a pin to stop a flush from handing the staging buffer back. With
  // the acquisition hoisted, the staged copy runs straight through on a
  // worker that cannot preempt it, and only the disk read below pins. The
  // disk sizing covers the staged copy too: read_bytes is at least
  // total_disk_bytes, which is header plus payload.
  if (trace != nullptr) {
    trace->buffer_acquire_start_ns = ReadTraceNowNanos();
  }
  auto acquired = co_await store.buffers.AcquireReadBuffer(read_bytes);
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  if (trace != nullptr) {
    trace->buffer_acquired_ns = ReadTraceNowNanos();
    trace->heap_read_buffer = !lease.registered();
  }

  BlockState* state = FindBlockState(store, location.block_id);
  if (state == nullptr || !state->allocated || state->freeing ||
      state->allocation_epoch != location.allocation_epoch) {
    co_return Status(StatusCode::kInternal, "stale index block epoch");
  }

  // Only records appended since the last flush live in a staging buffer, so
  // on a read-mostly workload this branch is rare. Keep the disk read on the
  // straight-line path.
  if (location.in_memory && state->in_memory) [[unlikely]] {
    auto in_mem_buffer = StagingBufferFor(store, *state);
    if (!in_mem_buffer.data || in_mem_buffer.size == 0 ||
        location.record_offset + location.total_disk_bytes > in_mem_buffer.size) {
      co_return Status(StatusCode::kInternal, "invalid in-memory location");
    }
    if (trace != nullptr) {
      trace->io_submit_ns = trace->buffer_acquired_ns;
      trace->io_complete_ns = trace->buffer_acquired_ns;
    }
    FixedBuffer io = lease.io_buffer();
    if (location.external) {
      co_return Status(StatusCode::kInternal,
                       "external value requires extent loading");
    }
    if (location.payload_bytes > io.size) {
      co_return Status(StatusCode::kOutOfRange,
                       "value exceeds registered read buffer capacity");
    }

    const std::byte* record_bytes =
        in_mem_buffer.data + location.record_offset;
    RecordHeader record{};
    std::string_view disk_key;
    if (!DecodeRecordHeader(
            std::span<const std::byte>(record_bytes,
                                       location.total_disk_bytes),
            &record, &disk_key) ||
        record.db_id != db_id || record.digest != digest || disk_key != key ||
        record.kind != RecordKind::kValue ||
        record.db_epoch != DbEpoch(db_id) ||
        record.mutation_sequence != location.mutation_sequence ||
        record.replication_epoch != location.replication_epoch ||
        record.relocation_sequence != location.relocation_sequence ||
        record.allocation_epoch != location.allocation_epoch ||
        record.expire_at_ms != location.expire_at_ms ||
        record.value_type != location.value_type ||
        record.external != location.external ||
        location.logical_size != record.logical_size ||
        location.payload_bytes != record.payload_bytes ||
        location.total_disk_bytes != record.total_disk_bytes) {
      co_return Status(StatusCode::kInternal,
                       "record does not match in-memory location");
    }
    std::memcpy(io.data, record_bytes + record.header_bytes,
                location.payload_bytes);
    if (options_.verify_read_crc &&
        Crc32c(std::span<const std::byte>(io.data, record.payload_bytes)) !=
            record.payload_checksum) {
      co_return Status(StatusCode::kInternal,
                       "record value checksum mismatch");
    }
    if (trace != nullptr) {
      trace->decode_done_ns = ReadTraceNowNanos();
    }
    const std::size_t value_offset = static_cast<std::size_t>(
        io.data - lease.bytes().data());
    co_return LoadedValue{std::move(lease), value_offset,
                          record.payload_bytes};
  }

  // Only the disk read suspends while holding the BlockState pointer, so it
  // is the only path that has to keep the state alive with a pin.
  ++state->pins;
  struct PinGuard {
    WorkerStore* store = nullptr;
    BlockState* state = nullptr;
    ~PinGuard() {
      if (state == nullptr) {
        return;
      }
      --state->pins;
      if (state->pins == 0 && state->release_pending) {
        ReleaseStagingBuffer(*store, *state);
      }
    }
  } pin{&store, state};

  if (trace != nullptr) {
    trace->disk_read = true;
  }
  FixedBuffer io = lease.io_buffer();
  if (read_bytes > io.size) {
    co_return Status(StatusCode::kOutOfRange,
                     "record exceeds registered read buffer capacity");
  }
  FixedBuffer record_buffer = io;
  record_buffer.size = read_bytes;
  if (trace != nullptr) {
    trace->io_submit_ns = ReadTraceNowNanos();
  }
  auto read = co_await ReadStorageBuffer(
      *store.worker, store.files[file_id], record_buffer,
      lease.registered(), aligned_offset);
  if (trace != nullptr) {
    trace->io_complete_ns = ReadTraceNowNanos();
  }
  if (!read.ok()) {
    co_return read.status();
  }
  if (*read != read_bytes) {
    co_return Status(StatusCode::kInternal, "short compact record read");
  }

  RecordHeader record{};
  std::string_view disk_key;
  const std::byte* record_data = io.data + record_headroom;
  std::span<const std::byte> record_bytes(record_data,
                                         location.total_disk_bytes);
  if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
      record.db_id != db_id || record.digest != digest || disk_key != key ||
      record.kind != RecordKind::kValue ||
      record.db_epoch != DbEpoch(db_id) ||
      record.mutation_sequence != location.mutation_sequence ||
      record.replication_epoch != location.replication_epoch ||
      record.relocation_sequence != location.relocation_sequence ||
      record.allocation_epoch != location.allocation_epoch ||
      record.expire_at_ms != location.expire_at_ms ||
      record.value_type != location.value_type ||
      record.external != location.external ||
      record.logical_size != location.logical_size ||
      record.payload_bytes != location.payload_bytes ||
      record.total_disk_bytes != location.total_disk_bytes) {
    co_return Status(StatusCode::kInternal,
                     "record does not match in-memory location");
  }
  const std::byte* value_data = record_data + record.header_bytes;
  if (options_.verify_read_crc &&
      Crc32c(std::span<const std::byte>(value_data, record.payload_bytes)) !=
          record.payload_checksum) {
    co_return Status(StatusCode::kInternal, "record value checksum mismatch");
  }
  const std::byte* framed_value = value_data;
  if (record.payload_bytes > DirectGetValueLimit()) {
    std::memmove(io.data, value_data, record.payload_bytes);
    framed_value = io.data;
  }
  if (trace != nullptr) {
    trace->decode_done_ns = ReadTraceNowNanos();
  }
  const std::size_t value_offset = static_cast<std::size_t>(
      framed_value - lease.bytes().data());
  co_return LoadedValue{std::move(lease), value_offset,
                        record.payload_bytes};
}

}  // namespace keylane::storage
