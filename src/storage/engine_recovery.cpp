#include "engine_impl.h"

namespace keylane::storage {

std::uint16_t StorageEngine::Impl::RecoveredBlockOwner(
    const BlockHeader& block, std::uint64_t block_id) const noexcept {
  // writer_id belongs to the topology that wrote the block and may be
  // greater than the current worker count after a scale-down.
  if (block.layout_worker_count == worker_count_ &&
      block.writer_id < worker_count_) {
    return static_cast<std::uint16_t>(block.writer_id);
  }
  std::uint64_t mixed = block_id ^
                        (block.allocation_epoch + 0x9e3779b97f4a7c15ULL);
  mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
  mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
  mixed ^= mixed >> 31;
  return static_cast<std::uint16_t>(mixed % worker_count_);
}

void StorageEngine::Impl::ReportRecoveryProgress(std::uint64_t records,
                                                 bool allocated) {
  const std::uint64_t scanned =
      recovery_scanned_blocks_.fetch_add(1, std::memory_order_relaxed) + 1;
  const std::uint64_t scanned_records =
      recovery_scanned_records_.fetch_add(records, std::memory_order_relaxed) +
      records;
  const std::uint64_t scanned_allocated =
      allocated
          ? recovery_scanned_allocated_.fetch_add(
                1, std::memory_order_relaxed) + 1
          : recovery_scanned_allocated_.load(std::memory_order_relaxed);
  const auto now = std::chrono::steady_clock::now();
  const std::int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          now.time_since_epoch())
          .count();
  const bool complete = scanned == total_data_blocks_;
  if (complete) {
    if (recovery_complete_logged_.exchange(true, std::memory_order_relaxed)) {
      return;
    }
  } else {
    std::int64_t next =
        recovery_next_log_ms_.load(std::memory_order_relaxed);
    if (now_ms < next ||
        !recovery_next_log_ms_.compare_exchange_strong(
            next, now_ms + 5000, std::memory_order_relaxed)) {
      return;
    }
  }

  // Free blocks are skipped without touching the disk, so the scan's workload
  // is the bitmap's allocated population, not device capacity. Rate, percent,
  // and ETA are all computed over allocated blocks.
  const std::uint64_t allocated_blocks = recovery_allocated_blocks_;
  const double elapsed_seconds =
      std::max(0.001, static_cast<double>(now_ms - recovery_started_ms_) /
                          1000.0);
  const double block_rate =
      static_cast<double>(scanned_allocated) / elapsed_seconds;
  const double record_rate =
      static_cast<double>(scanned_records) / elapsed_seconds;
  const double percent =
      allocated_blocks == 0
          ? 100.0
          : (static_cast<double>(scanned_allocated) * 100.0) /
                static_cast<double>(allocated_blocks);
  const std::uint64_t remaining =
      allocated_blocks > scanned_allocated
          ? allocated_blocks - scanned_allocated
          : 0;
  const double eta_seconds =
      block_rate == 0.0 ? 0.0 : static_cast<double>(remaining) / block_rate;
  spdlog::info(
      "storage recovery: blocks={}/{} ({:.1f}%) swept={}/{} records={} "
      "rate={:.0f} blocks/s {:.2f}M records/s eta={:.1f}s",
      scanned_allocated, allocated_blocks, percent, scanned,
      total_data_blocks_, scanned_records, block_rate,
      record_rate / 1000000.0, eta_seconds);
}

Task<Status> StorageEngine::Impl::ScanAssignedBlocks(
    WorkerStore& store, std::vector<RecoveryBatch>* batches,
    std::vector<std::uint64_t>* zero_blocks) {
  auto acquired = co_await store.buffers.AcquireReadBuffer();
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer header_buffer = lease.io_buffer();
  header_buffer.size = kBlockHeaderBytes;

  struct RecoveryBuffer {
    RegisteredBufferPool* pool = nullptr;
    std::uint16_t buffer_id = 0;
    std::byte* heap_data = nullptr;
    FixedBuffer buffer{};

    ~RecoveryBuffer() {
      if (buffer_id != 0) {
        pool->ReleaseWriteBuffer(buffer_id);
      } else if (heap_data != nullptr) {
        pool->ReleaseHeapWriteBuffer(heap_data);
      }
    }

    bool registered() const noexcept { return buffer_id != 0; }
  } recovery{.pool = &store.buffers};
  if (store.buffers.TryAcquireWriteBuffer(&recovery.buffer_id)) {
    recovery.buffer = store.buffers.write_buffer(recovery.buffer_id);
  } else if (store.buffers.TryAcquireHeapWriteBuffer(&recovery.heap_data)) {
    recovery.buffer = FixedBuffer{
        .data = recovery.heap_data,
        .size = options_.buffers.write_buffer_bytes,
        .index = 0,
    };
  } else {
    co_return Status(StatusCode::kResourceExhausted,
                     "failed to allocate recovery block buffer");
  }
  if (recovery.buffer.size < kStorageBlockBytes) {
    co_return Status(StatusCode::kResourceExhausted,
                     "recovery block buffer is smaller than a storage block");
  }
  recovery.buffer.size = kStorageBlockBytes;

  std::uint64_t device_linear_begin = 0;
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    const std::uint64_t first_device_offset =
        (store.worker->id() + worker_count_ -
         device_linear_begin % worker_count_) %
        worker_count_;
    for (std::uint64_t device_offset = first_device_offset;
         device_offset < device.data_block_count;
         device_offset += worker_count_) {
      const std::uint32_t local_block = static_cast<std::uint32_t>(
          device.data_block_begin + device_offset);
      const std::uint64_t block_id = MakeBlockId(device.id, local_block);
      const DeviceAllocator& allocator = *device_allocators_[device_index];
      const std::size_t bitmap_byte = local_block / 8;
      const unsigned bitmap_bit = local_block % 8;
      if ((std::to_integer<unsigned>(allocator.scan_bitmap[bitmap_byte]) &
           (1U << bitmap_bit)) == 0) {
        ReportRecoveryProgress(0, /*allocated=*/false);
        continue;
      }
      const std::uint32_t file_id = device.file_index;
      const std::uint64_t block_offset =
          static_cast<std::uint64_t>(local_block) * kStorageBlockBytes;
      auto read = co_await ReadStorageBuffer(
          *store.worker, store.files[file_id], header_buffer,
          lease.registered(), block_offset);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != kBlockHeaderBytes) {
        co_return Status(StatusCode::kInternal,
                         "short read while scanning block header");
      }
      std::span<const std::byte, kBlockHeaderBytes> block_bytes(
          header_buffer.data, kBlockHeaderBytes);
      if (IsZero(block_bytes)) {
        zero_blocks->push_back(block_id);
        ReportRecoveryProgress(0, /*allocated=*/true);
        continue;
      }

      BlockHeader block{};
      if (!DecodeBlockHeaderPages(block_bytes, &block)) {
        // Neither slot is valid. A block is allocated before it is ever
        // flushed, and its first flush zeroes the slot it does not write, so
        // this means no header ever committed here. Nothing in it was
        // durable, and the free slot cannot hold a header from an earlier
        // life, so the block is unused rather than corrupt.
        zero_blocks->push_back(block_id);
        ReportRecoveryProgress(0, /*allocated=*/true);
        continue;
      }
      if (block.block_id != block_id) {
        co_return Status(StatusCode::kInternal,
                         "invalid or corrupt block header");
      }
      AtomicMax(&recovery_device_cursors_[device_index].next_local,
                static_cast<std::uint64_t>(local_block) + 1);
      AtomicMax(
          &recovery_device_cursors_[device_index].next_allocation_epoch,
          block.allocation_epoch + 1);
      AtomicMax(&next_lsn_, block.max_lsn + 1);

      const std::uint16_t block_owner =
          RecoveredBlockOwner(block, block_id);
      batches->at(block_owner).blocks.push_back(RecoveryBlock{ActiveBlock{
          .block_id = block_id,
          .writer_id = block.writer_id,
          .layout_worker_count = block.layout_worker_count,
          .allocation_epoch = block.allocation_epoch,
          .committed_bytes = block.committed_bytes,
          .record_count = block.record_count,
          .max_lsn = block.max_lsn,
          .kind = block.kind,
          .extent_index = block.extent_index,
          .extent_payload_checksum = block.extent_payload_checksum,
      }});

      if (block.kind == BlockKind::kValueExtent) {
        ReportRecoveryProgress(0, /*allocated=*/true);
        continue;
      }

      read = co_await ReadStorageBuffer(
          *store.worker, store.files[file_id], recovery.buffer,
          recovery.registered(), block_offset);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != kStorageBlockBytes) {
        co_return Status(StatusCode::kInternal,
                         "short read while scanning committed block");
      }

      std::uint32_t record_offset = kBlockHeaderBytes;
      std::uint32_t records = 0;
      while (record_offset < block.committed_bytes) {
        const std::optional<std::uint32_t> next = NextRecordOffset(
            recovery.buffer.data, record_offset, block.committed_bytes);
        if (!next.has_value()) {
          co_return Status(StatusCode::kInternal,
                           "invalid or corrupt committed record header");
        }
        if (*next != record_offset) {
          record_offset = *next;
          continue;
        }
        RecordHeader record{};
        std::string_view key;
        std::span<const std::byte> record_bytes(
            recovery.buffer.data + record_offset,
            block.committed_bytes - record_offset);
        if (!DecodeRecordHeader(record_bytes, &record, &key) ||
            record.allocation_epoch != block.allocation_epoch ||
            record.digest != ComputeDigest(key) ||
            StorageShardForKey(key) % block.layout_worker_count !=
                block.writer_id ||
            record.relocation_sequence >
                std::numeric_limits<std::uint32_t>::max() ||
            record_offset + record.total_disk_bytes > block.committed_bytes) {
          co_return Status(StatusCode::kInternal,
                           "invalid or corrupt committed record header");
        }
        AtomicMax(&next_lsn_, record.lsn + 1);
        if (record.db_epoch != DbEpoch(record.db_id)) {
          record_offset += record.total_disk_bytes;
          ++records;
          continue;
        }
        const std::uint16_t partition_id = RedisSlot(key);
        if (record.replication_epoch !=
            epoch_values_[kLogicalDatabaseCount + partition_id]) {
          record_offset += record.total_disk_bytes;
          ++records;
          continue;
        }
        const unsigned key_owner = OwnerForKey(key);
        std::shared_ptr<const std::vector<ExtentRef>> extents;
        if (record.external) {
          if (record.kind != RecordKind::kValue ||
              record.value_type != ValueType::kString) {
            co_return Status(StatusCode::kInternal,
                             "unsupported external record type");
          }
          const std::byte* payload =
              recovery.buffer.data + record_offset + record.header_bytes;
          if (Crc32c(std::span<const std::byte>(payload,
                                                record.payload_bytes)) !=
              record.payload_checksum) {
            co_return Status(StatusCode::kInternal,
                             "external manifest checksum mismatch");
          }
          auto decoded = DecodeManifest(
              std::span<const std::byte>(payload, record.payload_bytes),
              record.logical_size);
          if (!decoded.ok()) {
            co_return decoded.status();
          }
          extents = std::move(*decoded);
        }
        batches->at(key_owner).records.push_back(RecoveryRecord{
            .digest = record.digest,
            .key = std::string(key),
            .db_id = record.db_id,
            .location = RecordLocation{
                .block_id = block_id,
                .replication_epoch = record.replication_epoch,
                .mutation_sequence = record.mutation_sequence,
                .allocation_epoch = record.allocation_epoch,
                .expire_at_ms = record.expire_at_ms,
                .block_owner = block_owner,
                .record_offset = record_offset,
                .total_disk_bytes = record.total_disk_bytes,
                .logical_size = record.logical_size,
                .payload_bytes = record.payload_bytes,
                .relocation_sequence = static_cast<std::uint32_t>(
                    record.relocation_sequence),
                .external = record.external,
                .kind = record.kind,
                .value_type = record.value_type,
                .extents = std::move(extents),
            },
        });
        record_offset += record.total_disk_bytes;
        ++records;
      }
      if (record_offset != block.committed_bytes ||
          records != block.record_count) {
        co_return Status(StatusCode::kInternal,
                         "block committed boundary does not match records");
      }
      ReportRecoveryProgress(records, /*allocated=*/true);
    }
    device_linear_begin += device.data_block_count;
  }
  co_return Status::Ok();
}

void StorageEngine::Impl::ApplyRecovery(unsigned target, RecoveryBatch batch) {
  WorkerStore& store = *stores_[target];
  for (const RecoveryBlock& recovered : batch.blocks) {
    const ActiveBlock& block = recovered.block;
    BlockState& state = CreateBlockState(store, block.block_id);
    state.writer_id = block.writer_id;
    state.layout_worker_count = block.layout_worker_count;
    state.allocation_epoch = block.allocation_epoch;
    state.committed_bytes = block.committed_bytes;
    state.allocated = true;
    state.kind = block.kind;
    if (block.kind == BlockKind::kValueExtent) {
      store.recovered_extents[block.block_id] = ExtentIdentity{
          .extent_index = block.extent_index,
          .payload_checksum = block.extent_payload_checksum,
      };
    }

    // Recovered blocks have no staging buffer. Keep partial blocks sealed;
    // appending to one would otherwise dereference an absent in-memory copy.
  }

  for (const RecoveryRecord& recovered : batch.records) {
    auto& partition = PartitionForKey(store, recovered.key);
    if (recovered.location.replication_epoch !=
        partition.replication_epoch) {
      continue;
    }
    partition.mutation_sequence = std::max(
        partition.mutation_sequence, recovered.location.mutation_sequence);
    auto& index = partition.indexes[recovered.db_id];
    auto* found = index.Find(recovered.digest, recovered.key);
    if (found == nullptr ||
        IsNewer(recovered.location, found->value)) {
      const bool was_live =
        found != nullptr && found->value.kind == RecordKind::kValue;
      const bool is_live = recovered.location.kind == RecordKind::kValue;
      const bool was_expiring =
          was_live && found->value.expire_at_ms != 0;
      const bool is_expiring =
          is_live && recovered.location.expire_at_ms != 0;
      index.InsertOrAssign(recovered.digest, recovered.key,
                           recovered.location);
      if (was_live != is_live) {
        if (is_live) {
          ++partition.live_key_count[recovered.db_id];
          ++store.live_key_count[recovered.db_id];
        } else {
          --partition.live_key_count[recovered.db_id];
          --store.live_key_count[recovered.db_id];
        }
      }
      if (was_expiring != is_expiring) {
        if (is_expiring) {
          ++partition.expiring_key_count[recovered.db_id];
        } else {
          --partition.expiring_key_count[recovered.db_id];
        }
      }
    }
  }
}

}  // namespace keylane::storage
