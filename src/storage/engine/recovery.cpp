#include "impl.h"

namespace keylane::storage {

Task<absl::StatusOr<std::string>>
StorageEngine::Impl::LoadExternalKeyForRecovery(WorkerStore& store,
                                                ExtentManifest extents,
                                                std::size_t key_bytes) {
  if (extents == nullptr || key_bytes == 0 || key_bytes > MaxKeyBytes()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "recovered external key manifest is invalid");
  }
  std::string key(key_bytes, '\0');
  std::size_t offset = 0;
  for (std::size_t index = 0; index < extents->size() && offset < key.size();
       ++index) {
    const ExtentRef& ref = extents->at(index);
    const std::size_t read_bytes =
        AlignDirect(kBlockHeaderBytes + ref.payload_bytes_);
    auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer io = lease.io_buffer();
    io.size_ = read_bytes;
    const auto [file_id, block_offset] = FileOffset(ref.block_id_);
    auto read =
        co_await ReadStorageBuffer(*store.worker_, store.files_[file_id], io,
                                   lease.registered(), block_offset);
    if (!read.ok()) {
      co_return read.status();
    }
    if (*read != read_bytes) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "short recovered key extent read");
    }
    BlockHeader header{};
    if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                    io.data_, kBlockHeaderBytes),
                                &header) ||
        header.kind_ != BlockKind::kPayloadExtent ||
        header.block_id_ != ref.block_id_ ||
        header.allocation_epoch_ != ref.allocation_epoch_ ||
        header.extent_index_ != index ||
        header.extent_payload_bytes_ != ref.payload_bytes_ ||
        header.extent_payload_checksum_ != ref.payload_checksum_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "recovered key extent does not match manifest");
    }
    const auto payload = std::span<const std::byte>(
        io.data_ + kBlockHeaderBytes, ref.payload_bytes_);
    if (Crc32c(payload) != ref.payload_checksum_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "recovered key extent checksum mismatch");
    }
    const std::size_t copy_bytes =
        std::min(payload.size(), key.size() - offset);
    std::memcpy(key.data() + offset, payload.data(), copy_bytes);
    offset += copy_bytes;
  }
  if (offset != key.size()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "recovered external key is truncated");
  }
  co_return key;
}

std::uint16_t StorageEngine::Impl::RecoveredBlockOwner(
    const BlockHeader& block, std::uint64_t block_id) const noexcept {
  // writer_id belongs to the topology that wrote the block and may be
  // greater than the current worker count after a scale-down.
  if (block.layout_worker_count_ == worker_count_ &&
      block.writer_id_ < worker_count_) {
    return static_cast<std::uint16_t>(block.writer_id_);
  }
  std::uint64_t mixed =
      block_id ^ (block.allocation_epoch_ + 0x9e3779b97f4a7c15ULL);
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
      allocated ? recovery_scanned_allocated_.fetch_add(
                      1, std::memory_order_relaxed) +
                      1
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
    std::int64_t next = recovery_next_log_ms_.load(std::memory_order_relaxed);
    if (now_ms < next || !recovery_next_log_ms_.compare_exchange_strong(
                             next, now_ms + 5000, std::memory_order_relaxed)) {
      return;
    }
  }

  // Free blocks are skipped without touching the disk, so the scan's workload
  // is the bitmap's allocated population, not device capacity. Rate, percent,
  // and ETA are all computed over allocated blocks.
  const std::uint64_t allocated_blocks = recovery_allocated_blocks_;
  const double elapsed_seconds = std::max(
      0.001, static_cast<double>(now_ms - recovery_started_ms_) / 1000.0);
  const double block_rate =
      static_cast<double>(scanned_allocated) / elapsed_seconds;
  const double record_rate =
      static_cast<double>(scanned_records) / elapsed_seconds;
  const double percent =
      allocated_blocks == 0 ? 100.0
                            : (static_cast<double>(scanned_allocated) * 100.0) /
                                  static_cast<double>(allocated_blocks);
  const std::uint64_t remaining = allocated_blocks > scanned_allocated
                                      ? allocated_blocks - scanned_allocated
                                      : 0;
  const double eta_seconds =
      block_rate == 0.0 ? 0.0 : static_cast<double>(remaining) / block_rate;
  spdlog::info(
      "storage recovery: blocks={}/{} ({:.1f}%) swept={}/{} records={} "
      "rate={:.0f} blocks/s {:.2f}M records/s eta={:.1f}s",
      scanned_allocated, allocated_blocks, percent, scanned, total_data_blocks_,
      scanned_records, block_rate, record_rate / 1000000.0, eta_seconds);
}

Task<absl::Status> StorageEngine::Impl::ScanAssignedBlocks(
    WorkerStore& store, std::vector<RecoveryBatch>* batches,
    std::vector<std::uint64_t>* zero_blocks,
    absl::flat_hash_set<std::uint64_t>* committed_txids) {
  auto acquired = co_await store.buffers_.AcquireReadBuffer();
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer header_buffer = lease.io_buffer();
  header_buffer.size_ = kBlockHeaderBytes;

  struct RecoveryBuffer {
    RegisteredBufferPool* pool_ = nullptr;
    std::uint16_t buffer_id_ = 0;
    std::byte* heap_data_ = nullptr;
    FixedBuffer buffer_{};

    ~RecoveryBuffer() {
      if (buffer_id_ != 0) {
        pool_->ReleaseWriteBuffer(buffer_id_);
      } else if (heap_data_ != nullptr) {
        pool_->ReleaseHeapWriteBuffer(heap_data_);
      }
    }

    bool registered() const noexcept { return buffer_id_ != 0; }
  } recovery{.pool_ = &store.buffers_};
  if (store.buffers_.TryAcquireWriteBuffer(&recovery.buffer_id_)) {
    recovery.buffer_ = store.buffers_.write_buffer(recovery.buffer_id_);
  } else if (store.buffers_.TryAcquireHeapWriteBuffer(&recovery.heap_data_)) {
    recovery.buffer_ = FixedBuffer{
        .data_ = recovery.heap_data_,
        .size_ = options_.buffers_.write_buffer_bytes_,
        .index_ = 0,
    };
  } else {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "failed to allocate recovery block buffer");
  }
  if (recovery.buffer_.size_ < kStorageBlockBytes) {
    co_return absl::Status(
        absl::StatusCode::kResourceExhausted,
        "recovery block buffer is smaller than a storage block");
  }
  recovery.buffer_.size_ = kStorageBlockBytes;

  std::uint64_t device_linear_begin = 0;
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    const std::uint64_t first_device_offset =
        (store.worker_->id() + worker_count_ -
         device_linear_begin % worker_count_) %
        worker_count_;
    for (std::uint64_t device_offset = first_device_offset;
         device_offset < device.data_block_count_;
         device_offset += worker_count_) {
      const std::uint32_t local_block =
          static_cast<std::uint32_t>(device.data_block_begin_ + device_offset);
      const std::uint64_t block_id = MakeBlockId(device.id_, local_block);
      const DeviceAllocator& allocator = *device_allocators_[device_index];
      const std::size_t bitmap_byte = local_block / 8;
      const unsigned bitmap_bit = local_block % 8;
      if ((std::to_integer<unsigned>(allocator.scan_bitmap_[bitmap_byte]) &
           (1U << bitmap_bit)) == 0) {
        ReportRecoveryProgress(0, /*allocated=*/false);
        continue;
      }
      const std::uint32_t file_id = device.file_index_;
      const std::uint64_t block_offset =
          static_cast<std::uint64_t>(local_block) * kStorageBlockBytes;
      auto read = co_await ReadStorageBuffer(
          *store.worker_, store.files_[file_id], header_buffer,
          lease.registered(), block_offset);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != kBlockHeaderBytes) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "short read while scanning block header");
      }
      std::span<const std::byte, kBlockHeaderBytes> block_bytes(
          header_buffer.data_, kBlockHeaderBytes);
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
      if (block.block_id_ != block_id) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "invalid or corrupt block header");
      }
      AtomicMax(&recovery_device_cursors_[device_index].next_local_,
                static_cast<std::uint64_t>(local_block) + 1);
      AtomicMax(&recovery_device_cursors_[device_index].next_allocation_epoch_,
                block.allocation_epoch_ + 1);
      AtomicMax(&next_lsn_, block.max_lsn_ + 1);

      const std::uint16_t block_owner = RecoveredBlockOwner(block, block_id);
      batches->at(block_owner)
          .blocks_.push_back(RecoveryBlock{ActiveBlock{
              .block_id_ = block_id,
              .writer_id_ = block.writer_id_,
              .layout_worker_count_ = block.layout_worker_count_,
              .allocation_epoch_ = block.allocation_epoch_,
              .committed_bytes_ = block.committed_bytes_,
              .record_count_ = block.record_count_,
              .max_lsn_ = block.max_lsn_,
              .kind_ = block.kind_,
              .extent_index_ = block.extent_index_,
              .extent_payload_checksum_ = block.extent_payload_checksum_,
          }});

      if (block.kind_ == BlockKind::kPayloadExtent) {
        ReportRecoveryProgress(0, /*allocated=*/true);
        continue;
      }

      read = co_await ReadStorageBuffer(*store.worker_, store.files_[file_id],
                                        recovery.buffer_, recovery.registered(),
                                        block_offset);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != kStorageBlockBytes) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "short read while scanning committed block");
      }

      std::uint32_t record_offset = kBlockHeaderBytes;
      std::uint32_t records = 0;
      while (record_offset < block.committed_bytes_) {
        const std::optional<std::uint32_t> next = NextRecordOffset(
            recovery.buffer_.data_, record_offset, block.committed_bytes_);
        if (!next.has_value()) {
          co_return absl::Status(absl::StatusCode::kInternal,
                                 "invalid or corrupt committed record header");
        }
        if (*next != record_offset) {
          record_offset = *next;
          continue;
        }
        RecordHeader record{};
        std::string_view key;
        std::span<const std::byte> record_bytes(
            recovery.buffer_.data_ + record_offset,
            block.committed_bytes_ - record_offset);
        if (!DecodeRecordHeader(record_bytes, &record, &key) ||
            record.allocation_epoch_ != block.allocation_epoch_ ||
            record.relocation_sequence_ >
                std::numeric_limits<std::uint32_t>::max() ||
            record_offset + record.total_disk_bytes_ > block.committed_bytes_) {
          co_return absl::Status(absl::StatusCode::kInternal,
                                 "invalid or corrupt committed record header");
        }
        AtomicMax(&next_lsn_, record.lsn_ + 1);
        AtomicMax(&recovery_max_txid_, record.txid_);
        if (record.kind_ == RecordKind::kTxCommit) {
          // A commit decision, not a keyed record: exempt from the key and
          // epoch filters below — the transaction it commits may span
          // databases and partitions whose epochs are unrelated to this
          // record's own header fields.
          committed_txids->insert(record.txid_);
          record_offset += record.total_disk_bytes_;
          ++records;
          continue;
        }
        if (record.db_epoch_ != DbEpoch(record.db_id_)) {
          record_offset += record.total_disk_bytes_;
          ++records;
          continue;
        }
        const std::byte* payload =
            recovery.buffer_.data_ + record_offset + record.header_bytes_;
        const auto payload_span =
            std::span<const std::byte>(payload, record.payload_bytes_);
        if (Crc32c(payload_span) != record.payload_checksum_) {
          co_return absl::Status(absl::StatusCode::kInternal,
                                 "record payload checksum mismatch");
        }
        ExtentManifest extents;
        if (record.external_) {
          const std::uint64_t extent_bytes =
              record.logical_size_ +
              (record.key_external_ ? record.key_bytes_ : 0);
          auto decoded =
              DecodeManifest(payload_span, extent_bytes,
                             record.kind_ != RecordKind::kValue ||
                                 record.value_type_ == ValueType::kString);
          if (!decoded.ok()) {
            co_return decoded.status();
          }
          extents = std::move(*decoded);
        }
        std::string loaded_key;
        if (record.key_external_) [[unlikely]] {
          if (record.external_) {
            auto external_key = co_await LoadExternalKeyForRecovery(
                store, extents, record.key_bytes_);
            if (!external_key.ok()) {
              co_return external_key.status();
            }
            loaded_key = std::move(*external_key);
            key = loaded_key;
          } else {
            if (record.payload_bytes_ < record.key_bytes_) {
              co_return absl::Status(absl::StatusCode::kInternal,
                                     "inline external key is truncated");
            }
            key = std::string_view(reinterpret_cast<const char*>(payload),
                                   record.key_bytes_);
          }
        }
        if (record.digest_ != ComputeDigest(key) ||
            StorageShardForKey(key) % block.layout_worker_count_ !=
                block.writer_id_) {
          co_return absl::Status(absl::StatusCode::kInternal,
                                 "invalid or corrupt committed record header");
        }
        const std::uint16_t partition_id = RedisSlot(key);
        if (record.replication_epoch_ !=
            epoch_values_[kLogicalDatabaseCount + partition_id]) {
          record_offset += record.total_disk_bytes_;
          ++records;
          continue;
        }
        const unsigned key_owner = OwnerForKey(key);
        batches->at(key_owner).records_.push_back(RecoveryRecord{
            .digest_ = record.digest_,
            .key_ = std::string(key),
            .db_id_ = record.db_id_,
            .txid_ = record.txid_,
            .location_ =
                RecordLocation{
                    .block_id_ = block_id,
                    .replication_epoch_ = record.replication_epoch_,
                    .mutation_sequence_ = record.mutation_sequence_,
                    .allocation_epoch_ = record.allocation_epoch_,
                    .expire_at_ms_ = record.expire_at_ms_,
                    .logical_size_ =
                        static_cast<std::uint32_t>(record.logical_size_),
                    .record_offset_ = record_offset,
                    .total_disk_bytes_ = record.total_disk_bytes_,
                    .payload_bytes_ = record.payload_bytes_,
                    .relocation_sequence_ =
                        static_cast<std::uint32_t>(record.relocation_sequence_),
                    .block_owner_ = block_owner,
                    .external_ = record.external_,
                    .key_external_ = record.key_external_,
                    .kind_ = record.kind_,
                    .value_type_ = record.value_type_,
                },
            .extents_ = extents,
        });
        record_offset += record.total_disk_bytes_;
        ++records;
      }
      if (record_offset != block.committed_bytes_ ||
          records != block.record_count_) {
        co_return absl::Status(
            absl::StatusCode::kInternal,
            "block committed boundary does not match records");
      }
      ReportRecoveryProgress(records, /*allocated=*/true);
    }
    device_linear_begin += device.data_block_count_;
  }
  co_return absl::OkStatus();
}

void StorageEngine::Impl::ApplyRecovery(unsigned target, RecoveryBatch batch) {
  WorkerStore& store = *stores_[target];
  for (const RecoveryBlock& recovered : batch.blocks_) {
    const ActiveBlock& block = recovered.block_;
    BlockState& state = CreateBlockState(store, block.block_id_);
    state.writer_id_ = block.writer_id_;
    state.layout_worker_count_ = block.layout_worker_count_;
    state.allocation_epoch_ = block.allocation_epoch_;
    state.committed_bytes_ = block.committed_bytes_;
    state.allocated_ = true;
    state.kind_ = block.kind_;
    if (block.kind_ == BlockKind::kPayloadExtent) {
      store.recovered_extents_[block.block_id_] = ExtentIdentity{
          .extent_index_ = block.extent_index_,
          .payload_checksum_ = block.extent_payload_checksum_,
      };
    }

    // Recovered blocks have no staging buffer. Keep partial blocks sealed;
    // appending to one would otherwise dereference an absent in-memory copy.
  }

  for (const RecoveryRecord& recovered : batch.records_) {
    if (recovered.txid_ != 0) {
      // Whether this record's transaction committed is only decidable once
      // every worker's scan has fed the committed set; park it until after
      // the recovery barrier.
      store.recovery_tx_records_.push_back(recovered);
      continue;
    }
    ApplyRecoveredRecord(store, recovered);
  }
}

void StorageEngine::Impl::ApplyRecoveredRecord(
    WorkerStore& store, const RecoveryRecord& recovered) {
  {
    auto& partition = PartitionForKey(store, recovered.key_);
    if (recovered.location_.replication_epoch_ !=
        partition.replication_epoch_) {
      return;
    }
    partition.mutation_sequence_ = std::max(
        partition.mutation_sequence_, recovered.location_.mutation_sequence_);
    auto& index = partition.indexes_[recovered.db_id_];
    RecordIndex::Entry* found = nullptr;
    for (RecordIndex::Entry* candidate :
         index.FindCandidates(recovered.digest_, recovered.key_)) {
      if (candidate->key_complete()) {
        found = candidate;
        break;
      }
      const auto external_key = store.recovery_external_keys_.find(candidate);
      if (external_key != store.recovery_external_keys_.end() &&
          external_key->second == recovered.key_) {
        found = candidate;
        break;
      }
    }
    // The shielding bit is not persisted; recovery rebuilds it exactly,
    // since every surviving record of the key passes through this merge:
    // whichever version currently wins learns whether a strictly older,
    // still-unexpired value remains on disk. Equal sequences are relocated
    // copies of the same version and shield nothing.
    if (found == nullptr || IsNewer(recovered.location_, found->value_)) {
      const bool was_live =
          found != nullptr && found->value_.kind_ == RecordKind::kValue;
      const bool is_live = recovered.location_.kind_ == RecordKind::kValue;
      const bool was_expiring = was_live && found->value_.expire_at_ms_ != 0;
      const bool is_expiring =
          is_live && recovered.location_.expire_at_ms_ != 0;
      RecordLocation winner = recovered.location_;
      if (found != nullptr) {
        winner.shielding_ = found->value_.shielding_ ||
                            (found->value_.kind_ == RecordKind::kValue &&
                             found->value_.mutation_sequence_ <
                                 recovered.location_.mutation_sequence_ &&
                             (found->value_.expire_at_ms_ == 0 ||
                              found->value_.expire_at_ms_ >
                                  std::max(recovered.location_.expire_at_ms_,
                                           UnixTimeMillis())));
      }
      RecordIndex::Entry* winner_entry = found;
      if (winner_entry != nullptr) {
        winner_entry->value_ = winner;
      } else {
        winner_entry = index.InsertNew(recovered.digest_, recovered.key_,
                                       winner, !winner.key_external_);
      }
      if (winner.external_) {
        store.external_manifests_.insert_or_assign(winner_entry,
                                                   recovered.extents_);
      } else {
        store.external_manifests_.erase(winner_entry);
      }
      if (winner.key_external_) [[unlikely]] {
        store.recovery_external_keys_.insert_or_assign(winner_entry,
                                                       recovered.key_);
      } else {
        store.recovery_external_keys_.erase(winner_entry);
      }
      if (was_live != is_live) {
        if (is_live) {
          ++partition.live_key_count_[recovered.db_id_];
          ++store.live_key_count_[recovered.db_id_];
        } else {
          --partition.live_key_count_[recovered.db_id_];
          --store.live_key_count_[recovered.db_id_];
        }
      }
      if (was_expiring != is_expiring) {
        if (is_expiring) {
          ++partition.expiring_key_count_[recovered.db_id_];
        } else {
          --partition.expiring_key_count_[recovered.db_id_];
        }
      }
    } else if (recovered.location_.kind_ == RecordKind::kValue &&
               recovered.location_.mutation_sequence_ <
                   found->value_.mutation_sequence_ &&
               (recovered.location_.expire_at_ms_ == 0 ||
                recovered.location_.expire_at_ms_ >
                    std::max(found->value_.expire_at_ms_, UnixTimeMillis()))) {
      found->value_.shielding_ = true;
    }
  }
}

}  // namespace keylane::storage
