/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <tuple>

#include "impl.h"

namespace lavik::storage {

Task<absl::StatusOr<std::string>>
StorageEngine::Impl::LoadExternalKeyForRecovery(WorkerStore& store,
                                                ExtentManifest extents,
                                                std::size_t key_bytes) {
  if (extents == nullptr || key_bytes == 0 || key_bytes > MaxKeyBytes()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "recovered external key manifest is invalid");
  }
  std::string key;
  key.resize(key_bytes);
  std::size_t offset = 0;
  for (std::size_t index = 0; index < extents->size() && offset < key.size();
       ++index) {
    const ExtentRef ref = extents->at(index);
    auto destination = std::span<std::byte>(
        reinterpret_cast<std::byte*>(key.data() + offset), key.size() - offset);
    absl::Status read;
    if (bycorf::SpdkStorageEnabled()) {
      const auto& owners = device_owners_[DeviceIndexForBlock(ref.block_id_)];
      const unsigned owner = owners[ref.block_id_ % owners.size()];
      if (owner == store.worker_->id()) {
        read = co_await ReadRecoveryExtentInto(
            store, ref, static_cast<std::uint32_t>(index), destination);
      } else {
        read = co_await bycorf::SubmitTaskTo(
            owner,
            [this, owner, ref, index, destination]() -> Task<absl::Status> {
              co_return co_await ReadRecoveryExtentInto(
                  *stores_[owner], ref, static_cast<std::uint32_t>(index),
                  destination);
            });
      }
    } else {
      read = co_await ReadRecoveryExtentInto(
          store, ref, static_cast<std::uint32_t>(index), destination);
    }
    if (!read.ok()) co_return read;
    offset += std::min<std::size_t>(ref.payload_bytes_, destination.size());
  }
  if (offset != key.size()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "recovered external key is truncated");
  }
  co_return key;
}

Task<absl::Status> StorageEngine::Impl::ReadRecoveryExtentInto(
    WorkerStore& store, ExtentRef ref, std::uint32_t extent_index,
    std::span<std::byte> destination, std::size_t payload_offset,
    OrderedGroupMetadataDecoder* ordered) {
  if (payload_offset > ref.payload_bytes_) {
    co_return absl::DataLossError("recovered extent slice is out of bounds");
  }
  const std::size_t read_bytes =
      AlignDirect(kBlockHeaderBytes + ref.payload_bytes_);
  auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer io = lease.io_buffer();
  io.size_ = read_bytes;
  const auto [file_id, block_offset] = FileOffset(ref.block_id_);
  auto read = co_await ReadStorageBuffer(*store.worker_, store.files_[file_id],
                                         io, lease.registered(), block_offset);
  if (!read.ok()) co_return read.status();
  if (*read != read_bytes) {
    co_return absl::InternalError("short recovered key extent read");
  }
  BlockHeader header{};
  if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                  io.data_, kBlockHeaderBytes),
                              &header) ||
      header.kind_ != BlockKind::kPayloadExtent ||
      header.block_id_ != ref.block_id_ ||
      header.allocation_epoch_ != ref.allocation_epoch_ ||
      header.extent_index_ != extent_index ||
      header.extent_payload_bytes_ != ref.payload_bytes_ ||
      header.extent_payload_checksum_ != ref.payload_checksum_) {
    co_return absl::InternalError(
        "recovered key extent does not match manifest");
  }
  const auto payload = std::span<const std::byte>(io.data_ + kBlockHeaderBytes,
                                                  ref.payload_bytes_);
  if (Crc32c(payload) != ref.payload_checksum_) {
    co_return absl::InternalError("recovered key extent checksum mismatch");
  }
  if (ordered != nullptr) {
    // The caller awaits each extent before proceeding. Even an SPDK owner hop
    // has exclusive access to this bounded, non-affine decoder until return;
    // no borrowed I/O span or worker-owned metadata survives this call.
    auto status = ordered->Read(std::string_view(
        reinterpret_cast<const char*>(payload.data() + payload_offset),
        payload.size() - payload_offset));
    if (!status.ok()) co_return status;
  }
  if (!destination.empty()) {
    std::memcpy(destination.data(), payload.data() + payload_offset,
                std::min(payload.size() - payload_offset, destination.size()));
  }
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::string>> StorageEngine::Impl::LoadRecoveryPayloadSlice(
    WorkerStore& store, ExtentManifest extents, std::size_t offset,
    std::size_t bytes, OrderedGroupMetadataDecoder* ordered) {
  if (extents == nullptr || bytes > kMaxRecordPayloadBytes) {
    co_return absl::DataLossError("recovered payload slice has no manifest");
  }
  std::string result(bytes, '\0');
  std::size_t copied = 0;
  for (std::size_t index = 0; index < extents->size(); ++index) {
    const ExtentRef ref = extents->at(index);
    std::size_t slice_offset = offset;
    std::size_t count = 0;
    if (offset >= ref.payload_bytes_) {
      offset -= ref.payload_bytes_;
      slice_offset = ref.payload_bytes_;
    } else {
      count =
          std::min<std::size_t>(bytes - copied, ref.payload_bytes_ - offset);
      offset = 0;
    }
    // The envelope is small, but every extent still crosses checksum and
    // identity validation before this graph can become recovery authority.
    // Empty destinations validate the remaining payload without retaining it.
    // Ordered recovery also observes its entry headers in this same pass to
    // derive score bounds without a format change or value-sized allocation.
    const auto destination = std::span<std::byte>(
        reinterpret_cast<std::byte*>(result.data() + copied), count);
    absl::Status read;
    if (bycorf::SpdkStorageEnabled()) {
      // Scan-time extent owners may not be published yet. Use an eligible
      // device reader, exactly as external-key recovery does.
      const auto& owners = device_owners_[DeviceIndexForBlock(ref.block_id_)];
      const unsigned owner = owners[ref.block_id_ % owners.size()];
      if (owner != store.worker_->id()) {
        read = co_await bycorf::SubmitTaskTo(
            owner,
            [this, owner, ref, index, destination, slice_offset,
             ordered]() -> Task<absl::Status> {
              co_return co_await ReadRecoveryExtentInto(
                  *stores_[owner], ref, static_cast<std::uint32_t>(index),
                  destination, slice_offset, ordered);
            });
      } else {
        read = co_await ReadRecoveryExtentInto(
            store, ref, static_cast<std::uint32_t>(index), destination,
            slice_offset, ordered);
      }
    } else {
      read = co_await ReadRecoveryExtentInto(
          store, ref, static_cast<std::uint32_t>(index), destination,
          slice_offset, ordered);
    }
    if (!read.ok()) co_return read;
    copied += count;
  }
  if (copied != bytes) {
    co_return absl::DataLossError("recovered payload slice is truncated");
  }
  co_return result;
}

std::uint16_t StorageEngine::Impl::RecoveredBlockOwner(
    const BlockHeader& block, std::uint64_t block_id) const noexcept {
  std::uint64_t mixed =
      block_id ^ (block.allocation_epoch_ + 0x9e3779b97f4a7c15ULL);
  mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
  mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
  mixed ^= mixed >> 31;
  if (bycorf::SpdkStorageEnabled()) {
    const auto& owners = device_owners_[DeviceIndexForBlock(block_id)];
    if (block.layout_worker_count_ == worker_count_ &&
        std::binary_search(owners.begin(), owners.end(), block.writer_id_))
      return static_cast<std::uint16_t>(block.writer_id_);
    return owners[mixed % owners.size()];
  }
  // A durable writer ID may exceed the current topology after a scale-down.
  if (block.layout_worker_count_ == worker_count_ &&
      block.writer_id_ < worker_count_)
    return static_cast<std::uint16_t>(block.writer_id_);
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

Task<absl::Status> StorageEngine::Impl::ApplyRecoveryBatches(
    WorkerStore& store, std::vector<RecoveryBatch>* batches) {
  for (unsigned target = 0; target < worker_count_; ++target) {
    RecoveryBatch& pending = batches->at(target);
    if (pending.blocks_.empty() && pending.records_.empty() &&
        pending.commit_records_.empty()) {
      continue;
    }
    RecoveryBatch batch;
    std::swap(batch, pending);
    if (target == store.worker_->id()) {
      absl::Status applied = ApplyRecovery(target, std::move(batch));
      if (!applied.ok()) co_return applied;
      continue;
    }
    absl::Status applied = co_await bycorf::SubmitTo(
        target, [this, target, batch = std::move(batch)]() mutable {
          return ApplyRecovery(target, std::move(batch));
        });
    if (!applied.ok()) {
      co_return applied;
    }
  }
  co_return absl::OkStatus();
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

    bool registered() const noexcept {
      return buffer_id_ != 0 && pool_->buffers_registered();
    }
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
  // Merge recovered entries incrementally. The byte target is divided across
  // scan workers, so adding recovery concurrency cannot multiply temporary
  // routing memory without bound. One maximum-size key may overshoot its
  // worker target, but it is flushed before another record is retained.
  const std::size_t batch_target_bytes =
      RecoveryWorkerBatchTargetBytes(worker_count_);
  std::size_t buffered_bytes = 0;

  // Preserve the global striped ownership of physical blocks, but visit one
  // block from each device in turn. Scanning every device to completion in
  // file order makes all workers saturate device 0 while every other NVMe is
  // idle, then move to device 1 together.
  std::vector<std::uint64_t> next_device_offsets(devices_.size());
  std::uint64_t device_linear_begin = 0;
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    if (bycorf::SpdkStorageEnabled()) {
      const auto& owners = device_owners_[device_index];
      const auto owner =
          std::lower_bound(owners.begin(), owners.end(), store.worker_->id());
      next_device_offsets[device_index] =
          owner == owners.end() || *owner != store.worker_->id()
              ? device.data_block_count_
              : static_cast<std::uint64_t>(owner - owners.begin());
    } else {
      next_device_offsets[device_index] =
          (store.worker_->id() + worker_count_ -
           device_linear_begin % worker_count_) %
          worker_count_;
    }
    device_linear_begin += device.data_block_count_;
  }
  while (true) {
    bool scanned_block = false;
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const StorageDevice& device = devices_[device_index];
      std::uint64_t& next_device_offset = next_device_offsets[device_index];
      if (next_device_offset >= device.data_block_count_) {
        continue;
      }
      const std::uint64_t device_offset = next_device_offset;
      if (bycorf::SpdkStorageEnabled()) {
        next_device_offset += device_owners_[device_index].size();
      } else {
        next_device_offset += worker_count_;
      }
      scanned_block = true;
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
        // The bitmap records activation, not a committed write. A valid
        // header naming another physical block is stale media contents. Do
        // not recover it and do not rewrite the allocation bitmap.
        ReportRecoveryProgress(0, /*allocated=*/true);
        continue;
      }
      if (!RecordLocation::CanEncodeBlockIdentity(block_id,
                                                  block.allocation_epoch_)) {
        co_return absl::Status(
            absl::StatusCode::kOutOfRange,
            "durable block identity exceeds the runtime index range");
      }
      AtomicMax(&recovery_device_cursors_[device_index].next_local_,
                static_cast<std::uint64_t>(local_block) + 1);
      AtomicMax(&recovery_device_cursors_[device_index].next_allocation_epoch_,
                block.allocation_epoch_ + 1);
      AtomicMax(&recovery_max_lsn_, block.max_lsn_);

      if (block.kind_ == BlockKind::kCheckpointIndex) {
        // Checkpoint blocks are acceleration state, not record ownership.
        // The selected generation was already loaded through its bitmap;
        // every selected or stale checkpoint block becomes ordinary free space
        // once this startup completes.
        zero_blocks->push_back(block_id);
        ReportRecoveryProgress(0, /*allocated=*/true);
        continue;
      }

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
              .tx_generation_ = block.tx_generation_,
              .extent_index_ = block.extent_index_,
              .extent_payload_checksum_ = block.extent_payload_checksum_,
          }});
      buffered_bytes += sizeof(RecoveryBlock);

      if (block.kind_ == BlockKind::kPayloadExtent) {
        ReportRecoveryProgress(0, /*allocated=*/true);
        if (buffered_bytes >= batch_target_bytes) {
          absl::Status applied = co_await ApplyRecoveryBatches(store, batches);
          if (!applied.ok()) {
            co_return applied;
          }
          buffered_bytes = 0;
        }
        continue;
      }

      if (block.kind_ == BlockKind::kRecords &&
          checkpoint_active_.load(std::memory_order_acquire)) {
        ReportRecoveryProgress(0, /*allocated=*/true);
        if (buffered_bytes >= batch_target_bytes) {
          absl::Status applied = co_await ApplyRecoveryBatches(store, batches);
          if (!applied.ok()) co_return applied;
          buffered_bytes = 0;
        }
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
            record_offset + record.total_disk_bytes_ > block.committed_bytes_) {
          co_return absl::Status(absl::StatusCode::kInternal,
                                 "invalid or corrupt committed record header");
        }
        AtomicMax(&recovery_max_lsn_, record.lsn_);
        AtomicMax(&recovery_max_txid_, record.txid_);
        // A batch prepare can survive without its decision. Never reuse that
        // id on the next boot and accidentally commit old prepared groups.
        AtomicMax(&recovery_max_txid_, record.group_batch_txid_);
        if (record.auxiliary_group_) {
          // Cleaner promotion clears transaction tags, not the independent
          // object revision. Its globally allocated id must remain reserved
          // even when this is the only surviving physical record carrying it.
          AtomicMax(&recovery_max_txid_, record.mutation_sequence_);
          AtomicMax(&recovery_max_txid_, record.group_incarnation_);
        }
        if ((block.kind_ == BlockKind::kTransaction) != (record.txid_ != 0)) {
          co_return absl::Status(
              absl::StatusCode::kInternal,
              "record txid does not match its physical block kind");
        }
        if (record.kind_ == RecordKind::kTxCommit &&
            block.kind_ != BlockKind::kTransaction) {
          co_return absl::Status(
              absl::StatusCode::kInternal,
              "TxCommit appears outside a transaction block");
        }
        if (record.kind_ == RecordKind::kTxCommit) {
          // A commit decision, not a keyed record: exempt from the key and
          // epoch filters below — the transaction it commits may span
          // databases and partitions whose epochs are unrelated to this
          // record's own header fields.
          committed_txids->insert(record.txid_);
          batches->at(block_owner)
              .commit_records_.push_back(RecoveryBatch::CommitRecord{
                  .block_id_ = block_id,
                  .txid_ = record.txid_,
                  .bytes_ = record.total_disk_bytes_,
              });
          buffered_bytes += sizeof(RecoveryBatch::CommitRecord);
          record_offset += record.total_disk_bytes_;
          ++records;
          if (buffered_bytes >= batch_target_bytes) {
            absl::Status applied =
                co_await ApplyRecoveryBatches(store, batches);
            if (!applied.ok()) {
              co_return applied;
            }
            buffered_bytes = 0;
          }
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
        const std::uint16_t partition_id = RedisSlot(key);
        if (partition_id % block.layout_worker_count_ != block.writer_id_) {
          co_return absl::Status(absl::StatusCode::kInternal,
                                 "invalid or corrupt committed record header");
        }
        const Digest digest = ComputeDigest(key);
        if (record.replication_epoch_ !=
            epoch_values_[kLogicalDatabaseCount + partition_id]) {
          record_offset += record.total_disk_bytes_;
          ++records;
          continue;
        }
        if (record.db_id_ >= options_.database_count_) {
          co_return absl::FailedPreconditionError(
              "storage contains current records in a disabled database");
        }
        const bool ordered = record.value_type_ == ValueType::kList ||
                             record.value_type_ == ValueType::kSortedSet;
        const auto ordered_kind = record.value_type_ == ValueType::kList
                                      ? OrderedCollectionKind::kList
                                      : OrderedCollectionKind::kSortedSet;
        std::optional<RecoveredGroupedRoot> grouped_root;
        std::optional<RecoveredHashGroup> auxiliary_group;
        std::optional<RecoveredOrderedGroup> ordered_group;
        if (record.auxiliary_group_) {
          // Value-only extents of an obsolete group may already have been
          // reclaimed while other live records retain this source block.
          // Its checked header contains all winner-selection metadata; touch
          // external values only after the complete root graph is selected.
          auxiliary_group = RecoveredHashGroup{
              .incarnation_ = record.group_incarnation_,
              .id_ = {.prefix_ = record.group_prefix_,
                      .bits_ = record.group_prefix_bits_},
              .sequence_ = record.mutation_sequence_,
              .lsn_ = record.lsn_,
              .txid_ = record.txid_,
              .batch_txid_ = record.group_batch_txid_,
              .field_count_ = record.logical_size_,
              .retired_ = record.group_retired_,
          };
          if (!record.external_) {
            const std::size_t key_prefix =
                record.key_external_ ? record.key_bytes_ : 0;
            if (record.payload_bytes_ < key_prefix) {
              co_return absl::DataLossError("recovered group key is truncated");
            }
            const std::string_view encoded(
                reinterpret_cast<const char*>(payload) + key_prefix,
                record.payload_bytes_ - key_prefix);
            if (ordered && IsOrderedPageId(auxiliary_group->id_)) {
              auto decoded = DecodeOrderedGroup(encoded);
              if (!decoded.ok()) co_return decoded.status();
              if (decoded->kind_ != ordered_kind ||
                  decoded->incarnation_ != auxiliary_group->incarnation_ ||
                  decoded->id_ != auxiliary_group->id_.prefix_ ||
                  decoded->retired_ != auxiliary_group->retired_ ||
                  decoded->entries_.size() != auxiliary_group->field_count_) {
                co_return absl::DataLossError(
                    "ordered page disagrees with its record identity");
              }
              ordered_group = RecoveredOrderedGroup{
                  .incarnation_ = decoded->incarnation_,
                  .id_ = decoded->id_,
                  .previous_ = decoded->previous_,
                  .next_ = decoded->next_,
                  .sequence_ = record.mutation_sequence_,
                  .lsn_ = record.lsn_,
                  .txid_ = record.txid_,
                  .batch_txid_ = record.group_batch_txid_,
                  .item_count_ = record.logical_size_,
                  .retired_ = record.group_retired_,
                  .min_score_ = decoded->entries_.empty()
                                    ? 0
                                    : decoded->entries_.front().score_,
                  .max_score_ = decoded->entries_.empty()
                                    ? 0
                                    : decoded->entries_.back().score_,
              };
            } else {
              auto decoded = DecodeHashGroup(encoded);
              if (!decoded.ok()) co_return decoded.status();
              if (decoded->incarnation_ != auxiliary_group->incarnation_ ||
                  decoded->id_ != auxiliary_group->id_ ||
                  decoded->retired_ != auxiliary_group->retired_ ||
                  decoded->value_.entries_.size() !=
                      auxiliary_group->field_count_) {
                co_return absl::DataLossError(
                    "Hash group payload disagrees with its record identity");
              }
            }
          }
        }
        if (record.grouped_) {
          const std::size_t key_prefix =
              record.key_external_ ? record.key_bytes_ : 0;
          std::size_t encoded_bytes = record.payload_bytes_;
          std::string metadata_bytes;
          std::string_view encoded;
          if (record.external_) {
            encoded_bytes = 0;
            for (const ExtentRef& ref : *extents) {
              if (encoded_bytes > kMaxRecordPayloadBytes - ref.payload_bytes_) {
                co_return absl::DataLossError(
                    "recovered group extent payload exceeds its limit");
              }
              encoded_bytes += ref.payload_bytes_;
            }
            if (encoded_bytes < key_prefix) {
              co_return absl::DataLossError("recovered group key is truncated");
            }
            encoded_bytes -= key_prefix;
            // Root size is fixed by its payload version. External roots carry
            // large parent keys, whose source-dependent extent lifetime
            // protects reads of older root versions before the top-level merge
            // is complete.
            const std::size_t metadata_size = encoded_bytes;
            if (metadata_size > (ordered ? kIndexedSortedSetRootBytes
                                         : kGroupedHashRootBytes)) {
              co_return absl::DataLossError(
                  "recovered grouped root is too large");
            }
            auto loaded = co_await LoadRecoveryPayloadSlice(
                store, extents, key_prefix, metadata_size);
            if (!loaded.ok()) co_return loaded.status();
            metadata_bytes = std::move(*loaded);
            encoded = metadata_bytes;
          } else {
            if (encoded_bytes < key_prefix) {
              co_return absl::DataLossError("recovered group key is truncated");
            }
            encoded_bytes -= key_prefix;
            encoded = std::string_view(
                reinterpret_cast<const char*>(payload) + key_prefix,
                encoded_bytes);
          }
          if (ordered) {
            auto decoded = DecodeOrderedCollectionRoot(encoded);
            if (!decoded.ok()) co_return decoded.status();
            if (decoded->revision_ == 0) {
              decoded->revision_ = record.mutation_sequence_;
            }
            if (decoded->kind_ != ordered_kind ||
                decoded->item_count_ != record.logical_size_) {
              co_return absl::DataLossError(
                  "ordered root disagrees with its record header");
            }
            grouped_root = *decoded;
            AtomicMax(&recovery_max_txid_, decoded->revision_);
            AtomicMax(&recovery_max_txid_, decoded->incarnation_);
          } else {
            auto decoded = DecodeGroupedHashRoot(encoded);
            if (!decoded.ok()) co_return decoded.status();
            if (decoded->field_count_ != record.logical_size_) {
              co_return absl::DataLossError(
                  "grouped root count disagrees with its record header");
            }
            grouped_root = *decoded;
            AtomicMax(&recovery_max_txid_, decoded->revision_);
            AtomicMax(&recovery_max_txid_, decoded->incarnation_);
          }
        }
        RecoveryRecord recovered{
            .digest_ = digest,
            .key_ = record.key_external_ && record.external_
                        ? std::move(loaded_key)
                        : std::string(key),
            .db_id_ = record.db_id_,
            .txid_ = record.txid_,
            .lsn_ = record.lsn_,
            .replication_epoch_ = record.replication_epoch_,
            .location_ = RecordLocation(
                block_id, record.mutation_sequence_, record.allocation_epoch_,
                record.expire_at_ms_,
                static_cast<std::uint32_t>(record.logical_size_),
                RecordLocation::PackedMetadata::Encode(
                    record_offset, record.total_disk_bytes_, block_owner, false,
                    record.external_, record.key_external_, false, false,
                    record.txid_ != 0, record.kind_, record.value_type_,
                    record.expire_at_ms_ != 0, record.grouped_)),
            .extents_ = extents,
            .auxiliary_group_ = auxiliary_group,
            .ordered_group_ = ordered_group,
            .grouped_root_ = grouped_root};
        buffered_bytes += sizeof(RecoveryRecord) + recovered.key_.capacity();
        if (recovered.extents_ != nullptr) {
          buffered_bytes += recovered.extents_->capacity() * sizeof(ExtentRef);
        }
        const unsigned key_owner = partition_id % worker_count_;
        batches->at(key_owner).records_.push_back(std::move(recovered));
        record_offset += record.total_disk_bytes_;
        ++records;
        if (buffered_bytes >= batch_target_bytes) {
          absl::Status applied = co_await ApplyRecoveryBatches(store, batches);
          if (!applied.ok()) {
            co_return applied;
          }
          buffered_bytes = 0;
        }
      }
      if (record_offset != block.committed_bytes_ ||
          records != block.record_count_) {
        co_return absl::Status(
            absl::StatusCode::kInternal,
            "block committed boundary does not match records");
      }
      ReportRecoveryProgress(records, /*allocated=*/true);
      if (buffered_bytes >= batch_target_bytes) {
        absl::Status applied = co_await ApplyRecoveryBatches(store, batches);
        if (!applied.ok()) {
          co_return applied;
        }
        buffered_bytes = 0;
      }
    }
    if (!scanned_block) break;
  }
  co_return absl::OkStatus();
}

absl::Status StorageEngine::Impl::ApplyRecovery(unsigned target,
                                                RecoveryBatch batch) {
  WorkerStore& store = *stores_[target];
  for (const RecoveryBlock& recovered : batch.blocks_) {
    const ActiveBlock& block = recovered.block_;
    BlockState& state =
        CreateBlockState(store, block.block_id_, block.allocation_epoch_);
    state.writer_id_ = block.writer_id_;
    state.layout_worker_count_ = block.layout_worker_count_;
    state.committed_bytes_ = block.committed_bytes_;
    state.allocated_ = true;
    state.kind_ = block.kind_;
    if (block.kind_ == BlockKind::kTransaction) {
      RegisterRecoveredTxGeneration(store, block.tx_generation_);
      store.tx_blocks_.insert_or_assign(
          block.block_id_, WorkerStore::TxBlockRuntime{
                               .allocation_epoch_ = block.allocation_epoch_,
                               .generation_ = block.tx_generation_,
                           });
    }
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
    if (recovered.auxiliary_group_.has_value()) {
      // Auxiliaries are not user-key versions. Keep committed and prepared
      // candidates separate from the root index until the global decision
      // barrier can adjudicate the entire graph together.
      store.recovery_hash_groups_.push_back(recovered);
      continue;
    }
    if (recovered.txid_ != 0) {
      // Whether this record's transaction committed is only decidable once
      // every worker's scan has fed the committed set; park it until after
      // the recovery barrier.
      store.recovery_tx_records_.push_back(recovered);
      continue;
    }
    absl::Status applied = ApplyRecoveredRecord(store, recovered);
    if (!applied.ok()) return applied;
  }
  for (const RecoveryBatch::CommitRecord& commit : batch.commit_records_) {
    BlockState* state = FindBlockState(store, commit.block_id_);
    if (state == nullptr || !state->allocated_) {
      Fail(absl::Status(absl::StatusCode::kInternal,
                        "recovered commit record has no owning block"));
      continue;
    }
    state->live_bytes_ += commit.bytes_;
    if (state->kind_ == BlockKind::kTransaction) {
      const auto tx_block = store.tx_blocks_.find(commit.block_id_);
      if (tx_block == store.tx_blocks_.end()) {
        Fail(absl::InternalError("recovered transaction block is untracked"));
        continue;
      }
      NoteTxRecordLocal(store, commit.block_id_, state->allocation_epoch_,
                        tx_block->second.generation_, commit.txid_,
                        commit.bytes_, true);
    }
  }
  return absl::OkStatus();
}

absl::Status StorageEngine::Impl::ApplyRecoveredRecord(
    WorkerStore& store, const RecoveryRecord& recovered) {
  auto& partition = PartitionForKey(store, recovered.key_);
  const RecoveryRecordView view{
      .digest_ = recovered.digest_,
      .key_ = recovered.key_,
      .db_id_ = recovered.db_id_,
      .txid_ = recovered.txid_,
      .lsn_ = recovered.lsn_,
      .replication_epoch_ = recovered.replication_epoch_,
      .location_ = recovered.location_,
      .extents_ = &recovered.extents_,
      .grouped_root_ = recovered.grouped_root_.has_value()
                           ? &*recovered.grouped_root_
                           : nullptr,
      .checkpoint_snapshot_ = recovered.checkpoint_snapshot_,
  };
  return ApplyRecoveredRecord(store, partition, view);
}

absl::Status StorageEngine::Impl::ApplyRecoveredRecord(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    const RecoveryRecordView& recovered) {
  assert(recovered.extents_ != nullptr);
  {
    if (recovered.replication_epoch_ != partition.replication_epoch_) {
      return absl::OkStatus();
    }
    if (recovered.db_id_ >= options_.database_count_) {
      return absl::FailedPreconditionError(
          "storage contains current records in a disabled database");
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
    std::uint64_t current_lsn = 0;
    if (found != nullptr) {
      const auto recovered_lsn = store.recovery_lsns_.find(found);
      if (recovered_lsn != store.recovery_lsns_.end()) {
        current_lsn = recovered_lsn->second;
      } else if (checkpoint_active_.load(std::memory_order_acquire)) {
        // Checkpoint entries are the clean-shutdown winners. Avoid retaining
        // one redundant hash-table node per key on the successful path while
        // still preventing an equal-sequence stale copy from replacing one.
        current_lsn = std::numeric_limits<std::uint64_t>::max();
      }
    }
    bool equal_command_newer = recovered.lsn_ > current_lsn;
    if (found != nullptr && recovered.grouped_root_ != nullptr &&
        found->value_.grouped() &&
        recovered.location_.mutation_sequence_ ==
            found->value_.mutation_sequence_) {
      const auto current_root = store.recovery_grouped_roots_.find(found);
      if (current_root == store.recovery_grouped_roots_.end()) {
        return absl::DataLossError("grouped winner has no revision metadata");
      }
      // One replay envelope can modify the same Hash several times while
      // retaining its outer command sequence. The independent root revision
      // orders those mutations; a later physical GC copy is not a newer value.
      const auto revision = [](const auto& root) { return root.revision_; };
      const auto incoming_revision =
          std::visit(revision, *recovered.grouped_root_);
      const auto current_revision = std::visit(revision, current_root->second);
      if (incoming_revision != current_revision) {
        equal_command_newer = incoming_revision > current_revision;
      }
    }
    const bool candidate_newer = found == nullptr ||
                                 recovered.location_.mutation_sequence_ >
                                     found->value_.mutation_sequence_ ||
                                 (recovered.location_.mutation_sequence_ ==
                                      found->value_.mutation_sequence_ &&
                                  equal_command_newer);
    if (candidate_newer) {
      if (recovered.location_.grouped() !=
          (recovered.grouped_root_ != nullptr)) {
        return absl::DataLossError(
            "recovered grouped winner has no root metadata");
      }
      const bool was_live =
          found != nullptr && found->value_.kind() == RecordKind::kValue;
      const bool is_live = recovered.location_.kind() == RecordKind::kValue;
      const bool was_expiring = was_live && ExpireAt(*found) != 0;
      const bool is_expiring =
          is_live && recovered.location_.expire_at_ms_ != 0;
      RecordLocation winner = recovered.location_;
      if (found != nullptr) {
        winner.set_shielding(
            found->value_.shielding() ||
            (found->value_.kind() == RecordKind::kValue &&
             found->value_.mutation_sequence_ <
                 recovered.location_.mutation_sequence_ &&
             (ExpireAt(*found) == 0 ||
              ExpireAt(*found) > std::max(recovered.location_.expire_at_ms_,
                                          UnixTimeMillis()))));
      }
      winner.set_tx_tagged(recovered.txid_ != 0);
      RecordIndex::Entry* winner_entry = found;
      if (winner_entry != nullptr) {
        auto replaced = ReplaceIndexLocation(store, index, winner_entry,
                                             recovered.digest_, winner);
        if (!replaced.ok()) return replaced.status();
        winner_entry = *replaced;
      } else {
        winner_entry = index.InsertNew(recovered.digest_, recovered.key_,
                                       winner, !winner.key_external());
        if (winner_entry == nullptr) {
          return absl::ResourceExhaustedError(
              "recovery index entry capacity exhausted");
        }
        AddFullSyncCoverageEntry(partition, recovered.db_id_,
                                 recovered.key_.size());
      }
      if (!recovered.checkpoint_snapshot_) {
        store.recovery_lsns_.insert_or_assign(winner_entry, recovered.lsn_);
      }
      if (recovered.grouped_root_ != nullptr) {
        store.recovery_grouped_roots_.insert_or_assign(
            winner_entry, *recovered.grouped_root_);
      } else {
        store.recovery_grouped_roots_.erase(winner_entry);
      }
      if (recovered.txid_ != 0) {
        store.recovery_txids_.insert_or_assign(winner_entry, recovered.txid_);
      } else if (found != nullptr || !recovered.checkpoint_snapshot_) {
        store.recovery_txids_.erase(winner_entry);
      }
      if (winner.external()) {
        store.external_manifests_.insert_or_assign(winner_entry,
                                                   *recovered.extents_);
      } else if (found != nullptr || !recovered.checkpoint_snapshot_) {
        store.external_manifests_.erase(winner_entry);
      }
      if (winner.key_external()) [[unlikely]] {
        store.recovery_external_keys_.insert_or_assign(winner_entry,
                                                       recovered.key_);
      } else if (found != nullptr || !recovered.checkpoint_snapshot_) {
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
    } else if (recovered.location_.kind() == RecordKind::kValue &&
               recovered.location_.mutation_sequence_ <
                   found->value_.mutation_sequence_ &&
               (recovered.location_.expire_at_ms_ == 0 ||
                recovered.location_.expire_at_ms_ >
                    std::max(ExpireAt(*found), UnixTimeMillis()))) {
      found->value_.set_shielding(true);
    }
  }
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RecoverGroupedObjects(
    WorkerStore& store) {
  auto& records = store.recovery_hash_groups_;
  // Group candidates by logical key once. Recovery memory is proportional to
  // scanned auxiliary metadata, never to retained field/value bodies; a root
  // examines only its own candidates rather than rescanning all large keys.
  std::sort(records.begin(), records.end(),
            [](const RecoveryRecord& left, const RecoveryRecord& right) {
              return std::tie(left.db_id_, left.key_) <
                     std::tie(right.db_id_, right.key_);
            });
  for (const auto& [entry, root] : store.recovery_grouped_roots_) {
    const RecordLocation location = MaterializeIndexLocation(*entry);
    if (!location.grouped()) {
      co_return absl::DataLossError(
          "grouped recovery metadata names a compact root");
    }
    std::string_view key;
    if (entry->key_complete()) {
      key = entry->key();
    } else {
      const auto found = store.recovery_external_keys_.find(entry);
      if (found == store.recovery_external_keys_.end()) {
        co_return absl::DataLossError(
            "grouped recovery root has no complete key");
      }
      key = found->second;
    }
    auto& partition = PartitionForKey(store, key);
    std::optional<std::uint8_t> root_db;
    // The transient metadata key is an entry address, not a persisted DB.
    // Resolve it against the small, owner-local index array exactly once per
    // grouped key. Physical identity disambiguates a reused name across DBs.
    const Digest digest = ComputeDigest(key);
    for (std::uint8_t db = 0; db < options_.database_count_; ++db) {
      for (const auto* candidate :
           partition.indexes_[db].FindCandidates(digest, key)) {
        if (candidate == entry) {
          root_db = db;
          break;
        }
      }
      if (root_db.has_value()) break;
    }
    if (!root_db.has_value()) {
      co_return absl::DataLossError(
          "grouped recovery root is not a key winner");
    }
    const auto lower = std::lower_bound(
        records.begin(), records.end(), std::pair(*root_db, key),
        [](const RecoveryRecord& record, const auto& target) {
          return std::pair(record.db_id_, std::string_view(record.key_)) <
                 target;
        });
    const GroupedObjectVersion version{
        .root_ = location,
        .db_epoch_ = DbEpoch(*root_db),
        .replication_epoch_ = partition.replication_epoch_,
        .index_generation_ = partition.grouped_generations_[*root_db],
    };
    if (const auto* ordered = std::get_if<OrderedCollectionRoot>(&root)) {
      auto end = lower;
      while (end != records.end() && end->db_id_ == *root_db &&
             end->key_ == key)
        ++end;
      auto object = co_await RecoverOrderedObject(
          store, *ordered, version,
          std::span(records).subspan(lower - records.begin(), end - lower));
      if (!object.ok()) co_return object.status();
      auto published = partition.grouped_objects_[*root_db].Publish(
          key, nullptr, std::move(*object));
      if (!published.ok()) co_return published;
      continue;
    }
    std::vector<RecoveredHashGroup> candidates;
    for (auto it = lower;
         it != records.end() && it->db_id_ == *root_db && it->key_ == key;
         ++it) {
      auto candidate = *it->auxiliary_group_;
      candidate.record_token_ =
          static_cast<std::uint64_t>(it - records.begin());
      candidates.push_back(candidate);
    }
    auto directory = HashGroupDirectory::Recover(
        std::get<GroupedHashRoot>(root), location.mutation_sequence_,
        candidates, recovery_committed_txids_);
    if (!directory.ok()) co_return directory.status();
    std::vector<HashGroupLocation> locations;
    locations.reserve(directory->groups().size() +
                      directory->retired_groups().size());
    auto append_location = [&](const RecoveredHashGroup& selected) {
      RecoveryRecord& physical = records.at(selected.record_token_);
      physical.grouped_reachable_ = true;
      locations.push_back(HashGroupLocation{
          .id_ = selected.id_,
          .location_ = physical.location_,
          .extents_ = physical.extents_,
          .retired_ = selected.retired_,
      });
    };
    for (const auto& [prefix, selected] : directory->groups()) {
      append_location(selected);
    }
    // A split's retired parent remains live evidence while old parent bytes
    // can still be scanned. Forgetting it makes a later boot resurrect an
    // overlapping routing leaf even though the visible root did not change.
    for (const auto& [id, selected] : directory->retired_groups()) {
      append_location(selected);
    }
    auto object =
        GroupedHashObject::Create(version, std::move(*directory), locations,
                                  store.record_index_entry_arena_);
    if (!object.ok()) co_return object.status();
    auto published = partition.grouped_objects_[*root_db].Publish(
        key, nullptr, std::move(*object));
    if (!published.ok()) co_return published;
  }
  store.recovery_grouped_roots_.clear();
  store.recovery_grouped_roots_.rehash(0);
  co_return absl::OkStatus();
}

Task<absl::StatusOr<GroupedHashObject::Handle>>
StorageEngine::Impl::RecoverOrderedObject(WorkerStore& store,
                                          const OrderedCollectionRoot& root,
                                          GroupedObjectVersion version,
                                          std::span<RecoveryRecord> records) {
  const auto revision =
      root.revision_ == 0 ? version.root_.mutation_sequence_ : root.revision_;
  std::map<std::uint64_t, std::size_t> winners;
  std::vector<RecoveredHashGroup> member_candidates;
  for (std::size_t i = 0; i < records.size(); ++i) {
    const auto& candidate = *records[i].auxiliary_group_;
    if (candidate.incarnation_ != root.incarnation_ ||
        candidate.sequence_ > revision ||
        (candidate.txid_ != 0 &&
         !recovery_committed_txids_.contains(candidate.txid_)) ||
        (candidate.batch_txid_ != 0 &&
         !recovery_committed_txids_.contains(candidate.batch_txid_)))
      continue;
    if (records[i].location_.value_type() != version.root_.value_type()) {
      co_return absl::DataLossError("ordered candidate has a different type");
    }
    if (!IsOrderedPageId(candidate.id_)) {
      if (!candidate.id_.valid())
        co_return absl::DataLossError("invalid member group identity");
      auto member = candidate;
      member.record_token_ = i;
      member_candidates.push_back(member);
      continue;
    }
    auto [position, inserted] = winners.emplace(candidate.id_.prefix_, i);
    if (inserted) continue;
    const auto& previous = *records[position->second].auxiliary_group_;
    if (candidate.sequence_ == previous.sequence_ &&
        (candidate.field_count_ != previous.field_count_ ||
         candidate.retired_ != previous.retired_)) {
      co_return absl::DataLossError("conflicting ordered page header copies");
    }
    if (candidate.sequence_ > previous.sequence_ ||
        (candidate.sequence_ == previous.sequence_ &&
         candidate.lsn_ > previous.lsn_)) {
      position->second = i;
    }
  }
  std::vector<RecoveredOrderedGroup> candidates;
  candidates.reserve(winners.size());
  for (const auto& [id, token] : winners) {
    auto& physical = records[token];
    const auto& header = *physical.auxiliary_group_;
    RecoveredOrderedGroup candidate{
        .incarnation_ = header.incarnation_,
        .id_ = id,
        .sequence_ = header.sequence_,
        .lsn_ = header.lsn_,
        .txid_ = header.txid_,
        .batch_txid_ = header.batch_txid_,
        .item_count_ = header.field_count_,
        // The standalone ordered codec reserves zero as an invalid token.
        .record_token_ = token + 1,
        .retired_ = header.retired_,
    };
    if (physical.location_.external()) {
      if (physical.extents_ == nullptr) {
        co_return absl::DataLossError("ordered page has no extent manifest");
      }
      std::size_t bytes = 0;
      for (const auto& extent : *physical.extents_) {
        if (bytes > kMaxRecordPayloadBytes - extent.payload_bytes_)
          co_return absl::DataLossError("ordered page payload is too large");
        bytes += extent.payload_bytes_;
      }
      const std::size_t key_bytes =
          physical.location_.key_external() ? physical.key_.size() : 0;
      if (bytes < key_bytes)
        co_return absl::DataLossError("ordered page parent key is truncated");
      // Only the highest committed revision of each stable id reaches IO.
      // Superseded value-only extents may already be recycled. The selected
      // page is checked completely. Stream framing and scores into bounded
      // state while the same checksum pass skips member payloads; only the
      // routing envelope and two score bounds survive, even for huge members.
      OrderedGroupMetadataDecoder decoder(bytes - key_bytes);
      auto prefix =
          co_await LoadRecoveryPayloadSlice(store, physical.extents_, key_bytes,
                                            kOrderedGroupHeaderBytes, &decoder);
      if (!prefix.ok()) co_return prefix.status();
      auto metadata = decoder.Finish();
      if (!metadata.ok()) co_return metadata.status();
      if (metadata->kind_ != root.kind_ ||
          metadata->incarnation_ != candidate.incarnation_ ||
          metadata->id_ != candidate.id_ ||
          metadata->retired_ != candidate.retired_ ||
          metadata->item_count_ != candidate.item_count_) {
        co_return absl::DataLossError(
            "ordered page envelope disagrees with its record identity");
      }
      candidate.previous_ = metadata->previous_;
      candidate.next_ = metadata->next_;
      candidate.min_score_ = metadata->min_score_;
      candidate.max_score_ = metadata->max_score_;
    } else {
      if (!physical.ordered_group_.has_value())
        co_return absl::DataLossError("ordered inline page has no metadata");
      candidate.previous_ = physical.ordered_group_->previous_;
      candidate.next_ = physical.ordered_group_->next_;
      candidate.min_score_ = physical.ordered_group_->min_score_;
      candidate.max_score_ = physical.ordered_group_->max_score_;
    }
    candidates.push_back(candidate);
  }
  std::optional<HashGroupDirectory> members;
  if (root.member_index_) {
    auto recovered = HashGroupDirectory::Recover(
        *root.member_index_, version.root_.mutation_sequence_,
        member_candidates, recovery_committed_txids_);
    if (!recovered.ok()) co_return recovered.status();
    members = std::move(*recovered);
  }
  auto directory = OrderedGroupDirectory::Recover(
      root, revision, candidates, recovery_committed_txids_,
      version.root_.mutation_sequence_, std::move(members));
  if (!directory.ok()) co_return directory.status();
  std::vector<HashGroupLocation> locations;
  locations.reserve(candidates.size());
  const auto append = [&](const RecoveredOrderedGroup& candidate) {
    auto& physical = records[candidate.record_token_ - 1];
    physical.grouped_reachable_ = true;
    locations.push_back(HashGroupLocation{
        .id_ = {.prefix_ = candidate.id_, .bits_ = 0},
        .location_ = physical.location_,
        .extents_ = physical.extents_,
        .retired_ = candidate.retired_,
    });
  };
  for (const auto& candidate : directory->groups()) append(candidate);
  for (const auto& candidate : directory->retired_groups()) append(candidate);
  if (const auto* member_directory = directory->member_directory()) {
    auto append_member = [&](const RecoveredHashGroup& candidate) {
      auto& physical = records[candidate.record_token_];
      physical.grouped_reachable_ = true;
      locations.push_back({.id_ = candidate.id_,
                           .location_ = physical.location_,
                           .extents_ = physical.extents_,
                           .retired_ = candidate.retired_});
    };
    for (const auto& [prefix, member] : member_directory->groups())
      append_member(member);
    for (const auto& [id, member] : member_directory->retired_groups())
      append_member(member);
  }
  co_return GroupedHashObject::CreateOrdered(version, std::move(*directory),
                                             locations,
                                             store.record_index_entry_arena_);
}

Task<absl::Status> StorageEngine::Impl::ValidateRecoveredGroups(
    WorkerStore& store) {
  for (const RecoveryRecord& record : store.recovery_hash_groups_) {
    if (!record.grouped_reachable_ || !record.location_.external()) continue;
    // Ordered winners were fully checksummed while resolving their links;
    // there is no reason to read their potentially huge bodies twice.
    if ((record.location_.value_type() == ValueType::kList ||
         record.location_.value_type() == ValueType::kSortedSet) &&
        IsOrderedPageId(record.auxiliary_group_->id_))
      continue;
    if (record.extents_ == nullptr) {
      co_return absl::DataLossError(
          "live recovered group has no extent manifest");
    }
    std::size_t encoded_bytes = 0;
    for (const ExtentRef& ref : *record.extents_) {
      if (encoded_bytes > kMaxRecordPayloadBytes - ref.payload_bytes_) {
        co_return absl::DataLossError(
            "live recovered group payload is too large");
      }
      encoded_bytes += ref.payload_bytes_;
    }
    const std::size_t key_prefix =
        record.location_.key_external() ? record.key_.size() : 0;
    if (encoded_bytes < key_prefix) {
      co_return absl::DataLossError("live recovered group key is truncated");
    }
    encoded_bytes -= key_prefix;
    // This retains only a bounded envelope, but checks every byte of every
    // selected extent. Unreachable groups never reach this read: a freed or
    // reused obsolete extent cannot make an otherwise valid startup fail.
    auto prefix = co_await LoadRecoveryPayloadSlice(
        store, record.extents_, key_prefix, kHashGroupHeaderBytes);
    if (!prefix.ok()) co_return prefix.status();
    auto decoded = DecodeHashGroupMetadata(*prefix, encoded_bytes);
    if (!decoded.ok()) co_return decoded.status();
    const RecoveredHashGroup& expected = *record.auxiliary_group_;
    if (decoded->incarnation_ != expected.incarnation_ ||
        decoded->id_ != expected.id_ ||
        decoded->field_count_ != expected.field_count_ ||
        decoded->retired_ != expected.retired_) {
      co_return absl::DataLossError(
          "live Hash group envelope disagrees with its record identity");
    }
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
