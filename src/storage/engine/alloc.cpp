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

#include "impl.h"

namespace lavik::storage {

std::size_t StorageEngine::Impl::DeviceIndexForBlock(
    std::uint64_t block_id) const noexcept {
  const std::size_t device_index = DeviceIdForBlock(block_id);
  assert(device_index < devices_.size());
  assert(devices_[device_index].id_ == device_index);
  return device_index;
}

bool StorageEngine::Impl::BitmapBit(const DeviceAllocator& allocator,
                                    std::uint32_t local_block) noexcept {
  const std::size_t byte_index = local_block / 8;
  const unsigned bit_index = local_block % 8;
  return (std::to_integer<unsigned>(allocator.scan_bitmap_[byte_index]) &
          (1U << bit_index)) != 0;
}

void StorageEngine::Impl::SetBitmapBit(DeviceAllocator& allocator,
                                       std::uint32_t local_block) noexcept {
  const std::size_t byte_index = local_block / 8;
  const unsigned bit_index = local_block % 8;
  allocator.scan_bitmap_[byte_index] |= static_cast<std::byte>(1U << bit_index);
}

void StorageEngine::Impl::ClearBitmapBit(DeviceAllocator& allocator,
                                         std::uint32_t local_block) noexcept {
  const std::size_t byte_index = local_block / 8;
  const unsigned bit_index = local_block % 8;
  allocator.scan_bitmap_[byte_index] &=
      static_cast<std::byte>(~(1U << bit_index));
}

Task<absl::Status> StorageEngine::Impl::PersistBitmapPages(
    std::size_t device_index, DeviceAllocator& allocator,
    std::vector<std::size_t> page_indexes) {
  assert(bycorf::ThisWorker().id_ == allocator.owner_);
  if (page_indexes.empty()) {
    co_return absl::OkStatus();
  }
  std::sort(page_indexes.begin(), page_indexes.end());
  page_indexes.erase(std::unique(page_indexes.begin(), page_indexes.end()),
                     page_indexes.end());
  WorkerStore& store = *stores_[allocator.owner_];
  auto acquired = co_await store.buffers_.AcquireReadBuffer();
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size_ = kDirectIoAlignment;
  const StorageDevice& device = devices_[device_index];
  std::vector<MetadataPageState> committed;
  committed.reserve(page_indexes.size());
  for (const std::size_t page_index : page_indexes) {
    if (page_index >= allocator.bitmap_pages_.size()) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "bitmap page index is out of range");
    }
    const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
    const std::size_t payload_bytes = std::min(
        kMetadataPagePayloadBytes, allocator.scan_bitmap_.size() - byte_offset);
    const MetadataPageState current = allocator.bitmap_pages_[page_index];
    const std::uint8_t next_slot = current.active_slot_ == 0 ? 1 : 0;
    const std::uint64_t next_generation = current.generation_ + 1;
    std::span<std::byte, kDirectIoAlignment> output(buffer.data_,
                                                    kDirectIoAlignment);
    EncodeMetadataPage(
        MetadataPageKind::kScanBitmap, static_cast<std::uint32_t>(page_index),
        next_generation,
        std::span<const std::byte>(allocator.scan_bitmap_.data() + byte_offset,
                                   payload_bytes),
        output);
    auto written = co_await WriteStorageBuffer(
        *store.worker_, store.files_[device.file_index_], output,
        lease.registered(), buffer,
        MetadataPageSlotOffset(kScanBitmapMetadataOffset, page_index,
                               next_slot));
    if (!written.ok() || *written != kDirectIoAlignment) {
      co_return written.ok() ? absl::Status(absl::StatusCode::kInternal,
                                            "short scan-bitmap metadata write")
                             : written.status();
    }
    committed.push_back(MetadataPageState{
        .generation_ = next_generation,
        .active_slot_ = next_slot,
    });
  }
  absl::Status synced = co_await bycorf::Fdatasync(
      *store.worker_, store.files_[device.file_index_]);
  if (!synced.ok()) {
    co_return synced;
  }
  for (std::size_t i = 0; i < page_indexes.size(); ++i) {
    allocator.bitmap_pages_[page_indexes[i]] = committed[i];
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::InvalidateReactivatedBlockHeadersLocal(
    std::size_t device_index, std::span<const std::uint64_t> block_ids) {
  if (block_ids.empty()) {
    co_return absl::OkStatus();
  }
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(bycorf::ThisWorker().id_ == allocator.owner_);
  WorkerStore& store = *stores_[allocator.owner_];
  const StorageDevice& device = devices_[device_index];
  auto* zero_header = static_cast<std::byte*>(bycorf::AllocateStorageBuffer(
      kBlockHeaderBytes, options_.buffers_.alignment_));
  if (zero_header == nullptr) {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "failed to allocate recycled-block header buffer");
  }
  std::fill_n(zero_header, kBlockHeaderBytes, std::byte{0});
  absl::Status status = absl::OkStatus();
  for (const std::uint64_t block_id : block_ids) {
    assert(DeviceIndexForBlock(block_id) == device_index);
    auto written = co_await WriteStorageBuffer(
        *store.worker_, store.files_[device.file_index_],
        std::span<const std::byte>(zero_header, kBlockHeaderBytes), false, {},
        LocalBlockOffset(block_id));
    if (!written.ok() || *written != kBlockHeaderBytes) {
      status = written.ok()
                   ? absl::Status(absl::StatusCode::kInternal,
                                  "short recycled-block header invalidation")
                   : written.status();
      break;
    }
  }
  if (status.ok()) {
    status = co_await bycorf::Fdatasync(*store.worker_,
                                        store.files_[device.file_index_]);
  }
  bycorf::FreeStorageBuffer(zero_header, options_.buffers_.alignment_);
  co_return status;
}

Task<absl::Status> StorageEngine::Impl::RefillReadyBlocksLocal(
    std::size_t device_index, DeviceAllocator& allocator) {
  assert(bycorf::ThisWorker().id_ == allocator.owner_);
  constexpr std::size_t kActivationBatchBlocks = 256;
  const StorageDevice& device = devices_[device_index];
  std::vector<std::uint64_t> activated;
  activated.reserve(kActivationBatchBlocks);
  std::vector<std::uint64_t> reactivated;
  reactivated.reserve(kActivationBatchBlocks);
  // Reuse reclaimed blocks before touching new offsets. Besides bounding the
  // physical footprint of sparse file-backed devices under overwrite-heavy
  // workloads, this makes blocks released by replication backlog trimming
  // immediately useful to both foreground writes and the next backlog batch.
  while (activated.size() < kActivationBatchBlocks &&
         !allocator.cold_free_.empty()) {
    const std::uint64_t block_id = allocator.cold_free_.back();
    allocator.cold_free_.pop_back();
    activated.push_back(block_id);
    reactivated.push_back(block_id);
  }
  while (activated.size() < kActivationBatchBlocks &&
         allocator.next_pristine_ < device.capacity_blocks_) {
    const std::uint32_t local =
        static_cast<std::uint32_t>(allocator.next_pristine_++);
    activated.push_back(MakeBlockId(device.id_, local));
  }
  if (activated.empty()) {
    co_return absl::OkStatus();
  }

  // A cold block deliberately retains its old header while its bitmap bit is
  // clear. Invalidate that stale header before making the bit durable again,
  // otherwise a crash between activation and the writer's first flush could
  // make recovery accept records from the block's previous allocation.
  absl::Status invalidated = co_await InvalidateReactivatedBlockHeadersLocal(
      device_index, reactivated);
  if (!invalidated.ok()) {
    allocator.failed_ = invalidated;
    LatchRuntimeFailure();
    co_return invalidated;
  }

  std::vector<std::size_t> dirty_pages;
  dirty_pages.reserve(activated.size());
  for (const std::uint64_t block_id : activated) {
    const std::uint32_t local = LocalBlockId(block_id);
    if (!BitmapBit(allocator, local)) {
      SetBitmapBit(allocator, local);
      dirty_pages.push_back((local / 8) / kMetadataPagePayloadBytes);
    }
  }
  absl::Status persisted = co_await PersistBitmapPages(device_index, allocator,
                                                       std::move(dirty_pages));
  if (!persisted.ok()) {
    // A failed metadata write has an ambiguous durable state. Do not skip
    // over this activation range or hand out later blocks until restart has
    // selected the newest valid A/B page.
    allocator.failed_ = persisted;
    LatchRuntimeFailure();
    co_return persisted;
  }
  allocator.ready_blocks_.insert(allocator.ready_blocks_.end(),
                                 activated.begin(), activated.end());
  co_return absl::OkStatus();
}

Task<absl::StatusOr<ReservedBlock>>
StorageEngine::Impl::AllocateFromDeviceLocal(std::size_t device_index,
                                             AllocationPurpose purpose) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(bycorf::ThisWorker().id_ == allocator.owner_);
  co_await allocator.mutex_.Lock();
  UnlockGuard unlock(&allocator.mutex_, stores_[allocator.owner_]->worker_);
  if (allocator.failed_.has_value()) {
    co_return *allocator.failed_;
  }
  const std::size_t reserve = purpose == AllocationPurpose::kDefrag
                                  ? 0
                                  : DefragReserveForDevice(device_index);
  if (allocator.ready_blocks_.size() <= reserve) {
    absl::Status refill =
        co_await RefillReadyBlocksLocal(device_index, allocator);
    if (!refill.ok()) {
      co_return refill;
    }
  }
  if (allocator.ready_blocks_.size() <= reserve) {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "device has no allocatable blocks");
  }
  if (allocator.next_allocation_epoch_ > RecordLocation::kAllocationEpochMask) {
    // The durable epoch remains 64-bit, but publishing a block that the
    // runtime index cannot identify exactly would make reuse checks unsafe.
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "device allocation epoch exhausted");
  }
  const std::uint64_t block_id = allocator.ready_blocks_.back();
  allocator.ready_blocks_.pop_back();
  if (purpose != AllocationPurpose::kShutdownMetadata) {
    // Shutdown waits only for the explicit metadata barriers. Starting an
    // allocator refill here would create new background work after the normal
    // drain has already declared the worker quiescent.
    MaybeRefillDeviceInBackground(device_index, allocator);
  }
  co_return ReservedBlock{
      .block_id_ = block_id,
      .allocation_epoch_ = allocator.next_allocation_epoch_++,
  };
}

// Tops the ready pool up from the background once it runs low, so the batch
// bitmap persist in RefillReadyBlocksLocal stays off allocation paths. The
// refill cadence is unchanged — the pool still drains by one batch between
// refills — only the trigger point moves ahead of empty.
void StorageEngine::Impl::MaybeRefillDeviceInBackground(
    std::size_t device_index, DeviceAllocator& allocator) {
  constexpr std::size_t kLowWaterBlocks = 32;
  if (allocator.refill_pending_ || allocator.failed_.has_value()) {
    return;
  }
  if (allocator.next_pristine_ >= devices_[device_index].capacity_blocks_ &&
      allocator.cold_free_.empty()) {
    return;  // Nothing to activate; allocation reports exhaustion itself.
  }
  if (allocator.ready_blocks_.size() >=
      DefragReserveForDevice(device_index) + kLowWaterBlocks) {
    return;
  }
  allocator.refill_pending_ = true;
  stores_[allocator.owner_]->worker_->SpawnBackground(
      RefillDeviceInBackground(device_index));
}

Task<absl::Status> StorageEngine::Impl::RefillDeviceInBackground(
    std::size_t device_index) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  co_await allocator.mutex_.Lock();
  UnlockGuard unlock(&allocator.mutex_, stores_[allocator.owner_]->worker_);
  absl::Status refilled = absl::OkStatus();
  if (!allocator.failed_.has_value()) {
    refilled = co_await RefillReadyBlocksLocal(device_index, allocator);
  }
  allocator.refill_pending_ = false;
  if (!refilled.ok()) {
    spdlog::error("background block refill failed on device {}: {}",
                  device_index, refilled.message());
  }
  co_return refilled;
}

Task<absl::StatusOr<ReservedBlock>> StorageEngine::Impl::AllocateFromDevice(
    std::size_t device_index, AllocationPurpose purpose) {
  const bycorf::WorkerId owner = device_allocators_[device_index]->owner_;
  if (bycorf::ThisWorker().id_ == owner) {
    co_return co_await AllocateFromDeviceLocal(device_index, purpose);
  }
  co_return co_await bycorf::SubmitTaskTo(
      owner,
      [this, device_index, purpose]() -> Task<absl::StatusOr<ReservedBlock>> {
        co_return co_await AllocateFromDeviceLocal(device_index, purpose);
      });
}

Task<absl::Status> StorageEngine::Impl::ReturnColdBlocksLocal(
    std::size_t device_index, std::vector<std::uint64_t> block_ids) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(bycorf::ThisWorker().id_ == allocator.owner_);
  co_await allocator.mutex_.Lock();
  UnlockGuard unlock(&allocator.mutex_, stores_[allocator.owner_]->worker_);
  if (allocator.failed_.has_value()) {
    co_return *allocator.failed_;
  }
  std::vector<std::size_t> dirty_pages;
  dirty_pages.reserve(block_ids.size());
  for (const std::uint64_t block_id : block_ids) {
    assert(DeviceIndexForBlock(block_id) == device_index);
    const std::uint32_t local = LocalBlockId(block_id);
    if (BitmapBit(allocator, local)) {
      ClearBitmapBit(allocator, local);
      dirty_pages.push_back((local / 8) / kMetadataPagePayloadBytes);
    }
  }
  absl::Status persisted = co_await PersistBitmapPages(device_index, allocator,
                                                       std::move(dirty_pages));
  if (!persisted.ok()) {
    allocator.failed_ = persisted;
    LatchRuntimeFailure();
    co_return persisted;
  }
  allocator.cold_free_.insert(allocator.cold_free_.end(), block_ids.begin(),
                              block_ids.end());
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReturnColdBlocks(
    std::vector<std::uint64_t> block_ids) {
  std::vector<std::vector<std::uint64_t>> by_device(devices_.size());
  for (const std::uint64_t block_id : block_ids) {
    by_device[DeviceIndexForBlock(block_id)].push_back(block_id);
  }
  for (std::size_t device_index = 0; device_index < by_device.size();
       ++device_index) {
    if (by_device[device_index].empty()) {
      continue;
    }
    const bycorf::WorkerId owner = device_allocators_[device_index]->owner_;
    // Deliberately if/else, not a conditional expression: two co_awaits in
    // one full expression miscompile under GCC coroutines (branch awaiter
    // temporaries alias frame slots; destroying the suspended frame then
    // runs destructors on garbage).
    absl::Status returned;
    if (owner == bycorf::ThisWorker().id_) {
      returned = co_await ReturnColdBlocksLocal(
          device_index, std::move(by_device[device_index]));
    } else {
      returned = co_await bycorf::SubmitTaskTo(
          owner,
          [this, device_index,
           blocks = std::move(
               by_device[device_index])]() mutable -> Task<absl::Status> {
            co_return co_await ReturnColdBlocksLocal(device_index,
                                                     std::move(blocks));
          });
    }
    if (!returned.ok()) {
      co_return returned;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PersistEpochValueOnDeviceLocal(
    std::size_t device_index, std::size_t value_index, std::uint64_t epoch) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(bycorf::ThisWorker().id_ == allocator.owner_);
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::Status(
        absl::StatusCode::kFailedPrecondition,
        "epoch metadata writer is stopped after an IO failure");
  }
  if (value_index >= allocator.epoch_values_.size()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "epoch metadata index is out of range");
  }
  co_await allocator.mutex_.Lock();
  UnlockGuard allocator_unlock(&allocator.mutex_,
                               stores_[allocator.owner_]->worker_);
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::Status(
        absl::StatusCode::kFailedPrecondition,
        "epoch metadata writer is stopped after an IO failure");
  }
  if (epoch < allocator.epoch_values_[value_index]) {
    co_return absl::OkStatus();
  }
  const std::uint64_t desired =
      std::max(epoch, allocator.epoch_values_[value_index]);
  if (allocator.durable_epoch_values_[value_index] >= desired) {
    co_return absl::OkStatus();
  }
  const std::size_t byte_offset = value_index * sizeof(std::uint64_t);
  const std::size_t page_index = byte_offset / kMetadataPagePayloadBytes;
  const std::size_t page_byte_offset = page_index * kMetadataPagePayloadBytes;
  const std::size_t payload_bytes = std::min(
      kMetadataPagePayloadBytes, kEpochMetadataBytes - page_byte_offset);
  WorkerStore& store = *stores_[allocator.owner_];
  auto acquired = co_await store.buffers_.AcquireReadBuffer();
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size_ = kDirectIoAlignment;
  allocator.epoch_values_[value_index] = desired;
  const MetadataPageState current = allocator.epoch_pages_[page_index];
  const std::uint8_t next_slot = current.active_slot_ == 0 ? 1 : 0;
  const std::uint64_t next_generation = current.generation_ + 1;
  std::span<std::byte, kDirectIoAlignment> output(buffer.data_,
                                                  kDirectIoAlignment);
  EncodeMetadataPage(
      MetadataPageKind::kEpochs, static_cast<std::uint32_t>(page_index),
      next_generation,
      std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(allocator.epoch_values_.data()) +
              page_byte_offset,
          payload_bytes),
      output);
  const StorageDevice& device = devices_[device_index];
  auto written = co_await WriteStorageBuffer(
      *store.worker_, store.files_[device.file_index_], output,
      lease.registered(), buffer,
      MetadataPageSlotOffset(kEpochMetadataOffset, page_index, next_slot));
  if (!written.ok() || *written != kDirectIoAlignment) {
    epoch_metadata_failed_.store(true, std::memory_order_release);
    LatchRuntimeFailure();
    co_return written.ok()
        ? absl::Status(absl::StatusCode::kInternal,
                       "short write of device epoch metadata")
        : written.status();
  }
  absl::Status synced = co_await bycorf::Fdatasync(
      *store.worker_, store.files_[device.file_index_]);
  if (!synced.ok()) {
    epoch_metadata_failed_.store(true, std::memory_order_release);
    LatchRuntimeFailure();
    co_return synced;
  }
  allocator.epoch_pages_[page_index] = MetadataPageState{
      .generation_ = next_generation,
      .active_slot_ = next_slot,
  };
  const std::size_t first_value = page_byte_offset / sizeof(std::uint64_t);
  const std::size_t value_count = payload_bytes / sizeof(std::uint64_t);
  for (std::size_t i = 0; i < value_count; ++i) {
    allocator.durable_epoch_values_[first_value + i] =
        allocator.epoch_values_[first_value + i];
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PersistEpochValuesOnDeviceLocal(
    std::size_t device_index,
    std::span<const std::pair<std::size_t, std::uint64_t>> values) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(bycorf::ThisWorker().id_ == allocator.owner_);
  if (values.empty()) co_return absl::OkStatus();
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::Status(
        absl::StatusCode::kFailedPrecondition,
        "epoch metadata writer is stopped after an IO failure");
  }
  for (const auto& [value_index, epoch] : values) {
    if (value_index >= allocator.epoch_values_.size() || epoch == 0) {
      co_return absl::Status(absl::StatusCode::kOutOfRange,
                             "epoch metadata update is out of range");
    }
  }

  co_await allocator.mutex_.Lock();
  UnlockGuard allocator_unlock(&allocator.mutex_,
                               stores_[allocator.owner_]->worker_);
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::Status(
        absl::StatusCode::kFailedPrecondition,
        "epoch metadata writer is stopped after an IO failure");
  }

  std::vector<bool> dirty_pages(allocator.epoch_pages_.size(), false);
  for (const auto& [value_index, epoch] : values) {
    const std::uint64_t desired =
        std::max(epoch, allocator.epoch_values_[value_index]);
    allocator.epoch_values_[value_index] = desired;
    if (allocator.durable_epoch_values_[value_index] < desired) {
      const std::size_t byte_offset = value_index * sizeof(std::uint64_t);
      dirty_pages[byte_offset / kMetadataPagePayloadBytes] = true;
    }
  }
  if (std::none_of(dirty_pages.begin(), dirty_pages.end(),
                   [](bool dirty) { return dirty; })) {
    co_return absl::OkStatus();
  }

  WorkerStore& store = *stores_[allocator.owner_];
  auto acquired = co_await store.buffers_.AcquireReadBuffer();
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size_ = kDirectIoAlignment;
  std::vector<MetadataPageState> next_states = allocator.epoch_pages_;
  const StorageDevice& device = devices_[device_index];
  for (std::size_t page_index = 0; page_index < dirty_pages.size();
       ++page_index) {
    if (!dirty_pages[page_index]) continue;
    const std::size_t page_byte_offset = page_index * kMetadataPagePayloadBytes;
    const std::size_t payload_bytes = std::min(
        kMetadataPagePayloadBytes, kEpochMetadataBytes - page_byte_offset);
    const MetadataPageState current = allocator.epoch_pages_[page_index];
    const std::uint8_t next_slot = current.active_slot_ == 0 ? 1 : 0;
    const std::uint64_t next_generation = current.generation_ + 1;
    std::span<std::byte, kDirectIoAlignment> output(buffer.data_,
                                                    kDirectIoAlignment);
    EncodeMetadataPage(
        MetadataPageKind::kEpochs, static_cast<std::uint32_t>(page_index),
        next_generation,
        std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(allocator.epoch_values_.data()) +
                page_byte_offset,
            payload_bytes),
        output);
    auto written = co_await WriteStorageBuffer(
        *store.worker_, store.files_[device.file_index_], output,
        lease.registered(), buffer,
        MetadataPageSlotOffset(kEpochMetadataOffset, page_index, next_slot));
    if (!written.ok() || *written != kDirectIoAlignment) {
      epoch_metadata_failed_.store(true, std::memory_order_release);
      LatchRuntimeFailure();
      co_return written.ok()
          ? absl::Status(absl::StatusCode::kInternal,
                         "short write of device epoch metadata batch")
          : written.status();
    }
    next_states[page_index] = MetadataPageState{
        .generation_ = next_generation,
        .active_slot_ = next_slot,
    };
  }

  absl::Status synced = co_await bycorf::Fdatasync(
      *store.worker_, store.files_[device.file_index_]);
  if (!synced.ok()) {
    epoch_metadata_failed_.store(true, std::memory_order_release);
    LatchRuntimeFailure();
    co_return synced;
  }
  for (std::size_t page_index = 0; page_index < dirty_pages.size();
       ++page_index) {
    if (!dirty_pages[page_index]) continue;
    allocator.epoch_pages_[page_index] = next_states[page_index];
    const std::size_t page_byte_offset = page_index * kMetadataPagePayloadBytes;
    const std::size_t payload_bytes = std::min(
        kMetadataPagePayloadBytes, kEpochMetadataBytes - page_byte_offset);
    const std::size_t first_value = page_byte_offset / sizeof(std::uint64_t);
    const std::size_t value_count = payload_bytes / sizeof(std::uint64_t);
    for (std::size_t i = 0; i < value_count; ++i) {
      allocator.durable_epoch_values_[first_value + i] =
          allocator.epoch_values_[first_value + i];
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PersistEpochValue(
    std::size_t value_index, std::uint64_t epoch) {
  if (value_index >= kEpochValueCount) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "epoch metadata index is out of range");
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const bycorf::WorkerId owner = device_allocators_[device_index]->owner_;
    absl::Status persisted;
    if (owner == bycorf::ThisWorker().id_) {
      persisted = co_await PersistEpochValueOnDeviceLocal(device_index,
                                                          value_index, epoch);
    } else {
      persisted = co_await bycorf::SubmitTaskTo(
          owner,
          [this, device_index, value_index, epoch]() -> Task<absl::Status> {
            co_return co_await PersistEpochValueOnDeviceLocal(
                device_index, value_index, epoch);
          });
    }
    if (!persisted.ok()) {
      co_return persisted;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PersistEpochValues(
    std::span<const std::pair<std::size_t, std::uint64_t>> values) {
  for (const auto& [value_index, epoch] : values) {
    if (value_index >= kEpochValueCount || epoch == 0) {
      co_return absl::Status(absl::StatusCode::kOutOfRange,
                             "epoch metadata update is out of range");
    }
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const bycorf::WorkerId owner = device_allocators_[device_index]->owner_;
    absl::Status persisted;
    if (owner == bycorf::ThisWorker().id_) {
      persisted =
          co_await PersistEpochValuesOnDeviceLocal(device_index, values);
    } else {
      std::vector<std::pair<std::size_t, std::uint64_t>> copied(values.begin(),
                                                                values.end());
      persisted = co_await bycorf::SubmitTaskTo(
          owner,
          [this, device_index,
           copied = std::move(copied)]() -> Task<absl::Status> {
            co_return co_await PersistEpochValuesOnDeviceLocal(device_index,
                                                               copied);
          });
    }
    if (!persisted.ok()) co_return persisted;
  }
  co_return absl::OkStatus();
}

Task<absl::StatusOr<ReservedBlock>> StorageEngine::Impl::AllocateBlock(
    WorkerStore& store, AllocationPurpose purpose) {
  std::vector<std::size_t> home_order(store.home_devices_.size());
  for (std::size_t i = 0; i < home_order.size(); ++i) {
    home_order[i] = i;
  }
  std::sort(
      home_order.begin(), home_order.end(),
      [this, &store](std::size_t left, std::size_t right) {
        const long double left_score =
            static_cast<long double>(store.home_device_allocations_[left] + 1) /
            static_cast<long double>(
                ForegroundBlocksForDevice(store.home_devices_[left]));
        const long double right_score =
            static_cast<long double>(store.home_device_allocations_[right] +
                                     1) /
            static_cast<long double>(
                ForegroundBlocksForDevice(store.home_devices_[right]));
        return left_score < right_score;
      });
  std::vector<std::size_t> attempt_order;
  const std::size_t device_count = devices_.size();
  std::vector<bool> included;
  if (bycorf::SpdkStorageEnabled()) {
    attempt_order.reserve(home_order.size());
  } else {
    attempt_order.reserve(device_count);
    included.assign(device_count, false);
  }
  for (const std::size_t home_index : home_order) {
    const std::size_t device_index = store.home_devices_[home_index];
    attempt_order.push_back(device_index);
    if (!bycorf::SpdkStorageEnabled()) {
      included[device_index] = true;
    }
  }
  if (!bycorf::SpdkStorageEnabled()) {
    for (std::size_t device_index = 0; device_index < device_count;
         ++device_index) {
      if (!included[device_index]) {
        attempt_order.push_back(device_index);
      }
    }
  }
  while (true) {
    if (purpose == AllocationPurpose::kForeground &&
        shutdown_flush_requested_.load(std::memory_order_acquire)) {
      co_return absl::UnavailableError("storage is shutting down");
    }
    if (store.write_failed_ || RuntimeFailureLatched() ||
        epoch_metadata_failed_.load(std::memory_order_acquire)) {
      co_return absl::FailedPreconditionError(
          "storage writer is stopped after an IO failure");
    }
    const std::uint64_t generation_before =
        space_reclaim_generation_.load(std::memory_order_acquire);
    for (const std::size_t device_index : attempt_order) {
      auto allocated = co_await AllocateFromDevice(device_index, purpose);
      if (allocated.ok()) {
        auto home = std::find(store.home_devices_.begin(),
                              store.home_devices_.end(), device_index);
        if (home != store.home_devices_.end()) {
          const std::size_t home_index =
              static_cast<std::size_t>(home - store.home_devices_.begin());
          ++store.home_device_allocations_[home_index];
        }
        co_return *allocated;
      }
      if (allocated.status().code() != absl::StatusCode::kResourceExhausted) {
        co_return allocated.status();
      }
    }

    // Defrag allocations consume the protected reserve. Waiting for another
    // defrag from inside DefragOne would deadlock when the reserve is truly
    // exhausted, so only foreground allocation waits for reclaim progress.
    if (purpose == AllocationPurpose::kDefrag ||
        purpose == AllocationPurpose::kShutdownMetadata) {
      co_return absl::Status(absl::StatusCode::kResourceExhausted,
                             purpose == AllocationPurpose::kDefrag
                                 ? "defrag reserve is exhausted"
                                 : "shutdown metadata space is exhausted");
    }
    const std::uint64_t generation_after =
        space_reclaim_generation_.load(std::memory_order_acquire);
    if (generation_after != generation_before) {
      continue;
    }
    const bool defrag_can_reclaim =
        active_defrags_.load(std::memory_order_acquire) != 0 ||
        (pending_defrags_.load(std::memory_order_acquire) != 0 &&
         !defrag_config_.paused_.load(std::memory_order_acquire));
    if (defrag_can_reclaim ||
        active_flushes_.load(std::memory_order_acquire) != 0 ||
        active_extent_reclaims_.load(std::memory_order_acquire) != 0) {
      absl::Status waited = co_await bycorf::SleepFor(
          *store.worker_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        co_return waited;
      }
      continue;
    }
    // Close the race where the final defrag completed between the active
    // count and generation observations. Return FULL only from a stable
    // snapshot with no reclaim work in progress.
    if (space_reclaim_generation_.load(std::memory_order_acquire) !=
        generation_after) {
      continue;
    }
    std::string devices;
    for (std::size_t d = 0; d < device_allocators_.size(); ++d) {
      const DeviceAllocator& allocator = *device_allocators_[d];
      devices += " dev" + std::to_string(d) +
                 " ready=" + std::to_string(allocator.ready_blocks_.size()) +
                 " cold=" + std::to_string(allocator.cold_free_.size()) +
                 " reserve=" + std::to_string(DefragReserveForDevice(d));
    }
    spdlog::warn(
        "allocator: out of disk space;{} flushes={} defrags={}/{} "
        "extent_reclaims={}",
        devices, active_flushes_.load(std::memory_order_relaxed),
        active_defrags_.load(std::memory_order_relaxed),
        pending_defrags_.load(std::memory_order_relaxed),
        active_extent_reclaims_.load(std::memory_order_relaxed));
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "out of disk space");
  }
}

}  // namespace lavik::storage
