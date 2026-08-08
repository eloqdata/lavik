#include "engine_impl.h"

namespace keylane::storage {

std::size_t StorageEngine::Impl::DeviceIndexForBlock(
    std::uint64_t block_id) const noexcept {
  const std::size_t device_index = DeviceIdForBlock(block_id);
  assert(device_index < devices_.size());
  assert(devices_[device_index].id == device_index);
  return device_index;
}

bool StorageEngine::Impl::BitmapBit(const DeviceAllocator& allocator,
                                    std::uint32_t local_block) noexcept {
  const std::size_t byte_index = local_block / 8;
  const unsigned bit_index = local_block % 8;
  return (std::to_integer<unsigned>(allocator.scan_bitmap[byte_index]) &
          (1U << bit_index)) != 0;
}

void StorageEngine::Impl::SetBitmapBit(DeviceAllocator& allocator,
                                       std::uint32_t local_block) noexcept {
  const std::size_t byte_index = local_block / 8;
  const unsigned bit_index = local_block % 8;
  allocator.scan_bitmap[byte_index] |=
      static_cast<std::byte>(1U << bit_index);
}

void StorageEngine::Impl::ClearBitmapBit(DeviceAllocator& allocator,
                                         std::uint32_t local_block) noexcept {
  const std::size_t byte_index = local_block / 8;
  const unsigned bit_index = local_block % 8;
  allocator.scan_bitmap[byte_index] &=
      static_cast<std::byte>(~(1U << bit_index));
}

Task<Status> StorageEngine::Impl::PersistBitmapPages(
    std::size_t device_index, DeviceAllocator& allocator,
    std::vector<std::size_t> page_indexes) {
  assert(celer::ThisWorker().id == allocator.owner);
  if (page_indexes.empty()) {
    co_return Status::Ok();
  }
  std::sort(page_indexes.begin(), page_indexes.end());
  page_indexes.erase(std::unique(page_indexes.begin(), page_indexes.end()),
                     page_indexes.end());
  WorkerStore& store = *stores_[allocator.owner];
  auto acquired = co_await store.buffers.AcquireReadBuffer();
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size = kDirectIoAlignment;
  const StorageDevice& device = devices_[device_index];
  std::vector<MetadataPageState> committed;
  committed.reserve(page_indexes.size());
  for (const std::size_t page_index : page_indexes) {
    if (page_index >= allocator.bitmap_pages.size()) {
      co_return Status(StatusCode::kInternal,
                       "bitmap page index is out of range");
    }
    const std::size_t byte_offset =
        page_index * kMetadataPagePayloadBytes;
    const std::size_t payload_bytes = std::min(
        kMetadataPagePayloadBytes,
        allocator.scan_bitmap.size() - byte_offset);
    const MetadataPageState current = allocator.bitmap_pages[page_index];
    const std::uint8_t next_slot = current.active_slot == 0 ? 1 : 0;
    const std::uint64_t next_generation = current.generation + 1;
    std::span<std::byte, kDirectIoAlignment> output(
        buffer.data, kDirectIoAlignment);
    EncodeMetadataPage(
        MetadataPageKind::kScanBitmap,
        static_cast<std::uint32_t>(page_index), next_generation,
        std::span<const std::byte>(allocator.scan_bitmap.data() + byte_offset,
                                   payload_bytes),
        output);
    auto written = co_await WriteStorageBuffer(
        *store.worker, store.files[device.file_index], output,
        lease.registered(), buffer,
        MetadataPageSlotOffset(kScanBitmapMetadataOffset, page_index,
                               next_slot));
    if (!written.ok() || *written != kDirectIoAlignment) {
      co_return written.ok()
                    ? Status(StatusCode::kInternal,
                             "short scan-bitmap metadata write")
                    : written.status();
    }
    committed.push_back(MetadataPageState{
        .generation = next_generation,
        .active_slot = next_slot,
    });
  }
  Status synced = co_await celer::Fdatasync(
      *store.worker, store.files[device.file_index]);
  if (!synced.ok()) {
    co_return synced;
  }
  for (std::size_t i = 0; i < page_indexes.size(); ++i) {
    allocator.bitmap_pages[page_indexes[i]] = committed[i];
  }
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::InvalidateReactivatedBlockHeadersLocal(
    std::size_t device_index,
    std::span<const std::uint64_t> block_ids) {
  if (block_ids.empty()) {
    co_return Status::Ok();
  }
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(celer::ThisWorker().id == allocator.owner);
  WorkerStore& store = *stores_[allocator.owner];
  const StorageDevice& device = devices_[device_index];
  auto* zero_header = static_cast<std::byte*>(::operator new[](
      kBlockHeaderBytes, std::align_val_t(options_.buffers.alignment),
      std::nothrow));
  if (zero_header == nullptr) {
    co_return Status(StatusCode::kResourceExhausted,
                     "failed to allocate recycled-block header buffer");
  }
  std::fill_n(zero_header, kBlockHeaderBytes, std::byte{0});
  Status status = Status::Ok();
  for (const std::uint64_t block_id : block_ids) {
    assert(DeviceIndexForBlock(block_id) == device_index);
    auto written = co_await WriteStorageBuffer(
        *store.worker, store.files[device.file_index],
        std::span<const std::byte>(zero_header, kBlockHeaderBytes), false, {},
        LocalBlockOffset(block_id));
    if (!written.ok() || *written != kBlockHeaderBytes) {
      status = written.ok()
                   ? Status(StatusCode::kInternal,
                            "short recycled-block header invalidation")
                   : written.status();
      break;
    }
  }
  if (status.ok()) {
    status = co_await celer::Fdatasync(
        *store.worker, store.files[device.file_index]);
  }
  ::operator delete[](zero_header,
                      std::align_val_t(options_.buffers.alignment));
  co_return status;
}

Task<Status> StorageEngine::Impl::RefillReadyBlocksLocal(std::size_t device_index,
                                                         DeviceAllocator& allocator) {
  assert(celer::ThisWorker().id == allocator.owner);
  constexpr std::size_t kActivationBatchBlocks = 256;
  const StorageDevice& device = devices_[device_index];
  std::vector<std::uint64_t> activated;
  activated.reserve(kActivationBatchBlocks);
  std::vector<std::uint64_t> reactivated;
  reactivated.reserve(kActivationBatchBlocks);
  while (activated.size() < kActivationBatchBlocks &&
         allocator.next_pristine < device.capacity_blocks) {
    const std::uint32_t local =
        static_cast<std::uint32_t>(allocator.next_pristine++);
    activated.push_back(MakeBlockId(device.id, local));
  }
  while (activated.size() < kActivationBatchBlocks &&
         !allocator.cold_free.empty()) {
    const std::uint64_t block_id = allocator.cold_free.back();
    allocator.cold_free.pop_back();
    activated.push_back(block_id);
    reactivated.push_back(block_id);
  }
  if (activated.empty()) {
    co_return Status::Ok();
  }

  // A cold block deliberately retains its old header while its bitmap bit is
  // clear. Invalidate that stale header before making the bit durable again,
  // otherwise a crash between activation and the writer's first flush could
  // make recovery accept records from the block's previous allocation.
  Status invalidated = co_await InvalidateReactivatedBlockHeadersLocal(
      device_index, reactivated);
  if (!invalidated.ok()) {
    allocator.failed = invalidated;
    co_return invalidated;
  }

  std::vector<std::size_t> dirty_pages;
  dirty_pages.reserve(activated.size());
  for (const std::uint64_t block_id : activated) {
    const std::uint32_t local = LocalBlockId(block_id);
    if (!BitmapBit(allocator, local)) {
      SetBitmapBit(allocator, local);
      dirty_pages.push_back(
          (local / 8) / kMetadataPagePayloadBytes);
    }
  }
  Status persisted = co_await PersistBitmapPages(
      device_index, allocator, std::move(dirty_pages));
  if (!persisted.ok()) {
    // A failed metadata write has an ambiguous durable state. Do not skip
    // over this activation range or hand out later blocks until restart has
    // selected the newest valid A/B page.
    allocator.failed = persisted;
    co_return persisted;
  }
  allocator.ready_blocks.insert(allocator.ready_blocks.end(),
                                activated.begin(), activated.end());
  co_return Status::Ok();
}

Task<StatusOr<ReservedBlock>> StorageEngine::Impl::AllocateFromDeviceLocal(
    std::size_t device_index, bool for_defrag) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(celer::ThisWorker().id == allocator.owner);
  co_await allocator.mutex.Lock();
  UnlockGuard unlock(&allocator.mutex, stores_[allocator.owner]->worker);
  if (allocator.failed.has_value()) {
    co_return *allocator.failed;
  }
  const std::size_t reserve =
      for_defrag ? 0 : DefragReserveForDevice(device_index);
  if (allocator.ready_blocks.size() <= reserve) {
    Status refill = co_await RefillReadyBlocksLocal(device_index, allocator);
    if (!refill.ok()) {
      co_return refill;
    }
  }
  if (allocator.ready_blocks.size() <= reserve) {
    co_return Status(StatusCode::kResourceExhausted,
                     "device has no allocatable blocks");
  }
  const std::uint64_t block_id = allocator.ready_blocks.back();
  allocator.ready_blocks.pop_back();
  co_return ReservedBlock{
      .block_id = block_id,
      .allocation_epoch = allocator.next_allocation_epoch++,
  };
}

Task<StatusOr<ReservedBlock>> StorageEngine::Impl::AllocateFromDevice(
    std::size_t device_index, bool for_defrag) {
  const celer::WorkerId owner = device_allocators_[device_index]->owner;
  if (celer::ThisWorker().id == owner) {
    co_return co_await AllocateFromDeviceLocal(device_index, for_defrag);
  }
  co_return co_await celer::SubmitTaskTo(
      owner,
      [this, device_index, for_defrag]()
          -> Task<StatusOr<ReservedBlock>> {
        co_return co_await AllocateFromDeviceLocal(device_index,
                                                    for_defrag);
      });
}

Task<Status> StorageEngine::Impl::ReturnColdBlocksLocal(
    std::size_t device_index, std::vector<std::uint64_t> block_ids) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(celer::ThisWorker().id == allocator.owner);
  co_await allocator.mutex.Lock();
  UnlockGuard unlock(&allocator.mutex, stores_[allocator.owner]->worker);
  if (allocator.failed.has_value()) {
    co_return *allocator.failed;
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
  Status persisted = co_await PersistBitmapPages(
      device_index, allocator, std::move(dirty_pages));
  if (!persisted.ok()) {
    allocator.failed = persisted;
    co_return persisted;
  }
  allocator.cold_free.insert(allocator.cold_free.end(), block_ids.begin(),
                             block_ids.end());
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::ReturnColdBlocks(std::vector<std::uint64_t> block_ids) {
  std::vector<std::vector<std::uint64_t>> by_device(devices_.size());
  for (const std::uint64_t block_id : block_ids) {
    by_device[DeviceIndexForBlock(block_id)].push_back(block_id);
  }
  for (std::size_t device_index = 0; device_index < by_device.size();
       ++device_index) {
    if (by_device[device_index].empty()) {
      continue;
    }
    const celer::WorkerId owner = device_allocators_[device_index]->owner;
    // Deliberately if/else, not a conditional expression: two co_awaits in
    // one full expression miscompile under GCC coroutines (branch awaiter
    // temporaries alias frame slots; destroying the suspended frame then
    // runs destructors on garbage).
    Status returned;
    if (owner == celer::ThisWorker().id) {
      returned = co_await ReturnColdBlocksLocal(
          device_index, std::move(by_device[device_index]));
    } else {
      returned = co_await celer::SubmitTaskTo(
          owner,
          [this, device_index,
           blocks = std::move(by_device[device_index])]() mutable
              -> Task<Status> {
            co_return co_await ReturnColdBlocksLocal(device_index,
                                                     std::move(blocks));
          });
    }
    if (!returned.ok()) {
      co_return returned;
    }
  }
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::PersistEpochValueOnDeviceLocal(
    std::size_t device_index, std::size_t value_index, std::uint64_t epoch) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(celer::ThisWorker().id == allocator.owner);
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return Status(StatusCode::kFailedPrecondition,
                     "epoch metadata writer is stopped after an IO failure");
  }
  if (value_index >= allocator.epoch_values.size()) {
    co_return Status(StatusCode::kOutOfRange,
                     "epoch metadata index is out of range");
  }
  co_await allocator.mutex.Lock();
  UnlockGuard allocator_unlock(&allocator.mutex,
                               stores_[allocator.owner]->worker);
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return Status(StatusCode::kFailedPrecondition,
                     "epoch metadata writer is stopped after an IO failure");
  }
  if (epoch < allocator.epoch_values[value_index]) {
    co_return Status::Ok();
  }
  const std::uint64_t desired = std::max(
      epoch, allocator.epoch_values[value_index]);
  if (allocator.durable_epoch_values[value_index] >= desired) {
    co_return Status::Ok();
  }
  const std::size_t byte_offset = value_index * sizeof(std::uint64_t);
  const std::size_t page_index = byte_offset / kMetadataPagePayloadBytes;
  const std::size_t page_byte_offset =
      page_index * kMetadataPagePayloadBytes;
  const std::size_t payload_bytes = std::min(
      kMetadataPagePayloadBytes, kEpochMetadataBytes - page_byte_offset);
  WorkerStore& store = *stores_[allocator.owner];
  auto acquired = co_await store.buffers.AcquireReadBuffer();
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size = kDirectIoAlignment;
  allocator.epoch_values[value_index] = desired;
  const MetadataPageState current = allocator.epoch_pages[page_index];
  const std::uint8_t next_slot = current.active_slot == 0 ? 1 : 0;
  const std::uint64_t next_generation = current.generation + 1;
  std::span<std::byte, kDirectIoAlignment> output(
      buffer.data, kDirectIoAlignment);
  EncodeMetadataPage(
      MetadataPageKind::kEpochs,
      static_cast<std::uint32_t>(page_index), next_generation,
      std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(
              allocator.epoch_values.data()) +
              page_byte_offset,
          payload_bytes),
      output);
  const StorageDevice& device = devices_[device_index];
  auto written = co_await WriteStorageBuffer(
      *store.worker, store.files[device.file_index], output,
      lease.registered(), buffer,
      MetadataPageSlotOffset(kEpochMetadataOffset, page_index, next_slot));
  if (!written.ok() || *written != kDirectIoAlignment) {
    epoch_metadata_failed_.store(true, std::memory_order_release);
    co_return written.ok()
                  ? Status(StatusCode::kInternal,
                           "short write of device epoch metadata")
                  : written.status();
  }
  Status synced = co_await celer::Fdatasync(
      *store.worker, store.files[device.file_index]);
  if (!synced.ok()) {
    epoch_metadata_failed_.store(true, std::memory_order_release);
    co_return synced;
  }
  allocator.epoch_pages[page_index] = MetadataPageState{
      .generation = next_generation,
      .active_slot = next_slot,
  };
  const std::size_t first_value =
      page_byte_offset / sizeof(std::uint64_t);
  const std::size_t value_count = payload_bytes / sizeof(std::uint64_t);
  for (std::size_t i = 0; i < value_count; ++i) {
    allocator.durable_epoch_values[first_value + i] =
        allocator.epoch_values[first_value + i];
  }
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::PersistEpochValue(std::size_t value_index,
                                                    std::uint64_t epoch) {
  if (value_index >= kEpochValueCount) {
    co_return Status(StatusCode::kOutOfRange,
                     "epoch metadata index is out of range");
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const celer::WorkerId owner =
        device_allocators_[device_index]->owner;
    Status persisted;
    if (owner == celer::ThisWorker().id) {
      persisted = co_await PersistEpochValueOnDeviceLocal(device_index,
                                                         value_index, epoch);
    } else {
      persisted = co_await celer::SubmitTaskTo(
          owner, [this, device_index, value_index, epoch]() -> Task<Status> {
            co_return co_await PersistEpochValueOnDeviceLocal(
                device_index, value_index, epoch);
          });
    }
    if (!persisted.ok()) {
      co_return persisted;
    }
  }
  co_return Status::Ok();
}

Task<StatusOr<ReservedBlock>> StorageEngine::Impl::AllocateBlock(
    WorkerStore& store, bool for_defrag) {
  const std::size_t device_count = devices_.size();
  std::vector<std::size_t> home_order(store.home_devices.size());
  for (std::size_t i = 0; i < home_order.size(); ++i) {
    home_order[i] = i;
  }
  std::sort(home_order.begin(), home_order.end(),
            [this, &store](std::size_t left, std::size_t right) {
              const long double left_score =
                  static_cast<long double>(
                      store.home_device_allocations[left] + 1) /
                  static_cast<long double>(ForegroundBlocksForDevice(
                      store.home_devices[left]));
              const long double right_score =
                  static_cast<long double>(
                      store.home_device_allocations[right] + 1) /
                  static_cast<long double>(ForegroundBlocksForDevice(
                      store.home_devices[right]));
              return left_score < right_score;
            });
  std::vector<std::size_t> attempt_order;
  attempt_order.reserve(device_count);
  std::vector<bool> included(device_count, false);
  for (const std::size_t home_index : home_order) {
    const std::size_t device_index = store.home_devices[home_index];
    attempt_order.push_back(device_index);
    included[device_index] = true;
  }
  for (std::size_t device_index = 0; device_index < device_count;
       ++device_index) {
    if (!included[device_index]) {
      attempt_order.push_back(device_index);
    }
  }
  while (true) {
    if (store.write_failed ||
        epoch_metadata_failed_.load(std::memory_order_acquire)) {
      co_return Status(StatusCode::kFailedPrecondition,
                       "storage writer is stopped after an IO failure");
    }
    const std::uint64_t generation_before =
        space_reclaim_generation_.load(std::memory_order_acquire);
    for (const std::size_t device_index : attempt_order) {
      auto allocated = co_await AllocateFromDevice(device_index, for_defrag);
      if (allocated.ok()) {
        auto home = std::find(store.home_devices.begin(),
                              store.home_devices.end(), device_index);
        if (home != store.home_devices.end()) {
          const std::size_t home_index =
              static_cast<std::size_t>(home - store.home_devices.begin());
          ++store.home_device_allocations[home_index];
        }
        co_return *allocated;
      }
      if (allocated.status().code() != StatusCode::kResourceExhausted) {
        co_return allocated.status();
      }
    }

    // Defrag allocations consume the protected reserve. Waiting for another
    // defrag from inside DefragOne would deadlock when the reserve is truly
    // exhausted, so only foreground allocation waits for reclaim progress.
    if (for_defrag) {
      co_return Status(StatusCode::kResourceExhausted,
                       "defrag reserve is exhausted");
    }

    const std::uint64_t generation_after =
        space_reclaim_generation_.load(std::memory_order_acquire);
    if (generation_after != generation_before) {
      continue;
    }
    if (active_defrags_.load(std::memory_order_acquire) != 0 ||
        pending_defrags_.load(std::memory_order_acquire) != 0 ||
        active_flushes_.load(std::memory_order_acquire) != 0 ||
        active_extent_reclaims_.load(std::memory_order_acquire) != 0) {
      Status waited = co_await celer::SleepFor(
          *store.worker, std::chrono::milliseconds(1));
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
                 " ready=" + std::to_string(allocator.ready_blocks.size()) +
                 " cold=" + std::to_string(allocator.cold_free.size()) +
                 " reserve=" + std::to_string(DefragReserveForDevice(d));
    }
    spdlog::warn(
        "allocator: out of disk space;{} flushes={} defrags={}/{} "
        "extent_reclaims={}",
        devices, active_flushes_.load(std::memory_order_relaxed),
        active_defrags_.load(std::memory_order_relaxed),
        pending_defrags_.load(std::memory_order_relaxed),
        active_extent_reclaims_.load(std::memory_order_relaxed));
    co_return Status(StatusCode::kResourceExhausted, "out of disk space");
  }
}

}  // namespace keylane::storage
