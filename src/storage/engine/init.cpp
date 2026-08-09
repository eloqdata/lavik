#include <thread>

#include "impl.h"

namespace keylane::storage {

absl::Status StorageEngine::Impl::Prepare(unsigned worker_count) {
  if (worker_count == 0 || options_.data_files.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "storage requires workers and at least one data file");
  }
  if (worker_count > kLogicalStorageShards) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "storage worker count exceeds logical storage shards");
  }
  if (options_.data_files.size() > std::numeric_limits<std::uint16_t>::max()) {
    return absl::Status(absl::StatusCode::kOutOfRange, "too many data files");
  }

  std::size_t direct_io_alignment = 1;
  std::vector<StoragePathInfo> path_info;
  path_info.reserve(options_.data_files.size());
  std::vector<std::optional<DeviceLabel>> labels;
  labels.reserve(options_.data_files.size());
  for (const std::string& path : options_.data_files) {
    auto probed = ProbeStoragePath(path);
    if (!probed.ok()) {
      return probed.status();
    }
    if (probed->size_bytes < 2 * kStorageBlockBytes) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "storage path is too small to hold metadata and data: " + path);
    }
    direct_io_alignment = std::max(direct_io_alignment, probed->io_alignment);
    path_info.push_back(*probed);

    auto label = ReadDeviceLabel(path);
    if (!label.ok()) {
      return label.status();
    }
    labels.push_back(std::move(*label));
  }

  std::uint64_t storage_set_id = 0;
  std::uint32_t expected_device_count = 0;
  bool has_existing_device = false;
  bool has_empty_device = false;
  absl::flat_hash_map<std::uint64_t, std::size_t> seen_device_ids;
  for (std::size_t i = 0; i < labels.size(); ++i) {
    if (!labels[i].has_value()) {
      has_empty_device = true;
      continue;
    }
    has_existing_device = true;
    const DeviceLabel& label = *labels[i];
    if (storage_set_id == 0) {
      storage_set_id = label.storage_set_id;
    } else if (storage_set_id != label.storage_set_id) {
      return absl::Status(
          absl::StatusCode::kFailedPrecondition,
          "configured devices belong to different storage sets");
    }
    if (expected_device_count == 0) {
      expected_device_count = label.device_count;
    } else if (expected_device_count != label.device_count) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "configured devices disagree on storage-set size");
    }
    if (!seen_device_ids.try_emplace(label.device_id, i).second) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "duplicate device id in configured storage files");
    }
  }

  if (has_existing_device && has_empty_device) {
    return absl::Status(
        absl::StatusCode::kFailedPrecondition,
        "mixing initialized and empty storage devices is not supported; "
        "online device-set expansion is not implemented");
  }
  if (has_existing_device) {
    if (expected_device_count != labels.size()) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "configured storage device count does not match the "
                          "persisted storage set; a device may be missing");
    }
    for (std::uint64_t id = 0; id < expected_device_count; ++id) {
      if (!seen_device_ids.contains(id)) {
        return absl::Status(absl::StatusCode::kFailedPrecondition,
                            "configured storage set is missing device id " +
                                std::to_string(id));
      }
    }
  } else {
    auto generated = RandomStorageSetId();
    if (!generated.ok()) {
      return generated.status();
    }
    storage_set_id = *generated;
    expected_device_count = static_cast<std::uint32_t>(labels.size());
  }

  devices_.clear();
  devices_.reserve(options_.data_files.size());
  std::vector<std::uint64_t> capacity_by_path(labels.size(), 0);
  for (std::size_t i = 0; i < labels.size(); ++i) {
    const StoragePathInfo& probed = path_info[i];
    std::uint64_t capacity_blocks = 0;
    std::uint64_t device_id = i;
    if (labels[i].has_value()) {
      const DeviceLabel& label = *labels[i];
      capacity_blocks = label.capacity_blocks;
      device_id = label.device_id;
      const std::uint64_t required_bytes = capacity_blocks * kStorageBlockBytes;
      if (probed.size_bytes < required_bytes) {
        return absl::Status(
            absl::StatusCode::kFailedPrecondition,
            "storage path is smaller than its persisted capacity: " +
                options_.data_files[i]);
      }
      if (probed.size_bytes > required_bytes) {
        spdlog::info(
            "storage path {} has {} trailing bytes beyond its persisted "
            "capacity; ignoring them",
            options_.data_files[i], probed.size_bytes - required_bytes);
      }
    } else {
      if (!probed.is_block_device &&
          probed.size_bytes % kStorageBlockBytes != 0) {
        return absl::Status(
            absl::StatusCode::kInvalidArgument,
            "new regular storage file size must be a multiple of 8 MiB: " +
                options_.data_files[i]);
      }
      capacity_blocks = probed.size_bytes / kStorageBlockBytes;
      if (capacity_blocks > kLocalBlockIdLimit) {
        return absl::Status(absl::StatusCode::kOutOfRange,
                            "each data file or device is limited to 1 PiB: " +
                                options_.data_files[i]);
      }
      const std::uint64_t ignored_bytes =
          probed.size_bytes - capacity_blocks * kStorageBlockBytes;
      if (ignored_bytes != 0) {
        spdlog::info(
            "block device {} has {} tail bytes outside a complete 8 MiB "
            "block; ignoring them",
            options_.data_files[i], ignored_bytes);
      }
    }
    const std::uint32_t data_block_begin = DataBlockBegin(capacity_blocks);
    if (capacity_blocks > kLocalBlockIdLimit) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "persisted device capacity exceeds the 1 PiB limit: " +
              options_.data_files[i]);
    }
    if (data_block_begin >= capacity_blocks) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "fixed metadata leaves no data blocks: " + options_.data_files[i]);
    }
    if (capacity_blocks - data_block_begin <= kDefragReserveBlocksPerDevice) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "storage path has no foreground block after its per-device "
          "defrag reserve; each device must be at least 80 MiB: " +
              options_.data_files[i]);
    }
    capacity_by_path[i] = capacity_blocks;
    devices_.push_back(StorageDevice{
        .path = options_.data_files[i],
        .id = device_id,
        .capacity_blocks = capacity_blocks,
        .data_block_begin = data_block_begin,
        .data_block_count = capacity_blocks - data_block_begin,
        .file_index = static_cast<std::uint32_t>(i),
    });
  }
  std::sort(devices_.begin(), devices_.end(),
            [](const StorageDevice& left, const StorageDevice& right) {
              return left.id < right.id;
            });

  ConfigureDefragReserves();
  active_defrags_by_device_ =
      std::make_unique<std::atomic<unsigned>[]>(devices_.size());
  defrag_ready_by_device_ =
      std::make_unique<moodycamel::ConcurrentQueue<std::uint16_t>[]>(
          devices_.size());
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    active_defrags_by_device_[device_index].store(0, std::memory_order_relaxed);
  }
  total_data_blocks_ = 0;
  std::uint64_t foreground_blocks = 0;
  // Sized once and never resized: BlockState pointers are held across
  // suspension points, so entries must not move.
  device_block_states_.resize(devices_.size());
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    device_block_states_[device_index] =
        std::vector<BlockState>(static_cast<std::size_t>(
            device.capacity_blocks - device.data_block_begin));
    total_data_blocks_ += device.data_block_count;
    const std::size_t reserve = DefragReserveForDevice(device_index);
    if (device.data_block_count > reserve) {
      foreground_blocks += device.data_block_count - reserve;
    }
  }
  if (foreground_blocks == 0) {
    return absl::Status(
        absl::StatusCode::kOutOfRange,
        "storage set has no foreground blocks after the defrag reserve; "
        "a single-device storage set must be at least 80 MiB");
  }

  for (std::size_t i = 0; i < labels.size(); ++i) {
    if (!labels[i].has_value()) {
      DeviceLabel label{
          .magic = kDeviceLabelMagic,
          .version = kStorageFormatVersion,
          .header_bytes = kDirectIoAlignment,
          .storage_set_id = storage_set_id,
          .device_id = i,
          .capacity_blocks = capacity_by_path[i],
          .device_count = expected_device_count,
          .block_bytes = kStorageBlockBytes,
      };
      absl::Status written = WriteDeviceLabel(options_.data_files[i], label);
      if (!written.ok()) {
        return written;
      }
      labels[i] = label;
    }
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    const std::size_t reserve = DefragReserveForDevice(device_index);
    spdlog::info(
        "storage device id={} path={} capacity-bytes={} data-blocks={} "
        "foreground-blocks={} defrag-reserve-blocks={} data-bytes={}",
        device.id, device.path, device.capacity_blocks * kStorageBlockBytes,
        device.data_block_count, device.data_block_count - reserve, reserve,
        device.data_block_count * kStorageBlockBytes);
  }
  spdlog::info(
      "storage capacity: data-blocks={} foreground-blocks={} "
      "defrag-reserve-blocks={}",
      total_data_blocks_, foreground_blocks,
      total_data_blocks_ - foreground_blocks);
  direct_io_alignment_ = direct_io_alignment;
  if (options_.flush_size_bytes < direct_io_alignment_ ||
      options_.flush_size_bytes > kStorageBlockBytes ||
      (options_.flush_size_bytes & (options_.flush_size_bytes - 1)) != 0 ||
      options_.flush_size_bytes % direct_io_alignment_ != 0) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "flush size must be a power of two between the direct-I/O alignment "
        "and the 8 MiB storage block size");
  }
  spdlog::info("storage direct-I/O alignment={} bytes", direct_io_alignment_);
  spdlog::info("storage flush submission size={} bytes",
               options_.flush_size_bytes);

  worker_count_ = worker_count;
  epoch_values_.assign(kEpochValueCount, 1);
  device_allocators_.clear();
  device_allocators_.reserve(devices_.size());
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    const std::size_t bitmap_bytes = ScanBitmapBytes(device.capacity_blocks);
    const std::size_t bitmap_page_count =
        ScanBitmapPageCount(device.capacity_blocks);
    auto allocator = std::make_unique<DeviceAllocator>();
    allocator->owner =
        static_cast<celer::WorkerId>(device_index % worker_count_);
    allocator->data_block_begin = device.data_block_begin;
    allocator->next_pristine = device.data_block_begin;
    allocator->scan_bitmap.resize(bitmap_bytes, std::byte{0});
    allocator->bitmap_pages.resize(bitmap_page_count);
    allocator->epoch_pages.resize(kEpochMetadataPageCount);
    allocator->epoch_values.assign(kEpochValueCount, 1);
    allocator->durable_epoch_values.assign(kEpochValueCount, 1);

    const int fd = ::open(device.path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) {
      return absl::Status(absl::StatusCode::kInternal,
                          "open fixed metadata failed: " + device.path + ": " +
                              std::strerror(errno));
    }
    absl::Status load_status = absl::OkStatus();
    for (std::size_t page_index = 0; page_index < kEpochMetadataPageCount;
         ++page_index) {
      const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
      const std::size_t payload_bytes = std::min(
          kMetadataPagePayloadBytes, kEpochMetadataBytes - byte_offset);
      auto loaded = ReadMetadataPagePair(
          fd, kEpochMetadataOffset, MetadataPageKind::kEpochs,
          static_cast<std::uint32_t>(page_index), payload_bytes);
      if (!loaded.ok()) {
        load_status = loaded.status();
        break;
      }
      allocator->epoch_pages[page_index] = loaded->state;
      const std::size_t first_value = byte_offset / sizeof(std::uint64_t);
      const std::size_t value_count = payload_bytes / sizeof(std::uint64_t);
      for (std::size_t value_index = 0; value_index < value_count;
           ++value_index) {
        std::uint64_t value = 0;
        std::memcpy(
            &value,
            loaded->payload.data() + value_index * sizeof(std::uint64_t),
            sizeof(value));
        value = std::max<std::uint64_t>(value, 1);
        allocator->durable_epoch_values[first_value + value_index] = value;
        epoch_values_[first_value + value_index] =
            std::max(epoch_values_[first_value + value_index], value);
      }
    }
    for (std::size_t page_index = 0;
         load_status.ok() && page_index < bitmap_page_count; ++page_index) {
      const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
      const std::size_t payload_bytes =
          std::min(kMetadataPagePayloadBytes, bitmap_bytes - byte_offset);
      auto loaded = ReadMetadataPagePair(
          fd, kScanBitmapMetadataOffset, MetadataPageKind::kScanBitmap,
          static_cast<std::uint32_t>(page_index), payload_bytes);
      if (!loaded.ok()) {
        load_status = loaded.status();
        break;
      }
      allocator->bitmap_pages[page_index] = loaded->state;
      std::memcpy(allocator->scan_bitmap.data() + byte_offset,
                  loaded->payload.data(), payload_bytes);
    }
    const int close_error = ::close(fd);
    if (!load_status.ok()) {
      return absl::Status(
          load_status.code(),
          std::string(load_status.message()) + ": " + device.path);
    }
    if (close_error != 0) {
      return absl::Status(absl::StatusCode::kInternal,
                          "close fixed metadata failed: " + device.path);
    }

    for (std::uint64_t local = device.capacity_blocks;
         local-- > device.data_block_begin;) {
      const std::size_t byte_index = static_cast<std::size_t>(local / 8);
      const unsigned bit_index = static_cast<unsigned>(local % 8);
      if ((std::to_integer<unsigned>(allocator->scan_bitmap[byte_index]) &
           (1U << bit_index)) != 0) {
        allocator->next_pristine = local + 1;
        break;
      }
    }
    for (std::uint64_t local = device.data_block_begin;
         local < allocator->next_pristine; ++local) {
      const std::size_t byte_index = static_cast<std::size_t>(local / 8);
      const unsigned bit_index = static_cast<unsigned>(local % 8);
      if ((std::to_integer<unsigned>(allocator->scan_bitmap[byte_index]) &
           (1U << bit_index)) == 0) {
        allocator->cold_free.push_back(
            MakeBlockId(device.id, static_cast<std::uint32_t>(local)));
      }
    }
    for (std::uint64_t local = device.data_block_begin;
         local < device.capacity_blocks; ++local) {
      const std::size_t byte_index = static_cast<std::size_t>(local / 8);
      const unsigned bit_index = static_cast<unsigned>(local % 8);
      if ((std::to_integer<unsigned>(allocator->scan_bitmap[byte_index]) &
           (1U << bit_index)) != 0) {
        ++recovery_allocated_blocks_;
      }
    }
    device_allocators_.push_back(std::move(allocator));
  }
  // Runtime device owners start from the canonical component-wise maximum.
  // durable_epoch_values retains what each device actually contained, so a
  // later update to the same page also repairs stale mirror fields.
  for (auto& allocator : device_allocators_) {
    allocator->epoch_values = epoch_values_;
  }
  recovery_device_cursors_ =
      std::make_unique<RecoveryDeviceCursor[]>(devices_.size());
  for (std::size_t i = 0; i < devices_.size(); ++i) {
    recovery_device_cursors_[i].next_local.store(
        device_allocators_[i]->next_pristine, std::memory_order_relaxed);
    recovery_device_cursors_[i].next_allocation_epoch.store(
        1, std::memory_order_relaxed);
  }
  const auto recovery_start = std::chrono::steady_clock::now();
  recovery_started_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                             recovery_start.time_since_epoch())
                             .count();
  recovery_next_log_ms_.store(recovery_started_ms_ + 5000,
                              std::memory_order_relaxed);
  stores_.reserve(worker_count);
  for (unsigned i = 0; i < worker_count; ++i) {
    stores_.push_back(std::make_unique<WorkerStore>());
    WorkerStore& store = *stores_.back();
    store.partitions.reserve((kLogicalStorageShards + worker_count - 1 - i) /
                             worker_count);
    for (std::uint32_t partition = i; partition < kLogicalStorageShards;
         partition += worker_count) {
      store.partitions.emplace_back();
      store.partitions.back().id = static_cast<std::uint16_t>(partition);
      store.partitions.back().replication_epoch =
          epoch_values_[kLogicalDatabaseCount + partition];
    }
  }
  ConfigureWorkerDeviceAffinity();
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    db_epochs_[db_id].store(epoch_values_[db_id], std::memory_order_relaxed);
  }
  open_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  metadata_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  recovery_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  recovery_accounting_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  free_list_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::InitializeWorker(Worker& worker) {
  WorkerStore& store = *stores_[worker.id()];
  store.worker = &worker;

  absl::Status status = store.buffers.Init(worker, options_.buffers);
  if (status.ok()) {
    status = worker.RegisterFixedFiles(
        static_cast<unsigned>(options_.data_files.size()));
  }
  if (status.ok()) {
    store.files.reserve(options_.data_files.size());
    for (std::size_t i = 0; i < options_.data_files.size(); ++i) {
      FixedFile file{.index = static_cast<std::uint32_t>(i)};
      status = co_await celer::OpenFixedFile(worker, options_.data_files[i],
                                             O_RDWR | O_DIRECT, 0, file);
      if (!status.ok()) {
        break;
      }
      store.files.push_back(file);
    }
  }
  if (!status.ok()) {
    Fail(status);
    co_return status;
  }

  status = co_await open_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }

  status = co_await metadata_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }

  std::vector<RecoveryBatch> batches(worker_count_);
  std::vector<std::uint64_t> zero_blocks;
  absl::flat_hash_set<std::uint64_t> committed_txids;
  status = co_await ScanAssignedBlocks(store, &batches, &zero_blocks,
                                       &committed_txids);
  if (!status.ok()) {
    Fail(status);
    co_return status;
  }
  if (!committed_txids.empty()) {
    std::lock_guard<std::mutex> lock(recovery_committed_mutex_);
    recovery_committed_txids_.merge(committed_txids);
  }

  for (unsigned target = 0; target < worker_count_; ++target) {
    if (batches[target].blocks.empty() && batches[target].records.empty()) {
      continue;
    }
    if (target == worker.id()) {
      ApplyRecovery(target, std::move(batches[target]));
    } else {
      absl::Status apply = co_await celer::SubmitTo(
          target, [this, target, batch = std::move(batches[target])]() mutable {
            ApplyRecovery(target, std::move(batch));
            return absl::OkStatus();
          });
      if (!apply.ok()) {
        Fail(apply);
        co_return apply;
      }
    }
  }

  status = co_await recovery_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }

  // Every scan has fed the committed set by now (merges happen before the
  // barrier); decide the parked transaction-tagged records. A tagged record
  // without its commit record is a prepare whose transaction never durably
  // committed — recovery drops it, which is exactly the all-or-nothing the
  // commit protocol promises.
  for (const RecoveryRecord& parked : store.recovery_tx_records) {
    if (recovery_committed_txids_.contains(parked.txid)) {
      ApplyRecoveredRecord(store, parked);
    }
  }
  store.recovery_tx_records.clear();
  store.recovery_tx_records.shrink_to_fit();
  if (worker.id() == 0) {
    // Seed the transaction-id counter above everything on disk so a new
    // boot's transactions can never alias a previous boot's commit records.
    tx::TxRuntime::Get()->next_txid.store(
        std::max<std::uint64_t>(
            recovery_max_txid_.load(std::memory_order_relaxed) + 1, 1),
        std::memory_order_relaxed);
  }

  std::vector<std::vector<RecoveryLiveReference>> live_by_owner(worker_count_);
  for (auto& partition : store.partitions) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      partition.indexes[db_id].ForEach([&](const RecordIndex::Entry& entry) {
        const RecordLocation& location = entry.value;
        assert(location.block_owner < worker_count_);
        live_by_owner[location.block_owner].push_back(RecoveryLiveReference{
            .block_id = location.block_id,
            .allocation_epoch = location.allocation_epoch,
            .bytes = location.total_disk_bytes,
        });
        if (location.external && location.extents != nullptr) {
          for (std::size_t extent_index = 0;
               extent_index < location.extents->size(); ++extent_index) {
            const ExtentRef& extent = location.extents->at(extent_index);
            // An extent block's owner is derived from its own block id,
            // so it is unrelated to the owner of the block holding this
            // manifest. Charging the reference to the record's owner
            // sends it to a worker that has no state for the block,
            // which reads as corruption and fails recovery outright.
            const std::uint16_t extent_owner = BlockOwner(extent.block_id);
            if (extent_owner >= worker_count_) {
              Fail(absl::Status(absl::StatusCode::kInternal,
                                "manifest references an unscanned extent"));
              return;
            }
            live_by_owner[extent_owner].push_back(RecoveryLiveReference{
                .block_id = extent.block_id,
                .allocation_epoch = extent.allocation_epoch,
                .bytes = extent.payload_bytes,
                .extent = true,
                .extent_index = static_cast<std::uint32_t>(extent_index),
                .extent_payload_checksum = extent.payload_checksum,
            });
          }
        }
      });
    }
  }
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    if (live_by_owner[owner].empty()) {
      continue;
    }
    auto apply_live = [this, owner,
                       references = std::move(live_by_owner[owner])]() mutable {
      WorkerStore& owner_store = *stores_[owner];
      for (const RecoveryLiveReference& reference : references) {
        BlockState* state = FindBlockState(owner_store, reference.block_id);
        if (state == nullptr || !state->allocated ||
            state->allocation_epoch != reference.allocation_epoch) {
          return absl::Status(absl::StatusCode::kInternal,
                              "recovery live reference has no owning block");
        }
        if (reference.extent) {
          const auto found =
              owner_store.recovered_extents.find(reference.block_id);
          if (state->kind != BlockKind::kValueExtent ||
              state->committed_bytes != kBlockHeaderBytes + reference.bytes ||
              found == owner_store.recovered_extents.end() ||
              found->second.extent_index != reference.extent_index ||
              found->second.payload_checksum !=
                  reference.extent_payload_checksum) {
            return absl::Status(absl::StatusCode::kInternal,
                                "live extent header does not match manifest");
          }
        }
        state->live_bytes += reference.bytes;
      }
      return absl::OkStatus();
    };
    // if/else, not ?:, to keep the co_await out of a conditional
    // expression (GCC coroutine frame-slot aliasing).
    absl::Status applied = absl::OkStatus();
    if (owner == worker.id()) {
      applied = apply_live();
    } else {
      applied = co_await celer::SubmitTo(owner, std::move(apply_live));
    }
    if (!applied.ok()) {
      Fail(applied);
      co_return applied;
    }
  }

  status = co_await recovery_accounting_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }
  // Every worker's live-reference pass has run, so no manifest still needs
  // to be matched against a recovered extent header.
  store.recovered_extents.clear();

  std::vector<std::vector<std::uint64_t>> free_by_device(devices_.size());
  for (std::uint64_t block_id : zero_blocks) {
    const std::size_t device_index = DeviceIndexForBlock(block_id);
    const std::uint64_t pristine =
        recovery_device_cursors_[device_index].next_local.load(
            std::memory_order_acquire);
    if (LocalBlockId(block_id) < pristine) {
      free_by_device[device_index].push_back(block_id);
    }
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const celer::WorkerId allocator_owner =
        device_allocators_[device_index]->owner;
    auto apply_recovery_free =
        [this, device_index,
         blocks = std::move(free_by_device[device_index])]() mutable {
          DeviceAllocator& allocator = *device_allocators_[device_index];
          allocator.next_pristine =
              std::max(allocator.next_pristine,
                       recovery_device_cursors_[device_index].next_local.load(
                           std::memory_order_acquire));
          allocator.next_allocation_epoch = std::max(
              allocator.next_allocation_epoch,
              recovery_device_cursors_[device_index].next_allocation_epoch.load(
                  std::memory_order_acquire));
          allocator.ready_blocks.insert(allocator.ready_blocks.end(),
                                        std::make_move_iterator(blocks.begin()),
                                        std::make_move_iterator(blocks.end()));
          return absl::OkStatus();
        };
    // if/else, not ?:, to keep the co_await out of a conditional
    // expression (GCC coroutine frame-slot aliasing).
    if (allocator_owner == worker.id()) {
      status = apply_recovery_free();
    } else {
      status = co_await celer::SubmitTo(allocator_owner,
                                        std::move(apply_recovery_free));
    }
    if (!status.ok()) {
      Fail(status);
      co_return status;
    }
  }
  status = co_await free_list_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }
  auto orphan_extents = std::make_shared<std::vector<ExtentRef>>();
  ForEachOwnedBlock(store, [&](std::uint64_t block_id, BlockState& state) {
    if (state.kind == BlockKind::kValueExtent && state.live_bytes == 0) {
      orphan_extents->push_back(ExtentRef{
          .block_id = block_id,
          .allocation_epoch = state.allocation_epoch,
          .payload_bytes = static_cast<std::uint32_t>(state.committed_bytes -
                                                      kBlockHeaderBytes),
          .payload_checksum = 0,
      });
    } else {
      MaybeQueueDefrag(store, block_id);
    }
  });
  if (!orphan_extents->empty()) {
    SpawnExtentReclaim(store, std::shared_ptr<const std::vector<ExtentRef>>(
                                  std::move(orphan_extents)));
  }
  worker.SpawnRoot(PeriodicFlush(&store));
  if (options_.expiration_authority) {
    worker.SpawnBackground(ActiveExpiration(&store));
    // One coordinator drives the whole-engine round; worker 0 hosts it.
    if (worker.id() == 0 && options_.tomb_raider_interval_ms != 0) {
      worker.SpawnBackground(TombRaiderLoop(&store));
    }
  }
  co_return absl::OkStatus();
}

absl::Status StorageEngine::Impl::FlushForShutdown() {
  // Give in-flight commit chains a chance to append their commit records
  // before the flush order freezes the append streams: an acknowledged
  // multi-key write whose commit misses the shutdown flush is dropped whole
  // at recovery. Bounded — a stuck chain costs only its own transaction,
  // never the shutdown. Runs on the shutdown thread, not a worker.
  const auto commit_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (active_tx_commits_.load(std::memory_order_acquire) != 0 &&
         std::chrono::steady_clock::now() < commit_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  shutdown_flush_requested_.store(true, std::memory_order_release);
  unsigned completed =
      shutdown_flush_completed_.load(std::memory_order_acquire);
  while (completed < worker_count_) {
    shutdown_flush_completed_.wait(completed, std::memory_order_acquire);
    completed = shutdown_flush_completed_.load(std::memory_order_acquire);
  }
  if (shutdown_flush_failed_.load(std::memory_order_acquire)) {
    return absl::Status(absl::StatusCode::kInternal,
                        "one or more workers failed to flush during shutdown");
  }
  return absl::OkStatus();
}

void StorageEngine::Impl::Fail(const absl::Status& status) {
  open_barrier_->Abort(status);
  metadata_barrier_->Abort(status);
  recovery_barrier_->Abort(status);
  recovery_accounting_barrier_->Abort(status);
  free_list_barrier_->Abort(status);
}

void StorageEngine::Impl::ConfigureWorkerDeviceAffinity() {
  std::vector<std::size_t> usable_devices;
  std::uint64_t total_weight = 0;
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const std::uint64_t weight = ForegroundBlocksForDevice(device_index);
    if (weight != 0) {
      usable_devices.push_back(device_index);
      total_weight += weight;
    }
  }
  assert(!usable_devices.empty());

  if (worker_count_ >= usable_devices.size()) {
    std::vector<unsigned> quotas(devices_.size(), 0);
    for (const std::size_t device_index : usable_devices) {
      quotas[device_index] = 1;
    }
    const unsigned remaining_workers =
        worker_count_ - static_cast<unsigned>(usable_devices.size());
    std::vector<std::pair<std::uint64_t, std::size_t>> remainders;
    remainders.reserve(usable_devices.size());
    unsigned assigned_workers = static_cast<unsigned>(usable_devices.size());
    for (const std::size_t device_index : usable_devices) {
      const std::uint64_t weighted =
          remaining_workers * ForegroundBlocksForDevice(device_index);
      quotas[device_index] += static_cast<unsigned>(weighted / total_weight);
      assigned_workers += static_cast<unsigned>(weighted / total_weight);
      remainders.emplace_back(weighted % total_weight, device_index);
    }
    std::sort(remainders.begin(), remainders.end(),
              [](const auto& left, const auto& right) {
                if (left.first != right.first) {
                  return left.first > right.first;
                }
                return left.second < right.second;
              });
    for (unsigned i = assigned_workers; i < worker_count_; ++i) {
      ++quotas[remainders[i - assigned_workers].second];
    }

    std::vector<unsigned> quota_remaining = quotas;
    std::vector<std::int64_t> smooth_current(devices_.size(), 0);
    for (unsigned worker = 0; worker < worker_count_; ++worker) {
      std::size_t selected = usable_devices.front();
      bool selected_valid = false;
      for (const std::size_t device_index : usable_devices) {
        smooth_current[device_index] += quotas[device_index];
        if (quota_remaining[device_index] != 0 &&
            (!selected_valid ||
             smooth_current[device_index] > smooth_current[selected])) {
          selected = device_index;
          selected_valid = true;
        }
      }
      assert(selected_valid);
      smooth_current[selected] -= worker_count_;
      --quota_remaining[selected];
      stores_[worker]->home_devices.push_back(selected);
    }
  } else {
    std::sort(usable_devices.begin(), usable_devices.end(),
              [this](std::size_t left, std::size_t right) {
                return ForegroundBlocksForDevice(left) >
                       ForegroundBlocksForDevice(right);
              });
    std::vector<std::uint64_t> worker_weights(worker_count_, 0);
    for (const std::size_t device_index : usable_devices) {
      const auto lightest =
          std::min_element(worker_weights.begin(), worker_weights.end());
      const unsigned worker =
          static_cast<unsigned>(lightest - worker_weights.begin());
      stores_[worker]->home_devices.push_back(device_index);
      *lightest += ForegroundBlocksForDevice(device_index);
    }
  }

  std::vector<unsigned> home_workers(devices_.size(), 0);
  for (auto& store : stores_) {
    assert(!store->home_devices.empty());
    store->home_device_allocations.assign(store->home_devices.size(), 0);
    for (const std::size_t device_index : store->home_devices) {
      ++home_workers[device_index];
    }
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    spdlog::info("storage device id={} foreground-weight={} home-workers={}",
                 devices_[device_index].id,
                 ForegroundBlocksForDevice(device_index),
                 home_workers[device_index]);
  }
}

Task<absl::Status> StorageEngine::Impl::FlushWorkerForShutdown(
    WorkerStore* store) {
  while (active_defrags_.load(std::memory_order_acquire) != 0) {
    absl::Status status =
        co_await celer::SleepFor(*store->worker, std::chrono::milliseconds(1));
    if (!status.ok()) {
      co_return status;
    }
  }

  co_await store->store_state_mutex.Lock();
  {
    UnlockGuard guard(&store->store_state_mutex, store->worker);
    SealActiveBlocks(*store);
  }

  while (true) {
    co_await store->store_state_mutex.Lock();
    bool done = false;
    bool failed = false;
    {
      UnlockGuard guard(&store->store_state_mutex, store->worker);
      // Extent reclaims are detached and hop to whichever worker owns the
      // device allocator, so one can still be mid-flight across workers
      // here. Draining the flush queue is not enough: flush completion is
      // itself what spawns them, and letting a worker tear down under one
      // frees the coroutine frame it is running on.
      done = !store->flush_running && store->flush_queue.empty() &&
             active_extent_reclaims_.load(std::memory_order_acquire) == 0 &&
             active_settlements_.load(std::memory_order_acquire) == 0;
      failed = store->write_failed;
    }
    if (failed) {
      co_return absl::Status(
          absl::StatusCode::kInternal,
          "storage write failed while draining shutdown buffers");
    }
    if (done) {
      co_return absl::OkStatus();
    }
    absl::Status status =
        co_await celer::SleepFor(*store->worker, std::chrono::milliseconds(1));
    if (!status.ok()) {
      co_return status;
    }
  }
}

void StorageEngine::Impl::CompleteShutdownFlush(const absl::Status& status) {
  if (!status.ok()) {
    shutdown_flush_failed_.store(true, std::memory_order_release);
  }
  shutdown_flush_completed_.fetch_add(1, std::memory_order_acq_rel);
  shutdown_flush_completed_.notify_all();
}

}  // namespace keylane::storage
