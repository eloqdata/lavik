#include <cstring>

#include "impl.h"

namespace keylane::storage {
namespace {

constexpr std::uint64_t kCheckpointChunkMagic =
    0x314b4e5548434c4bULL;  // KLCHUNK1
constexpr std::uint64_t kCheckpointEntryMagic =
    0x315952544e454c4bULL;  // KLENTRY1
constexpr std::uint32_t kCheckpointWireVersion = 1;

enum CheckpointEntryFlag : std::uint8_t {
  kExternal = 1U << 0,
  kKeyExternal = 1U << 1,
  kShielding = 1U << 2,
  kUnclaimed = 1U << 3,
};

struct CheckpointChunkHeader {
  std::uint64_t magic_ = kCheckpointChunkMagic;
  std::uint64_t generation_ = 0;
  std::uint32_t version_ = kCheckpointWireVersion;
  std::uint32_t header_bytes_ = sizeof(CheckpointChunkHeader);
  std::uint32_t shard_id_ = 0;
  std::uint32_t entry_count_ = 0;
};

struct CheckpointEntryHeader {
  std::uint64_t magic_ = kCheckpointEntryMagic;
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint64_t mutation_sequence_ = 0;
  std::uint64_t expire_at_ms_ = 0;
  std::uint64_t replication_epoch_ = 0;
  std::uint32_t header_bytes_ = sizeof(CheckpointEntryHeader);
  std::uint32_t entry_bytes_ = 0;
  std::uint32_t key_bytes_ = 0;
  std::uint32_t logical_size_ = 0;
  std::uint32_t record_offset_ = 0;
  std::uint32_t total_disk_bytes_ = 0;
  std::uint32_t extent_count_ = 0;
  std::uint16_t block_owner_ = 0;
  std::uint8_t db_id_ = 0;
  RecordKind kind_ = RecordKind::kValue;
  ValueType value_type_ = ValueType::kNone;
  std::uint8_t flags_ = 0;
  std::uint16_t reserved_ = 0;
};

static_assert(std::is_trivially_copyable_v<CheckpointChunkHeader>);
static_assert(std::is_trivially_copyable_v<CheckpointEntryHeader>);

template <typename T>
void AppendPod(std::vector<std::byte>* output, const T& value) {
  const std::size_t offset = output->size();
  output->resize(offset + sizeof(value));
  std::memcpy(output->data() + offset, &value, sizeof(value));
}

bool CheckpointBlockAllocated(
    const std::vector<std::unique_ptr<DeviceAllocator>>& allocators,
    std::uint64_t block_id) {
  const std::size_t device_index = DeviceIdForBlock(block_id);
  if (device_index >= allocators.size()) return false;
  const DeviceAllocator& allocator = *allocators[device_index];
  const std::uint32_t local = LocalBlockId(block_id);
  if (local < allocator.data_block_begin_ ||
      local / 8 >= allocator.scan_bitmap_.size()) {
    return false;
  }
  return (std::to_integer<unsigned>(allocator.scan_bitmap_[local / 8]) &
          (1U << (local % 8))) != 0;
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::PersistCheckpointRootOnDeviceLocal(
    std::size_t device_index, const CheckpointRoot& root) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(celer::ThisWorker().id_ == allocator.owner_);
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::FailedPreconditionError(
        "checkpoint-root metadata writer is stopped after an IO failure");
  }
  co_await allocator.mutex_.Lock();
  UnlockGuard unlock(&allocator.mutex_, stores_[allocator.owner_]->worker_);
  if (epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::FailedPreconditionError(
        "checkpoint-root metadata writer is stopped after an IO failure");
  }

  const std::array<std::uint64_t, 4> values{
      root.generation_, root.consumed_generation_, root.block_count_,
      root.entry_count_};
  for (std::size_t i = 0; i < values.size(); ++i) {
    allocator.epoch_values_[kCheckpointGenerationIndex + i] = values[i];
  }
  const std::size_t byte_offset =
      kCheckpointGenerationIndex * sizeof(std::uint64_t);
  const std::size_t page_index = byte_offset / kMetadataPagePayloadBytes;
  const std::size_t page_byte_offset = page_index * kMetadataPagePayloadBytes;
  const std::size_t payload_bytes = std::min(
      kMetadataPagePayloadBytes, kEpochMetadataBytes - page_byte_offset);
  WorkerStore& store = *stores_[allocator.owner_];
  auto acquired = co_await store.buffers_.AcquireReadBuffer();
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size_ = kDirectIoAlignment;
  const MetadataPageState current = allocator.epoch_pages_[page_index];
  const std::uint8_t next_slot = current.active_slot_ == 0 ? 1 : 0;
  const std::uint64_t next_page_generation = current.generation_ + 1;
  std::span<std::byte, kDirectIoAlignment> output(buffer.data_,
                                                  kDirectIoAlignment);
  EncodeMetadataPage(
      MetadataPageKind::kEpochs, static_cast<std::uint32_t>(page_index),
      next_page_generation,
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
    co_return written.ok()
        ? absl::InternalError("short checkpoint-root metadata write")
        : written.status();
  }
  absl::Status synced = co_await celer::Fdatasync(
      *store.worker_, store.files_[device.file_index_]);
  if (!synced.ok()) {
    epoch_metadata_failed_.store(true, std::memory_order_release);
    co_return synced;
  }
  allocator.epoch_pages_[page_index] = {.generation_ = next_page_generation,
                                        .active_slot_ = next_slot};
  const std::size_t first = page_byte_offset / sizeof(std::uint64_t);
  const std::size_t count = payload_bytes / sizeof(std::uint64_t);
  for (std::size_t i = 0; i < count; ++i) {
    allocator.durable_epoch_values_[first + i] =
        allocator.epoch_values_[first + i];
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PersistCheckpointRoot(
    const CheckpointRoot& root) {
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const celer::WorkerId owner = device_allocators_[device_index]->owner_;
    absl::Status status;
    if (owner == celer::ThisWorker().id_) {
      status = co_await PersistCheckpointRootOnDeviceLocal(device_index, root);
    } else {
      status = co_await celer::SubmitTaskTo(
          owner, [this, device_index, root]() -> Task<absl::Status> {
            co_return co_await PersistCheckpointRootOnDeviceLocal(device_index,
                                                                  root);
          });
    }
    if (!status.ok()) co_return status;
  }
  checkpoint_root_ = root;
  epoch_values_[kCheckpointGenerationIndex] = root.generation_;
  epoch_values_[kCheckpointConsumedGenerationIndex] = root.consumed_generation_;
  epoch_values_[kCheckpointBlockCountIndex] = root.block_count_;
  epoch_values_[kCheckpointEntryCountIndex] = root.entry_count_;
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::WriteCheckpointBlock(
    WorkerStore& store, std::uint64_t generation, std::uint32_t shard_id,
    std::uint32_t record_count, std::span<const std::byte> payload) {
  if (payload.empty() || payload.size() > kExtentPayloadBytes) {
    co_return absl::InvalidArgumentError("invalid checkpoint block payload");
  }
  auto reserved = co_await AllocateBlock(store, AllocationPurpose::kCheckpoint);
  if (!reserved.ok()) co_return reserved.status();
  auto* data = static_cast<std::byte*>(celer::AllocateStorageBuffer(
      kStorageBlockBytes, options_.buffers_.alignment_));
  if (data == nullptr) {
    co_return absl::ResourceExhaustedError(
        "failed to allocate checkpoint write buffer");
  }
  std::fill_n(data, kStorageBlockBytes, std::byte{0});
  std::memcpy(data + kBlockHeaderBytes, payload.data(), payload.size());
  const std::uint32_t checksum = Crc32c(payload);
  BlockHeader header{
      .magic_ = kBlockMagic,
      .block_id_ = reserved->block_id_,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = kBlockHeaderBytes,
      .block_bytes_ = kStorageBlockBytes,
      .writer_id_ = store.worker_->id(),
      .allocation_epoch_ = reserved->allocation_epoch_,
      .committed_bytes_ =
          static_cast<std::uint32_t>(kBlockHeaderBytes + payload.size()),
      .record_count_ = record_count,
      .max_lsn_ = 0,
      .header_sequence_ = 1,
      .checksum_ = 0,
      .layout_worker_count_ = worker_count_,
      .kind_ = BlockKind::kCheckpointIndex,
      .reserved_ = {},
      .extent_index_ = shard_id,
      .extent_payload_bytes_ = static_cast<std::uint32_t>(payload.size()),
      .extent_payload_checksum_ = checksum,
      .reserved_runtime_ = {},
      .tx_generation_ = generation,
  };
  EncodeBlockHeader(header, std::span<std::byte, kBlockHeaderSlotBytes>(
                                data, kBlockHeaderSlotBytes));
  const auto [file_id, block_offset] = FileOffset(reserved->block_id_);
  const std::size_t write_bytes =
      AlignDirect(kBlockHeaderBytes + payload.size());
  auto written = co_await WriteStorageBuffer(
      *store.worker_, store.files_[file_id],
      std::span<const std::byte>(data, write_bytes), false, {}, block_offset);
  absl::Status status =
      written.ok() && *written == write_bytes
          ? absl::OkStatus()
          : (written.ok() ? absl::InternalError("short checkpoint block write")
                          : written.status());
  if (status.ok()) {
    status = co_await celer::Fdatasync(*store.worker_, store.files_[file_id]);
  }
  celer::FreeStorageBuffer(data, options_.buffers_.alignment_);
  if (!status.ok()) co_return status;
  co_return reserved->block_id_;
}

Task<absl::Status> StorageEngine::Impl::BuildShutdownCheckpointShard(
    WorkerStore& store, std::uint64_t generation) {
  CheckpointShardResult& result = checkpoint_shards_[store.worker_->id()];
  result = {};
  std::vector<std::byte> payload;
  payload.reserve(kExtentPayloadBytes);
  CheckpointChunkHeader chunk{
      .generation_ = generation,
      .shard_id_ = static_cast<std::uint32_t>(store.worker_->id())};
  AppendPod(&payload, chunk);
  std::uint32_t chunk_entries = 0;

  auto flush_chunk = [this, &store, generation, &result, &payload,
                      &chunk_entries]() -> Task<absl::Status> {
    auto* header = reinterpret_cast<CheckpointChunkHeader*>(payload.data());
    header->entry_count_ = chunk_entries;
    auto written = co_await WriteCheckpointBlock(
        store, generation, store.worker_->id(), chunk_entries, payload);
    if (!written.ok()) co_return written.status();
    result.blocks_.push_back(*written);
    payload.clear();
    CheckpointChunkHeader next{
        .generation_ = generation,
        .shard_id_ = static_cast<std::uint32_t>(store.worker_->id())};
    AppendPod(&payload, next);
    chunk_entries = 0;
    co_return absl::OkStatus();
  };

  for (WorkerStore::PartitionStore& partition : store.partitions_) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      RecordIndex& index = partition.indexes_[db_id];
      RecordIndex::StableScanCursor cursor;
      bool exhausted = false;
      while (!exhausted) {
        RecordIndex::Entry* entry = nullptr;
        exhausted = index.ScanStableWhile(
            &cursor, [&entry](RecordIndex::Entry& candidate) {
              entry = &candidate;
              return false;
            });
        if (entry == nullptr) continue;
        const RecordLocation location = MaterializeIndexLocation(*entry);
        if (location.tx_tagged()) {
          co_return absl::FailedPreconditionError(
              "shutdown transaction cleanup left a transaction-tagged index "
              "winner");
        }
        const ExtentManifest extents = ExtentsFor(store, entry);
        std::string key;
        if (entry->key_complete()) {
          key.assign(entry->key());
        } else {
          auto loaded = co_await LoadOutOfIndexKey(store, location, extents,
                                                   entry->logical_key_size());
          if (!loaded.ok()) co_return loaded.status();
          key = std::move(*loaded);
        }
        if (key.size() > std::numeric_limits<std::uint32_t>::max()) {
          co_return absl::ResourceExhaustedError("checkpoint key is too large");
        }
        const std::size_t extent_count =
            extents == nullptr ? 0 : extents->size();
        if (extent_count > std::numeric_limits<std::uint32_t>::max()) {
          co_return absl::ResourceExhaustedError(
              "checkpoint extent manifest is too large");
        }
        const std::size_t entry_bytes = sizeof(CheckpointEntryHeader) +
                                        key.size() +
                                        extent_count * sizeof(ExtentRef);
        if (entry_bytes > kExtentPayloadBytes - sizeof(CheckpointChunkHeader)) {
          co_return absl::ResourceExhaustedError(
              "one checkpoint index entry exceeds a storage block");
        }
        if (payload.size() + entry_bytes > kExtentPayloadBytes) {
          absl::Status flushed = co_await flush_chunk();
          if (!flushed.ok()) co_return flushed;
        }
        std::uint8_t flags = 0;
        if (location.external()) flags |= kExternal;
        if (location.key_external()) flags |= kKeyExternal;
        if (location.shielding()) flags |= kShielding;
        if (location.unclaimed()) flags |= kUnclaimed;
        CheckpointEntryHeader encoded{
            .block_id_ = location.block_id(),
            .allocation_epoch_ = location.allocation_epoch(),
            .mutation_sequence_ = location.mutation_sequence_,
            .expire_at_ms_ = location.expire_at_ms_,
            .replication_epoch_ = partition.replication_epoch_,
            .entry_bytes_ = static_cast<std::uint32_t>(entry_bytes),
            .key_bytes_ = static_cast<std::uint32_t>(key.size()),
            .logical_size_ = location.logical_size_,
            .record_offset_ = location.record_offset(),
            .total_disk_bytes_ = location.total_disk_bytes(),
            .extent_count_ = static_cast<std::uint32_t>(extent_count),
            .block_owner_ = location.block_owner(),
            .db_id_ = db_id,
            .kind_ = location.kind(),
            .value_type_ = location.value_type(),
            .flags_ = flags,
        };
        AppendPod(&payload, encoded);
        payload.insert(
            payload.end(), reinterpret_cast<const std::byte*>(key.data()),
            reinterpret_cast<const std::byte*>(key.data()) + key.size());
        if (extents != nullptr) {
          const auto* begin =
              reinterpret_cast<const std::byte*>(extents->data());
          payload.insert(payload.end(), begin,
                         begin + extents->size() * sizeof(ExtentRef));
        }
        ++chunk_entries;
        ++result.entry_count_;
      }
    }
  }
  co_return co_await flush_chunk();
}

Task<absl::Status> StorageEngine::Impl::PersistCheckpointBitmapOnDeviceLocal(
    std::size_t device_index, std::vector<std::uint64_t> block_ids) {
  DeviceAllocator& allocator = *device_allocators_[device_index];
  assert(celer::ThisWorker().id_ == allocator.owner_);
  co_await allocator.mutex_.Lock();
  UnlockGuard unlock(&allocator.mutex_, stores_[allocator.owner_]->worker_);

  std::vector<std::byte> desired(allocator.checkpoint_bitmap_.size(),
                                 std::byte{0});
  for (const std::uint64_t block_id : block_ids) {
    if (DeviceIndexForBlock(block_id) != device_index) {
      co_return absl::InternalError(
          "checkpoint bitmap received a block from another device");
    }
    const std::uint32_t local = LocalBlockId(block_id);
    if (local < allocator.data_block_begin_ || local / 8 >= desired.size() ||
        !BitmapBit(allocator, local)) {
      co_return absl::InternalError(
          "checkpoint bitmap references an unallocated block");
    }
    desired[local / 8] |= static_cast<std::byte>(1U << (local % 8));
  }

  std::vector<std::size_t> dirty_pages;
  for (std::size_t page_index = 0;
       page_index < allocator.checkpoint_bitmap_pages_.size(); ++page_index) {
    const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
    const std::size_t payload_bytes =
        std::min(kMetadataPagePayloadBytes, desired.size() - byte_offset);
    if (!allocator.checkpoint_bitmap_valid_ ||
        std::memcmp(desired.data() + byte_offset,
                    allocator.checkpoint_bitmap_.data() + byte_offset,
                    payload_bytes) != 0) {
      dirty_pages.push_back(page_index);
    }
  }
  if (dirty_pages.empty()) {
    allocator.checkpoint_bitmap_valid_ = true;
    co_return absl::OkStatus();
  }

  WorkerStore& store = *stores_[allocator.owner_];
  auto acquired = co_await store.buffers_.AcquireReadBuffer();
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size_ = kDirectIoAlignment;
  const StorageDevice& device = devices_[device_index];
  const std::uint64_t base_offset =
      CheckpointBitmapMetadataOffset(device.capacity_blocks_);
  std::vector<MetadataPageState> committed;
  committed.reserve(dirty_pages.size());
  for (const std::size_t page_index : dirty_pages) {
    const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
    const std::size_t payload_bytes =
        std::min(kMetadataPagePayloadBytes, desired.size() - byte_offset);
    const MetadataPageState current =
        allocator.checkpoint_bitmap_pages_[page_index];
    const std::uint8_t next_slot = current.active_slot_ == 0 ? 1 : 0;
    const std::uint64_t next_generation = current.generation_ + 1;
    std::span<std::byte, kDirectIoAlignment> output(buffer.data_,
                                                    kDirectIoAlignment);
    EncodeMetadataPage(
        MetadataPageKind::kCheckpointBitmap,
        static_cast<std::uint32_t>(page_index), next_generation,
        std::span<const std::byte>(desired.data() + byte_offset, payload_bytes),
        output);
    auto written = co_await WriteStorageBuffer(
        *store.worker_, store.files_[device.file_index_], output,
        lease.registered(), buffer,
        MetadataPageSlotOffset(base_offset, page_index, next_slot));
    if (!written.ok() || *written != kDirectIoAlignment) {
      allocator.checkpoint_bitmap_valid_ = false;
      co_return written.ok()
          ? absl::InternalError("short checkpoint-bitmap metadata write")
          : written.status();
    }
    committed.push_back(
        {.generation_ = next_generation, .active_slot_ = next_slot});
  }
  absl::Status synced = co_await celer::Fdatasync(
      *store.worker_, store.files_[device.file_index_]);
  if (!synced.ok()) {
    allocator.checkpoint_bitmap_valid_ = false;
    co_return synced;
  }
  for (std::size_t i = 0; i < dirty_pages.size(); ++i) {
    allocator.checkpoint_bitmap_pages_[dirty_pages[i]] = committed[i];
  }
  allocator.checkpoint_bitmap_ = std::move(desired);
  allocator.checkpoint_bitmap_valid_ = true;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PersistCheckpointBitmap(
    std::span<const std::uint64_t> block_ids) {
  std::vector<std::vector<std::uint64_t>> by_device(devices_.size());
  for (const std::uint64_t block_id : block_ids) {
    by_device[DeviceIndexForBlock(block_id)].push_back(block_id);
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const celer::WorkerId owner = device_allocators_[device_index]->owner_;
    absl::Status status;
    if (owner == celer::ThisWorker().id_) {
      status = co_await PersistCheckpointBitmapOnDeviceLocal(
          device_index, std::move(by_device[device_index]));
    } else {
      status = co_await celer::SubmitTaskTo(
          owner,
          [this, device_index,
           blocks = std::move(
               by_device[device_index])]() mutable -> Task<absl::Status> {
            co_return co_await PersistCheckpointBitmapOnDeviceLocal(
                device_index, std::move(blocks));
          });
    }
    if (!status.ok()) co_return status;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PublishShutdownCheckpoint(
    std::uint64_t generation) {
  std::uint64_t entry_count = 0;
  std::vector<std::uint64_t> blocks;
  for (const CheckpointShardResult& shard : checkpoint_shards_) {
    if (!shard.status_.ok()) co_return shard.status_;
    entry_count += shard.entry_count_;
    blocks.insert(blocks.end(), shard.blocks_.begin(), shard.blocks_.end());
  }
  if (blocks.size() < worker_count_) {
    co_return absl::InternalError(
        "checkpoint bitmap does not contain every worker shard");
  }
  std::sort(blocks.begin(), blocks.end());
  if (std::adjacent_find(blocks.begin(), blocks.end()) != blocks.end()) {
    co_return absl::InternalError(
        "checkpoint bitmap contains a duplicate block");
  }
  absl::Status bitmap = co_await PersistCheckpointBitmap(blocks);
  if (!bitmap.ok()) co_return bitmap;
  CheckpointRoot root{
      .generation_ = generation,
      .consumed_generation_ = checkpoint_root_.consumed_generation_,
      .block_count_ = blocks.size(),
      .entry_count_ = entry_count,
  };
  co_return co_await PersistCheckpointRoot(root);
}

Task<absl::Status> StorageEngine::Impl::LoadCheckpoint(
    WorkerStore& store, CheckpointLoadResult* result) {
  assert(result != nullptr);
  *result = {};
  result->saw_shards_.resize(worker_count_, false);
  if (!checkpoint_active_.load(std::memory_order_acquire)) {
    co_return absl::OkStatus();
  }
  auto* data = static_cast<std::byte*>(celer::AllocateStorageBuffer(
      kStorageBlockBytes, options_.buffers_.alignment_));
  if (data == nullptr) {
    co_return absl::ResourceExhaustedError(
        "failed to allocate checkpoint recovery buffer");
  }
  struct BufferGuard {
    std::byte* data_;
    std::size_t alignment_;
    ~BufferGuard() { celer::FreeStorageBuffer(data_, alignment_); }
  } guard{data, options_.buffers_.alignment_};

  auto read_block = [this, &store, data](std::uint64_t block_id)
      -> Task<absl::StatusOr<std::optional<BlockHeader>>> {
    if (!CheckpointBlockAllocated(device_allocators_, block_id)) {
      co_return std::optional<BlockHeader>{};
    }
    const auto [file_id, offset] = FileOffset(block_id);
    FixedBuffer io{.data_ = data, .size_ = kBlockHeaderBytes, .index_ = 0};
    auto read = co_await ReadStorageBuffer(
        *store.worker_, store.files_[file_id], io, false, offset);
    if (!read.ok() || *read != kBlockHeaderBytes) {
      co_return read.ok() ? absl::InternalError("short checkpoint header read")
                          : read.status();
    }
    BlockHeader header{};
    if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                    data, kBlockHeaderBytes),
                                &header) ||
        header.block_id_ != block_id ||
        header.kind_ != BlockKind::kCheckpointIndex ||
        header.tx_generation_ != checkpoint_root_.generation_ ||
        header.layout_worker_count_ != worker_count_) {
      // The bitmap is allowed to retain false positives from an interrupted
      // generation. Only a self-validating block stamped with the selected
      // generation participates in completeness checks.
      co_return std::optional<BlockHeader>{};
    }
    io.size_ = AlignDirect(header.committed_bytes_);
    read = co_await ReadStorageBuffer(*store.worker_, store.files_[file_id], io,
                                      false, offset);
    if (!read.ok() || *read != io.size_) {
      co_return read.ok() ? absl::InternalError("short checkpoint payload read")
                          : read.status();
    }
    BlockHeader reread{};
    if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                    data, kBlockHeaderBytes),
                                &reread) ||
        reread.block_id_ != block_id ||
        reread.allocation_epoch_ != header.allocation_epoch_ ||
        reread.kind_ != BlockKind::kCheckpointIndex ||
        reread.tx_generation_ != checkpoint_root_.generation_ ||
        reread.extent_index_ != header.extent_index_ ||
        reread.extent_payload_bytes_ != header.extent_payload_bytes_ ||
        reread.extent_payload_checksum_ != header.extent_payload_checksum_ ||
        reread.record_count_ != header.record_count_) {
      co_return absl::InternalError("checkpoint header changed while reading");
    }
    const auto payload = std::span<const std::byte>(
        data + kBlockHeaderBytes, header.extent_payload_bytes_);
    if (Crc32c(payload) != header.extent_payload_checksum_) {
      co_return absl::InternalError("checkpoint payload checksum mismatch");
    }
    co_return std::optional<BlockHeader>{header};
  };

  // Use the same topology-aware striped ownership as ordinary recovery. On
  // io_uring every worker scans a disjoint global stripe; with SPDK only a
  // device's configured qpair owners touch it. Interleaving devices keeps all
  // controllers busy without giving any worker more than one decoded block.
  std::vector<std::uint64_t> next_device_offsets(devices_.size());
  std::uint64_t device_linear_begin = 0;
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
#ifdef CELER_WITH_SPDK_STORAGE
    const auto& owners = device_owners_[device_index];
    const auto owner =
        std::lower_bound(owners.begin(), owners.end(), store.worker_->id());
    next_device_offsets[device_index] =
        owner == owners.end() || *owner != store.worker_->id()
            ? device.data_block_count_
            : static_cast<std::uint64_t>(owner - owners.begin());
#else
    next_device_offsets[device_index] =
        (store.worker_->id() + worker_count_ -
         device_linear_begin % worker_count_) %
        worker_count_;
#endif
    device_linear_begin += device.data_block_count_;
  }

  while (true) {
    bool scanned_block = false;
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const DeviceAllocator& allocator = *device_allocators_[device_index];
      const StorageDevice& device = devices_[device_index];
      std::uint64_t& next_device_offset = next_device_offsets[device_index];
      if (next_device_offset >= device.data_block_count_) continue;
      const std::uint64_t device_offset = next_device_offset;
#ifdef CELER_WITH_SPDK_STORAGE
      next_device_offset += device_owners_[device_index].size();
#else
      next_device_offset += worker_count_;
#endif
      scanned_block = true;
      const std::uint32_t local = static_cast<std::uint32_t>(
          device.data_block_begin_ + device_offset);
      if ((std::to_integer<unsigned>(allocator.checkpoint_bitmap_[local / 8]) &
           (1U << (local % 8))) == 0) {
        continue;
      }
      const std::uint64_t block_id = MakeBlockId(device.id_, local);
      auto candidate = co_await read_block(block_id);
      if (!candidate.ok()) co_return candidate.status();
      if (!candidate->has_value()) continue;
      const BlockHeader& block = **candidate;
      if (block.extent_index_ >= worker_count_) {
        co_return absl::InternalError("checkpoint block has an invalid shard");
      }
      result->saw_shards_[block.extent_index_] = true;
      const std::byte* cursor = data + kBlockHeaderBytes;
      const std::byte* end = cursor + block.extent_payload_bytes_;
      if (static_cast<std::size_t>(end - cursor) <
          sizeof(CheckpointChunkHeader)) {
        co_return absl::InternalError("checkpoint chunk is truncated");
      }
      CheckpointChunkHeader chunk{};
      std::memcpy(&chunk, cursor, sizeof(chunk));
      cursor += sizeof(chunk);
      if (chunk.magic_ != kCheckpointChunkMagic ||
          chunk.version_ != kCheckpointWireVersion ||
          chunk.header_bytes_ != sizeof(chunk) ||
          chunk.generation_ != checkpoint_root_.generation_ ||
          chunk.shard_id_ != block.extent_index_ ||
          chunk.entry_count_ != block.record_count_ ||
          chunk.entry_count_ >
              static_cast<std::size_t>(end - cursor) /
                  sizeof(CheckpointEntryHeader)) {
        co_return absl::InternalError("invalid checkpoint chunk header");
      }
      // Decode only this physical chunk. It is fully checked before any of
      // its entries become visible, then immediately handed to its shard so
      // recovery memory is bounded by one checkpoint block rather than the
      // complete dataset. If a later chunk fails, ordinary scanning merges
      // with this already validated prefix and fills the missing keys.
      RecoveryBatch recovered;
      recovered.records_.reserve(chunk.entry_count_);
      for (std::uint32_t i = 0; i < chunk.entry_count_; ++i) {
        if (static_cast<std::size_t>(end - cursor) <
            sizeof(CheckpointEntryHeader)) {
          co_return absl::InternalError("checkpoint entry is truncated");
        }
        CheckpointEntryHeader entry{};
        std::memcpy(&entry, cursor, sizeof(entry));
        if (entry.magic_ != kCheckpointEntryMagic ||
            entry.header_bytes_ != sizeof(entry) ||
            entry.entry_bytes_ < sizeof(entry) ||
            entry.entry_bytes_ > static_cast<std::size_t>(end - cursor) ||
            entry.db_id_ >= kLogicalDatabaseCount ||
            entry.block_owner_ >= worker_count_ ||
            entry.replication_epoch_ == 0 || entry.reserved_ != 0 ||
            (entry.flags_ &
             ~(kExternal | kKeyExternal | kShielding | kUnclaimed)) != 0 ||
            (entry.kind_ != RecordKind::kValue &&
             entry.kind_ != RecordKind::kTombstone) ||
            (entry.kind_ == RecordKind::kTombstone &&
             entry.value_type_ != ValueType::kNone) ||
            (entry.kind_ == RecordKind::kValue &&
             (entry.value_type_ < ValueType::kString ||
              entry.value_type_ > ValueType::kStream)) ||
            entry.logical_size_ > RecordIndexValue::kLogicalSizeMask ||
            entry.record_offset_ < kBlockHeaderBytes ||
            entry.record_offset_ % kRecordAlignment != 0 ||
            entry.total_disk_bytes_ == 0 ||
            entry.total_disk_bytes_ % kRecordAlignment != 0 ||
            static_cast<std::uint64_t>(entry.record_offset_) +
                    entry.total_disk_bytes_ >
                kStorageBlockBytes ||
            entry.entry_bytes_ !=
                sizeof(entry) + entry.key_bytes_ +
                    static_cast<std::size_t>(entry.extent_count_) *
                        sizeof(ExtentRef)) {
          co_return absl::InternalError("invalid checkpoint entry");
        }
        const char* key_data =
            reinterpret_cast<const char*>(cursor + sizeof(entry));
        std::string key(key_data, entry.key_bytes_);
        if (RedisSlot(key) % worker_count_ != block.extent_index_) {
          co_return absl::InternalError("checkpoint key is in the wrong shard");
        }
        ExtentManifest extents;
        if (entry.extent_count_ != 0) {
          auto mutable_extents =
              std::make_shared<std::vector<ExtentRef>>(entry.extent_count_);
          std::memcpy(mutable_extents->data(),
                      cursor + sizeof(entry) + entry.key_bytes_,
                      entry.extent_count_ * sizeof(ExtentRef));
          extents = std::move(mutable_extents);
        }
        const bool external = (entry.flags_ & kExternal) != 0;
        const bool key_external = (entry.flags_ & kKeyExternal) != 0;
        if (external != (extents != nullptr) ||
            !RecordLocation::CanEncodeBlockIdentity(entry.block_id_,
                                                    entry.allocation_epoch_)) {
          co_return absl::InternalError("invalid checkpoint record location");
        }
        const Digest digest = ComputeDigest(key);
        RecoveryRecord record{
            .digest_ = digest,
            .key_ = std::move(key),
            .db_id_ = entry.db_id_,
            .txid_ = 0,
            .lsn_ = std::numeric_limits<std::uint64_t>::max(),
            .replication_epoch_ = entry.replication_epoch_,
            .location_ = RecordLocation(
                entry.block_id_, entry.mutation_sequence_,
                entry.allocation_epoch_, entry.expire_at_ms_,
                entry.logical_size_,
                RecordLocation::PackedMetadata::Encode(
                    entry.record_offset_, entry.total_disk_bytes_,
                    entry.block_owner_, false, external, key_external,
                    (entry.flags_ & kShielding) != 0,
                    (entry.flags_ & kUnclaimed) != 0, false, entry.kind_,
                    entry.value_type_)),
            .extents_ = std::move(extents),
        };
        recovered.records_.push_back(std::move(record));
        cursor += entry.entry_bytes_;
        ++result->entry_count_;
      }
      if (cursor != end) {
        co_return absl::InternalError("checkpoint chunk has trailing bytes");
      }
      const unsigned owner = block.extent_index_;
      if (owner == store.worker_->id()) {
        ApplyRecovery(owner, std::move(recovered));
      } else {
        absl::Status applied = co_await celer::SubmitTo(
            owner, [this, owner, recovered = std::move(recovered)]() mutable {
              ApplyRecovery(owner, std::move(recovered));
              return absl::OkStatus();
        });
        if (!applied.ok()) co_return applied;
      }
      result->blocks_.push_back(block_id);
    }
    if (!scanned_block) break;
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
