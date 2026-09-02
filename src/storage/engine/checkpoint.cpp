#include <cstring>

#include "impl.h"

namespace keylane::storage {
namespace {

constexpr std::uint64_t kCheckpointChunkMagic =
    0x314b4e5548434c4bULL;  // KLCHUNK1
constexpr std::uint32_t kCheckpointWireVersion = 1;

enum class CheckpointChunkKind : std::uint32_t {
  kIndexEntries = 1,
  kBlockAccounting = 2,
};

enum CheckpointEntryFlag : std::uint8_t {
  kExternal = 1U << 0,
  kKeyExternal = 1U << 1,
  kShielding = 1U << 2,
  kUnclaimed = 1U << 3,
  kHasExpiry = 1U << 4,
};

constexpr std::uint32_t kCheckpointOwnerShift = 0;
constexpr std::uint32_t kCheckpointDbShift = 10;
constexpr std::uint32_t kCheckpointKindShift = 14;
constexpr std::uint32_t kCheckpointTypeShift = 16;
constexpr std::uint32_t kCheckpointFlagsShift = 19;
constexpr std::uint32_t kCheckpointOwnerMask = 0x3ffU;
constexpr std::uint32_t kCheckpointDbMask = 0xfU;
constexpr std::uint32_t kCheckpointKindMask = 0x3U;
constexpr std::uint32_t kCheckpointTypeMask = 0x7U;
constexpr std::uint32_t kCheckpointFlagsMask = 0x1fU;
constexpr std::uint32_t kCheckpointMetadataMask =
    (kCheckpointOwnerMask << kCheckpointOwnerShift) |
    (kCheckpointDbMask << kCheckpointDbShift) |
    (kCheckpointKindMask << kCheckpointKindShift) |
    (kCheckpointTypeMask << kCheckpointTypeShift) |
    (kCheckpointFlagsMask << kCheckpointFlagsShift);

struct CheckpointChunkHeader {
  std::uint64_t magic_ = kCheckpointChunkMagic;
  std::uint64_t generation_ = 0;
  std::uint32_t version_ = kCheckpointWireVersion;
  std::uint32_t header_bytes_ = sizeof(CheckpointChunkHeader);
  std::uint32_t shard_id_ = 0;
  CheckpointChunkKind kind_ = CheckpointChunkKind::kIndexEntries;
  std::uint32_t entry_count_ = 0;
  std::uint32_t reserved_ = 0;
};

struct CheckpointEntryHeader {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint64_t mutation_sequence_ = 0;
  std::uint64_t replication_epoch_ = 0;
  std::uint32_t key_bytes_ = 0;
  std::uint32_t logical_size_ = 0;
  std::uint32_t record_offset_ = 0;
  std::uint32_t total_disk_bytes_ = 0;
  std::uint32_t extent_count_ = 0;
  // Owner, database, kind, value type, and flags occupy 24 bits. The upper
  // bits must remain zero so format extensions cannot be misread silently.
  std::uint32_t metadata_ = 0;
};

struct CheckpointAccountingEntry {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t live_bytes_ = 0;
  // Extent identity remains outside the dense runtime BlockState. Persist it
  // here so restoring accounting preserves the same manifest/header checks as
  // cold recovery without charging ordinary blocks more resident memory.
  std::uint32_t extent_payload_bytes_ = 0;
  std::uint32_t extent_index_ = 0;
  std::uint32_t extent_payload_checksum_ = 0;
  std::uint16_t owner_ = kUnownedBlock;
  std::uint8_t extent_ = 0;
  std::uint8_t reserved_ = 0;
  std::uint32_t reserved_tail_ = 0;
};

static_assert(std::is_trivially_copyable_v<CheckpointChunkHeader>);
static_assert(std::is_trivially_copyable_v<CheckpointEntryHeader>);
static_assert(std::is_trivially_copyable_v<CheckpointAccountingEntry>);
static_assert(sizeof(CheckpointChunkHeader) == 40);
static_assert(sizeof(CheckpointEntryHeader) == 56);
static_assert(sizeof(CheckpointAccountingEntry) == 40);
static_assert(kMaxMemoryWorkers <= kCheckpointOwnerMask + 1);
static_assert(kLogicalDatabaseCount <= kCheckpointDbMask + 1);
static_assert(static_cast<std::uint8_t>(RecordKind::kTxCommit) <=
              kCheckpointKindMask);
static_assert(static_cast<std::uint8_t>(ValueType::kStream) <=
              kCheckpointTypeMask);

constexpr std::uint32_t EncodeCheckpointMetadata(
    std::uint16_t block_owner, std::uint8_t db_id, RecordKind kind,
    ValueType value_type, std::uint8_t flags) noexcept {
  return (static_cast<std::uint32_t>(block_owner) << kCheckpointOwnerShift) |
         (static_cast<std::uint32_t>(db_id) << kCheckpointDbShift) |
         (static_cast<std::uint32_t>(kind) << kCheckpointKindShift) |
         (static_cast<std::uint32_t>(value_type) << kCheckpointTypeShift) |
         (static_cast<std::uint32_t>(flags) << kCheckpointFlagsShift);
}

constexpr std::uint16_t CheckpointBlockOwner(std::uint32_t metadata) noexcept {
  return static_cast<std::uint16_t>((metadata >> kCheckpointOwnerShift) &
                                    kCheckpointOwnerMask);
}

constexpr std::uint8_t CheckpointDb(std::uint32_t metadata) noexcept {
  return static_cast<std::uint8_t>((metadata >> kCheckpointDbShift) &
                                   kCheckpointDbMask);
}

constexpr RecordKind CheckpointKind(std::uint32_t metadata) noexcept {
  return static_cast<RecordKind>((metadata >> kCheckpointKindShift) &
                                 kCheckpointKindMask);
}

constexpr ValueType CheckpointValueType(std::uint32_t metadata) noexcept {
  return static_cast<ValueType>((metadata >> kCheckpointTypeShift) &
                                kCheckpointTypeMask);
}

constexpr std::uint8_t CheckpointFlags(std::uint32_t metadata) noexcept {
  return static_cast<std::uint8_t>((metadata >> kCheckpointFlagsShift) &
                                   kCheckpointFlagsMask);
}

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

// A checkpoint reader alternates two of these slots. Each slot submits the
// header immediately and chains the payload read from that completion, so the
// next block remains in flight while the worker decodes the current block.
// State and its DMA buffer outlive the owning coroutine if recovery abandons a
// prefetch after detecting corruption; the I/O completion then reclaims both.
class CheckpointPrefetchSlot {
  struct State;

 public:
  using Result = absl::StatusOr<std::optional<BlockHeader>>;

  CheckpointPrefetchSlot() = default;
  CheckpointPrefetchSlot(const CheckpointPrefetchSlot&) = delete;
  CheckpointPrefetchSlot& operator=(const CheckpointPrefetchSlot&) = delete;
  ~CheckpointPrefetchSlot() { Release(); }

  absl::Status Initialize(std::size_t alignment) {
    assert(state_ == nullptr);
    State* state = new (std::nothrow) State(alignment);
    if (state == nullptr || state->data_ == nullptr) {
      delete state;
      return absl::ResourceExhaustedError(
          "failed to allocate checkpoint prefetch buffer");
    }
    state_ = state;
    return absl::OkStatus();
  }

  void Start(Worker& worker, FixedFile file, std::uint64_t offset,
             std::uint64_t block_id, std::uint64_t generation,
             unsigned worker_count) {
    assert(state_ != nullptr);
    assert(!state_->active_);
    state_->Start(worker, file, offset, block_id, generation, worker_count);
  }

  struct Awaiter {
    State* state_;

    bool await_ready() const noexcept { return state_->complete_; }

    bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
      assert(!state_->complete_);
      assert(!state_->waiter_);
      state_->waiter_ = awaiting;
      return true;
    }

    Result await_resume() {
      assert(state_->complete_);
      assert(state_->result_.has_value());
      state_->active_ = false;
      state_->complete_ = false;
      Result result = std::move(*state_->result_);
      state_->result_.reset();
      return result;
    }
  };

  Awaiter Wait() noexcept {
    assert(state_ != nullptr);
    assert(state_->active_);
    return Awaiter{state_};
  }

  std::byte* data() const noexcept {
    assert(state_ != nullptr);
    return state_->data_;
  }

 private:
  struct State final : celer::IoCompletion {
    explicit State(std::size_t alignment)
        : data_(static_cast<std::byte*>(
              celer::AllocateStorageBuffer(kStorageBlockBytes, alignment))),
          alignment_(alignment) {}

    ~State() override {
      celer::FreeStorageBuffer(data_, alignment_);
    }

    void Start(Worker& worker, FixedFile file, std::uint64_t offset,
               std::uint64_t block_id, std::uint64_t generation,
               unsigned worker_count) {
      worker_ = &worker;
      file_ = file;
      offset_ = offset;
      block_id_ = block_id;
      generation_ = generation;
      worker_count_ = worker_count;
      phase_ = Phase::kHeader;
      active_ = true;
      complete_ = false;
      orphaned_ = false;
      waiter_ = {};
      result_.reset();
      absl::Status submitted = Submit(kBlockHeaderBytes);
      if (!submitted.ok()) Finish(std::move(submitted));
    }

    void Complete(Worker& worker, int result, unsigned flags) override {
      (void)flags;
      assert(&worker == worker_);
      if (result < 0) {
        Finish(absl::ErrnoToStatus(-result, "checkpoint read failed"));
        return;
      }
      const std::size_t bytes = static_cast<std::size_t>(result);
      worker.RecordStorageReadCompletion(bytes);
      const std::size_t expected =
          phase_ == Phase::kHeader ? kBlockHeaderBytes : payload_read_bytes_;
      if (bytes != expected) {
        Finish(absl::InternalError(phase_ == Phase::kHeader
                                       ? "short checkpoint header read"
                                       : "short checkpoint payload read"));
        return;
      }
      if (phase_ == Phase::kPayload) {
        Finish(std::optional<BlockHeader>{header_});
        return;
      }

      BlockHeader header{};
      if (!DecodeBlockHeaderPages(
              std::span<const std::byte, kBlockHeaderBytes>(
                  data_, kBlockHeaderBytes),
              &header) ||
          header.block_id_ != block_id_ ||
          header.kind_ != BlockKind::kCheckpointIndex ||
          header.tx_generation_ != generation_ ||
          header.layout_worker_count_ != worker_count_) {
        // The discovery bitmap can retain blocks from an interrupted
        // generation. Do not spend a full-block read on a non-matching header.
        Finish(std::optional<BlockHeader>{});
        return;
      }
      header_ = header;
      payload_read_bytes_ = AlignDirect(header.committed_bytes_);
      phase_ = Phase::kPayload;
      absl::Status submitted = Submit(payload_read_bytes_);
      if (!submitted.ok()) {
        Finish(std::move(submitted));
        return;
      }
    }

    absl::Status Submit(std::size_t bytes) {
      return worker_->SubmitRead(
          file_, std::span<std::byte>(data_, bytes), offset_, this);
    }

    void Finish(Result result) {
      result_.emplace(std::move(result));
      complete_ = true;
      if (orphaned_) {
        delete this;
        return;
      }
      if (waiter_) {
        worker_->Enqueue(std::exchange(waiter_, {}));
      }
    }

    enum class Phase : std::uint8_t { kHeader, kPayload };

    std::byte* data_ = nullptr;
    std::size_t alignment_ = 0;
    Worker* worker_ = nullptr;
    FixedFile file_{};
    std::uint64_t offset_ = 0;
    std::uint64_t block_id_ = 0;
    std::uint64_t generation_ = 0;
    unsigned worker_count_ = 0;
    std::size_t payload_read_bytes_ = 0;
    BlockHeader header_{};
    Phase phase_ = Phase::kHeader;
    bool active_ = false;
    bool complete_ = false;
    bool orphaned_ = false;
    std::coroutine_handle<> waiter_{};
    std::optional<Result> result_;
  };

  void Release() noexcept {
    if (state_ == nullptr) return;
    if (state_->active_ && !state_->complete_) {
      // Completion owns the state from here. Clearing waiter_ prevents a
      // shutdown-destroyed recovery coroutine from being resumed later.
      state_->orphaned_ = true;
      state_->waiter_ = {};
    } else {
      delete state_;
    }
    state_ = nullptr;
  }

  State* state_ = nullptr;
};

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
  absl::flat_hash_map<std::uint64_t, RecoveryLiveReference>
      extent_identities;

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
        const bool has_expiry = location.expire_at_ms_ != 0;
        const std::size_t entry_bytes =
            sizeof(CheckpointEntryHeader) +
            (has_expiry ? sizeof(std::uint64_t) : 0) + key.size() +
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
        if (has_expiry) flags |= kHasExpiry;
        CheckpointEntryHeader encoded{
            .block_id_ = location.block_id(),
            .allocation_epoch_ = location.allocation_epoch(),
            .mutation_sequence_ = location.mutation_sequence_,
            .replication_epoch_ = partition.replication_epoch_,
            .key_bytes_ = static_cast<std::uint32_t>(key.size()),
            .logical_size_ = location.logical_size_,
            .record_offset_ = location.record_offset(),
            .total_disk_bytes_ = location.total_disk_bytes(),
            .extent_count_ = static_cast<std::uint32_t>(extent_count),
            .metadata_ = EncodeCheckpointMetadata(
                location.block_owner(), db_id, location.kind(),
                location.value_type(), flags),
        };
        AppendPod(&payload, encoded);
        if (has_expiry) {
          AppendPod(&payload, location.expire_at_ms_);
        }
        payload.insert(
            payload.end(), reinterpret_cast<const std::byte*>(key.data()),
            reinterpret_cast<const std::byte*>(key.data()) + key.size());
        if (extents != nullptr) {
          const auto* begin =
              reinterpret_cast<const std::byte*>(extents->data());
          payload.insert(payload.end(), begin,
                         begin + extents->size() * sizeof(ExtentRef));
          for (std::size_t extent_index = 0;
               extent_index < extents->size(); ++extent_index) {
            const ExtentRef& extent = extents->at(extent_index);
            RecoveryLiveReference identity{
                .block_id_ = extent.block_id_,
                .allocation_epoch_ = extent.allocation_epoch_,
                .bytes_ = extent.payload_bytes_,
                .extent_ = true,
                .extent_payload_bytes_ = extent.payload_bytes_,
                .extent_index_ = static_cast<std::uint32_t>(extent_index),
                .extent_payload_checksum_ = extent.payload_checksum_,
            };
            auto [position, inserted] = extent_identities.try_emplace(
                extent.block_id_, identity);
            if (!inserted) {
              const RecoveryLiveReference& existing = position->second;
              if (existing.allocation_epoch_ != identity.allocation_epoch_ ||
                  existing.extent_payload_bytes_ !=
                      identity.extent_payload_bytes_ ||
                  existing.extent_index_ != identity.extent_index_ ||
                  existing.extent_payload_checksum_ !=
                      identity.extent_payload_checksum_ ||
                  identity.bytes_ >
                      std::numeric_limits<std::uint32_t>::max() -
                          position->second.bytes_) {
                co_return absl::InternalError(
                    "checkpoint has inconsistent extent identities");
              }
              position->second.bytes_ += identity.bytes_;
            }
          }
        }
        ++chunk_entries;
        ++result.entry_count_;
      }
    }
  }
  absl::Status flushed = co_await flush_chunk();
  if (!flushed.ok()) co_return flushed;

  payload.clear();
  CheckpointChunkHeader accounting_chunk{
      .generation_ = generation,
      .shard_id_ = static_cast<std::uint32_t>(store.worker_->id()),
      .kind_ = CheckpointChunkKind::kBlockAccounting,
  };
  AppendPod(&payload, accounting_chunk);
  chunk_entries = 0;
  auto flush_accounting_chunk =
      [this, &store, generation, &result, &payload,
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
        .shard_id_ = static_cast<std::uint32_t>(store.worker_->id()),
        .kind_ = CheckpointChunkKind::kBlockAccounting,
    };
    AppendPod(&payload, next);
    chunk_entries = 0;
    co_return absl::OkStatus();
  };

  const std::uint16_t owner =
      static_cast<std::uint16_t>(store.worker_->id());
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    std::vector<BlockState>& states = device_block_states_[device_index];
    for (std::size_t slot = 0; slot < states.size(); ++slot) {
      BlockState& state = states[slot];
      if (state.owner_.load(std::memory_order_acquire) != owner ||
          !state.allocated_ || state.live_bytes_ == 0) {
        continue;
      }
      // Extent ownership is reassigned from the physical block id during
      // recovery and need not match the key-index shard. Their sparse
      // accounting is emitted from the manifests below instead.
      if (state.kind_ == BlockKind::kPayloadExtent) continue;
      const std::uint64_t block_id = MakeBlockId(
          device.id_,
          static_cast<std::uint32_t>(slot + device.data_block_begin_));
      CheckpointAccountingEntry entry{
          .block_id_ = block_id,
          .allocation_epoch_ = state.allocation_epoch_,
          .live_bytes_ = state.live_bytes_,
          .owner_ = owner,
      };
      if (state.kind_ != BlockKind::kRecords) {
        co_return absl::FailedPreconditionError(
            "checkpoint accounting found a live non-record block");
      }
      if (payload.size() + sizeof(entry) > kExtentPayloadBytes) {
        flushed = co_await flush_accounting_chunk();
        if (!flushed.ok()) co_return flushed;
      }
      AppendPod(&payload, entry);
      ++chunk_entries;
      ++result.accounting_entry_count_;
    }
  }
  for (const auto& [block_id, identity] : extent_identities) {
    CheckpointAccountingEntry entry{
        .block_id_ = block_id,
        .allocation_epoch_ = identity.allocation_epoch_,
        .live_bytes_ = identity.bytes_,
        .extent_payload_bytes_ = identity.extent_payload_bytes_,
        .extent_index_ = identity.extent_index_,
        .extent_payload_checksum_ = identity.extent_payload_checksum_,
        // The block-header scan determines the current physical owner.
        .owner_ = kUnownedBlock,
        .extent_ = 1,
    };
    if (payload.size() + sizeof(entry) > kExtentPayloadBytes) {
      flushed = co_await flush_accounting_chunk();
      if (!flushed.ok()) co_return flushed;
    }
    AppendPod(&payload, entry);
    ++chunk_entries;
    ++result.accounting_entry_count_;
  }
  co_return co_await flush_accounting_chunk();
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
  if (blocks.size() < 2 * worker_count_) {
    co_return absl::InternalError(
        "checkpoint bitmap does not contain both chunks for every worker");
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
  result->saw_accounting_shards_.resize(worker_count_, false);
  if (!checkpoint_active_.load(std::memory_order_acquire)) {
    co_return absl::OkStatus();
  }
  std::array<CheckpointPrefetchSlot, 2> prefetch;
  for (CheckpointPrefetchSlot& slot : prefetch) {
    absl::Status initialized =
        slot.Initialize(options_.buffers_.alignment_);
    if (!initialized.ok()) co_return initialized;
  }

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

  std::vector<std::uint64_t> checkpoint_blocks;
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
      if (CheckpointBlockAllocated(device_allocators_, block_id)) {
        checkpoint_blocks.push_back(block_id);
      }
    }
    if (!scanned_block) break;
  }

  auto decode_block = [this, &store, result](
                          std::uint64_t block_id, const std::byte* data,
                          BlockHeader block) -> Task<absl::Status> {
    BlockHeader reread{};
    if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                    data, kBlockHeaderBytes),
                                &reread) ||
        reread.block_id_ != block_id ||
        reread.allocation_epoch_ != block.allocation_epoch_ ||
        reread.kind_ != BlockKind::kCheckpointIndex ||
        reread.tx_generation_ != checkpoint_root_.generation_ ||
        reread.extent_index_ != block.extent_index_ ||
        reread.extent_payload_bytes_ != block.extent_payload_bytes_ ||
        reread.extent_payload_checksum_ != block.extent_payload_checksum_ ||
        reread.record_count_ != block.record_count_) {
      co_return absl::InternalError("checkpoint header changed while reading");
    }
    const auto payload = std::span<const std::byte>(
        data + kBlockHeaderBytes, block.extent_payload_bytes_);
    if (Crc32c(payload) != block.extent_payload_checksum_) {
      co_return absl::InternalError("checkpoint payload checksum mismatch");
    }
    if (block.extent_index_ >= worker_count_) {
      co_return absl::InternalError("checkpoint block has an invalid shard");
    }
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
        chunk.reserved_ != 0 ||
        (chunk.kind_ != CheckpointChunkKind::kIndexEntries &&
         chunk.kind_ != CheckpointChunkKind::kBlockAccounting)) {
      co_return absl::InternalError("invalid checkpoint chunk header");
    }
    if (chunk.kind_ == CheckpointChunkKind::kBlockAccounting) {
      if (chunk.entry_count_ > static_cast<std::size_t>(end - cursor) /
                                   sizeof(CheckpointAccountingEntry) ||
          static_cast<std::size_t>(end - cursor) !=
              static_cast<std::size_t>(chunk.entry_count_) *
                  sizeof(CheckpointAccountingEntry)) {
        co_return absl::InternalError(
            "invalid checkpoint accounting chunk size");
      }
      for (std::uint32_t i = 0; i < chunk.entry_count_; ++i) {
        CheckpointAccountingEntry entry{};
        std::memcpy(&entry, cursor, sizeof(entry));
        cursor += sizeof(entry);
        if (entry.live_bytes_ == 0 || entry.allocation_epoch_ == 0 ||
            entry.extent_ > 1 ||
            (entry.extent_ && entry.owner_ != kUnownedBlock) ||
            (!entry.extent_ &&
             (entry.owner_ != block.extent_index_ ||
              entry.owner_ >= worker_count_)) ||
            entry.reserved_ != 0 ||
            entry.reserved_tail_ != 0 ||
            !RecordLocation::CanEncodeBlockIdentity(entry.block_id_,
                                                    entry.allocation_epoch_) ||
            (entry.extent_ &&
             (entry.extent_payload_bytes_ == 0 ||
              entry.extent_payload_bytes_ > kExtentPayloadBytes)) ||
            (!entry.extent_ &&
             (entry.extent_payload_bytes_ != 0 || entry.extent_index_ != 0 ||
              entry.extent_payload_checksum_ != 0))) {
          co_return absl::InternalError(
              "invalid checkpoint accounting entry");
        }
        RecoveryLiveReference reference{
            .block_id_ = entry.block_id_,
            .allocation_epoch_ = entry.allocation_epoch_,
            .bytes_ = entry.live_bytes_,
            .expected_owner_ = entry.owner_,
            .extent_ = entry.extent_ != 0,
            .replace_live_bytes_ = true,
            .extent_payload_bytes_ = entry.extent_payload_bytes_,
            .extent_index_ = entry.extent_index_,
            .extent_payload_checksum_ = entry.extent_payload_checksum_,
        };
        if (!result->live_by_block_
                 .try_emplace(entry.block_id_, reference)
                 .second) {
          co_return absl::InternalError(
              "checkpoint accounting repeats a physical block");
        }
        ++result->accounting_entry_count_;
      }
      result->saw_accounting_shards_[block.extent_index_] = true;
      result->blocks_.push_back(block_id);
      co_return absl::OkStatus();
    }
    if (chunk.entry_count_ > static_cast<std::size_t>(end - cursor) /
                                 sizeof(CheckpointEntryHeader)) {
      co_return absl::InternalError("invalid checkpoint index entry count");
    }
    result->saw_shards_[block.extent_index_] = true;
    // Only the next block's I/O buffer overlaps this decode. Entries become
    // visible one completely validated chunk at a time, preserving bounded
    // recovery memory and cold-scan fallback semantics.
    RecoveryBatch recovered;
    recovered.records_.reserve(chunk.entry_count_);
    for (std::uint32_t i = 0; i < chunk.entry_count_; ++i) {
      if (static_cast<std::size_t>(end - cursor) <
          sizeof(CheckpointEntryHeader)) {
        co_return absl::InternalError("checkpoint entry is truncated");
      }
      CheckpointEntryHeader entry{};
      std::memcpy(&entry, cursor, sizeof(entry));
      const std::uint16_t block_owner =
          CheckpointBlockOwner(entry.metadata_);
      const std::uint8_t db_id = CheckpointDb(entry.metadata_);
      const RecordKind kind = CheckpointKind(entry.metadata_);
      const ValueType value_type = CheckpointValueType(entry.metadata_);
      const std::uint8_t flags = CheckpointFlags(entry.metadata_);
      const bool has_expiry = (flags & kHasExpiry) != 0;
      const std::size_t fixed_bytes =
          sizeof(entry) + (has_expiry ? sizeof(std::uint64_t) : 0);
      if ((entry.metadata_ & ~kCheckpointMetadataMask) != 0 ||
          db_id >= kLogicalDatabaseCount || block_owner >= worker_count_ ||
          entry.replication_epoch_ == 0 ||
          (flags & ~(kExternal | kKeyExternal | kShielding | kUnclaimed |
                     kHasExpiry)) != 0 ||
          (kind != RecordKind::kValue && kind != RecordKind::kTombstone) ||
          (kind == RecordKind::kTombstone &&
           value_type != ValueType::kNone) ||
          (kind == RecordKind::kValue &&
           (value_type < ValueType::kString ||
            value_type > ValueType::kStream)) ||
          entry.logical_size_ > RecordIndexValue::kLogicalSizeMask ||
          entry.record_offset_ < kBlockHeaderBytes ||
          entry.record_offset_ % kRecordAlignment != 0 ||
          entry.total_disk_bytes_ == 0 ||
          entry.total_disk_bytes_ % kRecordAlignment != 0 ||
          static_cast<std::uint64_t>(entry.record_offset_) +
                  entry.total_disk_bytes_ >
              kStorageBlockBytes ||
          fixed_bytes > static_cast<std::size_t>(end - cursor) ||
          entry.key_bytes_ >
              static_cast<std::size_t>(end - cursor) - fixed_bytes ||
          entry.extent_count_ >
              (static_cast<std::size_t>(end - cursor) - fixed_bytes -
               entry.key_bytes_) /
                  sizeof(ExtentRef)) {
        co_return absl::InternalError("invalid checkpoint entry");
      }
      const std::size_t entry_bytes =
          fixed_bytes + entry.key_bytes_ +
          static_cast<std::size_t>(entry.extent_count_) * sizeof(ExtentRef);
      std::uint64_t expire_at_ms = 0;
      if (has_expiry) {
        std::memcpy(&expire_at_ms, cursor + sizeof(entry),
                    sizeof(expire_at_ms));
        if (expire_at_ms == 0) {
          co_return absl::InternalError("checkpoint expiry extension is zero");
        }
      }
      const char* key_data =
          reinterpret_cast<const char*>(cursor + fixed_bytes);
      std::string key(key_data, entry.key_bytes_);
      if (RedisSlot(key) % worker_count_ != block.extent_index_) {
        co_return absl::InternalError("checkpoint key is in the wrong shard");
      }
      ExtentManifest extents;
      if (entry.extent_count_ != 0) {
        auto mutable_extents =
            std::make_shared<std::vector<ExtentRef>>(entry.extent_count_);
        std::memcpy(mutable_extents->data(),
                    cursor + fixed_bytes + entry.key_bytes_,
                    entry.extent_count_ * sizeof(ExtentRef));
        extents = std::move(mutable_extents);
      }
      const bool external = (flags & kExternal) != 0;
      const bool key_external = (flags & kKeyExternal) != 0;
      if (external != (extents != nullptr) ||
          !RecordLocation::CanEncodeBlockIdentity(entry.block_id_,
                                                  entry.allocation_epoch_)) {
        co_return absl::InternalError("invalid checkpoint record location");
      }
      const Digest digest = ComputeDigest(key);
      RecoveryRecord record{
          .digest_ = digest,
          .key_ = std::move(key),
          .db_id_ = db_id,
          .txid_ = 0,
          .lsn_ = std::numeric_limits<std::uint64_t>::max(),
          .replication_epoch_ = entry.replication_epoch_,
          .location_ = RecordLocation(
              entry.block_id_, entry.mutation_sequence_,
              entry.allocation_epoch_, expire_at_ms, entry.logical_size_,
              RecordLocation::PackedMetadata::Encode(
                  entry.record_offset_, entry.total_disk_bytes_, block_owner,
                  false, external, key_external,
                  (flags & kShielding) != 0, (flags & kUnclaimed) != 0, false,
                  kind, value_type)),
          .extents_ = std::move(extents),
          .checkpoint_snapshot_ = true,
      };
      recovered.records_.push_back(std::move(record));
      cursor += entry_bytes;
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
    co_return absl::OkStatus();
  };

  auto start_prefetch = [this, &store](CheckpointPrefetchSlot& slot,
                                       std::uint64_t block_id) {
    const auto [file_id, offset] = FileOffset(block_id);
    slot.Start(*store.worker_, store.files_[file_id], offset, block_id,
               checkpoint_root_.generation_, worker_count_);
  };
  if (!checkpoint_blocks.empty()) {
    start_prefetch(prefetch[0], checkpoint_blocks[0]);
  }
  for (std::size_t index = 0; index < checkpoint_blocks.size(); ++index) {
    CheckpointPrefetchSlot& current = prefetch[index % prefetch.size()];
    auto candidate = co_await current.Wait();
    if (!candidate.ok()) co_return candidate.status();
    if (index + 1 < checkpoint_blocks.size()) {
      start_prefetch(prefetch[(index + 1) % prefetch.size()],
                     checkpoint_blocks[index + 1]);
    }
    if (!candidate->has_value()) continue;
    absl::Status decoded =
        co_await decode_block(checkpoint_blocks[index], current.data(),
                              **candidate);
    if (!decoded.ok()) co_return decoded;
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
