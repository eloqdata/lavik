#include "keylane/storage/engine.h"

#include <fcntl.h>
#include <linux/fs.h>
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <charconv>
#include <cerrno>
#include <coroutine>
#include <cstring>
#include <deque>
#include <limits>
#include <new>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "celer/io/storage.h"
#include "celer/runtime/concurrentqueue.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "keylane/storage/format.h"
#include "keylane/storage/intent_lock.h"
#include "keylane/storage/scan_hash_map.h"
#include "spdlog/spdlog.h"

namespace keylane::storage {
namespace {

using celer::FixedBuffer;
using celer::FixedFile;
using celer::Status;
using celer::StatusCode;
using celer::StatusOr;
using celer::Task;
using celer::Worker;

struct RecordLocation {
  std::uint64_t block_id = 0;
  std::uint64_t replication_epoch = 1;
  std::uint64_t mutation_sequence = 0;
  std::uint64_t allocation_epoch = 0;
  // Owner in the current process topology. Unlike the persisted writer_id,
  // this must always be in [0, worker_count).
  std::uint16_t block_owner = 0;
  std::uint32_t record_offset = 0;
  std::uint32_t total_disk_bytes = 0;
  std::uint32_t value_bytes = 0;
  std::uint32_t relocation_sequence = 0;
  bool in_memory = false;
  RecordKind kind = RecordKind::kValue;

  bool SamePhysicalRecord(const RecordLocation& other) const noexcept {
    return block_id == other.block_id &&
           record_offset == other.record_offset &&
           allocation_epoch == other.allocation_epoch;
  }
};

static_assert(sizeof(RecordLocation) == 56);

using RecordIndex = ScanHashMap<RecordLocation>;

bool IsNewer(const RecordLocation& candidate,
             const RecordLocation& current) noexcept {
  if (candidate.replication_epoch != current.replication_epoch) {
    return candidate.replication_epoch > current.replication_epoch;
  }
  if (candidate.mutation_sequence != current.mutation_sequence) {
    return candidate.mutation_sequence > current.mutation_sequence;
  }
  if (candidate.relocation_sequence != current.relocation_sequence) {
    return candidate.relocation_sequence > current.relocation_sequence;
  }
  return false;
}

struct ActiveBlock {
  std::uint64_t block_id = 0;
  std::uint32_t writer_id = 0;
  std::uint32_t layout_worker_count = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = kBlockHeaderBytes;
  std::uint32_t record_count = 0;
  std::uint64_t max_lsn = 0;
  std::uint16_t write_buffer_id = 0;
  std::byte* heap_buffer = nullptr;
  std::size_t heap_buffer_size = 0;
};

struct BlockState {
  std::uint32_t writer_id = 0;
  std::uint32_t layout_worker_count = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = 0;
  std::uint32_t live_bytes = 0;
  std::uint32_t pins = 0;
  bool allocated = false;
  bool defrag_queued = false;
  bool defragging = false;
  bool freeing = false;
  bool in_memory = false;
  bool flush_queued = false;
  bool flush_in_progress = false;
  std::uint16_t write_buffer_id = 0;
  std::byte* heap_data = nullptr;
  std::size_t heap_data_size = 0;
  bool release_pending = false;
};

struct RecoveryRecord {
  Digest digest{};
  std::string key;
  std::uint8_t db_id = 0;
  RecordLocation location{};
};

struct RecoveryBlock {
  ActiveBlock block{};
};

struct RecoveryBatch {
  std::vector<RecoveryRecord> records;
  std::vector<RecoveryBlock> blocks;
};

struct RecoveryLiveReference {
  std::uint64_t block_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t bytes = 0;
};

struct RecordIdentity {
  RecordIndex::Entry* entry = nullptr;
};

class AsyncMutex {
 public:
  class LockAwaiter {
   public:
    explicit LockAwaiter(AsyncMutex* mutex) : mutex_(mutex) {}

    bool await_ready() noexcept {
      if (!mutex_->locked_) {
        mutex_->locked_ = true;
        return true;
      }
      return false;
    }

    bool await_suspend(std::coroutine_handle<> awaiting) {
      mutex_->waiters_.push_back(awaiting);
      return true;
    }

    void await_resume() noexcept {}

   private:
    AsyncMutex* mutex_;
  };

  LockAwaiter Lock() noexcept { return LockAwaiter(this); }

  void Unlock(Worker& worker) noexcept {
    if (waiters_.empty()) {
      locked_ = false;
      return;
    }
    std::coroutine_handle<> next = waiters_.front();
    waiters_.pop_front();
    worker.Enqueue(next);
  }

 private:
  bool locked_ = false;
  std::deque<std::coroutine_handle<>> waiters_;
};

class UnlockGuard {
 public:
  UnlockGuard(AsyncMutex* mutex, Worker* worker)
      : mutex_(mutex), worker_(worker) {}
  UnlockGuard(const UnlockGuard&) = delete;
  UnlockGuard& operator=(const UnlockGuard&) = delete;
  ~UnlockGuard() { mutex_->Unlock(*worker_); }

 private:
  AsyncMutex* mutex_;
  Worker* worker_;
};

class CoroutineBarrier {
 public:
  explicit CoroutineBarrier(unsigned participants)
      : participants_(participants) {}

  class Awaiter {
   public:
    Awaiter(CoroutineBarrier* barrier, Worker* worker)
        : barrier_(barrier), worker_(worker) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> awaiting) {
      return barrier_->Arrive(worker_, awaiting);
    }
    Status await_resume() { return barrier_->status(); }

   private:
    CoroutineBarrier* barrier_;
    Worker* worker_;
  };

  Awaiter Wait(Worker& worker) { return Awaiter(this, &worker); }

  void Abort(Status status) {
    std::vector<Waiter> wake;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!status_.ok()) {
        return;
      }
      status_ = std::move(status);
      completed_ = true;
      wake.swap(waiters_);
    }
    Wake(wake);
  }

 private:
  struct Waiter {
    Worker* worker = nullptr;
    std::coroutine_handle<> handle{};
  };

  static void ResumeRemote(void* context, std::uint64_t value) noexcept {
    static_cast<Worker*>(context)->Enqueue(
        std::coroutine_handle<>::from_address(
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(value))));
  }

  bool Arrive(Worker* worker, std::coroutine_handle<> awaiting) {
    std::vector<Waiter> wake;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (completed_) {
        return false;
      }
      waiters_.push_back(Waiter{worker, awaiting});
      ++arrived_;
      if (arrived_ != participants_) {
        return true;
      }
      completed_ = true;
      wake.swap(waiters_);
    }
    Wake(wake);
    return true;
  }

  void Wake(const std::vector<Waiter>& waiters) {
    const celer::CurrentWorker& current = celer::ThisWorker();
    for (const Waiter& waiter : waiters) {
      if (waiter.worker->id() == current.id) {
        waiter.worker->Enqueue(waiter.handle);
      } else {
        celer::PostNotification(
            current.cross_core, waiter.worker->id(),
            celer::RemoteNotification{
                .context = waiter.worker,
                .value = static_cast<std::uint64_t>(
                    reinterpret_cast<std::uintptr_t>(waiter.handle.address())),
                .run_fn = &CoroutineBarrier::ResumeRemote,
            });
      }
    }
  }

  Status status() {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
  }

  unsigned participants_ = 0;
  unsigned arrived_ = 0;
  bool completed_ = false;
  Status status_ = Status::Ok();
  std::mutex mutex_;
  std::vector<Waiter> waiters_;
};

bool IsZero(std::span<const std::byte> bytes) noexcept {
  return std::all_of(bytes.begin(), bytes.end(),
                     [](std::byte value) { return value == std::byte{0}; });
}

void AtomicMax(std::atomic<std::uint64_t>* target,
               std::uint64_t value) noexcept {
  std::uint64_t current = target->load(std::memory_order_relaxed);
  while (current < value &&
         !target->compare_exchange_weak(current, value,
                                        std::memory_order_relaxed)) {
  }
}

Status PrepareDataFile(const std::string& path, std::uint64_t size) {
  const int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open data file failed: " + path + ": " +
                      std::strerror(errno));
  }

  const int allocation_error =
      ::posix_fallocate(fd, 0, static_cast<off_t>(size));
  const int close_error = ::close(fd);
  if (allocation_error != 0) {
    return Status(StatusCode::kResourceExhausted,
                  "preallocate data file failed: " + path + ": " +
                      std::strerror(allocation_error));
  }
  if (close_error != 0) {
    return Status(StatusCode::kInternal,
                  "close data file failed: " + path);
  }
  return Status::Ok();
}

StatusOr<std::size_t> BlockDeviceIoAlignment(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open block device for alignment probe failed: " + path +
                      ": " + std::strerror(errno));
  }

  int logical_block_bytes = 0;
  const int ioctl_error = ::ioctl(fd, BLKSSZGET, &logical_block_bytes);
  const int saved_errno = errno;
  const int close_error = ::close(fd);
  if (ioctl_error != 0) {
    return Status(StatusCode::kInternal,
                  "BLKSSZGET failed: " + path + ": " +
                      std::strerror(saved_errno));
  }
  if (close_error != 0) {
    return Status(StatusCode::kInternal,
                  "close block device after alignment probe failed: " + path);
  }

  const auto alignment = static_cast<std::size_t>(logical_block_bytes);
  if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
    return Status(StatusCode::kInternal,
                  "block device logical sector size is not a power of two: " +
                      path);
  }
  return alignment;
}

Task<StatusOr<std::size_t>> ReadStorageBuffer(
    Worker& worker, FixedFile file, FixedBuffer buffer, bool registered,
    std::uint64_t offset) {
  if (registered) {
    co_return co_await celer::ReadFixed(worker, file, buffer, offset);
  }
  co_return co_await celer::Read(
      worker, file, std::span<std::byte>(buffer.data, buffer.size), offset);
}

Task<StatusOr<std::size_t>> WriteStorageBuffer(
    Worker& worker, FixedFile file,
    std::span<const std::byte> buffer, bool registered,
    FixedBuffer registered_buffer, std::uint64_t offset) {
  if (registered) {
    celer::FixedBuffer target = {
        .data = const_cast<std::byte*>(buffer.data()),
        .size = buffer.size(),
        .index = registered_buffer.index,
    };
    if (target.index == 0 || target.data == nullptr ||
        target.size > registered_buffer.size) {
      co_return Status(StatusCode::kInternal,
                       "invalid registered write buffer");
    }
    co_return co_await celer::WriteFixed(worker, file, target, offset);
  }
  co_return co_await celer::Write(worker, file, buffer, offset);
}

}  // namespace

class StorageEngine::Impl {
 public:
  explicit Impl(StorageEngineOptions options) : options_(std::move(options)) {}

 private:
  std::size_t direct_io_alignment_ = kDirectIoAlignment;

 public:
  struct WorkerStore {
    struct PartitionStore {
      std::uint16_t id = 0;
      std::array<RecordIndex, kLogicalDatabaseCount> indexes;
      std::array<std::size_t, kLogicalDatabaseCount> live_key_count{};
      std::uint64_t mutation_sequence = 0;
      std::uint64_t replication_epoch = 1;
      std::uint64_t delta_floor = 0;
      bool capture_deltas = false;
      bool delta_queued = false;
      std::deque<SnapshotRecord> deltas;
    };

    Worker* worker = nullptr;
    RegisteredBufferPool buffers;
    std::vector<FixedFile> files;
    std::vector<PartitionStore> partitions;
    absl::flat_hash_map<std::uint64_t, std::vector<RecordIdentity>>
        staged_records;
    std::array<std::size_t, kLogicalDatabaseCount> live_key_count{};
    std::array<IntentLockTable, kLogicalDatabaseCount> key_locks;
    std::optional<ActiveBlock> active_block;
    absl::flat_hash_map<std::uint64_t, std::unique_ptr<BlockState>> block_states;
    AsyncMutex writer_mutex;
    std::deque<std::uint64_t> flush_queue;
    std::deque<std::uint64_t> defrag_queue;
    bool flush_running = false;
    bool write_failed = false;
    bool defrag_running = false;
  };

  Status Prepare(unsigned worker_count) {
    if (worker_count == 0 || options_.data_files.empty()) {
      return Status(StatusCode::kInvalidArgument,
                    "storage requires workers and at least one data file");
    }
    if (worker_count > kLogicalStorageShards) {
      return Status(StatusCode::kInvalidArgument,
                    "storage worker count exceeds logical storage shards");
    }
    if (options_.data_files.size() >
        std::numeric_limits<std::uint16_t>::max()) {
      return Status(StatusCode::kOutOfRange, "too many data files");
    }
    if (options_.file_size_bytes < 2 * kStorageBlockBytes ||
        options_.file_size_bytes % kStorageBlockBytes != 0) {
      return Status(StatusCode::kInvalidArgument,
                    "data file size must be at least 16 MiB and a multiple of 8 MiB");
    }

    std::size_t direct_io_alignment = 1;
    for (const std::string& path : options_.data_files) {
      struct stat file_info {};
      if (::stat(path.c_str(), &file_info) == 0 &&
          S_ISBLK(file_info.st_mode)) {
        auto alignment = BlockDeviceIoAlignment(path);
        if (!alignment.ok()) {
          return alignment.status();
        }
        direct_io_alignment = std::max(direct_io_alignment, *alignment);
        spdlog::info(
            "using raw block device {} from offset 0 (configured bytes={} "
            "logical-sector-bytes={})",
            path, options_.file_size_bytes, *alignment);
        continue;
      }
      Status status = PrepareDataFile(path, options_.file_size_bytes);
      if (!status.ok()) {
        return status;
      }
      direct_io_alignment =
          std::max(direct_io_alignment, kDirectIoAlignment);
    }
    direct_io_alignment_ = direct_io_alignment;
    if (options_.flush_size_bytes < direct_io_alignment_ ||
        options_.flush_size_bytes > kStorageBlockBytes ||
        (options_.flush_size_bytes & (options_.flush_size_bytes - 1)) != 0 ||
        options_.flush_size_bytes % direct_io_alignment_ != 0) {
      return Status(
          StatusCode::kInvalidArgument,
          "flush size must be a power of two between the direct-I/O alignment "
          "and the 8 MiB storage block size");
    }
    spdlog::info("storage direct-I/O alignment={} bytes",
                 direct_io_alignment_);
    spdlog::info("storage flush submission size={} bytes",
                 options_.flush_size_bytes);

    worker_count_ = worker_count;
    blocks_per_file_ = options_.file_size_bytes / kStorageBlockBytes;
    total_blocks_ = blocks_per_file_ * options_.data_files.size();
    const auto recovery_start = std::chrono::steady_clock::now();
    recovery_started_ms_ =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            recovery_start.time_since_epoch())
            .count();
    recovery_next_log_ms_.store(recovery_started_ms_ + 5000,
                                std::memory_order_relaxed);
    stores_.reserve(worker_count);
    for (unsigned i = 0; i < worker_count; ++i) {
      stores_.push_back(std::make_unique<WorkerStore>());
      WorkerStore& store = *stores_.back();
      store.partitions.reserve(
          (kLogicalStorageShards + worker_count - 1 - i) / worker_count);
      for (std::uint32_t partition = i;
           partition < kLogicalStorageShards; partition += worker_count) {
        store.partitions.emplace_back();
        store.partitions.back().id = static_cast<std::uint16_t>(partition);
      }
    }
    open_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
    metadata_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
    recovery_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
    recovery_accounting_barrier_ =
        std::make_unique<CoroutineBarrier>(worker_count);
    free_list_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
    return Status::Ok();
  }

  Task<Status> InitializeWorker(Worker& worker) {
    WorkerStore& store = *stores_[worker.id()];
    store.worker = &worker;
    for (IntentLockTable& locks : store.key_locks) {
      locks.Bind(worker);
    }

    Status status = store.buffers.Init(worker, options_.buffers);
    if (status.ok()) {
      status = worker.RegisterFixedFiles(
          static_cast<unsigned>(options_.data_files.size()));
    }
    if (status.ok()) {
      store.files.reserve(options_.data_files.size());
      for (std::size_t i = 0; i < options_.data_files.size(); ++i) {
        FixedFile file{.index = static_cast<std::uint32_t>(i)};
        status = co_await celer::OpenFixedFile(
            worker, options_.data_files[i], O_RDWR | O_DIRECT, 0,
            file);
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

    if (worker.id() == 0) {
      status = co_await InitializeMetadata(store);
      if (!status.ok()) {
        Fail(status);
        co_return status;
      }
    }
    status = co_await metadata_barrier_->Wait(worker);
    if (!status.ok()) {
      co_return status;
    }

    std::vector<RecoveryBatch> batches(worker_count_);
    std::vector<std::uint64_t> zero_blocks;
    status = co_await ScanAssignedBlocks(store, &batches, &zero_blocks);
    if (!status.ok()) {
      Fail(status);
      co_return status;
    }

    for (unsigned target = 0; target < worker_count_; ++target) {
      if (batches[target].blocks.empty() && batches[target].records.empty()) {
        continue;
      }
      if (target == worker.id()) {
        ApplyRecovery(target, std::move(batches[target]));
      } else {
        Status apply = co_await celer::SubmitTo(
            target,
            [this, target, batch = std::move(batches[target])]() mutable {
              ApplyRecovery(target, std::move(batch));
              return Status::Ok();
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

    std::vector<std::vector<RecoveryLiveReference>> live_by_owner(
        worker_count_);
    for (auto& partition : store.partitions) {
      for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
        partition.indexes[db_id].ForEach(
            [&](const RecordIndex::Entry& entry) {
              const RecordLocation& location = entry.value;
              assert(location.block_owner < worker_count_);
              live_by_owner[location.block_owner].push_back(
                  RecoveryLiveReference{
                      .block_id = location.block_id,
                      .allocation_epoch = location.allocation_epoch,
                      .bytes = location.total_disk_bytes,
                  });
            });
      }
    }
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      if (live_by_owner[owner].empty()) {
        continue;
      }
      auto apply_live =
          [this, owner,
           references = std::move(live_by_owner[owner])]() mutable {
            WorkerStore& owner_store = *stores_[owner];
            for (const RecoveryLiveReference& reference : references) {
              BlockState* state =
                  FindBlockState(owner_store, reference.block_id);
              if (state == nullptr || !state->allocated ||
                  state->allocation_epoch != reference.allocation_epoch) {
                return Status(StatusCode::kInternal,
                              "recovery live reference has no owning block");
              }
              state->live_bytes += reference.bytes;
            }
            return Status::Ok();
          };
      Status applied = owner == worker.id()
                           ? apply_live()
                           : co_await celer::SubmitTo(owner,
                                                      std::move(apply_live));
      if (!applied.ok()) {
        Fail(applied);
        co_return applied;
      }
    }

    status = co_await recovery_accounting_barrier_->Wait(worker);
    if (!status.ok()) {
      co_return status;
    }

    const std::uint64_t pristine =
        next_block_id_.load(std::memory_order_acquire);
    for (std::uint64_t block_id : zero_blocks) {
      if (block_id < pristine) {
        if (!free_blocks_.enqueue(block_id)) {
          status = Status(StatusCode::kResourceExhausted,
                          "failed to rebuild free-block MPMC queue");
          Fail(status);
          co_return status;
        }
        free_block_count_.fetch_add(1, std::memory_order_release);
      }
    }
    status = co_await free_list_barrier_->Wait(worker);
    if (!status.ok()) {
      co_return status;
    }
    for (const auto& [block_id, state] : store.block_states) {
      if (state != nullptr) {
        MaybeQueueDefrag(store, block_id);
      }
    }
    worker.Spawn(PeriodicFlush(&store));
    co_return Status::Ok();
  }

  unsigned OwnerForKey(std::string_view key) const noexcept {
    return StorageShardForKey(key) % worker_count_;
  }

  Task<StatusOr<DiskValue>> Get(std::uint8_t db_id, std::string_view key,
                                ReadLatencyTrace* trace) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks[db_id].Acquire(digest,
                                                IntentLockMode::kShared);
    auto& index = partition.indexes[db_id];
    auto* found = index.Find(digest, key);
    if (found == nullptr || found->value.kind == RecordKind::kTombstone) {
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

    ReadBufferLease lease = std::move(loaded->lease);
    const std::size_t value_bytes = loaded->value_bytes;
    FixedBuffer io = lease.io_buffer();
    char length[32];
    auto [end, error] = std::to_chars(length, length + sizeof(length), value_bytes);
    if (error != std::errc{}) {
      co_return Status(StatusCode::kInternal, "bulk length formatting failed");
    }
    const std::size_t digits = static_cast<std::size_t>(end - length);
    const std::size_t prefix_bytes = digits + 3;
    std::byte* prefix = io.data - prefix_bytes;
    prefix[0] = std::byte{'$'};
    std::memcpy(prefix + 1, length, digits);
    prefix[digits + 1] = std::byte{'\r'};
    prefix[digits + 2] = std::byte{'\n'};
    io.data[value_bytes] = std::byte{'\r'};
    io.data[value_bytes + 1] = std::byte{'\n'};

    const std::size_t network_offset =
        static_cast<std::size_t>(prefix - lease.bytes().data());
    co_return DiskValue(std::move(lease), network_offset,
                        prefix_bytes + value_bytes + 2);
  }

  Task<Status> Set(std::uint8_t db_id, std::string_view key,
                   std::string_view value) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks[db_id].Acquire(digest,
                                                IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);
    co_return co_await AppendLocked(store, partition, db_id, key, value,
                                    RecordKind::kValue);
  }

  Task<StatusOr<bool>> Delete(std::uint8_t db_id, std::string_view key) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks[db_id].Acquire(digest,
                                                IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);

    auto& index = partition.indexes[db_id];
    auto* found = index.Find(digest, key);
    if (found == nullptr || found->value.kind == RecordKind::kTombstone) {
      co_return false;
    }
    Status status =
        co_await AppendLocked(store, partition, db_id, key, {},
                              RecordKind::kTombstone);
    if (!status.ok()) {
      co_return status;
    }
    co_return true;
  }

  Task<bool> Exists(std::uint8_t db_id, std::string_view key) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks[db_id].Acquire(digest,
                                                IntentLockMode::kShared);
    auto& index = partition.indexes[db_id];
    auto* found = index.Find(digest, key);
    co_return found != nullptr && found->value.kind == RecordKind::kValue;
  }

  Task<StatusOr<std::int64_t>> Increment(std::uint8_t db_id,
                                         std::string_view key) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks[db_id].Acquire(digest,
                                                IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);

    std::int64_t value = 0;
    auto& index = partition.indexes[db_id];
    auto* found = index.Find(digest, key);
    if (found != nullptr && found->value.kind == RecordKind::kValue) {
      auto loaded =
          co_await LoadValue(store, db_id, key, digest, found->value);
      if (!loaded.ok()) {
        co_return loaded.status();
      }
      FixedBuffer io = loaded->lease.io_buffer();
      std::string_view text(reinterpret_cast<const char*>(io.data),
                            loaded->value_bytes);
      auto [end, error] = std::from_chars(text.data(), text.data() + text.size(),
                                          value);
      if (error != std::errc{} || end != text.data() + text.size() ||
          value == std::numeric_limits<std::int64_t>::max()) {
        co_return Status(StatusCode::kInvalidArgument,
                         "value is not an integer or out of range");
      }
    }
    ++value;
    const std::string encoded = std::to_string(value);
    Status status =
        co_await AppendLocked(store, partition, db_id, key, encoded,
                              RecordKind::kValue);
    if (!status.ok()) {
      co_return status;
    }
    co_return value;
  }

  unsigned worker_count() const noexcept { return worker_count_; }

  std::size_t LocalSize(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return CurrentStore().live_key_count[db_id];
  }

  std::uint64_t DbEpoch(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return db_epochs_[db_id].load(std::memory_order_acquire);
  }

  Task<Status> FlushDb(std::uint8_t db_id) {
    assert(db_id < kLogicalDatabaseCount);
    if (celer::ThisWorker().id != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, db_id]() -> Task<Status> {
            co_return co_await FlushDb(db_id);
          });
    }

    const std::uint64_t current = DbEpoch(db_id);
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      co_return Status(StatusCode::kOutOfRange, "database epoch exhausted");
    }
    co_return co_await AdvanceDbEpoch(db_id, current + 1);
  }

  Task<Status> AdvanceDbEpoch(std::uint8_t db_id, std::uint64_t next) {
    if (celer::ThisWorker().id != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, db_id, next]() -> Task<Status> {
            co_return co_await AdvanceDbEpoch(db_id, next);
          });
    }
    const std::uint64_t current = DbEpoch(db_id);
    if (next < current) {
      co_return Status(StatusCode::kFailedPrecondition,
                       "replica database epoch is ahead of primary");
    }
    if (next == current) {
      co_return Status::Ok();
    }
    Status status = co_await PersistMetadata(db_id, next);
    if (!status.ok()) {
      co_return status;
    }
    db_epochs_[db_id].store(next, std::memory_order_release);

    for (unsigned target = 0; target < worker_count_; ++target) {
      Status cleared = target == 0
                           ? co_await ClearDbLocal(*stores_[target], db_id)
                           : co_await celer::SubmitTaskTo(
                                 target,
                                 [this, target, db_id]() -> Task<Status> {
                                   co_return co_await ClearDbLocal(
                                       *stores_[target], db_id);
                                 });
      if (!cleared.ok()) {
        co_return cleared;
      }
    }
    co_return Status::Ok();
  }

  ScanBatch ScanPartition(std::uint16_t partition_id, std::uint8_t db_id,
                          std::uint64_t cursor, std::size_t count) const {
    assert(db_id < kLogicalDatabaseCount);
    assert(count > 0);
    const auto& index =
        PartitionFor(CurrentStore(), partition_id).indexes[db_id];
    ScanBatch result;
    result.cursor = cursor;
    const std::size_t max_iterations =
        count > std::numeric_limits<std::size_t>::max() / 10
            ? std::numeric_limits<std::size_t>::max()
            : count * 10;
    std::size_t iterations = 0;
    do {
      result.cursor = index.Scan(
          result.cursor, [&](const RecordIndex::Entry& entry) {
            if (entry.value.kind == RecordKind::kValue) {
              result.keys.push_back(entry.key);
            }
          });
      ++iterations;
    } while (result.cursor != 0 && result.keys.size() < count &&
             iterations < max_iterations);
    return result;
  }

  PartitionReplicationStart BeginPartitionReplication(
      std::uint16_t partition_id) {
    auto& partition = PartitionFor(CurrentStore(), partition_id);
    partition.capture_deltas = true;
    partition.deltas.clear();
    partition.delta_floor = partition.mutation_sequence;
    partition.delta_queued = false;
    PartitionReplicationStart result;
    result.snapshot_sequence = partition.mutation_sequence;
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      result.db_epochs[db_id] = DbEpoch(db_id);
      if (partition.live_key_count[db_id] != 0) {
        result.nonempty_db_mask |= static_cast<std::uint16_t>(1U << db_id);
      }
    }
    return result;
  }

  Task<StatusOr<PartitionSnapshotBatch>> SnapshotPartition(
      std::uint16_t partition_id, std::uint8_t db_id,
      std::uint64_t cursor, std::size_t count) {
    if (db_id >= kLogicalDatabaseCount || count == 0) {
      co_return Status(StatusCode::kInvalidArgument,
                       "invalid partition snapshot request");
    }
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionFor(store, partition_id);
    auto& index = partition.indexes[db_id];
    std::vector<std::string> keys;
    std::uint64_t next = cursor;
    const std::size_t max_iterations =
        count > std::numeric_limits<std::size_t>::max() / 10
            ? std::numeric_limits<std::size_t>::max()
            : count * 10;
    std::size_t iterations = 0;
    do {
      next = index.Scan(next, [&](const RecordIndex::Entry& entry) {
        if (entry.value.kind == RecordKind::kValue) {
          keys.push_back(entry.key);
        }
      });
      ++iterations;
    } while (next != 0 && keys.size() < count &&
             iterations < max_iterations);

    PartitionSnapshotBatch batch;
    batch.cursor = next;
    batch.records.reserve(keys.size());
    for (const std::string& key : keys) {
      const Digest digest = ComputeDigest(key);
      auto key_lock = co_await store.key_locks[db_id].Acquire(
          digest, IntentLockMode::kShared);
      auto* current = index.Find(digest, key);
      if (current == nullptr || current->value.kind != RecordKind::kValue) {
        continue;
      }
      const RecordLocation location = current->value;
      auto loaded = co_await LoadValue(store, db_id, key, digest, location);
      if (!loaded.ok()) {
        if (loaded.status().code() == StatusCode::kNotFound) {
          continue;
        }
        co_return loaded.status();
      }
      const FixedBuffer value = loaded->lease.io_buffer();
      batch.records.push_back(SnapshotRecord{
          .kind = SnapshotRecord::Kind::kValue,
          .db_id = db_id,
          .db_epoch = DbEpoch(db_id),
          .mutation_sequence = location.mutation_sequence,
          .key = key,
          .value = std::string(reinterpret_cast<const char*>(value.data),
                               loaded->value_bytes),
      });
    }
    co_return batch;
  }

  PartitionDeltaBatch ReadPartitionDeltas(
      std::uint16_t partition_id, std::uint64_t after_sequence,
      std::size_t count) {
    auto& partition = PartitionFor(CurrentStore(), partition_id);
    partition.delta_queued = false;
    PartitionDeltaBatch batch;
    batch.watermark = partition.mutation_sequence;
    batch.overflow = after_sequence < partition.delta_floor;
    if (batch.overflow || count == 0) {
      return batch;
    }
    batch.records.reserve(std::min(count, partition.deltas.size()));
    for (const SnapshotRecord& mutation : partition.deltas) {
      if (mutation.mutation_sequence > after_sequence) {
        batch.records.push_back(mutation);
        if (batch.records.size() == count) {
          break;
        }
      }
    }
    return batch;
  }

  void AcknowledgePartitionDeltas(std::uint16_t partition_id,
                                  std::uint64_t through_sequence) {
    auto& partition = PartitionFor(CurrentStore(), partition_id);
    while (!partition.deltas.empty() &&
           partition.deltas.front().mutation_sequence <= through_sequence) {
      partition.delta_floor = std::max(
          partition.delta_floor,
          partition.deltas.front().mutation_sequence);
      partition.deltas.pop_front();
    }
    if (!partition.deltas.empty() && !partition.delta_queued) {
      partition.delta_queued = true;
      (void)replication_ready_.enqueue(partition.id);
    }
  }

  bool TryTakeReplicationReady(std::uint16_t* partition_id) {
    return partition_id != nullptr &&
           replication_ready_.try_dequeue(*partition_id);
  }

  Task<StatusOr<std::uint64_t>> ResetReplicaPartition(
      std::uint16_t partition_id,
      std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      Status advanced = co_await AdvanceDbEpoch(db_id, source_db_epochs[db_id]);
      if (!advanced.ok()) {
        co_return advanced;
      }
    }

    WorkerStore& store = CurrentStore();
    auto& partition = PartitionFor(store, partition_id);
    if (partition.replication_epoch ==
        std::numeric_limits<std::uint64_t>::max()) {
      co_return Status(StatusCode::kOutOfRange,
                       "partition replication epoch exhausted");
    }
    const std::uint64_t next_epoch = partition.replication_epoch + 1;

    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);
    std::vector<std::pair<std::uint8_t, std::string>> old_keys;
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      partition.indexes[db_id].ForEach(
          [&](const RecordIndex::Entry& entry) {
            old_keys.emplace_back(db_id, entry.key);
          });
    }
    partition.replication_epoch = next_epoch;
    partition.mutation_sequence = 0;
    partition.capture_deltas = false;
    partition.deltas.clear();
    partition.delta_floor = 0;
    partition.delta_queued = false;
    for (const auto& [db_id, key] : old_keys) {
      const Digest digest = ComputeDigest(key);
      Status tombstone = co_await WriteRecordLocked(
          store, db_id, key, {}, RecordKind::kTombstone, digest,
          0, 0, 0, false);
      if (!tombstone.ok()) {
        co_return tombstone;
      }
    }
    co_return next_epoch;
  }

  Task<Status> ApplyReplicaRecords(
      std::uint16_t partition_id, std::uint64_t replication_epoch,
      std::span<const SnapshotRecord> records) {
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionFor(store, partition_id);
    if (replication_epoch != partition.replication_epoch) {
      co_return Status(StatusCode::kFailedPrecondition,
                       "stale partition replication epoch");
    }
    for (const SnapshotRecord& record : records) {
      if (record.db_id >= kLogicalDatabaseCount ||
          RedisSlot(record.key) != partition_id) {
        if (record.kind != SnapshotRecord::Kind::kFlushDb) {
          co_return Status(StatusCode::kInvalidArgument,
                           "replica record belongs to another partition");
        }
      }
      if (record.kind == SnapshotRecord::Kind::kFlushDb) {
        Status advanced = co_await AdvanceDbEpoch(record.db_id,
                                                  record.db_epoch);
        if (!advanced.ok()) {
          co_return advanced;
        }
        partition.mutation_sequence = std::max(
            partition.mutation_sequence, record.mutation_sequence);
        continue;
      }
      if (record.db_epoch != DbEpoch(record.db_id)) {
        if (record.db_epoch < DbEpoch(record.db_id)) {
          continue;
        }
        co_return Status(StatusCode::kFailedPrecondition,
                         "replica record database epoch is not installed");
      }

      const Digest digest = ComputeDigest(record.key);
      auto key_lock = co_await store.key_locks[record.db_id].Acquire(
          digest, IntentLockMode::kExclusive);
      co_await store.writer_mutex.Lock();
      UnlockGuard write_unlock(&store.writer_mutex, store.worker);
      auto& index = partition.indexes[record.db_id];
      auto* current = index.Find(digest, record.key);
      if (current != nullptr &&
          current->value.replication_epoch == replication_epoch &&
          current->value.mutation_sequence >= record.mutation_sequence) {
        continue;
      }
      const RecordKind kind = record.kind == SnapshotRecord::Kind::kValue
                                  ? RecordKind::kValue
                                  : RecordKind::kTombstone;
      Status written = co_await WriteRecordLocked(
          store, record.db_id, record.key, record.value, kind, digest,
          record.mutation_sequence, record.mutation_sequence, 0, false);
      if (!written.ok()) {
        co_return written;
      }
      partition.mutation_sequence = std::max(
          partition.mutation_sequence, record.mutation_sequence);
    }
    co_return Status::Ok();
  }

  Status FlushForShutdown() {
    shutdown_flush_requested_.store(true, std::memory_order_release);
    unsigned completed =
        shutdown_flush_completed_.load(std::memory_order_acquire);
    while (completed < worker_count_) {
      shutdown_flush_completed_.wait(completed, std::memory_order_acquire);
      completed = shutdown_flush_completed_.load(std::memory_order_acquire);
    }
    if (shutdown_flush_failed_.load(std::memory_order_acquire)) {
      return Status(StatusCode::kInternal,
                    "one or more workers failed to flush during shutdown");
    }
    return Status::Ok();
  }

 private:
  FixedBuffer StagingBufferFor(const BlockState& state,
                              const RegisteredBufferPool& buffers) const {
    if (state.write_buffer_id != 0) {
      return buffers.write_buffer(state.write_buffer_id);
    }
    return FixedBuffer{.data = state.heap_data,
                      .size = state.heap_data_size,
                      .index = 0};
  }

  void ReleaseStagingBuffer(WorkerStore& store, BlockState& state) {
    if (state.write_buffer_id != 0) {
      store.buffers.ReleaseWriteBuffer(state.write_buffer_id);
    } else if (state.heap_data != nullptr) {
      store.buffers.ReleaseHeapWriteBuffer(state.heap_data);
    }
    state.write_buffer_id = 0;
    state.heap_data = nullptr;
    state.heap_data_size = 0;
    state.release_pending = false;
    state.in_memory = false;
  }

  struct LoadedValue {
    ReadBufferLease lease;
    std::size_t value_bytes = 0;
  };

  WorkerStore& CurrentStore() {
    return *stores_[celer::ThisWorker().id];
  }

  const WorkerStore& CurrentStore() const {
    return *stores_[celer::ThisWorker().id];
  }

  WorkerStore::PartitionStore& PartitionFor(
      WorkerStore& store, std::uint16_t partition_id) const {
    assert(partition_id < kLogicalStorageShards);
    assert(partition_id % worker_count_ == store.worker->id());
    auto& partition = store.partitions[partition_id / worker_count_];
    assert(partition.id == partition_id);
    return partition;
  }

  const WorkerStore::PartitionStore& PartitionFor(
      const WorkerStore& store, std::uint16_t partition_id) const {
    assert(partition_id < kLogicalStorageShards);
    assert(partition_id % worker_count_ == store.worker->id());
    const auto& partition = store.partitions[partition_id / worker_count_];
    assert(partition.id == partition_id);
    return partition;
  }

  WorkerStore::PartitionStore& PartitionForKey(
      WorkerStore& store, std::string_view key) const {
    return PartitionFor(store, RedisSlot(key));
  }

  const WorkerStore::PartitionStore& PartitionForKey(
      const WorkerStore& store, std::string_view key) const {
    return PartitionFor(store, RedisSlot(key));
  }

  static BlockState* FindBlockState(WorkerStore& store,
                                    std::uint64_t block_id) noexcept {
    auto found = store.block_states.find(block_id);
    return found == store.block_states.end() ? nullptr : found->second.get();
  }

  static const BlockState* FindBlockState(
      const WorkerStore& store, std::uint64_t block_id) noexcept {
    auto found = store.block_states.find(block_id);
    return found == store.block_states.end() ? nullptr : found->second.get();
  }

  static BlockState& CreateBlockState(WorkerStore& store,
                                      std::uint64_t block_id) {
    auto [found, inserted] = store.block_states.try_emplace(block_id, nullptr);
    if (inserted || found->second == nullptr) {
      found->second = std::make_unique<BlockState>();
    }
    return *found->second;
  }

  std::pair<std::uint32_t, std::uint64_t> FileOffset(
      std::uint64_t block_id) const noexcept {
    const std::uint32_t file_id =
        static_cast<std::uint32_t>(block_id / blocks_per_file_);
    const std::uint64_t local_block = block_id % blocks_per_file_;
    return {file_id, local_block * kStorageBlockBytes};
  }

  Task<Status> InitializeMetadata(WorkerStore& store) {
    auto acquired = co_await store.buffers.AcquireReadBuffer();
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer buffer = lease.io_buffer();
    buffer.size = kDirectIoAlignment;
    auto read = co_await ReadStorageBuffer(*store.worker, store.files[0],
                                           buffer, lease.registered(), 0);
    if (!read.ok()) {
      co_return read.status();
    }
    if (*read != kDirectIoAlignment) {
      co_return Status(StatusCode::kInternal,
                       "short read of storage metadata");
    }

    StorageMetadata metadata{};
    std::span<const std::byte, kDirectIoAlignment> input(buffer.data,
                                                         kDirectIoAlignment);
    if (IsZero(input)) {
      metadata.db_epochs.fill(1);
      std::span<std::byte, kDirectIoAlignment> output(buffer.data,
                                                      kDirectIoAlignment);
      EncodeStorageMetadata(metadata, output);
      auto written = co_await WriteStorageBuffer(
          *store.worker, store.files[0], output, lease.registered(), buffer, 0);
      if (!written.ok() || *written != kDirectIoAlignment) {
        co_return written.ok()
                      ? Status(StatusCode::kInternal,
                               "short write of storage metadata")
                      : written.status();
      }
      Status synced = co_await celer::Fdatasync(*store.worker, store.files[0]);
      if (!synced.ok()) {
        co_return synced;
      }
    } else if (!DecodeStorageMetadata(input, &metadata)) {
      co_return Status(StatusCode::kInternal,
                       "invalid or corrupt storage metadata");
    }
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      db_epochs_[db_id].store(metadata.db_epochs[db_id],
                              std::memory_order_release);
    }
    next_block_id_.store(1, std::memory_order_release);
    co_return Status::Ok();
  }

  Task<Status> PersistMetadata(std::uint8_t db_id, std::uint64_t epoch) {
    WorkerStore& store = *stores_[0];
    auto acquired = co_await store.buffers.AcquireReadBuffer();
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer buffer = lease.io_buffer();
    buffer.size = kDirectIoAlignment;
    StorageMetadata metadata{};
    for (std::uint8_t id = 0; id < kLogicalDatabaseCount; ++id) {
      metadata.db_epochs[id] = id == db_id ? epoch : DbEpoch(id);
    }
    std::span<std::byte, kDirectIoAlignment> output(buffer.data,
                                                    kDirectIoAlignment);
    EncodeStorageMetadata(metadata, output);
    auto written = co_await WriteStorageBuffer(
        *store.worker, store.files[0], output, lease.registered(), buffer, 0);
    if (!written.ok() || *written != kDirectIoAlignment) {
      co_return written.ok()
                    ? Status(StatusCode::kInternal,
                             "short write of storage metadata")
                    : written.status();
    }
    co_return co_await celer::Fdatasync(*store.worker, store.files[0]);
  }

  Task<Status> ClearDbLocal(WorkerStore& store, std::uint8_t db_id) {
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);
    std::vector<RecordLocation> old_locations;
    old_locations.reserve(store.live_key_count[db_id]);
    for (auto& partition : store.partitions) {
      auto& index = partition.indexes[db_id];
      index.ForEach([&](const RecordIndex::Entry& entry) {
        old_locations.push_back(entry.value);
      });
      index.Clear();
      partition.live_key_count[db_id] = 0;
      const std::uint64_t sequence = ++partition.mutation_sequence;
      if (partition.capture_deltas) {
        AppendDelta(partition, SnapshotRecord{
                                   .kind = SnapshotRecord::Kind::kFlushDb,
                                   .db_id = db_id,
                                   .db_epoch = DbEpoch(db_id),
                                   .mutation_sequence = sequence,
                                   .key = {},
                                   .value = {},
                               });
      }
    }
    store.live_key_count[db_id] = 0;
    for (const RecordLocation& location : old_locations) {
      Status dead = co_await MarkRecordDead(location);
      if (!dead.ok()) {
        store.write_failed = true;
        co_return dead;
      }
    }
    co_return Status::Ok();
  }

  void Fail(const Status& status) {
    open_barrier_->Abort(status);
    metadata_barrier_->Abort(status);
    recovery_barrier_->Abort(status);
    recovery_accounting_barrier_->Abort(status);
    free_list_barrier_->Abort(status);
  }

  unsigned RecoveredBlockOwner(const BlockHeader& block,
                               std::uint64_t block_id) const noexcept {
    // writer_id belongs to the topology that wrote the block and may be
    // greater than the current worker count after a scale-down.
    if (block.layout_worker_count == worker_count_ &&
        block.writer_id < worker_count_) {
      return block.writer_id;
    }
    std::uint64_t mixed = block_id ^
                          (block.allocation_epoch + 0x9e3779b97f4a7c15ULL);
    mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
    mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
    mixed ^= mixed >> 31;
    return static_cast<unsigned>(mixed % worker_count_);
  }


  void ReportRecoveryProgress(std::uint64_t records) {
    const std::uint64_t scanned =
        recovery_scanned_blocks_.fetch_add(1, std::memory_order_relaxed) + 1;
    const std::uint64_t scanned_records =
        recovery_scanned_records_.fetch_add(records, std::memory_order_relaxed) +
        records;
    const auto now = std::chrono::steady_clock::now();
    const std::int64_t now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch())
            .count();
    const std::uint64_t data_blocks = total_blocks_ - 1;
    const bool complete = scanned == data_blocks;
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

    const double elapsed_seconds =
        std::max(0.001, static_cast<double>(now_ms - recovery_started_ms_) /
                            1000.0);
    const double block_rate =
        static_cast<double>(scanned) / elapsed_seconds;
    const double record_rate =
        static_cast<double>(scanned_records) / elapsed_seconds;
    const double percent =
        data_blocks == 0
            ? 100.0
            : (static_cast<double>(scanned) * 100.0) /
                  static_cast<double>(data_blocks);
    const double eta_seconds =
        block_rate == 0.0
            ? 0.0
            : static_cast<double>(data_blocks - scanned) / block_rate;
    spdlog::info(
        "storage recovery: blocks={}/{} ({:.1f}%) records={} "
        "rate={:.0f} blocks/s {:.2f}M records/s eta={:.1f}s",
        scanned, data_blocks, percent, scanned_records, block_rate,
        record_rate / 1000000.0, eta_seconds);
  }
  Task<Status> ScanAssignedBlocks(WorkerStore& store,
                                  std::vector<RecoveryBatch>* batches,
                                  std::vector<std::uint64_t>* zero_blocks) {
    auto acquired = co_await store.buffers.AcquireReadBuffer();
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer header_buffer = lease.io_buffer();
    header_buffer.size = kDirectIoAlignment;

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

    for (std::uint64_t block_id = store.worker->id(); block_id < total_blocks_;
         block_id += worker_count_) {
      if (block_id == 0) {
        continue;
      }
      const auto [file_id, block_offset] = FileOffset(block_id);
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
        ReportRecoveryProgress(0);
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

      BlockHeader block{};
      std::span<const std::byte, kBlockHeaderBytes> recovered_block_header(
          recovery.buffer.data, kBlockHeaderBytes);
      if (!DecodeBlockHeader(recovered_block_header, &block)) {
        co_return Status(StatusCode::kInternal,
                         "invalid or corrupt block header");
      }
      AtomicMax(&next_block_id_, block_id + 1);
      AtomicMax(&next_allocation_epoch_, block.allocation_epoch + 1);
      AtomicMax(&next_lsn_, block.max_lsn + 1);

      const unsigned block_owner = RecoveredBlockOwner(block, block_id);
      batches->at(block_owner).blocks.push_back(RecoveryBlock{ActiveBlock{
          .block_id = block_id,
          .writer_id = block.writer_id,
          .layout_worker_count = block.layout_worker_count,
          .allocation_epoch = block.allocation_epoch,
          .committed_bytes = block.committed_bytes,
          .record_count = block.record_count,
          .max_lsn = block.max_lsn,
      }});

      std::uint32_t record_offset = kBlockHeaderBytes;
      std::uint32_t records = 0;
      while (record_offset < block.committed_bytes) {
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
        const unsigned key_owner = OwnerForKey(key);
        batches->at(key_owner).records.push_back(RecoveryRecord{
            .digest = record.digest,
            .key = std::string(key),
            .db_id = record.db_id,
            .location = RecordLocation{
                .block_id = block_id,
                .replication_epoch = record.replication_epoch,
                .mutation_sequence = record.mutation_sequence,
                .allocation_epoch = record.allocation_epoch,
                .block_owner = static_cast<std::uint16_t>(block_owner),
                .record_offset = record_offset,
                .total_disk_bytes = record.total_disk_bytes,
                .value_bytes = record.value_bytes,
                .relocation_sequence = static_cast<std::uint32_t>(
                    record.relocation_sequence),
                .kind = record.kind,
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
      ReportRecoveryProgress(records);
    }
    co_return Status::Ok();
  }

  void ApplyRecovery(unsigned target, RecoveryBatch batch) {
    WorkerStore& store = *stores_[target];
    for (const RecoveryBlock& recovered : batch.blocks) {
      const ActiveBlock& block = recovered.block;
      BlockState& state = CreateBlockState(store, block.block_id);
      state.writer_id = block.writer_id;
      state.layout_worker_count = block.layout_worker_count;
      state.allocation_epoch = block.allocation_epoch;
      state.committed_bytes = block.committed_bytes;
      state.allocated = true;

      // Recovered blocks have no staging buffer. Keep partial blocks sealed;
      // appending to one would otherwise dereference an absent in-memory copy.
    }

    for (const RecoveryRecord& recovered : batch.records) {
      auto& partition = PartitionForKey(store, recovered.key);
      if (recovered.location.replication_epoch <
          partition.replication_epoch) {
        continue;
      }
      if (recovered.location.replication_epoch >
          partition.replication_epoch) {
        for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
          store.live_key_count[db_id] -=
              partition.live_key_count[db_id];
          partition.live_key_count[db_id] = 0;
          partition.indexes[db_id].Clear();
        }
        partition.replication_epoch =
            recovered.location.replication_epoch;
        partition.mutation_sequence = 0;
      }
      partition.mutation_sequence = std::max(
          partition.mutation_sequence, recovered.location.mutation_sequence);
      auto& index = partition.indexes[recovered.db_id];
      auto* found = index.Find(recovered.digest, recovered.key);
      if (found == nullptr ||
          IsNewer(recovered.location, found->value)) {
        // TODO: add large-record reconstruction on recovery:
        // gather all chunks for a digest and coalesce into a logical key value.
        const bool was_live =
          found != nullptr && found->value.kind == RecordKind::kValue;
        const bool is_live = recovered.location.kind == RecordKind::kValue;
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
      }
    }
  }

  Task<StatusOr<LoadedValue>> LoadValue(WorkerStore& key_store,
                                        std::uint8_t db_id,
                                        std::string_view key,
                                        const Digest& digest,
                                        RecordLocation location,
                                        ReadLatencyTrace* trace = nullptr) {
    assert(location.block_owner < worker_count_);
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

  Task<StatusOr<LoadedValue>> LoadValueLocal(
      WorkerStore& store, std::uint8_t db_id, std::string_view key,
      const Digest& digest, RecordLocation location,
      ReadLatencyTrace* trace = nullptr) {
    // TODO: Coalesce concurrent reads of the same aligned disk page, like
    // the reference engine tiering::OpManager::pending_reads_. Key the in-flight table by
    // (file_id, aligned offset, aligned length), submit one read, and fan the
    // decoded result out to all waiting coroutines. In-flight operations must
    // retain values/leases, never flat_hash_map iterators or element pointers.
    BlockState* state = FindBlockState(store, location.block_id);
    if (state == nullptr || !state->allocated || state->freeing ||
        state->allocation_epoch != location.allocation_epoch) {
      co_return Status(StatusCode::kInternal, "stale index block epoch");
    }
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
          if (state->write_buffer_id != 0) {
            store->buffers.ReleaseWriteBuffer(state->write_buffer_id);
          } else if (state->heap_data != nullptr) {
            store->buffers.ReleaseHeapWriteBuffer(state->heap_data);
          }
          state->write_buffer_id = 0;
          state->heap_data = nullptr;
          state->heap_data_size = 0;
          state->release_pending = false;
          state->in_memory = false;
        }
      }
    } pin{&store, state};

    if (location.in_memory && state->in_memory) {

      auto in_mem_buffer =
          StagingBufferFor(*state, store.buffers);
      if (!in_mem_buffer.data || in_mem_buffer.size == 0 ||
          location.record_offset + location.total_disk_bytes > in_mem_buffer.size) {
        co_return Status(StatusCode::kInternal, "invalid in-memory location");
      }
      if (trace != nullptr) {
        trace->buffer_acquire_start_ns = ReadTraceNowNanos();
      }
      auto acquired = co_await store.buffers.AcquireReadBuffer();
      if (!acquired.ok()) {
        co_return acquired.status();
      }
      ReadBufferLease lease = std::move(*acquired);
      if (trace != nullptr) {
        trace->buffer_acquired_ns = ReadTraceNowNanos();
        trace->heap_read_buffer = !lease.registered();
        trace->io_submit_ns = trace->buffer_acquired_ns;
        trace->io_complete_ns = trace->buffer_acquired_ns;
      }
      FixedBuffer io = lease.io_buffer();
      if (location.value_bytes > io.size) {
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
          location.value_bytes != record.value_bytes ||
          location.total_disk_bytes != record.total_disk_bytes) {
        co_return Status(StatusCode::kInternal,
                         "record does not match in-memory location");
      }
      std::memcpy(io.data, record_bytes + record.header_bytes,
                  location.value_bytes);
      if (options_.verify_read_crc &&
          Crc32c(std::span<const std::byte>(io.data, record.value_bytes)) !=
              record.payload_checksum) {
        co_return Status(StatusCode::kInternal,
                         "record value checksum mismatch");
      }
      if (trace != nullptr) {
        trace->decode_done_ns = ReadTraceNowNanos();
      }
      co_return LoadedValue{std::move(lease), record.value_bytes};
    }

    if (trace != nullptr) {
      trace->buffer_acquire_start_ns = ReadTraceNowNanos();
    }
    auto acquired = co_await store.buffers.AcquireReadBuffer();
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    if (trace != nullptr) {
      trace->buffer_acquired_ns = ReadTraceNowNanos();
      trace->heap_read_buffer = !lease.registered();
      trace->disk_read = true;
    }
    FixedBuffer io = lease.io_buffer();

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
        record.value_bytes != location.value_bytes ||
        record.total_disk_bytes != location.total_disk_bytes) {
      co_return Status(StatusCode::kInternal,
                       "record does not match in-memory location");
    }
    const std::byte* value_data = record_data + record.header_bytes;
    if (options_.verify_read_crc &&
        Crc32c(std::span<const std::byte>(value_data, record.value_bytes)) !=
            record.payload_checksum) {
      co_return Status(StatusCode::kInternal, "record value checksum mismatch");
    }
    std::memmove(io.data, value_data, record.value_bytes);
    if (trace != nullptr) {
      trace->decode_done_ns = ReadTraceNowNanos();
    }
    co_return LoadedValue{std::move(lease), record.value_bytes};
  }

  std::optional<std::uint64_t> AllocateBlock(bool for_defrag) {
    std::uint64_t current = next_block_id_.load(std::memory_order_relaxed);
    while (current < total_blocks_) {
      if (next_block_id_.compare_exchange_weak(
              current, current + 1, std::memory_order_relaxed)) {
        return current;
      }
    }

    const std::size_t reserve = for_defrag ? 0 : kDefragReserveBlocks;
    std::size_t available =
        free_block_count_.load(std::memory_order_acquire);
    while (available > reserve) {
      if (!free_block_count_.compare_exchange_weak(
              available, available - 1, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        continue;
      }
      std::uint64_t block_id = 0;
      if (free_blocks_.try_dequeue(block_id)) {
        return block_id;
      }
      free_block_count_.fetch_add(1, std::memory_order_release);
      return std::nullopt;
    }
    return std::nullopt;
  }

  Status MarkRecordDeadLocal(unsigned owner,
                             const RecordLocation& location) {
    WorkerStore& store = *stores_[owner];
    BlockState* state = FindBlockState(store, location.block_id);
    if (state == nullptr || !state->allocated ||
        state->allocation_epoch != location.allocation_epoch) {
      return Status(StatusCode::kInternal,
                    "stale block owner while invalidating record");
    }
    state->live_bytes -=
        std::min(state->live_bytes, location.total_disk_bytes);
    MaybeQueueDefrag(store, location.block_id);
    return Status::Ok();
  }

  Task<Status> MarkRecordDead(const RecordLocation& location) {
    assert(location.block_owner < worker_count_);
    const unsigned owner = location.block_owner;
    if (owner == celer::ThisWorker().id) {
      co_return MarkRecordDeadLocal(owner, location);
    }
    co_return co_await celer::SubmitTo(
        owner, [this, owner, location] {
          return MarkRecordDeadLocal(owner, location);
        });
  }

  Task<Status> AppendLocked(WorkerStore& store,
                            WorkerStore::PartitionStore& partition,
                            std::uint8_t db_id,
                            std::string_view key, std::string_view value,
                            RecordKind kind) {
    const Digest digest = ComputeDigest(key);
    const std::uint64_t mutation_sequence = ++partition.mutation_sequence;
    Status status = co_await WriteRecordLocked(
        store, db_id, key, value, kind, digest, mutation_sequence,
        mutation_sequence, 0, false);
    if (status.ok() && partition.capture_deltas) {
      AppendDelta(partition, SnapshotRecord{
                                 .kind = kind == RecordKind::kValue
                                             ? SnapshotRecord::Kind::kValue
                                             : SnapshotRecord::Kind::kDelete,
                                 .db_id = db_id,
                                 .db_epoch = DbEpoch(db_id),
                                 .mutation_sequence = mutation_sequence,
                                 .key = std::string(key),
                                 .value = std::string(value),
                             });
    }
    co_return status;
  }

  void AppendDelta(WorkerStore::PartitionStore& partition,
                   SnapshotRecord record) {
    constexpr std::size_t kMaxRetainedMutations = 65536;
    partition.deltas.push_back(std::move(record));
    if (!partition.delta_queued) {
      partition.delta_queued = true;
      (void)replication_ready_.enqueue(partition.id);
    }
    while (partition.deltas.size() > kMaxRetainedMutations) {
      partition.delta_floor = std::max(
          partition.delta_floor,
          partition.deltas.front().mutation_sequence);
      partition.deltas.pop_front();
    }
  }

  Task<Status> WriteRecordLocked(WorkerStore& store, std::uint8_t db_id,
                                 std::string_view key, std::string_view value,
                                 RecordKind kind,
                                 const Digest& digest,
                                 std::uint64_t generation,
                                 std::uint64_t mutation_sequence,
                                 std::uint64_t relocation_sequence,
                                 bool for_defrag) {
    if (store.write_failed) {
      co_return Status(StatusCode::kFailedPrecondition,
                       "storage writer is stopped after an IO failure");
    }
    if (key.size() > MaxKeyBytes()) {
      co_return Status(StatusCode::kOutOfRange,
                       "key is too large for the on-disk record header");
    }
    const std::size_t record_header_bytes = RecordHeaderBytes(key.size());
    const std::size_t value_disk_bytes = value.size();
    const std::size_t total_disk_bytes = AlignRecord(
        record_header_bytes + value_disk_bytes);
    if (total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
        total_disk_bytes > options_.buffers.write_buffer_bytes) {
      // TODO: large-value chunking: persist value as manifest+segments so
      // restart can rebuild one logical value from ordered chunks.
      co_return Status(StatusCode::kOutOfRange,
                       "value requires dedicated multi-block storage");
    }

    // Logical partitions route keys, but physical append streams are per
    // worker. This keeps foreground writes local and bounds active 8 MiB
    // buffers by worker count rather than logical partition count.
    const std::uint32_t writer_id = store.worker->id();
    auto& partition = PartitionForKey(store, key);
    auto& index = partition.indexes[db_id];
    auto* previous_entry = index.Find(digest, key);
    const std::optional<RecordLocation> previous =
        previous_entry == nullptr
            ? std::nullopt
            : std::optional<RecordLocation>(previous_entry->value);
    const std::uint64_t lsn =
        next_lsn_.fetch_add(1, std::memory_order_relaxed);

    auto& active = store.active_block;
    if (!active.has_value() ||
        active->committed_bytes + total_disk_bytes > kStorageBlockBytes) {
      if (active.has_value()) {
        RequestFlush(store, active->block_id);
        active.reset();
      }
      const std::optional<std::uint64_t> allocated = AllocateBlock(for_defrag);
      if (!allocated.has_value()) {
        co_return Status(StatusCode::kResourceExhausted,
                         "no foreground blocks remain; defrag reserve is protected");
      }
      const std::uint64_t block_id = *allocated;
      std::uint16_t write_buffer_id = 0;
      std::byte* heap_buffer = nullptr;
      if (!store.buffers.TryAcquireWriteBuffer(&write_buffer_id)) {
        if (!store.buffers.TryAcquireHeapWriteBuffer(&heap_buffer)) {
          co_return Status(StatusCode::kResourceExhausted,
                           "no registered or fallback write buffers");
        }
      }
      FixedBuffer staging_buffer = write_buffer_id != 0
                                      ? store.buffers.write_buffer(write_buffer_id)
                                      : FixedBuffer{.data = heap_buffer,
                                                   .size = options_.buffers.write_buffer_bytes,
                                                   .index = 0};
      if (staging_buffer.data == nullptr ||
          staging_buffer.size == 0) {
        if (write_buffer_id != 0) {
          store.buffers.ReleaseWriteBuffer(write_buffer_id);
        } else {
          store.buffers.ReleaseHeapWriteBuffer(heap_buffer);
        }
        co_return Status(StatusCode::kInternal,
                         "active write staging allocation is invalid");
      }
      std::fill_n(staging_buffer.data, staging_buffer.size,
                  std::byte{0});
      active = ActiveBlock{
          .block_id = block_id,
          .writer_id = writer_id,
          .layout_worker_count = worker_count_,
          .allocation_epoch = next_allocation_epoch_.fetch_add(
              1, std::memory_order_relaxed),
          .committed_bytes = kBlockHeaderBytes,
          .record_count = 0,
          .max_lsn = 0,
          .write_buffer_id = write_buffer_id,
          .heap_buffer = heap_buffer,
          .heap_buffer_size = options_.buffers.write_buffer_bytes,
      };
      BlockState& state = CreateBlockState(store, block_id);
      state = BlockState{};
      state.writer_id = writer_id;
      state.layout_worker_count = worker_count_;
      state.allocation_epoch = active->allocation_epoch;
      state.committed_bytes = kBlockHeaderBytes;
      state.live_bytes = 0;
      state.pins = 0;
      state.allocated = true;
      state.defragging = false;
      state.in_memory = true;
      state.flush_queued = false;
      state.flush_in_progress = false;
      state.write_buffer_id = write_buffer_id;
      state.heap_data = heap_buffer;
      state.heap_data_size = options_.buffers.write_buffer_bytes;
      store.staged_records.erase(block_id);

      BlockHeader block{
          .magic = kBlockMagic,
          .version = kStorageFormatVersion,
          .header_bytes = kBlockHeaderBytes,
          .block_bytes = kStorageBlockBytes,
          .writer_id = writer_id,
          .allocation_epoch = active->allocation_epoch,
          .committed_bytes = kBlockHeaderBytes,
          .record_count = 0,
          .max_lsn = 0,
          .checksum = 0,
          .layout_worker_count = worker_count_,
      };
      std::span<std::byte, kBlockHeaderBytes> block_output(
          staging_buffer.data, kBlockHeaderBytes);
      EncodeBlockHeader(block, block_output);
    }

    ActiveBlock updated = *active;
    const std::uint32_t record_offset = updated.committed_bytes;
    updated.committed_bytes += static_cast<std::uint32_t>(total_disk_bytes);
    ++updated.record_count;
    updated.max_lsn = std::max(updated.max_lsn, lsn);

    BlockState* state_ptr = FindBlockState(store, updated.block_id);
    if (state_ptr == nullptr) {
      co_return Status(StatusCode::kInternal,
                       "active block has no owner state");
    }
    BlockState& state = *state_ptr;
    FixedBuffer staging = updated.write_buffer_id != 0
                              ? store.buffers.write_buffer(updated.write_buffer_id)
                              : FixedBuffer{.data = updated.heap_buffer,
                                           .size = state.heap_data_size,
                                           .index = 0};
    if (staging.data == nullptr ||
        record_offset + total_disk_bytes > staging.size) {
      co_return Status(StatusCode::kInternal, "invalid active staging block");
    }
    std::fill_n(staging.data + record_offset, total_disk_bytes, std::byte{0});
    RecordHeader record{
        .magic = kRecordMagic,
        .version = kStorageFormatVersion,
        .header_bytes = static_cast<std::uint16_t>(record_header_bytes),
        .kind = kind,
        .db_id = db_id,
        .digest = digest,
        .key_bytes = static_cast<std::uint32_t>(key.size()),
        .value_bytes = static_cast<std::uint32_t>(value.size()),
        .value_disk_bytes = static_cast<std::uint32_t>(value_disk_bytes),
        .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
        .generation = generation,
        .replication_epoch = partition.replication_epoch,
        .db_epoch = DbEpoch(db_id),
        .mutation_sequence = mutation_sequence,
        .relocation_sequence = relocation_sequence,
        .lsn = lsn,
        .allocation_epoch = updated.allocation_epoch,
        .payload_checksum = Crc32c(std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(value.data()), value.size())),
        .header_checksum = 0,
    };
    std::span<std::byte> record_output(staging.data + record_offset,
                                      record_header_bytes);
    if (!EncodeRecordHeader(record, key, record_output)) {
      co_return Status(StatusCode::kInternal, "record header encoding failed");
    }
    if (!value.empty()) {
      std::memcpy(staging.data + record_offset + record_header_bytes,
                  value.data(), value.size());
    }

    BlockHeader block{
        .magic = kBlockMagic,
        .version = kStorageFormatVersion,
        .header_bytes = kBlockHeaderBytes,
        .block_bytes = kStorageBlockBytes,
        .writer_id = updated.writer_id,
        .allocation_epoch = updated.allocation_epoch,
        .committed_bytes = updated.committed_bytes,
        .record_count = updated.record_count,
        .max_lsn = updated.max_lsn,
        .checksum = 0,
        .layout_worker_count = updated.layout_worker_count,
    };
    std::span<std::byte, kBlockHeaderBytes> block_output(
        staging.data, kBlockHeaderBytes);
    EncodeBlockHeader(block, block_output);
    if (updated.committed_bytes == kStorageBlockBytes) {
      state.in_memory = true;
      state.write_buffer_id = updated.write_buffer_id;
      state.heap_data = updated.heap_buffer;
      state.heap_data_size = updated.heap_buffer_size;
      RequestFlush(store, updated.block_id);
      active.reset();
    } else {
      state.heap_data = updated.heap_buffer;
      state.heap_data_size = updated.heap_buffer_size;
      *active = updated;
    }

    const RecordLocation location{
        .block_id = updated.block_id,
        .replication_epoch = partition.replication_epoch,
        .mutation_sequence = mutation_sequence,
        .allocation_epoch = updated.allocation_epoch,
        .block_owner = static_cast<std::uint16_t>(writer_id),
        .record_offset = record_offset,
        .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
        .value_bytes = static_cast<std::uint32_t>(value.size()),
        .relocation_sequence = static_cast<std::uint32_t>(
            relocation_sequence),
        .in_memory = true,
        .kind = kind,
    };
    const bool was_live =
        previous.has_value() && previous->kind == RecordKind::kValue;
    const bool is_live = kind == RecordKind::kValue;
    auto inserted = index.InsertOrAssign(digest, key, location);
    store.staged_records[updated.block_id].push_back(RecordIdentity{
        .entry = inserted.entry,
    });
    if (was_live != is_live) {
      if (is_live) {
        ++partition.live_key_count[db_id];
        ++store.live_key_count[db_id];
      } else {
        --partition.live_key_count[db_id];
        --store.live_key_count[db_id];
      }
    }
    state.committed_bytes = updated.committed_bytes;
    state.in_memory = true;
    state.write_buffer_id = updated.write_buffer_id;
    state.heap_data = updated.heap_buffer;
    state.heap_data_size = updated.heap_buffer_size;
    state.live_bytes += location.total_disk_bytes;
    state.flush_queued = updated.committed_bytes == kStorageBlockBytes;
    if (previous.has_value()) {
      Status dead = co_await MarkRecordDead(*previous);
      if (!dead.ok()) {
        store.write_failed = true;
        co_return dead;
      }
    }
    co_return Status::Ok();
  }

  void SealActiveBlocks(WorkerStore& store) {
    if (store.active_block.has_value() &&
        store.active_block->committed_bytes > kBlockHeaderBytes) {
      RequestFlush(store, store.active_block->block_id);
      store.active_block.reset();
    }
  }

  Task<Status> FlushWorkerForShutdown(WorkerStore* store) {
    while (active_defrags_.load(std::memory_order_acquire) != 0) {
      Status status = co_await celer::SleepFor(
          *store->worker, std::chrono::milliseconds(1));
      if (!status.ok()) {
        co_return status;
      }
    }

    co_await store->writer_mutex.Lock();
    {
      UnlockGuard guard(&store->writer_mutex, store->worker);
      SealActiveBlocks(*store);
    }

    while (true) {
      co_await store->writer_mutex.Lock();
      bool done = false;
      bool failed = false;
      {
        UnlockGuard guard(&store->writer_mutex, store->worker);
        done = !store->flush_running && store->flush_queue.empty();
        failed = store->write_failed;
      }
      if (failed) {
        co_return Status(StatusCode::kInternal,
                         "storage write failed while draining shutdown buffers");
      }
      if (done) {
        co_return Status::Ok();
      }
      Status status = co_await celer::SleepFor(
          *store->worker, std::chrono::milliseconds(1));
      if (!status.ok()) {
        co_return status;
      }
    }
  }

  void CompleteShutdownFlush(const Status& status) {
    if (!status.ok()) {
      shutdown_flush_failed_.store(true, std::memory_order_release);
    }
    shutdown_flush_completed_.fetch_add(1, std::memory_order_acq_rel);
    shutdown_flush_completed_.notify_all();
  }

  Task<Status> PeriodicFlush(WorkerStore* store) {
    const auto interval =
        std::chrono::milliseconds(options_.flush_max_ms);
    while (!store->worker->stop_requested()) {
      Status status = co_await celer::SleepFor(*store->worker, interval);
      if (!status.ok()) {
        CompleteShutdownFlush(status);
        co_return status;
      }
      if (store->worker->stop_requested()) {
        break;
      }

      if (shutdown_flush_requested_.load(std::memory_order_acquire)) {
        status = co_await FlushWorkerForShutdown(store);
        CompleteShutdownFlush(status);
        co_return status;
      }

      co_await store->writer_mutex.Lock();
      UnlockGuard guard(&store->writer_mutex, store->worker);
      SealActiveBlocks(*store);
    }
    co_return Status::Ok();
  }

  void RequestFlush(WorkerStore& store, std::uint64_t block_id) {
    BlockState* state = FindBlockState(store, block_id);
    if (state == nullptr || !state->allocated || !state->in_memory ||
        (state->write_buffer_id == 0 && state->heap_data == nullptr)) {
      return;
    }
    if (state->flush_queued || state->flush_in_progress) {
      return;
    }
    state->flush_queued = true;
    store.flush_queue.push_back(block_id);
    if (store.flush_running) {
      return;
    }
    store.flush_running = true;
    store.worker->Spawn(FlushPendingBlocks(&store));
  }

  Task<Status> FlushPendingBlocks(WorkerStore* store) {
    struct PendingFlush {
      std::uint64_t block_id = 0;
      std::uint32_t committed_bytes = 0;
      std::uint64_t allocation_epoch = 0;
      std::uint16_t write_buffer_id = 0;
      std::byte* heap_data = nullptr;
      std::size_t heap_data_size = 0;
      std::vector<RecordIdentity> staged_records;
    };

    while (true) {
      std::optional<PendingFlush> pending;
      auto release_pending = [&](const PendingFlush& block) {
        if (block.write_buffer_id != 0) {
          store->buffers.ReleaseWriteBuffer(block.write_buffer_id);
        } else if (block.heap_data != nullptr) {
          store->buffers.ReleaseHeapWriteBuffer(block.heap_data);
        }
      };

      {
        co_await store->writer_mutex.Lock();
        UnlockGuard guard(&store->writer_mutex, store->worker);

        if (store->flush_queue.empty()) {
          store->flush_running = false;
          co_return Status::Ok();
        }

        const std::uint64_t block_id = store->flush_queue.front();
        store->flush_queue.pop_front();
        BlockState* state = FindBlockState(*store, block_id);
        if (state == nullptr) {
          continue;
        }
        if (!state->allocated || !state->in_memory ||
            (state->write_buffer_id == 0 && state->heap_data == nullptr) ||
            state->flush_in_progress) {
          state->flush_queued = false;
          continue;
        }
        if (state->pins > 0) {
          store->flush_queue.push_back(block_id);
          state->flush_queued = true;
          continue;
        }

        pending.emplace(PendingFlush{
            .block_id = block_id,
            .committed_bytes = state->committed_bytes,
            .allocation_epoch = state->allocation_epoch,
            .write_buffer_id = state->write_buffer_id,
            .heap_data = state->heap_data,
            .heap_data_size = state->heap_data_size,
        });
        if (auto found = store->staged_records.find(block_id);
            found != store->staged_records.end()) {
          pending->staged_records = std::move(found->second);
          store->staged_records.erase(found);
        }
        state->flush_queued = false;
        state->flush_in_progress = true;
      }

      const auto [file_id, block_offset] =
          FileOffset(pending->block_id);
      FixedBuffer staging = pending->write_buffer_id != 0
                               ? store->buffers.write_buffer(
                                     pending->write_buffer_id)
                               : FixedBuffer{.data = pending->heap_data,
                                            .size = pending->heap_data_size,
                                            .index = 0};
      const std::size_t write_bytes = AlignDirect(pending->committed_bytes);
      if (staging.data == nullptr || staging.size < write_bytes) {
        co_await store->writer_mutex.Lock();
        UnlockGuard guard(&store->writer_mutex, store->worker);
        BlockState* state = FindBlockState(*store, pending->block_id);
        if (state != nullptr) {
          state->flush_in_progress = false;
          state->flush_queued = false;
        }
        store->write_failed = true;
        store->flush_running = false;
        release_pending(*pending);
        co_return Status(StatusCode::kInternal,
                         "invalid pending flush staging buffer");
      }

      for (std::size_t write_offset = 0; write_offset < write_bytes;) {
        const std::size_t chunk_bytes =
            std::min(options_.flush_size_bytes, write_bytes - write_offset);
        auto written = co_await WriteStorageBuffer(
            *store->worker, store->files[file_id],
            std::span<const std::byte>(staging.data + write_offset, chunk_bytes),
            pending->write_buffer_id != 0, staging,
            block_offset + write_offset);
        if (!written.ok() || *written != chunk_bytes) {
          co_await store->writer_mutex.Lock();
          UnlockGuard guard(&store->writer_mutex, store->worker);
          BlockState* state = FindBlockState(*store, pending->block_id);
          if (state != nullptr) {
            state->flush_in_progress = false;
            state->flush_queued = false;
          }
          store->write_failed = true;
          store->flush_running = false;
          release_pending(*pending);
          if (!written.ok()) {
            co_return written.status();
          }
          co_return Status(StatusCode::kInternal,
                           "short block flush write");
        }
        write_offset += chunk_bytes;
      }
      auto synced = co_await celer::Fdatasync(*store->worker,
                                              store->files[file_id]);
      if (!synced.ok()) {
        co_await store->writer_mutex.Lock();
        UnlockGuard guard(&store->writer_mutex, store->worker);
        BlockState* state = FindBlockState(*store, pending->block_id);
        if (state != nullptr) {
          state->flush_in_progress = false;
          state->flush_queued = false;
        }
        store->write_failed = true;
        store->flush_running = false;
        release_pending(*pending);
        co_return synced;
      }

      co_await store->writer_mutex.Lock();
      UnlockGuard write_guard(&store->writer_mutex, store->worker);
      BlockState* state = FindBlockState(*store, pending->block_id);
      if (state == nullptr) {
        store->flush_running = false;
        co_return Status::Ok();
      }
      if (!state->allocated || state->flush_in_progress == false ||
          state->allocation_epoch != pending->allocation_epoch) {
        state->flush_in_progress = false;
        state->flush_queued = false;
        release_pending(*pending);
        store->flush_running = false;
        co_return Status::Ok();
      }

      for (const RecordIdentity& identity : pending->staged_records) {
        assert(identity.entry != nullptr);
        RecordLocation& current = identity.entry->value;
        if (current.block_id == pending->block_id &&
            current.allocation_epoch == pending->allocation_epoch) {
          current.in_memory = false;
        }
      }

      state->flush_in_progress = false;
      state->flush_queued = false;
      state->in_memory = false;
      if (state->pins > 0) {
        state->release_pending = true;
        continue;
      }
      ReleaseStagingBuffer(*store, *state);
      MaybeQueueDefrag(*store, pending->block_id);
    }
  }

  bool IsActiveBlock(const WorkerStore& store,
                     std::uint64_t block_id) const noexcept {
    return store.active_block.has_value() &&
           store.active_block->block_id == block_id;
  }

  bool IsDefragCandidate(const WorkerStore& store,
                         std::uint64_t block_id) const noexcept {
    const BlockState* state = FindBlockState(store, block_id);
    if (state == nullptr || !state->allocated || state->defrag_queued ||
        state->defragging || state->pins != 0 || state->in_memory ||
        state->flush_queued || state->flush_in_progress ||
        IsActiveBlock(store, block_id) ||
        state->committed_bytes <= kBlockHeaderBytes) {
      return false;
    }
    const std::uint64_t used = state->committed_bytes - kBlockHeaderBytes;
    const std::uint64_t live_ratio =
        used == 0
            ? 0
            : (static_cast<std::uint64_t>(state->live_bytes) * 1000) / used;
    return live_ratio <= 500;
  }

  void MaybeQueueDefrag(WorkerStore& store, std::uint64_t block_id) {
    BlockState* state = FindBlockState(store, block_id);
    if (state == nullptr || !IsDefragCandidate(store, block_id)) {
      return;
    }
    state->defrag_queued = true;
    store.defrag_queue.push_back(block_id);
    RequestDefrag(store);
  }

  void RequestDefrag(WorkerStore& store) {
    if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
        store.defrag_running || store.defrag_queue.empty()) {
      return;
    }
    store.defrag_running = true;
    active_defrags_.fetch_add(1, std::memory_order_acq_rel);
    store.worker->Spawn(DefragOne(&store));
  }

  Task<Status> DefragOne(WorkerStore* store) {
    if (store->defrag_queue.empty()) {
      store->defrag_running = false;
      active_defrags_.fetch_sub(1, std::memory_order_acq_rel);
      co_return Status::Ok();
    }
    const std::uint64_t candidate = store->defrag_queue.front();
    store->defrag_queue.pop_front();
    BlockState* candidate_state = FindBlockState(*store, candidate);
    if (candidate_state == nullptr) {
      store->defrag_running = false;
      active_defrags_.fetch_sub(1, std::memory_order_acq_rel);
      RequestDefrag(*store);
      co_return Status::Ok();
    }
    candidate_state->defrag_queued = false;

    Status status = co_await CleanBlockLocked(*store, candidate);
    if (!status.ok()) {
      spdlog::error("worker[{}] defrag block {} failed: {}",
                    store->worker->id(), candidate, status.message());
    }
    store->defrag_running = false;
    active_defrags_.fetch_sub(1, std::memory_order_acq_rel);
    if (status.ok()) {
      RequestDefrag(*store);
    }
    co_return status;
  }

  Task<Status> RelocateIfCurrent(unsigned key_owner, std::string_view key,
                                 std::string_view value,
                                 const RecordHeader& record,
                                 const RecordLocation& source_location) {
    WorkerStore& key_store = *stores_[key_owner];
    auto key_lock = co_await key_store.key_locks[record.db_id].Acquire(
        record.digest, IntentLockMode::kExclusive);
    co_await key_store.writer_mutex.Lock();
    UnlockGuard write_unlock(&key_store.writer_mutex, key_store.worker);

    auto& partition = PartitionForKey(key_store, key);
    auto& index = partition.indexes[record.db_id];
    auto* current = index.Find(record.digest, key);
    if (current == nullptr ||
        !current->value.SamePhysicalRecord(source_location)) {
      co_return Status::Ok();
    }

    co_return co_await WriteRecordLocked(
        key_store, record.db_id, key, value, record.kind, record.digest,
        record.generation, record.mutation_sequence,
        record.relocation_sequence + 1, true);
  }

  Task<Status> CleanBlockLocked(WorkerStore& store,
                                std::uint64_t block_id) {
    BlockState* source_ptr = FindBlockState(store, block_id);
    if (source_ptr == nullptr) {
      co_return Status::Ok();
    }
    BlockState& source = *source_ptr;
    if (!source.allocated || source.in_memory || source.pins != 0 ||
        source.flush_queued || source.flush_in_progress ||
        IsActiveBlock(store, block_id)) {
      co_return Status::Ok();
    }
    source.defragging = true;

    struct DefragBuffer {
      RegisteredBufferPool* pool = nullptr;
      std::uint16_t buffer_id = 0;
      std::byte* heap_data = nullptr;
      FixedBuffer buffer{};

      ~DefragBuffer() {
        if (buffer_id != 0) {
          pool->ReleaseWriteBuffer(buffer_id);
        } else if (heap_data != nullptr) {
          pool->ReleaseHeapWriteBuffer(heap_data);
        }
      }

      bool registered() const noexcept { return buffer_id != 0; }
    } block_data{.pool = &store.buffers};
    if (store.buffers.TryAcquireWriteBuffer(&block_data.buffer_id)) {
      block_data.buffer = store.buffers.write_buffer(block_data.buffer_id);
    } else if (store.buffers.TryAcquireHeapWriteBuffer(&block_data.heap_data)) {
      block_data.buffer = FixedBuffer{
          .data = block_data.heap_data,
          .size = options_.buffers.write_buffer_bytes,
          .index = 0,
      };
    } else {
      source.defragging = false;
      co_return Status(StatusCode::kResourceExhausted,
                       "failed to allocate defrag block buffer");
    }
    if (block_data.buffer.size < kStorageBlockBytes) {
      source.defragging = false;
      co_return Status(StatusCode::kResourceExhausted,
                       "defrag block buffer is smaller than a storage block");
    }
    block_data.buffer.size = kStorageBlockBytes;
    const auto [source_file_id, source_block_offset] = FileOffset(block_id);
    auto read = co_await ReadStorageBuffer(
        *store.worker, store.files[source_file_id], block_data.buffer,
        block_data.registered(), source_block_offset);
    if (!read.ok() || *read != kStorageBlockBytes) {
      source.defragging = false;
      co_return read.ok()
                    ? Status(StatusCode::kInternal,
                             "short block read during defrag")
                    : read.status();
    }

    std::uint32_t record_offset = kBlockHeaderBytes;
    while (record_offset < source.committed_bytes) {
      RecordHeader record{};
      std::string_view disk_key;
      std::span<const std::byte> record_bytes(
          block_data.buffer.data + record_offset,
          source.committed_bytes - record_offset);
      if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
          record.allocation_epoch != source.allocation_epoch ||
          record_offset + record.total_disk_bytes > source.committed_bytes) {
        source.defragging = false;
        co_return Status(StatusCode::kInternal,
                         "corrupt committed record during defrag");
      }

      const RecordLocation source_location{
          .block_id = block_id,
          .replication_epoch = record.replication_epoch,
          .mutation_sequence = record.mutation_sequence,
          .allocation_epoch = record.allocation_epoch,
          .block_owner = static_cast<std::uint16_t>(store.worker->id()),
          .record_offset = record_offset,
          .total_disk_bytes = record.total_disk_bytes,
          .value_bytes = record.value_bytes,
          .relocation_sequence = static_cast<std::uint32_t>(
              record.relocation_sequence),
          .kind = record.kind,
      };
      const std::byte* value_data =
          block_data.buffer.data + record_offset + record.header_bytes;
      if (Crc32c(std::span<const std::byte>(value_data,
                                           record.value_bytes)) !=
          record.payload_checksum) {
        source.defragging = false;
        co_return Status(StatusCode::kInternal,
                         "value checksum mismatch during defrag");
      }

      const unsigned key_owner = OwnerForKey(disk_key);
      const std::string key(disk_key);
      const std::string value(reinterpret_cast<const char*>(value_data),
                              record.value_bytes);
      Status relocated;
      if (key_owner == store.worker->id()) {
        relocated = co_await RelocateIfCurrent(
            key_owner, key, value, record, source_location);
      } else {
        relocated = co_await celer::SubmitTaskTo(
            key_owner,
            [this, key_owner, key, value, record,
             source_location]() mutable -> Task<Status> {
              co_return co_await RelocateIfCurrent(
                  key_owner, key, value, record, source_location);
            });
      }
      if (!relocated.ok()) {
        source.defragging = false;
        co_return relocated;
      }
      record_offset += record.total_disk_bytes;
    }

    {
      co_await store.writer_mutex.Lock();
      UnlockGuard write_unlock(&store.writer_mutex, store.worker);
      if (source.live_bytes != 0) {
        source.defragging = false;
        co_return Status::Ok();
      }
      source.freeing = true;
    }
    while (source.pins != 0) {
      Status waited = co_await celer::SleepFor(
          *store.worker, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        source.freeing = false;
        source.defragging = false;
        co_return waited;
      }
    }

    auto* zero_buffer = static_cast<std::byte*>(::operator new[](
        kBlockHeaderBytes, std::align_val_t(options_.buffers.alignment),
        std::nothrow));
    if (zero_buffer == nullptr) {
      source.freeing = false;
      source.defragging = false;
      store.write_failed = true;
      co_return Status(StatusCode::kResourceExhausted,
                       "failed to allocate temporary zero header buffer");
    }
    std::fill_n(zero_buffer, kBlockHeaderBytes, std::byte{0});
    FixedBuffer zero{.data = zero_buffer, .size = kBlockHeaderBytes, .index = 0};
    std::fill_n(zero.data, zero.size, std::byte{0});
    auto written = co_await WriteStorageBuffer(
        *store.worker, store.files[source_file_id],
        std::span<const std::byte>(zero.data, zero.size),
        false, {}, source_block_offset);
    if (!written.ok() || *written != kBlockHeaderBytes) {
      ::operator delete[](zero_buffer,
                          std::align_val_t(options_.buffers.alignment));
      source.freeing = false;
      source.defragging = false;
      store.write_failed = true;
      co_return written.ok()
                    ? Status(StatusCode::kInternal,
                             "short free-header write during defrag")
                    : written.status();
    }
    Status sync =
        co_await celer::Fdatasync(*store.worker, store.files[source_file_id]);
    if (!sync.ok()) {
      ::operator delete[](zero_buffer,
                          std::align_val_t(options_.buffers.alignment));
      source.freeing = false;
      source.defragging = false;
      store.write_failed = true;
      co_return sync;
    }

    ::operator delete[](zero_buffer,
                        std::align_val_t(options_.buffers.alignment));

    store.block_states.erase(block_id);
    if (!free_blocks_.enqueue(block_id)) {
      co_return Status(StatusCode::kResourceExhausted,
                       "failed to return block to free MPMC queue");
    }
    free_block_count_.fetch_add(1, std::memory_order_release);
    co_return Status::Ok();
  }

  StorageEngineOptions options_;
  unsigned worker_count_ = 0;
  std::uint64_t blocks_per_file_ = 0;
  std::uint64_t total_blocks_ = 0;
  std::vector<std::unique_ptr<WorkerStore>> stores_;
  std::unique_ptr<CoroutineBarrier> open_barrier_;
  std::unique_ptr<CoroutineBarrier> metadata_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_accounting_barrier_;
  std::unique_ptr<CoroutineBarrier> free_list_barrier_;
  std::atomic<std::uint64_t> recovery_scanned_blocks_{0};
  std::atomic<std::uint64_t> recovery_scanned_records_{0};
  std::atomic<std::int64_t> recovery_next_log_ms_{0};
  std::atomic<bool> recovery_complete_logged_{false};
  std::int64_t recovery_started_ms_ = 0;
  moodycamel::ConcurrentQueue<std::uint64_t> free_blocks_;
  std::atomic<std::size_t> free_block_count_{0};
  static constexpr std::size_t kDefragReserveBlocks = 8;
  std::atomic<std::uint64_t> next_block_id_{0};
  std::atomic<std::uint64_t> next_allocation_epoch_{1};
  std::atomic<std::uint64_t> next_lsn_{1};
  std::array<std::atomic<std::uint64_t>, kLogicalDatabaseCount> db_epochs_{};
  std::atomic<unsigned> active_defrags_{0};
  std::atomic<bool> shutdown_flush_requested_{false};
  std::atomic<unsigned> shutdown_flush_completed_{0};
  std::atomic<bool> shutdown_flush_failed_{false};
  moodycamel::ConcurrentQueue<std::uint16_t> replication_ready_;
};

StorageEngine::StorageEngine(StorageEngineOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

StorageEngine::~StorageEngine() = default;

Status StorageEngine::Prepare(unsigned worker_count) {
  return impl_->Prepare(worker_count);
}

Task<Status> StorageEngine::InitializeWorker(Worker& worker) {
  co_return co_await impl_->InitializeWorker(worker);
}

Status StorageEngine::FlushForShutdown() {
  return impl_->FlushForShutdown();
}

unsigned StorageEngine::OwnerForKey(std::string_view key) const noexcept {
  return impl_->OwnerForKey(key);
}

unsigned StorageEngine::worker_count() const noexcept {
  return impl_->worker_count();
}

std::size_t StorageEngine::LocalSize(std::uint8_t db_id) const noexcept {
  return impl_->LocalSize(db_id);
}

ScanBatch StorageEngine::ScanPartition(std::uint16_t partition_id,
                                       std::uint8_t db_id,
                                       std::uint64_t cursor,
                                       std::size_t count) const {
  return impl_->ScanPartition(partition_id, db_id, cursor, count);
}

Task<Status> StorageEngine::FlushDb(std::uint8_t db_id) {
  co_return co_await impl_->FlushDb(db_id);
}

std::uint64_t StorageEngine::DbEpoch(std::uint8_t db_id) const noexcept {
  return impl_->DbEpoch(db_id);
}

PartitionReplicationStart StorageEngine::BeginPartitionReplication(
    std::uint16_t partition_id) {
  return impl_->BeginPartitionReplication(partition_id);
}

Task<StatusOr<PartitionSnapshotBatch>> StorageEngine::SnapshotPartition(
    std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
    std::size_t count) {
  co_return co_await impl_->SnapshotPartition(partition_id, db_id, cursor,
                                              count);
}

PartitionDeltaBatch StorageEngine::ReadPartitionDeltas(
    std::uint16_t partition_id, std::uint64_t after_sequence,
    std::size_t count) {
  return impl_->ReadPartitionDeltas(partition_id, after_sequence, count);
}

bool StorageEngine::TryTakeReplicationReady(std::uint16_t* partition_id) {
  return impl_->TryTakeReplicationReady(partition_id);
}

void StorageEngine::AcknowledgePartitionDeltas(
    std::uint16_t partition_id, std::uint64_t through_sequence) {
  impl_->AcknowledgePartitionDeltas(partition_id, through_sequence);
}

Task<StatusOr<std::uint64_t>> StorageEngine::ResetReplicaPartition(
    std::uint16_t partition_id,
    std::span<const std::uint64_t, 16> source_db_epochs) {
  co_return co_await impl_->ResetReplicaPartition(partition_id,
                                                  source_db_epochs);
}

Task<Status> StorageEngine::ApplyReplicaRecords(
    std::uint16_t partition_id, std::uint64_t replication_epoch,
    std::span<const SnapshotRecord> records) {
  co_return co_await impl_->ApplyReplicaRecords(partition_id,
                                                replication_epoch, records);
}

Task<StatusOr<DiskValue>> StorageEngine::Get(std::uint8_t db_id,
                                             std::string_view key,
                                             ReadLatencyTrace* trace) {
  co_return co_await impl_->Get(db_id, key, trace);
}

Task<Status> StorageEngine::Set(std::uint8_t db_id, std::string_view key,
                                std::string_view value) {
  co_return co_await impl_->Set(db_id, key, value);
}

Task<StatusOr<bool>> StorageEngine::Delete(std::uint8_t db_id,
                                           std::string_view key) {
  co_return co_await impl_->Delete(db_id, key);
}

Task<bool> StorageEngine::Exists(std::uint8_t db_id, std::string_view key) {
  co_return co_await impl_->Exists(db_id, key);
}

Task<StatusOr<std::int64_t>> StorageEngine::Increment(
    std::uint8_t db_id, std::string_view key) {
  co_return co_await impl_->Increment(db_id, key);
}

}  // namespace keylane::storage
