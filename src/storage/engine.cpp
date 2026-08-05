#include "keylane/storage/engine.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
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
  std::uint32_t record_offset = 0;
  std::uint32_t total_disk_bytes = 0;
  std::uint32_t value_bytes = 0;
  std::uint64_t generation = 0;
  std::uint64_t relocation_sequence = 0;
  std::uint64_t lsn = 0;
  std::uint64_t allocation_epoch = 0;
  bool in_memory = false;
  RecordKind kind = RecordKind::kValue;

  bool SamePhysicalRecord(const RecordLocation& other) const noexcept {
    return block_id == other.block_id &&
           record_offset == other.record_offset &&
           allocation_epoch == other.allocation_epoch;
  }
};

bool IsNewer(const RecordLocation& candidate,
             const RecordLocation& current) noexcept {
  if (candidate.generation != current.generation) {
    return candidate.generation > current.generation;
  }
  if (candidate.relocation_sequence != current.relocation_sequence) {
    return candidate.relocation_sequence > current.relocation_sequence;
  }
  return candidate.lsn > current.lsn;
}

struct ActiveBlock {
  std::uint64_t block_id = 0;
  std::uint32_t storage_shard_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = kBlockHeaderBytes;
  std::uint32_t record_count = 0;
  std::uint64_t max_lsn = 0;
  std::uint16_t write_buffer_id = 0;
  std::byte* heap_buffer = nullptr;
  std::size_t heap_buffer_size = 0;
};

struct BlockState {
  std::uint32_t storage_shard_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = 0;
  std::uint32_t live_bytes = 0;
  std::uint32_t pins = 0;
  bool allocated = false;
  bool defragging = false;
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
  RecordLocation location{};
};

struct RecoveryBlock {
  ActiveBlock block{};
};

struct RecoveryBatch {
  std::vector<RecoveryRecord> records;
  std::vector<RecoveryBlock> blocks;
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

  struct WorkerStore {
    explicit WorkerStore(std::size_t total_blocks)
        : block_states(total_blocks) {}

    Worker* worker = nullptr;
    RegisteredBufferPool buffers;
    std::vector<FixedFile> files;
    absl::flat_hash_map<Digest, RecordLocation, DigestHash> index;
  absl::flat_hash_map<std::uint64_t, std::vector<Digest>> staged_records;
    std::size_t live_key_count = 0;
    IntentLockTable key_locks;
    std::array<std::optional<ActiveBlock>, kLogicalStorageShards> active_blocks;
    std::vector<BlockState> block_states;
    AsyncMutex writer_mutex;
    std::deque<std::uint64_t> flush_queue;
    bool flush_running = false;
    bool write_failed = false;
    bool defrag_running = false;
  };

  Status Prepare(unsigned worker_count) {
    if (worker_count == 0 || options_.data_files.empty()) {
      return Status(StatusCode::kInvalidArgument,
                    "storage requires workers and at least one data file");
    }
    if (options_.data_files.size() >
        std::numeric_limits<std::uint16_t>::max()) {
      return Status(StatusCode::kOutOfRange, "too many data files");
    }
    if (options_.file_size_bytes < kStorageBlockBytes ||
        options_.file_size_bytes % kStorageBlockBytes != 0) {
      return Status(StatusCode::kInvalidArgument,
                    "data file size must be a positive multiple of 8 MiB");
    }

    for (const std::string& path : options_.data_files) {
      struct stat file_info {};
      if (::stat(path.c_str(), &file_info) == 0 &&
          S_ISBLK(file_info.st_mode)) {
        spdlog::info(
            "using raw block device {} from offset 0 (configured bytes={})",
            path, options_.file_size_bytes);
        continue;
      }
      Status status = PrepareDataFile(path, options_.file_size_bytes);
      if (!status.ok()) {
        return status;
      }
    }

    worker_count_ = worker_count;
    blocks_per_file_ = options_.file_size_bytes / kStorageBlockBytes;
    total_blocks_ = blocks_per_file_ * options_.data_files.size();
    stores_.reserve(worker_count);
    for (unsigned i = 0; i < worker_count; ++i) {
      stores_.push_back(std::make_unique<WorkerStore>(total_blocks_));
    }
    open_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
    recovery_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
    free_list_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
    return Status::Ok();
  }

  Task<Status> InitializeWorker(Worker& worker) {
    WorkerStore& store = *stores_[worker.id()];
    store.worker = &worker;
    store.key_locks.Bind(worker);

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
    worker.Spawn(PeriodicFlush(&store));
    co_return Status::Ok();
  }

  unsigned OwnerForKey(std::string_view key) const noexcept {
    return StorageShardForKey(key) % worker_count_;
  }

  Task<StatusOr<DiskValue>> Get(std::string_view key) {
    WorkerStore& store = CurrentStore();
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks.Acquire(digest, IntentLockMode::kShared);
    auto found = store.index.find(digest);
    if (found == store.index.end() ||
        found->second.kind == RecordKind::kTombstone) {
      co_return Status(StatusCode::kNotFound, "key not found");
    }

    auto loaded = co_await LoadValue(store, key, digest, found->second);
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

  Task<Status> Set(std::string_view key, std::string_view value) {
    WorkerStore& store = CurrentStore();
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks.Acquire(digest, IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);
    co_return co_await AppendLocked(store, key, value, RecordKind::kValue);
  }

  Task<StatusOr<bool>> Delete(std::string_view key) {
    WorkerStore& store = CurrentStore();
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks.Acquire(digest, IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);

    auto found = store.index.find(digest);
    if (found == store.index.end() ||
        found->second.kind == RecordKind::kTombstone) {
      co_return false;
    }
    Status status =
        co_await AppendLocked(store, key, {}, RecordKind::kTombstone);
    if (!status.ok()) {
      co_return status;
    }
    co_return true;
  }

  Task<bool> Exists(std::string_view key) {
    WorkerStore& store = CurrentStore();
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks.Acquire(digest, IntentLockMode::kShared);
    auto found = store.index.find(digest);
    co_return found != store.index.end() &&
              found->second.kind == RecordKind::kValue;
  }

  Task<StatusOr<std::int64_t>> Increment(std::string_view key) {
    WorkerStore& store = CurrentStore();
    const Digest digest = ComputeDigest(key);
    auto key_lock =
        co_await store.key_locks.Acquire(digest, IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);

    std::int64_t value = 0;
    auto found = store.index.find(digest);
    if (found != store.index.end() &&
        found->second.kind == RecordKind::kValue) {
      auto loaded = co_await LoadValue(store, key, digest, found->second);
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
        co_await AppendLocked(store, key, encoded, RecordKind::kValue);
    if (!status.ok()) {
      co_return status;
    }
    co_return value;
  }

  unsigned worker_count() const noexcept { return worker_count_; }

  std::size_t LocalSize() const noexcept {
    return CurrentStore().live_key_count;
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

  std::pair<std::uint32_t, std::uint64_t> FileOffset(
      std::uint64_t block_id) const noexcept {
    const std::uint32_t file_id =
        static_cast<std::uint32_t>(block_id / blocks_per_file_);
    const std::uint64_t local_block = block_id % blocks_per_file_;
    return {file_id, local_block * kStorageBlockBytes};
  }

  void Fail(const Status& status) {
    open_barrier_->Abort(status);
    recovery_barrier_->Abort(status);
    free_list_barrier_->Abort(status);
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

      const unsigned target =
          block.storage_shard_id % worker_count_;
      batches->at(target).blocks.push_back(RecoveryBlock{ActiveBlock{
          .block_id = block_id,
          .storage_shard_id = block.storage_shard_id,
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
            StorageShardForKey(key) % worker_count_ !=
                block.storage_shard_id % worker_count_ ||
            record_offset + record.total_disk_bytes > block.committed_bytes) {
          co_return Status(StatusCode::kInternal,
                           "invalid or corrupt committed record header");
        }
        AtomicMax(&next_lsn_, record.lsn + 1);
        batches->at(target).records.push_back(RecoveryRecord{
            .digest = record.digest,
            .location = RecordLocation{
                .block_id = block_id,
                .record_offset = record_offset,
                .total_disk_bytes = record.total_disk_bytes,
                .value_bytes = record.value_bytes,
                .generation = record.generation,
                .relocation_sequence = record.relocation_sequence,
                .lsn = record.lsn,
                .allocation_epoch = record.allocation_epoch,
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
    }
    co_return Status::Ok();
  }

  void ApplyRecovery(unsigned target, RecoveryBatch batch) {
    WorkerStore& store = *stores_[target];
    for (const RecoveryBlock& recovered : batch.blocks) {
      const ActiveBlock& block = recovered.block;
      BlockState& state = store.block_states[block.block_id];
      state.storage_shard_id = block.storage_shard_id;
      state.allocation_epoch = block.allocation_epoch;
      state.committed_bytes = block.committed_bytes;
      state.allocated = true;

      // Recovered blocks have no staging buffer. Keep partial blocks sealed;
      // appending to one would otherwise dereference an absent in-memory copy.
    }

    for (const RecoveryRecord& recovered : batch.records) {
      auto found = store.index.find(recovered.digest);
      if (found == store.index.end() ||
          IsNewer(recovered.location, found->second)) {
        // TODO: add large-record reconstruction on recovery:
        // gather all chunks for a digest and coalesce into a logical key value.
        const bool was_live =
          found != store.index.end() &&
          found->second.kind == RecordKind::kValue;
        const bool is_live = recovered.location.kind == RecordKind::kValue;
        if (found != store.index.end()) {
          BlockState& old_state =
              store.block_states[found->second.block_id];
          old_state.live_bytes -=
              std::min(old_state.live_bytes,
                       found->second.total_disk_bytes);
        }
        store.index.insert_or_assign(recovered.digest, recovered.location);
        if (was_live != is_live) {
          if (is_live) {
            ++store.live_key_count;
          } else {
            --store.live_key_count;
          }
        }
        BlockState& new_state =
            store.block_states[recovered.location.block_id];
        new_state.live_bytes += recovered.location.total_disk_bytes;
        new_state.in_memory = false;
        new_state.heap_data = nullptr;
        new_state.heap_data_size = 0;
        new_state.write_buffer_id = 0;
        new_state.release_pending = false;
        new_state.flush_queued = false;
        new_state.flush_in_progress = false;
      }
    }
  }

  Task<StatusOr<LoadedValue>> LoadValue(WorkerStore& store,
                                        std::string_view key,
                                        const Digest& digest,
                                        RecordLocation location) {
    // TODO: Coalesce concurrent reads of the same aligned disk page, like
    // the reference engine tiering::OpManager::pending_reads_. Key the in-flight table by
    // (file_id, aligned offset, aligned length), submit one read, and fan the
    // decoded result out to all waiting coroutines. In-flight operations must
    // retain values/leases, never flat_hash_map iterators or element pointers.
    if (location.block_id >= store.block_states.size()) {
      co_return Status(StatusCode::kInternal, "index block is out of range");
    }
    BlockState& state = store.block_states[location.block_id];
    if (!state.allocated ||
        state.allocation_epoch != location.allocation_epoch) {
      co_return Status(StatusCode::kInternal, "stale index block epoch");
    }
    if (location.in_memory && state.in_memory) {
      ++state.pins;
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
      } pin{&store, &state};

      auto in_mem_buffer =
          StagingBufferFor(state, store.buffers);
      if (!in_mem_buffer.data || in_mem_buffer.size == 0 ||
          location.record_offset + location.total_disk_bytes > in_mem_buffer.size) {
        co_return Status(StatusCode::kInternal, "invalid in-memory location");
      }
      auto acquired = co_await store.buffers.AcquireReadBuffer();
      if (!acquired.ok()) {
        co_return acquired.status();
      }
      ReadBufferLease lease = std::move(*acquired);
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
          record.digest != digest || disk_key != key ||
          record.kind != RecordKind::kValue ||
          record.generation != location.generation ||
          record.relocation_sequence != location.relocation_sequence ||
          record.allocation_epoch != location.allocation_epoch ||
          location.value_bytes != record.value_bytes ||
          location.total_disk_bytes != record.total_disk_bytes) {
        co_return Status(StatusCode::kInternal,
                         "record does not match in-memory location");
      }
      std::memcpy(io.data, record_bytes + record.header_bytes,
                  location.value_bytes);
      if (Crc32c(std::span<const std::byte>(io.data, record.value_bytes)) !=
          record.payload_checksum) {
        co_return Status(StatusCode::kInternal,
                         "record value checksum mismatch");
      }
      co_return LoadedValue{std::move(lease), record.value_bytes};
    }

    auto acquired = co_await store.buffers.AcquireReadBuffer();
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer io = lease.io_buffer();

    const auto [file_id, block_offset] = FileOffset(location.block_id);
    const std::uint64_t absolute_offset =
        block_offset + location.record_offset;
    const std::uint64_t aligned_offset =
        absolute_offset & ~(static_cast<std::uint64_t>(kDirectIoAlignment) - 1);
    const std::size_t record_headroom =
        static_cast<std::size_t>(absolute_offset - aligned_offset);
    const std::size_t read_bytes =
        AlignDirect(record_headroom + location.total_disk_bytes);
    if (read_bytes > io.size) {
      co_return Status(StatusCode::kOutOfRange,
                       "record exceeds registered read buffer capacity");
    }
    FixedBuffer record_buffer = io;
    record_buffer.size = read_bytes;
    auto read = co_await ReadStorageBuffer(
        *store.worker, store.files[file_id], record_buffer,
        lease.registered(), aligned_offset);
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
        record.digest != digest || disk_key != key ||
        record.kind != RecordKind::kValue ||
        record.generation != location.generation ||
        record.relocation_sequence != location.relocation_sequence ||
        record.allocation_epoch != location.allocation_epoch ||
        record.value_bytes != location.value_bytes ||
        record.total_disk_bytes != location.total_disk_bytes) {
      co_return Status(StatusCode::kInternal,
                       "record does not match in-memory location");
    }
    const std::byte* value_data = record_data + record.header_bytes;
    if (Crc32c(std::span<const std::byte>(value_data, record.value_bytes)) !=
        record.payload_checksum) {
      co_return Status(StatusCode::kInternal, "record value checksum mismatch");
    }
    std::memmove(io.data, value_data, record.value_bytes);
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

  Task<Status> AppendLocked(WorkerStore& store, std::string_view key,
                            std::string_view value, RecordKind kind) {
    const Digest digest = ComputeDigest(key);
    auto previous = store.index.find(digest);
    const std::uint64_t generation =
        previous == store.index.end() ? 1 : previous->second.generation + 1;
    co_return co_await WriteRecordLocked(store, key, value, kind, digest,
                                         generation, 0, false);
  }

  Task<Status> WriteRecordLocked(WorkerStore& store, std::string_view key,
                                 std::string_view value, RecordKind kind,
                                 const Digest& digest,
                                 std::uint64_t generation,
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

    // A worker owns one append stream. Records for different logical key
    // shards can share the same block because they are all routed to this
    // worker. The remaining registered write buffers cover blocks being
    // flushed; heap buffers are the fallback when those are all busy.
    const std::uint32_t shard =
        static_cast<std::uint32_t>(store.worker->id());
    auto previous_it = store.index.find(digest);
    const std::optional<RecordLocation> previous =
        previous_it == store.index.end()
            ? std::nullopt
            : std::optional<RecordLocation>(previous_it->second);
    const std::uint64_t lsn =
        next_lsn_.fetch_add(1, std::memory_order_relaxed);

    auto& active = store.active_blocks[shard];
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
          .storage_shard_id = shard,
          .allocation_epoch = next_allocation_epoch_.fetch_add(
              1, std::memory_order_relaxed),
          .committed_bytes = kBlockHeaderBytes,
          .record_count = 0,
          .max_lsn = 0,
          .write_buffer_id = write_buffer_id,
          .heap_buffer = heap_buffer,
          .heap_buffer_size = options_.buffers.write_buffer_bytes,
      };
      BlockState& state = store.block_states[block_id];
      state = BlockState{};
      state.storage_shard_id = shard;
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
          .storage_shard_id = shard,
          .allocation_epoch = active->allocation_epoch,
          .committed_bytes = kBlockHeaderBytes,
          .record_count = 0,
          .max_lsn = 0,
          .checksum = 0,
          .reserved = 0,
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

    BlockState& state = store.block_states[updated.block_id];
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
        .flags = 0,
        .digest = digest,
        .key_bytes = static_cast<std::uint32_t>(key.size()),
        .value_bytes = static_cast<std::uint32_t>(value.size()),
        .value_disk_bytes = static_cast<std::uint32_t>(value_disk_bytes),
        .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
        .generation = generation,
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
        .storage_shard_id = shard,
        .allocation_epoch = updated.allocation_epoch,
        .committed_bytes = updated.committed_bytes,
        .record_count = updated.record_count,
        .max_lsn = updated.max_lsn,
        .checksum = 0,
        .reserved = 0,
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
        .record_offset = record_offset,
        .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
        .value_bytes = static_cast<std::uint32_t>(value.size()),
        .generation = generation,
        .relocation_sequence = relocation_sequence,
        .lsn = lsn,
        .allocation_epoch = updated.allocation_epoch,
        .in_memory = true,
        .kind = kind,
    };
    const bool was_live =
        previous.has_value() && previous->kind == RecordKind::kValue;
    const bool is_live = kind == RecordKind::kValue;
    if (previous.has_value()) {
      BlockState& old_state = store.block_states[previous->block_id];
      old_state.live_bytes -= std::min(old_state.live_bytes,
                                       previous->total_disk_bytes);
    }
    store.index.insert_or_assign(digest, location);
    store.staged_records[updated.block_id].push_back(digest);
    if (was_live != is_live) {
      if (is_live) {
        ++store.live_key_count;
      } else {
        --store.live_key_count;
      }
    }
    state.committed_bytes = updated.committed_bytes;
    state.in_memory = true;
    state.write_buffer_id = updated.write_buffer_id;
    state.heap_data = updated.heap_buffer;
    state.heap_data_size = updated.heap_buffer_size;
    state.live_bytes += location.total_disk_bytes;
    state.flush_queued = updated.committed_bytes == kStorageBlockBytes;
    if (!for_defrag) {
      RequestDefrag(store);
    }
    co_return Status::Ok();
  }

  void SealActiveBlocks(WorkerStore& store) {
    for (auto& active : store.active_blocks) {
      if (!active.has_value() ||
          active->committed_bytes <= kBlockHeaderBytes) {
        continue;
      }
      RequestFlush(store, active->block_id);
      active.reset();
    }
  }

  Task<Status> FlushWorkerForShutdown(WorkerStore* store) {
    while (store->defrag_running) {
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
    BlockState& state = store.block_states[block_id];
    if (!state.allocated || !state.in_memory ||
        (state.write_buffer_id == 0 && state.heap_data == nullptr)) {
      return;
    }
    if (state.flush_queued || state.flush_in_progress) {
      return;
    }
    state.flush_queued = true;
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
      std::vector<Digest> staged_records;
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
        if (block_id >= store->block_states.size()) {
          continue;
        }
        BlockState& state = store->block_states[block_id];
        if (!state.allocated || !state.in_memory ||
            (state.write_buffer_id == 0 && state.heap_data == nullptr) ||
            state.flush_in_progress) {
          state.flush_queued = false;
          continue;
        }
        if (state.pins > 0) {
          store->flush_queue.push_back(block_id);
          state.flush_queued = true;
          continue;
        }

        pending.emplace(PendingFlush{
            .block_id = block_id,
            .committed_bytes = state.committed_bytes,
            .allocation_epoch = state.allocation_epoch,
            .write_buffer_id = state.write_buffer_id,
            .heap_data = state.heap_data,
            .heap_data_size = state.heap_data_size,
        });
        if (auto found = store->staged_records.find(block_id);
            found != store->staged_records.end()) {
          pending->staged_records = std::move(found->second);
          store->staged_records.erase(found);
        }
        state.flush_queued = false;
        state.flush_in_progress = true;
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
        BlockState& state = store->block_states[pending->block_id];
        state.flush_in_progress = false;
        state.flush_queued = false;
        store->write_failed = true;
        store->flush_running = false;
        release_pending(*pending);
        co_return Status(StatusCode::kInternal,
                         "invalid pending flush staging buffer");
      }

      auto written = co_await WriteStorageBuffer(
          *store->worker, store->files[file_id],
          std::span<const std::byte>(staging.data, write_bytes),
          pending->write_buffer_id != 0, staging,
          block_offset);
      if (!written.ok()) {
        co_await store->writer_mutex.Lock();
        UnlockGuard guard(&store->writer_mutex, store->worker);
        BlockState& state = store->block_states[pending->block_id];
        state.flush_in_progress = false;
        state.flush_queued = false;
        store->write_failed = true;
        store->flush_running = false;
        release_pending(*pending);
        co_return written.status();
      }
      if (*written != write_bytes) {
        co_await store->writer_mutex.Lock();
        UnlockGuard guard(&store->writer_mutex, store->worker);
        BlockState& state = store->block_states[pending->block_id];
        state.flush_in_progress = false;
        state.flush_queued = false;
        store->write_failed = true;
        store->flush_running = false;
        release_pending(*pending);
        co_return Status(StatusCode::kInternal,
                         "short block flush write");
      }
      auto synced = co_await celer::Fdatasync(*store->worker,
                                              store->files[file_id]);
      if (!synced.ok()) {
        co_await store->writer_mutex.Lock();
        UnlockGuard guard(&store->writer_mutex, store->worker);
        BlockState& state = store->block_states[pending->block_id];
        state.flush_in_progress = false;
        state.flush_queued = false;
        store->write_failed = true;
        store->flush_running = false;
        release_pending(*pending);
        co_return synced;
      }

      co_await store->writer_mutex.Lock();
      UnlockGuard write_guard(&store->writer_mutex, store->worker);
      if (pending->block_id >= store->block_states.size()) {
        store->flush_running = false;
        co_return Status::Ok();
      }
      BlockState& state = store->block_states[pending->block_id];
      if (!state.allocated || state.flush_in_progress == false ||
          state.allocation_epoch != pending->allocation_epoch) {
        state.flush_in_progress = false;
        state.flush_queued = false;
        release_pending(*pending);
        store->flush_running = false;
        co_return Status::Ok();
      }

      for (const Digest& digest : pending->staged_records) {
        auto current = store->index.find(digest);
        if (current != store->index.end() &&
            current->second.block_id == pending->block_id &&
            current->second.allocation_epoch == pending->allocation_epoch) {
          current->second.in_memory = false;
        }
      }

      state.flush_in_progress = false;
      state.flush_queued = false;
      state.in_memory = false;
      if (state.pins > 0) {
        state.release_pending = true;
        continue;
      }
      ReleaseStagingBuffer(*store, state);
    }
  }

  bool IsActiveBlock(const WorkerStore& store,
                     std::uint64_t block_id) const noexcept {
    for (const auto& active : store.active_blocks) {
      if (active.has_value() && active->block_id == block_id) {
        return true;
      }
    }
    return false;
  }

  std::optional<std::uint64_t> SelectDefragCandidate(
      const WorkerStore& store) const noexcept {
    std::optional<std::uint64_t> best;
    std::uint64_t best_live_ratio = std::numeric_limits<std::uint64_t>::max();
    for (std::uint64_t block_id = 0; block_id < store.block_states.size();
         ++block_id) {
      const BlockState& state = store.block_states[block_id];
      if (!state.allocated || state.defragging || state.pins != 0 ||
          state.in_memory || state.flush_queued || state.flush_in_progress ||
          IsActiveBlock(store, block_id) ||
          state.committed_bytes <= kBlockHeaderBytes) {
        continue;
      }
      const std::uint64_t used = state.committed_bytes - kBlockHeaderBytes;
      const std::uint64_t ratio =
          used == 0 ? 0 : (static_cast<std::uint64_t>(state.live_bytes) * 1000) / used;
      if (ratio > 500 || ratio >= best_live_ratio) {
        continue;
      }
      best = block_id;
      best_live_ratio = ratio;
    }
    return best;
  }

  void RequestDefrag(WorkerStore& store) {
    if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
        store.defrag_running || !SelectDefragCandidate(store).has_value()) {
      return;
    }
    store.defrag_running = true;
    store.worker->Spawn(DefragOne(&store));
  }

  Task<Status> DefragOne(WorkerStore* store) {
    const std::optional<std::uint64_t> candidate =
        SelectDefragCandidate(*store);
    Status status = Status::Ok();
    if (candidate.has_value()) {
      status = co_await CleanBlockLocked(*store, *candidate);
      if (!status.ok()) {
        spdlog::error("worker[{}] defrag block {} failed: {}",
                      store->worker->id(), *candidate, status.message());
      }
    }
    store->defrag_running = false;
    co_return status;
  }

  Task<Status> CleanBlockLocked(WorkerStore& store,
                                std::uint64_t block_id) {
    BlockState& source = store.block_states[block_id];
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
          .record_offset = record_offset,
          .total_disk_bytes = record.total_disk_bytes,
          .value_bytes = record.value_bytes,
          .generation = record.generation,
          .relocation_sequence = record.relocation_sequence,
          .lsn = record.lsn,
          .allocation_epoch = record.allocation_epoch,
          .kind = record.kind,
      };
      auto current = store.index.find(record.digest);
      if (current != store.index.end() &&
          current->second.SamePhysicalRecord(source_location)) {
        auto key_lock = co_await store.key_locks.Acquire(
            record.digest, IntentLockMode::kExclusive);
        co_await store.writer_mutex.Lock();
        UnlockGuard write_unlock(&store.writer_mutex, store.worker);

        // Both foreground writes and earlier defrag work may have replaced the
        // location while this cleaner was waiting. Never relocate a stale copy.
        current = store.index.find(record.digest);
        if (current == store.index.end() ||
            !current->second.SamePhysicalRecord(source_location)) {
          record_offset += record.total_disk_bytes;
          continue;
        }

        const std::string key(disk_key);
        const std::byte* value_data =
            block_data.buffer.data + record_offset + record.header_bytes;
        const std::string_view value(
            reinterpret_cast<const char*>(value_data), record.value_bytes);
        if (Crc32c(std::span<const std::byte>(value_data,
                                             record.value_bytes)) !=
            record.payload_checksum) {
          source.defragging = false;
          co_return Status(StatusCode::kInternal,
                           "value checksum mismatch during defrag");
        }

        Status relocated = co_await WriteRecordLocked(
            store, key, value, record.kind, record.digest, record.generation,
            record.relocation_sequence + 1, true);
        if (!relocated.ok()) {
          source.defragging = false;
          co_return relocated;
        }
      }
      record_offset += record.total_disk_bytes;
    }

    co_await store.writer_mutex.Lock();
    UnlockGuard write_unlock(&store.writer_mutex, store.worker);
    if (source.live_bytes != 0 || source.pins != 0) {
      source.defragging = false;
      co_return Status::Ok();
    }

    auto* zero_buffer = static_cast<std::byte*>(::operator new[](
        kBlockHeaderBytes, std::align_val_t(options_.buffers.alignment),
        std::nothrow));
    if (zero_buffer == nullptr) {
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
      source.defragging = false;
      store.write_failed = true;
      co_return sync;
    }

    ::operator delete[](zero_buffer,
                        std::align_val_t(options_.buffers.alignment));

    source = BlockState{};
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
  std::unique_ptr<CoroutineBarrier> recovery_barrier_;
  std::unique_ptr<CoroutineBarrier> free_list_barrier_;
  moodycamel::ConcurrentQueue<std::uint64_t> free_blocks_;
  std::atomic<std::size_t> free_block_count_{0};
  static constexpr std::size_t kDefragReserveBlocks = 8;
  std::atomic<std::uint64_t> next_block_id_{0};
  std::atomic<std::uint64_t> next_allocation_epoch_{1};
  std::atomic<std::uint64_t> next_lsn_{1};
  std::atomic<bool> shutdown_flush_requested_{false};
  std::atomic<unsigned> shutdown_flush_completed_{0};
  std::atomic<bool> shutdown_flush_failed_{false};
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

std::size_t StorageEngine::LocalSize() const noexcept {
  return impl_->LocalSize();
}

Task<StatusOr<DiskValue>> StorageEngine::Get(std::string_view key) {
  co_return co_await impl_->Get(key);
}

Task<Status> StorageEngine::Set(std::string_view key, std::string_view value) {
  co_return co_await impl_->Set(key, value);
}

Task<StatusOr<bool>> StorageEngine::Delete(std::string_view key) {
  co_return co_await impl_->Delete(key);
}

Task<bool> StorageEngine::Exists(std::string_view key) {
  co_return co_await impl_->Exists(key);
}

Task<StatusOr<std::int64_t>> StorageEngine::Increment(std::string_view key) {
  co_return co_await impl_->Increment(key);
}

}  // namespace keylane::storage
