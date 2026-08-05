#include "keylane/storage/engine.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cerrno>
#include <coroutine>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

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
};

struct BlockState {
  std::uint32_t storage_shard_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = 0;
  std::uint32_t live_bytes = 0;
  std::uint32_t pins = 0;
  bool allocated = false;
  bool defragging = false;
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
    std::size_t live_key_count = 0;
    IntentLockTable key_locks;
    std::array<std::optional<ActiveBlock>, kLogicalStorageShards> active_blocks;
    std::vector<BlockState> block_states;
    AsyncMutex writer_mutex;
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
    co_return co_await free_list_barrier_->Wait(worker);
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

 private:
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

      BlockHeader block{};
      if (!DecodeBlockHeader(block_bytes, &block)) {
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
        read = co_await ReadStorageBuffer(
            *store.worker, store.files[file_id], header_buffer,
            lease.registered(), block_offset + record_offset);
        if (!read.ok()) {
          co_return read.status();
        }
        if (*read != kRecordHeaderBytes) {
          co_return Status(StatusCode::kInternal,
                           "short read while scanning record header");
        }

        RecordHeader record{};
        std::string_view key;
        std::span<const std::byte, kRecordHeaderBytes> record_bytes(
            header_buffer.data, kRecordHeaderBytes);
        if (!DecodeRecordHeader(record_bytes, &record, &key) ||
            record.allocation_epoch != block.allocation_epoch ||
            StorageShardForKey(key) != block.storage_shard_id ||
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

      auto& active = store.active_blocks[block.storage_shard_id];
      if (block.committed_bytes < kStorageBlockBytes &&
          (!active.has_value() ||
           block.allocation_epoch > active->allocation_epoch)) {
        active = block;
      }
    }

    for (const RecoveryRecord& recovered : batch.records) {
      auto found = store.index.find(recovered.digest);
      if (found == store.index.end() ||
          IsNewer(recovered.location, found->second)) {
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
    ++state.pins;
    struct PinGuard {
      BlockState* state;
      ~PinGuard() { --state->pins; }
    } pin{&state};

    auto acquired = co_await store.buffers.AcquireReadBuffer();
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer io = lease.io_buffer();
    FixedBuffer header = io;
    header.size = kRecordHeaderBytes;

    const auto [file_id, block_offset] = FileOffset(location.block_id);
    auto read = co_await ReadStorageBuffer(
        *store.worker, store.files[file_id], header, lease.registered(),
        block_offset + location.record_offset);
    if (!read.ok()) {
      co_return read.status();
    }
    if (*read != kRecordHeaderBytes) {
      co_return Status(StatusCode::kInternal, "short record header read");
    }

    RecordHeader record{};
    std::string_view disk_key;
    std::span<const std::byte, kRecordHeaderBytes> record_bytes(
        header.data, kRecordHeaderBytes);
    if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
        record.digest != digest || disk_key != key ||
        record.kind != RecordKind::kValue ||
        record.generation != location.generation ||
        record.relocation_sequence != location.relocation_sequence ||
        record.allocation_epoch != location.allocation_epoch) {
      co_return Status(StatusCode::kInternal,
                       "record does not match in-memory location");
    }
    if (record.value_disk_bytes > io.size) {
      co_return Status(StatusCode::kOutOfRange,
                       "value exceeds registered read buffer capacity");
    }
    if (record.value_disk_bytes != 0) {
      FixedBuffer value_buffer = io;
      value_buffer.size = record.value_disk_bytes;
      read = co_await ReadStorageBuffer(
          *store.worker, store.files[file_id], value_buffer,
          lease.registered(),
          block_offset + location.record_offset + kRecordHeaderBytes);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != record.value_disk_bytes) {
        co_return Status(StatusCode::kInternal, "short record value read");
      }
    }
    if (Crc32c(std::span<const std::byte>(io.data, record.value_bytes)) !=
        record.payload_checksum) {
      co_return Status(StatusCode::kInternal, "record value checksum mismatch");
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
    const std::size_t value_disk_bytes = AlignDirect(value.size());
    const std::size_t total_disk_bytes =
        kRecordHeaderBytes + value_disk_bytes;
    if (total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
        total_disk_bytes > store.buffers.write_buffer().size) {
      co_return Status(StatusCode::kOutOfRange,
                       "value requires dedicated multi-block storage");
    }

    const std::uint32_t shard = StorageShardForKey(key);
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
      const std::optional<std::uint64_t> allocated = AllocateBlock(for_defrag);
      if (!allocated.has_value()) {
        co_return Status(StatusCode::kResourceExhausted,
                         "no foreground blocks remain; defrag reserve is protected");
      }
      const std::uint64_t block_id = *allocated;
      active = ActiveBlock{
          .block_id = block_id,
          .storage_shard_id = shard,
          .allocation_epoch = next_allocation_epoch_.fetch_add(
              1, std::memory_order_relaxed),
          .committed_bytes = kBlockHeaderBytes,
          .record_count = 0,
          .max_lsn = 0,
      };
      BlockState& state = store.block_states[block_id];
      state.storage_shard_id = shard;
      state.allocation_epoch = active->allocation_epoch;
      state.committed_bytes = kBlockHeaderBytes;
      state.live_bytes = 0;
      state.allocated = true;
    }

    ActiveBlock updated = *active;
    const std::uint32_t record_offset = updated.committed_bytes;
    updated.committed_bytes += static_cast<std::uint32_t>(total_disk_bytes);
    ++updated.record_count;
    updated.max_lsn = std::max(updated.max_lsn, lsn);

    FixedBuffer write_buffer = store.buffers.write_buffer();
    std::fill_n(write_buffer.data, total_disk_bytes, std::byte{0});
    RecordHeader record{
        .magic = kRecordMagic,
        .version = kStorageFormatVersion,
        .header_bytes = kRecordHeaderBytes,
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
    std::span<std::byte, kRecordHeaderBytes> record_output(
        write_buffer.data, kRecordHeaderBytes);
    if (!EncodeRecordHeader(record, key, record_output)) {
      co_return Status(StatusCode::kInternal, "record header encoding failed");
    }
    if (!value.empty()) {
      std::memcpy(write_buffer.data + kRecordHeaderBytes, value.data(),
                  value.size());
    }

    const auto [file_id, block_offset] = FileOffset(updated.block_id);
    FixedBuffer record_buffer = write_buffer;
    record_buffer.size = total_disk_bytes;
    auto written = co_await celer::WriteFixed(
        *store.worker, store.files[file_id], record_buffer,
        block_offset + record_offset);
    if (!written.ok() || *written != total_disk_bytes) {
      store.write_failed = true;
      co_return written.ok()
                    ? Status(StatusCode::kInternal, "short record write")
                    : written.status();
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
        write_buffer.data, kBlockHeaderBytes);
    EncodeBlockHeader(block, block_output);
    FixedBuffer block_buffer = write_buffer;
    block_buffer.size = kBlockHeaderBytes;
    written = co_await celer::WriteFixed(
        *store.worker, store.files[file_id], block_buffer, block_offset);
    if (!written.ok() || *written != kBlockHeaderBytes) {
      store.write_failed = true;
      co_return written.ok()
                    ? Status(StatusCode::kInternal, "short block header write")
                    : written.status();
    }

    Status sync = co_await celer::Fdatasync(*store.worker, store.files[file_id]);
    if (!sync.ok()) {
      store.write_failed = true;
      co_return sync;
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
    if (was_live != is_live) {
      if (is_live) {
        ++store.live_key_count;
      } else {
        --store.live_key_count;
      }
    }
    BlockState& state = store.block_states[updated.block_id];
    state.committed_bytes = updated.committed_bytes;
    state.live_bytes += location.total_disk_bytes;
    *active = updated;
    if (!for_defrag) {
      RequestDefrag(store);
    }
    co_return Status::Ok();
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
    if (store.defrag_running || !SelectDefragCandidate(store).has_value()) {
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
    if (!source.allocated || source.pins != 0 ||
        IsActiveBlock(store, block_id)) {
      co_return Status::Ok();
    }
    source.defragging = true;

    auto acquired = co_await store.buffers.AcquireReadBuffer();
    if (!acquired.ok()) {
      source.defragging = false;
      co_return acquired.status();
    }
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer io = lease.io_buffer();
    FixedBuffer header_buffer = io;
    header_buffer.size = kRecordHeaderBytes;
    const auto [source_file_id, source_block_offset] = FileOffset(block_id);

    std::uint32_t record_offset = kBlockHeaderBytes;
    while (record_offset < source.committed_bytes) {
      auto read = co_await ReadStorageBuffer(
          *store.worker, store.files[source_file_id], header_buffer,
          lease.registered(), source_block_offset + record_offset);
      if (!read.ok() || *read != kRecordHeaderBytes) {
        source.defragging = false;
        co_return read.ok()
                      ? Status(StatusCode::kInternal,
                               "short record header read during defrag")
                      : read.status();
      }

      RecordHeader record{};
      std::string_view disk_key;
      std::span<const std::byte, kRecordHeaderBytes> header_bytes(
          header_buffer.data, kRecordHeaderBytes);
      if (!DecodeRecordHeader(header_bytes, &record, &disk_key) ||
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
        std::string_view value;
        if (record.value_disk_bytes != 0) {
          if (record.value_disk_bytes > io.size) {
            source.defragging = false;
            co_return Status(StatusCode::kOutOfRange,
                             "defrag value exceeds registered read buffer");
          }
          FixedBuffer value_buffer = io;
          value_buffer.size = record.value_disk_bytes;
          read = co_await ReadStorageBuffer(
              *store.worker, store.files[source_file_id], value_buffer,
              lease.registered(),
              source_block_offset + record_offset + kRecordHeaderBytes);
          if (!read.ok() || *read != record.value_disk_bytes) {
            source.defragging = false;
            co_return read.ok()
                          ? Status(StatusCode::kInternal,
                                   "short value read during defrag")
                          : read.status();
          }
          value = std::string_view(reinterpret_cast<const char*>(io.data),
                                   record.value_bytes);
        }
        if (Crc32c(std::span<const std::byte>(io.data, record.value_bytes)) !=
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

    FixedBuffer zero = store.buffers.write_buffer();
    zero.size = kBlockHeaderBytes;
    std::fill_n(zero.data, zero.size, std::byte{0});
    auto written = co_await celer::WriteFixed(
        *store.worker, store.files[source_file_id], zero, source_block_offset);
    if (!written.ok() || *written != kBlockHeaderBytes) {
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
      source.defragging = false;
      store.write_failed = true;
      co_return sync;
    }

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
