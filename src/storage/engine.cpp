#include "keylane/storage/engine.h"

#include <fcntl.h>
#include <linux/fs.h>
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <sys/ioctl.h>
#include <sys/random.h>
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
#include "absl/container/flat_hash_set.h"
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
  std::uint64_t expire_at_ms = 0;
  // Owner in the current process topology. Unlike the persisted writer_id,
  // this must always be in [0, worker_count).
  std::uint16_t block_owner = 0;
  std::uint32_t record_offset = 0;
  std::uint32_t total_disk_bytes = 0;
  std::uint64_t logical_size = 0;
  std::uint32_t payload_bytes = 0;
  std::uint32_t relocation_sequence = 0;
  bool in_memory = false;
  bool external = false;
  RecordKind kind = RecordKind::kValue;
  ValueType value_type = ValueType::kNone;
  std::shared_ptr<const std::vector<ExtentRef>> extents;

  bool SamePhysicalRecord(const RecordLocation& other) const noexcept {
    return block_id == other.block_id &&
           record_offset == other.record_offset &&
           allocation_epoch == other.allocation_epoch;
  }
};


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

std::uint64_t UnixTimeMillis() noexcept {
  const auto value = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  return value > 0 ? static_cast<std::uint64_t>(value) : 0;
}

bool IsExpired(const RecordLocation& location,
               std::uint64_t now_ms) noexcept {
  return location.kind == RecordKind::kValue &&
         location.expire_at_ms != 0 && location.expire_at_ms <= now_ms;
}

StatusOr<std::shared_ptr<const std::vector<ExtentRef>>> DecodeManifest(
    std::span<const std::byte> payload, std::uint64_t logical_size) {
  if (payload.size() < sizeof(ExtentManifestHeader)) {
    return Status(StatusCode::kInternal, "external value manifest is truncated");
  }
  ExtentManifestHeader header{};
  std::memcpy(&header, payload.data(), sizeof(header));
  if (header.magic != kExtentManifestMagic ||
      header.version != kStorageFormatVersion || header.extent_count == 0 ||
      header.extent_count > kMaxStringExtents ||
      payload.size() != sizeof(header) +
                            static_cast<std::size_t>(header.extent_count) *
                                sizeof(ExtentRef)) {
    return Status(StatusCode::kInternal, "invalid external value manifest");
  }
  auto refs = std::make_shared<std::vector<ExtentRef>>(header.extent_count);
  std::memcpy(refs->data(), payload.data() + sizeof(header),
              refs->size() * sizeof(ExtentRef));
  std::uint64_t total = 0;
  for (const ExtentRef& ref : *refs) {
    if (ref.block_id == kInvalidBlockId || ref.allocation_epoch == 0 ||
        ref.payload_bytes == 0 || ref.payload_bytes > kExtentPayloadBytes ||
        total > kMaxStringBytes - ref.payload_bytes) {
      return Status(StatusCode::kInternal, "invalid extent reference");
    }
    total += ref.payload_bytes;
  }
  if (total != logical_size || total > kMaxStringBytes) {
    return Status(StatusCode::kInternal,
                  "extent manifest logical size mismatch");
  }
  return std::shared_ptr<const std::vector<ExtentRef>>(std::move(refs));
}

std::string EncodeManifest(std::span<const ExtentRef> refs) {
  ExtentManifestHeader header{.magic = kExtentManifestMagic,
                              .version = kStorageFormatVersion,
                              .extent_count =
                                  static_cast<std::uint32_t>(refs.size())};
  std::string output(sizeof(header) + refs.size_bytes(), '\0');
  std::memcpy(output.data(), &header, sizeof(header));
  std::memcpy(output.data() + sizeof(header), refs.data(), refs.size_bytes());
  return output;
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
  BlockKind kind = BlockKind::kRecords;
  std::uint32_t extent_index = 0;
  std::uint32_t extent_payload_checksum = 0;
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
  BlockKind kind = BlockKind::kRecords;
  std::uint32_t extent_index = 0;
  std::uint32_t extent_payload_checksum = 0;
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
  bool extent = false;
  std::uint32_t extent_index = 0;
  std::uint32_t extent_payload_checksum = 0;
};

// Back-pointer from a block to the index entries staged in its write buffer, so
// the flush completion can flip them to on-disk reads without re-hashing every
// key. The entry can outlive the index that owns it: FLUSHDB detaches every
// partition index for one database while blocks are still in flight. Recording
// which database generation produced the entry lets the completion detect that
// and skip the entry instead of following a pointer into a freed population.
struct RecordIdentity {
  RecordIndex::Entry* entry = nullptr;
  std::shared_ptr<const std::vector<ExtentRef>> retired_extents;
  std::uint64_t index_generation = 0;
  std::uint8_t db_id = 0;
};

struct ReplicaValueStage {
  std::uint8_t db_id = 0;
  std::uint64_t db_epoch = 0;
  std::uint64_t mutation_sequence = 0;
  std::uint64_t expire_at_ms = 0;
  std::uint64_t logical_size = 0;
  std::uint32_t next_chunk = 0;
  std::uint32_t chunk_count = 0;
  ValueType value_type = ValueType::kNone;
  std::string key;
  std::string value;
};

// One partition's worth of entries taken out of service by FLUSHDB. The entries
// are unreachable to readers the moment the index is detached, but the blocks
// they occupy still count them as live until the reclaimer subtracts them.
struct DetachedIndex {
  RecordIndex index;
  std::uint8_t db_id = 0;
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

class AsyncNotification {
 public:
  class Awaiter {
   public:
    explicit Awaiter(AsyncNotification* notification)
        : notification_(notification) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> awaiting) {
      notification_->waiters_.push_back(awaiting);
      return true;
    }
    void await_resume() const noexcept {}

   private:
    AsyncNotification* notification_;
  };

  Awaiter Wait() noexcept { return Awaiter(this); }

  void NotifyAll(Worker& worker) {
    std::deque<std::coroutine_handle<>> waiters;
    waiters.swap(waiters_);
    for (std::coroutine_handle<> waiter : waiters) {
      worker.Enqueue(waiter);
    }
  }

 private:
  std::deque<std::coroutine_handle<>> waiters_;
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

Status ReadExactlyAt(int fd, std::span<std::byte> output,
                     std::uint64_t offset) {
  std::size_t done = 0;
  while (done < output.size()) {
    const ssize_t read = ::pread(
        fd, output.data() + done, output.size() - done,
        static_cast<off_t>(offset + done));
    if (read < 0 && errno == EINTR) {
      continue;
    }
    if (read <= 0) {
      return Status(StatusCode::kInternal,
                    read == 0 ? "short device-label read"
                              : "device-label read failed: " +
                                    std::string(std::strerror(errno)));
    }
    done += static_cast<std::size_t>(read);
  }
  return Status::Ok();
}

Status WriteExactlyAt(int fd, std::span<const std::byte> input,
                      std::uint64_t offset) {
  std::size_t done = 0;
  while (done < input.size()) {
    const ssize_t written = ::pwrite(
        fd, input.data() + done, input.size() - done,
        static_cast<off_t>(offset + done));
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written <= 0) {
      return Status(StatusCode::kInternal,
                    "device-label write failed: " +
                        std::string(std::strerror(errno)));
    }
    done += static_cast<std::size_t>(written);
  }
  return Status::Ok();
}

StatusOr<std::optional<DeviceLabel>> ReadDeviceLabel(
    const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open device label failed: " + path + ": " +
                      std::strerror(errno));
  }
  std::array<std::byte, kDirectIoAlignment> page{};
  Status status = ReadExactlyAt(fd, page, kDeviceLabelOffset);
  const int close_error = ::close(fd);
  if (!status.ok()) {
    return status;
  }
  if (close_error != 0) {
    return Status(StatusCode::kInternal,
                  "close after device-label read failed: " + path);
  }
  if (IsZero(page)) {
    return std::optional<DeviceLabel>{};
  }
  DeviceLabel label{};
  if (!DecodeDeviceLabel(page, &label)) {
    return Status(StatusCode::kInternal,
                  "invalid or corrupt device label: " + path);
  }
  return std::optional<DeviceLabel>{label};
}

Status WriteDeviceLabel(const std::string& path, const DeviceLabel& label) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open device label for write failed: " + path + ": " +
                      std::strerror(errno));
  }
  std::array<std::byte, kDirectIoAlignment> page{};
  EncodeDeviceLabel(label, page);
  Status status = WriteExactlyAt(fd, page, kDeviceLabelOffset);
  if (status.ok() && ::fdatasync(fd) != 0) {
    status = Status(StatusCode::kInternal,
                    "device-label fdatasync failed: " + path + ": " +
                        std::strerror(errno));
  }
  const int close_error = ::close(fd);
  if (!status.ok()) {
    return status;
  }
  if (close_error != 0) {
    return Status(StatusCode::kInternal,
                  "close after device-label write failed: " + path);
  }
  return Status::Ok();
}

struct MetadataPageState {
  std::uint64_t generation = 0;
  std::uint8_t active_slot = 0;
};

struct LoadedMetadataPage {
  std::vector<std::byte> payload;
  MetadataPageState state{};
};

StatusOr<LoadedMetadataPage> ReadMetadataPagePair(
    int fd, std::uint64_t base_offset, MetadataPageKind kind,
    std::uint32_t page_index, std::size_t payload_bytes) {
  LoadedMetadataPage selected;
  selected.payload.resize(payload_bytes, std::byte{0});
  bool saw_nonzero = false;
  bool selected_valid = false;
  for (unsigned slot = 0; slot < 2; ++slot) {
    std::array<std::byte, kDirectIoAlignment> page{};
    Status read = ReadExactlyAt(
        fd, page, MetadataPageSlotOffset(base_offset, page_index, slot));
    if (!read.ok()) {
      return read;
    }
    if (IsZero(page)) {
      continue;
    }
    saw_nonzero = true;
    std::vector<std::byte> payload(payload_bytes, std::byte{0});
    std::uint64_t generation = 0;
    if (!DecodeMetadataPage(page, kind, page_index, &generation, payload)) {
      continue;
    }
    if (!selected_valid || generation > selected.state.generation) {
      selected.payload = std::move(payload);
      selected.state.generation = generation;
      selected.state.active_slot = static_cast<std::uint8_t>(slot);
      selected_valid = true;
    }
  }
  if (saw_nonzero && !selected_valid) {
    return Status(StatusCode::kInternal,
                  "both fixed-metadata page slots are corrupt");
  }
  return selected;
}

StatusOr<std::uint64_t> RandomStorageSetId() {
  std::uint64_t value = 0;
  while (value == 0) {
    const ssize_t bytes = ::getrandom(&value, sizeof(value), 0);
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes != static_cast<ssize_t>(sizeof(value))) {
      return Status(StatusCode::kInternal,
                    "getrandom for storage-set id failed: " +
                        std::string(std::strerror(errno)));
    }
  }
  return value;
}

struct StorageDevice {
  std::string path;
  std::uint64_t id = 0;
  std::uint64_t capacity_blocks = 0;
  std::uint32_t data_block_begin = 1;
  std::uint64_t data_block_count = 0;
  std::uint32_t file_index = 0;
};

inline constexpr std::size_t kCacheLineBytes = 64;

struct ReservedBlock {
  std::uint64_t block_id = 0;
  std::uint64_t allocation_epoch = 0;
};

struct alignas(kCacheLineBytes) RecoveryDeviceCursor {
  std::atomic<std::uint64_t> next_local{1};
  std::atomic<std::uint64_t> next_allocation_epoch{1};
};

static_assert(sizeof(RecoveryDeviceCursor) % kCacheLineBytes == 0);

struct DeviceAllocator {
  celer::WorkerId owner = 0;
  AsyncMutex mutex;
  std::uint32_t data_block_begin = 1;
  std::uint64_t next_pristine = 1;
  std::uint64_t next_allocation_epoch = 1;
  std::vector<std::uint64_t> ready_blocks;
  std::vector<std::uint64_t> cold_free;
  std::vector<std::byte> scan_bitmap;
  std::vector<MetadataPageState> bitmap_pages;
  std::vector<MetadataPageState> epoch_pages;
  std::vector<std::uint64_t> epoch_values;
  std::vector<std::uint64_t> durable_epoch_values;
  std::optional<Status> failed;
};

struct BlockDeviceInfo {
  std::size_t io_alignment = 0;
  std::uint64_t size_bytes = 0;
};

struct StoragePathInfo {
  bool is_block_device = false;
  std::size_t io_alignment = kDirectIoAlignment;
  std::uint64_t size_bytes = 0;
};

StatusOr<BlockDeviceInfo> ProbeBlockDevice(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open block device for probe failed: " + path +
                      ": " + std::strerror(errno));
  }

  int logical_block_bytes = 0;
  std::uint64_t size_bytes = 0;
  const int sector_error = ::ioctl(fd, BLKSSZGET, &logical_block_bytes);
  const int sector_errno = errno;
  const int size_error = ::ioctl(fd, BLKGETSIZE64, &size_bytes);
  const int size_errno = errno;
  const int close_error = ::close(fd);
  if (sector_error != 0) {
    return Status(StatusCode::kInternal,
                  "BLKSSZGET failed: " + path + ": " +
                      std::strerror(sector_errno));
  }
  if (size_error != 0) {
    return Status(StatusCode::kInternal,
                  "BLKGETSIZE64 failed: " + path + ": " +
                      std::strerror(size_errno));
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
  return BlockDeviceInfo{.io_alignment = alignment, .size_bytes = size_bytes};
}

StatusOr<StoragePathInfo> ProbeStoragePath(const std::string& path) {
  struct stat file_info {};
  if (::stat(path.c_str(), &file_info) != 0) {
    return Status(StatusCode::kInternal,
                  "stat storage path failed: " + path + ": " +
                      std::strerror(errno));
  }
  if (S_ISBLK(file_info.st_mode)) {
    auto device = ProbeBlockDevice(path);
    if (!device.ok()) {
      return device.status();
    }
    return StoragePathInfo{
        .is_block_device = true,
        .io_alignment = device->io_alignment,
        .size_bytes = device->size_bytes,
    };
  }
  if (!S_ISREG(file_info.st_mode)) {
    return Status(StatusCode::kInvalidArgument,
                  "storage path is neither a regular file nor a block device: " +
                      path);
  }
  if (file_info.st_size < 0) {
    return Status(StatusCode::kOutOfRange,
                  "storage file reports a negative size: " + path);
  }
  return StoragePathInfo{
      .is_block_device = false,
      .io_alignment = kDirectIoAlignment,
      .size_bytes = static_cast<std::uint64_t>(file_info.st_size),
  };
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
      std::array<std::size_t, kLogicalDatabaseCount> expiring_key_count{};
      std::uint64_t mutation_sequence = 0;
      std::uint64_t replication_epoch = 1;
      std::uint64_t delta_floor = 0;
      bool capture_deltas = false;
      bool delta_queued = false;
      std::deque<SnapshotRecord> deltas;
      std::optional<ReplicaValueStage> replica_value_stage;
    };

    struct ExpireCandidate {
      std::uint16_t partition_id = 0;
      std::uint8_t db_id = 0;
      Digest digest{};
      std::uint64_t mutation_sequence = 0;
      std::uint64_t expire_at_ms = 0;
      std::string key;
    };

    Worker* worker = nullptr;
    RegisteredBufferPool buffers;
    std::vector<FixedFile> files;
    std::vector<PartitionStore> partitions;
    absl::flat_hash_map<std::uint64_t, std::vector<RecordIdentity>>
        staged_records;
    // Bumped every time FLUSHDB detaches this database's partition indexes.
    // Every index for one database is detached together and without suspending,
    // so one counter per database describes all of them.
    std::array<std::uint64_t, kLogicalDatabaseCount> index_generations{};
    // Populations detached by FLUSHDB, still holding their entries. Draining
    // this is what actually frees them and settles the block accounting.
    std::deque<DetachedIndex> detached_indexes;
    bool detached_reclaim_running = false;
    std::array<std::size_t, kLogicalDatabaseCount> live_key_count{};
    std::array<IntentLockTable, kLogicalDatabaseCount> key_locks;
    std::optional<ActiveBlock> active_block;
    std::optional<ReservedBlock> standby_block;
    std::optional<Status> standby_error;
    AsyncNotification standby_ready;
    absl::flat_hash_map<std::uint64_t, std::unique_ptr<BlockState>> block_states;
    AsyncMutex writer_mutex;
    std::deque<std::uint64_t> flush_queue;
    std::deque<std::uint64_t> defrag_queue;
    std::vector<std::size_t> home_devices;
    std::vector<std::uint64_t> home_device_allocations;
    bool flush_running = false;
    bool write_failed = false;
    bool defrag_running = false;
    bool defrag_waiting = false;
    std::size_t defrag_waiting_device = 0;
    std::size_t active_defrag_device = 0;
    bool standby_request_pending = false;
    std::size_t expiry_partition_cursor = 0;
    std::uint8_t expiry_db_cursor = 0;
    std::uint64_t expiry_scan_cursor = 0;
    std::deque<ExpireCandidate> expired_candidates;
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
        return Status(StatusCode::kOutOfRange,
                      "storage path is too small to hold metadata and data: " +
                          path);
      }
      direct_io_alignment =
          std::max(direct_io_alignment, probed->io_alignment);
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
        return Status(StatusCode::kFailedPrecondition,
                      "configured devices belong to different storage sets");
      }
      if (expected_device_count == 0) {
        expected_device_count = label.device_count;
      } else if (expected_device_count != label.device_count) {
        return Status(StatusCode::kFailedPrecondition,
                      "configured devices disagree on storage-set size");
      }
      if (!seen_device_ids.try_emplace(label.device_id, i).second) {
        return Status(StatusCode::kFailedPrecondition,
                      "duplicate device id in configured storage files");
      }
    }

    if (has_existing_device && has_empty_device) {
      return Status(
          StatusCode::kFailedPrecondition,
          "mixing initialized and empty storage devices is not supported; "
          "online device-set expansion is not implemented");
    }
    if (has_existing_device) {
      if (expected_device_count != labels.size()) {
        return Status(StatusCode::kFailedPrecondition,
                      "configured storage device count does not match the "
                      "persisted storage set; a device may be missing");
      }
      for (std::uint64_t id = 0; id < expected_device_count; ++id) {
        if (!seen_device_ids.contains(id)) {
          return Status(StatusCode::kFailedPrecondition,
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
        const std::uint64_t required_bytes =
            capacity_blocks * kStorageBlockBytes;
        if (probed.size_bytes < required_bytes) {
          return Status(StatusCode::kFailedPrecondition,
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
          return Status(
              StatusCode::kInvalidArgument,
              "new regular storage file size must be a multiple of 8 MiB: " +
                  options_.data_files[i]);
        }
        capacity_blocks = probed.size_bytes / kStorageBlockBytes;
        if (capacity_blocks > kLocalBlockIdLimit) {
          return Status(StatusCode::kOutOfRange,
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
      const std::uint32_t data_block_begin =
          DataBlockBegin(capacity_blocks);
      if (capacity_blocks > kLocalBlockIdLimit) {
        return Status(StatusCode::kOutOfRange,
                      "persisted device capacity exceeds the 1 PiB limit: " +
                          options_.data_files[i]);
      }
      if (data_block_begin >= capacity_blocks) {
        return Status(StatusCode::kOutOfRange,
                      "fixed metadata leaves no data blocks: " +
                          options_.data_files[i]);
      }
      if (capacity_blocks - data_block_begin <=
          kDefragReserveBlocksPerDevice) {
        return Status(
            StatusCode::kOutOfRange,
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
    defrag_ready_by_device_ = std::make_unique<
        moodycamel::ConcurrentQueue<std::uint16_t>[]>(devices_.size());
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      active_defrags_by_device_[device_index].store(
          0, std::memory_order_relaxed);
    }
    total_data_blocks_ = 0;
    std::uint64_t foreground_blocks = 0;
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const StorageDevice& device = devices_[device_index];
      total_data_blocks_ += device.data_block_count;
      const std::size_t reserve = DefragReserveForDevice(device_index);
      if (device.data_block_count > reserve) {
        foreground_blocks += device.data_block_count - reserve;
      }
    }
    if (foreground_blocks == 0) {
      return Status(
          StatusCode::kOutOfRange,
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
        Status written = WriteDeviceLabel(options_.data_files[i], label);
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
          device.id, device.path,
          device.capacity_blocks * kStorageBlockBytes,
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
    epoch_values_.assign(kEpochValueCount, 1);
    device_allocators_.clear();
    device_allocators_.reserve(devices_.size());
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const StorageDevice& device = devices_[device_index];
      const std::size_t bitmap_bytes =
          ScanBitmapBytes(device.capacity_blocks);
      const std::size_t bitmap_page_count =
          ScanBitmapPageCount(device.capacity_blocks);
      auto allocator = std::make_unique<DeviceAllocator>();
      allocator->owner = static_cast<celer::WorkerId>(device_index %
                                                       worker_count_);
      allocator->data_block_begin = device.data_block_begin;
      allocator->next_pristine = device.data_block_begin;
      allocator->scan_bitmap.resize(bitmap_bytes, std::byte{0});
      allocator->bitmap_pages.resize(bitmap_page_count);
      allocator->epoch_pages.resize(kEpochMetadataPageCount);
      allocator->epoch_values.assign(kEpochValueCount, 1);
      allocator->durable_epoch_values.assign(kEpochValueCount, 1);

      const int fd = ::open(device.path.c_str(), O_RDWR | O_CLOEXEC);
      if (fd < 0) {
        return Status(StatusCode::kInternal,
                      "open fixed metadata failed: " + device.path + ": " +
                          std::strerror(errno));
      }
      Status load_status = Status::Ok();
      for (std::size_t page_index = 0;
           page_index < kEpochMetadataPageCount; ++page_index) {
        const std::size_t byte_offset =
            page_index * kMetadataPagePayloadBytes;
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
          std::memcpy(&value,
                      loaded->payload.data() +
                          value_index * sizeof(std::uint64_t),
                      sizeof(value));
          value = std::max<std::uint64_t>(value, 1);
          allocator->durable_epoch_values[first_value + value_index] =
              value;
          epoch_values_[first_value + value_index] = std::max(
              epoch_values_[first_value + value_index], value);
        }
      }
      for (std::size_t page_index = 0;
           load_status.ok() && page_index < bitmap_page_count; ++page_index) {
        const std::size_t byte_offset =
            page_index * kMetadataPagePayloadBytes;
        const std::size_t payload_bytes = std::min(
            kMetadataPagePayloadBytes, bitmap_bytes - byte_offset);
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
        return Status(load_status.code(),
                      load_status.message() + ": " + device.path);
      }
      if (close_error != 0) {
        return Status(StatusCode::kInternal,
                      "close fixed metadata failed: " + device.path);
      }

      for (std::uint64_t local = device.capacity_blocks;
           local-- > device.data_block_begin;) {
        const std::size_t byte_index = static_cast<std::size_t>(local / 8);
        const unsigned bit_index = static_cast<unsigned>(local % 8);
        if ((std::to_integer<unsigned>(
                 allocator->scan_bitmap[byte_index]) &
             (1U << bit_index)) != 0) {
          allocator->next_pristine = local + 1;
          break;
        }
      }
      for (std::uint64_t local = device.data_block_begin;
           local < allocator->next_pristine; ++local) {
        const std::size_t byte_index = static_cast<std::size_t>(local / 8);
        const unsigned bit_index = static_cast<unsigned>(local % 8);
        if ((std::to_integer<unsigned>(
                 allocator->scan_bitmap[byte_index]) &
             (1U << bit_index)) == 0) {
          allocator->cold_free.push_back(MakeBlockId(
              device.id, static_cast<std::uint32_t>(local)));
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
        store.partitions.back().replication_epoch =
            epoch_values_[kLogicalDatabaseCount + partition];
      }
    }
    ConfigureWorkerDeviceAffinity();
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      db_epochs_[db_id].store(epoch_values_[db_id],
                              std::memory_order_relaxed);
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
              if (location.external && location.extents != nullptr) {
                for (std::size_t extent_index = 0;
                     extent_index < location.extents->size();
                     ++extent_index) {
                  const ExtentRef& extent =
                      location.extents->at(extent_index);
                  live_by_owner[location.block_owner].push_back(
                      RecoveryLiveReference{
                          .block_id = extent.block_id,
                          .allocation_epoch = extent.allocation_epoch,
                          .bytes = extent.payload_bytes,
                          .extent = true,
                          .extent_index = static_cast<std::uint32_t>(
                              extent_index),
                          .extent_payload_checksum =
                              extent.payload_checksum,
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
              if (reference.extent &&
                  (state->kind != BlockKind::kValueExtent ||
                   state->committed_bytes !=
                       kBlockHeaderBytes + reference.bytes ||
                   state->extent_index != reference.extent_index ||
                   state->extent_payload_checksum !=
                       reference.extent_payload_checksum)) {
                return Status(StatusCode::kInternal,
                              "live extent header does not match manifest");
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
            allocator.next_pristine = std::max(
                allocator.next_pristine,
                recovery_device_cursors_[device_index].next_local.load(
                    std::memory_order_acquire));
            allocator.next_allocation_epoch = std::max(
                allocator.next_allocation_epoch,
                recovery_device_cursors_[device_index]
                    .next_allocation_epoch.load(std::memory_order_acquire));
            allocator.ready_blocks.insert(
                allocator.ready_blocks.end(),
                std::make_move_iterator(blocks.begin()),
                std::make_move_iterator(blocks.end()));
            return Status::Ok();
          };
      status = allocator_owner == worker.id()
                   ? apply_recovery_free()
                   : co_await celer::SubmitTo(allocator_owner,
                                               std::move(apply_recovery_free));
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
    for (const auto& [block_id, state] : store.block_states) {
      if (state != nullptr) {
        if (state->kind == BlockKind::kValueExtent &&
            state->live_bytes == 0) {
          orphan_extents->push_back(ExtentRef{
              .block_id = block_id,
              .allocation_epoch = state->allocation_epoch,
              .payload_bytes = static_cast<std::uint32_t>(
                  state->committed_bytes - kBlockHeaderBytes),
              .payload_checksum = 0,
          });
        } else {
          MaybeQueueDefrag(store, block_id);
        }
      }
    }
    if (!orphan_extents->empty()) {
      worker.Spawn(ReclaimExtents(
          &store, std::shared_ptr<const std::vector<ExtentRef>>(
                      std::move(orphan_extents))));
    }
    worker.Spawn(PeriodicFlush(&store));
    if (options_.expiration_authority) {
      worker.SpawnBackground(ActiveExpiration(&store));
    }
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
    const std::uint64_t now_ms = UnixTimeMillis();
    if (IsExpired(found->value, now_ms)) {
      QueueExpiredCandidate(store, partition.id, db_id, *found);
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

    co_return EncodeDiskValue(std::move(*loaded));
  }

  Task<StatusOr<std::uint64_t>> StringLength(std::uint8_t db_id,
                                             std::string_view key) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock = co_await store.key_locks[db_id].Acquire(
        digest, IntentLockMode::kShared);
    auto* found = partition.indexes[db_id].Find(digest, key);
    if (found == nullptr || found->value.kind != RecordKind::kValue ||
        IsExpired(found->value, UnixTimeMillis())) {
      if (found != nullptr && IsExpired(found->value, UnixTimeMillis())) {
        QueueExpiredCandidate(store, partition.id, db_id, *found);
      }
      co_return Status(StatusCode::kNotFound, "key not found");
    }
    if (found->value.value_type != ValueType::kString) {
      co_return Status(StatusCode::kInvalidArgument,
                       "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    co_return found->value.logical_size;
  }

  Task<StatusOr<SetResult>> Set(std::uint8_t db_id, std::string_view key,
                                std::string_view value,
                                SetOptions options) {
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
    const std::uint64_t now_ms = UnixTimeMillis();
    const bool exists = found != nullptr &&
                        found->value.kind == RecordKind::kValue &&
                        !IsExpired(found->value, now_ms);
    SetResult result;
    if (options.return_old_value && exists) {
      if (found->value.value_type != ValueType::kString) {
        co_return Status(StatusCode::kInvalidArgument,
                         "WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      auto loaded = co_await LoadValue(store, db_id, key, digest, found->value);
      if (!loaded.ok()) {
        co_return loaded.status();
      }
      auto encoded = EncodeDiskValue(std::move(*loaded));
      if (!encoded.ok()) {
        co_return encoded.status();
      }
      result.old_value.emplace(std::move(*encoded));
    }

    const bool condition_met =
        options.condition == SetCondition::kNone ||
        (options.condition == SetCondition::kIfAbsent && !exists) ||
        (options.condition == SetCondition::kIfPresent && exists);
    if (!condition_met) {
      co_return result;
    }

    const std::uint64_t expire_at_ms =
        options.keep_ttl && exists ? found->value.expire_at_ms
                                   : options.expire_at_ms;
    Status status = co_await AppendLocked(
        store, partition, db_id, key, value, RecordKind::kValue,
        ValueType::kString, expire_at_ms);
    if (!status.ok()) {
      co_return status;
    }
    result.applied = true;
    co_return result;
  }

  Task<ExpirationInfo> GetExpiration(std::uint8_t db_id,
                                     std::string_view key) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock = co_await store.key_locks[db_id].Acquire(
        digest, IntentLockMode::kShared);
    auto* found = partition.indexes[db_id].Find(digest, key);
    if (found == nullptr || found->value.kind != RecordKind::kValue) {
      co_return ExpirationInfo{};
    }
    if (IsExpired(found->value, UnixTimeMillis())) {
      QueueExpiredCandidate(store, partition.id, db_id, *found);
      co_return ExpirationInfo{};
    }
    co_return ExpirationInfo{
        .exists = true,
        .expire_at_ms = found->value.expire_at_ms,
    };
  }

  Task<StatusOr<bool>> UpdateExpiration(
      std::uint8_t db_id, std::string_view key,
      std::uint64_t expire_at_ms, ExpirationCondition condition) {
    assert(db_id < kLogicalDatabaseCount);
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const Digest digest = ComputeDigest(key);
    auto key_lock = co_await store.key_locks[db_id].Acquire(
        digest, IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);

    auto& index = partition.indexes[db_id];
    auto* found = index.Find(digest, key);
    const std::uint64_t now_ms = UnixTimeMillis();
    if (found == nullptr || found->value.kind != RecordKind::kValue ||
        IsExpired(found->value, now_ms)) {
      co_return false;
    }
    const std::uint64_t current = found->value.expire_at_ms;
    bool condition_met = true;
    switch (condition) {
      case ExpirationCondition::kNone:
        break;
      case ExpirationCondition::kIfNoExpiration:
        condition_met = current == 0;
        break;
      case ExpirationCondition::kIfHasExpiration:
        condition_met = current != 0;
        break;
      case ExpirationCondition::kIfGreater:
        condition_met = current != 0 && expire_at_ms > current;
        break;
      case ExpirationCondition::kIfLess:
        condition_met = current == 0 || expire_at_ms < current;
        break;
    }
    if (!condition_met) {
      co_return false;
    }

    if (expire_at_ms != 0 && expire_at_ms <= now_ms) {
      Status status = co_await AppendLocked(
          store, partition, db_id, key, {}, RecordKind::kTombstone,
          ValueType::kNone, 0);
      if (!status.ok()) {
        co_return status;
      }
      co_return true;
    }

    const RecordLocation previous = found->value;
    auto loaded = co_await LoadValue(store, db_id, key, digest, previous);
    if (!loaded.ok()) {
      co_return loaded.status();
    }
    FixedBuffer value_buffer = loaded->lease.io_buffer();
    std::string_view value(reinterpret_cast<const char*>(value_buffer.data),
                           loaded->value_bytes);
    Status status = co_await AppendLocked(
        store, partition, db_id, key, value, RecordKind::kValue,
        previous.value_type, expire_at_ms);
    if (!status.ok()) {
      co_return status;
    }
    co_return true;
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
    const bool expired = IsExpired(found->value, UnixTimeMillis());
    Status status =
        co_await AppendLocked(store, partition, db_id, key, {},
                              RecordKind::kTombstone, ValueType::kNone, 0);
    if (!status.ok()) {
      co_return status;
    }
    co_return !expired;
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
    if (found == nullptr || found->value.kind != RecordKind::kValue) {
      co_return false;
    }
    if (IsExpired(found->value, UnixTimeMillis())) {
      QueueExpiredCandidate(store, partition.id, db_id, *found);
      co_return false;
    }
    co_return true;
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
    std::uint64_t expire_at_ms = 0;
    auto& index = partition.indexes[db_id];
    auto* found = index.Find(digest, key);
    const bool exists = found != nullptr &&
                        found->value.kind == RecordKind::kValue &&
                        !IsExpired(found->value, UnixTimeMillis());
    if (exists) {
      if (found->value.value_type != ValueType::kString) {
        co_return Status(StatusCode::kInvalidArgument,
                         "WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      expire_at_ms = found->value.expire_at_ms;
      auto loaded =
          co_await LoadValue(store, db_id, key, digest, found->value);
      if (!loaded.ok()) {
        co_return loaded.status();
      }
      const std::span<const std::byte> value_bytes = loaded->value();
      std::string_view text(reinterpret_cast<const char*>(value_bytes.data()),
                            value_bytes.size());
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
                              RecordKind::kValue, ValueType::kString,
                              expire_at_ms);
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

  Task<Status> FlushDbDetach(std::uint8_t db_id) {
    assert(db_id < kLogicalDatabaseCount);
    if (celer::ThisWorker().id != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, db_id]() -> Task<Status> {
            co_return co_await FlushDbDetach(db_id);
          });
    }

    const std::uint64_t current = DbEpoch(db_id);
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      co_return Status(StatusCode::kOutOfRange, "database epoch exhausted");
    }
    co_return co_await DetachDbEpoch(db_id, current + 1);
  }

  Task<Status> FlushDbReclaim(bool wait) {
    co_return co_await ReclaimDetachedAllWorkers(wait);
  }

  // Persists the new epoch, then takes the database out of service on every
  // worker. Callers hold the database gate across this and can drop it as soon
  // as it returns: the keyspace is empty and durably so, and what remains is
  // reclamation that no reader can observe. Bounded by the partition count.
  //
  // The epoch has to reach the device before any index is detached. Crashing in
  // the other order leaves records on disk whose epoch still matches, and
  // recovery would resurrect the whole flushed database.
  Task<Status> DetachDbEpoch(std::uint8_t db_id, std::uint64_t next) {
    if (celer::ThisWorker().id != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, db_id, next]() -> Task<Status> {
            co_return co_await DetachDbEpoch(db_id, next);
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
    Status status = co_await PersistEpochValue(db_id, next);
    if (!status.ok()) {
      co_return status;
    }
    db_epochs_[db_id].store(next, std::memory_order_release);

    for (unsigned target = 0; target < worker_count_; ++target) {
      auto detach = [this, target, db_id]() -> Task<Status> {
        WorkerStore& store = *stores_[target];
        co_await store.writer_mutex.Lock();
        UnlockGuard unlock(&store.writer_mutex, store.worker);
        DetachDbLocal(store, db_id);
        co_return Status::Ok();
      };
      Status detached = target == 0
                            ? co_await detach()
                            : co_await celer::SubmitTaskTo(target, detach);
      if (!detached.ok()) {
        co_return detached;
      }
    }
    co_return Status::Ok();
  }

  // Retires what DetachDbEpoch took out of service. Runs with the gate open and
  // ordinary traffic flowing. `wait` is the difference between FLUSHDB SYNC and
  // FLUSHDB ASYNC: either way a reclaimer runs, only the reply waits or not.
  Task<Status> ReclaimDetachedAllWorkers(bool wait) {
    if (celer::ThisWorker().id != 0) {
      co_return co_await celer::SubmitTaskTo(
          0, [this, wait]() -> Task<Status> {
            co_return co_await ReclaimDetachedAllWorkers(wait);
          });
    }
    for (unsigned target = 0; target < worker_count_; ++target) {
      auto reclaim = [this, target, wait]() -> Task<Status> {
        WorkerStore& store = *stores_[target];
        if (!wait) {
          EnsureDetachedReclaim(store);
          co_return Status::Ok();
        }
        co_return co_await AwaitDetachedReclaim(store);
      };
      Status reclaimed = target == 0
                             ? co_await reclaim()
                             : co_await celer::SubmitTaskTo(target, reclaim);
      if (!reclaimed.ok()) {
        co_return reclaimed;
      }
    }
    co_return Status::Ok();
  }

  Task<Status> AdvanceDbEpoch(std::uint8_t db_id, std::uint64_t next) {
    Status detached = co_await DetachDbEpoch(db_id, next);
    if (!detached.ok()) {
      co_return detached;
    }
    co_return co_await ReclaimDetachedAllWorkers(/*wait=*/true);
  }

  ScanBatch ScanPartition(std::uint16_t partition_id, std::uint8_t db_id,
                          std::uint64_t cursor, std::size_t count) const {
    assert(db_id < kLogicalDatabaseCount);
    assert(count > 0);
    const auto& index =
        PartitionFor(CurrentStore(), partition_id).indexes[db_id];
    ScanBatch result;
    result.cursor = cursor;
    const std::uint64_t now_ms = UnixTimeMillis();
    const std::size_t max_iterations =
        count > std::numeric_limits<std::size_t>::max() / 10
            ? std::numeric_limits<std::size_t>::max()
            : count * 10;
    std::size_t iterations = 0;
    do {
      result.cursor = index.Scan(
          result.cursor, [&](const RecordIndex::Entry& entry) {
            if (entry.value.kind == RecordKind::kValue &&
                !IsExpired(entry.value, now_ms)) {
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
    const std::uint64_t now_ms = UnixTimeMillis();
    std::uint64_t next = cursor;
    const std::size_t max_iterations =
        count > std::numeric_limits<std::size_t>::max() / 10
            ? std::numeric_limits<std::size_t>::max()
            : count * 10;
    std::size_t iterations = 0;
    do {
      next = index.Scan(next, [&](const RecordIndex::Entry& entry) {
        if (entry.value.kind == RecordKind::kValue &&
            !IsExpired(entry.value, now_ms)) {
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
      if (IsExpired(location, UnixTimeMillis())) {
        QueueExpiredCandidate(store, partition.id, db_id, *current);
        continue;
      }
      auto loaded = co_await LoadValue(store, db_id, key, digest, location);
      if (!loaded.ok()) {
        if (loaded.status().code() == StatusCode::kNotFound) {
          continue;
        }
        co_return loaded.status();
      }
      const std::span<const std::byte> value = loaded->value();
      batch.records.push_back(SnapshotRecord{
          .kind = SnapshotRecord::Kind::kValue,
          .db_id = db_id,
          .db_epoch = DbEpoch(db_id),
          .mutation_sequence = location.mutation_sequence,
          .expire_at_ms = location.expire_at_ms,
          .value_type = location.value_type,
          .key = key,
          .value = std::string(reinterpret_cast<const char*>(value.data()),
                               value.size()),
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
    // Stop this worker's append stream before making the new epoch durable.
    // Otherwise a concurrent command could append an old-epoch record after
    // the metadata commit and receive OK even though restart must discard it.
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);
    const std::uint64_t next_epoch = partition.replication_epoch + 1;
    Status persisted = co_await PersistEpochValue(
        kLogicalDatabaseCount + partition_id, next_epoch);
    if (!persisted.ok()) {
      co_return persisted;
    }

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
    partition.replica_value_stage.reset();
    partition.delta_floor = 0;
    partition.delta_queued = false;
    for (const auto& [db_id, key] : old_keys) {
      const Digest digest = ComputeDigest(key);
      Status tombstone = co_await WriteRecordLocked(
          store, db_id, key, {}, RecordKind::kTombstone, ValueType::kNone,
          0, digest, 0, 0, 0, false, false);
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
      std::optional<SnapshotRecord> materialized;
      const SnapshotRecord* effective = &record;
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

      if (record.kind == SnapshotRecord::Kind::kValueBegin) {
        if (partition.replica_value_stage.has_value() ||
            record.value_type != ValueType::kString || !record.value.empty() ||
            record.logical_size == 0 ||
            record.logical_size > kMaxStringBytes ||
            record.chunk_count == 0 ||
            record.chunk_count !=
                (record.logical_size + kExtentPayloadBytes - 1) /
                    kExtentPayloadBytes) {
          co_return Status(StatusCode::kInvalidArgument,
                           "invalid replicated large value begin frame");
        }
        partition.replica_value_stage = ReplicaValueStage{
            .db_id = record.db_id,
            .db_epoch = record.db_epoch,
            .mutation_sequence = record.mutation_sequence,
            .expire_at_ms = record.expire_at_ms,
            .logical_size = record.logical_size,
            .next_chunk = 0,
            .chunk_count = record.chunk_count,
            .value_type = record.value_type,
            .key = record.key,
            .value = {},
        };
        partition.replica_value_stage->value.reserve(
            static_cast<std::size_t>(record.logical_size));
        continue;
      }
      if (record.kind == SnapshotRecord::Kind::kValueChunk) {
        auto& stage = partition.replica_value_stage;
        if (!stage.has_value() || stage->db_id != record.db_id ||
            stage->db_epoch != record.db_epoch ||
            stage->mutation_sequence != record.mutation_sequence ||
            stage->key != record.key ||
            stage->next_chunk != record.chunk_index ||
            stage->chunk_count != record.chunk_count ||
            record.value.empty() ||
            record.value.size() > kExtentPayloadBytes ||
            record.value.size() > stage->logical_size ||
            stage->value.size() > stage->logical_size - record.value.size()) {
          co_return Status(StatusCode::kInvalidArgument,
                           "invalid replicated large value chunk frame");
        }
        stage->value.append(record.value);
        ++stage->next_chunk;
        continue;
      }
      if (record.kind == SnapshotRecord::Kind::kValueCommit) {
        auto& stage = partition.replica_value_stage;
        if (!stage.has_value() || stage->db_id != record.db_id ||
            stage->db_epoch != record.db_epoch ||
            stage->mutation_sequence != record.mutation_sequence ||
            stage->key != record.key || !record.value.empty() ||
            stage->next_chunk != stage->chunk_count ||
            record.chunk_index != stage->chunk_count ||
            stage->value.size() != stage->logical_size) {
          co_return Status(StatusCode::kInvalidArgument,
                           "invalid replicated large value commit frame");
        }
        materialized.emplace(SnapshotRecord{
            .kind = SnapshotRecord::Kind::kValue,
            .db_id = stage->db_id,
            .db_epoch = stage->db_epoch,
            .mutation_sequence = stage->mutation_sequence,
            .expire_at_ms = stage->expire_at_ms,
            .value_type = stage->value_type,
            .logical_size = stage->logical_size,
            .key = std::move(stage->key),
            .value = std::move(stage->value),
        });
        stage.reset();
        effective = &*materialized;
      } else if (partition.replica_value_stage.has_value()) {
        co_return Status(StatusCode::kInvalidArgument,
                         "replicated large value frame sequence interrupted");
      }

      const SnapshotRecord& applied = *effective;
      const Digest digest = ComputeDigest(applied.key);
      auto key_lock = co_await store.key_locks[applied.db_id].Acquire(
          digest, IntentLockMode::kExclusive);
      co_await store.writer_mutex.Lock();
      UnlockGuard write_unlock(&store.writer_mutex, store.worker);
      auto& index = partition.indexes[applied.db_id];
      auto* current = index.Find(digest, applied.key);
      if (current != nullptr &&
          current->value.replication_epoch == replication_epoch &&
          current->value.mutation_sequence >= applied.mutation_sequence) {
        continue;
      }
      const RecordKind kind = applied.kind == SnapshotRecord::Kind::kValue
                                  ? RecordKind::kValue
                                  : RecordKind::kTombstone;
      const ValueType value_type = kind == RecordKind::kValue
                                       ? applied.value_type
                                       : ValueType::kNone;
      if (kind == RecordKind::kValue && value_type == ValueType::kNone) {
        co_return Status(StatusCode::kInvalidArgument,
                         "replicated value has no Redis type");
      }
      Status written;
      const std::size_t inline_bytes = AlignRecord(
          RecordHeaderBytes(applied.key.size()) + applied.value.size());
      if (kind == RecordKind::kValue && value_type == ValueType::kString &&
          inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) {
        auto extents = co_await WriteExtentValueLocked(store, applied.value);
        if (!extents.ok()) {
          co_return extents.status();
        }
        const std::string manifest = EncodeManifest(**extents);
        written = co_await WriteRecordLocked(
            store, applied.db_id, applied.key, manifest, kind, value_type,
            applied.expire_at_ms, digest, applied.mutation_sequence,
            applied.mutation_sequence, 0, false, true, true,
            applied.value.size(), *extents);
        if (!written.ok()) {
          store.worker->Spawn(ReclaimExtents(&store, *extents));
        }
      } else {
        written = co_await WriteRecordLocked(
            store, applied.db_id, applied.key, applied.value, kind, value_type,
            kind == RecordKind::kValue ? applied.expire_at_ms : 0, digest,
            applied.mutation_sequence, applied.mutation_sequence, 0, false);
      }
      if (!written.ok()) {
        co_return written;
      }
      partition.mutation_sequence = std::max(
          partition.mutation_sequence, applied.mutation_sequence);
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
    std::size_t value_offset = 0;
    std::size_t value_bytes = 0;

    std::span<const std::byte> value() const noexcept {
      const std::span<std::byte> buffer = lease.bytes();
      if (value_offset > buffer.size() ||
          value_bytes > buffer.size() - value_offset) {
        return {};
      }
      return buffer.subspan(value_offset, value_bytes);
    }
  };

  std::size_t DirectGetValueLimit() const noexcept {
    const RegisteredBufferPoolOptions& buffers = options_.buffers;
    // Keep the direct-from-disk framing path conservative: with the defaults,
    // values through 1 MiB - 8 KiB avoid the value copy. Larger values retain
    // the materializing memmove path below.
    const std::size_t framing_reserve =
        buffers.read_headroom_bytes + buffers.read_tailroom_bytes;
    return buffers.read_payload_bytes > framing_reserve
               ? buffers.read_payload_bytes - framing_reserve
               : 0;
  }

  StatusOr<DiskValue> EncodeDiskValue(LoadedValue loaded) {
    ReadBufferLease lease = std::move(loaded.lease);
    const std::size_t value_offset = loaded.value_offset;
    const std::size_t value_bytes = loaded.value_bytes;
    std::span<std::byte> buffer = lease.bytes();
    char length[32];
    auto [end, error] =
        std::to_chars(length, length + sizeof(length), value_bytes);
    if (error != std::errc{}) {
      return Status(StatusCode::kInternal, "bulk length formatting failed");
    }
    const std::size_t digits = static_cast<std::size_t>(end - length);
    const std::size_t prefix_bytes = digits + 3;
    if (value_offset < prefix_bytes || value_offset > buffer.size() ||
        value_bytes > buffer.size() - value_offset ||
        buffer.size() - value_offset - value_bytes < 2) {
      return Status(StatusCode::kInternal,
                    "value lacks RESP framing headroom or tailroom");
    }
    std::byte* prefix = buffer.data() + value_offset - prefix_bytes;
    prefix[0] = std::byte{'$'};
    std::memcpy(prefix + 1, length, digits);
    prefix[digits + 1] = std::byte{'\r'};
    prefix[digits + 2] = std::byte{'\n'};
    buffer[value_offset + value_bytes] = std::byte{'\r'};
    buffer[value_offset + value_bytes + 1] = std::byte{'\n'};
    const std::size_t network_offset = value_offset - prefix_bytes;
    return DiskValue(std::move(lease), network_offset,
                     prefix_bytes + value_bytes + 2);
  }

  void QueueExpiredCandidate(WorkerStore& store, std::uint16_t partition_id,
                             std::uint8_t db_id,
                             const RecordIndex::Entry& entry) {
    constexpr std::size_t kMaxQueuedExpiredCandidates = 4096;
    if (!options_.expiration_authority ||
        store.expired_candidates.size() >= kMaxQueuedExpiredCandidates ||
        entry.value.kind != RecordKind::kValue ||
        entry.value.expire_at_ms == 0) {
      return;
    }
    store.expired_candidates.push_back(WorkerStore::ExpireCandidate{
        .partition_id = partition_id,
        .db_id = db_id,
        .digest = entry.digest,
        .mutation_sequence = entry.value.mutation_sequence,
        .expire_at_ms = entry.value.expire_at_ms,
        .key = entry.key,
    });
  }

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

  std::size_t DeviceIndexForBlock(std::uint64_t block_id) const noexcept {
    const std::size_t device_index = DeviceIdForBlock(block_id);
    assert(device_index < devices_.size());
    assert(devices_[device_index].id == device_index);
    return device_index;
  }

  std::pair<std::uint32_t, std::uint64_t> FileOffset(
      std::uint64_t block_id) const noexcept {
    const StorageDevice& device = devices_[DeviceIndexForBlock(block_id)];
    assert(LocalBlockId(block_id) >= device.data_block_begin);
    assert(LocalBlockId(block_id) < device.capacity_blocks);
    return {device.file_index, LocalBlockOffset(block_id)};
  }

  static bool BitmapBit(const DeviceAllocator& allocator,
                        std::uint32_t local_block) noexcept {
    const std::size_t byte_index = local_block / 8;
    const unsigned bit_index = local_block % 8;
    return (std::to_integer<unsigned>(allocator.scan_bitmap[byte_index]) &
            (1U << bit_index)) != 0;
  }

  static void SetBitmapBit(DeviceAllocator& allocator,
                           std::uint32_t local_block) noexcept {
    const std::size_t byte_index = local_block / 8;
    const unsigned bit_index = local_block % 8;
    allocator.scan_bitmap[byte_index] |=
        static_cast<std::byte>(1U << bit_index);
  }

  static void ClearBitmapBit(DeviceAllocator& allocator,
                             std::uint32_t local_block) noexcept {
    const std::size_t byte_index = local_block / 8;
    const unsigned bit_index = local_block % 8;
    allocator.scan_bitmap[byte_index] &=
        static_cast<std::byte>(~(1U << bit_index));
  }

  Task<Status> PersistBitmapPages(
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

  Task<Status> RefillReadyBlocksLocal(std::size_t device_index,
                                      DeviceAllocator& allocator) {
    assert(celer::ThisWorker().id == allocator.owner);
    constexpr std::size_t kActivationBatchBlocks = 256;
    const StorageDevice& device = devices_[device_index];
    std::vector<std::uint64_t> activated;
    activated.reserve(kActivationBatchBlocks);
    while (activated.size() < kActivationBatchBlocks &&
           allocator.next_pristine < device.capacity_blocks) {
      const std::uint32_t local =
          static_cast<std::uint32_t>(allocator.next_pristine++);
      activated.push_back(MakeBlockId(device.id, local));
    }
    while (activated.size() < kActivationBatchBlocks &&
           !allocator.cold_free.empty()) {
      activated.push_back(allocator.cold_free.back());
      allocator.cold_free.pop_back();
    }
    if (activated.empty()) {
      co_return Status::Ok();
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

  Task<StatusOr<ReservedBlock>> AllocateFromDeviceLocal(
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

  Task<StatusOr<ReservedBlock>> AllocateFromDevice(
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

  Task<Status> ReturnReadyBlockLocal(std::size_t device_index,
                                     std::uint64_t block_id) {
    DeviceAllocator& allocator = *device_allocators_[device_index];
    assert(celer::ThisWorker().id == allocator.owner);
    co_await allocator.mutex.Lock();
    UnlockGuard unlock(&allocator.mutex, stores_[allocator.owner]->worker);
    assert(BitmapBit(allocator, LocalBlockId(block_id)));
    allocator.ready_blocks.push_back(block_id);
    co_return Status::Ok();
  }

  Task<Status> ReturnReadyBlock(std::uint64_t block_id) {
    const std::size_t device_index = DeviceIndexForBlock(block_id);
    const celer::WorkerId owner = device_allocators_[device_index]->owner;
    if (celer::ThisWorker().id == owner) {
      co_return co_await ReturnReadyBlockLocal(device_index, block_id);
    }
    co_return co_await celer::SubmitTaskTo(
        owner, [this, device_index, block_id]() -> Task<Status> {
          co_return co_await ReturnReadyBlockLocal(device_index, block_id);
        });
  }

  Task<Status> ReturnColdBlocksLocal(
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

  Task<Status> ReturnColdBlocks(std::vector<std::uint64_t> block_ids) {
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
      Status returned =
          owner == celer::ThisWorker().id
              ? co_await ReturnColdBlocksLocal(
                    device_index, std::move(by_device[device_index]))
              : co_await celer::SubmitTaskTo(
                    owner,
                    [this, device_index,
                     blocks = std::move(by_device[device_index])]() mutable
                        -> Task<Status> {
                      co_return co_await ReturnColdBlocksLocal(
                          device_index, std::move(blocks));
                    });
      if (!returned.ok()) {
        co_return returned;
      }
    }
    co_return Status::Ok();
  }

  Task<Status> PersistEpochValueOnDeviceLocal(std::size_t device_index,
                                              std::size_t value_index,
                                              std::uint64_t epoch) {
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

  Task<Status> PersistEpochValue(std::size_t value_index,
                                 std::uint64_t epoch) {
    if (value_index >= kEpochValueCount) {
      co_return Status(StatusCode::kOutOfRange,
                       "epoch metadata index is out of range");
    }
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const celer::WorkerId owner =
          device_allocators_[device_index]->owner;
      Status persisted = owner == celer::ThisWorker().id
          ? co_await PersistEpochValueOnDeviceLocal(
                device_index, value_index, epoch)
          : co_await celer::SubmitTaskTo(
                owner,
                [this, device_index, value_index, epoch]() -> Task<Status> {
                  co_return co_await PersistEpochValueOnDeviceLocal(
                      device_index, value_index, epoch);
                });
      if (!persisted.ok()) {
        co_return persisted;
      }
    }
    co_return Status::Ok();
  }

  // Takes the database out of service on this worker. Everything here is O(the
  // partition count) and runs without suspending, so the caller's FLUSHDB gate
  // stays closed for a bounded time no matter how many keys the database holds.
  // Retiring the detached entries is left to ReclaimDetachedIndexes.
  void DetachDbLocal(WorkerStore& store, std::uint8_t db_id) {
    ++store.index_generations[db_id];
    for (auto& partition : store.partitions) {
      store.detached_indexes.push_back(DetachedIndex{
          .index = partition.indexes[db_id].Detach(),
          .db_id = db_id,
      });
      partition.live_key_count[db_id] = 0;
      partition.expiring_key_count[db_id] = 0;
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
  }

  // Retires the entries FLUSHDB detached: subtracts what they contributed to
  // their blocks, then frees them. Readers can no longer reach any of it, so
  // this may run long after the command replied.
  //
  // Each block is credited once for the whole population rather than once per
  // record, which is the same total by construction and turns a per-record
  // cross-core hop into a per-block one. A block cannot be recycled underneath
  // an outstanding subtraction: whatever is still owed keeps its live_bytes
  // above zero, so it cannot reach the empty-block path until this settles.
  Task<Status> ReclaimDetachedIndexes(WorkerStore& store) {
    while (!store.detached_indexes.empty()) {
      DetachedIndex detached = std::move(store.detached_indexes.front());
      store.detached_indexes.pop_front();

      struct BlockDelta {
        std::uint64_t bytes = 0;
        std::uint16_t block_owner = 0;
      };
      // Keyed by allocation epoch as well as block id: entries naming the same
      // block at different epochs are an accounting violation rather than
      // something to sum, so they stay separate and MarkRecordDeadLocal's epoch
      // check rejects the stale one instead of the total silently absorbing it.
      // Bounded by the number of blocks the population touched, not by the
      // number of records in it.
      absl::flat_hash_map<std::pair<std::uint64_t, std::uint64_t>, BlockDelta>
          dead_by_block;
      // An external value's manifest is what the block accounting above sees;
      // the extent blocks holding its payload are owned by the manifest and
      // released as a unit, so they are collected per record rather than
      // folded into the per-block totals.
      std::vector<std::shared_ptr<const std::vector<ExtentRef>>> dead_extents;
      detached.index.ForEach([&](const RecordIndex::Entry& entry) {
        BlockDelta& delta = dead_by_block[std::pair(
            entry.value.block_id, entry.value.allocation_epoch)];
        delta.block_owner = entry.value.block_owner;
        delta.bytes += entry.value.total_disk_bytes;
        if (entry.value.external && entry.value.extents != nullptr) {
          dead_extents.push_back(entry.value.extents);
        }
      });

      for (const auto& extents : dead_extents) {
        store.worker->Spawn(ReclaimExtents(&store, extents));
      }

      for (const auto& [block, delta] : dead_by_block) {
        // A block holds at most kStorageBlockBytes, so the sum still fits the
        // per-record width.
        assert(delta.bytes <= kStorageBlockBytes);
        RecordLocation aggregate{
            .block_id = block.first,
            .allocation_epoch = block.second,
            .block_owner = delta.block_owner,
            .total_disk_bytes = static_cast<std::uint32_t>(delta.bytes),
        };
        Status dead = co_await MarkRecordDead(aggregate);
        if (!dead.ok()) {
          store.write_failed = true;
          co_return dead;
        }
      }

      // Freeing the entries is the expensive part of this loop, and it happens
      // as `detached` goes out of scope. Yield so online work is polled between
      // populations.
      co_await celer::Yield(*store.worker);
    }

    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);
    SealDeadActiveBlock(store);
    co_return Status::Ok();
  }

  // At most one reclaimer per store, so the two ways in — a background one that
  // FLUSHDB ASYNC leaves behind, and a caller waiting for SYNC — never split a
  // population between them. Whichever runs picks up work queued after it
  // started, so an arriving FLUSHDB only has to make sure one is alive.
  void EnsureDetachedReclaim(WorkerStore& store) {
    if (store.detached_reclaim_running || store.detached_indexes.empty()) {
      return;
    }
    store.detached_reclaim_running = true;
    store.worker->SpawnBackground(RunDetachedReclaim(&store));
  }

  Task<Status> RunDetachedReclaim(WorkerStore* store) {
    Status status = co_await ReclaimDetachedIndexes(*store);
    store->detached_reclaim_running = false;
    if (!status.ok()) {
      spdlog::error("worker[{}] detached index reclaim failed: {}",
                    store->worker->id(), status.message());
    }
    co_return status;
  }

  // Waits for everything detached so far to be retired. The keyspace is already
  // empty either way; this is what makes FLUSHDB SYNC mean the memory came back
  // before the reply.
  Task<Status> AwaitDetachedReclaim(WorkerStore& store) {
    EnsureDetachedReclaim(store);
    while (store.detached_reclaim_running || !store.detached_indexes.empty()) {
      Status waited = co_await celer::SleepFor(
          *store.worker, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        co_return waited;
      }
    }
    if (store.write_failed) {
      co_return Status(StatusCode::kInternal,
                       "storage writer stopped while reclaiming flushed keys");
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

  std::uint16_t RecoveredBlockOwner(const BlockHeader& block,
                                    std::uint64_t block_id) const noexcept {
    // writer_id belongs to the topology that wrote the block and may be
    // greater than the current worker count after a scale-down.
    if (block.layout_worker_count == worker_count_ &&
        block.writer_id < worker_count_) {
      return static_cast<std::uint16_t>(block.writer_id);
    }
    std::uint64_t mixed = block_id ^
                          (block.allocation_epoch + 0x9e3779b97f4a7c15ULL);
    mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
    mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
    mixed ^= mixed >> 31;
    return static_cast<std::uint16_t>(mixed % worker_count_);
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
    const std::uint64_t data_blocks = total_data_blocks_;
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

    std::uint64_t device_linear_begin = 0;
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const StorageDevice& device = devices_[device_index];
      const std::uint64_t first_device_offset =
          (store.worker->id() + worker_count_ -
           device_linear_begin % worker_count_) %
          worker_count_;
      for (std::uint64_t device_offset = first_device_offset;
           device_offset < device.data_block_count;
           device_offset += worker_count_) {
        const std::uint32_t local_block = static_cast<std::uint32_t>(
            device.data_block_begin + device_offset);
        const std::uint64_t block_id = MakeBlockId(device.id, local_block);
        const DeviceAllocator& allocator = *device_allocators_[device_index];
        const std::size_t bitmap_byte = local_block / 8;
        const unsigned bitmap_bit = local_block % 8;
        if ((std::to_integer<unsigned>(allocator.scan_bitmap[bitmap_byte]) &
             (1U << bitmap_bit)) == 0) {
          ReportRecoveryProgress(0);
          continue;
        }
        const std::uint32_t file_id = device.file_index;
        const std::uint64_t block_offset =
            static_cast<std::uint64_t>(local_block) * kStorageBlockBytes;
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

        BlockHeader block{};
        if (!DecodeBlockHeader(block_bytes, &block) ||
            block.block_id != block_id) {
          co_return Status(StatusCode::kInternal,
                           "invalid or corrupt block header");
        }
        AtomicMax(&recovery_device_cursors_[device_index].next_local,
                  static_cast<std::uint64_t>(local_block) + 1);
        AtomicMax(
            &recovery_device_cursors_[device_index].next_allocation_epoch,
            block.allocation_epoch + 1);
        AtomicMax(&next_lsn_, block.max_lsn + 1);

        const std::uint16_t block_owner =
            RecoveredBlockOwner(block, block_id);
        batches->at(block_owner).blocks.push_back(RecoveryBlock{ActiveBlock{
            .block_id = block_id,
            .writer_id = block.writer_id,
            .layout_worker_count = block.layout_worker_count,
            .allocation_epoch = block.allocation_epoch,
            .committed_bytes = block.committed_bytes,
            .record_count = block.record_count,
            .max_lsn = block.max_lsn,
            .kind = block.kind,
            .extent_index = block.extent_index,
            .extent_payload_checksum = block.extent_payload_checksum,
        }});

        if (block.kind == BlockKind::kValueExtent) {
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
          const std::uint16_t partition_id = RedisSlot(key);
          if (record.replication_epoch !=
              epoch_values_[kLogicalDatabaseCount + partition_id]) {
            record_offset += record.total_disk_bytes;
            ++records;
            continue;
          }
          const unsigned key_owner = OwnerForKey(key);
          std::shared_ptr<const std::vector<ExtentRef>> extents;
          if (record.external) {
            if (record.kind != RecordKind::kValue ||
                record.value_type != ValueType::kString) {
              co_return Status(StatusCode::kInternal,
                               "unsupported external record type");
            }
            const std::byte* payload =
                recovery.buffer.data + record_offset + record.header_bytes;
            if (Crc32c(std::span<const std::byte>(payload,
                                                  record.payload_bytes)) !=
                record.payload_checksum) {
              co_return Status(StatusCode::kInternal,
                               "external manifest checksum mismatch");
            }
            auto decoded = DecodeManifest(
                std::span<const std::byte>(payload, record.payload_bytes),
                record.logical_size);
            if (!decoded.ok()) {
              co_return decoded.status();
            }
            extents = std::move(*decoded);
          }
          batches->at(key_owner).records.push_back(RecoveryRecord{
              .digest = record.digest,
              .key = std::string(key),
              .db_id = record.db_id,
              .location = RecordLocation{
                  .block_id = block_id,
                  .replication_epoch = record.replication_epoch,
                  .mutation_sequence = record.mutation_sequence,
                  .allocation_epoch = record.allocation_epoch,
                  .expire_at_ms = record.expire_at_ms,
                  .block_owner = block_owner,
                  .record_offset = record_offset,
                  .total_disk_bytes = record.total_disk_bytes,
                  .logical_size = record.logical_size,
                  .payload_bytes = record.payload_bytes,
                  .relocation_sequence = static_cast<std::uint32_t>(
                      record.relocation_sequence),
                  .external = record.external,
                  .kind = record.kind,
                  .value_type = record.value_type,
                  .extents = std::move(extents),
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
      device_linear_begin += device.data_block_count;
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
      state.kind = block.kind;
      state.extent_index = block.extent_index;
      state.extent_payload_checksum = block.extent_payload_checksum;

      // Recovered blocks have no staging buffer. Keep partial blocks sealed;
      // appending to one would otherwise dereference an absent in-memory copy.
    }

    for (const RecoveryRecord& recovered : batch.records) {
      auto& partition = PartitionForKey(store, recovered.key);
      if (recovered.location.replication_epoch !=
          partition.replication_epoch) {
        continue;
      }
      partition.mutation_sequence = std::max(
          partition.mutation_sequence, recovered.location.mutation_sequence);
      auto& index = partition.indexes[recovered.db_id];
      auto* found = index.Find(recovered.digest, recovered.key);
      if (found == nullptr ||
          IsNewer(recovered.location, found->value)) {
        const bool was_live =
          found != nullptr && found->value.kind == RecordKind::kValue;
        const bool is_live = recovered.location.kind == RecordKind::kValue;
        const bool was_expiring =
            was_live && found->value.expire_at_ms != 0;
        const bool is_expiring =
            is_live && recovered.location.expire_at_ms != 0;
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
        if (was_expiring != is_expiring) {
          if (is_expiring) {
            ++partition.expiring_key_count[recovered.db_id];
          } else {
            --partition.expiring_key_count[recovered.db_id];
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

  Task<StatusOr<LoadedValue>> LoadExternalValueLocal(
      WorkerStore& store, const RecordLocation& location,
      ReadLatencyTrace* trace) {
    if (!location.external || location.extents == nullptr ||
        location.logical_size > kMaxStringBytes) {
      co_return Status(StatusCode::kInternal,
                       "external value has no valid extent manifest");
    }
    if (trace != nullptr) {
      trace->buffer_acquire_start_ns = ReadTraceNowNanos();
    }
    auto acquired = co_await store.buffers.AcquireReadBuffer(
        static_cast<std::size_t>(location.logical_size));
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    ReadBufferLease output = std::move(*acquired);
    FixedBuffer destination = output.io_buffer();
    if (destination.size < location.logical_size) {
      co_return Status(StatusCode::kOutOfRange,
                       "external value exceeds read buffer capacity");
    }
    if (trace != nullptr) {
      trace->buffer_acquired_ns = ReadTraceNowNanos();
      trace->heap_read_buffer = !output.registered();
      trace->disk_read = true;
    }
    std::size_t output_offset = 0;
    for (std::size_t index = 0; index < location.extents->size(); ++index) {
      const ExtentRef& ref = location.extents->at(index);
      BlockState* state = FindBlockState(store, ref.block_id);
      if (state == nullptr || !state->allocated || state->freeing ||
          state->kind != BlockKind::kValueExtent ||
          state->allocation_epoch != ref.allocation_epoch) {
        co_return Status(StatusCode::kInternal,
                         "stale or missing external extent");
      }
      ++state->pins;
      struct ExtentPin {
        BlockState* state;
        ~ExtentPin() { --state->pins; }
      } pin{state};
      const std::size_t read_bytes =
          AlignDirect(kBlockHeaderBytes + ref.payload_bytes);
      auto temp_acquired = co_await store.buffers.AcquireReadBuffer(read_bytes);
      if (!temp_acquired.ok()) {
        co_return temp_acquired.status();
      }
      ReadBufferLease temp = std::move(*temp_acquired);
      FixedBuffer io = temp.io_buffer();
      io.size = read_bytes;
      const auto [file_id, block_offset] = FileOffset(ref.block_id);
      if (trace != nullptr && index == 0) {
        trace->io_submit_ns = ReadTraceNowNanos();
      }
      auto read = co_await ReadStorageBuffer(*store.worker,
                                             store.files[file_id], io,
                                             temp.registered(), block_offset);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != read_bytes) {
        co_return Status(StatusCode::kInternal, "short extent block read");
      }
      BlockHeader header{};
      if (!DecodeBlockHeader(
              std::span<const std::byte, kBlockHeaderBytes>(
                  io.data, kBlockHeaderBytes),
              &header) ||
          header.kind != BlockKind::kValueExtent ||
          header.block_id != ref.block_id ||
          header.allocation_epoch != ref.allocation_epoch ||
          header.extent_index != index ||
          header.extent_payload_bytes != ref.payload_bytes ||
          header.extent_payload_checksum != ref.payload_checksum ||
          output_offset + ref.payload_bytes > location.logical_size) {
        co_return Status(StatusCode::kInternal,
                         "extent header does not match manifest");
      }
      const auto payload = std::span<const std::byte>(
          io.data + kBlockHeaderBytes, ref.payload_bytes);
      if (options_.verify_read_crc && Crc32c(payload) != ref.payload_checksum) {
        co_return Status(StatusCode::kInternal,
                         "extent payload checksum mismatch");
      }
      std::memcpy(destination.data + output_offset, payload.data(),
                  payload.size());
      output_offset += payload.size();
    }
    if (output_offset != location.logical_size) {
      co_return Status(StatusCode::kInternal,
                       "external value length does not match manifest");
    }
    if (trace != nullptr) {
      trace->io_complete_ns = ReadTraceNowNanos();
      trace->decode_done_ns = trace->io_complete_ns;
    }
    const std::size_t value_offset = static_cast<std::size_t>(
        destination.data - output.bytes().data());
    co_return LoadedValue{std::move(output), value_offset, output_offset};
  }

  Task<StatusOr<LoadedValue>> LoadValueLocal(
      WorkerStore& store, std::uint8_t db_id, std::string_view key,
      const Digest& digest, RecordLocation location,
      ReadLatencyTrace* trace = nullptr) {
    if (location.external) {
      co_return co_await LoadExternalValueLocal(store, location, trace);
    }
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
      if (location.external) {
        co_return Status(StatusCode::kInternal,
                         "external value requires extent loading");
      }
      if (location.payload_bytes > io.size) {
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
          record.expire_at_ms != location.expire_at_ms ||
          record.value_type != location.value_type ||
          record.external != location.external ||
          location.logical_size != record.logical_size ||
          location.payload_bytes != record.payload_bytes ||
          location.total_disk_bytes != record.total_disk_bytes) {
        co_return Status(StatusCode::kInternal,
                         "record does not match in-memory location");
      }
      std::memcpy(io.data, record_bytes + record.header_bytes,
                  location.payload_bytes);
      if (options_.verify_read_crc &&
          Crc32c(std::span<const std::byte>(io.data, record.payload_bytes)) !=
              record.payload_checksum) {
        co_return Status(StatusCode::kInternal,
                         "record value checksum mismatch");
      }
      if (trace != nullptr) {
        trace->decode_done_ns = ReadTraceNowNanos();
      }
      const std::size_t value_offset = static_cast<std::size_t>(
          io.data - lease.bytes().data());
      co_return LoadedValue{std::move(lease), value_offset,
                            record.payload_bytes};
    }

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
    if (trace != nullptr) {
      trace->buffer_acquire_start_ns = ReadTraceNowNanos();
    }
    auto acquired = co_await store.buffers.AcquireReadBuffer(read_bytes);
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
        record.expire_at_ms != location.expire_at_ms ||
        record.value_type != location.value_type ||
        record.external != location.external ||
        record.logical_size != location.logical_size ||
        record.payload_bytes != location.payload_bytes ||
        record.total_disk_bytes != location.total_disk_bytes) {
      co_return Status(StatusCode::kInternal,
                       "record does not match in-memory location");
    }
    const std::byte* value_data = record_data + record.header_bytes;
    if (options_.verify_read_crc &&
        Crc32c(std::span<const std::byte>(value_data, record.payload_bytes)) !=
            record.payload_checksum) {
      co_return Status(StatusCode::kInternal, "record value checksum mismatch");
    }
    const std::byte* framed_value = value_data;
    if (record.payload_bytes > DirectGetValueLimit()) {
      std::memmove(io.data, value_data, record.payload_bytes);
      framed_value = io.data;
    }
    if (trace != nullptr) {
      trace->decode_done_ns = ReadTraceNowNanos();
    }
    const std::size_t value_offset = static_cast<std::size_t>(
        framed_value - lease.bytes().data());
    co_return LoadedValue{std::move(lease), value_offset,
                          record.payload_bytes};
  }

  std::uint64_t ForegroundBlocksForDevice(
      std::size_t device_index) const noexcept {
    const std::uint64_t data_blocks = devices_[device_index].data_block_count;
    const std::size_t reserve = DefragReserveForDevice(device_index);
    return data_blocks > reserve ? data_blocks - reserve : 0;
  }

  void ConfigureDefragReserves() {
    defrag_reserve_blocks_.assign(devices_.size(),
                                  kDefragReserveBlocksPerDevice);
  }

  void ConfigureWorkerDeviceAffinity() {
    std::vector<std::size_t> usable_devices;
    std::uint64_t total_weight = 0;
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const std::uint64_t weight =
          ForegroundBlocksForDevice(device_index);
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
      unsigned assigned_workers =
          static_cast<unsigned>(usable_devices.size());
      for (const std::size_t device_index : usable_devices) {
        const std::uint64_t weighted =
            remaining_workers * ForegroundBlocksForDevice(device_index);
        quotas[device_index] +=
            static_cast<unsigned>(weighted / total_weight);
        assigned_workers +=
            static_cast<unsigned>(weighted / total_weight);
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
              (!selected_valid || smooth_current[device_index] >
                                      smooth_current[selected])) {
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
        const auto lightest = std::min_element(worker_weights.begin(),
                                               worker_weights.end());
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
      spdlog::info(
          "storage device id={} foreground-weight={} home-workers={}",
          devices_[device_index].id,
          ForegroundBlocksForDevice(device_index),
          home_workers[device_index]);
    }
  }

  Task<StatusOr<ReservedBlock>> AllocateBlock(WorkerStore& store,
                                              bool for_defrag) {
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
          active_flushes_.load(std::memory_order_acquire) != 0) {
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
      co_return Status(
          StatusCode::kResourceExhausted,
          "no foreground blocks remain and no flush or defrag can reclaim space");
    }
  }

  std::size_t DefragReserveForDevice(std::size_t device_index) const noexcept {
    assert(device_index < defrag_reserve_blocks_.size());
    return defrag_reserve_blocks_[device_index];
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
    // live_bytes is the byte sum over exactly the index entries naming this
    // block at this allocation epoch, so the entry being retired here is one of
    // the summands and the subtraction cannot underflow. Saturating instead of
    // reporting would drive live_bytes to zero while records are still
    // reachable, which CleanBlockLocked now reads as "nothing to salvage" and
    // frees without inspecting the block. Fail loudly rather than lose data.
    if (state->live_bytes < location.total_disk_bytes) {
      return Status(StatusCode::kInternal,
                    "block live-byte accounting underflow");
    }
    state->live_bytes -= location.total_disk_bytes;
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

  Task<StatusOr<ReservedBlock>> TakeStandaloneBlockLocked(
      WorkerStore& store) {
    while (!store.standby_block.has_value()) {
      Status waited = co_await WaitForStandbyWithWriterUnlocked(store, false);
      if (!waited.ok()) {
        co_return waited;
      }
    }
    const ReservedBlock block = *store.standby_block;
    store.standby_block.reset();
    RequestStandbyBlock(store, false);
    co_return block;
  }

  Task<StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
  WriteExtentValueLocked(WorkerStore& store, std::string_view value) {
    if (value.empty() || value.size() > kMaxStringBytes) {
      co_return Status(StatusCode::kOutOfRange,
                       "String exceeds the 512 MiB limit");
    }
    auto refs = std::make_shared<std::vector<ExtentRef>>();
    refs->reserve((value.size() + kExtentPayloadBytes - 1) /
                  kExtentPayloadBytes);
    auto reclaim_allocated = [&]() {
      if (!refs->empty()) {
        store.worker->Spawn(ReclaimExtents(
            &store,
            std::shared_ptr<const std::vector<ExtentRef>>(refs)));
      }
    };
    std::size_t value_offset = 0;
    std::uint32_t extent_index = 0;
    while (value_offset < value.size()) {
      auto reserved = co_await TakeStandaloneBlockLocked(store);
      if (!reserved.ok()) {
        reclaim_allocated();
        co_return reserved.status();
      }
      const std::size_t payload_bytes =
          std::min(kExtentPayloadBytes, value.size() - value_offset);
      const auto payload = std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(value.data() + value_offset),
          payload_bytes);
      const std::uint32_t payload_checksum = Crc32c(payload);
      BlockState& state = CreateBlockState(store, reserved->block_id);
      state = BlockState{};
      state.writer_id = store.worker->id();
      state.layout_worker_count = worker_count_;
      state.allocation_epoch = reserved->allocation_epoch;
      state.committed_bytes = static_cast<std::uint32_t>(
          kBlockHeaderBytes + payload_bytes);
      state.live_bytes = static_cast<std::uint32_t>(payload_bytes);
      state.allocated = true;
      state.kind = BlockKind::kValueExtent;
      state.extent_index = extent_index;
      state.extent_payload_checksum = payload_checksum;
      refs->push_back(ExtentRef{
          .block_id = reserved->block_id,
          .allocation_epoch = reserved->allocation_epoch,
          .payload_bytes = static_cast<std::uint32_t>(payload_bytes),
          .payload_checksum = payload_checksum,
      });
      std::uint16_t write_buffer_id = 0;
      std::byte* heap_buffer = nullptr;
      if (!store.buffers.TryAcquireWriteBuffer(&write_buffer_id) &&
          !store.buffers.TryAcquireHeapWriteBuffer(&heap_buffer)) {
        reclaim_allocated();
        co_return Status(StatusCode::kResourceExhausted,
                         "no extent write buffer is available");
      }
      auto release_buffer = [&]() {
        if (write_buffer_id != 0) {
          store.buffers.ReleaseWriteBuffer(write_buffer_id);
        } else {
          store.buffers.ReleaseHeapWriteBuffer(heap_buffer);
        }
      };
      FixedBuffer staging =
          write_buffer_id != 0
              ? store.buffers.write_buffer(write_buffer_id)
              : FixedBuffer{.data = heap_buffer,
                            .size = options_.buffers.write_buffer_bytes,
                            .index = 0};
      if (staging.data == nullptr || staging.size < kStorageBlockBytes) {
        release_buffer();
        reclaim_allocated();
        co_return Status(StatusCode::kInternal,
                         "extent staging buffer is smaller than a block");
      }
      std::fill_n(staging.data, kStorageBlockBytes, std::byte{0});
      BlockHeader header{
          .magic = kBlockMagic,
          .block_id = reserved->block_id,
          .version = kStorageFormatVersion,
          .header_bytes = kBlockHeaderBytes,
          .block_bytes = kStorageBlockBytes,
          .writer_id = store.worker->id(),
          .allocation_epoch = reserved->allocation_epoch,
          .committed_bytes = static_cast<std::uint32_t>(
              kBlockHeaderBytes + payload_bytes),
          .record_count = 0,
          .max_lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed),
          .checksum = 0,
          .layout_worker_count = worker_count_,
          .kind = BlockKind::kValueExtent,
          .reserved = {},
          .extent_index = extent_index,
          .extent_payload_bytes = static_cast<std::uint32_t>(payload_bytes),
          .extent_payload_checksum = payload_checksum,
      };
      EncodeBlockHeader(
          header, std::span<std::byte, kBlockHeaderBytes>(staging.data,
                                                         kBlockHeaderBytes));
      std::memcpy(staging.data + kBlockHeaderBytes, payload.data(),
                  payload.size());
      const auto [file_id, block_offset] = FileOffset(reserved->block_id);
      const std::size_t write_bytes = AlignDirect(payload_bytes);
      bool write_ok = true;
      Status write_status = Status::Ok();
      for (std::size_t offset = 0; offset < write_bytes;) {
        const std::size_t chunk =
            std::min(options_.flush_size_bytes, write_bytes - offset);
        auto written = co_await WriteStorageBuffer(
            *store.worker, store.files[file_id],
            std::span<const std::byte>(
                staging.data + kBlockHeaderBytes + offset, chunk),
            write_buffer_id != 0, staging,
            block_offset + kBlockHeaderBytes + offset);
        if (!written.ok() || *written != chunk) {
          write_ok = false;
          write_status = written.ok()
                             ? Status(StatusCode::kInternal,
                                      "short extent block write")
                             : written.status();
          break;
        }
        offset += chunk;
      }
      if (write_ok) {
        write_status =
            co_await celer::Fdatasync(*store.worker, store.files[file_id]);
      }
      if (write_status.ok()) {
        auto written = co_await WriteStorageBuffer(
            *store.worker, store.files[file_id],
            std::span<const std::byte>(staging.data, kBlockHeaderBytes),
            write_buffer_id != 0, staging, block_offset);
        if (!written.ok() || *written != kBlockHeaderBytes) {
          write_status = written.ok()
                             ? Status(StatusCode::kInternal,
                                      "short extent header write")
                             : written.status();
        }
      }
      if (write_status.ok()) {
        write_status =
            co_await celer::Fdatasync(*store.worker, store.files[file_id]);
      }
      release_buffer();
      if (!write_status.ok()) {
        store.write_failed = true;
        reclaim_allocated();
        co_return write_status;
      }
      value_offset += payload_bytes;
      ++extent_index;
    }
    co_return std::shared_ptr<const std::vector<ExtentRef>>(std::move(refs));
  }

  Task<Status> AppendLocked(WorkerStore& store,
                            WorkerStore::PartitionStore& partition,
                            std::uint8_t db_id,
                            std::string_view key, std::string_view value,
                            RecordKind kind, ValueType value_type,
                            std::uint64_t expire_at_ms) {
    const Digest digest = ComputeDigest(key);
    const std::uint64_t mutation_sequence = ++partition.mutation_sequence;
    Status status = Status::Ok();
    const std::size_t inline_bytes =
        AlignRecord(RecordHeaderBytes(key.size()) + value.size());
    if (kind == RecordKind::kValue && value_type == ValueType::kString &&
        inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) {
      auto extents = co_await WriteExtentValueLocked(store, value);
      if (!extents.ok()) {
        co_return extents.status();
      }
      const std::string manifest = EncodeManifest(**extents);
      status = co_await WriteRecordLocked(
          store, db_id, key, manifest, kind, value_type, expire_at_ms, digest,
          mutation_sequence, mutation_sequence, 0, false, true, true,
          value.size(), *extents);
      if (!status.ok()) {
        store.worker->Spawn(ReclaimExtents(&store, *extents));
      }
    } else {
      status = co_await WriteRecordLocked(
          store, db_id, key, value, kind, value_type, expire_at_ms, digest,
          mutation_sequence, mutation_sequence, 0, false);
    }
    if (status.ok() && partition.capture_deltas) {
      AppendDelta(partition, SnapshotRecord{
                                 .kind = kind == RecordKind::kValue
                                             ? SnapshotRecord::Kind::kValue
                                             : SnapshotRecord::Kind::kDelete,
                                 .db_id = db_id,
                                 .db_epoch = DbEpoch(db_id),
                                 .mutation_sequence = mutation_sequence,
                                 .expire_at_ms = expire_at_ms,
                                 .value_type = value_type,
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

  Task<Status> FetchStandbyBlock(WorkerStore* store, bool for_defrag) {
    auto allocated = co_await AllocateBlock(*store, for_defrag);
    store->standby_request_pending = false;
    if (allocated.ok()) {
      store->standby_block = *allocated;
      store->standby_error.reset();
    } else {
      store->standby_error = allocated.status();
    }
    store->standby_ready.NotifyAll(*store->worker);
    co_return allocated.ok() ? Status::Ok() : allocated.status();
  }

  void RequestStandbyBlock(WorkerStore& store, bool for_defrag) {
    if (store.standby_block.has_value() || store.standby_request_pending ||
        store.write_failed ||
        shutdown_flush_requested_.load(std::memory_order_acquire)) {
      return;
    }
    store.standby_error.reset();
    store.standby_request_pending = true;
    if (for_defrag) {
      store.worker->SpawnBackground(FetchStandbyBlock(&store, true));
    } else {
      store.worker->Spawn(FetchStandbyBlock(&store, false));
    }
  }

  void MaybePrefetchStandby(WorkerStore& store) {
    if (!store.active_block.has_value()) {
      return;
    }
    constexpr std::uint64_t kPrefetchNumerator = 3;
    constexpr std::uint64_t kPrefetchDenominator = 4;
    const std::uint64_t usable = kStorageBlockBytes - kBlockHeaderBytes;
    const std::uint64_t used =
        store.active_block->committed_bytes - kBlockHeaderBytes;
    if (used * kPrefetchDenominator >= usable * kPrefetchNumerator) {
      RequestStandbyBlock(store, false);
    }
  }

  Task<Status> WaitForStandbyWithWriterUnlocked(WorkerStore& store,
                                                bool for_defrag) {
    RequestStandbyBlock(store, for_defrag);
    store.writer_mutex.Unlock(*store.worker);
    co_await store.standby_ready.Wait();
    co_await store.writer_mutex.Lock();
    if (store.standby_error.has_value()) {
      Status status = *store.standby_error;
      store.standby_error.reset();
      co_return status;
    }
    co_return Status::Ok();
  }

  Task<Status> WaitForStandbyWithWriterLocked(WorkerStore& store,
                                              bool for_defrag) {
    RequestStandbyBlock(store, for_defrag);
    co_await store.standby_ready.Wait();
    if (store.standby_error.has_value()) {
      Status status = *store.standby_error;
      store.standby_error.reset();
      co_return status;
    }
    co_return Status::Ok();
  }

  Task<Status> WriteRecordLocked(WorkerStore& store, std::uint8_t db_id,
                                 std::string_view key, std::string_view value,
                                 RecordKind kind, ValueType value_type,
                                 std::uint64_t expire_at_ms,
                                 const Digest& digest,
                                 std::uint64_t generation,
                                 std::uint64_t mutation_sequence,
                                 std::uint64_t relocation_sequence,
                                 bool for_defrag,
                                 bool unlock_writer_while_waiting = true,
                                 bool external = false,
                                 std::uint64_t logical_size =
                                     std::numeric_limits<std::uint64_t>::max(),
                                 std::shared_ptr<const std::vector<ExtentRef>>
                                     extents = nullptr) {
    if (store.write_failed ||
        epoch_metadata_failed_.load(std::memory_order_acquire)) {
      co_return Status(StatusCode::kFailedPrecondition,
                       "storage writer is stopped after an IO failure");
    }
    if (logical_size == std::numeric_limits<std::uint64_t>::max()) {
      logical_size = value.size();
    }
    if ((kind == RecordKind::kValue && value_type == ValueType::kNone) ||
        (kind == RecordKind::kTombstone &&
         (value_type != ValueType::kNone || expire_at_ms != 0 ||
          !value.empty() || logical_size != 0 || external)) ||
        (external &&
         (kind != RecordKind::kValue || value_type != ValueType::kString ||
          extents == nullptr || extents->empty()))) {
      co_return Status(StatusCode::kInvalidArgument,
                       "invalid value type or expiration metadata");
    }
    if (key.size() > MaxKeyBytes()) {
      co_return Status(StatusCode::kOutOfRange,
                       "key is too large for the on-disk record header");
    }
    const std::size_t record_header_bytes = RecordHeaderBytes(key.size());
    const std::size_t payload_bytes = value.size();
    const std::size_t total_disk_bytes = AlignRecord(
        record_header_bytes + payload_bytes);
    if (total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
        total_disk_bytes > options_.buffers.write_buffer_bytes) {
      co_return Status(StatusCode::kOutOfRange,
                       "record payload does not fit an inline block");
    }

    // Logical partitions route keys, but physical append streams are per
    // worker. This keeps foreground writes local and bounds active 8 MiB
    // buffers by worker count rather than logical partition count.
    const celer::WorkerId writer_id = store.worker->id();
    auto& partition = PartitionForKey(store, key);
    auto& index = partition.indexes[db_id];
    const std::uint64_t lsn =
        next_lsn_.fetch_add(1, std::memory_order_relaxed);

    auto& active = store.active_block;
    if (!active.has_value() ||
        active->committed_bytes + total_disk_bytes > kStorageBlockBytes) {
      if (active.has_value()) {
        RequestFlush(store, active->block_id);
        active.reset();
      }
      while (!store.standby_block.has_value()) {
        Status standby = Status::Ok();
        if (unlock_writer_while_waiting) {
          standby =
              co_await WaitForStandbyWithWriterUnlocked(store, for_defrag);
        } else {
          standby = co_await WaitForStandbyWithWriterLocked(store,
                                                            for_defrag);
        }
        if (!standby.ok()) {
          co_return standby;
        }
        // Another writer may have installed an active block while this
        // coroutine had writer_mutex released. Reuse it instead of consuming
        // a second standby and overwriting that active block.
        if (active.has_value() &&
            active->committed_bytes + total_disk_bytes <=
                kStorageBlockBytes) {
          break;
        }
      }
      if (!active.has_value() ||
          active->committed_bytes + total_disk_bytes > kStorageBlockBytes) {
        if (active.has_value()) {
          RequestFlush(store, active->block_id);
          active.reset();
        }
        std::uint16_t write_buffer_id = 0;
        std::byte* heap_buffer = nullptr;
        if (!store.buffers.TryAcquireWriteBuffer(&write_buffer_id)) {
          if (!store.buffers.TryAcquireHeapWriteBuffer(&heap_buffer)) {
            co_return Status(StatusCode::kResourceExhausted,
                             "no registered or fallback write buffers");
          }
        }
        FixedBuffer staging_buffer =
            write_buffer_id != 0
                ? store.buffers.write_buffer(write_buffer_id)
                : FixedBuffer{.data = heap_buffer,
                              .size = options_.buffers.write_buffer_bytes,
                              .index = 0};
        if (staging_buffer.data == nullptr || staging_buffer.size == 0) {
          if (write_buffer_id != 0) {
            store.buffers.ReleaseWriteBuffer(write_buffer_id);
          } else {
            store.buffers.ReleaseHeapWriteBuffer(heap_buffer);
          }
          co_return Status(StatusCode::kInternal,
                           "active write staging allocation is invalid");
        }
        const ReservedBlock allocated = *store.standby_block;
        store.standby_block.reset();
        const std::uint64_t block_id = allocated.block_id;
        std::fill_n(staging_buffer.data, staging_buffer.size, std::byte{0});
        active = ActiveBlock{
            .block_id = block_id,
            .writer_id = writer_id,
            .layout_worker_count = worker_count_,
            .allocation_epoch = allocated.allocation_epoch,
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
            .block_id = block_id,
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
    }

    // The writer mutex may have been released while waiting for a standby.
    // FLUSHDB or partition reset can replace the index state during that gap,
    // so capture the previous location only after the append stream is locked
    // again and an active block is available.
    auto* previous_entry = index.Find(digest, key);
    const std::optional<RecordLocation> previous =
        previous_entry == nullptr
            ? std::nullopt
            : std::optional<RecordLocation>(previous_entry->value);
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
        .value_type = value_type,
        .external = external,
        .digest = digest,
        .key_bytes = static_cast<std::uint32_t>(key.size()),
        .logical_size = logical_size,
        .payload_bytes = static_cast<std::uint32_t>(payload_bytes),
        .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
        .generation = generation,
        .replication_epoch = partition.replication_epoch,
        .db_epoch = DbEpoch(db_id),
        .mutation_sequence = mutation_sequence,
        .relocation_sequence = relocation_sequence,
        .expire_at_ms = expire_at_ms,
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
        .block_id = updated.block_id,
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
        .expire_at_ms = expire_at_ms,
        .block_owner = writer_id,
        .record_offset = record_offset,
        .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
        .logical_size = logical_size,
        .payload_bytes = static_cast<std::uint32_t>(payload_bytes),
        .relocation_sequence = static_cast<std::uint32_t>(
            relocation_sequence),
        .in_memory = true,
        .external = external,
        .kind = kind,
        .value_type = value_type,
        .extents = std::move(extents),
    };
    const bool was_live =
        previous.has_value() && previous->kind == RecordKind::kValue;
    const bool is_live = kind == RecordKind::kValue;
    const bool was_expiring =
        was_live && previous->expire_at_ms != 0;
    const bool is_expiring = is_live && expire_at_ms != 0;
    auto inserted = index.InsertOrAssign(digest, key, location);
    store.staged_records[updated.block_id].push_back(RecordIdentity{
        .entry = inserted.entry,
        .retired_extents =
            !for_defrag && previous.has_value() && previous->external
                ? previous->extents
                : nullptr,
        .index_generation = store.index_generations[db_id],
        .db_id = db_id,
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
    if (was_expiring != is_expiring) {
      if (is_expiring) {
        ++partition.expiring_key_count[db_id];
      } else {
        --partition.expiring_key_count[db_id];
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
    if (!for_defrag && previous.has_value() && previous->external) {
      RequestFlush(store, updated.block_id);
      if (store.active_block.has_value() &&
          store.active_block->block_id == updated.block_id) {
        store.active_block.reset();
      }
    }
    MaybePrefetchStandby(store);
    co_return Status::Ok();
  }

  void SealActiveBlocks(WorkerStore& store) {
    if (store.active_block.has_value() &&
        store.active_block->committed_bytes > kBlockHeaderBytes) {
      RequestFlush(store, store.active_block->block_id);
      store.active_block.reset();
    }
  }

  void SealDeadActiveBlock(WorkerStore& store) {
    if (!store.active_block.has_value()) {
      return;
    }
    BlockState* state = FindBlockState(store, store.active_block->block_id);
    if (state == nullptr || state->live_bytes != 0) {
      return;
    }
    RequestFlush(store, store.active_block->block_id);
    store.active_block.reset();
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

  void AdvanceExpiryMap(WorkerStore& store) {
    store.expiry_scan_cursor = 0;
    ++store.expiry_db_cursor;
    if (store.expiry_db_cursor == kLogicalDatabaseCount) {
      store.expiry_db_cursor = 0;
      ++store.expiry_partition_cursor;
      if (store.expiry_partition_cursor == store.partitions.size()) {
        store.expiry_partition_cursor = 0;
      }
    }
  }

  Task<Status> ExpireCandidate(WorkerStore& store,
                               WorkerStore::ExpireCandidate candidate) {
    if (candidate.partition_id >= kLogicalStorageShards ||
        candidate.db_id >= kLogicalDatabaseCount) {
      co_return Status::Ok();
    }
    auto& partition = PartitionFor(store, candidate.partition_id);
    auto key_lock = co_await store.key_locks[candidate.db_id].Acquire(
        candidate.digest, IntentLockMode::kExclusive);
    co_await store.writer_mutex.Lock();
    UnlockGuard unlock(&store.writer_mutex, store.worker);
    auto* current = partition.indexes[candidate.db_id].Find(
        candidate.digest, candidate.key);
    if (current == nullptr || current->value.kind != RecordKind::kValue ||
        current->value.mutation_sequence != candidate.mutation_sequence ||
        current->value.expire_at_ms != candidate.expire_at_ms ||
        !IsExpired(current->value, UnixTimeMillis())) {
      co_return Status::Ok();
    }
    co_return co_await AppendLocked(
        store, partition, candidate.db_id, candidate.key, {},
        RecordKind::kTombstone, ValueType::kNone, 0);
  }

  Task<Status> ActiveExpiration(WorkerStore* store) {
    constexpr auto kInterval = std::chrono::milliseconds(10);
    constexpr std::size_t kMapStepsPerCycle = 256;
    constexpr std::size_t kDeletesPerCycle = 64;
    while (!store->worker->stop_requested()) {
      Status waited = co_await celer::SleepFor(*store->worker, kInterval);
      if (!waited.ok()) {
        co_return waited;
      }
      if (store->worker->stop_requested() ||
          shutdown_flush_requested_.load(std::memory_order_acquire)) {
        break;
      }

      const std::uint64_t now_ms = UnixTimeMillis();
      for (std::size_t step = 0;
           step < kMapStepsPerCycle && !store->partitions.empty(); ++step) {
        auto& partition =
            store->partitions[store->expiry_partition_cursor];
        const std::uint8_t db_id = store->expiry_db_cursor;
        if (partition.expiring_key_count[db_id] == 0) {
          AdvanceExpiryMap(*store);
        } else {
          auto& index = partition.indexes[db_id];
          store->expiry_scan_cursor = index.Scan(
              store->expiry_scan_cursor,
              [&](const RecordIndex::Entry& entry) {
                if (IsExpired(entry.value, now_ms)) {
                  QueueExpiredCandidate(*store, partition.id, db_id, entry);
                }
              });
          if (store->expiry_scan_cursor == 0) {
            AdvanceExpiryMap(*store);
          }
        }
        co_await celer::Yield(*store->worker);
      }

      std::size_t deleted = 0;
      while (deleted < kDeletesPerCycle &&
             !store->expired_candidates.empty()) {
        WorkerStore::ExpireCandidate candidate =
            std::move(store->expired_candidates.front());
        store->expired_candidates.pop_front();
        Status expired = co_await ExpireCandidate(*store,
                                                  std::move(candidate));
        if (!expired.ok()) {
          co_return expired;
        }
        ++deleted;
        co_await celer::Yield(*store->worker);
      }
    }
    co_return Status::Ok();
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

  Task<Status> ReclaimExtents(
      WorkerStore* store,
      std::shared_ptr<const std::vector<ExtentRef>> extents) {
    if (extents == nullptr) {
      co_return Status::Ok();
    }
    std::vector<std::uint64_t> released;
    released.reserve(extents->size());
    for (const ExtentRef& ref : *extents) {
      while (true) {
        co_await store->writer_mutex.Lock();
        BlockState* state = FindBlockState(*store, ref.block_id);
        if (state == nullptr || !state->allocated ||
            state->allocation_epoch != ref.allocation_epoch) {
          store->writer_mutex.Unlock(*store->worker);
          break;
        }
        if (state->kind != BlockKind::kValueExtent) {
          store->writer_mutex.Unlock(*store->worker);
          co_return Status(StatusCode::kInternal,
                           "extent reclaim found a record block");
        }
        state->live_bytes = 0;
        if (state->pins != 0 || state->freeing) {
          store->writer_mutex.Unlock(*store->worker);
          Status waited = co_await celer::SleepFor(
              *store->worker, std::chrono::milliseconds(1));
          if (!waited.ok()) {
            co_return waited;
          }
          continue;
        }
        state->freeing = true;
        store->writer_mutex.Unlock(*store->worker);

        co_await store->writer_mutex.Lock();
        BlockState* current = FindBlockState(*store, ref.block_id);
        if (current != nullptr && current->allocation_epoch ==
                                      ref.allocation_epoch) {
          store->block_states.erase(ref.block_id);
          released.push_back(ref.block_id);
        }
        store->writer_mutex.Unlock(*store->worker);
        break;
      }
    }
    Status returned = co_await ReturnColdBlocks(std::move(released));
    if (!returned.ok()) {
      co_return returned;
    }
    space_reclaim_generation_.fetch_add(1, std::memory_order_release);
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
    active_flushes_.fetch_add(1, std::memory_order_acq_rel);
    store.worker->Spawn(FlushPendingBlocks(&store));
  }

  Task<Status> FlushPendingBlocks(WorkerStore* store) {
    struct FlushRunGuard {
      Impl* engine = nullptr;
      ~FlushRunGuard() {
        engine->space_reclaim_generation_.fetch_add(
            1, std::memory_order_release);
        engine->active_flushes_.fetch_sub(1, std::memory_order_acq_rel);
      }
    } flush_run_guard{this};

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
            .staged_records = {},
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
        if (identity.retired_extents != nullptr) {
          store->worker->Spawn(
              ReclaimExtents(store, identity.retired_extents));
        }
        if (identity.entry == nullptr) {
          continue;
        }
        // FLUSHDB detached the population this entry belongs to. The entry is
        // either already freed or waiting to be, and nothing reaches it either
        // way, so it must not be dereferenced.
        if (identity.index_generation !=
            store->index_generations[identity.db_id]) {
          continue;
        }
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
        state->kind != BlockKind::kRecords ||
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

  bool TryAcquireDefragPermit(std::size_t device_index) {
    std::atomic<unsigned>& device_active =
        active_defrags_by_device_[device_index];
    unsigned active = device_active.load(std::memory_order_acquire);
    while (active < kDefragPermitsPerDevice) {
      if (device_active.compare_exchange_weak(
              active, active + 1, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        active_defrags_.fetch_add(1, std::memory_order_acq_rel);
        return true;
      }
    }
    return false;
  }

  void ReleaseDefragPermit(std::size_t device_index) {
    active_defrags_by_device_[device_index].fetch_sub(
        1, std::memory_order_acq_rel);
    active_defrags_.fetch_sub(1, std::memory_order_acq_rel);
  }

  Task<Status> StartQueuedDefrag(unsigned worker_id,
                                 std::size_t device_index) {
    WorkerStore& store = *stores_[worker_id];
    if (!store.defrag_waiting ||
        store.defrag_waiting_device != device_index) {
      ReleaseDefragPermit(device_index);
      co_return Status::Ok();
    }
    store.defrag_waiting = false;
    if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
        store.defrag_running || store.defrag_queue.empty() ||
        DeviceIndexForBlock(store.defrag_queue.front()) != device_index) {
      ReleaseDefragPermit(device_index);
      RequestDefrag(store);
      co_return Status::Ok();
    }
    store.defrag_running = true;
    store.active_defrag_device = device_index;
    store.worker->SpawnBackground(DefragOne(&store));
    co_return Status::Ok();
  }

  Task<Status> WakeQueuedDefrags(std::size_t device_index) {
    while (TryAcquireDefragPermit(device_index)) {
      std::uint16_t worker_id = 0;
      if (!defrag_ready_by_device_[device_index].try_dequeue(worker_id)) {
        ReleaseDefragPermit(device_index);
        co_return Status::Ok();
      }
      pending_defrags_.fetch_sub(1, std::memory_order_acq_rel);
      Status started = worker_id == celer::ThisWorker().id
                           ? co_await StartQueuedDefrag(worker_id,
                                                        device_index)
                           : co_await celer::SubmitTaskTo(
                                 worker_id,
                                 [this, worker_id,
                                  device_index]() -> Task<Status> {
                                   co_return co_await StartQueuedDefrag(
                                       worker_id, device_index);
                                 });
      if (!started.ok()) {
        ReleaseDefragPermit(device_index);
        co_return started;
      }
    }
    co_return Status::Ok();
  }

  void RequestDefrag(WorkerStore& store) {
    if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
        store.defrag_running || store.defrag_waiting ||
        store.defrag_queue.empty()) {
      return;
    }
    const std::size_t device_index =
        DeviceIndexForBlock(store.defrag_queue.front());
    store.defrag_waiting = true;
    store.defrag_waiting_device = device_index;
    pending_defrags_.fetch_add(1, std::memory_order_acq_rel);
    if (!defrag_ready_by_device_[device_index].enqueue(store.worker->id())) {
      pending_defrags_.fetch_sub(1, std::memory_order_acq_rel);
      store.defrag_waiting = false;
      spdlog::error(
          "worker[{}] failed to enqueue defrag request for device {}",
          store.worker->id(), devices_[device_index].id);
      return;
    }
    store.worker->SpawnBackground(WakeQueuedDefrags(device_index));
  }

  void FinishDefragPass(WorkerStore& store) {
    const std::size_t completed_device = store.active_defrag_device;
    store.defrag_running = false;
    // Queue this worker's next candidate before releasing the active permit,
    // so foreground allocation never observes a false no-reclaim gap.
    RequestDefrag(store);
    space_reclaim_generation_.fetch_add(1, std::memory_order_release);
    ReleaseDefragPermit(completed_device);
    store.worker->SpawnBackground(WakeQueuedDefrags(completed_device));
  }

  Task<Status> DefragOne(WorkerStore* store) {
    if (store->defrag_queue.empty()) {
      FinishDefragPass(*store);
      co_return Status::Ok();
    }
    const std::uint64_t candidate = store->defrag_queue.front();
    store->defrag_queue.pop_front();
    BlockState* candidate_state = FindBlockState(*store, candidate);
    if (candidate_state == nullptr) {
      FinishDefragPass(*store);
      co_return Status::Ok();
    }
    candidate_state->defrag_queued = false;

    Status status = co_await CleanBlockLocked(*store, candidate);
    if (!status.ok()) {
      spdlog::error("worker[{}] defrag block {} failed: {}",
                    store->worker->id(), candidate, status.message());
    }
    if (status.code() == StatusCode::kResourceExhausted) {
      MaybeQueueDefrag(*store, candidate);
    }
    FinishDefragPass(*store);
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
        key_store, record.db_id, key, value, record.kind, record.value_type,
        record.expire_at_ms, record.digest, record.generation,
        record.mutation_sequence,
        record.relocation_sequence + 1, true, true, record.external,
        record.logical_size, source_location.extents);
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
    const auto [source_file_id, source_block_offset] = FileOffset(block_id);

    // live_bytes counts exactly the index entries naming this block, so zero
    // means nothing here is reachable and the salvage pass is a provable no-op:
    // every record it decoded would find its key either absent from the index
    // or pointing at a different physical record, so RelocateIfCurrent would
    // rewrite nothing and the live_bytes re-check below would still see zero.
    // Skipping reaches the same free path under the same precondition, without
    // reading 8 MiB and CRC-checking every record in it. FLUSHDB empties whole
    // blocks at once, which is where this dominates.
    if (source.live_bytes != 0) {
      Status salvaged = co_await SalvageBlockRecords(
          store, block_id, source, source_file_id, source_block_offset);
      if (!salvaged.ok()) {
        co_return salvaged;
      }
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
    co_return co_await ReleaseEmptyBlock(store, block_id, source,
                                         source_file_id, source_block_offset);
  }

  // Rewrites every record in the block that is still current, so the block ends
  // up with no reachable data and the caller can free it. Clears `defragging`
  // on each failure path so the block stays eligible for a later pass.
  Task<Status> SalvageBlockRecords(WorkerStore& store, std::uint64_t block_id,
                                   BlockState& source,
                                   std::uint32_t source_file_id,
                                   std::uint64_t source_block_offset) {
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

      RecordLocation source_location{
          .block_id = block_id,
          .replication_epoch = record.replication_epoch,
          .mutation_sequence = record.mutation_sequence,
          .allocation_epoch = record.allocation_epoch,
          .expire_at_ms = record.expire_at_ms,
          .block_owner = store.worker->id(),
          .record_offset = record_offset,
          .total_disk_bytes = record.total_disk_bytes,
          .logical_size = record.logical_size,
          .payload_bytes = record.payload_bytes,
          .relocation_sequence = static_cast<std::uint32_t>(
              record.relocation_sequence),
          .external = record.external,
          .kind = record.kind,
          .value_type = record.value_type,
          .extents = {},
      };
      const std::byte* value_data =
          block_data.buffer.data + record_offset + record.header_bytes;
      if (Crc32c(std::span<const std::byte>(value_data,
                                           record.payload_bytes)) !=
          record.payload_checksum) {
        source.defragging = false;
        co_return Status(StatusCode::kInternal,
                         "value checksum mismatch during defrag");
      }
      if (record.external) {
        auto decoded = DecodeManifest(
            std::span<const std::byte>(value_data, record.payload_bytes),
            record.logical_size);
        if (!decoded.ok()) {
          source.defragging = false;
          co_return decoded.status();
        }
        source_location.extents = std::move(*decoded);
      }

      const unsigned key_owner = OwnerForKey(disk_key);
      const std::string key(disk_key);
      const std::string value(reinterpret_cast<const char*>(value_data),
                              record.payload_bytes);
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
      // Cheap while the 50 us background slice has budget remaining; once it
      // expires this defers the cleaner to the next scheduler round so online
      // I/O and cross-core work are polled first.
      co_await celer::Yield(*store.worker);
    }
    co_return Status::Ok();
  }

  // Drains readers and hands the block back to the allocator. The caller must
  // have observed live_bytes == 0 under writer_mutex and set `freeing`, which
  // stops LoadValueLocal from taking new pins.
  Task<Status> ReleaseEmptyBlock(WorkerStore& store, std::uint64_t block_id,
                                 BlockState& source,
                                 std::uint32_t source_file_id,
                                 std::uint64_t source_block_offset) {
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
    co_return co_await ReturnReadyBlock(block_id);
  }

  StorageEngineOptions options_;
  unsigned worker_count_ = 0;
  std::uint64_t total_data_blocks_ = 0;
  std::vector<StorageDevice> devices_;
  std::vector<std::unique_ptr<DeviceAllocator>> device_allocators_;
  std::vector<std::size_t> defrag_reserve_blocks_;
  std::unique_ptr<std::atomic<unsigned>[]> active_defrags_by_device_;
  std::unique_ptr<moodycamel::ConcurrentQueue<std::uint16_t>[]>
      defrag_ready_by_device_;
  std::unique_ptr<RecoveryDeviceCursor[]> recovery_device_cursors_;
  std::vector<std::uint64_t> epoch_values_;
  std::atomic<bool> epoch_metadata_failed_{false};
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
  static constexpr std::size_t kDefragReserveBlocksPerDevice = 8;
  static constexpr unsigned kDefragPermitsPerDevice = 8;
  std::atomic<std::uint64_t> next_lsn_{1};
  std::array<std::atomic<std::uint64_t>, kLogicalDatabaseCount> db_epochs_{};
  std::atomic<unsigned> active_defrags_{0};
  std::atomic<unsigned> pending_defrags_{0};
  std::atomic<unsigned> active_flushes_{0};
  std::atomic<std::uint64_t> space_reclaim_generation_{0};
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

Task<Status> StorageEngine::FlushDbDetach(std::uint8_t db_id) {
  co_return co_await impl_->FlushDbDetach(db_id);
}

Task<Status> StorageEngine::FlushDbReclaim(bool wait) {
  co_return co_await impl_->FlushDbReclaim(wait);
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

Task<StatusOr<std::uint64_t>> StorageEngine::StringLength(
    std::uint8_t db_id, std::string_view key) {
  co_return co_await impl_->StringLength(db_id, key);
}

Task<StatusOr<SetResult>> StorageEngine::Set(std::uint8_t db_id,
                                             std::string_view key,
                                             std::string_view value,
                                             SetOptions options) {
  co_return co_await impl_->Set(db_id, key, value, options);
}

Task<ExpirationInfo> StorageEngine::GetExpiration(std::uint8_t db_id,
                                                   std::string_view key) {
  co_return co_await impl_->GetExpiration(db_id, key);
}

Task<StatusOr<bool>> StorageEngine::UpdateExpiration(
    std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
    ExpirationCondition condition) {
  co_return co_await impl_->UpdateExpiration(db_id, key, expire_at_ms,
                                             condition);
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
