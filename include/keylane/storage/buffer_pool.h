#pragma once

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/io/storage.h"
#include "celer/runtime/sync.h"

namespace celer {
class CrossCore;
class Worker;
}  // namespace celer

namespace keylane::storage {

inline constexpr std::size_t kMiB = 1024 * 1024;
inline constexpr std::size_t kKiB = 1024;

struct RegisteredBufferPoolOptions {
  std::size_t registered_bytes_ = 64 * kMiB;
  std::size_t storage_write_buffer_count_ = 4;
  std::size_t write_buffer_bytes_ = 8 * kMiB;
  std::size_t read_payload_bytes_ = 1 * kMiB;
  std::size_t read_headroom_bytes_ = 4 * kKiB;
  std::size_t read_tailroom_bytes_ = 4 * kKiB;
  std::size_t alignment_ = 4 * kKiB;
};

class RegisteredBufferPool;

// A read slot remains owned by its storage worker. Moving this lease to a
// connection worker only loans the memory; Reset()/destruction returns the slot
// to the owner through a one-way cross-core notification.
class ReadBufferLease {
 public:
  ReadBufferLease() = default;
  ReadBufferLease(const ReadBufferLease&) = delete;
  ReadBufferLease& operator=(const ReadBufferLease&) = delete;
  ReadBufferLease(ReadBufferLease&& other) noexcept;
  ReadBufferLease& operator=(ReadBufferLease&& other) noexcept;
  ~ReadBufferLease();

  bool valid() const noexcept { return data_ != nullptr; }
  // True only when the slot is backed by a ring-registered iovec, so it gates
  // fixed-buffer IO. A pool that fell back to unregistered mode at Init keeps
  // its slot ids but reports false here. Defined after RegisteredBufferPool.
  bool registered() const noexcept;
  unsigned owner_worker() const noexcept { return owner_worker_; }
  std::uint16_t buffer_id() const noexcept { return buffer_id_; }

  // Entire registered iovec, including framing/alignment headroom and tailroom.
  celer::FixedBuffer registered_buffer() const noexcept {
    return {.data_ = data_, .size_ = size_, .index_ = buffer_id_};
  }

  // Aligned region intended as the destination of READ_FIXED.
  celer::FixedBuffer io_buffer() const noexcept;

  std::span<std::byte> bytes() const noexcept {
    return {data_, size_};
  }
  std::size_t headroom_bytes() const noexcept { return headroom_bytes_; }
  std::size_t tailroom_bytes() const noexcept { return tailroom_bytes_; }

  void Reset() noexcept;

 private:
  friend class RegisteredBufferPool;
  ReadBufferLease(RegisteredBufferPool* pool, unsigned owner_worker,
                  celer::FixedBuffer buffer, std::size_t headroom_bytes,
                  std::size_t tailroom_bytes) noexcept;
  ReadBufferLease(RegisteredBufferPool* pool, unsigned owner_worker,
                  celer::FixedBuffer buffer, std::size_t headroom_bytes,
                  std::size_t tailroom_bytes, std::size_t overflow_id) noexcept;

  // Every lease is pool-owned. release_id_ is zero for a fixed slot and the
  // one-based overflow-buffer id otherwise; those states are mutually
  // exclusive, so retaining the former standalone-heap fields only enlarged
  // every direct GET result moved through the command coroutines. Buffer
  // dimensions are bounded to uint32_t by Init and the 1 GiB limit on the
  // single string record carried by a direct GET. Collections are stored as
  // multiple records and do not use this reply lease for their total size.
  RegisteredBufferPool* pool_ = nullptr;
  std::byte* data_ = nullptr;
  std::uint32_t size_ = 0;
  std::uint32_t headroom_bytes_ = 0;
  std::uint32_t tailroom_bytes_ = 0;
  std::uint32_t release_id_ = 0;
  std::uint32_t owner_worker_ = 0;
  std::uint16_t buffer_id_ = 0;
};

static_assert(sizeof(ReadBufferLease) == 40);

class RegisteredBufferPool {
 public:
  RegisteredBufferPool() = default;
  RegisteredBufferPool(const RegisteredBufferPool&) = delete;
  RegisteredBufferPool& operator=(const RegisteredBufferPool&) = delete;
  ~RegisteredBufferPool();

  absl::Status Init(celer::Worker& worker,
                    const RegisteredBufferPoolOptions& options = {});

  bool initialized() const noexcept { return worker_ != nullptr; }
  // False when io_uring buffer registration failed at Init and the pool fell
  // back to plain (non-fixed) IO on the same aligned memory. Immutable after
  // Init, so cross-worker reads through moved leases need no synchronization.
  bool buffers_registered() const noexcept { return buffers_registered_; }
  unsigned owner_worker() const noexcept { return owner_worker_; }
  std::size_t read_buffer_count() const noexcept {
    return read_buffers_.size();
  }
  std::size_t available_read_buffers() const noexcept {
    return free_read_buffers_.size();
  }
  std::size_t overflow_read_buffer_count() const noexcept {
    return overflow_read_buffers_.size();
  }
  std::size_t write_buffer_count() const noexcept {
    return options_.storage_write_buffer_count_;
  }
  std::size_t available_write_buffers() const noexcept {
    return free_write_buffers_.size();
  }
  celer::FixedBuffer write_buffer(std::uint16_t buffer_id = 0) const noexcept {
    if (buffer_id == 0) {
      return {};
    }
    const auto id = static_cast<std::size_t>(buffer_id);
    return id <= write_buffers_.size() && id != 0 ? write_buffers_[id - 1]
                                                  : celer::FixedBuffer{};
  }

  bool TryAcquireWriteBuffer(std::uint16_t* buffer_id) noexcept;
  // Foreground storage writers wait only for the statically reserved storage
  // pool. A backlog-buffer release cannot wake or satisfy this waiter.
  celer::AsyncNotification::Awaiter WaitForWriteBuffer() noexcept {
    return storage_write_buffer_ready_.Wait();
  }
  void ReleaseWriteBuffer(std::uint16_t buffer_id) noexcept;
  bool TryAcquireHeapWriteBuffer(std::byte** buffer) noexcept;
  void ReleaseHeapWriteBuffer(std::byte* buffer) noexcept;

  class AcquireReadAwaiter {
   public:
    AcquireReadAwaiter(RegisteredBufferPool* pool,
                       std::size_t minimum_payload_bytes)
        : pool_(pool), minimum_payload_bytes_(minimum_payload_bytes) {}

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> awaiting);
    absl::StatusOr<ReadBufferLease> await_resume();

   private:
    friend class RegisteredBufferPool;

    RegisteredBufferPool* pool_ = nullptr;
    std::size_t minimum_payload_bytes_ = 0;
    std::coroutine_handle<> awaiting_{};
    std::uint16_t assigned_buffer_id_ = 0;
  };

  // Requests larger than a registered read slot use an aligned heap lease.
  AcquireReadAwaiter AcquireReadBuffer(
      std::size_t minimum_payload_bytes = 0) noexcept {
    return AcquireReadAwaiter(this, minimum_payload_bytes);
  }

 private:
  friend class ReadBufferLease;

  ReadBufferLease TakeReadBuffer();
  ReadBufferLease LeaseReadBuffer(std::uint16_t buffer_id);
  absl::StatusOr<ReadBufferLease> AllocateHeapReadBuffer(
      std::size_t minimum_payload_bytes);
  void ReleaseOverflow(std::size_t overflow_id) noexcept;
  void ReleaseOverflowLocal(std::size_t overflow_id) noexcept;
  std::optional<std::uint16_t> TakeWriteBuffer();
  void ReleaseWriteBufferLocal(std::uint16_t buffer_id) noexcept;
  void Release(std::uint16_t buffer_id) noexcept;
  void ReleaseLocal(std::uint16_t buffer_id) noexcept;
  static void HandleRemoteRelease(void* context, std::uint64_t value) noexcept;
  static void HandleRemoteOverflowRelease(void* context,
                                          std::uint64_t value) noexcept;
  bool IsReadBufferId(std::uint16_t buffer_id) const noexcept;
  bool IsWriteBufferId(std::uint16_t buffer_id) const noexcept;

  celer::Worker* worker_ = nullptr;
  celer::CrossCore* cross_core_ = nullptr;
  unsigned owner_worker_ = 0;
  bool buffers_registered_ = false;
  RegisteredBufferPoolOptions options_{};
  std::byte* sentinel_buffer_ = nullptr;
  std::size_t sentinel_buffer_bytes_ = 0;
  std::vector<celer::FixedBuffer> write_buffers_;
  std::vector<std::uint16_t> free_write_buffers_;
  std::vector<bool> write_buffer_in_use_;
  celer::AsyncNotification storage_write_buffer_ready_;
  std::vector<std::byte*> heap_write_buffers_;
  std::vector<std::byte*> free_heap_write_buffers_;
  std::vector<celer::FixedBuffer> read_buffers_;
  std::vector<std::uint16_t> free_read_buffers_;
  std::vector<bool> read_buffer_in_use_;
  std::deque<AcquireReadAwaiter*> read_waiters_;
  // SPDK and io_uring overflow reads grow this cache to the observed
  // concurrency high-water mark. Released DMA/aligned buffers are reused,
  // avoiding allocation and huge-page faults on every pool miss.
  std::vector<celer::FixedBuffer> overflow_read_buffers_;
  std::vector<std::size_t> free_overflow_read_buffers_;
  std::vector<bool> overflow_read_buffer_in_use_;
};

inline bool ReadBufferLease::registered() const noexcept {
  return buffer_id_ != 0 && pool_ != nullptr && pool_->buffers_registered_;
}

}  // namespace keylane::storage
