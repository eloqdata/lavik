#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "celer/base/status.h"
#include "celer/io/storage.h"

namespace celer {
class CrossCore;
class Worker;
}  // namespace celer

namespace keylane::storage {

inline constexpr std::size_t kMiB = 1024 * 1024;
inline constexpr std::size_t kKiB = 1024;

struct RegisteredBufferPoolOptions {
  std::size_t registered_bytes = 16 * kMiB;
  std::size_t write_buffer_bytes = 8 * kMiB;
  std::size_t read_payload_bytes = 1 * kMiB;
  std::size_t read_headroom_bytes = 4 * kKiB;
  std::size_t read_tailroom_bytes = 4 * kKiB;
  std::size_t alignment = 4 * kKiB;
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

  bool valid() const noexcept { return buffer_.data != nullptr; }
  bool registered() const noexcept { return pool_ != nullptr; }
  unsigned owner_worker() const noexcept { return owner_worker_; }
  std::uint16_t buffer_id() const noexcept { return buffer_.index; }

  // Entire registered iovec, including framing/alignment headroom and tailroom.
  celer::FixedBuffer registered_buffer() const noexcept { return buffer_; }

  // Aligned region intended as the destination of READ_FIXED.
  celer::FixedBuffer io_buffer() const noexcept;

  std::span<std::byte> bytes() const noexcept {
    return {buffer_.data, buffer_.size};
  }
  std::size_t headroom_bytes() const noexcept { return headroom_bytes_; }
  std::size_t tailroom_bytes() const noexcept { return tailroom_bytes_; }

  void Reset() noexcept;

 private:
  friend class RegisteredBufferPool;
  ReadBufferLease(RegisteredBufferPool* pool, unsigned owner_worker,
                  celer::FixedBuffer buffer, std::size_t headroom_bytes,
                  std::size_t tailroom_bytes) noexcept;
  ReadBufferLease(unsigned owner_worker, celer::FixedBuffer buffer,
                  std::size_t headroom_bytes, std::size_t tailroom_bytes,
                  std::size_t heap_alignment) noexcept;

  RegisteredBufferPool* pool_ = nullptr;
  unsigned owner_worker_ = 0;
  celer::FixedBuffer buffer_{};
  std::size_t headroom_bytes_ = 0;
  std::size_t tailroom_bytes_ = 0;
  std::size_t heap_alignment_ = 0;
};

class RegisteredBufferPool {
 public:
  RegisteredBufferPool() = default;
  RegisteredBufferPool(const RegisteredBufferPool&) = delete;
  RegisteredBufferPool& operator=(const RegisteredBufferPool&) = delete;
  ~RegisteredBufferPool();

  celer::Status Init(celer::Worker& worker,
                     const RegisteredBufferPoolOptions& options = {});

  bool initialized() const noexcept { return arena_ != nullptr; }
  unsigned owner_worker() const noexcept { return owner_worker_; }
  std::size_t read_buffer_count() const noexcept {
    return read_buffers_.size();
  }
  std::size_t available_read_buffers() const noexcept {
    return free_read_buffers_.size();
  }
  celer::FixedBuffer write_buffer() const noexcept { return write_buffer_; }

  class AcquireReadAwaiter {
   public:
    explicit AcquireReadAwaiter(RegisteredBufferPool* pool) : pool_(pool) {}

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> awaiting);
    celer::StatusOr<ReadBufferLease> await_resume();

   private:
    RegisteredBufferPool* pool_ = nullptr;
  };

  AcquireReadAwaiter AcquireReadBuffer() noexcept {
    return AcquireReadAwaiter(this);
  }

 private:
  friend class ReadBufferLease;

  ReadBufferLease TakeReadBuffer();
  celer::StatusOr<ReadBufferLease> AllocateHeapReadBuffer();
  void Release(std::uint16_t buffer_id) noexcept;
  void ReleaseLocal(std::uint16_t buffer_id) noexcept;
  static void HandleRemoteRelease(void* context, std::uint64_t value) noexcept;

  celer::Worker* worker_ = nullptr;
  celer::CrossCore* cross_core_ = nullptr;
  unsigned owner_worker_ = 0;
  RegisteredBufferPoolOptions options_{};
  std::byte* arena_ = nullptr;
  std::size_t arena_bytes_ = 0;
  celer::FixedBuffer write_buffer_{};
  std::vector<celer::FixedBuffer> read_buffers_;
  std::vector<std::uint16_t> free_read_buffers_;
  std::vector<bool> read_buffer_in_use_;
};

}  // namespace keylane::storage
