#include "keylane/storage/buffer_pool.h"

#include <sys/uio.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <new>
#include <optional>
#include <vector>

#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"

namespace keylane::storage {
namespace {

bool IsPowerOfTwo(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

bool IsAligned(std::size_t value, std::size_t alignment) noexcept {
  return (value & (alignment - 1)) == 0;
}

}  // namespace

ReadBufferLease::ReadBufferLease(RegisteredBufferPool* pool,
                                 unsigned owner_worker,
                                 celer::FixedBuffer buffer,
                                 std::size_t headroom_bytes,
                                 std::size_t tailroom_bytes) noexcept
    : pool_(pool),
      owner_worker_(owner_worker),
      buffer_(buffer),
      headroom_bytes_(headroom_bytes),
      tailroom_bytes_(tailroom_bytes) {}

ReadBufferLease::ReadBufferLease(unsigned owner_worker,
                                 celer::FixedBuffer buffer,
                                 std::size_t headroom_bytes,
                                 std::size_t tailroom_bytes,
                                 std::size_t heap_alignment) noexcept
    : owner_worker_(owner_worker),
      buffer_(buffer),
      headroom_bytes_(headroom_bytes),
      tailroom_bytes_(tailroom_bytes),
      heap_alignment_(heap_alignment) {}

ReadBufferLease::ReadBufferLease(ReadBufferLease&& other) noexcept
    : pool_(std::exchange(other.pool_, nullptr)),
      owner_worker_(other.owner_worker_),
      buffer_(other.buffer_),
      headroom_bytes_(other.headroom_bytes_),
      tailroom_bytes_(other.tailroom_bytes_),
      heap_alignment_(other.heap_alignment_) {
  other.buffer_ = {};
  other.heap_alignment_ = 0;
}

ReadBufferLease& ReadBufferLease::operator=(ReadBufferLease&& other) noexcept {
  if (this != &other) {
    Reset();
    pool_ = std::exchange(other.pool_, nullptr);
    owner_worker_ = other.owner_worker_;
    buffer_ = other.buffer_;
    headroom_bytes_ = other.headroom_bytes_;
    tailroom_bytes_ = other.tailroom_bytes_;
    heap_alignment_ = other.heap_alignment_;
    other.buffer_ = {};
    other.heap_alignment_ = 0;
  }
  return *this;
}

ReadBufferLease::~ReadBufferLease() { Reset(); }

celer::FixedBuffer ReadBufferLease::io_buffer() const noexcept {
  if (!valid() || buffer_.size < headroom_bytes_ + tailroom_bytes_) {
    return {};
  }
  return celer::FixedBuffer{
      .data = buffer_.data + headroom_bytes_,
      .size = buffer_.size - headroom_bytes_ - tailroom_bytes_,
      .index = buffer_.index,
  };
}

void ReadBufferLease::Reset() noexcept {
  if (pool_ != nullptr) {
    RegisteredBufferPool* pool = std::exchange(pool_, nullptr);
    pool->Release(buffer_.index);
  } else if (buffer_.data != nullptr && heap_alignment_ != 0) {
    ::operator delete[](buffer_.data, std::align_val_t(heap_alignment_));
  }
  buffer_ = {};
  heap_alignment_ = 0;
}

RegisteredBufferPool::~RegisteredBufferPool() {
  if (sentinel_buffer_ != nullptr) {
    ::operator delete[](sentinel_buffer_, std::align_val_t(options_.alignment));
    sentinel_buffer_ = nullptr;
    sentinel_buffer_bytes_ = 0;
  }
  for (const celer::FixedBuffer& buffer : write_buffers_) {
    if (buffer.data != nullptr &&
        IsAligned(reinterpret_cast<std::uintptr_t>(buffer.data),
                  options_.alignment)) {
      ::operator delete[](buffer.data, std::align_val_t(options_.alignment));
    }
  }
  for (const celer::FixedBuffer& buffer : read_buffers_) {
    if (buffer.data != nullptr &&
        IsAligned(reinterpret_cast<std::uintptr_t>(buffer.data),
                  options_.alignment)) {
      ::operator delete[](buffer.data, std::align_val_t(options_.alignment));
    }
  }
  for (std::byte* buffer : heap_write_buffers_) {
    if (buffer != nullptr && IsAligned(reinterpret_cast<std::uintptr_t>(buffer),
                                       options_.alignment)) {
      ::operator delete[](buffer, std::align_val_t(options_.alignment));
    }
  }
}

absl::Status RegisteredBufferPool::Init(
    celer::Worker& worker, const RegisteredBufferPoolOptions& options) {
  if (initialized()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "registered buffer pool is already initialized");
  }
  if (!IsPowerOfTwo(options.alignment) || options.alignment < 4096) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "registered buffer alignment must be a power of two >= 4096");
  }
  if (!IsAligned(options.write_buffer_bytes, options.alignment) ||
      !IsAligned(options.read_payload_bytes, options.alignment) ||
      !IsAligned(options.read_headroom_bytes, options.alignment) ||
      !IsAligned(options.read_tailroom_bytes, options.alignment)) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "registered buffer sizes must satisfy O_DIRECT alignment");
  }
  if (options.write_buffer_count == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "at least one write buffer is required");
  }
  if (options.write_buffer_count >
      static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max() - 1)) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "too many write buffers for fixed-buffer indices");
  }

  if (options.write_buffer_bytes == 0 || options.read_payload_bytes == 0 ||
      options.read_headroom_bytes == 0 || options.read_tailroom_bytes == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "registered buffer sizes must be positive");
  }
  if (options.write_buffer_bytes > 0 &&
      (std::numeric_limits<std::size_t>::max() / options.write_buffer_count) <
          options.write_buffer_bytes) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "write buffer budget overflow");
  }

  const std::size_t read_slot_bytes = options.read_headroom_bytes +
                                      options.read_payload_bytes +
                                      options.read_tailroom_bytes;
  const std::size_t registered_write_count =
      std::min(options.write_buffer_count,
               options.registered_bytes / options.write_buffer_bytes);
  const std::size_t registered_write_bytes =
      registered_write_count * options.write_buffer_bytes;
  const std::size_t read_count =
      (options.registered_bytes - registered_write_bytes) / read_slot_bytes;
  const std::size_t total_count = registered_write_count + read_count;
  const std::size_t registration_count = total_count + 1;
  if (read_count >= std::numeric_limits<std::uint16_t>::max() ||
      total_count >= std::numeric_limits<std::uint16_t>::max()) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "total fixed-buffer count exceeds index range");
  }

  const std::size_t read_base = registered_write_count + 1;
  std::vector<iovec> iovecs;
  iovecs.reserve(registration_count);

  const std::size_t sentinel_bytes = options.alignment;
  auto* sentinel = static_cast<std::byte*>(::operator new[](
      sentinel_bytes, std::align_val_t(options.alignment), std::nothrow));
  if (sentinel == nullptr) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "aligned sentinel buffer allocation failed");
  }
  std::fill_n(sentinel, sentinel_bytes, std::byte{0});
  iovecs.push_back(iovec{.iov_base = sentinel, .iov_len = sentinel_bytes});
  std::vector<celer::FixedBuffer> write_buffers;
  write_buffers.reserve(registered_write_count);
  for (std::size_t i = 0; i < registered_write_count; ++i) {
    auto* data = static_cast<std::byte*>(
        ::operator new[](options.write_buffer_bytes,
                         std::align_val_t(options.alignment), std::nothrow));
    if (data == nullptr) {
      ::operator delete[](sentinel, std::align_val_t(options.alignment));
      for (const celer::FixedBuffer& buffer : write_buffers) {
        ::operator delete[](buffer.data, std::align_val_t(options.alignment));
      }
      return absl::Status(absl::StatusCode::kResourceExhausted,
                          "aligned registered-write-buffer allocation failed");
    }
    const std::size_t id = i + 1;
    iovecs.push_back(
        iovec{.iov_base = data, .iov_len = options.write_buffer_bytes});
    write_buffers.push_back(celer::FixedBuffer{
        .data = data,
        .size = options.write_buffer_bytes,
        .index = static_cast<std::uint16_t>(id),
    });
  }

  std::vector<celer::FixedBuffer> read_buffers;
  read_buffers.reserve(read_count);
  for (std::size_t i = 0; i < read_count; ++i) {
    auto* data = static_cast<std::byte*>(::operator new[](
        read_slot_bytes, std::align_val_t(options.alignment), std::nothrow));
    if (data == nullptr) {
      ::operator delete[](sentinel, std::align_val_t(options.alignment));
      for (const celer::FixedBuffer& buffer : write_buffers) {
        ::operator delete[](buffer.data, std::align_val_t(options.alignment));
      }
      for (const celer::FixedBuffer& buffer : read_buffers) {
        ::operator delete[](buffer.data, std::align_val_t(options.alignment));
      }
      return absl::Status(absl::StatusCode::kResourceExhausted,
                          "aligned registered-read-buffer allocation failed");
    }
    const std::size_t id = read_base + i;
    iovecs.push_back(iovec{.iov_base = data, .iov_len = read_slot_bytes});
    read_buffers.push_back(celer::FixedBuffer{
        .data = data,
        .size = read_slot_bytes,
        .index = static_cast<std::uint16_t>(id),
    });
  }

  absl::Status status = worker.RegisterBuffers(iovecs);
  if (!status.ok()) {
    ::operator delete[](sentinel, std::align_val_t(options.alignment));
    for (const celer::FixedBuffer& buffer : write_buffers) {
      ::operator delete[](buffer.data, std::align_val_t(options.alignment));
    }
    for (const celer::FixedBuffer& buffer : read_buffers) {
      ::operator delete[](buffer.data, std::align_val_t(options.alignment));
    }
    return status;
  }

  worker_ = &worker;
  cross_core_ = celer::ThisWorker().cross_core;
  owner_worker_ = worker.id();
  options_ = options;
  sentinel_buffer_ = sentinel;
  sentinel_buffer_bytes_ = sentinel_bytes;
  write_buffers_ = std::move(write_buffers);
  read_buffers_ = std::move(read_buffers);
  write_buffer_in_use_.assign(write_buffers_.size(), false);
  read_buffer_in_use_.assign(read_buffers_.size(), false);
  free_write_buffers_.reserve(write_buffers_.size());
  for (std::size_t i = write_buffers_.size(); i > 0; --i) {
    free_write_buffers_.push_back(static_cast<std::uint16_t>(i));
  }
  free_read_buffers_.reserve(read_buffers_.size());
  for (std::size_t i = read_count; i > 0; --i) {
    free_read_buffers_.push_back(static_cast<std::uint16_t>(read_base + i - 1));
  }
  return absl::OkStatus();
}

bool RegisteredBufferPool::IsWriteBufferId(
    std::uint16_t buffer_id) const noexcept {
  return buffer_id != 0 &&
         buffer_id <= static_cast<std::uint16_t>(write_buffers_.size());
}

bool RegisteredBufferPool::IsReadBufferId(
    std::uint16_t buffer_id) const noexcept {
  if (buffer_id == 0 || read_buffers_.empty()) {
    return false;
  }
  const std::size_t index = static_cast<std::size_t>(buffer_id);
  const std::size_t read_base = write_buffers_.size() + 1;
  return index >= read_base && index < read_base + read_buffers_.size();
}

bool RegisteredBufferPool::AcquireReadAwaiter::await_ready() const noexcept {
  return true;
}

bool RegisteredBufferPool::AcquireReadAwaiter::await_suspend(
    std::coroutine_handle<> awaiting) {
  (void)awaiting;
  return false;
}

absl::StatusOr<ReadBufferLease>
RegisteredBufferPool::AcquireReadAwaiter::await_resume() {
  if (pool_ == nullptr || !pool_->initialized()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "registered buffer pool is not initialized");
  }
  if (minimum_payload_bytes_ <= pool_->options_.read_payload_bytes &&
      !pool_->free_read_buffers_.empty()) {
    return pool_->TakeReadBuffer();
  }
  return pool_->AllocateHeapReadBuffer(minimum_payload_bytes_);
}

ReadBufferLease RegisteredBufferPool::TakeReadBuffer() {
  const std::uint16_t buffer_id = free_read_buffers_.back();
  free_read_buffers_.pop_back();
  const std::size_t read_base = write_buffers_.size() + 1;
  const std::size_t offset = static_cast<std::size_t>(buffer_id - read_base);
  read_buffer_in_use_[offset] = true;
  return ReadBufferLease(this, owner_worker_, read_buffers_[offset],
                         options_.read_headroom_bytes,
                         options_.read_tailroom_bytes);
}

absl::StatusOr<ReadBufferLease> RegisteredBufferPool::AllocateHeapReadBuffer(
    std::size_t minimum_payload_bytes) {
  std::size_t payload_bytes =
      std::max(options_.read_payload_bytes, minimum_payload_bytes);
  if (payload_bytes >
      std::numeric_limits<std::size_t>::max() - (options_.alignment - 1)) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "heap read buffer size overflow");
  }
  payload_bytes =
      (payload_bytes + options_.alignment - 1) & ~(options_.alignment - 1);
  if (payload_bytes > std::numeric_limits<std::size_t>::max() -
                          options_.read_headroom_bytes ||
      payload_bytes + options_.read_headroom_bytes >
          std::numeric_limits<std::size_t>::max() -
              options_.read_tailroom_bytes) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "heap read buffer size overflow");
  }
  const std::size_t bytes = options_.read_headroom_bytes + payload_bytes +
                            options_.read_tailroom_bytes;
  auto* data = static_cast<std::byte*>(::operator new[](
      bytes, std::align_val_t(options_.alignment), std::nothrow));
  if (data == nullptr) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "aligned heap read buffer allocation failed");
  }
  return ReadBufferLease(
      owner_worker_,
      celer::FixedBuffer{.data = data, .size = bytes, .index = 0},
      options_.read_headroom_bytes, options_.read_tailroom_bytes,
      options_.alignment);
}

std::optional<std::uint16_t> RegisteredBufferPool::TakeWriteBuffer() {
  if (free_write_buffers_.empty()) {
    return std::nullopt;
  }
  const std::uint16_t buffer_id = free_write_buffers_.back();
  free_write_buffers_.pop_back();
  const std::size_t offset = static_cast<std::size_t>(buffer_id - 1);
  write_buffer_in_use_[offset] = true;
  return buffer_id;
}

bool RegisteredBufferPool::TryAcquireWriteBuffer(
    std::uint16_t* buffer_id) noexcept {
  if (buffer_id == nullptr) {
    return false;
  }
  auto taken = TakeWriteBuffer();
  if (!taken.has_value()) {
    return false;
  }
  *buffer_id = *taken;
  return true;
}

void RegisteredBufferPool::ReleaseWriteBuffer(
    std::uint16_t buffer_id) noexcept {
  ReleaseWriteBufferLocal(buffer_id);
}

bool RegisteredBufferPool::TryAcquireHeapWriteBuffer(
    std::byte** buffer) noexcept {
  if (buffer == nullptr) {
    return false;
  }
  if (!free_heap_write_buffers_.empty()) {
    *buffer = free_heap_write_buffers_.back();
    free_heap_write_buffers_.pop_back();
    return true;
  }
  auto* data = static_cast<std::byte*>(
      ::operator new[](options_.write_buffer_bytes,
                       std::align_val_t(options_.alignment), std::nothrow));
  if (data == nullptr) {
    return false;
  }
  heap_write_buffers_.push_back(data);
  *buffer = data;
  return true;
}

void RegisteredBufferPool::ReleaseHeapWriteBuffer(std::byte* buffer) noexcept {
  if (buffer == nullptr) {
    return;
  }
  free_heap_write_buffers_.push_back(buffer);
}

void RegisteredBufferPool::ReleaseWriteBufferLocal(
    std::uint16_t buffer_id) noexcept {
  if (!IsWriteBufferId(buffer_id)) {
    return;
  }
  const std::size_t offset = static_cast<std::size_t>(buffer_id - 1);
  if (!write_buffer_in_use_[offset]) {
    return;
  }
  write_buffer_in_use_[offset] = false;
  free_write_buffers_.push_back(buffer_id);
}

void RegisteredBufferPool::Release(std::uint16_t buffer_id) noexcept {
  const celer::CurrentWorker& current = celer::ThisWorker();
  if (current.cross_core == cross_core_ && current.id == owner_worker_) {
    ReleaseLocal(buffer_id);
    return;
  }
  if (current.cross_core == nullptr || current.cross_core != cross_core_) {
    return;
  }
  celer::PostNotification(
      cross_core_, owner_worker_,
      celer::RemoteNotification{
          .context = this,
          .value = buffer_id,
          .run_fn = &RegisteredBufferPool::HandleRemoteRelease,
      });
}

void RegisteredBufferPool::ReleaseLocal(std::uint16_t buffer_id) noexcept {
  if (IsWriteBufferId(buffer_id)) {
    ReleaseWriteBufferLocal(buffer_id);
    return;
  }
  if (!IsReadBufferId(buffer_id)) {
    return;
  }
  const std::size_t read_base = write_buffers_.size() + 1;
  const std::size_t offset = static_cast<std::size_t>(buffer_id - read_base);
  if (!read_buffer_in_use_[offset]) {
    return;
  }
  read_buffer_in_use_[offset] = false;
  free_read_buffers_.push_back(buffer_id);
}

void RegisteredBufferPool::HandleRemoteRelease(void* context,
                                               std::uint64_t value) noexcept {
  static_cast<RegisteredBufferPool*>(context)->ReleaseLocal(
      static_cast<std::uint16_t>(value));
}

}  // namespace keylane::storage
