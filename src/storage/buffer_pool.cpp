#include "keylane/storage/buffer_pool.h"

#include <sys/mman.h>
#include <sys/uio.h>

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

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

ReadBufferLease::~ReadBufferLease() {
  Reset();
}

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
  if (arena_ != nullptr) {
    ::munmap(arena_, arena_bytes_);
  }
}

celer::Status RegisteredBufferPool::Init(
    celer::Worker& worker, const RegisteredBufferPoolOptions& options) {
  using celer::Status;
  using celer::StatusCode;

  if (initialized()) {
    return Status(StatusCode::kFailedPrecondition,
                  "registered buffer pool is already initialized");
  }
  if (!IsPowerOfTwo(options.alignment) || options.alignment < 4096) {
    return Status(StatusCode::kInvalidArgument,
                  "registered buffer alignment must be a power of two >= 4096");
  }
  if (!IsAligned(options.write_buffer_bytes, options.alignment) ||
      !IsAligned(options.read_payload_bytes, options.alignment) ||
      !IsAligned(options.read_headroom_bytes, options.alignment) ||
      !IsAligned(options.read_tailroom_bytes, options.alignment)) {
    return Status(StatusCode::kInvalidArgument,
                  "registered buffer sizes must satisfy O_DIRECT alignment");
  }

  const std::size_t read_slot_bytes = options.read_headroom_bytes +
                                      options.read_payload_bytes +
                                      options.read_tailroom_bytes;
  if (options.write_buffer_bytes == 0 || read_slot_bytes == 0 ||
      options.registered_bytes < options.write_buffer_bytes + read_slot_bytes) {
    return Status(StatusCode::kInvalidArgument,
                  "registered buffer budget must fit one write and one read buffer");
  }

  const std::size_t read_count =
      (options.registered_bytes - options.write_buffer_bytes) / read_slot_bytes;
  if (read_count >= std::numeric_limits<std::uint16_t>::max()) {
    return Status(StatusCode::kOutOfRange,
                  "registered buffer count exceeds fixed-buffer index range");
  }

  const std::size_t arena_bytes =
      options.write_buffer_bytes + read_count * read_slot_bytes;
  void* mapping = ::mmap(nullptr, arena_bytes, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    return Status(StatusCode::kResourceExhausted,
                  "failed to allocate registered buffer arena");
  }
  (void)::madvise(mapping, arena_bytes, MADV_HUGEPAGE);

  std::vector<iovec> iovecs;
  iovecs.reserve(read_count + 1);
  auto* base = static_cast<std::byte*>(mapping);
  iovecs.push_back(iovec{.iov_base = base,
                         .iov_len = options.write_buffer_bytes});

  std::vector<celer::FixedBuffer> read_buffers;
  read_buffers.reserve(read_count);
  for (std::size_t i = 0; i < read_count; ++i) {
    std::byte* slot = base + options.write_buffer_bytes + i * read_slot_bytes;
    iovecs.push_back(iovec{.iov_base = slot, .iov_len = read_slot_bytes});
    read_buffers.push_back(celer::FixedBuffer{
        .data = slot,
        .size = read_slot_bytes,
        .index = static_cast<std::uint16_t>(i + 1),
    });
  }

  Status status = worker.RegisterBuffers(iovecs);
  if (!status.ok()) {
    ::munmap(mapping, arena_bytes);
    return status;
  }

  worker_ = &worker;
  cross_core_ = celer::ThisWorker().cross_core;
  owner_worker_ = worker.id();
  options_ = options;
  arena_ = base;
  arena_bytes_ = arena_bytes;
  write_buffer_ = celer::FixedBuffer{
      .data = base,
      .size = options.write_buffer_bytes,
      .index = 0,
  };
  read_buffers_ = std::move(read_buffers);
  free_read_buffers_.reserve(read_count);
  read_buffer_in_use_.assign(read_count, false);
  for (std::size_t i = read_count; i > 0; --i) {
    free_read_buffers_.push_back(static_cast<std::uint16_t>(i));
  }
  return Status::Ok();
}

bool RegisteredBufferPool::AcquireReadAwaiter::await_ready() const noexcept {
  return true;
}

bool RegisteredBufferPool::AcquireReadAwaiter::await_suspend(
    std::coroutine_handle<> awaiting) {
  (void)awaiting;
  return false;
}

celer::StatusOr<ReadBufferLease>
RegisteredBufferPool::AcquireReadAwaiter::await_resume() {
  if (pool_ == nullptr || !pool_->initialized()) {
    return celer::Status(celer::StatusCode::kFailedPrecondition,
                         "registered buffer pool is not initialized");
  }
  if (!pool_->free_read_buffers_.empty()) {
    return pool_->TakeReadBuffer();
  }
  return pool_->AllocateHeapReadBuffer();
}

ReadBufferLease RegisteredBufferPool::TakeReadBuffer() {
  const std::uint16_t buffer_id = free_read_buffers_.back();
  free_read_buffers_.pop_back();
  const std::size_t offset = static_cast<std::size_t>(buffer_id - 1);
  read_buffer_in_use_[offset] = true;
  return ReadBufferLease(this, owner_worker_, read_buffers_[offset],
                         options_.read_headroom_bytes,
                         options_.read_tailroom_bytes);
}

celer::StatusOr<ReadBufferLease>
RegisteredBufferPool::AllocateHeapReadBuffer() {
  const std::size_t bytes = options_.read_headroom_bytes +
                            options_.read_payload_bytes +
                            options_.read_tailroom_bytes;
  auto* data = static_cast<std::byte*>(::operator new[](
      bytes, std::align_val_t(options_.alignment), std::nothrow));
  if (data == nullptr) {
    return celer::Status(celer::StatusCode::kResourceExhausted,
                         "aligned heap read buffer allocation failed");
  }
  return ReadBufferLease(owner_worker_,
                         celer::FixedBuffer{.data = data,
                                            .size = bytes,
                                            .index = 0},
                         options_.read_headroom_bytes,
                         options_.read_tailroom_bytes, options_.alignment);
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
  if (buffer_id == 0 || buffer_id > read_buffers_.size()) {
    return;
  }
  const std::size_t offset = static_cast<std::size_t>(buffer_id - 1);
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
