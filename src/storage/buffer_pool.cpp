/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "lavik/storage/buffer_pool.h"

#include <sys/resource.h>
#include <sys/uio.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <new>
#include <optional>
#include <vector>

#include "bycorf/io/spdk_storage.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace lavik::storage {
namespace {

bool IsPowerOfTwo(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

bool IsAligned(std::size_t value, std::size_t alignment) noexcept {
  return (value & (alignment - 1)) == 0;
}

}  // namespace

ReadBufferLease::ReadBufferLease(RegisteredBufferPool* pool,
                                 bycorf::FixedBuffer buffer,
                                 std::size_t headroom_bytes,
                                 std::size_t tailroom_bytes,
                                 std::uint16_t slot_id) noexcept
    : pool_(pool),
      data_(buffer.data_),
      size_(static_cast<std::uint32_t>(buffer.size_)),
      headroom_bytes_(static_cast<std::uint32_t>(headroom_bytes)),
      tailroom_bytes_(static_cast<std::uint32_t>(tailroom_bytes)),
      release_token_(slot_id) {
  assert(buffer.size_ <= std::numeric_limits<std::uint32_t>::max());
  assert(headroom_bytes <= std::numeric_limits<std::uint32_t>::max());
  assert(tailroom_bytes <= std::numeric_limits<std::uint32_t>::max());
  assert(slot_id != 0);
}

ReadBufferLease::ReadBufferLease(RegisteredBufferPool* pool,
                                 bycorf::FixedBuffer buffer,
                                 std::size_t headroom_bytes,
                                 std::size_t tailroom_bytes,
                                 std::size_t overflow_id) noexcept
    : pool_(pool),
      data_(buffer.data_),
      size_(static_cast<std::uint32_t>(buffer.size_)),
      headroom_bytes_(static_cast<std::uint32_t>(headroom_bytes)),
      tailroom_bytes_(static_cast<std::uint32_t>(tailroom_bytes)),
      release_token_(kOverflowTokenBit |
                     static_cast<std::uint32_t>(overflow_id)) {
  assert(buffer.size_ <= std::numeric_limits<std::uint32_t>::max());
  assert(headroom_bytes <= std::numeric_limits<std::uint32_t>::max());
  assert(tailroom_bytes <= std::numeric_limits<std::uint32_t>::max());
  assert(overflow_id != 0 && overflow_id <= kReleaseTokenMask);
  assert(buffer.index_ == 0);
}

ReadBufferLease::ReadBufferLease(ReadBufferLease&& other) noexcept
    : pool_(std::exchange(other.pool_, nullptr)),
      data_(std::exchange(other.data_, nullptr)),
      size_(other.size_),
      headroom_bytes_(other.headroom_bytes_),
      tailroom_bytes_(other.tailroom_bytes_),
      release_token_(std::exchange(other.release_token_, 0)) {}

ReadBufferLease& ReadBufferLease::operator=(ReadBufferLease&& other) noexcept {
  if (this != &other) {
    Reset();
    pool_ = std::exchange(other.pool_, nullptr);
    data_ = std::exchange(other.data_, nullptr);
    size_ = other.size_;
    headroom_bytes_ = other.headroom_bytes_;
    tailroom_bytes_ = other.tailroom_bytes_;
    release_token_ = std::exchange(other.release_token_, 0);
  }
  return *this;
}

ReadBufferLease::~ReadBufferLease() { Reset(); }

bycorf::FixedBuffer ReadBufferLease::registered_buffer() const noexcept {
  const std::uint16_t id = buffer_id();
  // The immutable pool table maps release ids to arena registrations. Keeping
  // this mapping in the pool preserves the compact cross-worker lease.
  const std::uint16_t index =
      id == 0
          ? 0
          : pool_->read_buffers_[id - pool_->write_buffers_.size() - 1].index_;
  return {.data_ = data_, .size_ = size_, .index_ = index};
}

bycorf::FixedBuffer ReadBufferLease::io_buffer() const noexcept {
  if (!valid() || size_ < headroom_bytes_ + tailroom_bytes_) {
    return {};
  }
  return bycorf::FixedBuffer{
      .data_ = data_ + headroom_bytes_,
      .size_ = size_ - headroom_bytes_ - tailroom_bytes_,
      .index_ = registered_buffer().index_,
  };
}

void ReadBufferLease::Reset() noexcept {
  if (pool_ != nullptr) {
    RegisteredBufferPool* pool = std::exchange(pool_, nullptr);
    if ((release_token_ & kOverflowTokenBit) != 0) {
      pool->ReleaseOverflow(release_token_ & kReleaseTokenMask);
    } else {
      pool->Release(static_cast<std::uint16_t>(release_token_));
    }
  }
  data_ = nullptr;
  size_ = 0;
  release_token_ = 0;
}

RegisteredBufferPool::~RegisteredBufferPool() {
  if (sentinel_buffer_ != nullptr) {
    bycorf::FreeStorageBuffer(sentinel_buffer_, options_.alignment_);
    sentinel_buffer_ = nullptr;
    sentinel_buffer_bytes_ = 0;
  }
  for (std::byte* arena : arenas_) {
    bycorf::FreeStorageBuffer(arena, options_.alignment_);
  }
  if (arenas_.empty()) {
    for (const auto& buffer : write_buffers_) {
      bycorf::FreeStorageBuffer(buffer.data_, options_.alignment_);
    }
    for (const auto& buffer : read_buffers_) {
      bycorf::FreeStorageBuffer(buffer.data_, options_.alignment_);
    }
  }
  for (std::byte* buffer : heap_write_buffers_) {
    if (buffer != nullptr && IsAligned(reinterpret_cast<std::uintptr_t>(buffer),
                                       options_.alignment_)) {
      bycorf::FreeStorageBuffer(buffer, options_.alignment_);
    }
  }
  for (const bycorf::FixedBuffer& buffer : overflow_read_buffers_) {
    if (buffer.data_ != nullptr &&
        IsAligned(reinterpret_cast<std::uintptr_t>(buffer.data_),
                  options_.alignment_)) {
      bycorf::FreeStorageBuffer(buffer.data_, options_.alignment_);
    }
  }
}

absl::Status RegisteredBufferPool::Init(
    bycorf::Worker& worker, const RegisteredBufferPoolOptions& options) {
  if (initialized()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "registered buffer pool is already initialized");
  }
  if (!IsPowerOfTwo(options.alignment_) || options.alignment_ < 4096) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "registered buffer alignment must be a power of two >= 4096");
  }
  if (!IsAligned(options.write_buffer_bytes_, options.alignment_) ||
      !IsAligned(options.read_payload_bytes_, options.alignment_) ||
      !IsAligned(options.read_headroom_bytes_, options.alignment_) ||
      !IsAligned(options.read_tailroom_bytes_, options.alignment_)) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "registered buffer sizes must satisfy O_DIRECT alignment");
  }
  if (options.storage_write_buffer_count_ == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "storage write buffers are required");
  }
  const std::size_t configured_write_count =
      options.storage_write_buffer_count_;
  if (configured_write_count >
      static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max() - 1)) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "too many write buffers for fixed-buffer indices");
  }

  if (options.write_buffer_bytes_ == 0 || options.read_payload_bytes_ == 0 ||
      options.read_headroom_bytes_ == 0 || options.read_tailroom_bytes_ == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "registered buffer sizes must be positive");
  }
  if (options.read_payload_bytes_ > std::numeric_limits<std::uint32_t>::max() ||
      options.read_headroom_bytes_ >
          std::numeric_limits<std::uint32_t>::max() ||
      options.read_tailroom_bytes_ >
          std::numeric_limits<std::uint32_t>::max()) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "read buffer dimensions exceed the 32-bit lease "
                        "representation");
  }
  if (options.write_buffer_bytes_ > 0 &&
      (std::numeric_limits<std::size_t>::max() / configured_write_count) <
          options.write_buffer_bytes_) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "write buffer budget overflow");
  }

  if (options.read_headroom_bytes_ > std::numeric_limits<std::size_t>::max() -
                                         options.read_payload_bytes_ ||
      options.read_headroom_bytes_ + options.read_payload_bytes_ >
          std::numeric_limits<std::size_t>::max() -
              options.read_tailroom_bytes_) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "registered read buffer size overflow");
  }
  const std::size_t read_slot_bytes = options.read_headroom_bytes_ +
                                      options.read_payload_bytes_ +
                                      options.read_tailroom_bytes_;
  if (read_slot_bytes > std::numeric_limits<std::uint32_t>::max()) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "read buffer slot exceeds the 32-bit lease "
                        "representation");
  }
  const std::size_t registered_write_count = configured_write_count;
  const std::size_t registered_write_bytes =
      registered_write_count * options.write_buffer_bytes_;
  if (registered_write_bytes > options.registered_bytes_) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "registered buffer budget cannot fit configured storage write "
        "buffers");
  }
  const std::size_t read_count =
      (options.registered_bytes_ - registered_write_bytes) / read_slot_bytes;
  const std::size_t total_count = registered_write_count + read_count;
  if (read_count >= std::numeric_limits<std::uint16_t>::max() ||
      total_count >= std::numeric_limits<std::uint16_t>::max()) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "total fixed-buffer count exceeds index range");
  }

  const std::size_t read_base = registered_write_count + 1;
  const bool shared_arena = !bycorf::SpdkStorageEnabled();
  const std::size_t sentinel_bytes = options.alignment_;
  auto* sentinel = static_cast<std::byte*>(
      bycorf::AllocateStorageBuffer(sentinel_bytes, options.alignment_));
  if (sentinel == nullptr) {
    return absl::ResourceExhaustedError(
        "aligned storage buffer allocation failed");
  }
  std::fill_n(sentinel, sentinel_bytes, std::byte{0});
  std::vector<iovec> iovecs;
  iovecs.push_back(iovec{.iov_base = sentinel, .iov_len = sentinel_bytes});
  std::vector<bycorf::FixedBuffer> write_buffers;
  std::vector<bycorf::FixedBuffer> read_buffers;
  write_buffers.reserve(registered_write_count);
  read_buffers.reserve(read_count);
  std::vector<std::byte*> arenas;
  if (shared_arena) {
    // Read slots precede write slots across shared allocations. Registration
    // can pin a read-first prefix without changing any pool ownership. Linux
    // 6.1 limits each iovec to 1 GiB; split on whole-slot boundaries so each
    // I/O stays within one region. An individually oversized configured slot
    // still follows the existing registration-failure/ordinary-I/O fallback.
    constexpr std::size_t kMaxRegisteredBytes = 1024 * kMiB;
    auto plan_slice = [&](std::size_t bytes) {
      if (iovecs.size() == 1 || bytes > kMaxRegisteredBytes ||
          iovecs.back().iov_len > kMaxRegisteredBytes - bytes) {
        iovecs.push_back(iovec{});
      }
      iovecs.back().iov_len += bytes;
      return bycorf::FixedBuffer{
          .data_ = nullptr,
          .size_ = bytes,
          .index_ = static_cast<std::uint16_t>(iovecs.size() - 1)};
    };
    for (std::size_t i = 0; i < read_count; ++i) {
      read_buffers.push_back(plan_slice(read_slot_bytes));
    }
    for (std::size_t i = 0; i < registered_write_count; ++i) {
      write_buffers.push_back(plan_slice(options.write_buffer_bytes_));
    }
    for (std::size_t i = 1; i < iovecs.size(); ++i) {
      auto* arena = static_cast<std::byte*>(
          bycorf::AllocateStorageBuffer(iovecs[i].iov_len, options.alignment_));
      if (arena == nullptr) {
        for (auto* allocated : arenas) {
          bycorf::FreeStorageBuffer(allocated, options.alignment_);
        }
        bycorf::FreeStorageBuffer(sentinel, options.alignment_);
        return absl::ResourceExhaustedError(
            "aligned storage arena allocation failed");
      }
      arenas.push_back(arena);
      iovecs[i].iov_base = arena;
    }
    std::vector<std::size_t> offsets(iovecs.size(), 0);
    auto assign_slices = [&](auto& buffers) {
      for (auto& buffer : buffers) {
        auto& offset = offsets[buffer.index_];
        buffer.data_ = arenas[buffer.index_ - 1] + offset;
        offset += buffer.size_;
      }
    };
    assign_slices(read_buffers);
    assign_slices(write_buffers);
  } else {
    for (std::size_t i = 0; i < registered_write_count; ++i) {
      auto* data = static_cast<std::byte*>(bycorf::AllocateStorageBuffer(
          options.write_buffer_bytes_, options.alignment_));
      if (data == nullptr) {
        bycorf::FreeStorageBuffer(sentinel, options.alignment_);
        for (const bycorf::FixedBuffer& buffer : write_buffers) {
          bycorf::FreeStorageBuffer(buffer.data_, options.alignment_);
        }
        return absl::Status(
            absl::StatusCode::kResourceExhausted,
            "aligned registered-write-buffer allocation failed");
      }
      const std::size_t id = i + 1;
      iovecs.push_back(
          iovec{.iov_base = data, .iov_len = options.write_buffer_bytes_});
      write_buffers.push_back(bycorf::FixedBuffer{
          .data_ = data,
          .size_ = options.write_buffer_bytes_,
          .index_ = static_cast<std::uint16_t>(id),
      });
    }

    for (std::size_t i = 0; i < read_count; ++i) {
      auto* data = static_cast<std::byte*>(
          bycorf::AllocateStorageBuffer(read_slot_bytes, options.alignment_));
      if (data == nullptr) {
        bycorf::FreeStorageBuffer(sentinel, options.alignment_);
        for (const bycorf::FixedBuffer& buffer : write_buffers) {
          bycorf::FreeStorageBuffer(buffer.data_, options.alignment_);
        }
        for (const bycorf::FixedBuffer& buffer : read_buffers) {
          bycorf::FreeStorageBuffer(buffer.data_, options.alignment_);
        }
        return absl::Status(absl::StatusCode::kResourceExhausted,
                            "aligned registered-read-buffer allocation failed");
      }
      const std::size_t id = read_base + i;
      iovecs.push_back(iovec{.iov_base = data, .iov_len = read_slot_bytes});
      read_buffers.push_back(bycorf::FixedBuffer{
          .data_ = data,
          .size_ = read_slot_bytes,
          .index_ = static_cast<std::uint16_t>(id),
      });
    }
  }

  absl::Status status = worker.RegisterBuffers(iovecs);
  bool buffers_registered = status.ok();
  std::size_t registered_reads = buffers_registered ? read_count : 0;
  std::size_t registered_writes =
      buffers_registered ? registered_write_count : 0;
  if (!status.ok()) {
    if (bycorf::SpdkStorageEnabled()) {
      // SPDK registration is a DMA-addressability check; memory that fails it
      // cannot be handed to the device at all, so plain IO would fail the same
      // way. Fail fast instead of degrading.
      bycorf::FreeStorageBuffer(sentinel, options.alignment_);
      for (const bycorf::FixedBuffer& buffer : write_buffers) {
        bycorf::FreeStorageBuffer(buffer.data_, options.alignment_);
      }
      for (const bycorf::FixedBuffer& buffer : read_buffers) {
        bycorf::FreeStorageBuffer(buffer.data_, options.alignment_);
      }
      return status;
    } else {
      rlimit memlock{};
      if (::getrlimit(RLIMIT_MEMLOCK, &memlock) == 0) {
        if (memlock.rlim_cur != RLIM_INFINITY) {
          // Divide the allowance across runtime workers (including Meta is
          // conservative). This is an estimate, not a reservation: other
          // rings/registrations can consume it before this worker gets here.
          const auto* cross_core = bycorf::ThisWorker().cross_core_;
          const unsigned workers =
              cross_core == nullptr ? 1 : cross_core->size();
          std::uint64_t budget = memlock.rlim_cur / std::max(workers, 1U);
          std::vector<iovec> partial(iovecs.size());
          while (budget > sentinel_bytes) {
            std::uint64_t remaining = budget - sentinel_bytes;
            const std::size_t reads = std::min<std::uint64_t>(
                read_count, remaining / read_slot_bytes);
            remaining -= reads * read_slot_bytes;
            const std::size_t writes =
                reads == read_count
                    ? std::min<std::uint64_t>(
                          registered_write_count,
                          remaining / options.write_buffer_bytes_)
                    : 0;
            if (reads == 0 && writes == 0) break;
            if (reads == read_count && writes == registered_write_count) {
              budget /= 2;  // The full registration already failed.
              continue;
            }
            // Pin a whole-slot prefix: read slots first, then complete write
            // slots. Even slots sharing one index can be outside this prefix;
            // buffer_registered(slot_id) selects ordinary I/O for those slots.
            std::fill(partial.begin(), partial.end(), iovec{});
            partial[0] = iovecs[0];
            std::size_t bytes =
                reads * read_slot_bytes + writes * options.write_buffer_bytes_;
            for (std::size_t i = 1; i < iovecs.size() && bytes != 0; ++i) {
              partial[i] = iovecs[i];
              partial[i].iov_len = std::min(bytes, iovecs[i].iov_len);
              bytes -= partial[i].iov_len;
            }
            status = worker.RegisterBuffers(partial);
            if (status.ok()) {
              registered_reads = reads;
              registered_writes = writes;
              spdlog::warn(
                  "worker {}: io_uring buffer registration retried with "
                  "RLIMIT_MEMLOCK soft={}; registered read slots={}/{} "
                  "write slots={}/{} bytes={}; pool capacity unchanged, "
                  "remaining slots use unregistered IO",
                  worker.id(), static_cast<std::uint64_t>(memlock.rlim_cur),
                  reads, read_count, writes, registered_write_count,
                  sentinel_bytes + reads * read_slot_bytes +
                      writes * options.write_buffer_bytes_);
              break;
            }
            // Existing registrations may leave less than the estimated
            // share. Retry geometrically, with no cross-worker lock or wait.
            budget /= 2;
          }
        }
        if (!status.ok()) {
          spdlog::warn(
              "worker {}: io_uring buffer registration failed ({}); requested "
              "registered bytes={} RLIMIT_MEMLOCK soft={} hard={}; falling "
              "back "
              "to the same reusable buffers with unregistered IO",
              worker.id(), status.message(), options.registered_bytes_,
              memlock.rlim_cur == RLIM_INFINITY
                  ? std::numeric_limits<std::uint64_t>::max()
                  : static_cast<std::uint64_t>(memlock.rlim_cur),
              memlock.rlim_max == RLIM_INFINITY
                  ? std::numeric_limits<std::uint64_t>::max()
                  : static_cast<std::uint64_t>(memlock.rlim_max));
        }
      } else {
        spdlog::warn(
            "worker {}: io_uring buffer registration failed ({}); requested "
            "registered bytes={}; falling back to the same reusable buffers "
            "with unregistered IO",
            worker.id(), status.message(), options.registered_bytes_);
      }
    }
  }

  worker_ = &worker;
  cross_core_ = bycorf::ThisWorker().cross_core_;
  owner_worker_ = worker.id();
  buffers_registered_ = buffers_registered;
  registered_read_count_ = registered_reads;
  registered_write_count_ = registered_writes;
  options_ = options;
  sentinel_buffer_ = sentinel;
  sentinel_buffer_bytes_ = sentinel_bytes;
  arenas_ = std::move(arenas);
  write_buffers_ = std::move(write_buffers);
  read_buffers_ = std::move(read_buffers);
  write_buffer_in_use_.assign(write_buffers_.size(), false);
  read_buffer_in_use_.assign(read_buffers_.size(), false);
  free_write_buffers_.reserve(options_.storage_write_buffer_count_);
  for (std::size_t i = options_.storage_write_buffer_count_; i > 0; --i) {
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
         buffer_id <=
             static_cast<std::uint16_t>(options_.storage_write_buffer_count_);
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
  if (pool_ == nullptr || !pool_->initialized()) {
    return true;
  }
  // Oversized reads cannot use a fixed slot and retain the aligned overflow
  // path. Ordinary reads wait for their worker's bounded fixed pool instead
  // of turning a transient hand-back delay into unbounded DMA allocation.
  return minimum_payload_bytes_ > pool_->options_.read_payload_bytes_ ||
         !pool_->free_read_buffers_.empty();
}

bool RegisteredBufferPool::AcquireReadAwaiter::await_suspend(
    std::coroutine_handle<> awaiting) {
  if (pool_ == nullptr || !pool_->initialized() ||
      minimum_payload_bytes_ > pool_->options_.read_payload_bytes_ ||
      !pool_->free_read_buffers_.empty()) {
    return false;
  }
  awaiting_ = awaiting;
  pool_->read_waiters_.push_back(this);
  return true;
}

absl::StatusOr<ReadBufferLease>
RegisteredBufferPool::AcquireReadAwaiter::await_resume() {
  if (pool_ == nullptr || !pool_->initialized()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "registered buffer pool is not initialized");
  }
  if (assigned_buffer_id_ != 0) {
    return pool_->LeaseReadBuffer(assigned_buffer_id_);
  }
  if (minimum_payload_bytes_ <= pool_->options_.read_payload_bytes_ &&
      !pool_->free_read_buffers_.empty()) {
    return pool_->TakeReadBuffer();
  }
  if (minimum_payload_bytes_ <= pool_->options_.read_payload_bytes_) {
    return absl::Status(absl::StatusCode::kInternal,
                        "read buffer waiter resumed without a buffer");
  }
  return pool_->AllocateHeapReadBuffer(minimum_payload_bytes_);
}

ReadBufferLease RegisteredBufferPool::TakeReadBuffer() {
  const std::uint16_t buffer_id = free_read_buffers_.back();
  free_read_buffers_.pop_back();
  const std::size_t read_base = write_buffers_.size() + 1;
  const std::size_t offset = static_cast<std::size_t>(buffer_id - read_base);
  read_buffer_in_use_[offset] = true;
  return LeaseReadBuffer(buffer_id);
}

ReadBufferLease RegisteredBufferPool::LeaseReadBuffer(std::uint16_t buffer_id) {
  const std::size_t read_base = write_buffers_.size() + 1;
  const std::size_t offset = static_cast<std::size_t>(buffer_id - read_base);
  return ReadBufferLease(this, read_buffers_[offset],
                         options_.read_headroom_bytes_,
                         options_.read_tailroom_bytes_, buffer_id);
}

absl::StatusOr<ReadBufferLease> RegisteredBufferPool::AllocateHeapReadBuffer(
    std::size_t minimum_payload_bytes) {
  std::size_t payload_bytes =
      std::max(options_.read_payload_bytes_, minimum_payload_bytes);
  if (payload_bytes >
      std::numeric_limits<std::size_t>::max() - (options_.alignment_ - 1)) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "heap read buffer size overflow");
  }
  payload_bytes =
      (payload_bytes + options_.alignment_ - 1) & ~(options_.alignment_ - 1);
  if (payload_bytes > std::numeric_limits<std::size_t>::max() -
                          options_.read_headroom_bytes_ ||
      payload_bytes + options_.read_headroom_bytes_ >
          std::numeric_limits<std::size_t>::max() -
              options_.read_tailroom_bytes_) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "heap read buffer size overflow");
  }
  const std::size_t bytes = options_.read_headroom_bytes_ + payload_bytes +
                            options_.read_tailroom_bytes_;
  if (bytes > std::numeric_limits<std::uint32_t>::max()) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "overflow read buffer exceeds the 32-bit lease "
                        "representation");
  }
  std::size_t best_free = free_overflow_read_buffers_.size();
  std::size_t best_bytes = std::numeric_limits<std::size_t>::max();
  for (std::size_t i = 0; i < free_overflow_read_buffers_.size(); ++i) {
    const std::size_t overflow_id = free_overflow_read_buffers_[i];
    const std::size_t candidate_bytes =
        overflow_read_buffers_[overflow_id - 1].size_;
    if (candidate_bytes >= bytes && candidate_bytes < best_bytes) {
      best_free = i;
      best_bytes = candidate_bytes;
    }
  }
  if (best_free != free_overflow_read_buffers_.size()) {
    const std::size_t overflow_id = free_overflow_read_buffers_[best_free];
    free_overflow_read_buffers_[best_free] = free_overflow_read_buffers_.back();
    free_overflow_read_buffers_.pop_back();
    overflow_read_buffer_in_use_[overflow_id - 1] = true;
    return ReadBufferLease(this, overflow_read_buffers_[overflow_id - 1],
                           options_.read_headroom_bytes_,
                           options_.read_tailroom_bytes_, overflow_id);
  }
  auto* data = static_cast<std::byte*>(
      bycorf::AllocateStorageBuffer(bytes, options_.alignment_));
  if (data == nullptr) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "aligned heap read buffer allocation failed");
  }
  if (overflow_read_buffers_.size() >= ReadBufferLease::kReleaseTokenMask) {
    bycorf::FreeStorageBuffer(data, options_.alignment_);
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "overflow read buffer id space exhausted");
  }
  overflow_read_buffers_.push_back(
      bycorf::FixedBuffer{.data_ = data, .size_ = bytes, .index_ = 0});
  overflow_read_buffer_in_use_.push_back(true);
  const std::size_t overflow_id = overflow_read_buffers_.size();
  return ReadBufferLease(this, overflow_read_buffers_.back(),
                         options_.read_headroom_bytes_,
                         options_.read_tailroom_bytes_, overflow_id);
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
  auto* data = static_cast<std::byte*>(bycorf::AllocateStorageBuffer(
      options_.write_buffer_bytes_, options_.alignment_));
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
  if (worker_ != nullptr) storage_write_buffer_ready_.NotifyAll(*worker_);
}

void RegisteredBufferPool::Release(std::uint16_t buffer_id) noexcept {
  const bycorf::CurrentWorker& current = bycorf::ThisWorker();
  if (current.cross_core_ == cross_core_ && current.id_ == owner_worker_) {
    ReleaseLocal(buffer_id);
    return;
  }
  if (current.cross_core_ == nullptr || current.cross_core_ != cross_core_) {
    return;
  }
  bycorf::PostNotification(
      cross_core_, owner_worker_,
      bycorf::RemoteNotification{
          .context_ = this,
          .value_ = buffer_id,
          .run_fn_ = &RegisteredBufferPool::HandleRemoteRelease,
      });
}

void RegisteredBufferPool::ReleaseOverflow(std::size_t overflow_id) noexcept {
  const bycorf::CurrentWorker& current = bycorf::ThisWorker();
  if (current.cross_core_ == cross_core_ && current.id_ == owner_worker_) {
    ReleaseOverflowLocal(overflow_id);
    return;
  }
  if (current.cross_core_ == nullptr || current.cross_core_ != cross_core_) {
    return;
  }
  bycorf::PostNotification(
      cross_core_, owner_worker_,
      bycorf::RemoteNotification{
          .context_ = this,
          .value_ = overflow_id,
          .run_fn_ = &RegisteredBufferPool::HandleRemoteOverflowRelease,
      });
}

void RegisteredBufferPool::ReleaseOverflowLocal(
    std::size_t overflow_id) noexcept {
  if (overflow_id == 0 || overflow_id > overflow_read_buffers_.size()) {
    return;
  }
  const std::size_t offset = overflow_id - 1;
  if (!overflow_read_buffer_in_use_[offset]) {
    return;
  }
  overflow_read_buffer_in_use_[offset] = false;
  free_overflow_read_buffers_.push_back(overflow_id);
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
  if (!read_waiters_.empty()) {
    AcquireReadAwaiter* waiter = read_waiters_.front();
    read_waiters_.pop_front();
    waiter->assigned_buffer_id_ = buffer_id;
    if (worker_ != nullptr) {
      worker_->Enqueue(waiter->awaiting_);
    }
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

void RegisteredBufferPool::HandleRemoteOverflowRelease(
    void* context, std::uint64_t value) noexcept {
  static_cast<RegisteredBufferPool*>(context)->ReleaseOverflowLocal(
      static_cast<std::size_t>(value));
}

}  // namespace lavik::storage
