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
#include <bit>
#include <cstddef>
#include <limits>
#include <new>
#include <optional>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "bycorf/io/spdk_storage.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "lavik/fault_injection.h"
#include "lavik/storage/format.h"
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
                                 std::uint32_t release_token) noexcept
    : pool_(pool),
      data_(buffer.data_),
      size_(static_cast<std::uint32_t>(buffer.size_)),
      headroom_bytes_(static_cast<std::uint32_t>(headroom_bytes)),
      tailroom_bytes_(static_cast<std::uint32_t>(tailroom_bytes)),
      release_token_(release_token) {
  assert(buffer.size_ <= std::numeric_limits<std::uint32_t>::max());
  assert(release_token != 0);
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

void ReadBufferLease::Reset() noexcept {
  if (pool_ != nullptr) {
    RegisteredBufferPool* pool = std::exchange(pool_, nullptr);
    if ((release_token_ & kOutputTokenBit) != 0) {
      pool->ReleaseOutput(release_token_ & kReleaseTokenMask);
    } else {
      pool->ReleaseRead(release_token_ - 1, size_ >> pool->read_unit_shift_);
    }
  }
  data_ = nullptr;
  size_ = 0;
  release_token_ = 0;
}

RegisteredBufferPool::~RegisteredBufferPool() {
  for (const auto& buffer : maintenance_read_buffers_) {
    if (buffer.data_ != nullptr)
      bycorf::FreeStorageBuffer(buffer.data_, maintenance_alignment_);
  }
  if (read_arena_.data_ != nullptr)
    bycorf::FreeStorageBuffer(read_arena_.data_, options_.alignment_);
  for (const auto& buffer : write_buffers_)
    bycorf::FreeStorageBuffer(buffer.data_, options_.alignment_);
  for (const auto& buffer : output_buffers_) {
    if (buffer.data_ != nullptr)
      ::operator delete[](buffer.data_, std::align_val_t(options_.alignment_));
  }
}

absl::Status RegisteredBufferPool::PrepareMaintenanceBuffers(
    unsigned owner_worker, std::size_t alignment) {
  if (!IsPowerOfTwo(alignment) || alignment < kDirectIoAlignment ||
      kStorageBlockBytes % alignment != 0) {
    return absl::InvalidArgumentError("invalid maintenance buffer alignment");
  }
  if (maintenance_alignment_ != 0) {
    if (owner_worker_ != owner_worker || maintenance_alignment_ != alignment) {
      return absl::FailedPreconditionError(
          "prepared maintenance buffer owner or alignment changed");
    }
    return absl::OkStatus();
  }
  // Scanners can suspend while relocating or making cross-worker claims.
  // Separate workspaces preserve their concurrency without consuming the
  // write slots whose progress those same scans may need.
  std::array<bycorf::FixedBuffer, 3> maintenance_buffers{};
  absl::Cleanup free_maintenance = [&] {
    for (const auto& buffer : maintenance_buffers) {
      if (buffer.data_ != nullptr)
        bycorf::FreeStorageBuffer(buffer.data_, alignment);
    }
  };
  constexpr std::array<const char*, 3> maintenance_names{"defrag", "tx-cleaner",
                                                         "tomb-raider"};
  for (std::size_t i = 0; i < maintenance_buffers.size(); ++i) {
    bool fail_allocation = false;
    LAVIK_FAULT_INJECT(
        fail_allocation = LAVIK_FAULT_MATCHES(
            "LAVIK_FAIL_MAINTENANCE_BUFFER_ALLOCATION", maintenance_names[i]););
    auto* data = fail_allocation
                     ? nullptr
                     : static_cast<std::byte*>(bycorf::AllocateStorageBuffer(
                           kStorageBlockBytes, alignment));
    if (data == nullptr) {
      return absl::ResourceExhaustedError(absl::StrCat(
          "worker ", owner_worker, ": failed to allocate ",
          maintenance_names[i], " maintenance read buffer (",
          kStorageBlockBytes, " bytes, ",
          bycorf::SpdkStorageEnabled() ? "SPDK DMA" : "aligned memory", ")"));
    }
    maintenance_buffers[i] = {.data_ = data, .size_ = kStorageBlockBytes};
  }
  maintenance_read_buffers_ = maintenance_buffers;
  maintenance_alignment_ = alignment;
  owner_worker_ = owner_worker;
  std::move(free_maintenance).Cancel();
  maintenance_memory_.Account(
      owner_worker + 1, maintenance_read_buffers_.size() * kStorageBlockBytes);
  return absl::OkStatus();
}

absl::Status RegisteredBufferPool::Init(
    bycorf::Worker& worker, const RegisteredBufferPoolOptions& options,
    std::span<bycorf::FixedBuffer> priority_buffers) {
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
      !IsAligned(options.read_slot_bytes_, options.alignment_) ||
      !IsAligned(options.read_headroom_bytes_, options.alignment_) ||
      !IsAligned(options.read_tailroom_bytes_, options.alignment_)) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "registered buffer sizes must satisfy O_DIRECT alignment");
  }
  if (options.storage_write_buffer_count_ < 2) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "at least two storage write buffers are required");
  }
  const std::size_t configured_write_count =
      options.storage_write_buffer_count_;
  if (configured_write_count >
      static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max() - 1)) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "too many write buffers for fixed-buffer indices");
  }

  if (options.write_buffer_bytes_ == 0 || options.read_payload_bytes() == 0 ||
      options.read_headroom_bytes_ == 0 || options.read_tailroom_bytes_ == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "registered buffer sizes must be positive");
  }
  if (options.read_slot_bytes_ > std::numeric_limits<std::uint32_t>::max() ||
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

  const std::size_t registered_write_count = configured_write_count;
  const std::size_t registered_write_bytes =
      registered_write_count * options.write_buffer_bytes_;
  if (registered_write_bytes > options.registered_bytes_) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "registered buffer budget cannot fit configured storage write "
        "buffers");
  }
  const std::size_t read_bytes =
      (options.registered_bytes_ - registered_write_bytes) &
      ~(options.alignment_ - 1);
  const std::size_t framing =
      options.read_headroom_bytes_ + options.read_tailroom_bytes_;
  if (read_bytes <= framing ||
      read_bytes / options.alignment_ > ReadBufferLease::kReleaseTokenMask)
    return absl::InvalidArgumentError(
        "read arena must fit one payload page and the range-token capacity");

  for (const auto& buffer : priority_buffers) {
    if (buffer.data_ == nullptr || buffer.size_ == 0 ||
        reinterpret_cast<std::uintptr_t>(buffer.data_) % kDirectIoAlignment !=
            0 ||
        buffer.size_ % kDirectIoAlignment != 0) {
      return absl::InvalidArgumentError("invalid priority storage workspace");
    }
  }
  absl::Status prepared =
      PrepareMaintenanceBuffers(worker.id(), options.alignment_);
  if (!prepared.ok()) return prepared;
  std::vector<iovec> iovecs;
  std::vector<bycorf::FixedBuffer> write_buffers;
  bycorf::FixedBuffer read_arena{};
  absl::Cleanup free_pool = [&] {
    if (read_arena.data_ != nullptr)
      bycorf::FreeStorageBuffer(read_arena.data_, options.alignment_);
    for (const auto& buffer : write_buffers)
      bycorf::FreeStorageBuffer(buffer.data_, options.alignment_);
  };
  read_arena.data_ = static_cast<std::byte*>(
      bycorf::AllocateStorageBuffer(read_bytes, options.alignment_));
  if (read_arena.data_ == nullptr)
    return absl::ResourceExhaustedError("storage read arena allocation failed");
  read_arena.size_ = read_bytes;
  // One virtual arena on both backends. Registration splits do not constrain
  // allocation: a lease crossing a kernel region uses plain asynchronous I/O.
  for (std::size_t offset = 0; offset < read_bytes;) {
    const auto bytes = std::min(kReadRegionBytes, read_bytes - offset);
    iovecs.push_back({read_arena.data_ + offset, bytes});
    offset += bytes;
  }
  write_buffers.reserve(registered_write_count);
  for (std::size_t i = 0; i < registered_write_count; ++i) {
    auto* data = static_cast<std::byte*>(bycorf::AllocateStorageBuffer(
        options.write_buffer_bytes_, options.alignment_));
    if (data == nullptr)
      return absl::ResourceExhaustedError(
          "storage write buffer allocation failed");
    write_buffers.push_back(
        {.data_ = data,
         .size_ = options.write_buffer_bytes_,
         .index_ = static_cast<std::uint16_t>(iovecs.size())});
    iovecs.push_back({data, options.write_buffer_bytes_});
  }
  // Descriptors live on the ordinary heap, never in DMA memory. Allocate every
  // possible boundary before registration/publication so splits cannot fail.
  try {
    read_metadata_.resize(read_bytes / options.alignment_);
  } catch (const std::bad_alloc&) {
    return absl::ResourceExhaustedError(
        "read arena metadata allocation failed");
  }
  if (!read_allocator_.Init(read_metadata_))
    return absl::OutOfRangeError("read arena exceeds allocator capacity");
  const std::size_t priority_count = priority_buffers.size();
  const std::size_t maintenance_count = maintenance_read_buffers_.size();
  constexpr std::size_t kMaxRegions =
      std::size_t{std::numeric_limits<std::uint16_t>::max()} + 1;
  if (priority_count > kMaxRegions - maintenance_count ||
      iovecs.size() > kMaxRegions - maintenance_count - priority_count) {
    return absl::OutOfRangeError("too many storage registration regions");
  }
  // Registration borrows the device images; ownership and retained charges
  // stay with their allocators. Read and write registration indices follow
  // these small high-priority pages without moving their underlying memory.
  std::vector<iovec> registration;
  registration.reserve(priority_count + iovecs.size() + maintenance_count);
  for (const auto& buffer : priority_buffers)
    registration.push_back({buffer.data_, buffer.size_});
  registration.insert(registration.end(), iovecs.begin(), iovecs.end());
  read_arena.index_ = static_cast<std::uint16_t>(priority_count);
  for (auto& buffer : write_buffers) buffer.index_ += priority_count;
  for (auto& buffer : maintenance_read_buffers_) {
    buffer.index_ = static_cast<std::uint16_t>(registration.size());
    registration.push_back({buffer.data_, buffer.size_});
  }
  iovecs = std::move(registration);
  std::size_t requested_bytes = 0;
  for (const auto& region : iovecs) requested_bytes += region.iov_len;

  absl::Status status = worker.RegisterBuffers(iovecs);
  std::size_t registered_priority = status.ok() ? priority_count : 0;
  std::size_t registered_reads = status.ok() ? read_bytes : 0;
  std::size_t registered_writes = status.ok() ? registered_write_count : 0;
  std::size_t registered_maintenance = status.ok() ? maintenance_count : 0;
  std::vector<iovec> registered = status.ok() ? iovecs : std::vector<iovec>{};
  if (!status.ok()) {
    if (bycorf::SpdkStorageEnabled()) {
      // SPDK registration validates DMA addressability. Ordinary I/O cannot
      // rescue an invalid DMA image, so fail startup without degrading.
      return status;
    }
    rlimit memlock{};
    if (::getrlimit(RLIMIT_MEMLOCK, &memlock) == 0) {
      if (memlock.rlim_cur != RLIM_INFINITY) {
        // The share is an estimate: other rings may pin memory concurrently.
        // Retry geometrically without a cross-worker lock or reservation.
        const auto* cross_core = bycorf::ThisWorker().cross_core_;
        const unsigned workers = cross_core == nullptr ? 1 : cross_core->size();
        std::uint64_t budget = memlock.rlim_cur / std::max(workers, 1U);
        while (budget != 0) {
          std::uint64_t remaining = budget;
          std::size_t priority = 0;
          while (priority < priority_count &&
                 priority_buffers[priority].size_ <= remaining) {
            remaining -= priority_buffers[priority++].size_;
          }
          const std::size_t reads =
              priority == priority_count
                  ? std::min<std::uint64_t>(
                        read_bytes, remaining & ~(options.alignment_ - 1))
                  : 0;
          remaining -= reads;
          const std::size_t writes =
              priority == priority_count && reads == read_bytes
                  ? std::min<std::uint64_t>(
                        registered_write_count,
                        remaining / options.write_buffer_bytes_)
                  : 0;
          remaining -= writes * options.write_buffer_bytes_;
          const std::size_t maintenance =
              priority == priority_count && reads == read_bytes &&
                      writes == registered_write_count
                  ? std::min<std::uint64_t>(maintenance_count,
                                            remaining / kStorageBlockBytes)
                  : 0;
          remaining -= maintenance * kStorageBlockBytes;
          const std::size_t bytes = budget - remaining;
          if (bytes == 0) break;
          if (bytes == requested_bytes) {
            budget /= 2;  // The full registration already failed.
            continue;
          }
          // All four priority classes form one prefix: page-aligned for reads,
          // whole-buffer aligned for the other classes. Trim unused entries;
          // even a metadata-only registration must work without reserving table
          // space for the ordinary pool.
          std::vector<iovec> partial;
          std::size_t unfilled = bytes;
          for (const auto& region : iovecs) {
            if (unfilled == 0) break;
            const auto size = std::min(unfilled, region.iov_len);
            partial.push_back({region.iov_base, size});
            unfilled -= size;
          }
          status = worker.RegisterBuffers(partial);
          if (status.ok()) {
            registered_priority = priority;
            registered_reads = reads;
            registered_writes = writes;
            registered_maintenance = maintenance;
            registered = std::move(partial);
            spdlog::warn(
                "worker {}: io_uring registration retried with RLIMIT_MEMLOCK "
                "soft={}; device workspaces={}/{} read arena bytes={}/{} write "
                "slots={}/{} maintenance buffers={}/{} bytes={}; remaining "
                "buffers use unregistered IO, capacity unchanged",
                worker.id(), static_cast<std::uint64_t>(memlock.rlim_cur),
                priority, priority_count, reads, read_bytes, writes,
                registered_write_count, maintenance, maintenance_count, bytes);
            break;
          }
          budget /= 2;
        }
      }
      if (!status.ok()) {
        spdlog::warn(
            "worker {}: io_uring buffer registration failed ({}); requested "
            "registered bytes={} RLIMIT_MEMLOCK soft={} hard={}; falling back "
            "to the same reusable buffers with unregistered IO",
            worker.id(), status.message(), requested_bytes,
            static_cast<std::uint64_t>(memlock.rlim_cur),
            static_cast<std::uint64_t>(memlock.rlim_max));
      }
    } else {
      spdlog::warn(
          "worker {}: io_uring buffer registration failed ({}); requested "
          "registered bytes={}; falling back to the same reusable buffers "
          "with unregistered IO",
          worker.id(), status.message(), requested_bytes);
    }
  }
  if (registered_priority == priority_count && registered_reads == read_bytes &&
      registered_writes == registered_write_count &&
      registered_maintenance == maintenance_count) {
    spdlog::info(
        "worker {}: registered device workspaces={} read arena bytes={} "
        "write slots={} maintenance buffers={} bytes={}",
        worker.id(), priority_count, read_bytes, registered_write_count,
        maintenance_count, requested_bytes);
  }
  for (std::size_t i = 0; i < priority_count; ++i)
    priority_buffers[i].index_ = static_cast<std::uint16_t>(i);
  for (const auto& region : registered) {
    registered_regions_.push_back(
        {.data_ = static_cast<std::byte*>(region.iov_base),
         .size_ = region.iov_len});
  }

  worker_ = &worker;
  cross_core_ = bycorf::ThisWorker().cross_core_;
  owner_worker_ = worker.id();
  buffers_registered_ = registered_reads == read_bytes &&
                        registered_writes == registered_write_count;
  registered_read_bytes_ = registered_reads;
  registered_write_count_ = registered_writes;
  options_ = options;
  read_unit_shift_ = std::countr_zero(options.alignment_);
  std::move(free_pool).Cancel();
  read_arena_ = read_arena;
  write_buffers_ = std::move(write_buffers);
  write_buffer_in_use_.assign(write_buffers_.size(), false);
  free_write_buffers_.reserve(options_.storage_write_buffer_count_);
  for (std::size_t i = options_.storage_write_buffer_count_; i > 0; --i)
    free_write_buffers_.push_back(static_cast<std::uint16_t>(i));
  // The fixed read arena and its descriptor/control storage stay charged even
  // when every lease is returned. Borrowing and coalescing change no charge.
  buffer_memory_.Account(
      worker.id() + 1,
      registered_write_bytes + read_bytes +
          read_metadata_.capacity() * sizeof(TlsfRangeAllocator::Metadata) +
          sizeof(read_allocator_));
  return absl::OkStatus();
}

bool RegisteredBufferPool::workspace_registered(
    bycorf::FixedBuffer buffer) const noexcept {
  if (buffer.data_ == nullptr || buffer.index_ >= registered_regions_.size())
    return false;
  const auto& region = registered_regions_[buffer.index_];
  const auto begin = reinterpret_cast<std::uintptr_t>(region.data_);
  const auto address = reinterpret_cast<std::uintptr_t>(buffer.data_);
  return address >= begin && address - begin <= region.size_ &&
         buffer.size_ <= region.size_ - (address - begin);
}

bool RegisteredBufferPool::IsWriteBufferId(
    std::uint16_t buffer_id) const noexcept {
  return buffer_id != 0 &&
         buffer_id <=
             static_cast<std::uint16_t>(options_.storage_write_buffer_count_);
}

std::uint32_t RegisteredBufferPool::ReadUnits(
    std::size_t payload_bytes) const noexcept {
  if (payload_bytes == 0) payload_bytes = options_.read_payload_bytes();
  const auto framing =
      options_.read_headroom_bytes_ + options_.read_tailroom_bytes_;
  // Arena capacity can exceed one lease's 32-bit byte length. Check before
  // adding alignment/framing so an impossible request never enters the queue.
  if (payload_bytes > read_arena_.size_ - framing ||
      payload_bytes > std::numeric_limits<std::uint32_t>::max() - framing -
                          (options_.alignment_ - 1))
    return 0;
  return static_cast<std::uint32_t>(
      (payload_bytes + framing + options_.alignment_ - 1) >> read_unit_shift_);
}

ReadBufferLease RegisteredBufferPool::TakeReadBuffer(std::size_t payload_bytes,
                                                     bool chunk) {
  if (payload_bytes == 0) payload_bytes = options_.read_payload_bytes();
  if (chunk)
    payload_bytes = std::min(payload_bytes, read_arena_.size_ -
                                                options_.read_headroom_bytes_ -
                                                options_.read_tailroom_bytes_);
  for (;;) {
    const auto units = ReadUnits(payload_bytes);
    if (units != 0) {
      const auto allocation = read_allocator_.Allocate(units);
      if (allocation) {
        bycorf::FixedBuffer buffer{
            .data_ = read_arena_.data_ +
                     (std::size_t{allocation.offset} << read_unit_shift_),
            .size_ = (std::size_t{allocation.units} << read_unit_shift_)};
        return ReadBufferLease(this, buffer, options_.read_headroom_bytes_,
                               options_.read_tailroom_bytes_,
                               allocation.offset + 1);
      }
    }
    if (!chunk || payload_bytes <= options_.alignment_) return {};
    // At most 32 size reductions, independent of arena fragmentation. An exact
    // one-page payload class ensures the final probe cannot miss a fitting
    // head.
    payload_bytes = std::max(options_.alignment_,
                             (payload_bytes / 2) & ~(options_.alignment_ - 1));
  }
}

absl::StatusOr<ReadBufferLease> RegisteredBufferPool::TryAcquireReadBuffer(
    std::size_t minimum_payload_bytes) {
  if (!initialized())
    return absl::FailedPreconditionError("read arena is not initialized");
  // Fault only the speculative full-image path. Chunk acquisition must still
  // progress, just as it does when the arena has no large contiguous range.
  LAVIK_FAULT_INJECT(
      if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_READ_BUFFER_ALLOCATION", "1")) {
        return absl::ResourceExhaustedError(
            "injected read arena allocation failure");
      });
  if (read_waiters_.empty()) {
    auto lease = TakeReadBuffer(minimum_payload_bytes, false);
    if (lease.valid()) return lease;
  }
  return absl::ResourceExhaustedError(
      "no contiguous read arena range available");
}

bool RegisteredBufferPool::AcquireReadAwaiter::await_ready() {
  if (assigned_.valid() || pool_ == nullptr || !pool_->initialized())
    return true;
  if (!chunk_ && pool_->ReadUnits(minimum_payload_bytes_) == 0) return true;
  if (!pool_->read_waiters_.empty()) return false;
  assigned_ = pool_->TakeReadBuffer(minimum_payload_bytes_, chunk_);
  return assigned_.valid();
}

bool RegisteredBufferPool::AcquireReadAwaiter::await_suspend(
    std::coroutine_handle<> awaiting) {
  if (await_ready()) return false;
  awaiting_ = awaiting;
  pool_->read_waiters_.push_back(this);
  return true;
}

bool RegisteredBufferPool::AcquireReadAwaiter::SuspendWith(
    bycorf::RemoteWork& continuation) {
  if (await_ready()) return false;
  continuation_ = &continuation;
  continuation.origin_ = pool_->owner_worker_;
  continuation.reply_deferred_ = true;
  pool_->read_waiters_.push_back(this);
  return true;
}

absl::StatusOr<ReadBufferLease>
RegisteredBufferPool::AcquireReadAwaiter::await_resume() {
  if (pool_ == nullptr || !pool_->initialized())
    return absl::FailedPreconditionError("read arena is not initialized");
  if (!assigned_.valid()) await_ready();
  if (assigned_.valid()) return std::move(assigned_);
  return absl::ResourceExhaustedError(
      "no contiguous read arena range available");
}

absl::StatusOr<ReadBufferLease> RegisteredBufferPool::AllocateReadOutputBuffer(
    std::size_t minimum_payload_bytes) {
  if (!initialized())
    return absl::FailedPreconditionError("read arena is not initialized");
  const auto framing =
      options_.read_headroom_bytes_ + options_.read_tailroom_bytes_;
  const auto payload = std::max(minimum_payload_bytes, options_.alignment_);
  if (payload > std::numeric_limits<std::uint32_t>::max() - framing -
                    (options_.alignment_ - 1))
    return absl::OutOfRangeError(
        "heap output exceeds the 32-bit lease representation");
  const auto bytes = framing + ((payload + options_.alignment_ - 1) &
                                ~(options_.alignment_ - 1));
  if (free_output_buffers_.empty() &&
      output_buffers_.size() >= ReadBufferLease::kReleaseTokenMask)
    return absl::ResourceExhaustedError("heap output ID space exhausted");
  if (free_output_buffers_.empty()) {
    // Returning an output is noexcept and must not allocate bookkeeping. Grow
    // both ID tables before allocating the payload; failure leaves no lease.
    try {
      if (output_buffers_.size() == output_buffers_.capacity()) {
        const auto capacity = std::min<std::size_t>(
            ReadBufferLease::kReleaseTokenMask,
            std::max<std::size_t>(8, output_buffers_.capacity() * 2));
        output_buffers_.reserve(capacity);
      }
      free_output_buffers_.reserve(output_buffers_.capacity());
    } catch (const std::bad_alloc&) {
      return absl::ResourceExhaustedError(
          "heap output bookkeeping allocation failed");
    }
  }
  auto* data = static_cast<std::byte*>(::operator new[](
      bytes, std::align_val_t(options_.alignment_), std::nothrow));
  if (data == nullptr)
    return absl::ResourceExhaustedError("heap output allocation failed");
  const bycorf::FixedBuffer buffer{.data_ = data, .size_ = bytes};
  std::uint32_t id;
  if (free_output_buffers_.empty()) {
    output_buffers_.push_back(buffer);
    id = static_cast<std::uint32_t>(output_buffers_.size());
  } else {
    id = static_cast<std::uint32_t>(free_output_buffers_.back());
    free_output_buffers_.pop_back();
    output_buffers_[id - 1] = buffer;
  }
  // Assembly must make progress at the retained limit just as reads do. Live
  // ordinary-heap outputs are charged without an additional admission gate.
  buffer_memory_.Resize(buffer_memory_.bytes() + bytes);
  return ReadBufferLease(this, buffer, options_.read_headroom_bytes_,
                         options_.read_tailroom_bytes_,
                         ReadBufferLease::kOutputTokenBit | id);
}

std::optional<std::uint16_t> RegisteredBufferPool::TakeWriteBuffer() {
  if (write_acquisition_failed_ || !write_waiters_.empty() ||
      free_write_buffers_.empty()) {
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

bool RegisteredBufferPool::AcquireWriteAwaiter::await_ready() noexcept {
  return pool_->write_acquisition_failed_ || assigned_buffer_id_ != 0 ||
         pool_->TryAcquireWriteBuffer(&assigned_buffer_id_);
}

bool RegisteredBufferPool::AcquireWriteAwaiter::await_suspend(
    std::coroutine_handle<> awaiting) {
  if (await_ready()) return false;
  awaiting_ = awaiting;
  pool_->write_waiters_.push_back(this);
  return true;
}

absl::StatusOr<std::uint16_t>
RegisteredBufferPool::AcquireWriteAwaiter::await_resume() {
  if (pool_->write_acquisition_failed_) {
    if (assigned_buffer_id_ != 0)
      pool_->ReleaseWriteBuffer(std::exchange(assigned_buffer_id_, 0));
    return absl::FailedPreconditionError(
        "storage writer is stopped after an IO failure");
  }
  assert(assigned_buffer_id_ != 0);
  return std::exchange(assigned_buffer_id_, 0);
}

void RegisteredBufferPool::FailWriteWaiters() noexcept {
  write_acquisition_failed_ = true;
  while (!write_waiters_.empty()) {
    auto* waiter = write_waiters_.front();
    write_waiters_.pop_front();
    worker_->Enqueue(waiter->awaiting_);
  }
}

void RegisteredBufferPool::ReleaseWriteBuffer(
    std::uint16_t buffer_id) noexcept {
  ReleaseWriteBufferLocal(buffer_id);
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
  if (!write_waiters_.empty()) {
    auto* waiter = write_waiters_.front();
    write_waiters_.pop_front();
    waiter->assigned_buffer_id_ = buffer_id;
    // Keep the slot in use across the handoff. A new borrower cannot steal it
    // while the scheduled writer waits for its next worker time slice.
    worker_->Enqueue(waiter->awaiting_);
    return;
  }
  write_buffer_in_use_[offset] = false;
  free_write_buffers_.push_back(buffer_id);
}

void RegisteredBufferPool::ReleaseRead(std::uint32_t offset,
                                       std::uint32_t units) noexcept {
  const auto& current = bycorf::ThisWorker();
  if (current.cross_core_ == cross_core_ && current.id_ == owner_worker_) {
    ReleaseReadLocal(offset, units);
    return;
  }
  if (current.cross_core_ == nullptr || current.cross_core_ != cross_core_)
    return;
  bycorf::PostNotification(
      cross_core_, owner_worker_,
      bycorf::RemoteNotification{
          .context_ = this,
          .value_ = (std::uint64_t{units} << 32) | offset,
          .run_fn_ = &RegisteredBufferPool::HandleRemoteReadRelease});
}

void RegisteredBufferPool::ReleaseReadLocal(std::uint32_t offset,
                                            std::uint32_t units) noexcept {
  read_allocator_.Free({.offset = offset, .units = units});
  WakeReadWaiters();
}

void RegisteredBufferPool::WakeReadWaiters() {
  while (!read_waiters_.empty()) {
    auto* waiter = read_waiters_.front();
    waiter->assigned_ =
        TakeReadBuffer(waiter->minimum_payload_bytes_, waiter->chunk_);
    if (!waiter->assigned_.valid()) break;
    read_waiters_.pop_front();
    // Reserve before scheduling and preserve FIFO even when a younger, smaller
    // request would fit. Release never resumes a coroutine inline.
    if (waiter->continuation_ != nullptr)
      bycorf::PostRequest(cross_core_, owner_worker_, waiter->continuation_);
    else
      worker_->Enqueue(waiter->awaiting_);
  }
}

void RegisteredBufferPool::ReleaseOutput(std::size_t output_id) noexcept {
  const auto& current = bycorf::ThisWorker();
  if (current.cross_core_ == cross_core_ && current.id_ == owner_worker_) {
    ReleaseOutputLocal(output_id);
    return;
  }
  if (current.cross_core_ == nullptr || current.cross_core_ != cross_core_)
    return;
  bycorf::PostNotification(
      cross_core_, owner_worker_,
      bycorf::RemoteNotification{
          .context_ = this,
          .value_ = output_id,
          .run_fn_ = &RegisteredBufferPool::HandleRemoteOutputRelease});
}

void RegisteredBufferPool::ReleaseOutputLocal(std::size_t output_id) noexcept {
  assert(output_id != 0 && output_id <= output_buffers_.size());
  const auto buffer = std::exchange(output_buffers_[output_id - 1], {});
  assert(buffer.data_ != nullptr);
  ::operator delete[](buffer.data_, std::align_val_t(options_.alignment_));
  buffer_memory_.Resize(buffer_memory_.bytes() - buffer.size_);
  free_output_buffers_.push_back(output_id);
}

void RegisteredBufferPool::HandleRemoteReadRelease(
    void* context, std::uint64_t value) noexcept {
  static_cast<RegisteredBufferPool*>(context)->ReleaseReadLocal(
      static_cast<std::uint32_t>(value),
      static_cast<std::uint32_t>(value >> 32));
}

void RegisteredBufferPool::HandleRemoteOutputRelease(
    void* context, std::uint64_t value) noexcept {
  static_cast<RegisteredBufferPool*>(context)->ReleaseOutputLocal(
      static_cast<std::size_t>(value));
}

}  // namespace lavik::storage
