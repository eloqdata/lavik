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

#pragma once

#include <array>
#include <cassert>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/sync.h"
#include "lavik/memory.h"
#include "lavik/tlsf_range_allocator.h"

namespace bycorf {
class CrossCore;
class Worker;
struct RemoteWork;
}  // namespace bycorf

namespace lavik::storage {

inline constexpr std::size_t kMiB = 1024 * 1024;
inline constexpr std::size_t kKiB = 1024;

struct RegisteredBufferPoolOptions {
  std::size_t registered_bytes_ = 64 * kMiB;
  // At least two; shutdown checkpoints borrow both after ordinary writes drain.
  std::size_t storage_write_buffer_count_ = 4;
  std::size_t write_buffer_bytes_ = 8 * kMiB;
  // Preferred chunk size, including framing margins, for streamed reads and
  // acquisitions without an explicit length. Explicit reads use exact aligned
  // ranges from the fixed arena; this is not their maximum allocation size.
  std::size_t read_slot_bytes_ = 32 * kKiB;
  std::size_t read_headroom_bytes_ = 4 * kKiB;
  std::size_t read_tailroom_bytes_ = 4 * kKiB;
  std::size_t alignment_ = 4 * kKiB;

  // Preferred chunk's I/O capacity; zero means it cannot fit framing.
  std::size_t read_payload_bytes() const noexcept {
    if (read_slot_bytes_ <= read_headroom_bytes_ ||
        read_slot_bytes_ - read_headroom_bytes_ <= read_tailroom_bytes_) {
      return 0;
    }
    return read_slot_bytes_ - read_headroom_bytes_ - read_tailroom_bytes_;
  }
};

class RegisteredBufferPool;

// A read or assembly buffer remains owned by its storage worker. Moving it to a
// connection worker only loans the memory; Reset()/destruction returns
// ownership to the owner through a one-way cross-core notification.
class ReadBufferLease {
 public:
  ReadBufferLease() = default;
  ReadBufferLease(const ReadBufferLease&) = delete;
  ReadBufferLease& operator=(const ReadBufferLease&) = delete;
  ReadBufferLease(ReadBufferLease&& other) noexcept;
  ReadBufferLease& operator=(ReadBufferLease&& other) noexcept;
  ~ReadBufferLease();

  bool valid() const noexcept { return data_ != nullptr; }
  // True only when the complete lease fits one registered region. A range
  // crossing a registration boundary uses ordinary I/O on io_uring.
  bool registered() const noexcept;
  // Pool ownership is immutable after Init; do not duplicate it in every
  // lease moved through the direct-GET coroutine chain.
  unsigned owner_worker() const noexcept;
  // Nonzero arena offset token, independent of write slots and fixed-I/O
  // indices. Zero identifies an ordinary-heap assembly output or empty lease.
  std::uint32_t buffer_id() const noexcept {
    return (release_token_ & kOutputTokenBit) == 0 ? release_token_ : 0;
  }

  // Entire leased range, including framing/alignment space. Its index names
  // the region containing its start; registered() checks the complete range.
  bycorf::FixedBuffer registered_buffer() const noexcept;

  // Aligned region intended as the destination of READ_FIXED.
  bycorf::FixedBuffer io_buffer() const noexcept;

  std::span<std::byte> bytes() const noexcept { return {data_, size_}; }
  std::size_t headroom_bytes() const noexcept { return headroom_bytes_; }
  std::size_t tailroom_bytes() const noexcept { return tailroom_bytes_; }

  void Reset() noexcept;

 private:
  friend class RegisteredBufferPool;
  ReadBufferLease(RegisteredBufferPool* pool, bycorf::FixedBuffer buffer,
                  std::size_t headroom_bytes, std::size_t tailroom_bytes,
                  std::uint32_t release_token) noexcept;

  // Arena leases encode offset + 1; size_ supplies the allocation length.
  // Heap outputs use a separate reusable ID space marked by the high bit.
  static constexpr std::uint32_t kOutputTokenBit = std::uint32_t{1} << 31;
  static constexpr std::uint32_t kReleaseTokenMask = kOutputTokenBit - 1;

  // Every lease is pool-owned. Buffer dimensions are bounded to uint32_t by
  // Init and the 1 GiB limit on the single string record carried by a direct
  // GET. Collections are stored as multiple records and do not use this reply
  // lease for their total size.
  RegisteredBufferPool* pool_ = nullptr;
  std::byte* data_ = nullptr;
  std::uint32_t size_ = 0;
  std::uint32_t headroom_bytes_ = 0;
  std::uint32_t tailroom_bytes_ = 0;
  std::uint32_t release_token_ = 0;
};

static_assert(sizeof(ReadBufferLease) == 32);

enum class MaintenanceReadKind { kDefrag, kTxCleaner, kTombRaider };

class RegisteredBufferPool {
 public:
  RegisteredBufferPool() = default;
  RegisteredBufferPool(const RegisteredBufferPool&) = delete;
  RegisteredBufferPool& operator=(const RegisteredBufferPool&) = delete;
  ~RegisteredBufferPool();

  // Try full registration first. If io_uring rejects it, use the finite
  // memlock allowance to retry a subset: borrowed device workspaces first,
  // then an aligned read-arena prefix, write slots, and maintenance images.
  // Pool sizes never shrink; unregistered buffers use ordinary I/O. SPDK does
  // not degrade. Reuses any prepared maintenance buffers, otherwise allocates
  // them here. priority_buffers remain caller-owned and charged; successful
  // Init updates their registration indices. Their allocations must outlive
  // worker I/O and ring teardown. Registration never allocates or charges a
  // second image. Any allocation failure aborts initialization.
  absl::Status Init(bycorf::Worker& worker,
                    const RegisteredBufferPoolOptions& options = {},
                    std::span<bycorf::FixedBuffer> priority_buffers = {});

  // Reserve and account the three 8 MiB maintenance read buffers before a
  // worker exists. Startup may borrow them synchronously; Init must use the
  // same owner/alignment and does not allocate or charge them again. Prepared
  // buffers remain owned until teardown, including if later Init fails.
  absl::Status PrepareMaintenanceBuffers(unsigned owner_worker,
                                         std::size_t alignment);

  bool initialized() const noexcept { return worker_ != nullptr; }
  // True when the ordinary arena and all write slots registered. A read can
  // still cross two registration regions: use lease.registered() for reads
  // and buffer_registered(id) for write slots when selecting fixed I/O.
  bool buffers_registered() const noexcept { return buffers_registered_; }
  // Registration is immutable after Init, including across lease handoffs.
  // Write-slot ids identify ownership independently of registration indices.
  [[gnu::always_inline]] bool buffer_registered(
      std::uint16_t buffer_id) const noexcept {
    if (buffer_id == 0) return false;
    if (buffer_id <= write_buffers_.size()) {
      return buffer_id <= registered_write_count_;
    }
    return false;
  }
  unsigned owner_worker() const noexcept { return owner_worker_; }
  // Check both index and address range against this worker's immutable
  // registration table. Accepts subranges of registered workspaces; a borrowed
  // image from another worker must not reuse that worker's fixed-buffer index.
  bool workspace_registered(bycorf::FixedBuffer buffer) const noexcept;
  // Each kind has at most one scanner on this worker. Its buffer stays owned
  // until teardown, including across I/O and relocation waits; callers must
  // preserve that single-scanner invariant. workspace_registered() selects
  // fixed or ordinary I/O. SPDK allocates these buffers from DMA memory.
  // Startup recovery borrows these workspaces before maintenance can start;
  // it must drain prefetch I/O before reuse or the transition to maintenance.
  bycorf::FixedBuffer maintenance_read_buffer(MaintenanceReadKind kind) const {
    assert(maintenance_alignment_ != 0);
    return maintenance_read_buffers_[static_cast<std::size_t>(kind)];
  }
  std::size_t read_buffer_count() const noexcept {
    return read_capacity_bytes() / options_.read_slot_bytes_;
  }
  // Nominal preferred-chunk counts for diagnostics, not a contiguous-fit
  // guarantee. Acquisition must use TryAcquireReadBuffer or an awaiter.
  std::size_t available_read_buffers() const noexcept {
    return available_read_bytes() / options_.read_slot_bytes_;
  }
  std::size_t read_capacity_bytes() const noexcept { return read_arena_.size_; }
  std::size_t available_read_bytes() const noexcept {
    return std::size_t{read_allocator_.free_units()} << read_unit_shift_;
  }
  // Only assembled results use dynamic memory; it is never submitted to disk.
  std::size_t output_buffer_count() const noexcept {
    return output_buffers_.size() - free_output_buffers_.size();
  }
  std::size_t write_buffer_count() const noexcept {
    return options_.storage_write_buffer_count_;
  }
  std::size_t available_write_buffers() const noexcept {
    return free_write_buffers_.size();
  }
  bool has_write_waiters() const noexcept { return !write_waiters_.empty(); }
  bycorf::FixedBuffer write_buffer(std::uint16_t buffer_id = 0) const noexcept {
    if (buffer_id == 0) {
      return {};
    }
    const auto id = static_cast<std::size_t>(buffer_id);
    return id <= write_buffers_.size() && id != 0 ? write_buffers_[id - 1]
                                                  : bycorf::FixedBuffer{};
  }

  // Never takes a slot already handed to a queued writer, including before
  // that writer resumes. All write-pool borrowers obey the same ordering.
  bool TryAcquireWriteBuffer(std::uint16_t* buffer_id) noexcept;
  class AcquireWriteAwaiter {
   public:
    explicit AcquireWriteAwaiter(RegisteredBufferPool* pool) : pool_(pool) {}
    bool await_ready() noexcept;
    bool await_suspend(std::coroutine_handle<> awaiting);
    absl::StatusOr<std::uint16_t> await_resume();

   private:
    friend class RegisteredBufferPool;
    RegisteredBufferPool* pool_;
    std::coroutine_handle<> awaiting_{};
    std::uint16_t assigned_buffer_id_ = 0;
  };

  // Owner-local FIFO acquisition from the fixed pool, shared by foreground
  // and maintenance writers. Release hands ownership directly to the oldest
  // waiter; callers must not hold locks needed by flush while awaiting it.
  // The returned slot must be returned with ReleaseWriteBuffer(). A stopped
  // writer reports FailedPrecondition instead of waiting on failed flushes.
  AcquireWriteAwaiter AcquireWriteBuffer() noexcept {
    return AcquireWriteAwaiter(this);
  }
  // Irreversibly fail owner-local queued/future acquisitions without releasing
  // staging memory that failed I/O or outstanding readers can still borrow.
  void FailWriteWaiters() noexcept;
  void ReleaseWriteBuffer(std::uint16_t buffer_id) noexcept;

  class AcquireReadAwaiter {
   public:
    AcquireReadAwaiter(RegisteredBufferPool* pool,
                       std::size_t minimum_payload_bytes, bool chunk = false)
        : pool_(pool),
          minimum_payload_bytes_(minimum_payload_bytes),
          chunk_(chunk) {}

    bool await_ready();
    bool await_suspend(std::coroutine_handle<> awaiting);
    absl::StatusOr<ReadBufferLease> await_resume();

    // Queue an owner-local continuation when a range is handed off. This uses
    // the worker's scheduled work queue (including its task-class budget),
    // never runs inline in Release(), and sends no cross-worker reply. The
    // caller supplies run_fn_/task_class_ and keeps both objects alive until
    // that callback consumes the assigned lease through await_resume().
    bool SuspendWith(bycorf::RemoteWork& continuation);

   private:
    friend class RegisteredBufferPool;

    RegisteredBufferPool* pool_ = nullptr;
    std::size_t minimum_payload_bytes_ = 0;
    std::coroutine_handle<> awaiting_{};
    bycorf::RemoteWork* continuation_ = nullptr;
    // Own the reservation before scheduling the waiter: newcomers cannot steal
    // it, and repeated readiness checks cannot acquire a second range.
    ReadBufferLease assigned_;
    bool chunk_ = false;
  };

  // Exact aligned acquisition, FIFO on exhaustion/fragmentation. A request
  // larger than the arena fails immediately. Never allocates more DMA memory.
  // Callers must not retain resources needed by earlier waiters while waiting.
  AcquireReadAwaiter AcquireReadBuffer(
      std::size_t minimum_payload_bytes = 0) noexcept {
    return AcquireReadAwaiter(this, minimum_payload_bytes);
  }

  // Nonblocking exact acquisition for full-record reads and batch assembly.
  // ResourceExhausted means the caller should drain its batch or stream chunks.
  absl::StatusOr<ReadBufferLease> TryAcquireReadBuffer(
      std::size_t minimum_payload_bytes);

  // Prefer a normal chunk, reducing its size under fragmentation/pressure.
  // Wait FIFO only when even one aligned payload page plus framing cannot fit.
  AcquireReadAwaiter AcquireReadChunk() noexcept {
    return AcquireReadAwaiter(this, 0, true);
  }

  // Allocate ordinary heap memory for assembling a value, even on SPDK.
  // Never consumes arena capacity and must not be submitted to storage I/O.
  // Preserves reply framing space, retained accounting and owner-local release
  // through the same lease type as disk reads. Allocation failure is returned.
  absl::StatusOr<ReadBufferLease> AllocateReadOutputBuffer(
      std::size_t minimum_payload_bytes);

 private:
  friend class ReadBufferLease;

  ReadBufferLease TakeReadBuffer(std::size_t payload_bytes, bool chunk);
  std::uint32_t ReadUnits(std::size_t payload_bytes) const noexcept;
  void ReleaseRead(std::uint32_t offset, std::uint32_t units) noexcept;
  void ReleaseReadLocal(std::uint32_t offset, std::uint32_t units) noexcept;
  void WakeReadWaiters();
  void ReleaseOutput(std::size_t output_id) noexcept;
  void ReleaseOutputLocal(std::size_t output_id) noexcept;
  std::optional<std::uint16_t> TakeWriteBuffer();
  void ReleaseWriteBufferLocal(std::uint16_t buffer_id) noexcept;
  static void HandleRemoteReadRelease(void* context,
                                      std::uint64_t value) noexcept;
  static void HandleRemoteOutputRelease(void* context,
                                        std::uint64_t value) noexcept;
  bool IsWriteBufferId(std::uint16_t buffer_id) const noexcept;

  bycorf::Worker* worker_ = nullptr;
  bycorf::CrossCore* cross_core_ = nullptr;
  unsigned owner_worker_ = 0;
  bool buffers_registered_ = false;
  // Linux limits registered regions to 1 GiB. The arena remains contiguous;
  // leases crossing a region boundary use plain I/O rather than READ_FIXED.
  static constexpr std::size_t kReadRegionBytes = 1024 * kMiB;
  std::size_t registered_read_bytes_ = 0;
  std::size_t registered_write_count_ = 0;
  std::vector<bycorf::FixedBuffer> registered_regions_;
  RegisteredBufferPoolOptions options_{};
  // Charge owned capacity for both aligned and SPDK DMA allocations. Pool
  // borrowing does not change ownership; teardown may run off the owner worker.
  RetainedMemoryCharge buffer_memory_;
  RetainedMemoryCharge maintenance_memory_;
  std::size_t maintenance_alignment_ = 0;
  std::array<bycorf::FixedBuffer, 3> maintenance_read_buffers_{};
  std::vector<bycorf::FixedBuffer> write_buffers_;
  std::vector<std::uint16_t> free_write_buffers_;
  std::vector<bool> write_buffer_in_use_;
  std::deque<AcquireWriteAwaiter*> write_waiters_;
  bool write_acquisition_failed_ = false;
  bycorf::FixedBuffer read_arena_{};
  unsigned read_unit_shift_ = 12;
  std::vector<TlsfRangeAllocator::Metadata> read_metadata_;
  TlsfRangeAllocator read_allocator_;
  std::deque<AcquireReadAwaiter*> read_waiters_;
  // Only ordinary-heap outputs are dynamic. Vacant IDs own no storage and are
  // reused after local or remote return; the backing arena never grows.
  std::vector<bycorf::FixedBuffer> output_buffers_;
  std::vector<std::size_t> free_output_buffers_;
};

// Keep address and registration checks in the read coroutine: GCC can
// otherwise outline even these small accessors under Release LTO.
[[gnu::always_inline]] inline bool ReadBufferLease::registered()
    const noexcept {
  const auto id = buffer_id();
  if (pool_ == nullptr || id == 0) return false;
  const auto offset = std::size_t{id - 1} << pool_->read_unit_shift_;
  const auto end = offset + size_;
  // Registration is an immutable arena prefix; both endpoints must belong to
  // the same kernel region. No descriptor lookup on a connection worker.
  return end <= pool_->registered_read_bytes_ &&
         offset / RegisteredBufferPool::kReadRegionBytes ==
             (end - 1) / RegisteredBufferPool::kReadRegionBytes;
}

[[gnu::always_inline]] inline bycorf::FixedBuffer
ReadBufferLease::registered_buffer() const noexcept {
  const std::uint32_t id = buffer_id();
  const std::uint16_t index =
      id == 0 ? 0
              : static_cast<std::uint16_t>(
                    pool_->read_arena_.index_ +
                    (std::size_t{id - 1} << pool_->read_unit_shift_) /
                        RegisteredBufferPool::kReadRegionBytes);
  return {.data_ = data_, .size_ = size_, .index_ = index};
}

[[gnu::always_inline]] inline bycorf::FixedBuffer ReadBufferLease::io_buffer()
    const noexcept {
  if (!valid() || size_ < headroom_bytes_ + tailroom_bytes_) {
    return {};
  }
  return bycorf::FixedBuffer{
      .data_ = data_ + headroom_bytes_,
      .size_ = size_ - headroom_bytes_ - tailroom_bytes_,
      .index_ = registered_buffer().index_,
  };
}

inline unsigned ReadBufferLease::owner_worker() const noexcept {
  return pool_ != nullptr ? pool_->owner_worker_ : 0;
}

}  // namespace lavik::storage
