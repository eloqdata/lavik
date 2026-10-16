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

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/runtime/task.h"
#include "bycorf/runtime/worker.h"
#include "lavik/memory.h"

namespace lavik::storage {

// These queues belong to one request on its key owner. Every loader acquires
// against its refreshed physical owner, including relocation retries. Release
// only after decoding and releasing the page lease; waking never resumes
// inline.
class GroupReadLimiter {
 public:
  static constexpr std::size_t kWidth = 16;
  struct Slot;

 private:
  struct Owner {
    std::size_t active_ = 0;
    Slot* first_ = nullptr;
    Slot* last_ = nullptr;
  };

 public:
  class Permit {
   public:
    Permit() = default;
    Permit(GroupReadLimiter* window, unsigned owner)
        : window_(window), owner_(owner) {}
    Permit(const Permit&) = delete;
    Permit& operator=(const Permit&) = delete;
    Permit(Permit&& other) noexcept
        : window_(std::exchange(other.window_, nullptr)),
          owner_(other.owner_) {}
    Permit& operator=(Permit&& other) noexcept {
      if (window_) window_->Release(owner_);
      window_ = std::exchange(other.window_, nullptr);
      owner_ = other.owner_;
      return *this;
    }
    ~Permit() {
      if (window_) window_->Release(owner_);
    }

   private:
    GroupReadLimiter* window_ = nullptr;
    unsigned owner_ = 0;
  };

  struct Slot {
    Slot(GroupReadLimiter& window, unsigned owner)
        : window_(window), owner_(owner) {}
    Slot(const Slot&) = delete;
    Slot& operator=(const Slot&) = delete;
    ~Slot() {
      if (queued_) window_.Unlink(*this);
      // Shutdown may destroy a granted waiter before its queued resume runs.
      if (acquired_) window_.Release(owner_);
    }
    bool await_ready() noexcept {
      auto& owner = window_.owners_[owner_];
      if (owner.active_ == kWidth) return false;
      ++owner.active_;
      acquired_ = true;
      return true;
    }
    void await_suspend(std::coroutine_handle<> awaiting) noexcept {
      awaiting_ = awaiting;
      auto& owner = window_.owners_[owner_];
      previous_ = owner.last_;
      if (previous_)
        previous_->next_ = this;
      else
        owner.first_ = this;
      owner.last_ = this;
      queued_ = true;
    }
    Permit await_resume() noexcept {
      assert(acquired_);
      acquired_ = false;
      return Permit(&window_, owner_);
    }

    GroupReadLimiter& window_;
    unsigned owner_;
    Slot* previous_ = nullptr;
    Slot* next_ = nullptr;
    std::coroutine_handle<> awaiting_{};
    bool queued_ = false;
    bool acquired_ = false;
  };

  explicit GroupReadLimiter(bycorf::Worker* worker, unsigned owners)
      : worker_(worker), owners_(owners) {}
  GroupReadLimiter(const GroupReadLimiter&) = delete;
  GroupReadLimiter& operator=(const GroupReadLimiter&) = delete;

  static std::size_t StorageBytes(unsigned owners) {
    return AllocatorUsableSizeForRequest(owners * sizeof(Owner));
  }
  Slot Acquire(unsigned owner) { return Slot(*this, owner); }
  // Worker shutdown quiesces I/O before reclaiming child frames. Keep waiter
  // queues alive while those frames unlink, without waking additional work.
  void Stop() noexcept { stopping_ = true; }

 private:
  void Unlink(Slot& slot) noexcept {
    auto& owner = owners_[slot.owner_];
    if (slot.previous_)
      slot.previous_->next_ = slot.next_;
    else
      owner.first_ = slot.next_;
    if (slot.next_)
      slot.next_->previous_ = slot.previous_;
    else
      owner.last_ = slot.previous_;
    slot.queued_ = false;
  }

  void Release(unsigned id) noexcept {
    auto& owner = owners_[id];
    assert(owner.active_ != 0);
    if (!stopping_ && owner.first_) {
      Slot* next = owner.first_;
      Unlink(*next);
      next->acquired_ = true;
      worker_->Enqueue(next->awaiting_);
    } else {
      --owner.active_;
    }
  }

  bycorf::Worker* worker_;
  std::vector<Owner> owners_;
  bool stopping_ = false;
};

// Own every started task until Join, including errors and partial starts.
// Returned values must own decoded data, not fixed read-buffer leases:
// retaining those leases until Join can deadlock a smaller pool. Declare the
// window after all borrowed output/key/selection storage and its admission.
template <typename T>
class GroupReadWindow {
 public:
  using Read = bycorf::Task<absl::StatusOr<T>>;

  static std::size_t StorageBytes(unsigned owners, std::size_t width) {
    const auto limit = std::numeric_limits<std::size_t>::max();
    if (width > limit / sizeof(Entry)) return limit;
    const auto limiter = GroupReadLimiter::StorageBytes(owners);
    const auto tasks = AllocatorUsableSizeForRequest(width * sizeof(Entry));
    return tasks > limit - limiter ? limit : limiter + tasks;
  }
  GroupReadWindow(bycorf::Worker* worker, unsigned owners, std::size_t width)
      : worker_(worker), limiter_(worker, owners) {
    reads_.reserve(width);
  }
  GroupReadWindow(const GroupReadWindow&) = delete;
  GroupReadWindow& operator=(const GroupReadWindow&) = delete;
  ~GroupReadWindow() {
    limiter_.Stop();
    reads_.clear();
  }

  GroupReadLimiter* limiter() noexcept { return &limiter_; }
  std::size_t size() const noexcept { return reads_.size(); }

  void Add(Read read, std::size_t ordinal) {
    assert(reads_.size() < reads_.capacity());
    read.SetCompletionCallback(
        this, [](void* context, std::coroutine_handle<>) noexcept {
          auto& window = *static_cast<GroupReadWindow*>(context);
          assert(window.pending_ != 0);
          if (--window.pending_ == 0 && window.awaiting_)
            window.worker_->Enqueue(std::exchange(window.awaiting_, {}));
        });
    const auto handle = std::move(read).ReleaseHandle();
    reads_.push_back({Read(handle), ordinal});
    ++pending_;
    // Explicitly queued children bypass Task::await_suspend's inheritance.
    if (bycorf::CurrentTaskClass() == bycorf::TaskClass::kBackground)
      worker_->RegisterBackground(handle);
    worker_->Enqueue(handle);
  }

  auto Join() {
    struct Awaiter {
      GroupReadWindow* window_;
      bool await_ready() const noexcept { return window_->pending_ == 0; }
      void await_suspend(std::coroutine_handle<> awaiting) const noexcept {
        window_->awaiting_ = awaiting;
      }
      void await_resume() const noexcept {}
    };
    return Awaiter{this};
  }

  // The original selection ordinal lets consumers assemble logical order
  // independently of owner buckets and completion order. Destroy every result
  // on error as well, after all children have completed.
  template <typename Consumer>
  absl::Status Collect(Consumer&& consume) {
    assert(pending_ == 0);
    absl::Status status;
    for (auto& entry : reads_) {
      auto result = std::move(entry.read_).TakeResult();
      if (!status.ok()) continue;
      status = result.ok() ? consume(entry.ordinal_, std::move(*result))
                           : result.status();
    }
    reads_.clear();
    return status;
  }

 private:
  struct Entry {
    Read read_;
    std::size_t ordinal_;
  };
  bycorf::Worker* worker_;
  GroupReadLimiter limiter_;
  std::vector<Entry> reads_;
  std::size_t pending_ = 0;
  std::coroutine_handle<> awaiting_{};
};

// Initial owner hints organize waves; the limiter separately enforces the
// actual owner cap after GC refresh. Admission for this optional plan and the
// extra concurrency is fallible so a caller can retain its serial memory floor.
class GroupReadPlan {
 public:
  // Reserve before allocating the plan or an optional caller-owned selection
  // table. That table must be destroyed before this plan releases admission.
  // A single selected group keeps the caller's direct, unqueued read path.
  static std::optional<GroupReadPlan> TryCreate(
      unsigned owners, std::size_t count,
      std::size_t extra_metadata_bytes = 0) {
    constexpr auto limit = std::numeric_limits<std::size_t>::max();
    if (owners == 0 || count <= 1 || count > limit / sizeof(std::size_t))
      return std::nullopt;
    const auto buckets = AllocatorUsableSizeForRequest(owners * sizeof(Bucket));
    const auto links =
        AllocatorUsableSizeForRequest(count * sizeof(std::size_t));
    if (buckets > limit - links ||
        extra_metadata_bytes > limit - buckets - links)
      return std::nullopt;
    auto admission = TryReserveMemory(buckets + links + extra_metadata_bytes);
    if (!admission) return std::nullopt;
    return GroupReadPlan(std::move(*admission), owners, count);
  }

  void Add(unsigned owner, std::size_t ordinal) {
    assert(owner < owners_.size() && ordinal < next_.size());
    auto& bucket = owners_[owner];
    if (bucket.count_ < GroupReadLimiter::kWidth) ++width_;
    if (bucket.count_++ == 0)
      bucket.first_ = ordinal;
    else
      next_[bucket.last_] = ordinal;
    bucket.last_ = ordinal;
    ++remaining_;
  }

  template <typename T>
  bool ReserveWindow(std::size_t extra_scratch_per_read) {
    assert(width_ > 1);
    constexpr auto limit = std::numeric_limits<std::size_t>::max();
    const auto metadata =
        GroupReadWindow<T>::StorageBytes(owner_count(), width_);
    if (metadata == limit ||
        extra_scratch_per_read > (limit - metadata) / (width_ - 1))
      return false;
    window_admission_ =
        TryReserveMemory(metadata + extra_scratch_per_read * (width_ - 1));
    return window_admission_.has_value();
  }

  std::size_t width() const noexcept { return width_; }
  unsigned owner_count() const noexcept { return owners_.size(); }
  bool empty() const noexcept { return remaining_ == 0; }
  std::optional<std::size_t> Pop(unsigned owner) {
    auto& bucket = owners_[owner];
    if (bucket.first_ == kNone) return std::nullopt;
    const auto ordinal = bucket.first_;
    bucket.first_ = next_[ordinal];
    --remaining_;
    return ordinal;
  }

 private:
  static constexpr auto kNone = std::numeric_limits<std::size_t>::max();
  struct Bucket {
    std::size_t first_ = kNone;
    std::size_t last_ = kNone;
    std::size_t count_ = 0;
  };
  GroupReadPlan(MemoryReservation admission, unsigned owners, std::size_t count)
      : admission_(std::move(admission)),
        owners_(owners),
        next_(count, kNone) {}

  // Reservations outlive their owned allocations and caller-owned selections.
  MemoryReservation admission_;
  std::optional<MemoryReservation> window_admission_;
  std::vector<Bucket> owners_;
  std::vector<std::size_t> next_;
  std::size_t width_ = 0;
  std::size_t remaining_ = 0;
};

}  // namespace lavik::storage
