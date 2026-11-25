/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <sys/queue.h>

#include <algorithm>
#include <chrono>
#include <utility>

#include "lavik/command_args.h"
#include "lavik/memory.h"

namespace lavik {

// Only the embedded connection slot can retain an allocation across commands.
// Independent pipeline and MULTI storage never enters the maintenance queue.
struct ConnectionArgsStorage : CommandArgsStorage {
 private:
  friend class RespBufferBudget;
  TAILQ_ENTRY(ConnectionArgsStorage) review_link_;
  std::chrono::steady_clock::time_point review_at_;
  std::size_t peak_bytes_ = 0;
};

// Each Data worker owns a deadline-ordered queue of its admitted allocations.
// The queue links live in the connections so disconnect can unlink in O(1).
class alignas(64) RespBufferBudget {
 public:
  using Clock = std::chrono::steady_clock;
  static constexpr auto kWindow = std::chrono::seconds(1);
  static constexpr std::size_t kReviewBatch = 64;

  RespBufferBudget() noexcept { TAILQ_INIT(&queue_); }
  RespBufferBudget(const RespBufferBudget&) = delete;
  RespBufferBudget& operator=(const RespBufferBudget&) = delete;

  // Sample the actual packed bytes after a socket read round. Return true
  // only when the caller needs to start the worker's maintenance coroutine.
  [[nodiscard]] bool Admit(ConnectionArgsStorage& slot) noexcept {
    const auto capacity = slot.bytes.capacity();
    if (capacity == slot.retained_byte_capacity_) {
      if (capacity > CommandArgs::kInlineBytes)
        slot.peak_bytes_ = std::max(slot.peak_bytes_, slot.bytes.size());
      return false;
    }
    Release(slot);
    // A still-borrowed buffer marked for trimming must not regain its old
    // credit just because a small command or fragment is seen again.
    if (capacity <= CommandArgs::kInlineBytes ||
        slot.bytes.size() <= capacity / 4)
      return false;
    const auto bytes = AllocatorUsableSizeForRequest(capacity);
    auto reservation = TryReserveMemory(bytes);
    if (!reservation) {
      admission_failed_ = !TAILQ_EMPTY(&queue_);
      return false;
    }
    reservation->Commit(bytes);
    slot.retained_byte_capacity_ = capacity;
    slot.peak_bytes_ = slot.bytes.size();
    Schedule(slot, Clock::now());
    return !std::exchange(maintenance_running_, true);
  }

  // Revoke retention only. A borrowed handle still owns every argument until
  // its release, which then discards the overflow through its normal path.
  void Release(ConnectionArgsStorage& slot) noexcept {
    if (slot.retained_byte_capacity_ == CommandArgs::kInlineBytes) return;
    TAILQ_REMOVE(&queue_, &slot, review_link_);
    const auto bytes =
        AllocatorUsableSizeForRequest(slot.retained_byte_capacity_);
    ReleaseRetainedMemory(CurrentMemoryAccountingShard(), bytes);
    slot.retained_byte_capacity_ = CommandArgs::kInlineBytes;
  }

  // A zero delay ends maintenance until the next admission starts it again.
  // If a timer fails, drain outstanding cache credits in background batches.
  // Bound each pass, including when many connections went idle together.
  Clock::duration Review(Clock::time_point now, bool drain = false) noexcept {
    const bool due =
        !TAILQ_EMPTY(&queue_) && TAILQ_FIRST(&queue_)->review_at_ <= now;
    const bool pressure =
        drain || (due && (std::exchange(admission_failed_, false) ||
                          WouldExceedMemoryLimit(0)));
    for (std::size_t i = 0; i < kReviewBatch; ++i) {
      auto* slot = TAILQ_FIRST(&queue_);
      if (!slot || (!drain && slot->review_at_ > now)) break;
      if (pressure || slot->peak_bytes_ <= slot->retained_byte_capacity_ / 4) {
        Release(*slot);
        // Drop the empty allocation directly. A smaller active workload grows
        // to its new demand on the next request without copying old contents.
        if (!slot->borrowed()) slot->bytes.clear();
      } else {
        slot->peak_bytes_ = 0;
        TAILQ_REMOVE(&queue_, slot, review_link_);
        Schedule(*slot, now);
      }
    }
    if (auto* next = TAILQ_FIRST(&queue_)) {
      // Leave room for foreground work between overdue batches.
      return std::max<Clock::duration>(std::chrono::milliseconds(10),
                                       next->review_at_ - now);
    }
    maintenance_running_ = false;
    admission_failed_ = false;
    return Clock::duration::zero();
  }

 private:
  void Schedule(ConnectionArgsStorage& slot, Clock::time_point now) noexcept {
    slot.review_at_ = now + kWindow;
    TAILQ_INSERT_TAIL(&queue_, &slot, review_link_);
  }
  TAILQ_HEAD(, ConnectionArgsStorage) queue_;
  bool maintenance_running_ = false;
  bool admission_failed_ = false;
};

}  // namespace lavik
