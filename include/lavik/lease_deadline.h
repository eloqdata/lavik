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

#include <atomic>
#include <chrono>
#include <cstdint>

namespace lavik {

// One finite CLOCK_BOOTTIME lease shared by request admission, expiration,
// and replication admission. Identity belongs to the enclosing authority epoch;
// a revoked or expired epoch must be drained and replaced, never reactivated.
// Only the control worker renews. Consumers can revoke concurrently.
class LeaseDeadline {
 public:
  explicit LeaseDeadline(std::chrono::nanoseconds deadline) noexcept
      : deadline_ns_(deadline.count() > 0 ? deadline.count() : 0) {}

  std::chrono::nanoseconds deadline() const noexcept {
    const auto state = deadline_ns_.load(std::memory_order_acquire);
    return std::chrono::nanoseconds(state < 0 ? -state : state);
  }
  bool valid_at(std::chrono::nanoseconds now) const noexcept {
    auto current = deadline_ns_.load(std::memory_order_acquire);
    while (current > 0 && current <= now.count()) {
      // The first observer makes expiry terminal. A renewer descheduled after
      // sampling an earlier clock cannot revive a capability already rejected
      // by a request, TTL mutation, or native source handshake.
      if (deadline_ns_.compare_exchange_weak(current, -current,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire))
        return false;
    }
    return current > 0 && current > now.count();
  }
  // No allocation, locks, or worker submission. CAS makes revocation terminal
  // even if it races renewal. The caller validates the unchanged authority
  // identity and samples CLOCK_BOOTTIME immediately before this operation.
  bool Renew(std::chrono::nanoseconds now,
             std::chrono::nanoseconds deadline) noexcept {
    if (!valid_at(now)) return false;
    auto current = deadline_ns_.load(std::memory_order_acquire);
    return current > 0 && current > now.count() && deadline > now &&
           deadline_ns_.compare_exchange_strong(current, deadline.count(),
                                                std::memory_order_acq_rel,
                                                std::memory_order_acquire);
  }
  void Revoke() noexcept { deadline_ns_.store(0, std::memory_order_release); }

 private:
  static_assert(std::atomic<std::int64_t>::is_always_lock_free);
  // Positive: live deadline; negative: observed expiry, retaining the exact
  // cut for timer cleanup/metrics; zero: explicit revocation. Ordinary reads
  // only load. The sign change and renewal compete on the same atomic word.
  mutable std::atomic<std::int64_t> deadline_ns_;
};

}  // namespace lavik
