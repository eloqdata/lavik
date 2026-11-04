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
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "lavik/tx/fingerprint.h"

namespace lavik::tx {

// VLL-style non-blocking lock table, one per (worker, logical DB). All methods
// run on the owning worker thread in non-suspending sections — no atomics.
//
// Two counter layers per fingerprint:
//  - intents: recorded from scheduling until the transaction's release hop.
//    AcquireIntent never blocks; it records the intent and reports whether it
//    was granted (sole/compatible owner). Intents arbitrate scheduling: a
//    fully-granted transaction may run immediately, anything else queues.
//  - holds: marked only while a callback is actually executing (possibly
//    suspended on disk I/O). The queue head may start only when CanHold passes
//    for its whole key set, i.e. every conflicting suspended runner drained.
//    Traditional VLL has no hold layer because callbacks there
//    run to completion; lavik callbacks suspend, so "currently executing"
//    must be visible to the scheduler. Invariant: holds ⊆ intents.
//
// IS/IX are lock modes, distinct from the scheduling-intent counter layer.
// Compatible holders still record intents: an earlier S or X waiter fences
// later IX arrivals, and an earlier X fences later IS arrivals as well.
class LockTable {
 public:
  // Single-key acquisitions dominate ordinary command traffic. Combining the
  // intent and hold transition keeps the uncontended path to one hash-table
  // probe while preserving the same published state: a failed acquisition
  // retains only its intent for queue ordering, and a successful one owns
  // both the intent and compatible hold before it can suspend.
  bool AcquireIntentAndHoldIfGranted(LockFp fp, LockMode mode) {
    IntentLock& lock = map_[fp];
    if (!lock.AcquireIntent(mode)) return false;
    lock.AcquireHold(mode);
    return true;
  }

  // Releases the two layers acquired for one running key with one lookup.
  // Queued single-key work reaches this method only after Poll has installed
  // its hold, so both counters are present regardless of how it was granted.
  void ReleaseHoldAndIntent(LockFp fp, LockMode mode) {
    auto it = map_.find(fp);
    assert(it != map_.end());
    IntentLock& lock = it->second;
    lock.ReleaseHold(mode);
    lock.ReleaseIntent(mode);
    if (lock.IsFree()) {
      map_.erase(it);
    }
  }

  // Records the intent unconditionally. IS admits every mode except X; S and
  // IX conflict with each other, and X requires sole ownership. Conflicting
  // queued intents prevent barging.
  bool AcquireIntent(LockFp fp, LockMode mode) {
    return map_[fp].AcquireIntent(mode);
  }

  void ReleaseIntent(LockFp fp, LockMode mode) {
    auto it = map_.find(fp);
    assert(it != map_.end());
    IntentLock& lock = it->second;
    lock.ReleaseIntent(mode);
    if (lock.IsFree()) {
      map_.erase(it);
    }
  }

  bool CanHold(LockFp fp, LockMode mode) const {
    auto it = map_.find(fp);
    if (it == map_.end()) {
      return true;
    }
    return it->second.CanHold(mode);
  }

  void AcquireHold(LockFp fp, LockMode mode) { map_[fp].AcquireHold(mode); }

  void ReleaseHold(LockFp fp, LockMode mode) {
    auto it = map_.find(fp);
    assert(it != map_.end());
    it->second.ReleaseHold(mode);
    // holds ⊆ intents: a held entry always has intents, so IsFree can only
    // trigger from ReleaseIntent.
  }

  std::size_t size() const noexcept { return map_.size(); }

 private:
  struct IntentLock {
    // LockMode values are contiguous indices. Keep counter updates independent
    // of compatibility checks so releasing an intent needs no mode dispatch.
    std::array<std::uint32_t, 4> intents_{};
    // S and IX cannot coexist, and X is exclusive. Pack those holders' mode
    // with their count; IS needs a separate count because it can coexist with
    // either S or IX. This keeps the state at 24 bytes rather than eight
    // independent counters. The packed count allows 2^30-1 S or IX holders;
    // X always has exactly one holder.
    static constexpr std::uint32_t kCountMask = (std::uint32_t{1} << 30) - 1;
    std::uint32_t held_ = 0;
    std::uint32_t intent_shared_held_ = 0;

    static constexpr std::uint32_t ModeBits(LockMode mode) noexcept {
      return static_cast<std::uint32_t>(mode) << 30;
    }

    [[gnu::always_inline]] std::uint32_t& IntentCount(LockMode mode) noexcept {
      const auto index = static_cast<std::size_t>(mode);
      assert(index < intents_.size());
      return intents_[index];
    }

    bool AcquireIntent(LockMode mode) {
      // Compatibility already dispatches on mode. Keep increments at fixed
      // offsets in these branches to avoid indexed addressing on acquisition.
      switch (mode) {
        case LockMode::kShared:
          ++IntentCount(LockMode::kShared);
          return IntentCount(LockMode::kExclusive) == 0 &&
                 IntentCount(LockMode::kIntentExclusive) == 0;
        case LockMode::kExclusive:
          ++IntentCount(LockMode::kExclusive);
          return IntentCount(LockMode::kShared) == 0 &&
                 IntentCount(LockMode::kIntentExclusive) == 0 &&
                 IntentCount(LockMode::kIntentShared) == 0 &&
                 IntentCount(LockMode::kExclusive) == 1;
        case LockMode::kIntentExclusive:
          ++IntentCount(LockMode::kIntentExclusive);
          return IntentCount(LockMode::kShared) == 0 &&
                 IntentCount(LockMode::kExclusive) == 0;
        case LockMode::kIntentShared:
          ++IntentCount(LockMode::kIntentShared);
          return IntentCount(LockMode::kExclusive) == 0;
      }
      assert(false && "invalid lock mode");
      std::unreachable();
    }

    void ReleaseIntent(LockMode mode) {
      auto& count = IntentCount(mode);
      assert(count > 0);
      --count;
    }

    bool CanHold(LockMode mode) const noexcept {
      if (mode == LockMode::kExclusive)
        return held_ == 0 && intent_shared_held_ == 0;
      if (mode == LockMode::kIntentShared)
        return (held_ & ~kCountMask) != ModeBits(LockMode::kExclusive);
      return held_ == 0 || (held_ & ~kCountMask) == ModeBits(mode);
    }

    void AcquireHold(LockMode mode) {
      assert(CanHold(mode));
      if (mode == LockMode::kIntentShared) {
        ++intent_shared_held_;
        return;
      }
      assert((held_ & kCountMask) < kCountMask);
      if (held_ == 0) held_ = ModeBits(mode);
      ++held_;
    }

    void ReleaseHold(LockMode mode) {
      if (mode == LockMode::kIntentShared) {
        assert(intent_shared_held_ > 0);
        --intent_shared_held_;
        return;
      }
      assert((held_ & kCountMask) != 0);
      assert((held_ & ~kCountMask) == ModeBits(mode));
      --held_;
      if ((held_ & kCountMask) == 0) held_ = 0;
    }

    bool IsFree() const noexcept {
      for (const auto count : intents_) {
        if (count != 0) return false;
      }
      return held_ == 0 && intent_shared_held_ == 0;
    }
  };
  static_assert(sizeof(IntentLock) == 24);

  absl::flat_hash_map<LockFp, IntentLock, LockFpIdentityHash> map_;
};

}  // namespace lavik::tx
