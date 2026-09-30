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

#include <cstdint>
#include <memory>
#include <utility>

#include "absl/container/flat_hash_map.h"

namespace lavik::storage {

// Owner-local membership for one allocation of a Tx block. Keep all txids for
// durable decision accounting, including writers whose leases have expired.
// This class never extends a writer's lifetime and performs no synchronization;
// storage callers hold the owning worker's store-state guard.
class TxBlockLeases {
 public:
  // Any membership replacement invalidates both witnesses. In particular, a
  // previously settled block may acquire another receipt during recovery or
  // append; a released weak_ptr itself cannot become live again.
  void Record(std::uint64_t txid, std::weak_ptr<void> lease) {
    entries_.insert_or_assign(txid, std::move(lease));
    settled_ = false;
    writer_.reset();
  }

  // Recheck a live witness in O(1). Scan membership only when that witness
  // expires, and remember a complete proof of expiration until Record/reset.
  bool HasLiveWriter() {
    if (settled_) return false;
    if (!writer_.expired()) return true;
    for (const auto& [txid, lease] : entries_) {
      (void)txid;
      if (!lease.expired()) {
        writer_ = lease;
        return true;
      }
    }
    settled_ = true;
    return false;
  }

  const auto& entries() const noexcept { return entries_; }

 private:
  absl::flat_hash_map<std::uint64_t, std::weak_ptr<void>> entries_;
  std::weak_ptr<void> writer_;
  bool settled_ = false;
};

}  // namespace lavik::storage
