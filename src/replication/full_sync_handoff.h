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
#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik::detail {

// One FULL session/flow owns this ledger on its worker. Entries cover all
// physical partitions assigned to that flow, including empty/non-member ones.
// Only a matching ACK reduces unfinished; sending the last partition does not
// establish completeness. Nothing survives session replacement.
class FullSyncHandoffProgress {
 public:
  FullSyncHandoffProgress(std::size_t partition_count, unsigned flow,
                          unsigned flow_count)
      : flow_(flow),
        flow_count_(flow_count),
        partition_count_(partition_count) {
    assert(flow_count != 0 && flow < flow_count);
    entries_.resize(flow < partition_count
                        ? (partition_count - 1 - flow) / flow_count + 1
                        : 0);
    unfinished_ = entries_.size();
  }

  // Register before the first suspending write: an ACK can arrive before that
  // write's continuation resumes. A failed write terminates the session.
  absl::Status Begin(std::uint16_t partition, std::uint64_t sequence) {
    Entry* entry = Find(partition);
    if (entry == nullptr || sequence == 0 || entry->state_ != State::kNotSent) {
      return absl::FailedPreconditionError("invalid or repeated handoff send");
    }
    entry->state_ = State::kInFlight;
    entry->sequence_ = sequence;
    ++inflight_;
    return absl::OkStatus();
  }

  // False means the ACK must be matched against another FULL frame kind.
  // An exact duplicate is a protocol error, never another count decrement.
  absl::StatusOr<bool> Acknowledge(std::uint16_t partition,
                                   std::uint64_t sequence) {
    Entry* entry = Find(partition);
    if (entry == nullptr || entry->state_ == State::kNotSent ||
        entry->sequence_ != sequence) {
      return false;
    }
    if (entry->state_ != State::kInFlight) {
      return absl::InvalidArgumentError("duplicate partition handoff ACK");
    }
    entry->state_ = State::kAcked;
    --inflight_;
    --unfinished_;
    return true;
  }

  bool acknowledged(std::uint16_t partition) const {
    return partition < partition_count_ && partition % flow_count_ == flow_ &&
           entries_[partition / flow_count_].state_ == State::kAcked;
  }
  std::size_t inflight() const noexcept { return inflight_; }
  std::size_t unfinished() const noexcept { return unfinished_; }

 private:
  enum class State : std::uint8_t { kNotSent, kInFlight, kAcked };
  struct Entry {
    std::uint64_t sequence_ = 0;
    State state_ = State::kNotSent;
  };
  Entry* Find(std::uint16_t partition) {
    return partition < partition_count_ && partition % flow_count_ == flow_
               ? &entries_[partition / flow_count_]
               : nullptr;
  }

  unsigned flow_;
  unsigned flow_count_;
  std::size_t partition_count_;
  std::vector<Entry> entries_;
  std::size_t unfinished_ = 0;
  std::size_t inflight_ = 0;
};

}  // namespace lavik::detail
