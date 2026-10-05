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
#include <cstddef>
#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik::detail {

// One source flow owns this fixed-size ledger on its worker. It retains only
// completion identities: the ordered writer releases each frame buffer after
// WriteAll. Callers retain ordinary snapshot completion identities until each
// exact ACK. Unlike fragmented FULL commands, every record frame has its own
// ACK and can therefore return byte credit independently.
class FullSyncRecordWindow {
 public:
  static constexpr std::size_t kMaxFrames = 8;
  static constexpr std::size_t kMaxBytes = 16 * 1024 * 1024;

  // Wire bytes include the transport header and FULL sequence. The frame-count
  // ceiling also bounds tiny record batches independently of byte credit.
  bool CanSend(std::size_t wire_bytes) const noexcept {
    return wire_bytes != 0 && inflight_frames_ < kMaxFrames &&
           wire_bytes <= kMaxBytes - inflight_bytes_;
  }

  // Register before any suspending write: the ACK reader can resume before the
  // writer's continuation. A failed write aborts the whole FULL attempt;
  // entries are never retried or transferred to a replacement session.
  absl::Status Begin(std::uint16_t partition, std::uint64_t sequence,
                     std::size_t wire_bytes) {
    if (sequence == 0 || wire_bytes == 0) {
      return absl::InvalidArgumentError(
          "invalid full-sync record window entry");
    }
    if (wire_bytes > kMaxBytes) {
      return absl::ResourceExhaustedError(
          "full-sync record exceeds send window byte limit");
    }
    for (const Entry& entry : entries_) {
      if (entry.sequence_ == sequence) {
        return absl::FailedPreconditionError(
            "overlapping full-sync record sequence");
      }
    }
    if (!CanSend(wire_bytes)) {
      return absl::UnavailableError("full-sync record window is full");
    }
    for (Entry& entry : entries_) {
      if (entry.sequence_ != 0) continue;
      entry = Entry{partition, sequence, wire_bytes};
      ++inflight_frames_;
      inflight_bytes_ += wire_bytes;
      return absl::OkStatus();
    }
    return absl::InternalError("full-sync record window accounting mismatch");
  }

  // False delegates this ACK to another FULL frame kind. A matching sequence
  // with the wrong partition is an error. Exact completion releases only its
  // own credit, even when a later frame or handoff has already completed.
  absl::StatusOr<bool> Acknowledge(std::uint16_t partition,
                                   std::uint64_t sequence) {
    if (sequence == 0) return false;
    for (Entry& entry : entries_) {
      if (entry.sequence_ != sequence) continue;
      if (entry.partition_ != partition) {
        return absl::InvalidArgumentError(
            "full-sync record ACK partition mismatch");
      }
      --inflight_frames_;
      inflight_bytes_ -= entry.wire_bytes_;
      entry = {};
      return true;
    }
    return false;
  }

  // A writer-owned completion may be reaped only after its exact entry has
  // left the ledger; a different frame's ACK says nothing about this one.
  bool Contains(std::uint64_t sequence) const noexcept {
    if (sequence == 0) return false;
    for (const Entry& entry : entries_) {
      if (entry.sequence_ == sequence) return true;
    }
    return false;
  }

  std::size_t inflight_frames() const noexcept { return inflight_frames_; }
  std::size_t inflight_bytes() const noexcept { return inflight_bytes_; }

 private:
  struct Entry {
    std::uint16_t partition_ = 0;
    std::uint64_t sequence_ = 0;
    std::size_t wire_bytes_ = 0;
  };
  std::array<Entry, kMaxFrames> entries_{};
  std::size_t inflight_frames_ = 0;
  std::size_t inflight_bytes_ = 0;
};

}  // namespace lavik::detail
