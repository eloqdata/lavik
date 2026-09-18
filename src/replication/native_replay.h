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

#include <memory>
#include <span>
#include <string>
#include <vector>

#include "lavik/replication_command.h"
#include "lavik/replication_history.h"
#include "replica_applied_frontier.h"

namespace lavik::detail {

struct NativeTransactionRecord {
  ReplicationTransactionEnvelope envelope_;
  ReplicatedCommand payload_;  // Empty arguments on a non-payload participant.
};

// Shared strict semantic decoders for normal streaming and retained replay.
absl::StatusOr<NativeTransactionRecord> DecodeNativeTransactionRecord(
    ReplicatedCommand command, unsigned flow_id, unsigned origin_flow_count);
absl::StatusOr<std::uint64_t> NativeControlBarrierId(
    const ReplicatedCommand& command);

enum class NativeReplayDisposition { kReady, kDuplicate, kNeedsPredecessor };
struct NativeReplayEffect {
  NativeReplayDisposition disposition_ = NativeReplayDisposition::kReady;
  ReplicatedCommand command_;
  std::vector<ReplicaAppliedFrontier::FlowApplied> updates_;
  std::vector<NativeHistoryRecord> records_;
};

// Transport-independent complete-effect admission and publication. The host
// supplies storage apply and action/population fencing. A donor disappearing
// cannot mutate this object's Applied cut: only PublishAfterApply can do so.
class NativeReplay {
 public:
  NativeReplay(std::shared_ptr<ReplicaAppliedFrontier> applied,
               std::vector<std::shared_ptr<ReplicationHistory>> histories,
               std::string history_id)
      : applied_(std::move(applied)),
        histories_(std::move(histories)),
        history_id_(std::move(history_id)) {}

  // Validates a fully received original logical effect. Missing fragments or
  // participants never enter apply. The caller serializes retained replay and
  // drains normal ingress before using the returned complete cut for admission.
  absl::StatusOr<NativeReplayEffect> PrepareEffect(
      std::vector<NativeHistoryRecord> records) const;
  // Shared completion boundary for normal streaming, recovery and partial
  // replay. Call only after storage has applied every part of the logical
  // effect; publication precedes network ACK and survives transport loss.
  // publisher is the calling worker; only its cache is accessed. All records
  // of a transaction/control effect remain together on that worker.
  absl::Status PublishAfterApply(
      unsigned publisher,
      std::span<const ReplicaAppliedFrontier::FlowApplied> updates,
      std::vector<NativeHistoryRecord> records);
  // Ordinary single-flow events carry their applied identity directly. Empty
  // canonical bytes omit optional retention, never the completed apply.
  absl::Status PublishAfterApply(unsigned publisher,
                                 NativeHistoryRecord record);

 private:
  std::shared_ptr<ReplicaAppliedFrontier> applied_;
  const std::vector<std::shared_ptr<ReplicationHistory>> histories_;
  const std::string history_id_;
};

}  // namespace lavik::detail
