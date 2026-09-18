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

#include "native_replay.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <optional>
#include <utility>

#include "lavik/storage/format.h"

namespace lavik::detail {

absl::StatusOr<NativeTransactionRecord> DecodeNativeTransactionRecord(
    ReplicatedCommand command, unsigned flow_id, unsigned origin_flow_count) {
  if (command.args_.empty() ||
      !IsReplicationTransactionEnvelope(command.args_[0])) {
    return absl::InvalidArgumentError(
        "malformed replicated transaction envelope");
  }
  auto metadata = DecodeReplicationTransactionEnvelope(command.args_[0]);
  if (!metadata.ok()) return metadata.status();
  if (metadata->payload_flow_ >= origin_flow_count ||
      metadata->participants_.size() > origin_flow_count ||
      std::ranges::any_of(metadata->participants_,
                          [origin_flow_count](unsigned flow) {
                            return flow >= origin_flow_count;
                          }) ||
      std::ranges::find(metadata->participants_, flow_id) ==
          metadata->participants_.end() ||
      std::ranges::find(metadata->participants_, metadata->payload_flow_) ==
          metadata->participants_.end() ||
      ((flow_id == metadata->payload_flow_) != (command.args_.size() > 1))) {
    return absl::InvalidArgumentError(
        "replicated transaction payload or participant does not match its "
        "origin flow");
  }
  command.args_.erase(command.args_.begin());
  return NativeTransactionRecord{std::move(*metadata), std::move(command)};
}

absl::StatusOr<std::uint64_t> NativeControlBarrierId(
    const ReplicatedCommand& command) {
  if (command.args_.empty() ||
      (command.args_[0] != "FLUSHDB" && command.args_[0] != "FLUSHALL") ||
      command.db_id_ >= storage::kLogicalDatabaseCount ||
      command.args_.size() != (command.args_[0] == "FLUSHDB"
                                   ? 3U
                                   : 2U + storage::kLogicalDatabaseCount)) {
    return absl::InvalidArgumentError("malformed replicated control barrier");
  }
  std::uint64_t id = 0;
  for (std::size_t index = 1; index < command.args_.size(); ++index) {
    std::uint64_t value = 0;
    const auto& encoded = command.args_[index];
    auto parsed =
        std::from_chars(encoded.data(), encoded.data() + encoded.size(), value);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != encoded.data() + encoded.size() || value == 0) {
      return absl::InvalidArgumentError(
          "invalid replicated control barrier identity or epoch");
    }
    if (index == 1) id = value;
  }
  return id;
}

absl::StatusOr<NativeReplayEffect> NativeReplay::PrepareEffect(
    std::vector<NativeHistoryRecord> records) const {
  if (!applied_ || records.empty() || records.size() > applied_->size()) {
    return absl::InvalidArgumentError(
        "retained logical effect has no complete origin layout");
  }
  std::ranges::sort(records, {}, &NativeHistoryRecord::flow_id_);
  std::vector<ReplicatedCommand> decoded;
  decoded.reserve(records.size());
  std::uint64_t bytes = 0;
  for (std::size_t index = 0; index < records.size(); ++index) {
    const auto& record = records[index];
    if (record.flow_id_ >= applied_->size() || record.lsn_ == 0 ||
        record.lsn_ == std::numeric_limits<std::uint64_t>::max() ||
        (index != 0 && records[index - 1].flow_id_ == record.flow_id_) ||
        record.canonical_.size() > kMaxNativeReplicationEventBytes - bytes) {
      return absl::InvalidArgumentError(
          "retained logical effect has invalid participants or exceeds its "
          "byte bound");
    }
    bytes += record.canonical_.size();
    auto command = DecodeReplicationCommand(record.canonical_);
    if (!command.ok()) return command.status();
    decoded.push_back(std::move(*command));
  }
  NativeReplayEffect effect;
  const bool transaction =
      IsReplicationTransactionEnvelope(decoded.front().args_[0]);
  const bool control = decoded.front().args_[0] == "FLUSHDB" ||
                       decoded.front().args_[0] == "FLUSHALL";
  if (transaction) {
    std::optional<ReplicationTransactionEnvelope> expected;
    std::optional<std::uint8_t> db;
    bool has_payload = false;
    for (std::size_t index = 0; index < records.size(); ++index) {
      auto record = DecodeNativeTransactionRecord(
          std::move(decoded[index]), records[index].flow_id_, applied_->size());
      if (!record.ok()) return record.status();
      if (!expected.has_value()) {
        expected = record->envelope_;
        db = record->payload_.db_id_;
      }
      if (record->envelope_.id_ != expected->id_ ||
          record->envelope_.participants_ != expected->participants_ ||
          record->envelope_.payload_flow_ != expected->payload_flow_ ||
          record->payload_.db_id_ != *db) {
        return absl::InvalidArgumentError(
            "conflicting retained transaction participants");
      }
      if (!record->payload_.args_.empty()) {
        if (has_payload)
          return absl::InvalidArgumentError(
              "duplicate retained transaction payload");
        effect.command_ = std::move(record->payload_);
        has_payload = true;
      }
    }
    if (!has_payload || expected->participants_.size() != records.size()) {
      return absl::InvalidArgumentError(
          "retained transaction participants are incomplete");
    }
  } else if (control) {
    auto barrier = NativeControlBarrierId(decoded.front());
    if (!barrier.ok()) return barrier.status();
    if (records.size() != applied_->size())
      return absl::InvalidArgumentError(
          "retained control barrier is incomplete");
    for (const auto& command : decoded) {
      if (command.db_id_ != decoded.front().db_id_ ||
          command.args_ != decoded.front().args_) {
        return absl::InvalidArgumentError(
            "conflicting retained control markers");
      }
    }
    effect.command_ = std::move(decoded.front());
  } else {
    if (records.size() != 1)
      return absl::InvalidArgumentError(
          "ordinary retained effect has multiple participants");
    effect.command_ = std::move(decoded.front());
  }
  auto applied = applied_->TrySnapshot();
  if (!applied.ok()) return applied.status();
  bool any_applied = false;
  bool all_applied = true;
  bool missing_predecessor = false;
  for (const auto& record : records) {
    const auto next = (*applied)[record.flow_id_];
    any_applied |= next > record.lsn_;
    all_applied &= next > record.lsn_;
    missing_predecessor |= next < record.lsn_;
    effect.updates_.push_back({record.flow_id_, record.lsn_});
  }
  if (all_applied)
    effect.disposition_ = NativeReplayDisposition::kDuplicate;
  else if (any_applied)
    return absl::InvalidArgumentError(
        "retained effect straddles an incompatible Applied cut");
  else if (missing_predecessor)
    effect.disposition_ = NativeReplayDisposition::kNeedsPredecessor;
  effect.records_ = std::move(records);
  return effect;
}

absl::Status NativeReplay::PublishAfterApply(unsigned publisher,
                                             NativeHistoryRecord record) {
  if (!applied_ || (!histories_.empty() && publisher >= histories_.size()))
    return absl::FailedPreconditionError("native replay has no Applied cut");
  auto status = applied_->AdvanceAfterApply(record.flow_id_, record.lsn_);
  if (status.ok() && !histories_.empty() && !record.canonical_.empty())
    histories_[publisher]->TryRetainOne(history_id_, record);
  return status;
}

absl::Status NativeReplay::PublishAfterApply(
    unsigned publisher,
    std::span<const ReplicaAppliedFrontier::FlowApplied> updates,
    std::vector<NativeHistoryRecord> records) {
  if (!applied_ || updates.empty() ||
      (!histories_.empty() && publisher >= histories_.size()))
    return absl::FailedPreconditionError("native replay has no Applied cut");
  auto status = updates.size() == 1
                    ? applied_->AdvanceAfterApply(updates.front().flow_id_,
                                                  updates.front().applied_lsn_)
                    : applied_->AdvanceBatchAfterApply(publisher, updates);
  // Retention may lose one optional participant under receive pressure. A
  // complete apply remains valid, but such a bundle must advertise no range.
  const bool complete_retention =
      records.size() == updates.size() &&
      std::ranges::all_of(records, [&](const auto& record) {
        return !record.canonical_.empty() &&
               std::ranges::any_of(updates, [&](const auto& update) {
                 return update.flow_id_ == record.flow_id_ &&
                        update.applied_lsn_ == record.lsn_;
               });
      });
  if (status.ok() && !histories_.empty() && complete_retention)
    histories_[publisher]->TryRetain(history_id_, std::move(records));
  return status;
}

}  // namespace lavik::detail
