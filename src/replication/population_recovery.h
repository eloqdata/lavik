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

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/replication_group.h"

namespace lavik::detail {

// A versioned, bounded durable payload owned by Replication. Source identity
// and frontier describe the previous boot; target identity is rebound only in
// memory after Storage has consumed the enclosing clean-shutdown certificate.
struct RecoveredPopulation {
  RebuildIdentity identity_;
  std::vector<std::uint64_t> frontier_;
};

inline std::string EncodeRecoveredPopulation(
    const RebuildIdentity& identity,
    std::span<const std::uint64_t> frontier = {}) {
  std::string out("LVRP1");
  auto number = [&](std::uint64_t value) {
    for (unsigned i = 0; i < 8; ++i)
      out.push_back(static_cast<char>(value >> (i * 8)));
  };
  for (const auto* value :
       {&identity.group_id_, &identity.assignment_id_,
        &identity.source_node_id_, &identity.source_assignment_id_,
        &identity.source_boot_id_, &identity.source_history_id_,
        &identity.target_node_id_, &identity.target_boot_id_}) {
    number(value->size());
    out.append(*value);
  }
  number(identity.term_);
  number(identity.manifest_revision_);
  out.append(reinterpret_cast<const char*>(identity.manifest_id_.bytes_.data()),
             32);
  number(identity.partition_replication_epoch_);
  number(frontier.size());
  for (auto cursor : frontier) number(cursor);
  return out;
}

inline absl::StatusOr<RecoveredPopulation> DecodeRecoveredPopulation(
    std::string_view in) {
  const auto invalid = [] {
    return absl::DataLossError(
        "invalid durable cluster population recovery payload");
  };
  if (!in.starts_with("LVRP1") || in.size() > 1024 * 1024) return invalid();
  in.remove_prefix(5);
  auto number = [&](std::uint64_t& value) {
    if (in.size() < 8) return false;
    value = 0;
    for (unsigned i = 0; i < 8; ++i)
      value |= std::uint64_t(static_cast<unsigned char>(in[i])) << (i * 8);
    in.remove_prefix(8);
    return true;
  };
  RecoveredPopulation result;
  auto& identity = result.identity_;
  for (auto* value :
       {&identity.group_id_, &identity.assignment_id_,
        &identity.source_node_id_, &identity.source_assignment_id_,
        &identity.source_boot_id_, &identity.source_history_id_,
        &identity.target_node_id_, &identity.target_boot_id_}) {
    std::uint64_t size = 0;
    if (!number(size) || size > in.size() || size > 4096) return invalid();
    value->assign(in.substr(0, size));
    in.remove_prefix(size);
  }
  if (!number(identity.term_) || !number(identity.manifest_revision_) ||
      in.size() < 32)
    return invalid();
  std::copy_n(reinterpret_cast<const std::uint8_t*>(in.data()), 32,
              identity.manifest_id_.bytes_.begin());
  in.remove_prefix(32);
  std::uint64_t flows = 0;
  if (!number(identity.partition_replication_epoch_) || !number(flows) ||
      flows > 4096 || flows != in.size() / 8 || in.size() % 8 != 0)
    return invalid();
  for (std::uint64_t flow = 0; flow < flows; ++flow) {
    std::uint64_t cursor = 0;
    if (!number(cursor) || cursor == 0) return invalid();
    result.frontier_.push_back(cursor);
  }
  if (identity.group_id_.empty() || identity.assignment_id_.empty() ||
      identity.target_node_id_.empty() || identity.target_boot_id_.empty() ||
      identity.term_ == 0 || identity.manifest_revision_ == 0 ||
      identity.partition_replication_epoch_ == 0)
    return invalid();
  if (!result.frontier_.empty() &&
      (identity.source_node_id_.empty() ||
       identity.source_assignment_id_.empty() ||
       identity.source_boot_id_.empty() || identity.source_history_id_.empty()))
    return invalid();
  return result;
}

inline bool SameRecoveredPopulationScope(const RebuildIdentity& a,
                                         const RebuildIdentity& b) {
  return a.group_id_ == b.group_id_ && a.assignment_id_ == b.assignment_id_ &&
         a.target_node_id_ == b.target_node_id_ &&
         a.manifest_revision_ == b.manifest_revision_ &&
         a.manifest_id_ == b.manifest_id_ &&
         a.partition_replication_epoch_ == b.partition_replication_epoch_;
}

}  // namespace lavik::detail
