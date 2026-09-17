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

// Raft-free manifest model for declarative cluster creation and the immutable
// genesis Meta cohort. Local binds, storage configuration, and TLS credentials
// stay outside this interface: the manifest describes only durable cluster
// identity and advertised topology.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/policy_store.h"

namespace lavik::meta {

// Normalized durable topology accepted by cluster-create protocol v1. All
// vectors are in canonical order and slot_ranges_ is the complete derived
// table even when the manifest requested automatic allocation.
struct ClusterCreateManifestV1 {
  struct MetaMember {
    std::uint32_t server_id_ = 0;
    std::string raft_endpoint_;
    std::string data_control_endpoint_;
    std::string ctl_endpoint_;
    bool operator==(const MetaMember&) const = default;
  };

  struct DataNode {
    std::string node_id_;
    // At least one advertised listener is required. Both transports share
    // one host in the Data topology, but have distinct, explicit ports.
    std::string client_endpoint_;
    std::string tls_endpoint_;
    bool operator==(const DataNode&) const = default;
  };

  struct Group {
    std::string group_id_;
    std::string primary_node_id_;
    std::vector<std::string> replica_node_ids_;
    bool operator==(const Group&) const = default;
  };

  struct SlotRange {
    std::uint16_t first_ = 0;
    std::uint16_t last_ = 0;
    std::string group_id_;
    bool operator==(const SlotRange&) const = default;
  };

  std::uint32_t schema_version_ = 0;
  std::uint64_t automatic_uncontrolled_failover_suspect_after_ms_ =
      kDefaultAutomaticFailoverSuspectAfterMs;
  std::uint64_t authority_lease_duration_ms_ = kDefaultAuthorityLeaseDurationMs;
  std::uint64_t candidate_recovery_budget_ms_ =
      kDefaultCandidateRecoveryBudgetMs;
  std::vector<MetaMember> meta_members_;
  bool slots_generated_ = false;
  std::vector<DataNode> data_nodes_;
  std::vector<Group> groups_;
  std::vector<SlotRange> slot_ranges_;

  bool operator==(const ClusterCreateManifestV1&) const = default;
};

// Acceptance result for the atomic Genesis commit. The background workflow
// may still be Creating after this result is returned.
struct ClusterCreateOutcome {
  std::uint64_t genesis_commit_index_ = 0;
  std::string operation_id_;
  bool operator==(const ClusterCreateOutcome&) const = default;
};

// Strictly parses and normalizes a v1 manifest. The input is capped at 64 KiB
// and unknown structure is rejected so deployment fields cannot silently
// become part of the durable cluster contract.
absl::StatusOr<ClusterCreateManifestV1> ParseClusterCreateManifest(
    std::string_view toml);

// Produces the bounded binary request accepted by the Meta Admin adapter.
// The caller-generated root id gives a client that loses the proposal reply a
// stable correlation key for cluster-status and logs.
absl::StatusOr<std::string> EncodeClusterCreateRequest(
    const ClusterCreateManifestV1& manifest,
    const MetaOperationId& root_operation_id);

// Strictly decodes the current binary request and returns the root id
// separately from the normalized durable manifest. Older intents are outside
// the current-format product contract.
absl::StatusOr<ClusterCreateManifestV1> DecodeClusterCreateRequest(
    std::string_view request, MetaOperationId* root_operation_id);

// Decodes the successful Admin response; structured ERR responses are mapped
// by ClusterOperator because their status controls the CLI exit contract.
absl::StatusOr<ClusterCreateOutcome> DecodeClusterCreateReply(
    std::string_view reply);

}  // namespace lavik::meta
