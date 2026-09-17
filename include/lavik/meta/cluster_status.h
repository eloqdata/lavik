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

// Raft-free model for `lavik-ctl cluster-status`. The server
// translates its committed/runtime state into these bounded values; clients
// strictly decode them and never need NuRaft types or a public leader-route
// cache.

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/meta/admin_client.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/failover_admin.h"

namespace lavik::meta {

enum class ClusterMetaRole : std::uint8_t {
  kFollower = 0,
  kLeader = 1,
};

struct ClusterMetaMemberWireV1 {
  std::uint32_t server_id_ = 0;
  std::optional<std::string> ctl_endpoint_;
  bool is_leader_ = false;
  bool operator==(const ClusterMetaMemberWireV1&) const = default;
};

struct ClusterHeadWireV1 {
  std::uint32_t responder_id_ = 0;
  ClusterMetaRole role_ = ClusterMetaRole::kFollower;
  std::uint64_t term_ = 0;
  std::optional<std::uint32_t> leader_id_;
  std::uint64_t config_index_ = 0;
  std::vector<ClusterMetaMemberWireV1> meta_members_;
  bool operator==(const ClusterHeadWireV1&) const = default;
};

enum class ClusterLeaseStatus : std::uint8_t {
  kRecentlyGranted = 0,
  kDenied = 1,
  kUnknown = 2,
};

enum class ClusterDataNodeRole : std::uint8_t {
  kPrimary = 0,
  kReplica = 1,
};

struct ClusterDataNodeWireV1 {
  std::string node_id_;
  ClusterDataNodeRole role_ = ClusterDataNodeRole::kReplica;
  bool retired_ = false;
  std::optional<std::string> group_id_;
  bool current_session_ = false;
  bool projection_current_ = false;
  bool health_fresh_ = false;
  bool population_current_ = false;
  ClusterLeaseStatus lease_status_ = ClusterLeaseStatus::kUnknown;
  bool operator==(const ClusterDataNodeWireV1&) const = default;
};

// Stable public detector states. This status enum deliberately belongs to the
// Raft-free Admin model so status clients do not depend on the leader-local
// detector Module or its timer state.
enum class ClusterAutomaticFailoverState : std::uint8_t {
  kHealthy = 1,
  kSuspect = 2,
  kBlocked = 3,
  kTriggering = 4,
};

struct ClusterGroupWireV1 {
  std::string group_id_;
  std::uint64_t term_ = 0;
  std::optional<std::string> owner_node_id_;
  bool serving_ready_ = false;
  bool topology_converged_ = false;
  // Leader-local automatic-failover diagnostics for this Group.
  // `current_reason_` is present only for exact Unserviceable
  // SUSPECT/TRIGGERING classifications; `blocked_reason_` is present only for
  // BLOCKED. Durations are milliseconds: elapsed suspicion may freeze while
  // BLOCKED, while `effective_threshold_ms_` is the threshold resolved from the
  // current global Policy for this cut. It may be zero for BLOCKED Groups
  // that legally predate Genesis Policy installation in a non-pristine cluster.
  ClusterAutomaticFailoverState automatic_failover_state_ =
      ClusterAutomaticFailoverState::kBlocked;
  std::optional<std::string> current_reason_;
  std::uint64_t suspect_elapsed_ms_ = 0;
  std::uint64_t effective_threshold_ms_ = 0;
  std::optional<std::string> blocked_reason_ = "indeterminate_evidence";
  bool operator==(const ClusterGroupWireV1&) const = default;
};

struct ClusterSlotRangeWireV1 {
  std::uint32_t first_ = 0;
  std::uint32_t last_ = 0;
  std::string group_id_;
  bool operator==(const ClusterSlotRangeWireV1&) const = default;
};

struct ClusterBlockerWireV1 {
  std::string code_;
  std::string scope_;
  std::string detail_;
  bool operator==(const ClusterBlockerWireV1&) const = default;
};

// Retained alongside the explicit lifecycle so existing blocker consumers can
// diagnose the active creation workflow in the ordinary readiness list.
inline constexpr std::string_view kClusterCreateActiveBlockerCode =
    "cluster_create_active";

struct ClusterCaptureWireV1 {
  std::uint32_t responder_id_ = 0;
  std::uint64_t term_ = 0;
  std::uint64_t config_index_ = 0;
  std::uint64_t committed_index_ = 0;
  std::uint64_t topology_epoch_ = 0;
  bool operator==(const ClusterCaptureWireV1&) const = default;
};

enum class ClusterStateWireV1 : std::uint8_t {
  kUninitialized = 0,
  kCreating = 1,
  kCreated = 2,
  kProvisioningFailed = 3,
  kNonPristine = 4,
};

struct ClusterStatusWireV1 {
  ClusterCaptureWireV1 capture_;
  ClusterStateWireV1 cluster_state_ = ClusterStateWireV1::kUninitialized;
  std::uint64_t lifecycle_revision_ = 0;
  std::optional<std::string> root_operation_id_;
  std::optional<std::uint64_t> genesis_commit_index_;
  std::optional<std::string> cluster_create_phase_;
  std::optional<std::string> provisioning_failure_summary_;
  bool meta_available_ = false;
  bool meta_membership_stable_ = false;
  bool topology_converged_ = false;
  bool serving_ready_ = false;
  bool cluster_ready_ = false;
  std::vector<ClusterMetaMemberWireV1> meta_members_;
  std::vector<ClusterDataNodeWireV1> data_nodes_;
  std::vector<ClusterGroupWireV1> groups_;
  std::vector<ClusterSlotRangeWireV1> slot_ranges_;
  std::vector<ClusterBlockerWireV1> blockers_;
  bool operator==(const ClusterStatusWireV1&) const = default;
};

// `clusterhead 1` and `clusterstatus 1` name the outer Admin verb revision;
// their lowercase-hex binary payloads have independent leading schema markers
// (both v1). The WireV1 suffix names the outer contract, not the inner payload
// marker. The human-inspectable line envelope
// also lets typed transient errors use the normal `ERR <kind>` form.
absl::StatusOr<std::string> EncodeClusterHeadReply(
    const ClusterHeadWireV1& head);
absl::StatusOr<ClusterHeadWireV1> DecodeClusterHeadReply(
    std::string_view reply);
absl::StatusOr<std::string> EncodeClusterStatusReply(
    const ClusterStatusWireV1& status);
absl::StatusOr<ClusterStatusWireV1> DecodeClusterStatusReply(
    std::string_view reply);

enum class ClusterStatusResult {
  kReady,
  kNotReady,
  kRetryable,
};

struct ClusterStatusOutcome {
  ClusterStatusResult result_ = ClusterStatusResult::kRetryable;
  std::optional<ClusterStatusWireV1> status_;
  std::string retry_reason_;
};

struct ClusterStatusOptions {
  MetaAdminTlsOptions tls_;
  bool allow_plaintext_admin_ = false;
  MetaAdminDeadline deadline_ = std::chrono::steady_clock::time_point::max();
};

using MetaAdminRoundTrip = std::function<absl::StatusOr<std::string>(
    const MetaAdminTarget&, std::string_view, MetaAdminDeadline)>;

class ClusterOperator {
 public:
  ClusterOperator();
  explicit ClusterOperator(MetaAdminRoundTrip round_trip);

  // Discovers the current leader privately and returns one stable status cut.
  // Transient discovery/capture failures are represented by kRetryable;
  // malformed data, unsafe transport configuration, and identity failures are
  // returned as a non-OK status.
  absl::StatusOr<ClusterStatusOutcome> Status(
      const MetaAdminTarget& seed, const ClusterStatusOptions& options) const;

  // Atomically commits the creation root and Creating lifecycle, then returns
  // without waiting for topology provisioning or runtime readiness. A
  // transport failure after the mutation request starts is not retried and
  // carries the caller-generated root operation id for status correlation.
  absl::StatusOr<ClusterCreateOutcome> Create(
      const MetaAdminTarget& seed, const ClusterCreateManifestV1& manifest,
      const ClusterStatusOptions& options) const;

  // Submits one idempotent controlled-failover request to the discovered
  // leader and returns after that request is committed. Execution and
  // terminalization remain owned by the leader-scoped reconciler.
  absl::StatusOr<FailoverOutcome> Failover(
      const MetaAdminTarget& seed, const FailoverRequestOptions& request,
      const ClusterStatusOptions& options) const;

 private:
  // Shared leader-resolution/status capture seam. Mutation workflows receive
  // the exact endpoint whose head/status identity checks succeeded instead of
  // rediscovering or accidentally sending a write back to the seed.
  absl::StatusOr<ClusterStatusOutcome> CaptureStatus(
      const MetaAdminTarget& seed, const ClusterStatusOptions& options,
      MetaAdminTarget* leader_target) const;

  MetaAdminRoundTrip round_trip_;
};

// Deterministic public renderers. JSON has its own schema v1, independent of
// both Admin verb and binary payload revisions. JSON u64 values are decimal
// strings and all arrays are sorted independently of server iteration order.
absl::StatusOr<std::string> RenderClusterStatusJson(
    const ClusterStatusOutcome& outcome);
absl::StatusOr<std::string> RenderClusterStatusText(
    const ClusterStatusOutcome& outcome);

}  // namespace lavik::meta
