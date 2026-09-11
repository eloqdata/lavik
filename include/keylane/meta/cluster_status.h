#pragma once

// Raft-free model for `keylane-ctl cluster-status`. The server
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
#include "keylane/meta/admin_client.h"
#include "keylane/meta/cluster_create.h"

namespace keylane::meta {

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

struct ClusterGroupWireV1 {
  std::string group_id_;
  std::uint64_t term_ = 0;
  std::optional<std::string> owner_node_id_;
  std::uint64_t config_epoch_ = 0;
  std::optional<std::uint64_t> grant_revision_;
  bool serving_ready_ = false;
  bool topology_converged_ = false;
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

// Reuses the extensible v1 blocker list for the cluster-create preflight;
// adding a fixed field would silently change the established v1 wire layout.
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

struct ClusterStatusWireV1 {
  ClusterCaptureWireV1 capture_;
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

// Reply payloads are lowercase hex around a bounded binary schema. The outer
// line envelope remains human-inspectable and lets typed transient errors use
// the normal `ERR <kind>` form.
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

  // Creates a v1 multi-Group topology from an empty single-Meta cluster and
  // returns only after cluster-status observes the exact manifest as READY.
  // A transport failure after the mutation request starts is intentionally
  // not retried; interruption recovery belongs to the resume workflow.
  absl::StatusOr<ClusterCreateOutcome> Create(
      const MetaAdminTarget& seed, const ClusterCreateManifestV1& manifest,
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

// Deterministic public renderers. JSON u64 values are decimal strings and all
// arrays are sorted independently of server iteration order.
absl::StatusOr<std::string> RenderClusterStatusJson(
    const ClusterStatusOutcome& outcome);
absl::StatusOr<std::string> RenderClusterStatusText(
    const ClusterStatusOutcome& outcome);

}  // namespace keylane::meta
