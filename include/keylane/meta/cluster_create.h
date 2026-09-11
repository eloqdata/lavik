#pragma once

// Raft-free manifest model for the first single-Meta/single-Data cluster
// creation workflow. Deployment configuration deliberately stays outside this
// interface: the manifest describes only durable cluster identity and shape.

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"

namespace keylane::meta {

// Normalized durable topology accepted by cluster-create protocol v1. The
// collection cardinalities are part of v1, so representing the one allowed
// item directly prevents callers from constructing unsupported shapes.
struct ClusterCreateManifestV1 {
  std::uint32_t schema_version_ = 0;
  std::uint32_t meta_member_id_ = 0;
  std::string data_node_id_;
  std::string client_endpoint_;
  std::string group_id_;
  std::string primary_node_id_;
  std::uint16_t first_slot_ = 0;
  std::uint16_t last_slot_ = 0;

  bool operator==(const ClusterCreateManifestV1&) const = default;
};

// Terminal Meta result. committed_index_ identifies a committed cut observing
// completion (possibly newer than its commit); operation_id_ names the
// retained creation intent and progress record.
struct ClusterCreateOutcome {
  std::uint64_t committed_index_ = 0;
  std::string operation_id_;
};

// Strictly parses the v1 tracer-bullet manifest. The input is capped at 64
// KiB and unknown structure is rejected so deployment fields cannot silently
// become part of the durable cluster contract.
absl::StatusOr<ClusterCreateManifestV1> ParseClusterCreateManifest(
    std::string_view toml);

// Produces the bounded binary request accepted by the Meta Admin adapter.
// Keeping opaque Raft revisions and generated identities out of this
// interface lets the leader own the complete ordered mutation workflow.
absl::StatusOr<std::string> EncodeClusterCreateRequest(
    const ClusterCreateManifestV1& manifest, std::uint32_t wait_timeout_ms);

// Strictly decodes the versioned binary request and returns the leader-side
// projection-wait budget separately from the durable manifest.
absl::StatusOr<ClusterCreateManifestV1> DecodeClusterCreateRequest(
    std::string_view request, std::uint32_t* wait_timeout_ms);

// Decodes the successful Admin response; structured ERR responses are mapped
// by ClusterOperator because their status controls the CLI exit contract.
absl::StatusOr<ClusterCreateOutcome> DecodeClusterCreateReply(
    std::string_view reply);

}  // namespace keylane::meta
