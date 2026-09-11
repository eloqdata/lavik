#pragma once

// Raft-free manifest model for the single-Meta declarative cluster creation
// workflow. Deployment configuration deliberately stays outside this
// interface: the manifest describes only durable cluster identity and shape.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"

namespace keylane::meta {

// Normalized durable topology accepted by cluster-create protocol v1. All
// vectors are in canonical order and slot_ranges_ is the complete derived
// table even when the manifest requested automatic allocation.
struct ClusterCreateManifestV1 {
  struct DataNode {
    std::string node_id_;
    std::string client_endpoint_;
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
  std::uint32_t meta_member_id_ = 0;
  bool slots_generated_ = false;
  std::vector<DataNode> data_nodes_;
  std::vector<Group> groups_;
  std::vector<SlotRange> slot_ranges_;

  bool operator==(const ClusterCreateManifestV1&) const = default;
};

// Terminal Meta result. committed_index_ identifies a committed cut observing
// completion; groups_ names every retained per-Group operation and the commit
// that proved its population initialization complete.
struct ClusterCreateOutcome {
  struct Group {
    std::string group_id_;
    std::uint64_t committed_index_ = 0;
    std::string operation_id_;
    bool operator==(const Group&) const = default;
  };

  std::uint64_t committed_index_ = 0;
  std::vector<Group> groups_;
  bool operator==(const ClusterCreateOutcome&) const = default;
};

// Strictly parses and normalizes a v1 manifest. The input is capped at 64 KiB
// and unknown structure is rejected so deployment fields cannot silently
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
