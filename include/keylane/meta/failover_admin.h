#pragma once

// Raft-free wire model for the dedicated controlled-failover operator entry.
// The Admin server translates this bounded request into the durable
// FailoverOperationIntent; clients do not depend on Meta stores or NuRaft.

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"

namespace keylane::meta {

struct FailoverAdminRequestV1 {
  MetaOperationId operation_id_{};
  std::string group_id_;
  std::uint64_t absolute_deadline_unix_ms_ = 0;

  bool operator==(const FailoverAdminRequestV1&) const = default;
};

struct FailoverRequestOptions {
  std::string group_id_;
  std::chrono::milliseconds transition_timeout_{120'000};
  // These fields are an all-or-none exact idempotency pair. An embedding that
  // retries must preserve both the permanent operation id and the absolute
  // workflow deadline; recomputing the latter would change the durable
  // intent. The CLI leaves both empty and reports its generated id on every
  // uncertain outcome.
  std::optional<MetaOperationId> operation_id_;
  std::optional<std::uint64_t> absolute_deadline_unix_ms_;
};

struct FailoverOutcome {
  std::uint64_t submission_commit_index_ = 0;
  std::string operation_id_;

  bool operator==(const FailoverOutcome&) const = default;
};

absl::StatusOr<std::string> EncodeFailoverAdminRequest(
    const FailoverAdminRequestV1& request);
absl::StatusOr<FailoverAdminRequestV1> DecodeFailoverAdminRequest(
    std::string_view request);
absl::StatusOr<FailoverOutcome> DecodeFailoverAdminReply(
    std::string_view reply);

}  // namespace keylane::meta
