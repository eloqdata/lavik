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

// Canonical version-1 payload accepted by the dedicated Admin endpoint. The
// caller-generated operation id is the durable idempotency key, and the
// absolute deadline is part of that immutable intent rather than a per-attempt
// timeout.
struct FailoverAdminRequestV1 {
  MetaOperationId operation_id_{};
  std::string group_id_;
  std::uint64_t absolute_deadline_unix_ms_ = 0;

  bool operator==(const FailoverAdminRequestV1&) const = default;
};

// Operator-facing inputs used to construct FailoverAdminRequestV1. Supplying
// neither idempotency field requests a freshly generated operation id and an
// absolute deadline derived from transition_timeout_.
struct FailoverRequestOptions {
  std::string group_id_;
  std::chrono::milliseconds transition_timeout_{120'000};
  // These fields are an all-or-none exact idempotency pair. An embedding that
  // retries must preserve both the retained operation id and the absolute
  // workflow deadline; recomputing the latter would change the durable intent.
  // The CLI leaves both empty and reports its generated id on every uncertain
  // outcome.
  std::optional<MetaOperationId> operation_id_;
  std::optional<std::uint64_t> absolute_deadline_unix_ms_;
};

// Durable submission receipt. The commit index proves that the controlled
// Operation intent entered the Meta state machine; Begin and completion remain
// asynchronous and observable through the returned operation id.
struct FailoverOutcome {
  std::uint64_t submission_commit_index_ = 0;
  std::string operation_id_;

  bool operator==(const FailoverOutcome&) const = default;
};

// Encodes and validates the bounded canonical `failover 1` Admin command.
absl::StatusOr<std::string> EncodeFailoverAdminRequest(
    const FailoverAdminRequestV1& request);
// Decodes the canonical request and rejects malformed, noncanonical, or
// out-of-bounds payloads before they reach proposal validation.
absl::StatusOr<FailoverAdminRequestV1> DecodeFailoverAdminRequest(
    std::string_view request);
// Decodes a successful submission receipt. Server error replies are handled by
// the Admin client before this success-only decoder is called.
absl::StatusOr<FailoverOutcome> DecodeFailoverAdminReply(
    std::string_view reply);

}  // namespace keylane::meta
