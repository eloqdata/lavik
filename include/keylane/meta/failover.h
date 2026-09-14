#pragma once

// Typed state carried inside the generic MetaOperationRecord for one
// top-level failover. The journal remains operation-kind agnostic; this codec
// gives the failover reconciler a strict, replayable contract without adding
// nested promotion operations.

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"

namespace keylane::meta {

class MetaCommittedView;
class MetaObservationStore;

inline constexpr std::string_view kFailoverOperationKind = "failover";
inline constexpr std::string_view kFailoverCompletedResult =
    "failover-completed";

// Durable operator request stored in MetaOperationRecord. The Group's
// failover transition owns the durable election and Candidate Action state;
// live liveness, frontier, and prepared observations remain leader-local. The
// operation intent stays stable for permanent operation-id idempotency.
struct FailoverOperationIntent {
  std::string group_id_;
  std::uint64_t absolute_deadline_unix_ms_ = 0;

  bool operator==(const FailoverOperationIntent&) const = default;
};

// Validates caller-constructed intent without consulting wall clock or
// committed state. Encoders return domain rejection for invalid input;
// decoders classify malformed or semantically invalid committed bytes as
// fail-stop.
absl::Status ValidateFailoverOperationIntent(
    const FailoverOperationIntent& intent);
absl::StatusOr<std::string> EncodeFailoverOperationIntent(
    const FailoverOperationIntent& intent);
absl::StatusOr<FailoverOperationIntent> DecodeFailoverOperationIntent(
    std::string_view encoded);

// Coordinator admission for workflow ownership and canonical operator input.
// Runtime progress is represented by MetaFailoverTransition and the eight
// typed commands, never by generic operation phases, directives, or receipts.
// For a typed transition, this hook rechecks the committed transition
// pre-state and its matching leader-local observations at
// `proposal_now_unix_ms`; apply remains deterministic and repeats every
// durable CAS/invariant. Malformed
// requests and generic failover mutations are rejected before Raft append.
absl::Status ValidateFailoverProposal(const MetaCommand& command,
                                      const MetaCommittedView& view,
                                      const MetaObservationStore& observations,
                                      std::int64_t proposal_now_unix_ms);

}  // namespace keylane::meta
