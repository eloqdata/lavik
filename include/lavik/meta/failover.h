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

// Typed durable intent for an operator-requested controlled failover. The
// generic operation journal stores only this stable request identity; the
// topology-owned MetaFailoverTransition carries election and execution state.

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/topology_store.h"

namespace lavik::meta {

class MetaCommittedFacts;
class MetaProposalView;
class MetaObservationStore;

inline constexpr std::string_view kFailoverOperationKind = "failover";
inline constexpr std::string_view kFailoverCompletedResult =
    "failover-completed";

// Durable operator request stored in MetaOperationRecord. The Group's
// failover transition owns the durable election and Candidate Action state;
// live liveness, frontier, and prepared observations remain leader-local. The
// operation intent stays stable so retries deduplicate throughout the live and
// archived-record retention window.
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

// Shared typed-transition validation for proposal admission and Admin promote.
// Group, authority and the complete facts domain must belong to one owned cut
// for the command's Group. These borrowed inputs are used only during the call.
// Non-transition commands are rejected; generic ownership remains in the hook.
absl::Status ValidateFailoverTransition(
    const MetaCommand& command,
    const std::optional<MetaTopologyGroupView>& group,
    const std::optional<MetaGroupAuthorityView>& authority,
    const MetaCommittedFacts& facts, const MetaObservationStore& observations,
    std::int64_t proposal_now_unix_ms);

// Coordinator admission for workflow ownership and canonical operator input.
// Runtime progress is represented by MetaFailoverTransition and the eight
// typed commands, never by generic operation phases, directives, or receipts.
// For a typed transition, this hook rechecks the committed transition
// pre-state and its matching leader-local observations at
// `proposal_now_unix_ms`; apply remains deterministic and repeats every
// durable CAS/invariant. Malformed
// requests and generic failover mutations are rejected before Raft append.
absl::Status ValidateFailoverProposal(const MetaCommand& command,
                                      const MetaProposalView& view,
                                      const MetaObservationStore& observations,
                                      std::int64_t proposal_now_unix_ms);

}  // namespace lavik::meta
