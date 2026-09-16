#pragma once

// Pure Owner Serviceability evaluation for the Automatic Failover Detector.
// The caller anchor-validates internally consistent committed, runtime, and
// observation snapshots. A cross-source tear remains explicit and fails
// closed as Indeterminate. This module performs no I/O, reads no clocks, and
// retains no state.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "keylane/meta/commands.h"

namespace keylane::meta {

// Exact committed authority and node-specific Full Desired State projection
// against which every volatile observation is checked. `projection_hash_` is
// the semantic FDS projection hash, not its transport object hash.
struct MetaOwnerAuthorityAnchor {
  std::string group_id_;
  std::string owner_node_id_;
  MetaAssignmentId owner_assignment_id_{};
  std::uint64_t group_term_ = 0;
  MetaHash256 projection_hash_{};

  bool operator==(const MetaOwnerAuthorityAnchor&) const = default;
};

// Whether the Adapter can prove that the last possible causal Owner lease is
// still inside or beyond its own effective duration. Unknown is distinct from
// expired: a torn runtime/observation join must not manufacture failure, while
// a stale projection cannot hide a lease whose finite deadline is known past.
enum class MetaCausalProgressFreshness : std::uint8_t {
  kUnknown = 0,
  kFresh = 1,
  kExpired = 2,
};

// Self-contained, anchor-validated input cut for one Group. Each source
// snapshot is internally coherent; cross-source mismatch remains represented
// for fail-closed evaluation. Blockers are deliberately included beside
// observations so a caller cannot accidentally evaluate runtime health under
// an ineligible leader or during an authority handoff.
struct MetaOwnerServiceabilityCut {
  // One connected authenticated session from the observation Adapter. The
  // Adapter reduces the runtime/session join to `current_`; boot and session
  // generations do not cross this pure evaluation seam only to be copied and
  // compared again.
  struct Session {
    // Latest Owner heartbeat and the exact installed FDS that underlay it.
    // Freshness is computed before crossing this seam, keeping clock units and
    // wraparound out of the evaluator.
    struct Heartbeat {
      MetaOwnerAuthorityAnchor installed_anchor_;
      std::uint64_t sequence_ = 0;
      bool fresh_ = false;
      bool draining_ = false;
      bool storage_ready_ = false;
      bool population_ready_ = false;

      bool operator==(const Heartbeat&) const = default;
    };

    bool current_ = false;
    std::optional<Heartbeat> heartbeat_;
    // The Adapter bounds both a pending grant-confirmation window and a
    // confirmed lease that has stopped advancing. Expiry is classified with
    // heartbeat expiry so ordinary health traffic cannot hide loss of causal
    // authority progress.
    MetaCausalProgressFreshness causal_progress_freshness_ =
        MetaCausalProgressFreshness::kUnknown;
    // Presence means a higher-sequence heartbeat proved that Data processed
    // this Grant Ack. The current heartbeat sequence remains in Heartbeat, so
    // a second confirming-sequence field would carry no additional evidence.
    std::optional<std::uint64_t> confirmed_grant_sequence_;

    bool operator==(const Session&) const = default;
  };

  bool leader_authority_eligible_ = false;
  bool leadership_warmup_complete_ = false;
  bool authority_handoff_complete_ = false;
  bool failover_transition_active_ = false;
  MetaOwnerAuthorityAnchor committed_anchor_;
  std::optional<Session> session_;

  bool operator==(const MetaOwnerServiceabilityCut&) const = default;
};

enum class MetaOwnerServiceabilityState : std::uint8_t {
  kServiceable = 1,
  kUnserviceable = 2,
  kIndeterminate = 3,
  kBlocked = 4,
};

// Stable diagnostic reasons. The five failure reasons have fixed precedence:
// session_missing, heartbeat_expired, draining, storage_unready, then
// population_unready. Stale anchors and a fresh or unknown pending causal Ack
// are explicitly Indeterminate rather than evidence of failure; known-expired
// causal progress is heartbeat_expired even when the last heartbeat names the
// preceding projection.
enum class MetaOwnerServiceabilityReason : std::uint8_t {
  kNone = 0,
  kSessionMissing = 1,
  kHeartbeatExpired = 2,
  kDraining = 3,
  kStorageUnready = 4,
  kPopulationUnready = 5,
  kStaleOwnerAnchor = 6,
  kCausalLeasePending = 7,
};

// Stable control-plane blocker codes. Evaluation prioritizes leader
// eligibility, warmup, an existing transition, and then authority handoff;
// the numeric values are identifiers rather than a precedence encoding.
enum class MetaOwnerServiceabilityBlocker : std::uint8_t {
  kNone = 0,
  kLeaderIneligible = 1,
  kLeadershipWarmup = 2,
  kAuthorityHandoff = 3,
  kFailoverTransition = 4,
};

struct MetaOwnerServiceabilityDecision {
  MetaOwnerServiceabilityState state_ =
      MetaOwnerServiceabilityState::kIndeterminate;
  MetaOwnerServiceabilityReason reason_ = MetaOwnerServiceabilityReason::kNone;
  MetaOwnerServiceabilityBlocker blocker_ =
      MetaOwnerServiceabilityBlocker::kNone;

  bool operator==(const MetaOwnerServiceabilityDecision&) const = default;
};

// Evaluates exactly one cut with no side effects or hidden inputs. Only a
// current session, heartbeat, FDS/authority anchor, fresh causal progress, and
// a causally confirmed lease can produce Serviceable.
[[nodiscard]] MetaOwnerServiceabilityDecision EvaluateOwnerServiceability(
    const MetaOwnerServiceabilityCut& cut) noexcept;

// Stable lowercase codes for diagnostics and structured logs. Invalid enum
// values render as "unknown" and never acquire serviceability semantics.
std::string_view MetaOwnerServiceabilityStateName(
    MetaOwnerServiceabilityState state) noexcept;
std::string_view MetaOwnerServiceabilityReasonName(
    MetaOwnerServiceabilityReason reason) noexcept;
std::string_view MetaOwnerServiceabilityBlockerName(
    MetaOwnerServiceabilityBlocker blocker) noexcept;

}  // namespace keylane::meta
