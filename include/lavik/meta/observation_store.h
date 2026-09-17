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

// MetaObservationStore is the volatile, leader-local store for soft
// observations.
//
// Observations are NEVER committed to the Raft log and never survive a Meta
// restart or leader change: nodes re-report through freshly authenticated
// sessions. The store answers "what does the leader currently believe about
// the runtime" for ValidateProposal and coordinator decisions; committed
// truth lives in MetaStores and is only consulted through MetaCommittedFacts.
//
// Freshness and lifecycle:
//   - Every observation arrives on a trusted {node_id, boot_incarnation,
//     session_generation} triple. ClientHello additionally authenticates the
//     current replication history for failover source-lineage decisions.
//     boot_incarnation is opaque and is NEVER
//     ordered by value; ordering comes from session_generation, a
//     controller-local monotonic sequence issued by the authenticated
//     session layer; tests inject it directly through the ctl adapter.
//   - AdoptSession() establishes the current generation for a node and
//     atomically purges every older observation of that node. Only the
//     current generation may ingest.
//   - Candidate disconnect is withdrawn by the session layer. Other
//     invalidation is checked at ingest against MetaCommittedFacts, actively
//     purged when committed state changes (RevalidateAll), and re-filtered at
//     every read (Latest*/List* take facts and filter again), so a commit
//     landing between ingest and query cannot leak stale data.
//   - Group-bound observations require the authenticated node's exact current
//     membership assignment and group_term == committed current term (older is
//     stale, newer is forged: both rejected). Manifest ids match committed
//     values. Candidate
//     reporter history comes from ClientHello, while its independent source
//     history is a compatibility-domain anchor for the internal selector.
//
// Rejections and evictions are appended to a bounded audit ring buffer for
// operators (MetaObsAuditEvent); this ring is debugging surface, not the
// durable audit trail (that lives in MetaAuditStore).
//
// Resource bounds are layered: sessions and observation entries bound fixed
// container overhead, candidate/failover domain caps stop one group
// or reporter from monopolizing keys, and exact charged-byte
// budgets cover every large payload and its primary index copies. Capacity
// rejection preserves the previous latest-wins value for diagnostic ingest;
// heartbeat replace-or-clear still removes stale role evidence. Observations
// are soft state, so exhaustion cannot create authority.
//
// Threading: public operations are internally serialized. This is required
// because authenticated sessions ingest on control-channel workers while the
// coordinator invalidates state on commit and leadership workers. Time may be
// read here (volatile state only) — TTL expiry uses caller-supplied `now`.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/operation_store.h"

namespace lavik::meta {

// ---------------------------------------------------------------- identities

struct MetaObservationIdentity {
  std::string node_id_;
  MetaBootIncarnation boot_incarnation_;
  uint64_t session_generation_ = 0;
  bool operator==(const MetaObservationIdentity&) const = default;
};

// Exact candidate action contained in the FDS that underlay one authenticated
// heartbeat. This is leader-local session context, not Data-supplied evidence:
// it distinguishes an old-projection heartbeat that has not seen a newly
// committed action from an exact-action heartbeat that affirmatively omitted
// the candidate role.
struct MetaObservedFailoverProjection {
  std::string group_id_;
  std::uint64_t group_term_ = 0;
  MetaFailoverTransitionId transition_id_{};
  std::uint64_t transition_revision_ = 0;
  MetaFailoverActionId action_id_{};
  std::string candidate_node_id_;
  MetaAssignmentId candidate_assignment_id_{};
  MetaBootIncarnation candidate_boot_id_{};

  bool operator==(const MetaObservedFailoverProjection&) const = default;
};

// Exact Owner authority contained in the FDS that underlay one authenticated
// heartbeat. The marker is supplied by the trusted session publisher, never
// by Data, so a detector can compare health and authority without joining a
// client claim to a different projection.
struct MetaObservedOwnerProjection {
  std::string group_id_;
  std::string owner_node_id_;
  MetaAssignmentId owner_assignment_id_{};
  std::uint64_t group_term_ = 0;
  std::uint64_t control_revision_ = 0;
  // Effective duration from this installed FDS after the Meta Leader's local
  // validity cap. Retaining it with the projection lets a later snapshot tear
  // distinguish a known-expired old lease from unknown runtime evidence.
  std::uint32_t authority_lease_duration_ms_ = 0;
  bool operator==(const MetaObservedOwnerProjection&) const = default;
};

// Leader-local authenticated session fact used to distinguish a node that has
// not re-reported after a Meta leadership change from an exact session that
// this leader observed disconnecting. The last-disconnect latch survives a
// later session adoption within the same leadership epoch. Fresh generic
// CandidateProgress may clear a pre-attempt latch while no committed action
// binds that boot; once an action is committed, neither generic progress nor
// Prepared from a replacement session can revive it.
struct MetaObservedSessionState {
  MetaBootIncarnation current_boot_id_{};
  // Authenticated from ClientHello together with the boot id. Failover must
  // treat a same-boot history rotation as a new source incarnation rather
  // than infer lineage from whichever replicas happen to be reporting.
  std::optional<MetaReplicationHistoryId> current_history_id_;
  std::uint64_t current_generation_ = 0;
  bool connected_ = false;
  std::optional<std::int64_t> disconnected_unix_ms_;
  std::optional<MetaBootIncarnation> disconnected_boot_id_;
  std::optional<std::uint64_t> disconnected_generation_;
  std::optional<MetaObservedFailoverProjection> heartbeat_failover_projection_;
  bool operator==(const MetaObservedSessionState&) const = default;
};

// ------------------------------------------------------------- payload types

struct MetaNodeBootObs {
  // Marks liveness of a boot; carries no term/manifest binding.
  bool operator==(const MetaNodeBootObs&) const = default;
};

struct MetaNodeHealthObs {
  bool storage_ready_ = false;
  bool population_ready_ = false;
  bool draining_ = false;
  std::uint32_t active_groups_ = 0;
  std::string health_;  // bounded free-form diagnostic summary
  bool operator==(const MetaNodeHealthObs&) const = default;
};

// One atomic session/health/projection cut for Owner serviceability. health_
// retains only the fixed-size typed fields; the independently budgeted
// diagnostic string is intentionally empty. Presence means an authenticated
// session incarnation is known; connected_ and the optional heartbeat fields
// distinguish disconnect from an adopted session that has not yet reported.
struct MetaObservedOwnerState {
  // A Grant Ack whose delivery is possible but has not yet been proved by a
  // higher-sequence heartbeat. The same value type also retains the exact
  // causally installed Grant window across same-authority FDS replacements.
  struct LeaseWindow {
    MetaObservedOwnerProjection projection_;
    std::uint64_t granted_heartbeat_sequence_ = 0;
    std::uint64_t heartbeat_received_steady_ms_ = 0;

    bool operator==(const LeaseWindow&) const = default;
  };

  MetaObservationIdentity identity_;
  bool connected_ = false;
  std::uint64_t heartbeat_sequence_ = 0;
  std::optional<MetaNodeHealthObs> health_;
  // Owner failure detection uses the same monotonic clock as detector
  // debounce; wall-clock corrections cannot manufacture stale health.
  std::optional<std::uint64_t> heartbeat_received_steady_ms_;
  std::optional<MetaObservedOwnerProjection> owner_projection_;
  // Starts when this exact session first reports an Owner projection and
  // advances only when a heartbeat proves a strictly newer granted Ack for
  // that same projection. Ordinary heartbeats deliberately do not refresh
  // it: callers use its monotonic age to bound causal-lease uncertainty.
  std::optional<std::uint64_t> causal_progress_received_steady_ms_;
  // Receipt of a higher-sequence heartbeat proves that Data processed the
  // named Grant Ack. The projection is the atomic owner_projection_ in this
  // same cut, so retaining another copy would create an invalid state.
  std::optional<std::uint64_t> confirmed_grant_sequence_;
  // Maximum deadline among unconfirmed Grants this leader may have delivered
  // for the current Owner authority.
  std::optional<LeaseWindow> possible_owner_lease_;
  // Exact attempt that produced the newest causally confirmed installed
  // lease. It survives a same-authority FDS replacement, while confirmation
  // of a later Grant replaces it because Data processes Acks in order.
  std::optional<LeaseWindow> installed_owner_lease_;
  // Present after this leader's handoff guard denied an otherwise healthy
  // exact Owner challenge. Same-authority projection replacement does not
  // prove the 2D quarantine finished; a later Grant attempt or a same/newer
  // NodeNotReady Ack evaluated after that deadline supersedes it.
  std::optional<std::uint64_t> authority_handoff_pending_sequence_;
  bool operator==(const MetaObservedOwnerState&) const = default;
};

struct MetaCandidateProgressObs {
  // Copied from the authenticated observation identity so group queries keep
  // the complete reporter incarnation instead of returning an anonymous
  // observation that a reconciler would have to join against another query.
  std::string node_id_;
  MetaBootIncarnation boot_incarnation_{};
  std::uint64_t session_generation_ = 0;
  std::string group_id_;
  MetaAssignmentId assignment_id_{};
  uint64_t group_term_ = 0;
  uint64_t population_manifest_revision_ = 0;
  MetaHash256 population_manifest_digest_{};
  uint64_t partition_replication_epoch_ = 0;
  // Reporter-local history remains the compatibility/diagnostic `history`
  // field. Candidate comparison uses the independent rebuild source lineage.
  MetaReplicationHistoryId replication_history_id_{};
  // This is the term of the copied source population, not necessarily the
  // reporter's current assignment term. After an uncontrolled fence the two
  // intentionally differ while older recoverable data remains eligible.
  std::uint64_t source_group_term_ = 0;
  std::string source_node_id_;
  MetaAssignmentId source_assignment_id_{};
  MetaBootIncarnation source_boot_incarnation_{};
  MetaReplicationHistoryId source_replication_history_id_{};
  std::vector<std::uint64_t> applied_next_lsns_;
  bool storage_ready_ = false;
  bool population_ready_ = false;
  bool draining_ = false;
  std::int64_t received_unix_ms_ = 0;
  std::int64_t expires_unix_ms_ = 0;
  bool recovered_ = false;
  bool operator_recovery_ = false;
  bool operator==(const MetaCandidateProgressObs&) const = default;
};

// Transition-scoped heartbeat facts are soft evidence. They are intentionally
// separate from role_information because a source still requests ordinary
// leases while paused and a candidate still reports its recoverable frontier
// while preparing an action.
struct MetaSourcePausedObs {
  std::string group_id_;
  MetaFailoverTransitionId transition_id_{};
  std::string source_node_id_;
  MetaAssignmentId source_assignment_id_{};
  MetaBootIncarnation source_boot_id_{};
  MetaReplicationHistoryId source_history_id_{};
  std::uint64_t source_group_term_ = 0;
  std::vector<std::uint64_t> stable_next_lsns_;
  std::int64_t received_unix_ms_ = 0;
  std::int64_t expires_unix_ms_ = 0;
  bool operator==(const MetaSourcePausedObs&) const = default;
};

struct MetaCandidatePreparedObs {
  // Leader-local authenticated session generation, copied by the observation
  // store rather than accepted from the wire. It binds Prepared to the current
  // adopted session so a disconnect or superseding session cannot leave stale
  // action evidence usable by the reconciler.
  std::uint64_t session_generation_ = 0;
  std::string group_id_;
  MetaFailoverTransitionId transition_id_{};
  MetaFailoverActionId action_id_{};
  std::string candidate_node_id_;
  MetaAssignmentId candidate_assignment_id_{};
  MetaBootIncarnation candidate_boot_id_{};
  MetaRequestId prepared_context_id_{};
  std::int64_t received_unix_ms_ = 0;
  std::int64_t expires_unix_ms_ = 0;
  bool operator==(const MetaCandidatePreparedObs&) const = default;
};

struct MetaActionFailedObs {
  std::string group_id_;
  MetaFailoverTransitionId transition_id_{};
  MetaFailoverActionId action_id_{};
  std::string candidate_node_id_;
  MetaAssignmentId candidate_assignment_id_{};
  MetaBootIncarnation candidate_boot_id_{};
  std::uint64_t population_manifest_revision_ = 0;
  MetaHash256 population_manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  std::string failure_class_;
  std::string failure_detail_;
  std::int64_t received_unix_ms_ = 0;
  std::int64_t expires_unix_ms_ = 0;
  bool operator==(const MetaActionFailedObs&) const = default;
};

using MetaFailoverObservationPayload =
    std::variant<MetaSourcePausedObs, MetaCandidatePreparedObs,
                 MetaActionFailedObs>;

struct MetaFailoverObservationObs {
  MetaFailoverObservationPayload payload_;
  bool operator==(const MetaFailoverObservationObs&) const = default;
};

using MetaObservationPayload =
    std::variant<MetaNodeBootObs, MetaNodeHealthObs, MetaCandidateProgressObs,
                 MetaFailoverObservationObs>;

struct MetaObservation {
  MetaObservationIdentity identity_;
  MetaObservationPayload payload_;
  int64_t received_unix_ms_ = 0;  // volatile-local receive time (TTL only)
};

// ------------------------------------------------- committed facts interface

// Read-only projection of committed MetaStores used for freshness checks.
// Implemented by the state-machine/coordinator wiring and by test fakes.
// All "unknown" answers must be conservative (0/false/zero hash) so that
// unknown committed state rejects rather than admits. A zero manifest digest
// is therefore a sentinel, never an admissible committed manifest anchor.
class MetaCommittedFacts {
 public:
  virtual ~MetaCommittedFacts() = default;

  virtual bool IsActiveNode(std::string_view node_id) const = 0;
  // 0 when the group does not exist.
  virtual uint64_t CurrentGroupTerm(std::string_view group_id) const = 0;
  // 0 when the group does not exist.
  virtual uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const = 0;
  // Zero hash when the group or manifest does not exist.
  virtual MetaHash256 CurrentPopulationManifestDigest(
      std::string_view group_id) const = 0;
  // 0 when the group does not exist. Zero may also be the initial committed
  // epoch; term matching distinguishes a real group from unknown state.
  virtual uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const = 0;
  // True only for the exact current membership incarnation. An active node
  // outside the group, or a removed-and-readded node using an old assignment,
  // must not contribute candidate or failover observations.
  virtual bool AssignmentMatches(
      std::string_view group_id, std::string_view node_id,
      const MetaAssignmentId& assignment_id) const = 0;
  // True only when this exact member assignment is the committed owner.
  virtual bool IsOwnerAssignment(
      std::string_view group_id, std::string_view node_id,
      const MetaAssignmentId& assignment_id) const = 0;
  // True only for the retained historical owner under an exact active
  // uncontrolled target-term fence. Observation admission additionally
  // verifies boot-local self-origin lineage against the authenticated
  // session; this committed predicate never admits an active grant owner.
  virtual bool MayReportFencedOwnerCandidate(
      const MetaCandidateProgressObs&) const {
    return false;
  }
  // True when any committed active transition binds this exact node boot as
  // its current Candidate Action. The conservative default preserves a
  // disconnect latch for adapters that cannot inspect failover state.
  virtual bool IsCurrentFailoverCandidate(std::string_view,
                                          const MetaBootIncarnation&) const {
    return true;
  }

  struct FailoverTransitionView {
    std::string group_id_;
    MetaFailoverTransition transition_;
  };

  // Finds an active transition by its globally unique identity. The default
  // keeps older test/admin adapters conservative: without committed context,
  // transition-scoped evidence is rejected.
  virtual std::optional<FailoverTransitionView> FailoverTransitionById(
      const MetaFailoverTransitionId&) const {
    return std::nullopt;
  }
};

// ------------------------------------------------------------ event auditing

enum class MetaObsAuditKind : std::uint8_t {
  kRejected,     // failed ingest validation
  kStalePurged,  // purged by a newer session generation or commit
  kTtlExpired,   // swept by TTL
};

struct MetaObsAuditEvent {
  MetaObsAuditKind kind_;
  std::string node_id_;
  std::string detail_;  // bounded
  int64_t unix_ms_ = 0;
};

// ------------------------------------------------------------------ the store

class MetaObservationStore {
 public:
  struct HeartbeatReplaceResult {
    absl::Status boot_status_;
    absl::Status health_status_;
    absl::Status candidate_status_;
    absl::Status failover_status_;
  };

  struct Limits {
    // Session keys have fixed-size authenticated node ids in production. The
    // explicit count cap also protects test/administrative adapters before an
    // observation can be checked against committed active-node facts.
    size_t max_sessions_total_ = kMaxMetaNodes;
    // A valid committed group may contain the whole node domain. Candidate
    // admission therefore uses that same bound instead of selecting the first
    // reporters by arrival order.
    size_t max_candidates_per_group_ = kMaxMetaNodes;
    size_t max_observations_total_ = 65536;
    // Charged bytes include every variable-length observation field and its
    // lookup-key copies; fixed container overhead remains count-bounded by
    // max_observations_total_. The pooled default can retain one maximum
    // direct observation frame plus one identifier-sized index allowance per
    // maximum registered node. Each node retains the latest heartbeat fields
    // and their bounded lookup keys.
    std::uint64_t max_retained_bytes_total_ =
        static_cast<std::uint64_t>(kMaxMetaNodes) *
        (cluster::control::kMaxFrameBytes +
         cluster::control::kMaxIdentifierBytes);
    std::uint64_t max_retained_bytes_per_node_ =
        cluster::control::kMaxFrameBytes +
        cluster::control::kMaxIdentifierBytes;
    size_t audit_ring_capacity_ = 4096;
    int64_t ttl_ms_ = 30000;  // expected heartbeat multiple; configurable
  };

  explicit MetaObservationStore(Limits limits);
  // Default construction delegates with the documented Limits defaults. The
  // frozen spelling `Limits limits = {}` cannot survive GCC: a default
  // argument that list-initializes a nested type trips over the member
  // initializers being "before the end of the enclosing class" (gcc 88165).
  // The two overloads expose exactly the same public surface.
  MetaObservationStore() : MetaObservationStore(Limits{}) {}
  ~MetaObservationStore();
  MetaObservationStore(MetaObservationStore&&) = delete;
  MetaObservationStore& operator=(MetaObservationStore&&) = delete;

  // Drops sessions, observations, and the volatile audit ring at a Raft role
  // edge. A new leader must authenticate and adopt fresh sessions; a former
  // leader must retain no soft evidence that could be reused after re-election.
  void ResetForLeadershipChange();

  // Session lifecycle (trusted session layer only). Adopting a generation
  // atomically purges all of the node's older observations. Production passes
  // the ClientHello replication history; the optional form exists for legacy
  // diagnostic/test adapters that never make source-lineage decisions.
  // Generations must increase monotonically per node; adopting an older/equal
  // generation is a domain rejection.
  absl::Status AdoptSession(const MetaObservationIdentity& identity,
                            int64_t now_unix_ms,
                            std::optional<MetaReplicationHistoryId>
                                replication_history_id = std::nullopt);

  // Records an exact authenticated disconnect and immediately withdraws
  // candidate evidence. A stale completion from an older generation cannot
  // disconnect a replacement session or clear its candidate; source and
  // diagnostic evidence retain their ordinary TTL semantics.
  void InvalidateCandidateOnDisconnect(const MetaObservationIdentity& identity,
                                       int64_t now_unix_ms);

  // Ingest one observation. Validates identity (registered active node,
  // current generation) and freshness (facts) before storing; rejection is
  // recorded in the audit ring and returned as a domain error.
  absl::Status Ingest(MetaObservation observation,
                      const MetaCommittedFacts& facts, int64_t now_unix_ms);

  // Replaces common liveness/diagnostic health and the role-derived candidate
  // state under one lock. The fixed-size typed health used by Owner
  // serviceability is stored with the heartbeat sequence, installed-FDS
  // marker, and causal lease confirmation as one session cut; diagnostic text
  // capacity cannot splice that cut across frames. Candidate and transition
  // evidence remain replace-or-clear: absence or component rejection clears
  // the corresponding older fact. A lease-only heartbeat may retain operator
  // recovery availability for an unready, storage-healthy node, without
  // refreshing its TTL; it never retains automatic candidate progress.
  // Rejecting the heartbeat identity leaves the replacement session untouched.
  // The shorter overloads deliberately supply
  // nullopt for fields they cannot carry and therefore clear them. Component
  // statuses report diagnostic and role-evidence admission independently.
  // The full overload takes both clock cuts: Unix time retains the existing
  // generic observation TTL/audit semantics, while steady time is stored only
  // for Owner heartbeat and causal-lease freshness.
  HeartbeatReplaceResult ReplaceHeartbeat(
      const MetaObservationIdentity& identity, MetaNodeHealthObs health,
      std::optional<MetaCandidateProgressObs> candidate,
      const MetaCommittedFacts& facts, int64_t now_unix_ms);
  HeartbeatReplaceResult ReplaceHeartbeat(
      const MetaObservationIdentity& identity, MetaNodeHealthObs health,
      std::optional<MetaCandidateProgressObs> candidate,
      std::optional<MetaFailoverObservationObs> failover,
      const MetaCommittedFacts& facts, int64_t now_unix_ms);
  HeartbeatReplaceResult ReplaceHeartbeat(
      const MetaObservationIdentity& identity, MetaNodeHealthObs health,
      std::optional<MetaCandidateProgressObs> candidate,
      std::optional<MetaFailoverObservationObs> failover,
      std::optional<MetaObservedFailoverProjection> failover_projection,
      const MetaCommittedFacts& facts, int64_t now_unix_ms);
  HeartbeatReplaceResult ReplaceHeartbeat(
      const MetaObservationIdentity& identity, MetaNodeHealthObs health,
      std::optional<MetaCandidateProgressObs> candidate,
      std::optional<MetaFailoverObservationObs> failover,
      std::optional<MetaObservedFailoverProjection> failover_projection,
      std::optional<MetaObservedOwnerProjection> owner_projection,
      std::uint64_t heartbeat_sequence,
      std::optional<std::uint64_t> confirmed_grant_sequence,
      const MetaCommittedFacts& facts, int64_t now_unix_ms,
      std::uint64_t now_steady_ms, bool lease_only = false);

  // Records the handoff or Grant consequence immediately before the Ack's
  // first send attempt. A failed network write can be ambiguous, so a Grant
  // attempt may delay failure by at most one finite lease but cannot forget
  // authority that Data might have installed. The exact current session and
  // heartbeat must match; a Grant must also match the installed projection.
  absl::Status RecordOwnerLeaseDecisionAttempt(
      const MetaObservationIdentity& identity, std::uint64_t heartbeat_sequence,
      const cluster::control::LeaseDecision& decision);

  // Retires an older handoff denial only after a later NodeNotReady Ack has
  // been written successfully. The server evaluates handoff quarantine
  // before publishing that denial, so it proves the deadline elapsed. Other
  // denial kinds bypass the guard and cannot retire the marker. Grant
  // attempts already clear it and enter the possible-lease envelope above.
  absl::Status RecordOwnerLeaseDecisionWritten(
      const MetaObservationIdentity& identity, std::uint64_t heartbeat_sequence,
      const cluster::control::LeaseDecision& decision);

  // Commit-driven invalidation: drop observations whose node, assignment,
  // term, manifest, or partition-epoch bindings no longer match committed
  // state. Candidate reporter-local history is session-bound;
  // its independent source history is a compatibility-domain anchor rather
  // than a committed Meta fact. Events are audited, and callers run this
  // after each committed batch.
  void RevalidateAll(const MetaCommittedFacts& facts, int64_t now_unix_ms);

  // TTL sweep (events audited).
  void SweepExpired(int64_t now_unix_ms);
  // Amortized ingest-path sweep. Returns true only when this call performed a
  // full scan. At the default TTL the scan runs at most once per second;
  // explicit query paths retain SweepExpired's exact boundary semantics.
  bool MaybeSweepExpired(int64_t now_unix_ms);

  // Read paths re-filter against current committed facts. Planning queries
  // also apply TTL at the caller's fixed observation cut.
  std::optional<MetaCandidateProgressObs> LatestCandidateProgress(
      std::string_view group_id, const MetaCommittedFacts& facts) const;
  std::vector<MetaCandidateProgressObs> CandidateProgressFor(
      std::string_view group_id, const MetaCommittedFacts& facts) const;
  // Applies TTL at the caller's fixed planning instant without mutating the
  // deadline or depending on a global observation revision. Automatic
  // selectors use the default, which excludes unknown-frontier populations;
  // explicit operator actions may request those scoped availability reports.
  std::vector<MetaCandidateProgressObs> LiveCandidateProgressFor(
      std::string_view group_id, const MetaCommittedFacts& facts,
      int64_t now_unix_ms, bool include_operator_recovery = false) const;
  // Passing a fixed decision time applies the store-wide observation TTL.
  // Omitting it returns the committed-anchor-matching observation so a caller
  // can apply its own freshness window, such as failover source grace;
  // diagnostics may use the same unexpired-agnostic view.
  std::optional<MetaObservation> LatestForNode(
      std::string_view node_id, const MetaCommittedFacts& facts,
      std::optional<int64_t> now_unix_ms = std::nullopt) const;
  // Returns only TTL-fresh transition evidence that still matches committed
  // transition, action, membership, boot, and current-session anchors.
  // SourcePaused is keyed by transition; candidate outcomes additionally bind
  // the exact action so evidence from a superseded attempt cannot be reused.
  // Authority decisions correlate candidate results with SessionStateFor's
  // trusted installed-FDS projection marker.
  std::optional<MetaSourcePausedObs> SourcePausedFor(
      const MetaFailoverTransitionId& transition_id,
      const MetaCommittedFacts& facts, int64_t now_unix_ms) const;
  std::optional<MetaCandidatePreparedObs> CandidatePreparedFor(
      const MetaFailoverTransitionId& transition_id,
      const MetaFailoverActionId& action_id, const MetaCommittedFacts& facts,
      int64_t now_unix_ms) const;
  std::optional<MetaActionFailedObs> ActionFailedFor(
      const MetaFailoverTransitionId& transition_id,
      const MetaFailoverActionId& action_id, const MetaCommittedFacts& facts,
      int64_t now_unix_ms) const;

  // Exposes leader-local authenticated session state for failover liveness
  // decisions. Disconnect latches and installed-FDS projection markers are
  // volatile and are reset at a Meta leadership edge.
  std::optional<uint64_t> CurrentGeneration(std::string_view node_id) const;
  std::optional<MetaObservedSessionState> SessionStateFor(
      std::string_view node_id) const;
  std::optional<MetaObservedOwnerState> OwnerObservationFor(
      std::string_view node_id) const;

  std::vector<MetaObsAuditEvent> AuditRing() const;
  size_t size() const;
  // Exact logical byte charge used by admission. These accessors make
  // capacity telemetry and boundary tests observe the same accounting that
  // guards insertion; transient query copies and the separately bounded audit
  // ring are intentionally excluded.
  std::uint64_t retained_bytes() const;
  std::uint64_t retained_bytes_for_node(std::string_view node_id) const;

 private:
  absl::Status IngestLocked(MetaObservation observation,
                            const MetaCommittedFacts& facts,
                            int64_t now_unix_ms);
  void ClearCandidatesForNodeLocked(std::string_view node_id,
                                    int64_t now_unix_ms,
                                    std::string_view detail,
                                    bool retain_operator_recovery = false);
  void ClearCandidateFailoverForNodeLocked(std::string_view node_id,
                                           int64_t now_unix_ms,
                                           std::string_view detail);
  void SweepExpiredLocked(int64_t now_unix_ms);

  Limits limits_;
  // Defined in the .cpp: per-node current generation and exact resource
  // usage; per-(node, kind) latest observations; per-group/per-operation
  // bounded evidence sets; audit ring.
  struct Impl;
  mutable std::mutex mutex_;
  std::unique_ptr<Impl> impl_;
};

}  // namespace lavik::meta
