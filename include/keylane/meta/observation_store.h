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
//     session_generation} triple. boot_incarnation is opaque and is NEVER
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
//     stale, newer is forged: both rejected). Manifest ids and operation
//     evidence histories must match committed values/bindings. Candidate
//     reporter history comes from ClientHello, while its independent source
//     history is a compatibility-domain anchor for the internal selector.
//
// Rejections and evictions are appended to a bounded audit ring buffer for
// operators (MetaObsAuditEvent); this ring is debugging surface, not the
// durable audit trail (that lives in MetaAuditStore).
//
// Resource bounds are layered: sessions and observation entries bound fixed
// container overhead, candidate/evidence domain caps stop one group,
// operation, or reporter from monopolizing keys, and exact charged-byte
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
#include <vector>

#include "absl/status/status.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/operation_store.h"

namespace keylane::meta {

// ---------------------------------------------------------------- identities

struct MetaObservationIdentity {
  std::string node_id_;
  MetaBootIncarnation boot_incarnation_;
  uint64_t session_generation_ = 0;
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

struct MetaCandidateProgressObs {
  // Copied from the authenticated observation identity so group queries keep
  // the complete reporter incarnation instead of returning an anonymous
  // proof that a reconciler would have to join against another query.
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
  std::string source_node_id_;
  MetaAssignmentId source_assignment_id_{};
  MetaBootIncarnation source_boot_incarnation_{};
  MetaReplicationHistoryId source_replication_history_id_{};
  std::vector<std::uint64_t> applied_next_lsns_;
  std::string applied_flow_vector_;  // compatibility diagnostic rendering
  std::string backlog_coverage_;     // opaque, bounded
  std::string readiness_;            // opaque, bounded
  bool storage_ready_ = false;
  bool population_ready_ = false;
  bool draining_ = false;
  std::int64_t received_unix_ms_ = 0;
  std::int64_t expires_unix_ms_ = 0;
  bool operator==(const MetaCandidateProgressObs&) const = default;
};

struct MetaOperationEvidenceObs {
  // Bound to the authenticated reporter and its exact current membership
  // incarnation; neither field is accepted as an independent identity claim.
  std::string node_id_;
  MetaBootIncarnation boot_incarnation_{};
  MetaAssignmentId assignment_id_{};
  MetaOperationId operation_id_;
  std::string kind_phase_;  // bounded; the phase this evidence supports
  MetaHash256 evidence_hash_;
  std::string evidence_;  // bounded normalized evidence payload
  // Committed population and operation-history anchors for this evidence:
  std::string group_id_;
  uint64_t group_term_ = 0;
  uint64_t population_manifest_revision_ = 0;
  uint64_t partition_replication_epoch_ = 0;
  MetaReplicationHistoryId replication_history_id_{};
  bool operator==(const MetaOperationEvidenceObs&) const = default;
};

// Canonical conversion used after EvidenceForOperation returns a validated,
// self-contained observation. The manifest digest comes from the same
// committed view used for that query; every observation/session anchor is
// copied without a second lookup that could race session supersession.
MetaEvidenceSummary SummarizeOperationEvidence(
    const MetaOperationEvidenceObs& evidence,
    const MetaHash256& population_manifest_digest);

using MetaObservationPayload =
    std::variant<MetaNodeBootObs, MetaNodeHealthObs, MetaCandidateProgressObs,
                 MetaOperationEvidenceObs>;

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
  // must not contribute candidate or operation evidence.
  virtual bool AssignmentMatches(
      std::string_view group_id, std::string_view node_id,
      const MetaAssignmentId& assignment_id) const = 0;
  // True only when this exact member assignment is the committed owner.
  virtual bool IsOwnerAssignment(
      std::string_view group_id, std::string_view node_id,
      const MetaAssignmentId& assignment_id) const = 0;
  // True when the operation exists (including archived summaries) and is not
  // in a terminal lifecycle state.
  virtual bool OperationNonTerminal(const MetaOperationId& id) const = 0;
  // True when `history_id` is bound to the operation by a committed journal
  // record (the history anchor for evidence freshness).
  virtual bool HistoryBoundToOperation(
      const MetaOperationId& id,
      const MetaReplicationHistoryId& history_id) const = 0;
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
    // Operation evidence is latest-wins by (operation, node, kind/phase).
    // One reporter and one operation domain may each retain no more distinct
    // phase keys than a durable operation record can ever consume.
    size_t max_evidence_phases_per_node_ =
        kMaxMetaOperationEvidencePerRecord;
    size_t max_evidence_per_operation_ =
        kMaxMetaOperationEvidencePerRecord;
    size_t max_observations_total_ = 65536;
    // Charged bytes include every variable-length observation field and its
    // lookup-key copies; fixed container overhead remains count-bounded by
    // max_observations_total_. The pooled default can retain one maximum
    // direct observation frame plus one identifier-sized index allowance per
    // maximum registered node. A single node may retain one maximum streamed
    // evidence object together with one direct heartbeat and its index.
    std::uint64_t max_retained_bytes_total_ =
        static_cast<std::uint64_t>(kMaxMetaNodes) *
        (cluster::control::kMaxFrameBytes +
         cluster::control::kMaxIdentifierBytes);
    std::uint64_t max_retained_bytes_per_node_ =
        cluster::control::kMaxOperationEvidenceTransferBytes +
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
  // atomically purges all of the node's older observations. Generations must
  // increase monotonically per node; adopting an older/equal generation is a
  // domain rejection.
  absl::Status AdoptSession(const MetaObservationIdentity& identity,
                            int64_t now_unix_ms);

  // Immediately withdraws candidate evidence when the exact authenticated
  // session disconnects. A stale completion from an older generation cannot
  // clear a replacement session's candidate; health and diagnostic evidence
  // retain their ordinary TTL semantics.
  void InvalidateCandidateOnDisconnect(
      const MetaObservationIdentity& identity, int64_t now_unix_ms);

  // Ingest one observation. Validates identity (registered active node,
  // current generation) and freshness (facts) before storing; rejection is
  // recorded in the audit ring and returned as a domain error.
  absl::Status Ingest(MetaObservation observation,
                      const MetaCommittedFacts& facts, int64_t now_unix_ms);

  // Replaces common liveness/health and the role-derived candidate state under
  // one lock. Absence or rejection of candidate evidence clears every older
  // candidate for this node, so a promotion heartbeat cannot preserve the
  // node's previous replica role.
  HeartbeatReplaceResult ReplaceHeartbeat(
      const MetaObservationIdentity& identity, MetaNodeHealthObs health,
      std::optional<MetaCandidateProgressObs> candidate,
      const MetaCommittedFacts& facts, int64_t now_unix_ms);

  // Commit-driven invalidation: drop observations whose node, assignment,
  // term, manifest, or partition-epoch bindings no longer match committed
  // state; operation evidence additionally checks its committed history and
  // operation anchors. Candidate reporter-local history is session-bound;
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

  // Read paths re-filter against current facts and never return stale data.
  std::optional<MetaCandidateProgressObs> LatestCandidateProgress(
      std::string_view group_id, const MetaCommittedFacts& facts) const;
  std::vector<MetaCandidateProgressObs> CandidateProgressFor(
      std::string_view group_id, const MetaCommittedFacts& facts) const;
  // Selector-only view. Unlike the compatibility diagnostics above, this
  // applies TTL at the caller's fixed planning instant without mutating the
  // deadline or depending on a global observation revision.
  std::vector<MetaCandidateProgressObs> LiveCandidateProgressFor(
      std::string_view group_id, const MetaCommittedFacts& facts,
      int64_t now_unix_ms) const;
  std::optional<MetaObservation> LatestForNode(
      std::string_view node_id, const MetaCommittedFacts& facts) const;
  std::vector<MetaOperationEvidenceObs> EvidenceForOperation(
      const MetaOperationId& id, const MetaCommittedFacts& facts) const;

  std::optional<uint64_t> CurrentGeneration(std::string_view node_id) const;

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

}  // namespace keylane::meta
