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
//   - Invalidation is threefold: checked at ingest against MetaCommittedFacts,
//     actively purged when committed state changes (RevalidateAll), and
//     re-filtered at every read (Latest*/List* take facts and filter again),
//     so a commit landing between ingest and query cannot leak stale data.
//   - Group-bound observations require the authenticated node's exact current
//     membership assignment and group_term == committed current term (older is
//     stale, newer is forged: both rejected). Manifest/history ids must match
//     committed values or operation-committed bindings.
//
// Rejections and evictions are appended to a bounded audit ring buffer for
// operators (MetaObsAuditEvent); this ring is debugging surface, not the
// durable audit trail (that lives in MetaAuditStore).
//
// Threading: public operations are internally serialized. This is required
// because authenticated sessions ingest on control-channel workers while the
// coordinator invalidates state on commit and leadership workers. Time may be
// read here (volatile state only) — TTL expiry uses caller-supplied `now`.

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "keylane/meta/commands.h"

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
  std::string health_;  // bounded free-form status (e.g. "ok", degraded flags)
  bool operator==(const MetaNodeHealthObs&) const = default;
};

struct MetaCandidateProgressObs {
  // Copied from the authenticated observation identity so group queries keep
  // the complete reporter incarnation instead of returning an anonymous
  // proof that a reconciler would have to join against another query.
  std::string node_id_;
  MetaBootIncarnation boot_incarnation_{};
  std::string group_id_;
  MetaAssignmentId assignment_id_{};
  uint64_t group_term_ = 0;
  uint64_t population_manifest_revision_ = 0;
  uint64_t partition_replication_epoch_ = 0;
  MetaReplicationHistoryId replication_history_id_{};
  std::string applied_flow_vector_;  // opaque and bounded
  std::string backlog_coverage_;     // opaque, bounded
  std::string readiness_;            // opaque, bounded
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
// All "unknown" answers must be conservative (0/false) so that unknown
// committed state rejects rather than admits.
class MetaCommittedFacts {
 public:
  virtual ~MetaCommittedFacts() = default;

  virtual bool IsActiveNode(std::string_view node_id) const = 0;
  // 0 when the group does not exist.
  virtual uint64_t CurrentGroupTerm(std::string_view group_id) const = 0;
  // 0 when the group does not exist.
  virtual uint64_t CurrentPopulationManifestRevision(
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
  struct Limits {
    size_t max_candidates_per_group_ = 16;
    size_t max_observations_total_ = 65536;
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

  // Ingest one observation. Validates identity (registered active node,
  // current generation) and freshness (facts) before storing; rejection is
  // recorded in the audit ring and returned as a domain error.
  absl::Status Ingest(MetaObservation observation,
                      const MetaCommittedFacts& facts, int64_t now_unix_ms);

  // Commit-driven invalidation: drop observations whose
  // term/manifest/partition-epoch/history bindings no longer match committed
  // state (events audited). Callers run this after each committed batch.
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
  std::optional<MetaObservation> LatestForNode(
      std::string_view node_id, const MetaCommittedFacts& facts) const;
  std::vector<MetaOperationEvidenceObs> EvidenceForOperation(
      const MetaOperationId& id, const MetaCommittedFacts& facts) const;

  std::optional<uint64_t> CurrentGeneration(std::string_view node_id) const;

  std::vector<MetaObsAuditEvent> AuditRing() const;
  size_t size() const;

 private:
  void SweepExpiredLocked(int64_t now_unix_ms);

  Limits limits_;
  // Defined in the .cpp: per-node current generation; per-(node, kind)
  // latest observations; per-group bounded candidate sets; audit ring.
  struct Impl;
  mutable std::mutex mutex_;
  std::unique_ptr<Impl> impl_;
};

}  // namespace keylane::meta
