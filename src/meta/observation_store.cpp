#include "keylane/meta/observation_store.h"

#include <algorithm>
#include <cassert>
#include <deque>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "keylane/meta/encoding.h"
#include "keylane/meta/hash.h"

namespace keylane::meta {

MetaEvidenceSummary SummarizeOperationEvidence(
    const MetaOperationEvidenceObs& evidence,
    const MetaHash256& population_manifest_digest) {
  MetaEvidenceSummary summary;
  summary.node_id_ = evidence.node_id_;
  summary.group_id_ = evidence.group_id_;
  summary.assignment_id_ = evidence.assignment_id_;
  summary.boot_incarnation_ = evidence.boot_incarnation_;
  summary.group_term_ = evidence.group_term_;
  summary.population_manifest_revision_ =
      evidence.population_manifest_revision_;
  summary.population_manifest_digest_ = population_manifest_digest;
  summary.partition_replication_epoch_ = evidence.partition_replication_epoch_;
  summary.replication_history_id_ = evidence.replication_history_id_;
  summary.operation_id_ = evidence.operation_id_;
  summary.kind_hash_ = evidence.evidence_hash_;
  return summary;
}

namespace {

// Observation payload strings are soft state but remain bounded. The cap
// matches the committed-side payload cap; an
// over-cap field rejects the ingest rather than being truncated.
constexpr std::uint32_t kMaxObsFieldBytes = kMaxMetaPayloadBytes;
// Audit details are single-token (no whitespace) so the ctl `obsaudit` line
// protocol can dump them verbatim; the cap keeps the bounded ring also
// byte-bounded.
constexpr std::size_t kMaxAuditDetailBytes = 256;

bool ObservationExpired(const MetaObservation& observation,
                        int64_t now_unix_ms, int64_t ttl_ms) {
  // Backwards wall-clock movement is conservative: it cannot expire evidence.
  // Avoid subtracting until the ordering check has ruled out underflow.
  return now_unix_ms > observation.received_unix_ms_ &&
         now_unix_ms - observation.received_unix_ms_ > ttl_ms;
}

std::string BoundedDetail(std::string detail) {
  if (detail.size() > kMaxAuditDetailBytes) {
    detail.resize(kMaxAuditDetailBytes);
  }
  return detail;
}

// The freshness verdict of the committed population anchor shared by
// candidate and evidence observations. group_term 0 can never anchor either:
// a group's term only begins via BeginGroupTerm(T >= 1), and
// MetaCommittedFacts reports 0 for a group that does not exist at all.
absl::Status CheckPopulationAnchor(std::string_view group_id,
                                   std::uint64_t term, std::uint64_t manifest,
                                   std::uint64_t partition_epoch,
                                   const MetaCommittedFacts& facts) {
  if (group_id.empty() || group_id.size() > kMaxMetaGroupIdBytes) {
    return MetaDomainRejectError("bad-group-id");
  }
  if (term == 0) {
    return MetaDomainRejectError("term-not-begun");
  }
  const std::uint64_t committed_term = facts.CurrentGroupTerm(group_id);
  if (term != committed_term) {
    return MetaDomainRejectError("term-mismatch:committed=" +
                                 std::to_string(committed_term));
  }
  const std::uint64_t committed_manifest =
      facts.CurrentPopulationManifestRevision(group_id);
  if (manifest != committed_manifest) {
    return MetaDomainRejectError("manifest-mismatch:committed=" +
                                 std::to_string(committed_manifest));
  }
  const std::uint64_t committed_partition_epoch =
      facts.CurrentPartitionReplicationEpoch(group_id);
  if (partition_epoch != committed_partition_epoch) {
    return MetaDomainRejectError("partition-epoch-mismatch:committed=" +
                                 std::to_string(committed_partition_epoch));
  }
  return absl::OkStatus();
}

absl::Status CheckFieldSize(std::string_view field, std::size_t max_bytes,
                            std::string_view name) {
  if (field.size() > max_bytes) {
    return MetaDomainRejectError("field-too-large:" + std::string(name));
  }
  return absl::OkStatus();
}

// Logical charged bytes conservatively cover every variable-length string
// retained by an observation and its lookup indexes. Candidate group keys are
// charged once per entry even though the outer map shares one key; this keeps
// replacement/removal accounting local and never undercounts allocations.
// Fixed-size identities, map nodes, and string objects are bounded by the
// independent entry/session/domain count limits.
std::uint64_t ChargedBytes(const MetaObservation& observation) {
  const auto bytes = [](std::string_view value) -> std::uint64_t {
    return static_cast<std::uint64_t>(value.size());
  };
  const std::uint64_t identity_node = bytes(observation.identity_.node_id_);
  if (std::holds_alternative<MetaNodeBootObs>(observation.payload_)) {
    return 2 * identity_node;  // identity plus boot_by_node_ key
  }
  if (const auto* health =
          std::get_if<MetaNodeHealthObs>(&observation.payload_)) {
    return 2 * identity_node + bytes(health->health_);
  }
  if (const auto* candidate =
          std::get_if<MetaCandidateProgressObs>(&observation.payload_)) {
    return identity_node + bytes(candidate->node_id_) +
           2 * bytes(candidate->group_id_) +
           bytes(candidate->source_node_id_) +
           static_cast<std::uint64_t>(candidate->applied_next_lsns_.size()) *
               sizeof(std::uint64_t) +
           bytes(candidate->applied_flow_vector_) +
           bytes(candidate->backlog_coverage_) +
           bytes(candidate->readiness_) + identity_node;
  }
  const auto& evidence =
      std::get<MetaOperationEvidenceObs>(observation.payload_);
  return identity_node + bytes(evidence.node_id_) +
         2 * bytes(evidence.kind_phase_) + bytes(evidence.evidence_) +
         bytes(evidence.group_id_) + identity_node;
}

bool ExceedsReplacementBudget(std::uint64_t current,
                              std::uint64_t replaced,
                              std::uint64_t incoming,
                              std::uint64_t limit) {
  assert(current >= replaced);
  // Written without addition so a caller-supplied UINT64_MAX limit cannot
  // turn an overflowing sum into an admission.
  return incoming > limit || current - replaced > limit - incoming;
}

}  // namespace

struct MetaObservationStore::Impl {
  // The trusted session triple of one node, fixed at AdoptSession time:
  // boot_incarnation may never change within a generation (a reboot comes
  // back with a new generation), so a mismatch under the current generation
  // is an identity violation, not an ordering question.
  struct Session {
    MetaBootIncarnation boot_incarnation_{};
    std::uint64_t generation_ = 0;
  };

  // Latest-wins key for operation evidence: each (node, kind_phase) pair
  // keeps only its newest report within the current session generation.
  struct EvidenceKey {
    std::string node_id_;
    std::string kind_phase_;
    bool operator<(const EvidenceKey& other) const {
      if (node_id_ != other.node_id_) return node_id_ < other.node_id_;
      return kind_phase_ < other.kind_phase_;
    }
  };

  struct NodeUsage {
    std::size_t observations_ = 0;
    std::size_t evidence_phases_ = 0;
    std::uint64_t retained_bytes_ = 0;
  };

  std::size_t TotalObservations() const { return total_observations_; }

  std::uint64_t NodeRetainedBytes(std::string_view node_id) const {
    const auto it = usage_by_node_.find(std::string(node_id));
    return it == usage_by_node_.end() ? 0 : it->second.retained_bytes_;
  }

  std::size_t NodeEvidencePhases(std::string_view node_id) const {
    const auto it = usage_by_node_.find(std::string(node_id));
    return it == usage_by_node_.end() ? 0 : it->second.evidence_phases_;
  }

  absl::Status CheckByteBudget(const MetaObservation& observation,
                               const MetaObservation* replaced,
                               const Limits& limits) const {
    const std::string& node_id = observation.identity_.node_id_;
    const std::uint64_t incoming = ChargedBytes(observation);
    const std::uint64_t old = replaced == nullptr ? 0 : ChargedBytes(*replaced);
    if (ExceedsReplacementBudget(retained_bytes_, old, incoming,
                                 limits.max_retained_bytes_total_)) {
      return MetaDomainRejectError("store-bytes-full");
    }
    if (ExceedsReplacementBudget(NodeRetainedBytes(node_id), old, incoming,
                                 limits.max_retained_bytes_per_node_)) {
      return MetaDomainRejectError("node-bytes-full");
    }
    return absl::OkStatus();
  }

  void AccountInsert(const MetaObservation& observation, bool evidence) {
    const std::string& node_id = observation.identity_.node_id_;
    const std::uint64_t charged = ChargedBytes(observation);
    NodeUsage& usage = usage_by_node_[node_id];
    ++usage.observations_;
    usage.evidence_phases_ += evidence ? 1 : 0;
    usage.retained_bytes_ += charged;
    ++total_observations_;
    retained_bytes_ += charged;
  }

  void AccountReplace(std::string_view node_id, std::uint64_t old_bytes,
                      std::uint64_t new_bytes) {
    NodeUsage& usage = usage_by_node_.at(std::string(node_id));
    assert(usage.retained_bytes_ >= old_bytes);
    assert(retained_bytes_ >= old_bytes);
    usage.retained_bytes_ = usage.retained_bytes_ - old_bytes + new_bytes;
    retained_bytes_ = retained_bytes_ - old_bytes + new_bytes;
  }

  void AccountErase(const MetaObservation& observation, bool evidence) {
    const std::string& node_id = observation.identity_.node_id_;
    const std::uint64_t charged = ChargedBytes(observation);
    auto usage_it = usage_by_node_.find(node_id);
    assert(usage_it != usage_by_node_.end());
    NodeUsage& usage = usage_it->second;
    assert(usage.observations_ != 0 && total_observations_ != 0);
    assert(usage.retained_bytes_ >= charged && retained_bytes_ >= charged);
    if (evidence) {
      assert(usage.evidence_phases_ != 0);
      --usage.evidence_phases_;
    }
    --usage.observations_;
    --total_observations_;
    usage.retained_bytes_ -= charged;
    retained_bytes_ -= charged;
    if (usage.observations_ == 0) {
      assert(usage.evidence_phases_ == 0 && usage.retained_bytes_ == 0);
      usage_by_node_.erase(usage_it);
    }
  }

  // Identity gate shared by ingest, commit-driven revalidation, and read
  // re-filtering: registered active node + the exact current session triple.
  absl::Status CheckIdentity(const MetaObservationIdentity& identity,
                             const MetaCommittedFacts& facts) const {
    if (!facts.IsActiveNode(identity.node_id_)) {
      return MetaDomainRejectError("node-not-active");
    }
    const auto it = sessions_.find(identity.node_id_);
    if (it == sessions_.end()) {
      return MetaDomainRejectError("no-session");
    }
    if (it->second.generation_ != identity.session_generation_) {
      return MetaDomainRejectError(
          "stale-generation:current=" + std::to_string(it->second.generation_) +
          ",got=" + std::to_string(identity.session_generation_));
    }
    if (it->second.boot_incarnation_ != identity.boot_incarnation_) {
      return MetaDomainRejectError("boot-mismatch");
    }
    return absl::OkStatus();
  }

  // Full admission check: identity plus the per-type freshness rules against
  // committed facts. Also used (status discarded) by the const read paths.
  absl::Status Validate(const MetaObservation& observation,
                        const MetaCommittedFacts& facts) const {
    const absl::Status identity = CheckIdentity(observation.identity_, facts);
    if (!identity.ok()) {
      return identity;
    }
    const auto& payload = observation.payload_;
    if (const auto* boot = std::get_if<MetaNodeBootObs>(&payload)) {
      // Liveness marker only; carries no term/manifest binding (header).
      (void)boot;
      return absl::OkStatus();
    }
    if (const auto* health = std::get_if<MetaNodeHealthObs>(&payload)) {
      return CheckFieldSize(health->health_, kMaxObsFieldBytes, "health");
    }
    if (const auto* candidate =
            std::get_if<MetaCandidateProgressObs>(&payload)) {
      if (candidate->node_id_ != observation.identity_.node_id_) {
        return MetaDomainRejectError("candidate-reporter-mismatch");
      }
      if (candidate->boot_incarnation_ !=
          observation.identity_.boot_incarnation_) {
        return MetaDomainRejectError("candidate-boot-mismatch");
      }
      if (candidate->session_generation_ !=
          observation.identity_.session_generation_) {
        return MetaDomainRejectError("candidate-session-mismatch");
      }
      const absl::Status anchor =
          CheckPopulationAnchor(candidate->group_id_, candidate->group_term_,
                                candidate->population_manifest_revision_,
                                candidate->partition_replication_epoch_, facts);
      if (!anchor.ok()) {
        return anchor;
      }
      if (!facts.AssignmentMatches(candidate->group_id_, candidate->node_id_,
                                   candidate->assignment_id_)) {
        return MetaDomainRejectError("assignment-mismatch");
      }
      // The legacy ctl observation surface may retain an opaque vector for
      // diagnostics. Only typed heartbeat candidates populate this vector and
      // source lineage, and only those enter LiveCandidateProgressFor.
      if (!candidate->applied_next_lsns_.empty()) {
        if (!candidate->storage_ready_ || !candidate->population_ready_ ||
            candidate->draining_) {
          return MetaDomainRejectError("candidate-not-ready");
        }
        if (candidate->population_manifest_digest_ !=
            facts.CurrentPopulationManifestDigest(candidate->group_id_)) {
          return MetaDomainRejectError("manifest-digest-mismatch");
        }
        if (facts.IsOwnerAssignment(candidate->group_id_, candidate->node_id_,
                                    candidate->assignment_id_)) {
          return MetaDomainRejectError("candidate-is-committed-owner");
        }
        if (!cluster::control::IsCanonicalIdentity160(
                candidate->source_node_id_)) {
          return MetaDomainRejectError("bad-candidate-source-node");
        }
        const auto is_zero = [](const auto& value) {
          return std::ranges::all_of(value,
                                     [](std::uint8_t byte) { return byte == 0; });
        };
        if (is_zero(candidate->source_assignment_id_) ||
            is_zero(candidate->source_boot_incarnation_) ||
            is_zero(candidate->source_replication_history_id_)) {
          return MetaDomainRejectError("empty-candidate-source-lineage");
        }
        if (candidate->applied_next_lsns_.size() >
            cluster::control::kMaxCandidateFlows) {
          return MetaDomainRejectError("bad-candidate-flow-count");
        }
        if (std::ranges::any_of(candidate->applied_next_lsns_,
                                [](std::uint64_t lsn) { return lsn == 0; })) {
          return MetaDomainRejectError("zero-candidate-next-lsn");
        }
      }
      // GroupRecord deliberately carries no history id because replication
      // history is scoped to a data-plane boot. Typed heartbeat candidates
      // carry an independent source history anchor used by CandidatePlanFor;
      // these legacy opaque fields remain diagnostic-only for ctl clients.
      if (const absl::Status size =
              CheckFieldSize(candidate->applied_flow_vector_,
                             kMaxObsFieldBytes, "flow");
          !size.ok()) {
        return size;
      }
      if (const absl::Status size =
              CheckFieldSize(candidate->backlog_coverage_, kMaxObsFieldBytes,
                             "backlog");
          !size.ok()) {
        return size;
      }
      return CheckFieldSize(candidate->readiness_, kMaxObsFieldBytes,
                            "readiness");
    }
    const auto& evidence = std::get<MetaOperationEvidenceObs>(payload);
    if (evidence.node_id_ != observation.identity_.node_id_) {
      return MetaDomainRejectError("evidence-reporter-mismatch");
    }
    if (evidence.boot_incarnation_ != observation.identity_.boot_incarnation_) {
      return MetaDomainRejectError("evidence-boot-mismatch");
    }
    if (!facts.OperationNonTerminal(evidence.operation_id_)) {
      return MetaDomainRejectError("operation-unknown-or-terminal");
    }
    const absl::Status anchor =
        CheckPopulationAnchor(evidence.group_id_, evidence.group_term_,
                              evidence.population_manifest_revision_,
                              evidence.partition_replication_epoch_, facts);
    if (!anchor.ok()) {
      return anchor;
    }
    if (!facts.AssignmentMatches(evidence.group_id_, evidence.node_id_,
                                 evidence.assignment_id_)) {
      return MetaDomainRejectError("assignment-mismatch");
    }
    if (!facts.HistoryBoundToOperation(evidence.operation_id_,
                                       evidence.replication_history_id_)) {
      return MetaDomainRejectError("history-not-bound");
    }
    if (const absl::Status size =
            CheckFieldSize(evidence.kind_phase_,
                           cluster::control::kMaxIdentifierBytes,
                           "kind_phase");
        !size.ok()) {
      return size;
    }
    if (const absl::Status size =
            CheckFieldSize(evidence.evidence_, kMaxObsFieldBytes, "evidence");
        !size.ok()) {
      return size;
    }
    if (evidence.evidence_hash_ != MetaSha256(evidence.evidence_)) {
      return MetaDomainRejectError("evidence-hash-mismatch");
    }
    return absl::OkStatus();
  }

  void Audit(MetaObsAuditKind kind, const std::string& node_id,
             std::string detail, std::int64_t now_unix_ms,
             std::size_t ring_capacity) {
    if (ring_capacity == 0) {
      return;
    }
    while (audit_ring_.size() >= ring_capacity) {
      audit_ring_.pop_front();  // bounded ring: the oldest event is sacrificed
    }
    audit_ring_.push_back(
        MetaObsAuditEvent{kind, BoundedDetail(node_id),
                          BoundedDetail(std::move(detail)), now_unix_ms});
  }

  // Drops every observation of one node from all four buckets, auditing each
  // drop. Buckets left empty are erased so TotalObservations stays exact.
  void PurgeNode(const std::string& node_id, const std::string& detail,
                 std::int64_t now_unix_ms, std::size_t ring_capacity) {
    if (const auto it = boot_by_node_.find(node_id);
        it != boot_by_node_.end()) {
      AccountErase(it->second, false);
      boot_by_node_.erase(it);
      Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
            ring_capacity);
    }
    if (const auto it = health_by_node_.find(node_id);
        it != health_by_node_.end()) {
      AccountErase(it->second, false);
      health_by_node_.erase(it);
      Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
            ring_capacity);
    }
    for (auto it = candidates_by_group_.begin();
         it != candidates_by_group_.end();) {
      if (const auto node_it = it->second.find(node_id);
          node_it != it->second.end()) {
        AccountErase(node_it->second, false);
        it->second.erase(node_it);
        Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
              ring_capacity);
      }
      if (it->second.empty()) {
        it = candidates_by_group_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = evidence_by_operation_.begin();
         it != evidence_by_operation_.end();) {
      for (auto key_it = it->second.begin(); key_it != it->second.end();) {
        if (key_it->first.node_id_ == node_id) {
          AccountErase(key_it->second, true);
          key_it = it->second.erase(key_it);
          Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
                ring_capacity);
        } else {
          ++key_it;
        }
      }
      if (it->second.empty()) {
        it = evidence_by_operation_.erase(it);
      } else {
        ++it;
      }
    }
    assert(!usage_by_node_.contains(node_id));
  }

  std::map<std::string, Session> sessions_;
  std::map<std::string, MetaObservation> boot_by_node_;
  std::map<std::string, MetaObservation> health_by_node_;
  // group_id -> node_id -> that node's latest candidate for the group;
  // per-group node count is the bounded candidate set (Limits).
  std::map<std::string, std::map<std::string, MetaObservation>>
      candidates_by_group_;
  // operation_id -> (node, phase) -> latest evidence.
  std::map<MetaOperationId, std::map<EvidenceKey, MetaObservation>>
      evidence_by_operation_;
  // FIFO overwrite-oldest ring; debugging surface, not the durable audit
  // trail (that is MetaAuditStore).
  std::deque<MetaObsAuditEvent> audit_ring_;
  std::optional<std::int64_t> last_periodic_sweep_unix_ms_;
  std::map<std::string, NodeUsage> usage_by_node_;
  std::size_t total_observations_ = 0;
  std::uint64_t retained_bytes_ = 0;
};

MetaObservationStore::MetaObservationStore(Limits limits)
    : limits_(std::move(limits)), impl_(std::make_unique<Impl>()) {}

MetaObservationStore::~MetaObservationStore() = default;

void MetaObservationStore::ResetForLeadershipChange() {
  std::lock_guard<std::mutex> lock(mutex_);
  impl_ = std::make_unique<Impl>();
}

absl::Status MetaObservationStore::AdoptSession(
    const MetaObservationIdentity& identity, int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  Impl& impl = *impl_;
  if (identity.node_id_.empty() || identity.node_id_.size() > kMetaNodeIdBytes) {
    const std::string detail = "bad-node-id";
    impl.Audit(MetaObsAuditKind::kRejected, identity.node_id_, detail,
               now_unix_ms, limits_.audit_ring_capacity_);
    return MetaDomainRejectError(detail);
  }
  const auto it = impl.sessions_.find(identity.node_id_);
  if (it != impl.sessions_.end() &&
      identity.session_generation_ <= it->second.generation_) {
    // Generations are a per-node monotonic sequence issued by the trusted
    // session layer: an equal/older one is replayed or forged state, never a
    // new session. The store's state is untouched.
    const std::string detail =
        "stale-session-generation:current=" +
        std::to_string(it->second.generation_) +
        ",got=" + std::to_string(identity.session_generation_);
    impl.Audit(MetaObsAuditKind::kRejected, identity.node_id_, detail,
               now_unix_ms, limits_.audit_ring_capacity_);
    return MetaDomainRejectError(detail);
  }
  if (it == impl.sessions_.end() &&
      impl.sessions_.size() >= limits_.max_sessions_total_) {
    const std::string detail = "session-store-full";
    impl.Audit(MetaObsAuditKind::kRejected, identity.node_id_, detail,
               now_unix_ms, limits_.audit_ring_capacity_);
    return MetaDomainRejectError(detail);
  }
  // Atomic purge of everything the old generation reported (header contract);
  // only then does the new triple become current.
  impl.PurgeNode(identity.node_id_,
                 "superseded-by-generation:" +
                     std::to_string(identity.session_generation_),
                 now_unix_ms, limits_.audit_ring_capacity_);
  impl.sessions_[identity.node_id_] =
      Impl::Session{identity.boot_incarnation_, identity.session_generation_};
  return absl::OkStatus();
}

void MetaObservationStore::InvalidateCandidateOnDisconnect(
    const MetaObservationIdentity& identity, int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto session = impl_->sessions_.find(identity.node_id_);
  if (session == impl_->sessions_.end() ||
      session->second.generation_ != identity.session_generation_ ||
      session->second.boot_incarnation_ != identity.boot_incarnation_) {
    return;
  }
  ClearCandidatesForNodeLocked(identity.node_id_, now_unix_ms,
                               "session-disconnected");
}

absl::Status MetaObservationStore::Ingest(MetaObservation observation,
                                          const MetaCommittedFacts& facts,
                                          int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  return IngestLocked(std::move(observation), facts, now_unix_ms);
}

absl::Status MetaObservationStore::IngestLocked(
    MetaObservation observation, const MetaCommittedFacts& facts,
    int64_t now_unix_ms) {
  Impl& impl = *impl_;
  if (auto* candidate =
          std::get_if<MetaCandidateProgressObs>(&observation.payload_);
      candidate != nullptr && candidate->session_generation_ == 0) {
    // Preserve the administrative/test observation adapter: it predates the
    // explicit payload copy but still arrives through a trusted identity.
    candidate->session_generation_ = observation.identity_.session_generation_;
  }
  const absl::Status valid = impl.Validate(observation, facts);
  if (!valid.ok()) {
    const std::string detail(valid.message());
    impl.Audit(MetaObsAuditKind::kRejected, observation.identity_.node_id_,
               detail, now_unix_ms, limits_.audit_ring_capacity_);
    return MetaDomainRejectError(detail);
  }
  observation.received_unix_ms_ = now_unix_ms;  // volatile-local TTL clock
  const std::string& node_id = observation.identity_.node_id_;
  const auto reject = [&](std::string detail) -> absl::Status {
    impl.Audit(MetaObsAuditKind::kRejected, node_id, detail, now_unix_ms,
               limits_.audit_ring_capacity_);
    return MetaDomainRejectError(detail);
  };
  const auto check_budget = [&](const MetaObservation* replaced) {
    return impl.CheckByteBudget(observation, replaced, limits_);
  };
  const auto store_latest =
      [&](std::map<std::string, MetaObservation>& bucket) -> absl::Status {
    const auto existing = bucket.find(node_id);
    if (existing == bucket.end() &&
        impl.TotalObservations() >= limits_.max_observations_total_) {
      return reject("store-full");
    }
    const absl::Status budget =
        check_budget(existing == bucket.end() ? nullptr : &existing->second);
    if (!budget.ok()) {
      return reject(std::string(budget.message()));
    }
    if (existing == bucket.end()) {
      // The map key must not alias the observation string being moved: C++
      // does not order emplace argument evaluation.
      std::string node_key = node_id;
      const auto inserted =
          bucket.emplace(std::move(node_key), std::move(observation));
      assert(inserted.second);
      impl.AccountInsert(inserted.first->second, false);
    } else {
      const std::uint64_t old_bytes = ChargedBytes(existing->second);
      const std::uint64_t new_bytes = ChargedBytes(observation);
      existing->second = std::move(observation);
      impl.AccountReplace(existing->first, old_bytes, new_bytes);
    }
    return absl::OkStatus();
  };

  if (std::holds_alternative<MetaNodeBootObs>(observation.payload_)) {
    return store_latest(impl.boot_by_node_);
  }
  if (std::holds_alternative<MetaNodeHealthObs>(observation.payload_)) {
    return store_latest(impl.health_by_node_);
  }
  if (const auto* candidate =
          std::get_if<MetaCandidateProgressObs>(&observation.payload_)) {
    auto group_it = impl.candidates_by_group_.find(candidate->group_id_);
    auto existing = group_it == impl.candidates_by_group_.end()
                        ? std::map<std::string, MetaObservation>::iterator{}
                        : group_it->second.find(node_id);
    const bool is_new = group_it == impl.candidates_by_group_.end() ||
                        existing == group_it->second.end();
    if (is_new) {
      // Hard caps fail safe: a NEW node beyond the bounded
      // per-group candidate set or the total cap is rejected, never silently
      // squeezed in; refreshing an existing key never grows the state.
      const std::size_t group_size =
          group_it == impl.candidates_by_group_.end()
              ? 0
              : group_it->second.size();
      if (group_size >= limits_.max_candidates_per_group_) {
        return reject("candidate-set-full:group=" + candidate->group_id_);
      }
      if (impl.TotalObservations() >= limits_.max_observations_total_) {
        return reject("store-full");
      }
    }
    const MetaObservation* replaced = is_new ? nullptr : &existing->second;
    const absl::Status budget = check_budget(replaced);
    if (!budget.ok()) {
      return reject(std::string(budget.message()));
    }
    if (group_it == impl.candidates_by_group_.end()) {
      group_it = impl.candidates_by_group_
                     .emplace(candidate->group_id_,
                              std::map<std::string, MetaObservation>{})
                     .first;
    }
    std::map<std::string, MetaObservation>& by_node = group_it->second;
    if (is_new) {
      std::string node_key = node_id;
      const auto inserted =
          by_node.emplace(std::move(node_key), std::move(observation));
      assert(inserted.second);
      impl.AccountInsert(inserted.first->second, false);
    } else {
      // `existing` belongs to by_node because a missing group always implies
      // is_new. Capture its charge before move-assignment replaces the value.
      const std::uint64_t old_bytes = ChargedBytes(existing->second);
      const std::uint64_t new_bytes = ChargedBytes(observation);
      existing->second = std::move(observation);
      impl.AccountReplace(existing->first, old_bytes, new_bytes);
    }
    return absl::OkStatus();
  }

  const auto& evidence =
      std::get<MetaOperationEvidenceObs>(observation.payload_);
  auto op_it = impl.evidence_by_operation_.find(evidence.operation_id_);
  const Impl::EvidenceKey key{node_id, evidence.kind_phase_};
  auto existing =
      op_it == impl.evidence_by_operation_.end()
          ? std::map<Impl::EvidenceKey, MetaObservation>::iterator{}
          : op_it->second.find(key);
  const bool is_new = op_it == impl.evidence_by_operation_.end() ||
                      existing == op_it->second.end();
  if (is_new) {
    if (impl.NodeEvidencePhases(node_id) >=
        limits_.max_evidence_phases_per_node_) {
      return reject("evidence-phase-set-full:node");
    }
    const std::size_t operation_size =
        op_it == impl.evidence_by_operation_.end() ? 0 : op_it->second.size();
    if (operation_size >= limits_.max_evidence_per_operation_) {
      return reject("evidence-set-full:operation");
    }
    if (impl.TotalObservations() >= limits_.max_observations_total_) {
      return reject("store-full");
    }
  }
  const MetaObservation* replaced = is_new ? nullptr : &existing->second;
  const absl::Status budget = check_budget(replaced);
  if (!budget.ok()) {
    return reject(std::string(budget.message()));
  }
  if (op_it == impl.evidence_by_operation_.end()) {
    op_it = impl.evidence_by_operation_
                .emplace(evidence.operation_id_,
                         std::map<Impl::EvidenceKey, MetaObservation>{})
                .first;
  }
  std::map<Impl::EvidenceKey, MetaObservation>& by_key = op_it->second;
  if (is_new) {
    const auto inserted = by_key.emplace(key, std::move(observation));
    assert(inserted.second);
    impl.AccountInsert(inserted.first->second, true);
  } else {
    const std::uint64_t old_bytes = ChargedBytes(existing->second);
    const std::uint64_t new_bytes = ChargedBytes(observation);
    existing->second = std::move(observation);
    impl.AccountReplace(existing->first.node_id_, old_bytes, new_bytes);
  }
  return absl::OkStatus();
}

void MetaObservationStore::ClearCandidatesForNodeLocked(
    std::string_view node_id, int64_t now_unix_ms, std::string_view detail) {
  Impl& impl = *impl_;
  for (auto group_it = impl.candidates_by_group_.begin();
       group_it != impl.candidates_by_group_.end();) {
    auto node_it = group_it->second.find(std::string(node_id));
    if (node_it != group_it->second.end()) {
      impl.AccountErase(node_it->second, false);
      group_it->second.erase(node_it);
      if (!detail.empty()) {
        impl.Audit(MetaObsAuditKind::kStalePurged, std::string(node_id),
                   std::string(detail), now_unix_ms,
                   limits_.audit_ring_capacity_);
      }
    }
    if (group_it->second.empty()) {
      group_it = impl.candidates_by_group_.erase(group_it);
    } else {
      ++group_it;
    }
  }
}

MetaObservationStore::HeartbeatReplaceResult
MetaObservationStore::ReplaceHeartbeat(
    const MetaObservationIdentity& identity, MetaNodeHealthObs health,
    std::optional<MetaCandidateProgressObs> candidate,
    const MetaCommittedFacts& facts, int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  const absl::Status identity_status = impl_->CheckIdentity(identity, facts);
  if (!identity_status.ok()) {
    const absl::Status rejected =
        MetaDomainRejectError(std::string(identity_status.message()));
    impl_->Audit(MetaObsAuditKind::kRejected, identity.node_id_,
                 std::string(identity_status.message()), now_unix_ms,
                 limits_.audit_ring_capacity_);
    return {.boot_status_ = rejected,
            .health_status_ = rejected,
            .candidate_status_ = rejected};
  }

  HeartbeatReplaceResult result;
  result.boot_status_ = IngestLocked(
      MetaObservation{.identity_ = identity, .payload_ = MetaNodeBootObs{}},
      facts, now_unix_ms);
  result.health_status_ = IngestLocked(
      MetaObservation{.identity_ = identity, .payload_ = std::move(health)},
      facts, now_unix_ms);

  // Candidate is replace-or-clear, never patch-in-place. This also removes a
  // candidate for another group left behind by an FDS role or membership
  // transition before attempting to admit the new report.
  ClearCandidatesForNodeLocked(identity.node_id_, now_unix_ms,
                               candidate.has_value()
                                   ? std::string_view{}
                                   : "heartbeat-role-has-no-candidate");
  if (candidate.has_value() && result.boot_status_.ok() &&
      result.health_status_.ok()) {
    result.candidate_status_ = IngestLocked(
        MetaObservation{.identity_ = identity,
                        .payload_ = std::move(*candidate)},
        facts, now_unix_ms);
  } else if (candidate.has_value()) {
    result.candidate_status_ = absl::FailedPreconditionError(
        "candidate requires accepted heartbeat boot and health");
  } else {
    result.candidate_status_ = absl::OkStatus();
  }
  return result;
}

void MetaObservationStore::RevalidateAll(const MetaCommittedFacts& facts,
                                         int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  Impl& impl = *impl_;
  // Committed state moved under stored observations (term promoted, manifest
  // or partition epoch changed, operation terminated, node retired): anything
  // that no longer
  // passes the full admission check is actively purged. Read paths re-filter
  // independently, so a purge miss here could
  // never leak stale data — this pass is what bounds memory instead.
  auto revalidate_map = [&](std::map<std::string, MetaObservation>& by_node) {
    for (auto it = by_node.begin(); it != by_node.end();) {
      const absl::Status valid = impl.Validate(it->second, facts);
      if (!valid.ok()) {
        const std::string detail =
            "commit-stale:" + std::string(valid.message());
        const std::string node_id = it->first;
        impl.AccountErase(it->second, false);
        it = by_node.erase(it);
        impl.Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
                   limits_.audit_ring_capacity_);
      } else {
        ++it;
      }
    }
  };
  revalidate_map(impl.boot_by_node_);
  revalidate_map(impl.health_by_node_);
  for (auto group_it = impl.candidates_by_group_.begin();
       group_it != impl.candidates_by_group_.end();) {
    revalidate_map(group_it->second);
    if (group_it->second.empty()) {
      group_it = impl.candidates_by_group_.erase(group_it);
    } else {
      ++group_it;
    }
  }
  for (auto op_it = impl.evidence_by_operation_.begin();
       op_it != impl.evidence_by_operation_.end();) {
    std::map<Impl::EvidenceKey, MetaObservation>& by_key = op_it->second;
    for (auto key_it = by_key.begin(); key_it != by_key.end();) {
      const absl::Status valid = impl.Validate(key_it->second, facts);
      if (!valid.ok()) {
        const std::string detail =
            "commit-stale:" + std::string(valid.message());
        const std::string node_id = key_it->first.node_id_;
        impl.AccountErase(key_it->second, true);
        key_it = by_key.erase(key_it);
        impl.Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
                   limits_.audit_ring_capacity_);
      } else {
        ++key_it;
      }
    }
    if (by_key.empty()) {
      op_it = impl.evidence_by_operation_.erase(op_it);
    } else {
      ++op_it;
    }
  }
}

void MetaObservationStore::SweepExpired(int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  impl_->last_periodic_sweep_unix_ms_ = now_unix_ms;
  SweepExpiredLocked(now_unix_ms);
}

bool MetaObservationStore::MaybeSweepExpired(int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  Impl& impl = *impl_;
  // Keep periodic cleanup well below the observation TTL without turning N
  // node heartbeats into N complete map scans. A backwards wall-clock step
  // starts a new cadence epoch; expiry itself remains conservative because
  // SweepExpiredLocked uses the caller's current wall time.
  const std::int64_t interval_ms =
      std::max<std::int64_t>(
          1, std::min<std::int64_t>(1000, std::max<std::int64_t>(
                                                1, limits_.ttl_ms_ / 4)));
  if (impl.last_periodic_sweep_unix_ms_.has_value() &&
      now_unix_ms >= *impl.last_periodic_sweep_unix_ms_ &&
      now_unix_ms - *impl.last_periodic_sweep_unix_ms_ < interval_ms) {
    return false;
  }
  impl.last_periodic_sweep_unix_ms_ = now_unix_ms;
  SweepExpiredLocked(now_unix_ms);
  return true;
}

void MetaObservationStore::SweepExpiredLocked(int64_t now_unix_ms) {
  Impl& impl = *impl_;
  // An entry exactly ttl_ms_ old still survives: expiry is strictly older
  // than the TTL so a sweep tick at the boundary never races a report.
  auto expired = [&](const MetaObservation& observation) {
    return ObservationExpired(observation, now_unix_ms, limits_.ttl_ms_);
  };
  auto sweep_map = [&](std::map<std::string, MetaObservation>& by_node) {
    for (auto it = by_node.begin(); it != by_node.end();) {
      if (expired(it->second)) {
        const std::string detail =
            "ttl-expired:age_ms=" +
            std::to_string(now_unix_ms - it->second.received_unix_ms_);
        const std::string node_id = it->first;
        impl.AccountErase(it->second, false);
        it = by_node.erase(it);
        impl.Audit(MetaObsAuditKind::kTtlExpired, node_id, detail, now_unix_ms,
                   limits_.audit_ring_capacity_);
      } else {
        ++it;
      }
    }
  };
  sweep_map(impl.boot_by_node_);
  sweep_map(impl.health_by_node_);
  for (auto group_it = impl.candidates_by_group_.begin();
       group_it != impl.candidates_by_group_.end();) {
    sweep_map(group_it->second);
    if (group_it->second.empty()) {
      group_it = impl.candidates_by_group_.erase(group_it);
    } else {
      ++group_it;
    }
  }
  for (auto op_it = impl.evidence_by_operation_.begin();
       op_it != impl.evidence_by_operation_.end();) {
    std::map<Impl::EvidenceKey, MetaObservation>& by_key = op_it->second;
    for (auto key_it = by_key.begin(); key_it != by_key.end();) {
      if (expired(key_it->second)) {
        const std::string detail =
            "ttl-expired:age_ms=" +
            std::to_string(now_unix_ms - key_it->second.received_unix_ms_);
        const std::string node_id = key_it->first.node_id_;
        impl.AccountErase(key_it->second, true);
        key_it = by_key.erase(key_it);
        impl.Audit(MetaObsAuditKind::kTtlExpired, node_id, detail, now_unix_ms,
                   limits_.audit_ring_capacity_);
      } else {
        ++key_it;
      }
    }
    if (by_key.empty()) {
      op_it = impl.evidence_by_operation_.erase(op_it);
    } else {
      ++op_it;
    }
  }
}

// Read paths re-filter through the same admission check as ingest (header:
// "never return stale data"), verdict only — nothing is admitted or evicted
// here, so the audit ring is not appended on reads.

std::optional<MetaCandidateProgressObs>
MetaObservationStore::LatestCandidateProgress(
    std::string_view group_id, const MetaCommittedFacts& facts) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  const auto group_it = impl.candidates_by_group_.find(std::string(group_id));
  if (group_it == impl.candidates_by_group_.end()) {
    return std::nullopt;
  }
  const MetaObservation* newest = nullptr;
  for (const auto& [node_id, observation] : group_it->second) {
    if (!impl.Validate(observation, facts).ok()) {
      continue;
    }
    if (newest == nullptr ||
        observation.received_unix_ms_ >= newest->received_unix_ms_) {
      newest = &observation;
    }
  }
  if (newest == nullptr) {
    return std::nullopt;
  }
  return std::get<MetaCandidateProgressObs>(newest->payload_);
}

std::vector<MetaCandidateProgressObs>
MetaObservationStore::CandidateProgressFor(
    std::string_view group_id, const MetaCommittedFacts& facts) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  std::vector<MetaCandidateProgressObs> out;
  const auto group_it = impl.candidates_by_group_.find(std::string(group_id));
  if (group_it == impl.candidates_by_group_.end()) {
    return out;
  }
  // Node-sorted (map order): a deterministic candidate set for the caller.
  for (const auto& [node_id, observation] : group_it->second) {
    if (impl.Validate(observation, facts).ok()) {
      out.push_back(std::get<MetaCandidateProgressObs>(observation.payload_));
    }
  }
  return out;
}

std::vector<MetaCandidateProgressObs>
MetaObservationStore::LiveCandidateProgressFor(
    std::string_view group_id, const MetaCommittedFacts& facts,
    int64_t now_unix_ms) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  std::vector<MetaCandidateProgressObs> out;
  const auto group_it = impl.candidates_by_group_.find(std::string(group_id));
  if (group_it == impl.candidates_by_group_.end()) return out;
  for (const auto& [node_id, observation] : group_it->second) {
    const std::int64_t age = now_unix_ms - observation.received_unix_ms_;
    const auto& stored =
        std::get<MetaCandidateProgressObs>(observation.payload_);
    if (age > limits_.ttl_ms_ || stored.applied_next_lsns_.empty() ||
        !stored.storage_ready_ || !stored.population_ready_ ||
        stored.draining_ || !impl.Validate(observation, facts).ok()) {
      continue;
    }
    MetaCandidateProgressObs candidate =
        std::get<MetaCandidateProgressObs>(observation.payload_);
    candidate.received_unix_ms_ = observation.received_unix_ms_;
    candidate.expires_unix_ms_ =
        observation.received_unix_ms_ >
                std::numeric_limits<std::int64_t>::max() - limits_.ttl_ms_
            ? std::numeric_limits<std::int64_t>::max()
            : observation.received_unix_ms_ + limits_.ttl_ms_;
    out.push_back(std::move(candidate));
  }
  return out;
}

std::optional<MetaObservation> MetaObservationStore::LatestForNode(
    std::string_view node_id, const MetaCommittedFacts& facts) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  const std::string node_key(node_id);
  const MetaObservation* newest = nullptr;
  auto consider = [&](const MetaObservation& observation) {
    if (!impl.Validate(observation, facts).ok()) {
      return;
    }
    if (newest == nullptr ||
        observation.received_unix_ms_ >= newest->received_unix_ms_) {
      newest = &observation;
    }
  };
  if (const auto it = impl.boot_by_node_.find(node_key);
      it != impl.boot_by_node_.end()) {
    consider(it->second);
  }
  if (const auto it = impl.health_by_node_.find(node_key);
      it != impl.health_by_node_.end()) {
    consider(it->second);
  }
  for (const auto& [group_id, by_node] : impl.candidates_by_group_) {
    if (const auto it = by_node.find(node_key); it != by_node.end()) {
      consider(it->second);
    }
  }
  for (const auto& [operation_id, by_key] : impl.evidence_by_operation_) {
    for (const auto& [key, observation] : by_key) {
      if (key.node_id_ == node_key) {
        consider(observation);
      }
    }
  }
  if (newest == nullptr) {
    return std::nullopt;
  }
  return *newest;
}

std::vector<MetaOperationEvidenceObs>
MetaObservationStore::EvidenceForOperation(
    const MetaOperationId& id, const MetaCommittedFacts& facts,
    int64_t now_unix_ms) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  std::vector<MetaOperationEvidenceObs> out;
  const auto op_it = impl.evidence_by_operation_.find(id);
  if (op_it == impl.evidence_by_operation_.end()) {
    return out;
  }
  // (node, phase)-sorted (map order): deterministic evidence sets.
  for (const auto& [key, observation] : op_it->second) {
    if (impl.Validate(observation, facts).ok() &&
        !ObservationExpired(observation, now_unix_ms, limits_.ttl_ms_)) {
      out.push_back(std::get<MetaOperationEvidenceObs>(observation.payload_));
    }
  }
  return out;
}

std::optional<uint64_t> MetaObservationStore::CurrentGeneration(
    std::string_view node_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = impl_->sessions_.find(std::string(node_id));
  if (it == impl_->sessions_.end()) {
    return std::nullopt;
  }
  return it->second.generation_;
}

std::vector<MetaObsAuditEvent> MetaObservationStore::AuditRing() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return std::vector<MetaObsAuditEvent>(impl_->audit_ring_.begin(),
                                        impl_->audit_ring_.end());
}

size_t MetaObservationStore::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return impl_->TotalObservations();
}

std::uint64_t MetaObservationStore::retained_bytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return impl_->retained_bytes_;
}

std::uint64_t MetaObservationStore::retained_bytes_for_node(
    std::string_view node_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return impl_->NodeRetainedBytes(node_id);
}

}  // namespace keylane::meta
