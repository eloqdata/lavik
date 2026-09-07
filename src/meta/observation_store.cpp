#include "keylane/meta/observation_store.h"

#include <deque>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "keylane/meta/encoding.h"

namespace keylane::meta {

namespace {

// Observation payload strings are soft state but remain bounded. The cap
// matches the committed-side payload cap; an
// over-cap field rejects the ingest rather than being truncated.
constexpr std::uint32_t kMaxObsFieldBytes = kMaxMetaPayloadBytes;
// Audit details are single-token (no whitespace) so the ctl `obsaudit` line
// protocol can dump them verbatim; the cap keeps the bounded ring also
// byte-bounded.
constexpr std::size_t kMaxAuditDetailBytes = 256;

std::string BoundedDetail(std::string detail) {
  if (detail.size() > kMaxAuditDetailBytes) {
    detail.resize(kMaxAuditDetailBytes);
  }
  return detail;
}

// The freshness verdict of the term/manifest anchor shared by candidate and
// evidence observations. group_term 0 can never anchor either: a group's term
// only begins via BeginGroupTerm(T >= 1), and MetaCommittedFacts reports 0
// for a group that does not exist at all — accepting term 0 would admit
// observations about nonexistent groups (the facts contract's "unknown
// answers reject" only works when term-bound observations carry T >= 1).
absl::Status CheckTermAnchor(std::string_view group_id, std::uint64_t term,
                             std::uint64_t manifest,
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
      facts.CurrentPopulationManifestId(group_id);
  if (manifest != committed_manifest) {
    return MetaDomainRejectError("manifest-mismatch:committed=" +
                                 std::to_string(committed_manifest));
  }
  return absl::OkStatus();
}

absl::Status CheckFieldSize(std::string_view field, std::string_view name) {
  if (field.size() > kMaxObsFieldBytes) {
    return MetaDomainRejectError("field-too-large:" + std::string(name));
  }
  return absl::OkStatus();
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

  std::size_t TotalObservations() const {
    std::size_t total = boot_by_node_.size() + health_by_node_.size();
    for (const auto& [group_id, by_node] : candidates_by_group_) {
      total += by_node.size();
    }
    for (const auto& [operation_id, by_key] : evidence_by_operation_) {
      total += by_key.size();
    }
    return total;
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
      return CheckFieldSize(health->health_, "health");
    }
    if (const auto* candidate =
            std::get_if<MetaCandidateProgressObs>(&payload)) {
      const absl::Status anchor =
          CheckTermAnchor(candidate->group_id_, candidate->group_term_,
                          candidate->population_manifest_id_, facts);
      if (!anchor.ok()) {
        return anchor;
      }
      // GroupRecord deliberately carries no history id because replication
      // history is scoped to a data-plane boot. A bare candidate therefore
      // has no committed history anchor; operation-specific comparison rules
      // consume the value as opaque payload.
      if (const absl::Status size =
              CheckFieldSize(candidate->applied_flow_vector_, "flow");
          !size.ok()) {
        return size;
      }
      if (const absl::Status size =
              CheckFieldSize(candidate->backlog_coverage_, "backlog");
          !size.ok()) {
        return size;
      }
      return CheckFieldSize(candidate->readiness_, "readiness");
    }
    const auto& evidence = std::get<MetaOperationEvidenceObs>(payload);
    if (!facts.OperationNonTerminal(evidence.operation_id_)) {
      return MetaDomainRejectError("operation-unknown-or-terminal");
    }
    const absl::Status anchor =
        CheckTermAnchor(evidence.group_id_, evidence.group_term_,
                        evidence.population_manifest_id_, facts);
    if (!anchor.ok()) {
      return anchor;
    }
    if (!facts.HistoryBoundToOperation(evidence.operation_id_,
                                       evidence.replication_history_id_)) {
      return MetaDomainRejectError("history-not-bound");
    }
    if (const absl::Status size =
            CheckFieldSize(evidence.kind_phase_, "kind_phase");
        !size.ok()) {
      return size;
    }
    return CheckFieldSize(evidence.evidence_, "evidence");
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
    audit_ring_.push_back(MetaObsAuditEvent{
        kind, node_id, BoundedDetail(std::move(detail)), now_unix_ms});
  }

  // Drops every observation of one node from all four buckets, auditing each
  // drop. Buckets left empty are erased so TotalObservations stays exact.
  void PurgeNode(const std::string& node_id, const std::string& detail,
                 std::int64_t now_unix_ms, std::size_t ring_capacity) {
    if (boot_by_node_.erase(node_id) != 0) {
      Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
            ring_capacity);
    }
    if (health_by_node_.erase(node_id) != 0) {
      Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
            ring_capacity);
    }
    for (auto it = candidates_by_group_.begin();
         it != candidates_by_group_.end();) {
      if (it->second.erase(node_id) != 0) {
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

absl::Status MetaObservationStore::Ingest(MetaObservation observation,
                                          const MetaCommittedFacts& facts,
                                          int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  Impl& impl = *impl_;
  const absl::Status valid = impl.Validate(observation, facts);
  if (!valid.ok()) {
    const std::string detail(valid.message());
    impl.Audit(MetaObsAuditKind::kRejected, observation.identity_.node_id_,
               detail, now_unix_ms, limits_.audit_ring_capacity_);
    return MetaDomainRejectError(detail);
  }
  observation.received_unix_ms_ = now_unix_ms;  // volatile-local TTL clock
  const std::string& node_id = observation.identity_.node_id_;

  if (std::holds_alternative<MetaNodeBootObs>(observation.payload_)) {
    if (!impl.boot_by_node_.contains(node_id) &&
        impl.TotalObservations() >= limits_.max_observations_total_) {
      const std::string detail = "store-full";
      impl.Audit(MetaObsAuditKind::kRejected, node_id, detail, now_unix_ms,
                 limits_.audit_ring_capacity_);
      return MetaDomainRejectError(detail);
    }
    impl.boot_by_node_[node_id] = std::move(observation);
    return absl::OkStatus();
  }
  if (std::holds_alternative<MetaNodeHealthObs>(observation.payload_)) {
    if (!impl.health_by_node_.contains(node_id) &&
        impl.TotalObservations() >= limits_.max_observations_total_) {
      const std::string detail = "store-full";
      impl.Audit(MetaObsAuditKind::kRejected, node_id, detail, now_unix_ms,
                 limits_.audit_ring_capacity_);
      return MetaDomainRejectError(detail);
    }
    impl.health_by_node_[node_id] = std::move(observation);
    return absl::OkStatus();
  }
  if (const auto* candidate =
          std::get_if<MetaCandidateProgressObs>(&observation.payload_)) {
    auto group_it = impl.candidates_by_group_.find(candidate->group_id_);
    if (group_it == impl.candidates_by_group_.end()) {
      group_it = impl.candidates_by_group_
                     .emplace(candidate->group_id_,
                              std::map<std::string, MetaObservation>{})
                     .first;
    }
    std::map<std::string, MetaObservation>& by_node = group_it->second;
    if (!by_node.contains(node_id)) {
      // Hard caps fail safe: a NEW node beyond the bounded
      // per-group candidate set or the total cap is rejected, never silently
      // squeezed in; refreshing an existing key never grows the state.
      if (by_node.size() >= limits_.max_candidates_per_group_) {
        const std::string detail =
            "candidate-set-full:group=" + candidate->group_id_;
        impl.Audit(MetaObsAuditKind::kRejected, node_id, detail, now_unix_ms,
                   limits_.audit_ring_capacity_);
        if (by_node.empty()) {
          impl.candidates_by_group_.erase(group_it);
        }
        return MetaDomainRejectError(detail);
      }
      if (impl.TotalObservations() >= limits_.max_observations_total_) {
        const std::string detail = "store-full";
        impl.Audit(MetaObsAuditKind::kRejected, node_id, detail, now_unix_ms,
                   limits_.audit_ring_capacity_);
        if (by_node.empty()) {
          impl.candidates_by_group_.erase(group_it);
        }
        return MetaDomainRejectError(detail);
      }
    }
    by_node[node_id] = std::move(observation);
    return absl::OkStatus();
  }

  const auto& evidence =
      std::get<MetaOperationEvidenceObs>(observation.payload_);
  auto op_it = impl.evidence_by_operation_.find(evidence.operation_id_);
  if (op_it == impl.evidence_by_operation_.end()) {
    op_it = impl.evidence_by_operation_
                .emplace(evidence.operation_id_,
                         std::map<Impl::EvidenceKey, MetaObservation>{})
                .first;
  }
  std::map<Impl::EvidenceKey, MetaObservation>& by_key = op_it->second;
  const Impl::EvidenceKey key{node_id, evidence.kind_phase_};
  if (!by_key.contains(key) &&
      impl.TotalObservations() >= limits_.max_observations_total_) {
    const std::string detail = "store-full";
    impl.Audit(MetaObsAuditKind::kRejected, node_id, detail, now_unix_ms,
               limits_.audit_ring_capacity_);
    if (by_key.empty()) {
      impl.evidence_by_operation_.erase(op_it);
    }
    return MetaDomainRejectError(detail);
  }
  by_key[key] = std::move(observation);
  return absl::OkStatus();
}

void MetaObservationStore::RevalidateAll(const MetaCommittedFacts& facts,
                                         int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  Impl& impl = *impl_;
  // Committed state moved under stored observations (term promoted, manifest
  // swapped, operation terminated, node retired): anything that no longer
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
  Impl& impl = *impl_;
  // An entry exactly ttl_ms_ old still survives: expiry is strictly older
  // than the TTL so a sweep tick at the boundary never races a report.
  auto expired = [&](const MetaObservation& observation) {
    return now_unix_ms - observation.received_unix_ms_ > limits_.ttl_ms_;
  };
  auto sweep_map = [&](std::map<std::string, MetaObservation>& by_node) {
    for (auto it = by_node.begin(); it != by_node.end();) {
      if (expired(it->second)) {
        const std::string detail =
            "ttl-expired:age_ms=" +
            std::to_string(now_unix_ms - it->second.received_unix_ms_);
        const std::string node_id = it->first;
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
    const MetaOperationId& id, const MetaCommittedFacts& facts) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  std::vector<MetaOperationEvidenceObs> out;
  const auto op_it = impl.evidence_by_operation_.find(id);
  if (op_it == impl.evidence_by_operation_.end()) {
    return out;
  }
  // (node, phase)-sorted (map order): deterministic evidence sets.
  for (const auto& [key, observation] : op_it->second) {
    if (impl.Validate(observation, facts).ok()) {
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

}  // namespace keylane::meta
