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

#include "lavik/meta/observation_store.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <deque>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "lavik/meta/encoding.h"

namespace lavik::meta {

namespace {

// Observation payload strings are soft state but remain bounded. The cap
// matches the committed-side payload cap; an
// over-cap field rejects the ingest rather than being truncated.
constexpr std::uint32_t kMaxObsFieldBytes = kMaxMetaPayloadBytes;
// Audit details are single-token (no whitespace) so the ctl `obsaudit` line
// protocol can dump them verbatim; the cap keeps the bounded ring also
// byte-bounded.
constexpr std::size_t kMaxAuditDetailBytes = 256;

template <std::size_t N>
bool IsZeroIdentity(const std::array<std::uint8_t, N>& value) {
  return std::ranges::all_of(value,
                             [](std::uint8_t byte) { return byte == 0; });
}

template <std::size_t N>
std::string HexIdentity(const std::array<std::uint8_t, N>& value) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string result;
  result.reserve(N * 2);
  for (const std::uint8_t byte : value) {
    result.push_back(kDigits[byte >> 4]);
    result.push_back(kDigits[byte & 0x0f]);
  }
  return result;
}

std::int64_t ObservationExpiry(std::int64_t received_unix_ms,
                               std::int64_t ttl_ms) {
  return received_unix_ms > std::numeric_limits<std::int64_t>::max() - ttl_ms
             ? std::numeric_limits<std::int64_t>::max()
             : received_unix_ms + ttl_ms;
}

bool ObservationExpired(const MetaObservation& observation, int64_t now_unix_ms,
                        int64_t ttl_ms) {
  // Backwards wall-clock movement is conservative: it cannot expire evidence.
  // Avoid subtracting until the ordering check has ruled out underflow.
  return now_unix_ms > observation.received_unix_ms_ &&
         now_unix_ms - observation.received_unix_ms_ > ttl_ms;
}

bool SameOwnerAuthority(const MetaObservedOwnerProjection& left,
                        const MetaObservedOwnerProjection& right) {
  return left.group_id_ == right.group_id_ &&
         left.owner_node_id_ == right.owner_node_id_ &&
         left.owner_assignment_id_ == right.owner_assignment_id_ &&
         left.group_term_ == right.group_term_;
}

std::uint64_t PossibleLeaseDeadline(
    const MetaObservedOwnerState::LeaseWindow& lease) {
  const std::uint64_t duration = lease.projection_.authority_lease_duration_ms_;
  return lease.heartbeat_received_steady_ms_ >
                 std::numeric_limits<std::uint64_t>::max() - duration
             ? std::numeric_limits<std::uint64_t>::max()
             : lease.heartbeat_received_steady_ms_ + duration;
}

bool GrantMatchesProjection(const cluster::control::LeaseGranted& grant,
                            const MetaObservedOwnerProjection& projection,
                            const MetaObservationIdentity& identity) {
  return grant.data_boot_id == HexIdentity(identity.boot_incarnation_) &&
         grant.control_revision == projection.control_revision_ &&
         grant.group_id == projection.group_id_ &&
         grant.assignment_id == projection.owner_assignment_id_ &&
         grant.group_term == projection.group_term_ &&
         grant.granted_duration_ms == projection.authority_lease_duration_ms_;
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
           2 * bytes(candidate->group_id_) + bytes(candidate->source_node_id_) +
           static_cast<std::uint64_t>(candidate->applied_next_lsns_.size()) *
               sizeof(std::uint64_t) +
           identity_node;
  }
  if (const auto* failover =
          std::get_if<MetaFailoverObservationObs>(&observation.payload_)) {
    return 2 * identity_node +
           std::visit(
               [&](const auto& payload) -> std::uint64_t {
                 using Payload = std::decay_t<decltype(payload)>;
                 if constexpr (std::is_same_v<Payload, MetaSourcePausedObs>) {
                   return bytes(payload.group_id_) +
                          bytes(payload.source_node_id_) +
                          payload.stable_next_lsns_.size() *
                              sizeof(std::uint64_t);
                 } else if constexpr (std::is_same_v<
                                          Payload, MetaCandidatePreparedObs>) {
                   return bytes(payload.group_id_) +
                          bytes(payload.candidate_node_id_);
                 } else {
                   return bytes(payload.group_id_) +
                          bytes(payload.candidate_node_id_) +
                          bytes(payload.failure_class_) +
                          bytes(payload.failure_detail_);
                 }
               },
               failover->payload_);
  }
  return 0;  // Every payload alternative is handled above.
}

bool ExceedsReplacementBudget(std::uint64_t current, std::uint64_t replaced,
                              std::uint64_t incoming, std::uint64_t limit) {
  assert(current >= replaced);
  // Written without addition so a caller-supplied UINT64_MAX limit cannot
  // turn an overflowing sum into an admission.
  return incoming > limit || current - replaced > limit - incoming;
}

}  // namespace

struct MetaObservationStore::Impl {
  // The trusted session identity of one node, fixed at AdoptSession time:
  // boot and ClientHello history may never change within a generation (a
  // reboot or history rotation reconnects with a new generation), so a
  // mismatch under the current generation is an identity violation, not an
  // ordering question.
  struct Session {
    MetaBootIncarnation boot_incarnation_{};
    std::optional<MetaReplicationHistoryId> replication_history_id_;
    std::uint64_t generation_ = 0;
    bool connected_ = true;
    std::optional<std::int64_t> disconnected_unix_ms_;
    std::optional<MetaBootIncarnation> disconnected_boot_id_;
    std::optional<std::uint64_t> disconnected_generation_;
    std::optional<MetaObservedFailoverProjection>
        heartbeat_failover_projection_;
    std::optional<MetaObservedOwnerProjection> heartbeat_owner_projection_;
    std::optional<std::uint64_t> confirmed_grant_sequence_;
    std::optional<MetaObservedOwnerState::LeaseWindow> possible_owner_lease_;
    std::optional<MetaObservedOwnerState::LeaseWindow>
        latest_owner_lease_attempt_;
    std::optional<MetaObservedOwnerState::LeaseWindow> installed_owner_lease_;
    std::optional<std::uint64_t> authority_handoff_pending_sequence_;
    std::optional<std::uint64_t> causal_progress_received_steady_ms_;
    // Owner serviceability depends only on fixed-size typed health. Keep it
    // in the same session cut as the projection and causal acknowledgement so
    // diagnostic-text admission cannot splice two heartbeat sequences.
    std::optional<MetaNodeHealthObs> heartbeat_health_;
    std::optional<std::uint64_t> heartbeat_received_steady_ms_;
    std::uint64_t heartbeat_sequence_ = 0;
  };

  struct NodeUsage {
    std::size_t observations_ = 0;
    std::uint64_t retained_bytes_ = 0;
  };

  std::size_t TotalObservations() const { return total_observations_; }

  std::uint64_t NodeRetainedBytes(std::string_view node_id) const {
    const auto it = usage_by_node_.find(std::string(node_id));
    return it == usage_by_node_.end() ? 0 : it->second.retained_bytes_;
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

  void AccountInsert(const MetaObservation& observation) {
    const std::string& node_id = observation.identity_.node_id_;
    const std::uint64_t charged = ChargedBytes(observation);
    NodeUsage& usage = usage_by_node_[node_id];
    ++usage.observations_;
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

  void AccountErase(const MetaObservation& observation) {
    const std::string& node_id = observation.identity_.node_id_;
    const std::uint64_t charged = ChargedBytes(observation);
    auto usage_it = usage_by_node_.find(node_id);
    assert(usage_it != usage_by_node_.end());
    NodeUsage& usage = usage_it->second;
    assert(usage.observations_ != 0 && total_observations_ != 0);
    assert(usage.retained_bytes_ >= charged && retained_bytes_ >= charged);
    --usage.observations_;
    --total_observations_;
    usage.retained_bytes_ -= charged;
    retained_bytes_ -= charged;
    if (usage.observations_ == 0) {
      assert(usage.retained_bytes_ == 0);
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
      if (candidate->operator_recovery_ &&
          (!candidate->storage_ready_ || candidate->draining_ ||
           candidate->recovered_ || candidate->source_group_term_ != 0 ||
           !candidate->source_node_id_.empty() ||
           candidate->source_assignment_id_ != MetaAssignmentId{} ||
           candidate->source_boot_incarnation_ != MetaBootIncarnation{} ||
           candidate->source_replication_history_id_ !=
               MetaReplicationHistoryId{} ||
           !candidate->applied_next_lsns_.empty() ||
           candidate->population_manifest_digest_ !=
               facts.CurrentPopulationManifestDigest(candidate->group_id_))) {
        return MetaDomainRejectError("invalid-operator-recovery-population");
      }
      // The legacy ctl observation surface may retain an opaque vector for
      // diagnostics. Only typed heartbeat candidates populate this vector and
      // source lineage, and only those enter LiveCandidateProgressFor.
      if (!candidate->applied_next_lsns_.empty()) {
        if (!candidate->storage_ready_ ||
            (!candidate->population_ready_ && !candidate->recovered_) ||
            candidate->draining_) {
          return MetaDomainRejectError("candidate-not-ready");
        }
        if (candidate->population_manifest_digest_ !=
            facts.CurrentPopulationManifestDigest(candidate->group_id_)) {
          return MetaDomainRejectError("manifest-digest-mismatch");
        }
        const bool candidate_is_owner =
            facts.IsOwnerAssignment(candidate->group_id_, candidate->node_id_,
                                    candidate->assignment_id_);
        if (candidate_is_owner &&
            !facts.MayReportFencedOwnerCandidate(*candidate)) {
          return MetaDomainRejectError("candidate-is-committed-owner");
        }
        if (!cluster::control::IsCanonicalIdentity160(
                candidate->source_node_id_)) {
          return MetaDomainRejectError("bad-candidate-source-node");
        }
        const auto is_zero = [](const auto& value) {
          return std::ranges::all_of(
              value, [](std::uint8_t byte) { return byte == 0; });
        };
        if (is_zero(candidate->source_assignment_id_) ||
            is_zero(candidate->source_boot_incarnation_) ||
            is_zero(candidate->source_replication_history_id_) ||
            candidate->source_group_term_ == 0 ||
            candidate->source_group_term_ > candidate->group_term_) {
          return MetaDomainRejectError("empty-candidate-source-lineage");
        }
        if (candidate_is_owner && !candidate->recovered_) {
          const auto session = sessions_.find(observation.identity_.node_id_);
          if (session == sessions_.end() ||
              !session->second.replication_history_id_.has_value() ||
              candidate->source_node_id_ != candidate->node_id_ ||
              candidate->source_assignment_id_ != candidate->assignment_id_ ||
              candidate->source_boot_incarnation_ !=
                  observation.identity_.boot_incarnation_ ||
              candidate->source_replication_history_id_ !=
                  candidate->replication_history_id_ ||
              candidate->replication_history_id_ !=
                  *session->second.replication_history_id_) {
            return MetaDomainRejectError(
                "fenced-owner-candidate-lineage-mismatch");
          }
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
      // Admin-injected history remains diagnostic-only without that lineage.
      return absl::OkStatus();
    }
    if (const auto* failover =
            std::get_if<MetaFailoverObservationObs>(&payload)) {
      return std::visit(
          [&](const auto& fact) -> absl::Status {
            using Fact = std::decay_t<decltype(fact)>;
            if (fact.group_id_.empty() ||
                fact.group_id_.size() > kMaxMetaGroupIdBytes ||
                IsZeroIdentity(fact.transition_id_)) {
              return MetaDomainRejectError("bad-failover-observation-id");
            }
            const auto committed =
                facts.FailoverTransitionById(fact.transition_id_);
            if (!committed.has_value() ||
                committed->group_id_ != fact.group_id_) {
              return MetaDomainRejectError("failover-transition-mismatch");
            }
            const MetaFailoverTransition& transition = committed->transition_;
            if constexpr (std::is_same_v<Fact, MetaSourcePausedObs>) {
              if (transition.mode_ != MetaFailoverMode::kControlled ||
                  !transition.candidate_action_.has_value()) {
                return MetaDomainRejectError(
                    "source-paused-requires-controlled-transition");
              }
              const MetaFailoverCompatibilityDomain& domain =
                  transition.candidate_action_->domain_;
              if (fact.source_node_id_ != observation.identity_.node_id_ ||
                  fact.source_boot_id_ !=
                      observation.identity_.boot_incarnation_ ||
                  fact.source_group_term_ != domain.source_group_term_ ||
                  fact.source_node_id_ != domain.source_node_id_ ||
                  fact.source_assignment_id_ != domain.source_assignment_id_ ||
                  fact.source_boot_id_ != domain.source_boot_id_ ||
                  fact.source_history_id_ != domain.source_history_id_ ||
                  !facts.IsOwnerAssignment(fact.group_id_, fact.source_node_id_,
                                           fact.source_assignment_id_)) {
                return MetaDomainRejectError("source-paused-anchor-mismatch");
              }
              if (fact.stable_next_lsns_.size() != domain.flow_count_ ||
                  std::ranges::any_of(
                      fact.stable_next_lsns_,
                      [](std::uint64_t lsn) { return lsn == 0; })) {
                return MetaDomainRejectError("source-paused-frontier-invalid");
              }
              return absl::OkStatus();
            } else {
              if (IsZeroIdentity(fact.action_id_) ||
                  !transition.candidate_action_.has_value() ||
                  transition.candidate_action_->action_id_ != fact.action_id_) {
                return MetaDomainRejectError("failover-action-mismatch");
              }
              const MetaFailoverCandidate& candidate =
                  transition.candidate_action_->candidate_;
              if (fact.candidate_node_id_ != observation.identity_.node_id_ ||
                  fact.candidate_boot_id_ !=
                      observation.identity_.boot_incarnation_ ||
                  fact.candidate_node_id_ != candidate.node_id_ ||
                  fact.candidate_assignment_id_ != candidate.assignment_id_ ||
                  fact.candidate_boot_id_ != candidate.boot_id_ ||
                  !facts.AssignmentMatches(fact.group_id_,
                                           fact.candidate_node_id_,
                                           fact.candidate_assignment_id_)) {
                return MetaDomainRejectError(
                    "failover-candidate-anchor-mismatch");
              }
              if constexpr (std::is_same_v<Fact, MetaCandidatePreparedObs>) {
                if (IsZeroIdentity(fact.prepared_context_id_)) {
                  return MetaDomainRejectError(
                      "prepared-context-identity-missing");
                }
                return absl::OkStatus();
              } else {
                if (fact.population_manifest_revision_ !=
                        facts.CurrentPopulationManifestRevision(
                            fact.group_id_) ||
                    fact.population_manifest_digest_ !=
                        facts.CurrentPopulationManifestDigest(fact.group_id_) ||
                    fact.partition_replication_epoch_ !=
                        facts.CurrentPartitionReplicationEpoch(
                            fact.group_id_)) {
                  return MetaDomainRejectError(
                      "action-failed-population-mismatch");
                }
                if (fact.failure_class_.empty() ||
                    CheckFieldSize(fact.failure_class_,
                                   cluster::control::kMaxIdentifierBytes,
                                   "failure_class")
                            .ok() == false ||
                    CheckFieldSize(fact.failure_detail_, kMaxObsFieldBytes,
                                   "failure_detail")
                            .ok() == false) {
                  return MetaDomainRejectError("action-failed-detail-invalid");
                }
                return absl::OkStatus();
              }
            }
          },
          failover->payload_);
    }
    return MetaDomainRejectError("unknown-observation-payload");
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
    audit_ring_.push_back(MetaObsAuditEvent{kind, BoundedDetail(node_id),
                                            BoundedDetail(std::move(detail)),
                                            now_unix_ms});
  }

  // Drops every observation of one node from all four buckets, auditing each
  // drop. Buckets left empty are erased so TotalObservations stays exact.
  void PurgeNode(const std::string& node_id, const std::string& detail,
                 std::int64_t now_unix_ms, std::size_t ring_capacity) {
    if (const auto it = boot_by_node_.find(node_id);
        it != boot_by_node_.end()) {
      AccountErase(it->second);
      boot_by_node_.erase(it);
      Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
            ring_capacity);
    }
    if (const auto it = health_by_node_.find(node_id);
        it != health_by_node_.end()) {
      AccountErase(it->second);
      health_by_node_.erase(it);
      Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
            ring_capacity);
    }
    if (const auto it = failover_by_node_.find(node_id);
        it != failover_by_node_.end()) {
      AccountErase(it->second);
      failover_by_node_.erase(it);
      Audit(MetaObsAuditKind::kStalePurged, node_id, detail, now_unix_ms,
            ring_capacity);
    }
    for (auto it = candidates_by_group_.begin();
         it != candidates_by_group_.end();) {
      if (const auto node_it = it->second.find(node_id);
          node_it != it->second.end()) {
        AccountErase(node_it->second);
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
    assert(!usage_by_node_.contains(node_id));
  }

  std::map<std::string, Session> sessions_;
  std::map<std::string, MetaObservation> boot_by_node_;
  std::map<std::string, MetaObservation> health_by_node_;
  // One replace-or-clear transition fact per authenticated heartbeat source.
  // Query paths join it back to the active committed transition/action.
  std::map<std::string, MetaObservation> failover_by_node_;
  // group_id -> node_id -> that node's latest candidate for the group;
  // per-group node count is the bounded candidate set (Limits).
  std::map<std::string, std::map<std::string, MetaObservation>>
      candidates_by_group_;
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
    const MetaObservationIdentity& identity, int64_t now_unix_ms,
    std::optional<MetaReplicationHistoryId> replication_history_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  Impl& impl = *impl_;
  if (identity.node_id_.empty() ||
      identity.node_id_.size() > kMetaNodeIdBytes) {
    const std::string detail = "bad-node-id";
    impl.Audit(MetaObsAuditKind::kRejected, identity.node_id_, detail,
               now_unix_ms, limits_.audit_ring_capacity_);
    return MetaDomainRejectError(detail);
  }
  if (replication_history_id.has_value() &&
      IsZeroIdentity(*replication_history_id)) {
    const std::string detail = "empty-replication-history-id";
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
  Impl::Session replacement{.boot_incarnation_ = identity.boot_incarnation_,
                            .replication_history_id_ = replication_history_id,
                            .generation_ = identity.session_generation_,
                            .connected_ = true,
                            .disconnected_unix_ms_ = std::nullopt,
                            .disconnected_boot_id_ = std::nullopt,
                            .disconnected_generation_ = std::nullopt,
                            .heartbeat_failover_projection_ = std::nullopt,
                            .heartbeat_owner_projection_ = std::nullopt,
                            .confirmed_grant_sequence_ = std::nullopt,
                            .possible_owner_lease_ = std::nullopt,
                            .latest_owner_lease_attempt_ = std::nullopt,
                            .installed_owner_lease_ = std::nullopt,
                            .authority_handoff_pending_sequence_ = std::nullopt,
                            .causal_progress_received_steady_ms_ = std::nullopt,
                            .heartbeat_health_ = std::nullopt,
                            .heartbeat_received_steady_ms_ = std::nullopt,
                            .heartbeat_sequence_ = 0};
  if (it != impl.sessions_.end()) {
    replacement.disconnected_unix_ms_ = it->second.disconnected_unix_ms_;
    replacement.disconnected_boot_id_ = it->second.disconnected_boot_id_;
    replacement.disconnected_generation_ = it->second.disconnected_generation_;
  }
  impl.sessions_[identity.node_id_] = std::move(replacement);
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
  session->second.connected_ = false;
  session->second.disconnected_unix_ms_ = now_unix_ms;
  session->second.disconnected_boot_id_ = identity.boot_incarnation_;
  session->second.disconnected_generation_ = identity.session_generation_;
  ClearCandidatesForNodeLocked(identity.node_id_, now_unix_ms,
                               "session-disconnected");
  ClearCandidateFailoverForNodeLocked(identity.node_id_, now_unix_ms,
                                      "session-disconnected");
}

absl::Status MetaObservationStore::Ingest(MetaObservation observation,
                                          const MetaCommittedFacts& facts,
                                          int64_t now_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  return IngestLocked(std::move(observation), facts, now_unix_ms);
}

absl::Status MetaObservationStore::IngestLocked(MetaObservation observation,
                                                const MetaCommittedFacts& facts,
                                                int64_t now_unix_ms) {
  Impl& impl = *impl_;
  if (auto* candidate =
          std::get_if<MetaCandidateProgressObs>(&observation.payload_);
      candidate != nullptr && candidate->session_generation_ == 0) {
    // Preserve the administrative/test observation adapter: it predates the
    // explicit payload copy but still arrives through a trusted identity.
    candidate->session_generation_ = observation.identity_.session_generation_;
  }
  if (auto* failover =
          std::get_if<MetaFailoverObservationObs>(&observation.payload_)) {
    if (auto* prepared =
            std::get_if<MetaCandidatePreparedObs>(&failover->payload_)) {
      // This value is session-layer evidence, not a Data-supplied claim.
      prepared->session_generation_ = observation.identity_.session_generation_;
    }
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
      impl.AccountInsert(inserted.first->second);
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
  if (std::holds_alternative<MetaFailoverObservationObs>(
          observation.payload_)) {
    return store_latest(impl.failover_by_node_);
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
      const std::size_t group_size = group_it == impl.candidates_by_group_.end()
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
      impl.AccountInsert(inserted.first->second);
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

  return reject("unknown-observation-payload");
}

void MetaObservationStore::ClearCandidatesForNodeLocked(
    std::string_view node_id, int64_t now_unix_ms, std::string_view detail,
    bool retain_operator_recovery) {
  Impl& impl = *impl_;
  for (auto group_it = impl.candidates_by_group_.begin();
       group_it != impl.candidates_by_group_.end();) {
    auto node_it = group_it->second.find(std::string(node_id));
    if (node_it != group_it->second.end()) {
      if (retain_operator_recovery &&
          std::get<MetaCandidateProgressObs>(node_it->second.payload_)
              .operator_recovery_) {
        // Keep the original observation identity and receive time. Selection
        // still revalidates boot/session, committed population scope and TTL.
        ++group_it;
        continue;
      }
      impl.AccountErase(node_it->second);
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

void MetaObservationStore::ClearCandidateFailoverForNodeLocked(
    std::string_view node_id, int64_t now_unix_ms, std::string_view detail) {
  const auto it = impl_->failover_by_node_.find(std::string(node_id));
  if (it == impl_->failover_by_node_.end()) return;
  const auto& failover =
      std::get<MetaFailoverObservationObs>(it->second.payload_);
  if (std::holds_alternative<MetaSourcePausedObs>(failover.payload_)) {
    // A disconnected old source observation remains usable on a best-effort
    // basis until its independent grace/TTL expires. Candidate action
    // observations have no such grace.
    return;
  }
  impl_->AccountErase(it->second);
  impl_->failover_by_node_.erase(it);
  if (!detail.empty()) {
    impl_->Audit(MetaObsAuditKind::kStalePurged, std::string(node_id),
                 std::string(detail), now_unix_ms,
                 limits_.audit_ring_capacity_);
  }
}

MetaObservationStore::HeartbeatReplaceResult
MetaObservationStore::ReplaceHeartbeat(
    const MetaObservationIdentity& identity, MetaNodeHealthObs health,
    std::optional<MetaCandidateProgressObs> candidate,
    const MetaCommittedFacts& facts, int64_t now_unix_ms) {
  return ReplaceHeartbeat(identity, std::move(health), std::move(candidate),
                          std::nullopt, std::nullopt, facts, now_unix_ms);
}

MetaObservationStore::HeartbeatReplaceResult
MetaObservationStore::ReplaceHeartbeat(
    const MetaObservationIdentity& identity, MetaNodeHealthObs health,
    std::optional<MetaCandidateProgressObs> candidate,
    std::optional<MetaFailoverObservationObs> failover,
    const MetaCommittedFacts& facts, int64_t now_unix_ms) {
  return ReplaceHeartbeat(identity, std::move(health), std::move(candidate),
                          std::move(failover), std::nullopt, facts,
                          now_unix_ms);
}

MetaObservationStore::HeartbeatReplaceResult
MetaObservationStore::ReplaceHeartbeat(
    const MetaObservationIdentity& identity, MetaNodeHealthObs health,
    std::optional<MetaCandidateProgressObs> candidate,
    std::optional<MetaFailoverObservationObs> failover,
    std::optional<MetaObservedFailoverProjection> failover_projection,
    const MetaCommittedFacts& facts, int64_t now_unix_ms) {
  return ReplaceHeartbeat(identity, std::move(health), std::move(candidate),
                          std::move(failover), std::move(failover_projection),
                          std::nullopt, 0, std::nullopt, facts, now_unix_ms,
                          /*now_steady_ms=*/0);
}

MetaObservationStore::HeartbeatReplaceResult
MetaObservationStore::ReplaceHeartbeat(
    const MetaObservationIdentity& identity, MetaNodeHealthObs health,
    std::optional<MetaCandidateProgressObs> candidate,
    std::optional<MetaFailoverObservationObs> failover,
    std::optional<MetaObservedFailoverProjection> failover_projection,
    std::optional<MetaObservedOwnerProjection> owner_projection,
    std::uint64_t heartbeat_sequence,
    std::optional<std::uint64_t> confirmed_grant_sequence,
    const MetaCommittedFacts& facts, int64_t now_unix_ms,
    std::uint64_t now_steady_ms, bool lease_only) {
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
            .candidate_status_ = rejected,
            .failover_status_ = rejected};
  }

  if ((owner_projection.has_value() || confirmed_grant_sequence.has_value()) &&
      heartbeat_sequence == 0) {
    const absl::Status rejected =
        MetaDomainRejectError("owner heartbeat sequence is zero");
    return {.boot_status_ = rejected,
            .health_status_ = rejected,
            .candidate_status_ = rejected,
            .failover_status_ = rejected};
  }
  if (owner_projection.has_value() &&
      owner_projection->authority_lease_duration_ms_ == 0) {
    const absl::Status rejected =
        MetaDomainRejectError("owner projection lease duration is zero");
    return {.boot_status_ = rejected,
            .health_status_ = rejected,
            .candidate_status_ = rejected,
            .failover_status_ = rejected};
  }
  if (confirmed_grant_sequence.has_value() &&
      (!owner_projection.has_value() || *confirmed_grant_sequence == 0 ||
       *confirmed_grant_sequence >= heartbeat_sequence)) {
    const absl::Status rejected =
        MetaDomainRejectError("causal lease does not match owner heartbeat");
    return {.boot_status_ = rejected,
            .health_status_ = rejected,
            .candidate_status_ = rejected,
            .failover_status_ = rejected};
  }

  // The projection marker and role replacement describe the same heartbeat.
  // Publishing them under this lock prevents a planner from observing an
  // exact-action omission paired with candidate state from another frame.
  Impl::Session& session = impl_->sessions_.at(identity.node_id_);
  MetaNodeHealthObs owner_health = health;
  owner_health.health_.clear();
  session.heartbeat_failover_projection_ = std::move(failover_projection);
  if (!owner_projection.has_value() ||
      (session.possible_owner_lease_.has_value() &&
       !SameOwnerAuthority(session.possible_owner_lease_->projection_,
                           *owner_projection))) {
    // Installing an FDS without this Owner authority revokes the old local
    // anchor. Projection-only changes keep an already installed finite lease
    // alive, so their possible window must survive until confirmation or its
    // own deadline.
    session.possible_owner_lease_.reset();
  }
  if (!owner_projection.has_value() ||
      (session.latest_owner_lease_attempt_.has_value() &&
       !SameOwnerAuthority(session.latest_owner_lease_attempt_->projection_,
                           *owner_projection))) {
    session.latest_owner_lease_attempt_.reset();
  }
  if (!owner_projection.has_value() ||
      (session.installed_owner_lease_.has_value() &&
       !SameOwnerAuthority(session.installed_owner_lease_->projection_,
                           *owner_projection))) {
    session.installed_owner_lease_.reset();
  }
  if (!owner_projection.has_value() ||
      (session.authority_handoff_pending_sequence_.has_value() &&
       (!session.heartbeat_owner_projection_.has_value() ||
        !SameOwnerAuthority(*session.heartbeat_owner_projection_,
                            *owner_projection)))) {
    session.authority_handoff_pending_sequence_.reset();
  }
  if (confirmed_grant_sequence.has_value() &&
      session.latest_owner_lease_attempt_.has_value() &&
      *owner_projection == session.latest_owner_lease_attempt_->projection_ &&
      *confirmed_grant_sequence >=
          session.latest_owner_lease_attempt_->granted_heartbeat_sequence_) {
    // Stop-and-wait proves Data processed every preceding Ack and installed
    // this latest Grant. It supersedes all older same-authority possibilities,
    // including a longer lease from the projection replaced just before this
    // confirmation.
    session.installed_owner_lease_ = session.latest_owner_lease_attempt_;
    session.latest_owner_lease_attempt_.reset();
    session.possible_owner_lease_.reset();
  }
  if (!owner_projection.has_value()) {
    session.confirmed_grant_sequence_.reset();
    session.causal_progress_received_steady_ms_.reset();
  } else if (session.heartbeat_owner_projection_ != owner_projection) {
    // Every field of the trusted projection participates in this equality.
    // A new Owner/assignment/term/projection begins a fresh, finite pending
    // interval rather than inheriting causal progress.
    session.confirmed_grant_sequence_ = confirmed_grant_sequence;
    session.causal_progress_received_steady_ms_ = now_steady_ms;
  } else if (confirmed_grant_sequence.has_value() &&
             (!session.confirmed_grant_sequence_.has_value() ||
              *confirmed_grant_sequence > *session.confirmed_grant_sequence_)) {
    // Heartbeat sequence alone is liveness, not proof that a newer lease Ack
    // reached Data. Only a strictly advancing acknowledged sequence extends
    // the causal-progress deadline.
    session.confirmed_grant_sequence_ = confirmed_grant_sequence;
    session.causal_progress_received_steady_ms_ = now_steady_ms;
  }
  session.heartbeat_owner_projection_ = std::move(owner_projection);
  session.heartbeat_health_ = std::move(owner_health);
  session.heartbeat_received_steady_ms_ = now_steady_ms;
  session.heartbeat_sequence_ = heartbeat_sequence;

  HeartbeatReplaceResult result;
  const bool retain_operator_recovery =
      lease_only && !candidate.has_value() && health.storage_ready_ &&
      !health.population_ready_ && !health.draining_;
  result.boot_status_ = IngestLocked(
      MetaObservation{.identity_ = identity, .payload_ = MetaNodeBootObs{}},
      facts, now_unix_ms);
  result.health_status_ = IngestLocked(
      MetaObservation{.identity_ = identity, .payload_ = std::move(health)},
      facts, now_unix_ms);

  // Ordinary candidates are replace-or-clear. A restarted former Owner
  // alternates availability with lease requests: the intervening request
  // must not withdraw its operator-only population. An explicit role omission,
  // unhealthy heartbeat or new candidate report still clears the old fact.
  ClearCandidatesForNodeLocked(
      identity.node_id_, now_unix_ms,
      candidate.has_value() ? std::string_view{}
                            : "heartbeat-role-has-no-candidate",
      retain_operator_recovery && result.boot_status_.ok() &&
          result.health_status_.ok());
  if (candidate.has_value() && result.boot_status_.ok() &&
      result.health_status_.ok()) {
    result.candidate_status_ =
        IngestLocked(MetaObservation{.identity_ = identity,
                                     .payload_ = std::move(*candidate)},
                     facts, now_unix_ms);
    if (result.candidate_status_.ok() &&
        !facts.IsCurrentFailoverCandidate(identity.node_id_,
                                          identity.boot_incarnation_)) {
      auto session = impl_->sessions_.find(identity.node_id_);
      if (session != impl_->sessions_.end() &&
          session->second.boot_incarnation_ == identity.boot_incarnation_ &&
          session->second.generation_ == identity.session_generation_ &&
          session->second.disconnected_boot_id_ ==
              std::optional(identity.boot_incarnation_)) {
        // Fresh generic progress before an action is selected is a new
        // eligibility observation, not a revival of an older attempt. Once
        // committed state binds this boot to an action, its disconnect latch
        // is never cleared and that action cannot be resurrected by reconnect.
        session->second.disconnected_unix_ms_.reset();
        session->second.disconnected_boot_id_.reset();
        session->second.disconnected_generation_.reset();
      }
    }
  } else if (candidate.has_value()) {
    result.candidate_status_ = absl::FailedPreconditionError(
        "candidate requires accepted heartbeat boot and health");
  } else {
    result.candidate_status_ = absl::OkStatus();
  }

  // Transition evidence is also replace-or-clear. Clearing first makes a
  // malformed new report fail closed instead of retaining a stale fact.
  if (const auto existing = impl_->failover_by_node_.find(identity.node_id_);
      existing != impl_->failover_by_node_.end()) {
    impl_->AccountErase(existing->second);
    impl_->failover_by_node_.erase(existing);
  }
  if (failover.has_value() && result.boot_status_.ok() &&
      result.health_status_.ok()) {
    result.failover_status_ =
        IngestLocked(MetaObservation{.identity_ = identity,
                                     .payload_ = std::move(*failover)},
                     facts, now_unix_ms);
  } else if (failover.has_value()) {
    result.failover_status_ = absl::FailedPreconditionError(
        "failover observation requires accepted heartbeat boot and health");
  } else {
    result.failover_status_ = absl::OkStatus();
  }
  return result;
}

absl::Status MetaObservationStore::RecordOwnerLeaseDecisionAttempt(
    const MetaObservationIdentity& identity, std::uint64_t heartbeat_sequence,
    const cluster::control::LeaseDecision& decision) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = impl_->sessions_.find(identity.node_id_);
  if (found == impl_->sessions_.end() || !found->second.connected_ ||
      found->second.boot_incarnation_ != identity.boot_incarnation_ ||
      found->second.generation_ != identity.session_generation_) {
    return absl::FailedPreconditionError(
        "possible Owner lease does not match the current session");
  }
  Impl::Session& session = found->second;
  if (heartbeat_sequence == 0 ||
      session.heartbeat_sequence_ != heartbeat_sequence ||
      !session.heartbeat_received_steady_ms_.has_value()) {
    return absl::FailedPreconditionError(
        "Owner lease decision does not match the current heartbeat");
  }

  const auto* grant = std::get_if<cluster::control::LeaseGranted>(&decision);
  const auto* denied = std::get_if<cluster::control::LeaseDenied>(&decision);
  const bool handoff_pending =
      denied != nullptr &&
      denied->reason ==
          cluster::control::LeaseDenialReason::kAuthorityHandoffPending;
  if (grant == nullptr && !handoff_pending) return absl::OkStatus();
  if (!session.heartbeat_owner_projection_.has_value()) {
    return absl::FailedPreconditionError(
        "Owner lease decision has no installed Owner projection");
  }
  const MetaObservedOwnerProjection& projection =
      *session.heartbeat_owner_projection_;
  if (handoff_pending) {
    session.authority_handoff_pending_sequence_ = heartbeat_sequence;
    return absl::OkStatus();
  }
  if (projection.authority_lease_duration_ms_ == 0 ||
      !GrantMatchesProjection(*grant, projection, identity)) {
    return absl::FailedPreconditionError(
        "Owner lease Grant does not match the current projection");
  }

  session.authority_handoff_pending_sequence_.reset();
  MetaObservedOwnerState::LeaseWindow possible{
      .projection_ = projection,
      .granted_heartbeat_sequence_ = heartbeat_sequence,
      .heartbeat_received_steady_ms_ = *session.heartbeat_received_steady_ms_,
  };
  session.latest_owner_lease_attempt_ = possible;
  if (!session.possible_owner_lease_.has_value() ||
      !SameOwnerAuthority(session.possible_owner_lease_->projection_,
                          projection) ||
      PossibleLeaseDeadline(possible) >
          PossibleLeaseDeadline(*session.possible_owner_lease_) ||
      (PossibleLeaseDeadline(possible) ==
           PossibleLeaseDeadline(*session.possible_owner_lease_) &&
       heartbeat_sequence >
           session.possible_owner_lease_->granted_heartbeat_sequence_)) {
    session.possible_owner_lease_ = std::move(possible);
  }
  return absl::OkStatus();
}

absl::Status MetaObservationStore::RecordOwnerLeaseDecisionWritten(
    const MetaObservationIdentity& identity, std::uint64_t heartbeat_sequence,
    const cluster::control::LeaseDecision& decision) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = impl_->sessions_.find(identity.node_id_);
  if (found == impl_->sessions_.end() || !found->second.connected_ ||
      found->second.boot_incarnation_ != identity.boot_incarnation_ ||
      found->second.generation_ != identity.session_generation_) {
    return absl::FailedPreconditionError(
        "written Owner lease decision does not match the current session");
  }
  Impl::Session& session = found->second;
  if (heartbeat_sequence == 0 ||
      session.heartbeat_sequence_ != heartbeat_sequence) {
    return absl::FailedPreconditionError(
        "written Owner lease decision does not match the current heartbeat");
  }
  if (!session.authority_handoff_pending_sequence_.has_value() ||
      heartbeat_sequence < *session.authority_handoff_pending_sequence_) {
    return absl::OkStatus();
  }
  const auto* denied = std::get_if<cluster::control::LeaseDenied>(&decision);
  if (denied == nullptr ||
      denied->reason != cluster::control::LeaseDenialReason::kNodeNotReady) {
    return absl::OkStatus();
  }
  session.authority_handoff_pending_sequence_.reset();
  return absl::OkStatus();
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
        impl.AccountErase(it->second);
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
  revalidate_map(impl.failover_by_node_);
  for (auto group_it = impl.candidates_by_group_.begin();
       group_it != impl.candidates_by_group_.end();) {
    revalidate_map(group_it->second);
    if (group_it->second.empty()) {
      group_it = impl.candidates_by_group_.erase(group_it);
    } else {
      ++group_it;
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
  const std::int64_t interval_ms = std::max<std::int64_t>(
      1, std::min<std::int64_t>(
             1000, std::max<std::int64_t>(1, limits_.ttl_ms_ / 4)));
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
        impl.AccountErase(it->second);
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
  sweep_map(impl.failover_by_node_);
  for (auto group_it = impl.candidates_by_group_.begin();
       group_it != impl.candidates_by_group_.end();) {
    sweep_map(group_it->second);
    if (group_it->second.empty()) {
      group_it = impl.candidates_by_group_.erase(group_it);
    } else {
      ++group_it;
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
    int64_t now_unix_ms, bool include_operator_recovery) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  std::vector<MetaCandidateProgressObs> out;
  const auto group_it = impl.candidates_by_group_.find(std::string(group_id));
  if (group_it == impl.candidates_by_group_.end()) return out;
  for (const auto& [node_id, observation] : group_it->second) {
    const std::int64_t age = now_unix_ms - observation.received_unix_ms_;
    const auto& stored =
        std::get<MetaCandidateProgressObs>(observation.payload_);
    if (age > limits_.ttl_ms_ ||
        (stored.operator_recovery_ ? !include_operator_recovery
                                   : stored.applied_next_lsns_.empty()) ||
        !stored.storage_ready_ ||
        (!stored.population_ready_ && !stored.recovered_ &&
         !stored.operator_recovery_) ||
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
    std::string_view node_id, const MetaCommittedFacts& facts,
    std::optional<int64_t> now_unix_ms) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  const std::string node_key(node_id);
  const MetaObservation* newest = nullptr;
  auto consider = [&](const MetaObservation& observation) {
    if (!impl.Validate(observation, facts).ok() ||
        (now_unix_ms.has_value() &&
         ObservationExpired(observation, *now_unix_ms, limits_.ttl_ms_))) {
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
  if (const auto it = impl.failover_by_node_.find(node_key);
      it != impl.failover_by_node_.end()) {
    consider(it->second);
  }
  for (const auto& [group_id, by_node] : impl.candidates_by_group_) {
    if (const auto it = by_node.find(node_key); it != by_node.end()) {
      consider(it->second);
    }
  }
  if (newest == nullptr) {
    return std::nullopt;
  }
  return *newest;
}

std::optional<MetaSourcePausedObs> MetaObservationStore::SourcePausedFor(
    const MetaFailoverTransitionId& transition_id,
    const MetaCommittedFacts& facts, int64_t now_unix_ms) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  for (const auto& [node_id, observation] : impl.failover_by_node_) {
    (void)node_id;
    if (ObservationExpired(observation, now_unix_ms, limits_.ttl_ms_) ||
        !impl.Validate(observation, facts).ok()) {
      continue;
    }
    const auto& failover =
        std::get<MetaFailoverObservationObs>(observation.payload_);
    const auto* source = std::get_if<MetaSourcePausedObs>(&failover.payload_);
    if (source == nullptr || source->transition_id_ != transition_id) continue;
    MetaSourcePausedObs result = *source;
    result.received_unix_ms_ = observation.received_unix_ms_;
    result.expires_unix_ms_ =
        ObservationExpiry(observation.received_unix_ms_, limits_.ttl_ms_);
    return result;
  }
  return std::nullopt;
}

std::optional<MetaCandidatePreparedObs>
MetaObservationStore::CandidatePreparedFor(
    const MetaFailoverTransitionId& transition_id,
    const MetaFailoverActionId& action_id, const MetaCommittedFacts& facts,
    int64_t now_unix_ms) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  for (const auto& [node_id, observation] : impl.failover_by_node_) {
    (void)node_id;
    if (ObservationExpired(observation, now_unix_ms, limits_.ttl_ms_) ||
        !impl.Validate(observation, facts).ok()) {
      continue;
    }
    const auto& failover =
        std::get<MetaFailoverObservationObs>(observation.payload_);
    const auto* prepared =
        std::get_if<MetaCandidatePreparedObs>(&failover.payload_);
    if (prepared == nullptr || prepared->transition_id_ != transition_id ||
        prepared->action_id_ != action_id) {
      continue;
    }
    MetaCandidatePreparedObs result = *prepared;
    result.received_unix_ms_ = observation.received_unix_ms_;
    result.expires_unix_ms_ =
        ObservationExpiry(observation.received_unix_ms_, limits_.ttl_ms_);
    return result;
  }
  return std::nullopt;
}

std::optional<MetaActionFailedObs> MetaObservationStore::ActionFailedFor(
    const MetaFailoverTransitionId& transition_id,
    const MetaFailoverActionId& action_id, const MetaCommittedFacts& facts,
    int64_t now_unix_ms) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const Impl& impl = *impl_;
  for (const auto& [node_id, observation] : impl.failover_by_node_) {
    (void)node_id;
    if (ObservationExpired(observation, now_unix_ms, limits_.ttl_ms_) ||
        !impl.Validate(observation, facts).ok()) {
      continue;
    }
    const auto& failover =
        std::get<MetaFailoverObservationObs>(observation.payload_);
    const auto* failed = std::get_if<MetaActionFailedObs>(&failover.payload_);
    if (failed == nullptr || failed->transition_id_ != transition_id ||
        failed->action_id_ != action_id) {
      continue;
    }
    MetaActionFailedObs result = *failed;
    result.received_unix_ms_ = observation.received_unix_ms_;
    result.expires_unix_ms_ =
        ObservationExpiry(observation.received_unix_ms_, limits_.ttl_ms_);
    return result;
  }
  return std::nullopt;
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

std::optional<MetaObservedSessionState> MetaObservationStore::SessionStateFor(
    std::string_view node_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = impl_->sessions_.find(std::string(node_id));
  if (it == impl_->sessions_.end()) {
    return std::nullopt;
  }
  return MetaObservedSessionState{
      .current_boot_id_ = it->second.boot_incarnation_,
      .current_history_id_ = it->second.replication_history_id_,
      .current_generation_ = it->second.generation_,
      .connected_ = it->second.connected_,
      .disconnected_unix_ms_ = it->second.disconnected_unix_ms_,
      .disconnected_boot_id_ = it->second.disconnected_boot_id_,
      .disconnected_generation_ = it->second.disconnected_generation_,
      .heartbeat_failover_projection_ =
          it->second.heartbeat_failover_projection_};
}

std::optional<MetaObservedOwnerState> MetaObservationStore::OwnerObservationFor(
    std::string_view node_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const std::string key(node_id);
  const auto session = impl_->sessions_.find(key);
  if (session == impl_->sessions_.end()) return std::nullopt;

  MetaObservedOwnerState result{
      .identity_ =
          MetaObservationIdentity{
              .node_id_ = key,
              .boot_incarnation_ = session->second.boot_incarnation_,
              .session_generation_ = session->second.generation_,
          },
      .connected_ = session->second.connected_,
      .heartbeat_sequence_ = session->second.heartbeat_sequence_,
      .health_ = session->second.heartbeat_health_,
      .heartbeat_received_steady_ms_ =
          session->second.heartbeat_received_steady_ms_,
      .owner_projection_ = session->second.heartbeat_owner_projection_,
      .causal_progress_received_steady_ms_ =
          session->second.causal_progress_received_steady_ms_,
      .confirmed_grant_sequence_ = session->second.confirmed_grant_sequence_,
      .possible_owner_lease_ = session->second.possible_owner_lease_,
      .installed_owner_lease_ = session->second.installed_owner_lease_,
      .authority_handoff_pending_sequence_ =
          session->second.authority_handoff_pending_sequence_,
  };
  return result;
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

}  // namespace lavik::meta
