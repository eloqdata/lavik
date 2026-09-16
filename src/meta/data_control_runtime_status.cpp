#include "keylane/meta/data_control_runtime_status.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace keylane::meta {
namespace {

std::vector<MetaDataControlRuntimeGroup> ProjectedGroups(
    std::string_view node_id,
    const cluster::control::FullDesiredState& projection) {
  std::vector<MetaDataControlRuntimeGroup> groups;
  groups.reserve(projection.groups.size());
  for (const auto& group : projection.groups) {
    auto member =
        std::find_if(group.members.begin(), group.members.end(),
                     [&](const auto& item) { return item.node_id == node_id; });
    if (member == group.members.end()) continue;
    groups.push_back({
        .group_id_ = group.group_id,
        .assignment_id_ = member->assignment_id,
        .group_term_ = group.group_term,
        .authority_version_ = group.authority_version,
        .grant_revision_ = group.grant_revision,
        .manifest_revision_ = group.manifest_revision,
        .manifest_digest_ = group.manifest_digest,
        .partition_replication_epoch_ = group.partition_replication_epoch,
    });
  }
  std::sort(groups.begin(), groups.end(),
            [](const auto& left, const auto& right) {
              return left.group_id_ < right.group_id_;
            });
  return groups;
}

void ApplyProjection(MetaDataControlRuntimeNode& node,
                     std::uint64_t validated_committed_high_water,
                     const cluster::control::FullDesiredState& projection) {
  node.source_meta_applied_index_ = projection.source_meta_applied_index;
  node.validated_committed_high_water_ = validated_committed_high_water;
  node.topology_epoch_ = projection.topology_epoch;
  node.projection_hash_ = projection.projection_hash;
  node.authority_lease_duration_ms_ = projection.authority_lease_duration_ms;
  node.groups_ = ProjectedGroups(node.node_id_, projection);
  // A replacement invalidates observations and a previous lease until a
  // heartbeat under the new projection is successfully acknowledged.
  node.health_.reset();
  node.last_lease_decision_.reset();
  node.health_received_unix_ms_ = 0;
  node.lease_decision_heartbeat_sequence_ = 0;
  node.lease_decision_written_unix_ms_ = 0;
}

}  // namespace

MetaLeaderAuthorityEligibilityState AdvanceLeaderAuthorityEligibility(
    MetaLeaderAuthorityEligibilityState current, bool eligible) noexcept {
  if (current.eligible_ == eligible) return current;
  if (current.revision_ == std::numeric_limits<std::uint64_t>::max()) {
    current.eligible_ = false;
    return current;
  }
  ++current.revision_;
  current.eligible_ = eligible;
  return current;
}

void MetaDataControlRuntimeStatus::BeginLeadership(
    std::uint64_t leadership_generation) {
  std::lock_guard<std::mutex> lock(mutex_);
  nodes_.clear();
  unregistered_retries_.clear();
  observed_nodes_.clear();
  leadership_generation_ = leadership_generation;
  leader_authority_eligible_ = false;
  leader_authority_eligibility_revision_ = 0;
}

bool MetaDataControlRuntimeStatus::SetLeaderAuthorityEligible(
    std::uint64_t leadership_generation, bool eligible) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (leadership_generation_ != leadership_generation) return false;
  const auto state = AdvanceLeaderAuthorityEligibility(
      {.eligible_ = leader_authority_eligible_,
       .revision_ = leader_authority_eligibility_revision_},
      eligible);
  leader_authority_eligible_ = state.eligible_;
  leader_authority_eligibility_revision_ = state.revision_;
  return state.eligible_;
}

void MetaDataControlRuntimeStatus::EndLeadership(
    std::uint64_t leadership_generation) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (leadership_generation_ != leadership_generation) return;
  nodes_.clear();
  unregistered_retries_.clear();
  observed_nodes_.clear();
  leadership_generation_ = 0;
  leader_authority_eligible_ = false;
  leader_authority_eligibility_revision_ = 0;
}

void MetaDataControlRuntimeStatus::NoteUnregisteredRetry(
    std::string node_id, std::uint64_t leadership_generation) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (leadership_generation_ != leadership_generation ||
      leadership_generation == 0) {
    return;
  }
  if (!unregistered_retries_.contains(node_id) &&
      unregistered_retries_.size() >= cluster::control::kMaxProjectedNodes) {
    return;
  }
  unregistered_retries_[std::move(node_id)] = leadership_generation;
}

void MetaDataControlRuntimeStatus::PublishCurrent(
    std::string node_id, std::string boot_id,
    const cluster::control::WireId128& session_id,
    const MetaReplicationHistoryId& replication_history_id,
    std::uint32_t replication_flow_count, std::uint64_t session_generation,
    std::uint64_t leadership_generation,
    std::uint64_t validated_committed_high_water,
    const cluster::control::FullDesiredState& projection) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (leadership_generation_ != leadership_generation ||
      !leader_authority_eligible_) {
    return;
  }
  MetaDataControlRuntimeNode node;
  node.node_id_ = std::move(node_id);
  node.boot_id_ = std::move(boot_id);
  node.session_id_ = session_id;
  node.replication_history_id_ = replication_history_id;
  node.replication_flow_count_ = replication_flow_count;
  node.session_generation_ = session_generation;
  node.leadership_generation_ = leadership_generation;
  ApplyProjection(node, validated_committed_high_water, projection);
  unregistered_retries_.erase(node.node_id_);
  if (observed_nodes_.contains(node.node_id_) ||
      observed_nodes_.size() < cluster::control::kMaxProjectedNodes) {
    observed_nodes_.insert(node.node_id_);
  }
  nodes_[node.node_id_] = std::move(node);
}

void MetaDataControlRuntimeStatus::MarkValidated(
    std::string_view node_id, const cluster::control::WireId128& session_id,
    std::uint64_t validated_committed_high_water) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = nodes_.find(std::string(node_id));
  if (found == nodes_.end() || found->second.session_id_ != session_id) return;
  found->second.validated_committed_high_water_ =
      std::max(found->second.validated_committed_high_water_,
               validated_committed_high_water);
}

void MetaDataControlRuntimeStatus::RecordHealth(
    std::string_view node_id, const cluster::control::WireId128& session_id,
    const cluster::control::HeartbeatHealth& health,
    std::int64_t received_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = nodes_.find(std::string(node_id));
  if (found == nodes_.end() || found->second.session_id_ != session_id) return;
  found->second.health_ = health;
  found->second.health_received_unix_ms_ = received_unix_ms;
}

void MetaDataControlRuntimeStatus::RecordLeaseDecisionWritten(
    std::string_view node_id, const cluster::control::WireId128& session_id,
    std::uint64_t heartbeat_sequence,
    const cluster::control::LeaseDecision& written_decision,
    std::int64_t written_unix_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = nodes_.find(std::string(node_id));
  if (found == nodes_.end() || found->second.session_id_ != session_id) return;
  found->second.last_lease_decision_ = written_decision;
  found->second.lease_decision_heartbeat_sequence_ = heartbeat_sequence;
  found->second.lease_decision_written_unix_ms_ = written_unix_ms;
}

void MetaDataControlRuntimeStatus::Remove(
    std::string_view node_id, const cluster::control::WireId128* session_id) {
  if (session_id == nullptr) return;
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = nodes_.find(std::string(node_id));
  if (found == nodes_.end() || found->second.session_id_ != *session_id) {
    return;
  }
  nodes_.erase(found);
}

MetaDataControlRuntimeSnapshot MetaDataControlRuntimeStatus::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  MetaDataControlRuntimeSnapshot snapshot;
  snapshot.leadership_generation_ = leadership_generation_;
  snapshot.leader_authority_eligible_ = leader_authority_eligible_;
  snapshot.leader_authority_eligibility_revision_ =
      leader_authority_eligibility_revision_;
  snapshot.nodes_.reserve(nodes_.size());
  for (const auto& [id, node] : nodes_) snapshot.nodes_.push_back(node);
  snapshot.unregistered_retries_.reserve(unregistered_retries_.size());
  for (const auto& retry : unregistered_retries_) {
    snapshot.unregistered_retries_.push_back(retry.first);
  }
  snapshot.observed_nodes_.assign(observed_nodes_.begin(),
                                  observed_nodes_.end());
  return snapshot;
}

MetaDataControlLeadershipState MetaDataControlRuntimeStatus::LeadershipState()
    const {
  std::lock_guard<std::mutex> lock(mutex_);
  return {.leadership_generation_ = leadership_generation_,
          .leader_authority_eligible_ = leader_authority_eligible_,
          .leader_authority_eligibility_revision_ =
              leader_authority_eligibility_revision_};
}

}  // namespace keylane::meta
