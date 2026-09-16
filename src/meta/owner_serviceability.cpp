#include "keylane/meta/owner_serviceability.h"

namespace keylane::meta {

MetaOwnerServiceabilityDecision EvaluateOwnerServiceability(
    const MetaOwnerServiceabilityCut& cut) noexcept {
  const auto blocked = [](MetaOwnerServiceabilityBlocker blocker) {
    return MetaOwnerServiceabilityDecision{
        .state_ = MetaOwnerServiceabilityState::kBlocked, .blocker_ = blocker};
  };
  if (!cut.leader_authority_eligible_) {
    return blocked(MetaOwnerServiceabilityBlocker::kLeaderIneligible);
  }
  if (!cut.leadership_warmup_complete_) {
    return blocked(MetaOwnerServiceabilityBlocker::kLeadershipWarmup);
  }
  if (cut.failover_transition_active_) {
    return blocked(MetaOwnerServiceabilityBlocker::kFailoverTransition);
  }
  if (!cut.authority_handoff_complete_) {
    return blocked(MetaOwnerServiceabilityBlocker::kAuthorityHandoff);
  }
  if (!cut.session_.has_value()) {
    return {.state_ = MetaOwnerServiceabilityState::kUnserviceable,
            .reason_ = MetaOwnerServiceabilityReason::kSessionMissing};
  }
  const auto indeterminate = [](MetaOwnerServiceabilityReason reason) {
    return MetaOwnerServiceabilityDecision{
        .state_ = MetaOwnerServiceabilityState::kIndeterminate,
        .reason_ = reason};
  };
  const MetaOwnerServiceabilityCut::Session& session = *cut.session_;
  if (!session.current_) {
    return indeterminate(MetaOwnerServiceabilityReason::kStaleOwnerAnchor);
  }
  if (!session.heartbeat_.has_value()) {
    return {.state_ = MetaOwnerServiceabilityState::kUnserviceable,
            .reason_ = MetaOwnerServiceabilityReason::kHeartbeatExpired};
  }
  const MetaOwnerServiceabilityCut::Session::Heartbeat& heartbeat =
      *session.heartbeat_;
  const auto unserviceable = [](MetaOwnerServiceabilityReason reason) {
    return MetaOwnerServiceabilityDecision{
        .state_ = MetaOwnerServiceabilityState::kUnserviceable,
        .reason_ = reason};
  };
  // An internally matched session timestamp beyond its fixed TTL, or causal
  // progress beyond the effective lease carried by its observed projection,
  // is exact failure evidence. Evaluate that finite bound before comparing
  // the heartbeat's FDS anchor: a publisher/runtime snapshot may already have
  // advanced while the Owner stopped on the preceding projection, and that
  // ordinary cross-source tear must not hide expiry forever.
  if (!heartbeat.fresh_ || session.causal_progress_freshness_ ==
                               MetaCausalProgressFreshness::kExpired) {
    return unserviceable(MetaOwnerServiceabilityReason::kHeartbeatExpired);
  }
  if (heartbeat.installed_anchor_ != cut.committed_anchor_) {
    return indeterminate(MetaOwnerServiceabilityReason::kStaleOwnerAnchor);
  }
  if (heartbeat.draining_) {
    return unserviceable(MetaOwnerServiceabilityReason::kDraining);
  }
  if (!heartbeat.storage_ready_) {
    return unserviceable(MetaOwnerServiceabilityReason::kStorageUnready);
  }
  if (!heartbeat.population_ready_) {
    return unserviceable(MetaOwnerServiceabilityReason::kPopulationUnready);
  }
  if (session.causal_progress_freshness_ !=
      MetaCausalProgressFreshness::kFresh) {
    return indeterminate(MetaOwnerServiceabilityReason::kCausalLeasePending);
  }
  if (!session.confirmed_grant_sequence_.has_value()) {
    return indeterminate(MetaOwnerServiceabilityReason::kCausalLeasePending);
  }
  if (*session.confirmed_grant_sequence_ == 0 ||
      heartbeat.sequence_ <= *session.confirmed_grant_sequence_) {
    return indeterminate(MetaOwnerServiceabilityReason::kCausalLeasePending);
  }
  return {.state_ = MetaOwnerServiceabilityState::kServiceable};
}

std::string_view MetaOwnerServiceabilityStateName(
    MetaOwnerServiceabilityState state) noexcept {
  switch (state) {
    case MetaOwnerServiceabilityState::kServiceable:
      return "serviceable";
    case MetaOwnerServiceabilityState::kUnserviceable:
      return "unserviceable";
    case MetaOwnerServiceabilityState::kIndeterminate:
      return "indeterminate";
    case MetaOwnerServiceabilityState::kBlocked:
      return "blocked";
  }
  return "unknown";
}

std::string_view MetaOwnerServiceabilityReasonName(
    MetaOwnerServiceabilityReason reason) noexcept {
  switch (reason) {
    case MetaOwnerServiceabilityReason::kNone:
      return "none";
    case MetaOwnerServiceabilityReason::kSessionMissing:
      return "session_missing";
    case MetaOwnerServiceabilityReason::kHeartbeatExpired:
      return "heartbeat_expired";
    case MetaOwnerServiceabilityReason::kDraining:
      return "draining";
    case MetaOwnerServiceabilityReason::kStorageUnready:
      return "storage_unready";
    case MetaOwnerServiceabilityReason::kPopulationUnready:
      return "population_unready";
    case MetaOwnerServiceabilityReason::kStaleOwnerAnchor:
      return "stale_owner_anchor";
    case MetaOwnerServiceabilityReason::kCausalLeasePending:
      return "causal_lease_pending";
  }
  return "unknown";
}

std::string_view MetaOwnerServiceabilityBlockerName(
    MetaOwnerServiceabilityBlocker blocker) noexcept {
  switch (blocker) {
    case MetaOwnerServiceabilityBlocker::kNone:
      return "none";
    case MetaOwnerServiceabilityBlocker::kLeaderIneligible:
      return "leader_ineligible";
    case MetaOwnerServiceabilityBlocker::kLeadershipWarmup:
      return "leadership_warmup";
    case MetaOwnerServiceabilityBlocker::kAuthorityHandoff:
      return "authority_handoff";
    case MetaOwnerServiceabilityBlocker::kFailoverTransition:
      return "failover_transition";
  }
  return "unknown";
}

}  // namespace keylane::meta
