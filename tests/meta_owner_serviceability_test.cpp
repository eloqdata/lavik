#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "keylane/meta/owner_serviceability.h"

namespace keylane::meta {
namespace {

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

MetaOwnerServiceabilityCut ServiceableCut() {
  MetaOwnerAuthorityAnchor anchor{
      .group_id_ = "group-a",
      .owner_node_id_ = std::string(40, '1'),
      .owner_assignment_id_ = Bytes<16>(0x21),
      .group_term_ = 7,
      .authority_version_ = 11,
      .grant_revision_ = 13,
      .projection_hash_ = Bytes<32>(0x31),
  };
  MetaOwnerObservationIdentity identity{
      .node_id_ = anchor.owner_node_id_,
      .boot_id_ = Bytes<20>(0x41),
      .session_generation_ = 17,
      .leadership_generation_ = 19,
  };
  MetaOwnerHeartbeatCut heartbeat{
      .identity_ = identity,
      .installed_anchor_ = anchor,
      .sequence_ = 23,
      .fresh_ = true,
      .draining_ = false,
      .storage_ready_ = true,
      .population_ready_ = true,
  };
  MetaCausalLeaseConfirmation lease{
      .identity_ = identity,
      .confirmed_anchor_ = anchor,
      .granted_heartbeat_sequence_ = 22,
      .confirming_heartbeat_sequence_ = 23,
  };
  return {
      .leadership_generation_ = identity.leadership_generation_,
      .leader_authority_eligible_ = true,
      .leadership_warmup_complete_ = true,
      .authority_handoff_complete_ = true,
      .failover_transition_active_ = false,
      .committed_anchor_ = anchor,
      .session_ =
          MetaOwnerSessionCut{
              .identity_ = identity,
              .connected_ = true,
              .heartbeat_ = heartbeat,
              .causal_progress_freshness_ = MetaCausalProgressFreshness::kFresh,
              .causal_lease_confirmation_ = lease,
          },
  };
}

TEST(MetaOwnerServiceabilityTest,
     FullyCurrentOwnerWithCausalLeaseIsServiceable) {
  EXPECT_EQ(EvaluateOwnerServiceability(ServiceableCut()),
            (MetaOwnerServiceabilityDecision{
                .state_ = MetaOwnerServiceabilityState::kServiceable}));
}

TEST(MetaOwnerServiceabilityTest, MissingOrDisconnectedSessionIsUnserviceable) {
  auto missing = ServiceableCut();
  missing.session_.reset();
  const MetaOwnerServiceabilityDecision expected{
      .state_ = MetaOwnerServiceabilityState::kUnserviceable,
      .reason_ = MetaOwnerServiceabilityReason::kSessionMissing,
  };
  EXPECT_EQ(EvaluateOwnerServiceability(missing), expected);

  auto disconnected = ServiceableCut();
  disconnected.session_->connected_ = false;
  EXPECT_EQ(EvaluateOwnerServiceability(disconnected), expected);
}

TEST(MetaOwnerServiceabilityTest,
     ControlPlaneConditionsBlockBeforeRuntimeEvidence) {
  auto cut = ServiceableCut();
  cut.leader_authority_eligible_ = false;
  cut.leadership_warmup_complete_ = false;
  cut.authority_handoff_complete_ = false;
  cut.failover_transition_active_ = true;
  cut.session_.reset();
  EXPECT_EQ(
      EvaluateOwnerServiceability(cut),
      (MetaOwnerServiceabilityDecision{
          .state_ = MetaOwnerServiceabilityState::kBlocked,
          .blocker_ = MetaOwnerServiceabilityBlocker::kLeaderIneligible}));

  cut.leader_authority_eligible_ = true;
  EXPECT_EQ(
      EvaluateOwnerServiceability(cut),
      (MetaOwnerServiceabilityDecision{
          .state_ = MetaOwnerServiceabilityState::kBlocked,
          .blocker_ = MetaOwnerServiceabilityBlocker::kLeadershipWarmup}));

  cut.leadership_warmup_complete_ = true;
  EXPECT_EQ(
      EvaluateOwnerServiceability(cut),
      (MetaOwnerServiceabilityDecision{
          .state_ = MetaOwnerServiceabilityState::kBlocked,
          .blocker_ = MetaOwnerServiceabilityBlocker::kFailoverTransition}));

  cut.failover_transition_active_ = false;
  EXPECT_EQ(
      EvaluateOwnerServiceability(cut),
      (MetaOwnerServiceabilityDecision{
          .state_ = MetaOwnerServiceabilityState::kBlocked,
          .blocker_ = MetaOwnerServiceabilityBlocker::kAuthorityHandoff}));
  cut.authority_handoff_complete_ = true;
  EXPECT_EQ(EvaluateOwnerServiceability(cut),
            (MetaOwnerServiceabilityDecision{
                .state_ = MetaOwnerServiceabilityState::kUnserviceable,
                .reason_ = MetaOwnerServiceabilityReason::kSessionMissing}));
}

TEST(MetaOwnerServiceabilityTest,
     StaleSessionOrHeartbeatAnchorIsIndeterminate) {
  const MetaOwnerServiceabilityDecision expected{
      .state_ = MetaOwnerServiceabilityState::kIndeterminate,
      .reason_ = MetaOwnerServiceabilityReason::kStaleOwnerAnchor,
  };
  const auto expect_stale = [&expected](MetaOwnerServiceabilityCut cut) {
    EXPECT_EQ(EvaluateOwnerServiceability(cut), expected);
  };

  auto wrong_owner = ServiceableCut();
  wrong_owner.session_->identity_.node_id_ = std::string(40, '2');
  expect_stale(std::move(wrong_owner));

  auto old_leader = ServiceableCut();
  --old_leader.session_->identity_.leadership_generation_;
  expect_stale(std::move(old_leader));

  auto wrong_boot = ServiceableCut();
  wrong_boot.session_->heartbeat_->identity_.boot_id_ = Bytes<20>(0x42);
  expect_stale(std::move(wrong_boot));

  auto wrong_group = ServiceableCut();
  wrong_group.session_->heartbeat_->installed_anchor_.group_id_ = "group-b";
  expect_stale(std::move(wrong_group));

  auto wrong_assignment = ServiceableCut();
  wrong_assignment.session_->heartbeat_->installed_anchor_
      .owner_assignment_id_ = Bytes<16>(0x22);
  expect_stale(std::move(wrong_assignment));

  auto wrong_term = ServiceableCut();
  ++wrong_term.session_->heartbeat_->installed_anchor_.group_term_;
  expect_stale(std::move(wrong_term));

  auto wrong_authority = ServiceableCut();
  ++wrong_authority.session_->heartbeat_->installed_anchor_.authority_version_;
  expect_stale(std::move(wrong_authority));

  auto wrong_grant = ServiceableCut();
  ++wrong_grant.session_->heartbeat_->installed_anchor_.grant_revision_;
  expect_stale(std::move(wrong_grant));

  auto wrong_fds = ServiceableCut();
  wrong_fds.session_->heartbeat_->installed_anchor_.projection_hash_ =
      Bytes<32>(0x32);
  expect_stale(std::move(wrong_fds));
}

TEST(MetaOwnerServiceabilityTest,
     DefiniteCausalExpiryOverridesAStaleHeartbeatAnchor) {
  auto cut = ServiceableCut();
  ++cut.session_->heartbeat_->installed_anchor_.grant_revision_;
  cut.session_->causal_progress_freshness_ =
      MetaCausalProgressFreshness::kExpired;

  EXPECT_EQ(EvaluateOwnerServiceability(cut),
            (MetaOwnerServiceabilityDecision{
                .state_ = MetaOwnerServiceabilityState::kUnserviceable,
                .reason_ = MetaOwnerServiceabilityReason::kHeartbeatExpired,
            }));
}

TEST(MetaOwnerServiceabilityTest,
     UnserviceableReasonsHaveFixedFailurePrecedence) {
  auto cut = ServiceableCut();
  cut.session_->heartbeat_->fresh_ = false;
  cut.session_->heartbeat_->draining_ = true;
  cut.session_->heartbeat_->storage_ready_ = false;
  cut.session_->heartbeat_->population_ready_ = false;

  const auto expect_reason = [&cut](MetaOwnerServiceabilityReason reason) {
    EXPECT_EQ(EvaluateOwnerServiceability(cut),
              (MetaOwnerServiceabilityDecision{
                  .state_ = MetaOwnerServiceabilityState::kUnserviceable,
                  .reason_ = reason,
              }));
  };

  cut.session_->connected_ = false;
  expect_reason(MetaOwnerServiceabilityReason::kSessionMissing);
  cut.session_->connected_ = true;
  expect_reason(MetaOwnerServiceabilityReason::kHeartbeatExpired);
  cut.session_->heartbeat_->fresh_ = true;
  expect_reason(MetaOwnerServiceabilityReason::kDraining);
  cut.session_->heartbeat_->draining_ = false;
  expect_reason(MetaOwnerServiceabilityReason::kStorageUnready);
  cut.session_->heartbeat_->storage_ready_ = true;
  expect_reason(MetaOwnerServiceabilityReason::kPopulationUnready);

  cut.session_->heartbeat_.reset();
  expect_reason(MetaOwnerServiceabilityReason::kHeartbeatExpired);
}

TEST(MetaOwnerServiceabilityTest,
     UnknownCausalFreshnessCannotManufactureOwnerFailure) {
  const MetaOwnerServiceabilityDecision pending{
      .state_ = MetaOwnerServiceabilityState::kIndeterminate,
      .reason_ = MetaOwnerServiceabilityReason::kCausalLeasePending,
  };
  auto exact = ServiceableCut();
  exact.session_->causal_progress_freshness_ =
      MetaCausalProgressFreshness::kUnknown;
  EXPECT_EQ(EvaluateOwnerServiceability(exact), pending);

  auto stale = std::move(exact);
  ++stale.session_->heartbeat_->installed_anchor_.grant_revision_;
  EXPECT_EQ(EvaluateOwnerServiceability(stale),
            (MetaOwnerServiceabilityDecision{
                .state_ = MetaOwnerServiceabilityState::kIndeterminate,
                .reason_ = MetaOwnerServiceabilityReason::kStaleOwnerAnchor,
            }));
}

TEST(MetaOwnerServiceabilityTest,
     LeaseMustBeConfirmedByTheCurrentHigherSequenceHeartbeat) {
  const MetaOwnerServiceabilityDecision pending{
      .state_ = MetaOwnerServiceabilityState::kIndeterminate,
      .reason_ = MetaOwnerServiceabilityReason::kCausalLeasePending,
  };

  auto absent = ServiceableCut();
  absent.session_->causal_lease_confirmation_.reset();
  EXPECT_EQ(EvaluateOwnerServiceability(absent), pending);

  auto same_sequence = ServiceableCut();
  same_sequence.session_->causal_lease_confirmation_
      ->granted_heartbeat_sequence_ = 23;
  EXPECT_EQ(EvaluateOwnerServiceability(same_sequence), pending);

  auto not_current = ServiceableCut();
  not_current.session_->causal_lease_confirmation_
      ->confirming_heartbeat_sequence_ = 24;
  EXPECT_EQ(EvaluateOwnerServiceability(not_current), pending);

  auto zero_grant_sequence = ServiceableCut();
  zero_grant_sequence.session_->causal_lease_confirmation_
      ->granted_heartbeat_sequence_ = 0;
  EXPECT_EQ(EvaluateOwnerServiceability(zero_grant_sequence), pending);
}

TEST(MetaOwnerServiceabilityTest, ExpiredCausalProgressIsAHeartbeatFailure) {
  auto cut = ServiceableCut();
  cut.session_->causal_progress_freshness_ =
      MetaCausalProgressFreshness::kExpired;
  EXPECT_EQ(EvaluateOwnerServiceability(cut),
            (MetaOwnerServiceabilityDecision{
                .state_ = MetaOwnerServiceabilityState::kUnserviceable,
                .reason_ = MetaOwnerServiceabilityReason::kHeartbeatExpired,
            }));

  // The same finite bound applies before the first Ack is causally confirmed.
  cut.session_->causal_lease_confirmation_.reset();
  EXPECT_EQ(EvaluateOwnerServiceability(cut),
            (MetaOwnerServiceabilityDecision{
                .state_ = MetaOwnerServiceabilityState::kUnserviceable,
                .reason_ = MetaOwnerServiceabilityReason::kHeartbeatExpired,
            }));
}

TEST(MetaOwnerServiceabilityTest,
     LeaseConfirmationFromAnotherIncarnationOrAnchorIsIndeterminate) {
  const MetaOwnerServiceabilityDecision stale{
      .state_ = MetaOwnerServiceabilityState::kIndeterminate,
      .reason_ = MetaOwnerServiceabilityReason::kStaleOwnerAnchor,
  };

  auto wrong_session = ServiceableCut();
  ++wrong_session.session_->causal_lease_confirmation_->identity_
        .session_generation_;
  EXPECT_EQ(EvaluateOwnerServiceability(wrong_session), stale);

  auto wrong_boot = ServiceableCut();
  wrong_boot.session_->causal_lease_confirmation_->identity_.boot_id_ =
      Bytes<20>(0x42);
  EXPECT_EQ(EvaluateOwnerServiceability(wrong_boot), stale);

  auto wrong_anchor = ServiceableCut();
  ++wrong_anchor.session_->causal_lease_confirmation_->confirmed_anchor_
        .grant_revision_;
  EXPECT_EQ(EvaluateOwnerServiceability(wrong_anchor), stale);
}

TEST(MetaOwnerServiceabilityTest,
     LaterHeartbeatPreservesEarlierCausalLeaseConfirmation) {
  auto cut = ServiceableCut();
  cut.session_->heartbeat_->sequence_ = 29;
  EXPECT_EQ(EvaluateOwnerServiceability(cut),
            (MetaOwnerServiceabilityDecision{
                .state_ = MetaOwnerServiceabilityState::kServiceable}));
}

TEST(MetaOwnerServiceabilityTest, DiagnosticCodesAreStable) {
  EXPECT_EQ(MetaOwnerServiceabilityStateName(
                MetaOwnerServiceabilityState::kServiceable),
            "serviceable");
  EXPECT_EQ(MetaOwnerServiceabilityStateName(
                MetaOwnerServiceabilityState::kUnserviceable),
            "unserviceable");
  EXPECT_EQ(MetaOwnerServiceabilityStateName(
                MetaOwnerServiceabilityState::kIndeterminate),
            "indeterminate");
  EXPECT_EQ(
      MetaOwnerServiceabilityStateName(MetaOwnerServiceabilityState::kBlocked),
      "blocked");

  EXPECT_EQ(
      MetaOwnerServiceabilityReasonName(MetaOwnerServiceabilityReason::kNone),
      "none");
  EXPECT_EQ(MetaOwnerServiceabilityReasonName(
                MetaOwnerServiceabilityReason::kSessionMissing),
            "session_missing");
  EXPECT_EQ(MetaOwnerServiceabilityReasonName(
                MetaOwnerServiceabilityReason::kHeartbeatExpired),
            "heartbeat_expired");
  EXPECT_EQ(MetaOwnerServiceabilityReasonName(
                MetaOwnerServiceabilityReason::kDraining),
            "draining");
  EXPECT_EQ(MetaOwnerServiceabilityReasonName(
                MetaOwnerServiceabilityReason::kStorageUnready),
            "storage_unready");
  EXPECT_EQ(MetaOwnerServiceabilityReasonName(
                MetaOwnerServiceabilityReason::kPopulationUnready),
            "population_unready");
  EXPECT_EQ(MetaOwnerServiceabilityReasonName(
                MetaOwnerServiceabilityReason::kStaleOwnerAnchor),
            "stale_owner_anchor");
  EXPECT_EQ(MetaOwnerServiceabilityReasonName(
                MetaOwnerServiceabilityReason::kCausalLeasePending),
            "causal_lease_pending");

  EXPECT_EQ(
      MetaOwnerServiceabilityBlockerName(MetaOwnerServiceabilityBlocker::kNone),
      "none");
  EXPECT_EQ(MetaOwnerServiceabilityBlockerName(
                MetaOwnerServiceabilityBlocker::kLeaderIneligible),
            "leader_ineligible");
  EXPECT_EQ(MetaOwnerServiceabilityBlockerName(
                MetaOwnerServiceabilityBlocker::kLeadershipWarmup),
            "leadership_warmup");
  EXPECT_EQ(MetaOwnerServiceabilityBlockerName(
                MetaOwnerServiceabilityBlocker::kAuthorityHandoff),
            "authority_handoff");
  EXPECT_EQ(MetaOwnerServiceabilityBlockerName(
                MetaOwnerServiceabilityBlocker::kFailoverTransition),
            "failover_transition");
}

}  // namespace
}  // namespace keylane::meta
