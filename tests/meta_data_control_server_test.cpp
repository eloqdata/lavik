#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>

#include "celer/net/connection.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/control_projector.h"
#include "keylane/meta/data_control_runtime_status.h"
#include "keylane/meta/data_control_server.h"
#include "keylane/meta/observation_store.h"

namespace keylane::meta {

// Constructs the smallest possible server shell for cancellation-policy
// tests. A rejected executor lets the test exercise the allocation/runtime
// failure branch deterministically without trying to induce host OOM.
class MetaDataControlServerTestPeer {
 public:
  static std::shared_ptr<MetaDataControlServer> LifecycleHarness(
      celer::ForeignExecutor executor, bool shutdown_complete) {
    return MetaDataControlServer::LifecycleHarnessForTest(executor,
                                                          shutdown_complete);
  }

  static void StartWithRejectedExecutor(MetaDataControlServer& server) {
    server.StartOnExecutor(nullptr);
  }
};

}  // namespace keylane::meta

namespace {

namespace control = keylane::cluster::control;
using keylane::meta::BuildCommittedMetaDirectory;
using keylane::meta::EvaluateLeaseChallenge;
using keylane::meta::EvaluateReplacementDisposition;
using keylane::meta::IngestHeartbeatObservations;
using keylane::meta::MetaCommittedFacts;
using keylane::meta::MetaCommittedView;
using keylane::meta::MetaDataControlRuntimeStatus;
using keylane::meta::MetaDataControlServer;
using keylane::meta::MetaDataControlServerOptions;
using keylane::meta::MetaDataControlServerTestPeer;
using keylane::meta::MetaLeaderRuntimeDisposition;
using keylane::meta::MetaLeaderRuntimeGuard;
using keylane::meta::MetaLeaseEvaluation;
using keylane::meta::MetaLeaseHandoffGuard;
using keylane::meta::MetaNodeHealthObs;
using keylane::meta::MetaObservationStore;
using keylane::meta::MetaObservedOwnerProjection;
using keylane::meta::MetaReplacementDisposition;
using keylane::meta::MetaStores;
using keylane::meta::UnfencedSupersededAuthorities;
using keylane::meta::detail::ApplyLeadershipValidityLimit;
using keylane::meta::detail::BoundNodeSessionRegistry;
using keylane::meta::detail::ConfirmedLeaseForHeartbeat;
using keylane::meta::detail::EstablishedSessionReadTimeout;
using keylane::meta::detail::FailoverProjectionForHeartbeat;
using keylane::meta::detail::MetaCommittedViewCache;
using keylane::meta::detail::PendingHandshakeLimiter;
using keylane::meta::detail::RecordEquivalentTransferBoundary;
using keylane::meta::detail::RetainedProjectionLimiter;
using keylane::meta::detail::TransferBoundaryNeedsProjectionValidation;

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

std::string Identity(char value) { return std::string(40, value); }

TEST(MetaDataControlRuntimeStatusTest,
     PublishesOnlyCurrentSessionAndAckedHeartbeatFacts) {
  MetaDataControlRuntimeStatus status;
  status.BeginLeadership(/*leadership_generation=*/11);
  status.SetLeaderAuthorityEligible(/*leadership_generation=*/11, true);
  control::FullDesiredState projection;
  projection.control_revision = 7;
  projection.topology_epoch = 3;
  projection.authority_lease_duration_ms = 250;
  projection.groups.push_back({
      .group_id = "group-a",
      .members = {{.node_id = Identity('1'), .assignment_id = Bytes<16>(0x11)},
                  {.node_id = Identity('3'), .assignment_id = Bytes<16>(0x12)}},
      .owner_node_id = Identity('1'),
      .owner_assignment_id = Bytes<16>(0x11),
      .group_term = 4,
      .manifest_revision = 8,
      .manifest_digest = Bytes<32>(0x32),
      .partition_replication_epoch = 9,
  });
  const auto session = Bytes<16>(0x41);
  status.PublishCurrent(Identity('1'), Identity('2'), session, Bytes<20>(0x51),
                        /*replication_flow_count=*/3,
                        /*session_generation=*/10,
                        /*leadership_generation=*/11,
                        /*validated_committed_high_water=*/7, projection);
  auto snapshot = status.Snapshot();
  ASSERT_EQ(snapshot.nodes_.size(), 1u);
  EXPECT_EQ(snapshot.observed_nodes_,
            std::vector<std::string>({Identity('1')}));
  EXPECT_EQ(snapshot.nodes_[0].replication_history_id_, Bytes<20>(0x51));
  EXPECT_EQ(snapshot.nodes_[0].replication_flow_count_, 3);
  EXPECT_FALSE(snapshot.nodes_[0].health_.has_value());
  EXPECT_EQ(snapshot.nodes_[0].groups_.size(), 1u);

  // Validating an unchanged FDS advances freshness, not the origin of the
  // object Data acknowledged. Old sessions cannot advance that proof.
  status.MarkValidated(Identity('1'), Bytes<16>(0x42), 99);
  EXPECT_EQ(status.Snapshot().nodes_[0].validated_committed_high_water_, 7u);
  status.MarkValidated(Identity('1'), session, 9);
  status.MarkValidated(Identity('1'), session, 8);
  snapshot = status.Snapshot();
  EXPECT_EQ(snapshot.nodes_[0].validated_committed_high_water_, 9u);
  EXPECT_EQ(snapshot.nodes_[0].control_revision_, 7u);

  control::LeaseDenied denied;
  denied.reason = control::LeaseDenialReason::kNodeNotReady;
  status.RecordHealth(
      Identity('1'), session,
      {.storage_ready = true, .population_ready = false, .draining = false},
      /*received_unix_ms=*/100);
  status.RecordLeaseDecisionWritten(Identity('1'), session,
                                    /*heartbeat_sequence=*/17,
                                    control::LeaseDecision(denied),
                                    /*written_unix_ms=*/101);
  snapshot = status.Snapshot();
  ASSERT_TRUE(snapshot.nodes_[0].health_.has_value());
  EXPECT_TRUE(snapshot.nodes_[0].last_lease_decision_.has_value());
  EXPECT_EQ(snapshot.nodes_[0].lease_decision_heartbeat_sequence_, 17u);
  EXPECT_EQ(snapshot.nodes_[0].lease_decision_written_unix_ms_, 101);

  const auto stale_session = Bytes<16>(0x42);
  status.Remove(Identity('1'), &stale_session);
  EXPECT_EQ(status.Snapshot().nodes_.size(), 1u);
  status.Remove(Identity('1'), &session);
  EXPECT_TRUE(status.Snapshot().nodes_.empty());
  EXPECT_EQ(status.Snapshot().observed_nodes_,
            std::vector<std::string>({Identity('1')}));

  status.PublishCurrent(Identity('3'), Identity('4'), session, Bytes<20>(0x52),
                        /*replication_flow_count=*/3,
                        /*session_generation=*/12,
                        /*leadership_generation=*/11,
                        /*validated_committed_high_water=*/7, projection);
  snapshot = status.Snapshot();
  ASSERT_EQ(snapshot.nodes_.size(), 1u);
  EXPECT_EQ(snapshot.observed_nodes_,
            std::vector<std::string>({Identity('1'), Identity('3')}));
  ASSERT_EQ(snapshot.nodes_[0].groups_.size(), 1u);
  EXPECT_EQ(snapshot.nodes_[0].groups_[0].assignment_id_, Bytes<16>(0x12));

  status.EndLeadership(/*leadership_generation=*/11);
  snapshot = status.Snapshot();
  EXPECT_EQ(snapshot.leadership_generation_, 0u);
  EXPECT_FALSE(snapshot.leader_authority_eligible_);
  EXPECT_TRUE(snapshot.nodes_.empty());
  EXPECT_TRUE(snapshot.observed_nodes_.empty());
  status.PublishCurrent(Identity('1'), Identity('2'), session, Bytes<20>(0x53),
                        /*replication_flow_count=*/3,
                        /*session_generation=*/13,
                        /*leadership_generation=*/11,
                        /*validated_committed_high_water=*/7, projection);
  EXPECT_TRUE(status.Snapshot().nodes_.empty());
}

TEST(MetaDataControlRuntimeStatusTest,
     EligibilityRecoversWithinTheSameLeadershipGeneration) {
  MetaDataControlRuntimeStatus status;
  MetaLeaderRuntimeGuard guard(/*leadership_validity_ms=*/250);
  status.BeginLeadership(/*leadership_generation=*/11);
  EXPECT_EQ(status.Snapshot().leader_authority_eligibility_revision_, 0u);
  guard.Reset(/*now_suspend_clock_ms=*/1'000,
              /*now_active_clock_ms=*/2'000);
  const auto session = Bytes<16>(0x41);
  control::FullDesiredState projection;
  const auto publish = [&] {
    status.PublishCurrent(Identity('1'), Identity('2'), session,
                          Bytes<20>(0x54), /*replication_flow_count=*/3,
                          /*session_generation=*/10,
                          /*leadership_generation=*/11,
                          /*validated_committed_high_water=*/7, projection);
  };

  // A suspend gap during initial membership reconciliation can leave the
  // first authority check temporarily ineligible within a valid leader epoch.
  const auto initial = guard.Observe(/*now_suspend_clock_ms=*/1'250,
                                     /*now_active_clock_ms=*/2'000);
  ASSERT_EQ(initial, MetaLeaderRuntimeDisposition::kQuarantineStarted);
  status.SetLeaderAuthorityEligible(11, false);
  EXPECT_EQ(status.Snapshot().leader_authority_eligibility_revision_, 0u);
  publish();
  EXPECT_FALSE(status.LeadershipState().leader_authority_eligible_);
  EXPECT_TRUE(status.Snapshot().nodes_.empty());

  const auto recovered = guard.Observe(/*now_suspend_clock_ms=*/1'500,
                                       /*now_active_clock_ms=*/2'250);
  ASSERT_EQ(recovered, MetaLeaderRuntimeDisposition::kEligible);
  status.SetLeaderAuthorityEligible(11, true);
  EXPECT_EQ(status.Snapshot().leader_authority_eligibility_revision_, 1u);
  publish();
  auto leadership = status.LeadershipState();
  EXPECT_TRUE(leadership.leader_authority_eligible_);
  EXPECT_EQ(leadership.leader_authority_eligibility_revision_, 1u);
  ASSERT_EQ(status.Snapshot().nodes_.size(), 1u);

  // A later transient authority loss changes the status bracket, without
  // destroying the established session or requiring a new leader generation.
  status.SetLeaderAuthorityEligible(11, false);
  EXPECT_EQ(status.Snapshot().leader_authority_eligibility_revision_, 2u);
  leadership = status.LeadershipState();
  EXPECT_FALSE(leadership.leader_authority_eligible_);
  EXPECT_EQ(leadership.leader_authority_eligibility_revision_, 2u);
  EXPECT_EQ(status.Snapshot().nodes_.size(), 1u);
  status.SetLeaderAuthorityEligible(11, true);
  EXPECT_EQ(status.Snapshot().leader_authority_eligibility_revision_, 3u);
  leadership = status.LeadershipState();
  EXPECT_TRUE(leadership.leader_authority_eligible_);
  EXPECT_EQ(leadership.leader_authority_eligibility_revision_, 3u);
  status.RecordHealth(Identity('1'), session,
                      {.storage_ready = true, .population_ready = true},
                      /*received_unix_ms=*/100);
  const auto snapshot = status.Snapshot();
  ASSERT_EQ(snapshot.nodes_.size(), 1u);
  EXPECT_EQ(snapshot.leadership_generation_, 11u);
  EXPECT_TRUE(snapshot.leader_authority_eligible_);
  EXPECT_EQ(snapshot.nodes_[0].health_received_unix_ms_, 100);
}

TEST(MetaDataControlRuntimeStatusTest,
     EligibilityMutationReportsTheCurrentGenerationEffectiveState) {
  MetaDataControlRuntimeStatus status;
  status.BeginLeadership(/*leadership_generation=*/11);

  EXPECT_FALSE(
      status.SetLeaderAuthorityEligible(/*leadership_generation=*/10, true));
  EXPECT_TRUE(
      status.SetLeaderAuthorityEligible(/*leadership_generation=*/11, true));
  EXPECT_TRUE(
      status.SetLeaderAuthorityEligible(/*leadership_generation=*/11, true));
  EXPECT_FALSE(
      status.SetLeaderAuthorityEligible(/*leadership_generation=*/11, false));
}

TEST(MetaDataControlRuntimeStatusTest,
     UnregisteredRetriesAreGenerationScopedAndClearedByAdmission) {
  MetaDataControlRuntimeStatus status;
  status.BeginLeadership(/*leadership_generation=*/11);
  status.NoteUnregisteredRetry(Identity('2'), /*leadership_generation=*/10);
  status.NoteUnregisteredRetry(Identity('2'), /*leadership_generation=*/11);
  status.NoteUnregisteredRetry(Identity('1'), /*leadership_generation=*/11);

  auto snapshot = status.Snapshot();
  EXPECT_EQ(snapshot.unregistered_retries_,
            std::vector<std::string>({Identity('1'), Identity('2')}));

  status.SetLeaderAuthorityEligible(/*leadership_generation=*/11, true);
  control::FullDesiredState projection;
  status.PublishCurrent(Identity('1'), Identity('3'), Bytes<16>(0x41),
                        Bytes<20>(0x51), /*replication_flow_count=*/3,
                        /*session_generation=*/1,
                        /*leadership_generation=*/11,
                        /*validated_committed_high_water=*/1, projection);
  EXPECT_EQ(status.Snapshot().unregistered_retries_,
            std::vector<std::string>({Identity('2')}));

  status.EndLeadership(/*leadership_generation=*/11);
  EXPECT_TRUE(status.Snapshot().unregistered_retries_.empty());
}

TEST(MetaDataControlRuntimeStatusTest, UnregisteredRetryEvidenceIsBounded) {
  MetaDataControlRuntimeStatus status;
  status.BeginLeadership(/*leadership_generation=*/11);
  for (std::size_t index = 0; index < control::kMaxProjectedNodes + 1;
       ++index) {
    status.NoteUnregisteredRetry("declared-" + std::to_string(index),
                                 /*leadership_generation=*/11);
  }

  EXPECT_EQ(status.Snapshot().unregistered_retries_.size(),
            control::kMaxProjectedNodes);
}

TEST(MetaCommittedViewCacheTest, CopiesOncePerNewAppliedHighWater) {
  std::uint64_t next_index = 3;
  std::size_t loads = 0;
  MetaCommittedViewCache cache([&] {
    ++loads;
    MetaStores stores;
    return MetaCommittedView(std::move(stores), next_index);
  });

  auto first = cache.Get(1);
  ASSERT_TRUE(first.ok()) << first.status();
  auto same = cache.Get(3);
  ASSERT_TRUE(same.ok()) << same.status();
  EXPECT_EQ(loads, 1u);
  EXPECT_EQ(first->get(), same->get());

  next_index = 7;
  auto newer = cache.Get(4);
  ASSERT_TRUE(newer.ok()) << newer.status();
  EXPECT_EQ(loads, 2u);
  EXPECT_NE(first->get(), newer->get());
  ASSERT_TRUE(cache.Get(7).ok());
  EXPECT_EQ(loads, 2u);

  MetaStores adopted_stores;
  auto adopted = cache.Adopt(MetaCommittedView(std::move(adopted_stores), 9));
  ASSERT_NE(adopted, nullptr);
  EXPECT_EQ(adopted->applied_index(), 9u);
  ASSERT_TRUE(cache.Get(9).ok());
  EXPECT_EQ(loads, 2u);

  next_index = 10;
  EXPECT_EQ(cache.Get(11).status().code(), absl::StatusCode::kInternal);
  EXPECT_EQ(loads, 3u);
}

TEST(MetaTransferBoundaryTest,
     EquivalentCommitIsProjectedOnceUntilTheHighWaterAdvances) {
  std::uint64_t validated = 10;
  EXPECT_TRUE(TransferBoundaryNeedsProjectionValidation(
      /*published_index=*/20, /*committed_high_water=*/20, validated));

  RecordEquivalentTransferBoundary(/*applied_index=*/20, &validated);
  EXPECT_EQ(validated, 20U);
  EXPECT_FALSE(TransferBoundaryNeedsProjectionValidation(
      /*published_index=*/20, /*committed_high_water=*/20, validated));

  EXPECT_TRUE(TransferBoundaryNeedsProjectionValidation(
      /*published_index=*/21, /*committed_high_water=*/21, validated));
}

TEST(MetaTransferBoundaryTest,
     SupersessionRetriesInSessionAndPreservesAVisibleObjectsAppliedAck) {
  EXPECT_EQ(keylane::meta::detail::ClassifyPublisherSupersession(
                /*receiver_can_apply=*/false),
            keylane::meta::detail::MetaPublisherTransferDisposition::
                kRetryBeforeApplyInSession);
  EXPECT_EQ(keylane::meta::detail::ClassifyPublisherSupersession(
                /*receiver_can_apply=*/true),
            keylane::meta::detail::MetaPublisherTransferDisposition::
                kAwaitExactAppliedAndRetryInSession);
}

TEST(MetaPublisherAdoptionGateTest,
     HoldsFollowingHeartbeatUntilPublisherAdoptsAppliedProjection) {
  keylane::meta::detail::MetaPublisherAdoptionGate gate;
  EXPECT_FALSE(gate.pending());

  // Deterministically model the scheduling gap: the reader consumes Applied,
  // then gets another turn before the awakened publisher updates installed_.
  gate.ObserveAppliedReceipt();
  EXPECT_TRUE(gate.pending());

  gate.MarkProjectionAdopted();
  EXPECT_FALSE(gate.pending());
}

control::FullDesiredState Desired() {
  control::FullDesiredState desired;
  desired.authority_lease_duration_ms = 5000;
  desired.control_revision = 0x42;
  control::WireDesiredGroup group;
  group.group_id = "group-a";
  group.owner_node_id = Identity('1');
  group.owner_assignment_id = Bytes<16>(0x22);
  group.group_term = 7;
  group.grant_active = true;
  group.partition_replication_epoch = 4;
  desired.groups.push_back(group);
  return desired;
}

control::LeaseChallenge Challenge() {
  return control::LeaseChallenge{
      .nonce = Bytes<16>(0x11),
      .control_revision = 0x42,
      .group_id = "group-a",
      .assignment_id = Bytes<16>(0x22),
      .group_term = 7,
  };
}

class HeartbeatFacts : public MetaCommittedFacts {
 public:
  bool IsActiveNode(std::string_view node_id) const override {
    return node_id == Identity('1');
  }
  std::uint64_t CurrentGroupTerm(std::string_view group_id) const override {
    return group_id == "group-a" ? 7 : 0;
  }
  std::uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const override {
    return group_id == "group-a" ? 9 : 0;
  }
  keylane::meta::MetaHash256 CurrentPopulationManifestDigest(
      std::string_view) const override {
    return {};
  }
  std::uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const override {
    return group_id == "group-a" ? 4 : 0;
  }
  bool AssignmentMatches(
      std::string_view group_id, std::string_view node_id,
      const keylane::meta::MetaAssignmentId& assignment_id) const override {
    return group_id == "group-a" && node_id == Identity('1') &&
           assignment_id == Bytes<16>(0x22);
  }
  bool IsOwnerAssignment(
      std::string_view, std::string_view,
      const keylane::meta::MetaAssignmentId&) const override {
    return false;
  }
};

class EvidenceFacts final : public HeartbeatFacts {
 public:
};

class FailoverHeartbeatFacts final : public HeartbeatFacts {
 public:
  std::optional<FailoverTransitionView> FailoverTransitionById(
      const keylane::meta::MetaFailoverTransitionId& id) const override {
    if (id != Bytes<16>(0x31)) return std::nullopt;
    keylane::meta::MetaFailoverCandidateAction action;
    action.action_id_ = Bytes<16>(0x32);
    action.candidate_ = {Identity('1'), Bytes<16>(0x22), Bytes<20>(0x22)};
    action.domain_ = {
        6, Identity('3'), Bytes<16>(0x33), Bytes<20>(0x44), Bytes<20>(0x55), 1};
    keylane::meta::MetaFailoverTransition transition;
    transition.transition_id_ = id;
    transition.revision_ = 8;
    transition.mode_ = keylane::meta::MetaFailoverMode::kUncontrolled;
    transition.target_term_ = 7;
    transition.candidate_action_ = action;
    return FailoverTransitionView{"group-a", std::move(transition)};
  }
};

TEST(MetaDataControlLifecycleTest,
     CompletedShutdownMakesCancellationAnExecutorIndependentNoOp) {
  auto server = MetaDataControlServerTestPeer::LifecycleHarness(
      celer::ForeignExecutor{}, /*shutdown_complete=*/true);
  server->Shutdown();
  server->CancelAndWait();
}

TEST(MetaDataControlLifecycleDeathTest,
     RejectedActiveCancellationNotificationFailsStop) {
  EXPECT_DEATH(
      {
        auto server = MetaDataControlServerTestPeer::LifecycleHarness(
            celer::ForeignExecutor{}, /*shutdown_complete=*/false);
        server->CancelAndWait();
      },
      "");
}

TEST(MetaDataControlLifecycleDeathTest,
     RejectedLeaderStartNotificationFailsStop) {
  EXPECT_DEATH(
      {
        auto server = MetaDataControlServerTestPeer::LifecycleHarness(
            celer::ForeignExecutor{}, /*shutdown_complete=*/false);
        MetaDataControlServerTestPeer::StartWithRejectedExecutor(*server);
      },
      "");
}

TEST(MetaDataControlLifecycleDeathTest,
     RejectedActiveShutdownNotificationFailsStop) {
  EXPECT_DEATH(
      {
        auto server = MetaDataControlServerTestPeer::LifecycleHarness(
            celer::ForeignExecutor{}, /*shutdown_complete=*/false);
        server->Shutdown();
      },
      "");
}

TEST(MetaDataControlHandshakeLimitTest,
     RejectsAtDomainCapAndReusesReleasedPermit) {
  PendingHandshakeLimiter limiter(/*limit=*/2);
  auto first = limiter.TryAcquire();
  auto second = limiter.TryAcquire();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(limiter.pending(), 2u);
  EXPECT_FALSE(limiter.TryAcquire().has_value());

  first->Release();
  EXPECT_EQ(limiter.pending(), 1u);
  auto replacement = limiter.TryAcquire();
  ASSERT_TRUE(replacement.has_value());
  EXPECT_EQ(limiter.pending(), 2u);

  replacement.reset();
  second.reset();
  EXPECT_EQ(limiter.pending(), 0u);
}

TEST(MetaDataControlHandshakeLimitTest,
     FollowerRetainsPermitUntilRedirectCompletion) {
  PendingHandshakeLimiter limiter(/*limit=*/1);
  auto redirect = limiter.TryAcquire();
  ASSERT_TRUE(redirect.has_value());

  // A committed binding alone is insufficient on a follower: the bounded
  // redirect write still owns this permit, so stalled writers cannot recycle
  // capacity into an unbounded task/descriptor population.
  EXPECT_FALSE(limiter.TryAcquire().has_value());
  redirect.reset();
  EXPECT_EQ(limiter.pending(), 0u);
  EXPECT_TRUE(limiter.TryAcquire().has_value());
}

TEST(MetaDataControlHandshakeLimitTest,
     LeaderReleasesPermitOnlyAfterClaimingOneNodeSlot) {
  PendingHandshakeLimiter limiter(/*limit=*/1);
  BoundNodeSessionRegistry slots;
  auto handshake = limiter.TryAcquire();
  ASSERT_TRUE(handshake.has_value());
  celer::Connection first;
  celer::Connection duplicate;
  MetaDataControlRuntimeStatus status;
  status.BeginLeadership(/*leadership_generation=*/11);
  status.SetLeaderAuthorityEligible(11, true);
  const auto session = Bytes<16>(0x41);
  control::FullDesiredState projection;

  ASSERT_TRUE(slots.TryClaim("node-a", &first));
  status.PublishCurrent("node-a", Identity('2'), session, Bytes<20>(0x55),
                        /*replication_flow_count=*/3,
                        /*session_generation=*/10,
                        /*leadership_generation=*/11,
                        /*validated_committed_high_water=*/7, projection);
  handshake->Release();
  auto next_handshake = limiter.TryAcquire();
  ASSERT_TRUE(next_handshake.has_value());
  EXPECT_FALSE(slots.TryClaim("node-a", &duplicate));
  EXPECT_EQ(slots.size(), 1u);

  // A rejected duplicate never receives a session ID. Its cleanup must keep
  // both the incumbent's admission slot and its live runtime observations.
  slots.Release("node-a", &duplicate);
  status.Remove("node-a", nullptr);
  EXPECT_EQ(slots.size(), 1u);
  ASSERT_EQ(status.Snapshot().nodes_.size(), 1u);
  status.MarkValidated("node-a", session, 8);
  status.RecordHealth("node-a", session,
                      {.storage_ready = true, .population_ready = true},
                      /*received_unix_ms=*/100);
  const auto snapshot = status.Snapshot();
  ASSERT_EQ(snapshot.nodes_.size(), 1u);
  EXPECT_EQ(snapshot.nodes_[0].validated_committed_high_water_, 8u);
  EXPECT_EQ(snapshot.nodes_[0].health_received_unix_ms_, 100);
  slots.Release("node-a", &first);
  status.Remove("node-a", &session);
  EXPECT_EQ(slots.size(), 0u);
  EXPECT_TRUE(status.Snapshot().nodes_.empty());
  EXPECT_TRUE(slots.TryClaim("node-a", &duplicate));
}

TEST(MetaDataControlProjectionLimitTest, ChargesResizesMovesAndReleases) {
  RetainedProjectionLimiter limiter(/*limit=*/100);
  auto first = limiter.TryAcquire(60);
  auto second = limiter.TryAcquire(30);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(limiter.retained_bytes(), 90U);
  EXPECT_FALSE(limiter.TryAcquire(11).has_value());

  EXPECT_TRUE(first->Resize(70).ok());
  EXPECT_EQ(limiter.retained_bytes(), 100U);
  EXPECT_EQ(first->Resize(71).code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(first->bytes(), 70U);
  EXPECT_EQ(limiter.retained_bytes(), 100U);

  RetainedProjectionLimiter::Permit moved = std::move(*first);
  first.reset();
  EXPECT_EQ(limiter.retained_bytes(), 100U);
  moved.Release();
  EXPECT_EQ(limiter.retained_bytes(), 30U);
  second.reset();
  EXPECT_EQ(limiter.retained_bytes(), 0U);
}

TEST(MetaDataControlProjectionLimitTest, BatchWeightIncludesOwnedCapacities) {
  keylane::meta::NodeControlBatch batch;
  const std::size_t empty = keylane::meta::NodeControlBatchRetainedBytes(batch);
  batch.encoded_full_state.reserve(1024);
  batch.full_state.nodes.push_back({Identity('1'), "127.0.0.1", 7000, 0});
  batch.full_state.nodes.front().host.reserve(512);
  EXPECT_GT(keylane::meta::NodeControlBatchRetainedBytes(batch), empty + 1400);
}

TEST(MetaDataControlOptionsTest, TlsIsAllOrNone) {
  MetaDataControlServerOptions options;
  options.server_id_ = 1;
  options.bind_host_ = "127.0.0.1";
  options.port_ = 7000;
  options.leadership_validity_ms_ = 300;
  options.lease_handoff_safety_margin_ms_ = 300;
  EXPECT_TRUE(MetaDataControlServer::ValidateOptions(options).ok());

  options.tls_ca_cert_file_ = "ca.pem";
  EXPECT_EQ(MetaDataControlServer::ValidateOptions(options).code(),
            absl::StatusCode::kInvalidArgument);
  options.tls_cert_file_ = "cert.pem";
  options.tls_key_file_ = "key.pem";
  EXPECT_TRUE(MetaDataControlServer::ValidateOptions(options).ok());

  options.lease_handoff_safety_margin_ms_ = 299;
  EXPECT_EQ(MetaDataControlServer::ValidateOptions(options).code(),
            absl::StatusCode::kInvalidArgument);
  options.lease_handoff_safety_margin_ms_ = 300;
  options.max_pending_handshakes_ = 0;
  EXPECT_EQ(MetaDataControlServer::ValidateOptions(options).code(),
            absl::StatusCode::kInvalidArgument);
  options.max_pending_handshakes_ = control::kMaxProjectedNodes + 1;
  EXPECT_EQ(MetaDataControlServer::ValidateOptions(options).code(),
            absl::StatusCode::kInvalidArgument);
  options.max_pending_handshakes_ = control::kMaxProjectedNodes;
  EXPECT_TRUE(MetaDataControlServer::ValidateOptions(options).ok());

  options.session_progress_timeout_ms_ = 10'001;
  EXPECT_EQ(MetaDataControlServer::ValidateOptions(options).code(),
            absl::StatusCode::kInvalidArgument);
  options.session_progress_timeout_ms_ = 10'000;
  options.max_retained_projection_bytes_ =
      2 * static_cast<std::size_t>(control::kMaxFullDesiredStateBytes) - 1;
  EXPECT_EQ(MetaDataControlServer::ValidateOptions(options).code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaDataControlOptionsTest,
     EstablishedReadTimeoutCoversLongResolvedLeaseCadenceWithoutWrapping) {
  using namespace std::chrono_literals;
  constexpr std::uint32_t lease_duration_ms = 60'000;
  constexpr std::uint32_t observation_ttl_ms = 60'001;
  constexpr std::uint32_t progress_timeout_ms = 10'000;

  const auto timeout =
      EstablishedSessionReadTimeout(observation_ttl_ms, progress_timeout_ms);
  EXPECT_EQ(timeout, 70'001ms);
  EXPECT_GT(timeout, std::chrono::milliseconds(lease_duration_ms / 3));

  const auto maximum =
      EstablishedSessionReadTimeout(std::numeric_limits<std::uint32_t>::max(),
                                    /*session_progress_timeout_ms=*/10'000);
  EXPECT_EQ(
      static_cast<std::uint64_t>(maximum.count()),
      static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) +
          10'000);
}

TEST(MetaDataControlOptionsTest,
     ObservationTtlCoversMaximumDerivedHeartbeatCadence) {
  MetaDataControlServerOptions options;
  options.server_id_ = 1;
  options.bind_host_ = "127.0.0.1";
  options.port_ = 7000;
  options.observation_ttl_ms_ = 100;
  options.leadership_validity_ms_ = 302;
  options.lease_handoff_safety_margin_ms_ = 302;
  EXPECT_TRUE(MetaDataControlServer::ValidateOptions(options).ok());

  options.leadership_validity_ms_ = 303;
  options.lease_handoff_safety_margin_ms_ = 303;
  EXPECT_EQ(MetaDataControlServer::ValidateOptions(options).code(),
            absl::StatusCode::kInvalidArgument);

  options.observation_ttl_ms_ = 1;
  options.leadership_validity_ms_ = 1;
  options.lease_handoff_safety_margin_ms_ = 1;
  EXPECT_TRUE(MetaDataControlServer::ValidateOptions(options).ok());
}

TEST(MetaDataControlDirectoryTest, UsesOnlyCommittedActiveMembersInIdOrder) {
  MetaStores stores;
  keylane::meta::BindMetaMember second;
  second.server_id_ = 2;
  second.principal_ = "keylane://meta/2";
  second.data_control_endpoint_ = "[2001:db8::2]:7200";
  ASSERT_TRUE(stores.identity_.Apply(second).ok());

  keylane::meta::BindMetaMember first;
  first.server_id_ = 1;
  first.principal_ = "keylane://meta/1";
  first.data_control_endpoint_ = "10.0.0.1:7100";
  ASSERT_TRUE(stores.identity_.Apply(first).ok());

  keylane::meta::RetireMetaMember retire;
  retire.server_id_ = 2;
  ASSERT_TRUE(stores.identity_.Apply(retire).ok());

  auto directory = BuildCommittedMetaDirectory(
      MetaCommittedView(std::move(stores), /*applied_index=*/3));
  ASSERT_TRUE(directory.ok()) << directory.status();
  ASSERT_EQ(directory->size(), 1u);
  EXPECT_EQ(directory->front().server_id, 1u);
  EXPECT_EQ(directory->front().host, "10.0.0.1");
  EXPECT_EQ(directory->front().port, 7100u);
  EXPECT_EQ(directory->front().principal, first.principal_);
}

TEST(MetaDataControlDirectoryTest,
     MalformedEndpointIsRejectedBeforeCommitAndPriorStateStillProjects) {
  MetaStores stores;
  keylane::meta::BindMetaMember prior;
  prior.server_id_ = 1;
  prior.principal_ = "keylane://meta/1";
  prior.data_control_endpoint_ = "10.0.0.1:7100";
  ASSERT_TRUE(stores.identity_.Apply(prior).ok());

  keylane::meta::BindMetaMember member;
  member.server_id_ = 2;
  member.principal_ = "keylane://meta/2";
  member.data_control_endpoint_ = "meta.internal:7100";
  EXPECT_FALSE(stores.identity_.Apply(member).ok());

  auto directory = BuildCommittedMetaDirectory(
      MetaCommittedView(std::move(stores), /*applied_index=*/1));
  ASSERT_TRUE(directory.ok()) << directory.status();
  ASSERT_EQ(directory->size(), 1u);
  EXPECT_EQ(directory->front().server_id, prior.server_id_);
}

TEST(MetaDataControlDirectoryTest,
     DuplicateEndpointIsRejectedBeforeCommitAndPriorStateStillProjects) {
  MetaStores stores;
  keylane::meta::BindMetaMember first;
  first.server_id_ = 1;
  first.principal_ = "keylane://meta/1";
  first.data_control_endpoint_ = "10.0.0.1:7100";
  keylane::meta::BindMetaMember middle;
  middle.server_id_ = 2;
  middle.principal_ = "keylane://meta/2";
  middle.data_control_endpoint_ = "10.0.0.2:7100";
  keylane::meta::BindMetaMember last;
  last.server_id_ = 3;
  last.principal_ = "keylane://meta/3";
  last.data_control_endpoint_ = "10.0.0.1:7100";
  ASSERT_TRUE(stores.identity_.Apply(first).ok());
  ASSERT_TRUE(stores.identity_.Apply(middle).ok());
  EXPECT_FALSE(stores.identity_.Apply(last).ok());

  auto directory = BuildCommittedMetaDirectory(
      MetaCommittedView(std::move(stores), /*applied_index=*/2));
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->size(), 2u);
}

TEST(MetaDataControlDirectoryTest,
     CanonicalizesIpv6ForReplayAndCommittedDirectory) {
  MetaStores stores;
  keylane::meta::BindMetaMember bind;
  bind.server_id_ = 2;
  bind.principal_ = "keylane://meta/2";
  bind.data_control_endpoint_ = "[0:0:0:0:0:0:0:1]:7302";
  ASSERT_TRUE(stores.identity_.Apply(bind).ok());

  const auto member = stores.identity_.FindMetaMember(2);
  ASSERT_TRUE(member.has_value());
  EXPECT_EQ(member->data_control_endpoint_, "[::1]:7302");
  EXPECT_TRUE(stores.identity_.Apply(bind).ok());
  bind.data_control_endpoint_ = "[::1]:7302";
  EXPECT_TRUE(stores.identity_.Apply(bind).ok());

  auto directory = BuildCommittedMetaDirectory(
      MetaCommittedView(std::move(stores), /*applied_index=*/1));
  ASSERT_TRUE(directory.ok()) << directory.status();
  ASSERT_EQ(directory->size(), 1u);
  EXPECT_EQ(directory->front().host, "::1");
  EXPECT_EQ(directory->front().port, 7302u);
}

TEST(MetaDataControlDirectoryTest,
     OversizedAggregateIsRejectedBeforeCommitAndPriorStateStillProjects) {
  MetaStores stores;
  std::uint32_t rejected_id = 0;
  for (std::uint32_t id = 1; id <= 1024; ++id) {
    keylane::meta::BindMetaMember bind;
    bind.server_id_ = id;
    bind.principal_ = "keylane://meta/" + std::to_string(id);
    bind.data_control_endpoint_ = "10." + std::to_string((id >> 16) & 0xff) +
                                  "." + std::to_string((id >> 8) & 0xff) + "." +
                                  std::to_string(id & 0xff) + ":7100";
    if (!stores.identity_.Apply(bind).ok()) {
      rejected_id = id;
      break;
    }
  }
  ASSERT_NE(rejected_id, 0u);
  const auto directory = BuildCommittedMetaDirectory(
      MetaCommittedView(std::move(stores),
                        /*applied_index=*/rejected_id - 1));
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->size(), rejected_id - 1);
}

TEST(MetaDataControlLeaseTest, ExactCommittedAnchorGetsBoundedGrant) {
  const control::FullDesiredState desired = Desired();
  MetaLeaseEvaluation evaluation{
      .leader_valid_ = true,
      .server_id_ = 3,
      .raft_term_ = 12,
      .leadership_generation_ = 4,
      .leadership_validity_ms_ = 250,
      .node_id_ = Identity('1'),
      .boot_id_ = Identity('2'),
      .applied_projection_index_ = desired.control_revision,
      .desired_ = &desired,
  };
  control::HeartbeatHealth health{.storage_ready = true,
                                  .population_ready = true};

  const auto decision = EvaluateLeaseChallenge(Challenge(), health, evaluation);
  const auto* grant = std::get_if<control::LeaseGranted>(&decision);
  ASSERT_NE(grant, nullptr);
  EXPECT_EQ(grant->nonce, Challenge().nonce);
  EXPECT_EQ(grant->leader_id, 3u);
  EXPECT_EQ(grant->raft_term, 12u);
  EXPECT_EQ(grant->leadership_generation, 4u);
  EXPECT_EQ(grant->data_boot_id, Identity('2'));
  EXPECT_EQ(grant->granted_duration_ms, 250u);
}

TEST(MetaDataControlLeaseTest,
     LeaderLocalValidityRebuildsTheResolvedProjectionWithoutCommittedInput) {
  control::FullDesiredState state;
  state.control_revision = 7;
  state.authority_lease_duration_ms = 900;
  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  keylane::meta::NodeControlBatch batch{state, *encoded};

  const absl::Status limited = ApplyLeadershipValidityLimit(batch, 250);
  ASSERT_TRUE(limited.ok()) << limited;
  EXPECT_EQ(batch.full_state.authority_lease_duration_ms, 250u);
  EXPECT_EQ(control::DataHeartbeatIntervalMs(
                batch.full_state.authority_lease_duration_ms),
            83u);
  auto decoded = control::DecodeFullDesiredState(batch.encoded_full_state);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->authority_lease_duration_ms, 250u);
  EXPECT_EQ(
      control::DataHeartbeatIntervalMs(decoded->authority_lease_duration_ms),
      83u);
  EXPECT_EQ(decoded->control_revision, batch.full_state.control_revision);
}

TEST(MetaDataControlLeaseTest,
     NewAuthorityWaitsOutPriorLeaseAndEqualSafetyMarginBeforeGrant) {
  const control::FullDesiredState desired = Desired();
  const MetaLeaseEvaluation evaluation{
      .leader_valid_ = true,
      .server_id_ = 3,
      .raft_term_ = 12,
      .leadership_generation_ = 4,
      .leadership_validity_ms_ = 250,
      .node_id_ = Identity('1'),
      .boot_id_ = Identity('2'),
      .applied_projection_index_ = desired.control_revision,
      .desired_ = &desired,
  };
  const control::HeartbeatHealth health{.storage_ready = true,
                                        .population_ready = true};
  MetaLeaseHandoffGuard guard(/*maximum_prior_lease_ms=*/250,
                              /*safety_margin_ms=*/250);

  auto decision =
      guard.Enforce(EvaluateLeaseChallenge(Challenge(), health, evaluation),
                    evaluation.node_id_,
                    /*now_lease_clock_ms=*/10'000);
  const auto* denied = std::get_if<control::LeaseDenied>(&decision);
  ASSERT_NE(denied, nullptr);
  EXPECT_EQ(denied->reason,
            control::LeaseDenialReason::kAuthorityHandoffPending);

  decision =
      guard.Enforce(EvaluateLeaseChallenge(Challenge(), health, evaluation),
                    evaluation.node_id_,
                    /*now_lease_clock_ms=*/10'499);
  EXPECT_NE(std::get_if<control::LeaseDenied>(&decision), nullptr);

  decision =
      guard.Enforce(EvaluateLeaseChallenge(Challenge(), health, evaluation),
                    evaluation.node_id_,
                    /*now_lease_clock_ms=*/10'500);
  EXPECT_NE(std::get_if<control::LeaseGranted>(&decision), nullptr);
}

TEST(MetaDataControlLeaseTest,
     UnhealthyOwnerCannotBypassButAlsoDoesNotOutliveHandoffWait) {
  const control::FullDesiredState desired = Desired();
  const MetaLeaseEvaluation evaluation{
      .leader_valid_ = true,
      .server_id_ = 3,
      .raft_term_ = 12,
      .leadership_generation_ = 4,
      .leadership_validity_ms_ = 250,
      .node_id_ = Identity('1'),
      .boot_id_ = Identity('2'),
      .applied_projection_index_ = desired.control_revision,
      .desired_ = &desired,
  };
  const control::HeartbeatHealth unhealthy{
      .storage_ready = false,
      .population_ready = true,
  };
  MetaLeaseHandoffGuard guard(/*maximum_prior_lease_ms=*/250,
                              /*safety_margin_ms=*/250);
  const auto evaluate = [&] {
    return EvaluateLeaseChallenge(Challenge(), unhealthy, evaluation);
  };

  auto decision = guard.Enforce(evaluate(), Challenge(), evaluation,
                                /*now_lease_clock_ms=*/10'000);
  const auto* denied = std::get_if<control::LeaseDenied>(&decision);
  ASSERT_NE(denied, nullptr);
  EXPECT_EQ(denied->reason,
            control::LeaseDenialReason::kAuthorityHandoffPending);

  decision = guard.Enforce(evaluate(), Challenge(), evaluation,
                           /*now_lease_clock_ms=*/10'499);
  denied = std::get_if<control::LeaseDenied>(&decision);
  ASSERT_NE(denied, nullptr);
  EXPECT_EQ(denied->reason,
            control::LeaseDenialReason::kAuthorityHandoffPending);

  decision = guard.Enforce(evaluate(), Challenge(), evaluation,
                           /*now_lease_clock_ms=*/10'500);
  denied = std::get_if<control::LeaseDenied>(&decision);
  ASSERT_NE(denied, nullptr);
  EXPECT_EQ(denied->reason, control::LeaseDenialReason::kNodeNotReady);
}

TEST(MetaDataControlLeaseTest, NewBootAndLeaderResetRestartHandoffWait) {
  const control::FullDesiredState desired = Desired();
  MetaLeaseEvaluation evaluation{
      .leader_valid_ = true,
      .server_id_ = 3,
      .raft_term_ = 12,
      .leadership_generation_ = 4,
      .leadership_validity_ms_ = 250,
      .node_id_ = Identity('1'),
      .boot_id_ = Identity('2'),
      .applied_projection_index_ = desired.control_revision,
      .desired_ = &desired,
  };
  const control::HeartbeatHealth health{.storage_ready = true,
                                        .population_ready = true};
  MetaLeaseHandoffGuard guard(/*maximum_prior_lease_ms=*/250,
                              /*safety_margin_ms=*/250);
  auto candidate = [&] {
    return EvaluateLeaseChallenge(Challenge(), health, evaluation);
  };

  (void)guard.Enforce(candidate(), evaluation.node_id_, 10'000);
  EXPECT_TRUE(std::holds_alternative<control::LeaseGranted>(
      guard.Enforce(candidate(), evaluation.node_id_, 10'500)));

  evaluation.boot_id_ = Identity('3');
  EXPECT_TRUE(std::holds_alternative<control::LeaseDenied>(
      guard.Enforce(candidate(), evaluation.node_id_, 10'501)));
  EXPECT_TRUE(std::holds_alternative<control::LeaseGranted>(
      guard.Enforce(candidate(), evaluation.node_id_, 11'001)));

  ++evaluation.leadership_generation_;
  EXPECT_TRUE(std::holds_alternative<control::LeaseDenied>(
      guard.Enforce(candidate(), evaluation.node_id_, 11'002)));
  EXPECT_TRUE(std::holds_alternative<control::LeaseGranted>(
      guard.Enforce(candidate(), evaluation.node_id_, 11'502)));

  guard.Reset();
  EXPECT_TRUE(std::holds_alternative<control::LeaseDenied>(
      guard.Enforce(candidate(), evaluation.node_id_, 11'503)));
  EXPECT_TRUE(std::holds_alternative<control::LeaseGranted>(
      guard.Enforce(candidate(), evaluation.node_id_, 12'003)));
}

TEST(MetaDataControlLeaseTest,
     MetaLeaderSuspendRequiresAFullActiveLivenessWindow) {
  MetaLeaderRuntimeGuard guard(/*leadership_validity_ms=*/250);
  guard.Reset(/*now_suspend_clock_ms=*/10'000,
              /*now_active_clock_ms=*/20'000);

  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/10'249,
                          /*now_active_clock_ms=*/20'000),
            MetaLeaderRuntimeDisposition::kEligible);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/10'250,
                          /*now_active_clock_ms=*/20'000),
            MetaLeaderRuntimeDisposition::kQuarantineStarted);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/10'250,
                          /*now_active_clock_ms=*/20'249),
            MetaLeaderRuntimeDisposition::kQuarantined);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/10'250,
                          /*now_active_clock_ms=*/20'250),
            MetaLeaderRuntimeDisposition::kEligible);

  // Equal progress after the proven active cut is ordinary runtime, not a
  // second suspension event.
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/10'500,
                          /*now_active_clock_ms=*/20'500),
            MetaLeaderRuntimeDisposition::kEligible);
}

TEST(MetaDataControlLeaseTest,
     MetaLeaderSuspendAccumulatesAndExtendsAnActiveQuarantine) {
  MetaLeaderRuntimeGuard guard(/*leadership_validity_ms=*/250);
  guard.Reset(/*now_suspend_clock_ms=*/1'000,
              /*now_active_clock_ms=*/2'000);

  // Individually smaller pauses remain accumulated until an active liveness
  // window proves this leadership generation again.
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/1'200,
                          /*now_active_clock_ms=*/2'050),
            MetaLeaderRuntimeDisposition::kEligible);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/1'320,
                          /*now_active_clock_ms=*/2'070),
            MetaLeaderRuntimeDisposition::kQuarantineStarted);

  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/1'520,
                          /*now_active_clock_ms=*/2'120),
            MetaLeaderRuntimeDisposition::kQuarantined);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/1'650,
                          /*now_active_clock_ms=*/2'130),
            MetaLeaderRuntimeDisposition::kQuarantineStarted);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/1'650,
                          /*now_active_clock_ms=*/2'379),
            MetaLeaderRuntimeDisposition::kQuarantined);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/1'650,
                          /*now_active_clock_ms=*/2'380),
            MetaLeaderRuntimeDisposition::kEligible);
}

TEST(MetaDataControlLeaseTest, MetaLeaderClockRegressionFailsClosed) {
  MetaLeaderRuntimeGuard guard(/*leadership_validity_ms=*/250);
  guard.Reset(/*now_suspend_clock_ms=*/1'000,
              /*now_active_clock_ms=*/2'000);
  EXPECT_EQ(guard.Observe(/*now_suspend_clock_ms=*/999,
                          /*now_active_clock_ms=*/2'001),
            MetaLeaderRuntimeDisposition::kQuarantineStarted);
}

TEST(MetaDataControlLeaseTest, StaleProjectionIsOutOfDate) {
  const control::FullDesiredState desired = Desired();
  MetaLeaseEvaluation evaluation{
      .leader_valid_ = true,
      .leadership_validity_ms_ = 250,
      .node_id_ = Identity('1'),
      .boot_id_ = Identity('2'),
      .applied_projection_index_ = 0x99,
      .desired_ = &desired,
  };
  const auto decision = EvaluateLeaseChallenge(
      Challenge(),
      control::HeartbeatHealth{.storage_ready = true, .population_ready = true},
      evaluation);
  const auto* stale = std::get_if<control::LeaseStateOutOfDate>(&decision);
  ASSERT_NE(stale, nullptr);
  EXPECT_EQ(stale->current_control_revision, desired.control_revision);
}

TEST(MetaDataControlLeaseTest, InvalidChallengeDoesNotBecomeAGrant) {
  const control::FullDesiredState desired = Desired();
  MetaLeaseEvaluation evaluation{
      .leader_valid_ = true,
      .leadership_validity_ms_ = 250,
      .node_id_ = Identity('1'),
      .boot_id_ = Identity('2'),
      .applied_projection_index_ = desired.control_revision,
      .desired_ = &desired,
  };
  control::LeaseChallenge challenge = Challenge();
  ++challenge.group_term;
  const auto mismatch = EvaluateLeaseChallenge(
      challenge,
      control::HeartbeatHealth{.storage_ready = true, .population_ready = true},
      evaluation);
  const auto* denied = std::get_if<control::LeaseDenied>(&mismatch);
  ASSERT_NE(denied, nullptr);
  EXPECT_EQ(denied->reason, control::LeaseDenialReason::kAuthorityMismatch);

  const auto unhealthy = EvaluateLeaseChallenge(
      Challenge(), control::HeartbeatHealth{}, evaluation);
  denied = std::get_if<control::LeaseDenied>(&unhealthy);
  ASSERT_NE(denied, nullptr);
  EXPECT_EQ(denied->reason, control::LeaseDenialReason::kNodeNotReady);
}

TEST(MetaDataControlLeaseTest, MissingChallengeHasExplicitDecision) {
  const auto decision = EvaluateLeaseChallenge(
      std::nullopt, control::HeartbeatHealth{}, MetaLeaseEvaluation{});
  EXPECT_TRUE(std::holds_alternative<control::NoChallenge>(decision));
}

TEST(MetaDataControlFenceTest,
     ChunkBoundaryCheckSelectsEachSupersededAnchorOnce) {
  const std::string node_id = Identity('1');
  const control::FullDesiredState installed = Desired();
  control::FullDesiredState unchanged = installed;
  EXPECT_TRUE(UnfencedSupersededAuthorities(installed, &unchanged, node_id, {})
                  .empty());

  control::FullDesiredState revoked = installed;
  revoked.groups.front().grant_active = false;
  const auto selected =
      UnfencedSupersededAuthorities(installed, &revoked, node_id, {});
  ASSERT_EQ(selected.size(), 1u);
  EXPECT_EQ(selected.front().group_id, "group-a");
  EXPECT_EQ(selected.front().assignment_id, Bytes<16>(0x22));
  EXPECT_TRUE(
      UnfencedSupersededAuthorities(installed, &revoked, node_id, selected)
          .empty());

  // A projection failure is fail-closed: no latest state is treated as every
  // installed local authority having disappeared.
  EXPECT_EQ(
      UnfencedSupersededAuthorities(installed, nullptr, node_id, {}).size(),
      1u);
}

TEST(MetaDataControlFenceTest,
     SupersededInitialOrIntermediateProjectionMustAbortBeforeDirectives) {
  const std::string node_id = Identity('1');
  const control::FullDesiredState installed = Desired();

  control::FullDesiredState intermediate = installed;
  ++intermediate.control_revision;
  intermediate.groups.front().owner_assignment_id = Bytes<16>(0x31);
  intermediate.current_directives.push_back(control::WireProjectedDirective{
      .identity = {.operation_id = Bytes<16>(0x40),
                   .directive_id = Bytes<16>(0x41),
                   .attempt_id = Bytes<16>(0x42),
                   .directive_revision = 1},
      .recipient_node_id = node_id,
      .recipient_boot_id = Identity('2'),
      .target_node_id = node_id,
      .target_boot_id = Identity('2'),
      .kind = control::WireDirectiveKind::kRebuild,

  });

  control::FullDesiredState latest = installed;
  latest.control_revision += 2;
  latest.groups.front().grant_active = false;

  EXPECT_EQ(EvaluateReplacementDisposition(intermediate, latest),
            MetaReplacementDisposition::kAbortSuperseded);
  EXPECT_EQ(EvaluateReplacementDisposition(installed, latest),
            MetaReplacementDisposition::kAbortSuperseded);
  const auto fences =
      UnfencedSupersededAuthorities(installed, &latest, node_id, {});
  ASSERT_EQ(fences.size(), 1u);
  EXPECT_EQ(fences.front().assignment_id, Bytes<16>(0x22));

  latest = intermediate;
  ++latest.control_revision;
  EXPECT_EQ(EvaluateReplacementDisposition(intermediate, latest),
            MetaReplacementDisposition::kContinue);
}

TEST(MetaHeartbeatObservationTest,
     DerivesCandidateActionBasisOnlyForExactInstalledBoot) {
  control::FullDesiredState installed;
  control::WireDesiredGroup group;
  group.group_id = "group-a";
  group.group_term = 8;
  group.failover_transition = control::WireFailoverTransition{
      .transition_id = Bytes<16>(0x31),
      .revision = 17,
      .mode = control::WireFailoverMode::kUncontrolled,
      .target_term = 8,
      .candidate_action =
          control::WireFailoverCandidateAction{
              .action_id = Bytes<16>(0x32),
              .candidate = {.node_id = Identity('1'),
                            .assignment_id = Bytes<16>(0x33),
                            .boot_id = Identity('2')},
          },
  };
  installed.groups.push_back(std::move(group));

  const auto exact =
      FailoverProjectionForHeartbeat(installed, Identity('1'), Bytes<20>(0x22));
  ASSERT_TRUE(exact.ok()) << exact.status();
  ASSERT_TRUE(exact->has_value());
  EXPECT_EQ((*exact)->group_id_, "group-a");
  EXPECT_EQ((*exact)->group_term_, 8u);
  EXPECT_EQ((*exact)->transition_id_, Bytes<16>(0x31));
  EXPECT_EQ((*exact)->transition_revision_, 17u);
  EXPECT_EQ((*exact)->action_id_, Bytes<16>(0x32));
  EXPECT_EQ((*exact)->candidate_assignment_id_, Bytes<16>(0x33));
  EXPECT_EQ((*exact)->candidate_boot_id_, Bytes<20>(0x22));

  const auto old_boot =
      FailoverProjectionForHeartbeat(installed, Identity('1'), Bytes<20>(0x23));
  ASSERT_TRUE(old_boot.ok()) << old_boot.status();
  EXPECT_FALSE(old_boot->has_value());
}

TEST(MetaHeartbeatObservationTest,
     HigherSequenceConfirmsOnlyTheExactPriorGrantedOwnerLease) {
  MetaObservedOwnerProjection owner{
      .group_id_ = "group-a",
      .owner_node_id_ = Identity('1'),
      .owner_assignment_id_ = Bytes<16>(0x22),
      .group_term_ = 7,
      .control_revision_ = 0x42,
      .authority_lease_duration_ms_ = 5000,
  };
  control::LeaseGranted granted{
      .data_boot_id = Identity('2'),
      .control_revision = owner.control_revision_,
      .group_id = owner.group_id_,
      .assignment_id = owner.owner_assignment_id_,
      .group_term = owner.group_term_,
      .granted_duration_ms = 5000,
  };
  control::HeartbeatAck ack{
      .heartbeat_sequence = 4,
      .lease_decision = granted,
  };

  const auto exact = ConfirmedLeaseForHeartbeat(ack, /*heartbeat_sequence=*/5,
                                                Identity('2'), owner);
  ASSERT_TRUE(exact.has_value());
  EXPECT_EQ(*exact, 4u);

  EXPECT_FALSE(ConfirmedLeaseForHeartbeat(ack, /*heartbeat_sequence=*/4,
                                          Identity('2'), owner)
                   .has_value());
  EXPECT_FALSE(ConfirmedLeaseForHeartbeat(ack, /*heartbeat_sequence=*/5,
                                          Identity('3'), owner)
                   .has_value());

  ++granted.assignment_id[0];
  ack.lease_decision = granted;
  EXPECT_FALSE(ConfirmedLeaseForHeartbeat(ack, /*heartbeat_sequence=*/5,
                                          Identity('2'), owner)
                   .has_value());

  granted.assignment_id = owner.owner_assignment_id_;
  granted.granted_duration_ms = 4999;
  ack.lease_decision = granted;
  EXPECT_FALSE(ConfirmedLeaseForHeartbeat(ack, /*heartbeat_sequence=*/5,
                                          Identity('2'), owner)
                   .has_value());
  ack.lease_decision = control::LeaseDenied{};
  EXPECT_FALSE(ConfirmedLeaseForHeartbeat(ack, /*heartbeat_sequence=*/5,
                                          Identity('2'), owner)
                   .has_value());
}

TEST(MetaHeartbeatObservationTest,
     RejectedDiagnosticSummaryDoesNotSuppressTypedOwnerHealth) {
  MetaObservationStore observations;
  HeartbeatFacts facts;
  const auto boot = Bytes<20>(0x22);
  ASSERT_TRUE(observations
                  .AdoptSession({Identity('1'), boot, 1},
                                /*now_unix_ms=*/1000)
                  .ok());
  auto owner = keylane::meta::detail::OwnerProjectionForHeartbeat(
      Desired(), Identity('1'));
  ASSERT_TRUE(owner.ok()) << owner.status();
  ASSERT_TRUE(owner->has_value());
  const control::HeartbeatHealth health{
      .storage_ready = true,
      .population_ready = true,
      .draining = false,
      .active_groups = 1,
      .summary = std::string(control::kMaxOpaqueFieldBytes, 's'),
  };

  const auto result = IngestHeartbeatObservations(
      observations, facts, Identity('1'), boot, Bytes<20>(0x55), 1, health,
      control::NoRoleInformation{}, std::nullopt, std::nullopt,
      std::move(*owner), /*heartbeat_sequence=*/1, std::nullopt,
      /*now_unix_ms=*/1010, /*now_steady_ms=*/2010);
  // Diagnostic text can exceed the per-node cache budget; typed health is
  // still authoritative for this heartbeat and must survive that rejection.
  EXPECT_EQ(result.status, control::ObservationStatus::kRejected)
      << result.detail;

  const auto observed = observations.OwnerObservationFor(Identity('1'));
  ASSERT_TRUE(observed.has_value());
  ASSERT_TRUE(observed->health_.has_value());
  EXPECT_EQ(observed->heartbeat_sequence_, 1u);
  EXPECT_TRUE(observed->health_->storage_ready_);
  EXPECT_TRUE(observed->health_->population_ready_);
  EXPECT_FALSE(observed->health_->draining_);
}

TEST(MetaHeartbeatObservationTest,
     ReporterHistoryIsIndependentAndRoleReplacementClearsCandidate) {
  MetaObservationStore observations;
  HeartbeatFacts facts;
  const auto boot = Bytes<20>(0x22);
  ASSERT_TRUE(observations
                  .AdoptSession({Identity('1'), boot, 1},
                                /*now_unix_ms=*/1000)
                  .ok());
  control::CandidateProgress candidate{
      .group_id = "group-a",
      .assignment_id = Bytes<16>(0x22),
      .group_term = 7,
      .source_group_term = 7,
      .manifest_revision = 9,
      .manifest_digest = {},
      .partition_replication_epoch = 4,
      .source_node_id = Identity('3'),
      .source_assignment_id = Bytes<16>(0x33),
      .source_boot_id = Identity('4'),
      .source_history_id = Identity('5'),
      .applied_next_lsns = {10},
  };
  const control::HeartbeatHealth health{
      .storage_ready = true,
      .population_ready = true,
      .active_groups = 1,
      .summary = "ok",
  };
  const keylane::meta::MetaObservedFailoverProjection projection_basis{
      .group_id_ = "group-a",
      .group_term_ = 7,
      .transition_id_ = Bytes<16>(0x71),
      .transition_revision_ = 12,
      .action_id_ = Bytes<16>(0x72),
      .candidate_node_id_ = Identity('1'),
      .candidate_assignment_id_ = Bytes<16>(0x22),
      .candidate_boot_id_ = boot,
  };

  const auto first = IngestHeartbeatObservations(
      observations, facts, Identity('1'), boot, Bytes<20>(0x66), 1, health,
      control::ReplicaCandidate{candidate}, std::nullopt, projection_basis,
      /*now_unix_ms=*/1001);
  EXPECT_EQ(first.status, control::ObservationStatus::kAccepted);
  EXPECT_EQ(observations.size(), 3u);
  const auto session = observations.SessionStateFor(Identity('1'));
  ASSERT_TRUE(session.has_value());
  EXPECT_EQ(session->heartbeat_failover_projection_, projection_basis);
  const auto latest = observations.LatestForNode(Identity('1'), facts);
  ASSERT_TRUE(latest.has_value());
  ASSERT_TRUE(std::holds_alternative<keylane::meta::MetaCandidateProgressObs>(
      latest->payload_));

  candidate.partition_replication_epoch = 3;
  const auto stale_population = IngestHeartbeatObservations(
      observations, facts, Identity('1'), boot, Bytes<20>(0x55), 1, health,
      control::ReplicaCandidate{candidate},
      /*now_unix_ms=*/1002);
  EXPECT_EQ(stale_population.status, control::ObservationStatus::kRejected);
  EXPECT_EQ(stale_population.detail,
            "candidate: partition-epoch-mismatch:committed=4");
  EXPECT_TRUE(observations.CandidateProgressFor("group-a", facts).empty());

  candidate.partition_replication_epoch = 4;
  candidate.assignment_id = Bytes<16>(0x23);
  const auto stale_assignment = IngestHeartbeatObservations(
      observations, facts, Identity('1'), boot, Bytes<20>(0x55), 1, health,
      control::ReplicaCandidate{candidate},
      /*now_unix_ms=*/1003);
  EXPECT_EQ(stale_assignment.status, control::ObservationStatus::kRejected);
  EXPECT_EQ(stale_assignment.detail, "candidate: assignment-mismatch");
  EXPECT_TRUE(observations.CandidateProgressFor("group-a", facts).empty());

  candidate.assignment_id = Bytes<16>(0x22);
  const auto accepted = IngestHeartbeatObservations(
      observations, facts, Identity('1'), boot, Bytes<20>(0x55), 1, health,
      control::ReplicaCandidate{candidate},
      /*now_unix_ms=*/1004);
  EXPECT_EQ(accepted.status, control::ObservationStatus::kAccepted);
  EXPECT_TRUE(accepted.detail.empty());
  const auto progress = observations.CandidateProgressFor("group-a", facts);
  ASSERT_EQ(progress.size(), 1u);
  EXPECT_EQ(progress.front().node_id_, Identity('1'));
  EXPECT_EQ(progress.front().boot_incarnation_, boot);
  EXPECT_EQ(progress.front().assignment_id_, Bytes<16>(0x22));
  EXPECT_EQ(progress.front().partition_replication_epoch_, 4u);
  EXPECT_EQ(progress.front().replication_history_id_, Bytes<20>(0x55));
  EXPECT_EQ(progress.front().source_group_term_, 7u);
  EXPECT_EQ(progress.front().source_replication_history_id_, Bytes<20>(0x55));
  EXPECT_EQ(progress.front().applied_next_lsns_,
            (std::vector<std::uint64_t>{10}));
  EXPECT_EQ(progress.front().applied_next_lsns_,
            (std::vector<std::uint64_t>{10}));

  const auto authority = IngestHeartbeatObservations(
      observations, facts, Identity('1'), boot, Bytes<20>(0x55), 1, health,
      control::AuthorityLeaseRequest{.challenge = Challenge()},
      /*now_unix_ms=*/1005);
  EXPECT_EQ(authority.status, control::ObservationStatus::kAccepted);
  EXPECT_TRUE(observations.CandidateProgressFor("group-a", facts).empty());
}

TEST(MetaHeartbeatObservationTest,
     BridgesActionObservationWithoutRequiringCandidateRolePayload) {
  MetaObservationStore observations;
  FailoverHeartbeatFacts facts;
  const auto boot = Bytes<20>(0x22);
  ASSERT_TRUE(observations
                  .AdoptSession({Identity('1'), boot, 1},
                                /*now_unix_ms=*/1000)
                  .ok());
  const control::HeartbeatHealth health{
      .storage_ready = true,
      .population_ready = true,
      .active_groups = 1,
      .summary = "ok",
  };
  control::CandidatePrepared prepared{
      .transition_id = Bytes<16>(0x31),
      .action_id = Bytes<16>(0x32),
      .candidate_node_id = Identity('1'),
      .candidate_assignment_id = Bytes<16>(0x22),
      .candidate_boot_id = Identity('2'),
      .prepared_context_id = Bytes<16>(0x41),
  };

  const auto result = IngestHeartbeatObservations(
      observations, facts, Identity('1'), boot, Bytes<20>(0x66), 1, health,
      control::NoRoleInformation{}, control::FailoverObservation{prepared},
      /*now_unix_ms=*/1001);
  EXPECT_EQ(result.status, control::ObservationStatus::kAccepted)
      << result.detail;
  const auto observed = observations.CandidatePreparedFor(
      Bytes<16>(0x31), Bytes<16>(0x32), facts, 1001);
  ASSERT_TRUE(observed.has_value());
  EXPECT_EQ(observed->prepared_context_id_, prepared.prepared_context_id);
}





}  // namespace
