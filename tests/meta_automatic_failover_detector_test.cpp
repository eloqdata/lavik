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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/meta/automatic_failover_detector.h"

namespace lavik::meta {
namespace {

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

MetaAutomaticFailoverStateMachine::Input UnserviceableInput() {
  return {
      .anchor_ =
          {
              .group_id_ = "group-a",
              .leadership_generation_ = 3,
              .owner_node_id_ = std::string(40, '1'),
              .owner_assignment_id_ = Bytes<16>(0x21),
              .group_term_ = 5,
              .automatic_failover_policy_version_ = 13,
              .authority_lease_policy_version_ = 17,
          },
      .leader_authority_eligible_ = true,
      .suspect_after_ms_ = 1'000,
      .owner_serviceability_ =
          {
              .state_ = MetaOwnerServiceabilityState::kUnserviceable,
              .reason_ = MetaOwnerServiceabilityReason::kSessionMissing,
          },
  };
}

MetaAutomaticFailoverStatus Diagnostic(
    std::string group_id, std::uint64_t leadership_generation,
    MetaAutomaticFailoverState state = MetaAutomaticFailoverState::kHealthy,
    std::uint64_t eligibility_revision = 1) {
  MetaAutomaticFailoverStatus status;
  status.anchor_ = UnserviceableInput().anchor_;
  status.anchor_.group_id_ = std::move(group_id);
  status.anchor_.leadership_generation_ = leadership_generation;
  status.anchor_.leader_authority_eligibility_revision_ = eligibility_revision;
  status.state_ = state;
  status.effective_threshold_ms_ = 1'000;
  return status;
}

TEST(MetaAutomaticFailoverDiagnosticsRegistryTest,
     PublishesOneAtomicGroupSortedGenerationCut) {
  MetaAutomaticFailoverDiagnosticsRegistry registry;
  registry.BeginLeadership(7);
  registry.Publish(7, 1, 101,
                   {Diagnostic("group-b", 7), Diagnostic("group-a", 7),
                    Diagnostic("group-c", 7)});

  auto snapshot = registry.Snapshot();
  ASSERT_EQ(snapshot.leadership_generation_, 7u);
  ASSERT_EQ(snapshot.leader_authority_eligibility_revision_, 1u);
  ASSERT_EQ(snapshot.evaluated_applied_index_, 101u);
  ASSERT_EQ(snapshot.statuses_.size(), 3u);
  EXPECT_EQ(snapshot.statuses_[0].anchor_.group_id_, "group-a");
  EXPECT_EQ(snapshot.statuses_[1].anchor_.group_id_, "group-b");
  EXPECT_EQ(snapshot.statuses_[2].anchor_.group_id_, "group-c");

  registry.Publish(
      7, 2, 102,
      {Diagnostic("replacement", 7, MetaAutomaticFailoverState::kHealthy, 2)});
  snapshot = registry.Snapshot();
  EXPECT_EQ(snapshot.leader_authority_eligibility_revision_, 2u);
  EXPECT_EQ(snapshot.evaluated_applied_index_, 102u);
  ASSERT_EQ(snapshot.statuses_.size(), 1u);
  EXPECT_EQ(snapshot.statuses_[0].anchor_.group_id_, "replacement");
}

TEST(MetaAutomaticFailoverDiagnosticsRegistryTest,
     StaleLifecycleCallbacksCannotOverwriteANewerGeneration) {
  MetaAutomaticFailoverDiagnosticsRegistry registry;
  registry.BeginLeadership(10);
  registry.Publish(10, 1, 201, {Diagnostic("old", 10)});

  // A duplicate start is idempotent and cannot erase the current cut.
  registry.BeginLeadership(10);
  ASSERT_EQ(registry.Snapshot().statuses_.size(), 1u);

  registry.BeginLeadership(11);
  auto snapshot = registry.Snapshot();
  EXPECT_EQ(snapshot.leadership_generation_, 11u);
  EXPECT_EQ(snapshot.evaluated_applied_index_, 0u);
  EXPECT_TRUE(snapshot.statuses_.empty());

  registry.Publish(10, 1, 202, {Diagnostic("stale", 10)});
  registry.EndLeadership(10);
  snapshot = registry.Snapshot();
  EXPECT_EQ(snapshot.leadership_generation_, 11u);
  EXPECT_TRUE(snapshot.statuses_.empty());

  registry.Publish(11, 1, 203, {Diagnostic("current", 11)});
  registry.Publish(11, 1, 202, {Diagnostic("regressed-index", 11)});
  registry.Publish(10, 1, 204, {Diagnostic("stale-after-current", 10)});
  registry.Publish(11, 0, 204,
                   {Diagnostic("regressed-revision", 11,
                               MetaAutomaticFailoverState::kHealthy, 0)});
  snapshot = registry.Snapshot();
  ASSERT_EQ(snapshot.leadership_generation_, 11u);
  ASSERT_EQ(snapshot.evaluated_applied_index_, 203u);
  ASSERT_EQ(snapshot.statuses_.size(), 1u);
  EXPECT_EQ(snapshot.statuses_[0].anchor_.group_id_, "current");

  registry.EndLeadership(11);
  snapshot = registry.Snapshot();
  EXPECT_EQ(snapshot.leadership_generation_, 0u);
  EXPECT_EQ(snapshot.evaluated_applied_index_, 0u);
  EXPECT_TRUE(snapshot.statuses_.empty());

  // Late work cannot resurrect an ended bracket, including a repeated Begin.
  registry.Publish(11, 1, 205, {Diagnostic("late", 11)});
  registry.BeginLeadership(11);
  EXPECT_EQ(registry.Snapshot(), snapshot);

  registry.BeginLeadership(12);
  EXPECT_EQ(registry.Snapshot().leadership_generation_, 12u);
}

TEST(MetaAutomaticFailoverDiagnosticsRegistryTest,
     InvalidPublicationFailsClosedWithoutChangingTheLastValidCut) {
  MetaAutomaticFailoverDiagnosticsRegistry registry;
  registry.BeginLeadership(20);
  registry.Publish(20, 1, 301, {Diagnostic("valid", 20)});
  const auto valid = registry.Snapshot();

  const auto expect_unchanged = [&](std::uint64_t generation, auto statuses) {
    registry.Publish(generation, 1, 302, std::move(statuses));
    EXPECT_EQ(registry.Snapshot(), valid);
  };

  expect_unchanged(0, std::vector{Diagnostic("zero-generation", 0)});
  expect_unchanged(21, std::vector{Diagnostic("future-generation", 21)});

  auto mismatched_anchor = Diagnostic("mismatched-anchor", 19);
  expect_unchanged(20, std::vector{mismatched_anchor});
  auto mismatched_revision = Diagnostic("mismatched-revision", 20);
  mismatched_revision.anchor_.leader_authority_eligibility_revision_ = 2;
  expect_unchanged(20, std::vector{mismatched_revision});
  expect_unchanged(20, std::vector{Diagnostic("duplicate", 20),
                                   Diagnostic("duplicate", 20)});
  expect_unchanged(20, std::vector{Diagnostic("", 20)});
  expect_unchanged(20, std::vector{Diagnostic(
                           std::string(kMaxMetaGroupIdBytes + 1, 'g'), 20)});

  auto invalid_state = Diagnostic("bad-state", 20);
  invalid_state.state_ = static_cast<MetaAutomaticFailoverState>(0xff);
  expect_unchanged(20, std::vector{invalid_state});
  auto invalid_reason = Diagnostic("bad-reason", 20);
  invalid_reason.current_reason_ =
      static_cast<MetaOwnerServiceabilityReason>(0xff);
  expect_unchanged(20, std::vector{invalid_reason});
  auto invalid_blocker = Diagnostic("bad-blocker", 20);
  invalid_blocker.blocker_ = static_cast<MetaAutomaticFailoverBlocker>(0xff);
  expect_unchanged(20, std::vector{invalid_blocker});

  expect_unchanged(
      20, std::vector<MetaAutomaticFailoverStatus>(kMaxMetaGroups + 1));

  registry.BeginLeadership(0);
  EXPECT_EQ(registry.Snapshot(), valid);
  registry.EndLeadership(0);
  EXPECT_EQ(registry.Snapshot(), valid);
}

TEST(MetaAutomaticFailoverDiagnosticsRegistryTest,
     ConcurrentPublishAndSnapshotNeverExposeAPartialBatch) {
  MetaAutomaticFailoverDiagnosticsRegistry registry;
  registry.BeginLeadership(30);
  std::atomic<bool> invalid_cut = false;

  std::thread writer([&] {
    for (int i = 0; i < 2'000; ++i) {
      const auto state = (i & 1) == 0 ? MetaAutomaticFailoverState::kHealthy
                                      : MetaAutomaticFailoverState::kBlocked;
      const std::uint64_t evaluated_index = 401 + static_cast<std::uint64_t>(i);
      registry.Publish(
          30, 1, evaluated_index,
          {Diagnostic("group-b", 30, state), Diagnostic("group-a", 30, state)});
    }
  });
  std::thread reader([&] {
    for (int i = 0; i < 2'000; ++i) {
      const auto snapshot = registry.Snapshot();
      const bool initial_empty =
          snapshot.statuses_.empty() &&
          snapshot.leader_authority_eligibility_revision_ == 0 &&
          snapshot.evaluated_applied_index_ == 0;
      const bool published =
          snapshot.leader_authority_eligibility_revision_ == 1 &&
          snapshot.statuses_.size() == 2 &&
          snapshot.statuses_[0].anchor_.group_id_ == "group-a" &&
          snapshot.statuses_[1].anchor_.group_id_ == "group-b" &&
          snapshot.statuses_[0].state_ == snapshot.statuses_[1].state_ &&
          (snapshot.statuses_[0].state_ == MetaAutomaticFailoverState::kHealthy
               ? (snapshot.evaluated_applied_index_ & 1) != 0
               : (snapshot.evaluated_applied_index_ & 1) == 0);
      if (snapshot.leadership_generation_ != 30 ||
          (!initial_empty && !published)) {
        invalid_cut = true;
      }
    }
  });
  writer.join();
  reader.join();

  EXPECT_FALSE(invalid_cut.load());
  EXPECT_EQ(registry.Snapshot().statuses_.size(), 2u);
}

TEST(MetaAutomaticFailoverStateMachineTest,
     ExactFailureTriggersOnceAtTheThreshold) {
  MetaAutomaticFailoverStateMachine machine;
  auto first = machine.Advance(UnserviceableInput(), 10'000);
  ASSERT_TRUE(first.ok()) << first.status();
  EXPECT_EQ(first->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(first->status_.accumulated_suspect_ms_, 0u);
  EXPECT_FALSE(first->trigger_now_);

  auto before = machine.Advance(UnserviceableInput(), 10'999);
  ASSERT_TRUE(before.ok()) << before.status();
  EXPECT_EQ(before->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(before->status_.accumulated_suspect_ms_, 999u);
  EXPECT_FALSE(before->trigger_now_);

  auto threshold = machine.Advance(UnserviceableInput(), 11'000);
  ASSERT_TRUE(threshold.ok()) << threshold.status();
  EXPECT_EQ(threshold->status_.state_, MetaAutomaticFailoverState::kTriggering);
  EXPECT_EQ(threshold->status_.accumulated_suspect_ms_, 1'000u);
  EXPECT_EQ(threshold->status_.current_reason_,
            MetaOwnerServiceabilityReason::kSessionMissing);
  EXPECT_TRUE(threshold->trigger_now_);

  auto later_failure = UnserviceableInput();
  later_failure.owner_serviceability_.reason_ =
      MetaOwnerServiceabilityReason::kDraining;
  auto latched = machine.Advance(later_failure, 12'000);
  ASSERT_TRUE(latched.ok()) << latched.status();
  EXPECT_EQ(latched->status_, threshold->status_);
  EXPECT_FALSE(latched->trigger_now_);

  auto regressed_clock = machine.Advance(later_failure, 9'000);
  ASSERT_TRUE(regressed_clock.ok()) << regressed_clock.status();
  EXPECT_EQ(regressed_clock->status_, threshold->status_);
  EXPECT_FALSE(regressed_clock->trigger_now_);

  auto new_policy = later_failure;
  ++new_policy.anchor_.authority_lease_policy_version_;
  auto reset = machine.Advance(new_policy, 13'000);
  ASSERT_TRUE(reset.ok()) << reset.status();
  EXPECT_EQ(reset->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(reset->status_.accumulated_suspect_ms_, 0u);
  EXPECT_FALSE(reset->trigger_now_);
}

TEST(MetaAutomaticFailoverStateMachineTest,
     ServiceableInputDiscardsSuspectTime) {
  MetaAutomaticFailoverStateMachine machine;
  auto initial = machine.Advance(UnserviceableInput(), 20'000);
  ASSERT_TRUE(initial.ok()) << initial.status();
  EXPECT_EQ(initial->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(initial->status_.accumulated_suspect_ms_, 0u);

  auto partial = machine.Advance(UnserviceableInput(), 20'900);
  ASSERT_TRUE(partial.ok()) << partial.status();
  EXPECT_EQ(partial->status_.accumulated_suspect_ms_, 900u);

  auto serviceable = UnserviceableInput();
  serviceable.owner_serviceability_ = {
      .state_ = MetaOwnerServiceabilityState::kServiceable};
  auto healthy = machine.Advance(serviceable, 20'950);
  ASSERT_TRUE(healthy.ok()) << healthy.status();
  EXPECT_EQ(healthy->status_.state_, MetaAutomaticFailoverState::kHealthy);
  EXPECT_EQ(healthy->status_.accumulated_suspect_ms_, 0u);
  EXPECT_EQ(healthy->status_.current_reason_,
            MetaOwnerServiceabilityReason::kNone);

  auto restarted = machine.Advance(UnserviceableInput(), 21'500);
  ASSERT_TRUE(restarted.ok()) << restarted.status();
  EXPECT_EQ(restarted->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(restarted->status_.accumulated_suspect_ms_, 0u);
}

TEST(MetaAutomaticFailoverStateMachineTest,
     IndeterminateFreezesAndReasonChangesPreserveElapsedTime) {
  MetaAutomaticFailoverStateMachine machine;
  ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'000).ok());

  auto draining = UnserviceableInput();
  draining.owner_serviceability_.reason_ =
      MetaOwnerServiceabilityReason::kDraining;
  auto changed_reason = machine.Advance(draining, 10'400);
  ASSERT_TRUE(changed_reason.ok()) << changed_reason.status();
  EXPECT_EQ(changed_reason->status_.state_,
            MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(changed_reason->status_.current_reason_,
            MetaOwnerServiceabilityReason::kDraining);
  EXPECT_EQ(changed_reason->status_.accumulated_suspect_ms_, 400u);

  auto indeterminate = UnserviceableInput();
  indeterminate.owner_serviceability_ = {
      .state_ = MetaOwnerServiceabilityState::kIndeterminate,
      .reason_ = MetaOwnerServiceabilityReason::kStaleOwnerAnchor,
  };
  auto frozen = machine.Advance(indeterminate, 10'500);
  ASSERT_TRUE(frozen.ok()) << frozen.status();
  EXPECT_EQ(frozen->status_.state_, MetaAutomaticFailoverState::kBlocked);
  EXPECT_EQ(frozen->status_.blocker_,
            MetaAutomaticFailoverBlocker::kStaleOwnerAnchor);
  EXPECT_EQ(frozen->status_.current_reason_,
            MetaOwnerServiceabilityReason::kNone);
  EXPECT_EQ(frozen->status_.accumulated_suspect_ms_, 500u);
  EXPECT_FALSE(frozen->trigger_now_);

  auto still_frozen = machine.Advance(indeterminate, 20'000);
  ASSERT_TRUE(still_frozen.ok()) << still_frozen.status();
  EXPECT_EQ(still_frozen->status_.accumulated_suspect_ms_, 500u);

  auto population = UnserviceableInput();
  population.owner_serviceability_.reason_ =
      MetaOwnerServiceabilityReason::kPopulationUnready;
  auto resumed = machine.Advance(population, 21'000);
  ASSERT_TRUE(resumed.ok()) << resumed.status();
  EXPECT_EQ(resumed->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(resumed->status_.current_reason_,
            MetaOwnerServiceabilityReason::kPopulationUnready);
  EXPECT_EQ(resumed->status_.accumulated_suspect_ms_, 500u);

  auto threshold = machine.Advance(population, 21'500);
  ASSERT_TRUE(threshold.ok()) << threshold.status();
  EXPECT_EQ(threshold->status_.state_, MetaAutomaticFailoverState::kTriggering);
  EXPECT_EQ(threshold->status_.accumulated_suspect_ms_, 1'000u);
  EXPECT_TRUE(threshold->trigger_now_);
}

TEST(MetaAutomaticFailoverStateMachineTest,
     AnyAuthorityLeadershipOrPolicyAnchorChangeResetsElapsedTime) {
  const auto expect_reset = [](auto mutate) {
    MetaAutomaticFailoverStateMachine machine;
    ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'000).ok());
    ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'900).ok());
    auto changed = UnserviceableInput();
    mutate(changed.anchor_);
    auto update = machine.Advance(changed, 10'950);
    ASSERT_TRUE(update.ok()) << update.status();
    EXPECT_EQ(update->status_.state_, MetaAutomaticFailoverState::kSuspect);
    EXPECT_EQ(update->status_.accumulated_suspect_ms_, 0u);
    EXPECT_FALSE(update->trigger_now_);
  };

  expect_reset([](auto& anchor) { anchor.owner_node_id_[0] = '2'; });
  expect_reset(
      [](auto& anchor) { anchor.owner_assignment_id_ = Bytes<16>(0x22); });
  expect_reset([](auto& anchor) { ++anchor.group_term_; });
  expect_reset([](auto& anchor) { ++anchor.leadership_generation_; });
  expect_reset(
      [](auto& anchor) { ++anchor.automatic_failover_policy_version_; });
  expect_reset([](auto& anchor) { ++anchor.authority_lease_policy_version_; });
}

TEST(MetaAutomaticFailoverStateMachineTest,
     LeadershipEligibilityLossAndRegainBothResetElapsedTime) {
  MetaAutomaticFailoverStateMachine machine;
  ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'000).ok());
  ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'900).ok());

  auto ineligible = UnserviceableInput();
  ineligible.leader_authority_eligible_ = false;
  auto lost = machine.Advance(ineligible, 10'950);
  ASSERT_TRUE(lost.ok()) << lost.status();
  EXPECT_EQ(lost->status_.state_, MetaAutomaticFailoverState::kBlocked);
  EXPECT_EQ(lost->status_.blocker_,
            MetaAutomaticFailoverBlocker::kLeaderIneligible);
  EXPECT_EQ(lost->status_.accumulated_suspect_ms_, 0u);

  auto regained = machine.Advance(UnserviceableInput(), 20'000);
  ASSERT_TRUE(regained.ok()) << regained.status();
  EXPECT_EQ(regained->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(regained->status_.accumulated_suspect_ms_, 0u);
}

TEST(MetaAutomaticFailoverStateMachineTest,
     BlockedInputCanFreezePastThresholdButOnlyFailureTriggers) {
  MetaAutomaticFailoverStateMachine machine;
  ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'000).ok());

  auto blocked = UnserviceableInput();
  blocked.owner_serviceability_ = {
      .state_ = MetaOwnerServiceabilityState::kBlocked,
      .blocker_ = MetaOwnerServiceabilityBlocker::kLeadershipWarmup,
  };
  auto frozen = machine.Advance(blocked, 11'500);
  ASSERT_TRUE(frozen.ok()) << frozen.status();
  EXPECT_EQ(frozen->status_.state_, MetaAutomaticFailoverState::kBlocked);
  EXPECT_EQ(frozen->status_.blocker_,
            MetaAutomaticFailoverBlocker::kLeadershipWarmup);
  EXPECT_EQ(frozen->status_.accumulated_suspect_ms_, 1'500u);
  EXPECT_FALSE(frozen->trigger_now_);

  auto still_blocked = machine.Advance(blocked, 20'000);
  ASSERT_TRUE(still_blocked.ok()) << still_blocked.status();
  EXPECT_EQ(still_blocked->status_.accumulated_suspect_ms_, 1'500u);
  EXPECT_FALSE(still_blocked->trigger_now_);

  auto exact = machine.Advance(UnserviceableInput(), 21'000);
  ASSERT_TRUE(exact.ok()) << exact.status();
  EXPECT_EQ(exact->status_.state_, MetaAutomaticFailoverState::kTriggering);
  EXPECT_EQ(exact->status_.accumulated_suspect_ms_, 1'500u);
  EXPECT_TRUE(exact->trigger_now_);
}

TEST(MetaAutomaticFailoverStateMachineTest,
     MonotonicClockRegressionDiscardsElapsedTime) {
  MetaAutomaticFailoverStateMachine machine;
  ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'000).ok());
  auto partial = machine.Advance(UnserviceableInput(), 10'900);
  ASSERT_TRUE(partial.ok()) << partial.status();
  EXPECT_EQ(partial->status_.accumulated_suspect_ms_, 900u);

  auto regressed = machine.Advance(UnserviceableInput(), 9'000);
  ASSERT_TRUE(regressed.ok()) << regressed.status();
  EXPECT_EQ(regressed->status_.state_, MetaAutomaticFailoverState::kSuspect);
  EXPECT_EQ(regressed->status_.accumulated_suspect_ms_, 0u);
  EXPECT_FALSE(regressed->trigger_now_);
}

TEST(MetaAutomaticFailoverStateMachineTest,
     GroupsAdvanceIndependentlyAndStorageIsBounded) {
  MetaAutomaticFailoverStateMachine machine(/*max_groups=*/2);
  ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'000).ok());
  ASSERT_TRUE(machine.Advance(UnserviceableInput(), 10'900).ok());

  auto group_b = UnserviceableInput();
  group_b.anchor_.group_id_ = "group-b";
  auto first_b = machine.Advance(group_b, 50'000);
  ASSERT_TRUE(first_b.ok()) << first_b.status();
  EXPECT_EQ(first_b->status_.accumulated_suspect_ms_, 0u);

  auto group_a_threshold = machine.Advance(UnserviceableInput(), 11'000);
  ASSERT_TRUE(group_a_threshold.ok()) << group_a_threshold.status();
  EXPECT_TRUE(group_a_threshold->trigger_now_);

  const auto snapshot = machine.Snapshot();
  ASSERT_EQ(snapshot.size(), 2u);
  EXPECT_EQ(snapshot[0].anchor_.group_id_, "group-a");
  EXPECT_EQ(snapshot[0].state_, MetaAutomaticFailoverState::kTriggering);
  EXPECT_EQ(snapshot[1].anchor_.group_id_, "group-b");
  EXPECT_EQ(snapshot[1].state_, MetaAutomaticFailoverState::kSuspect);

  auto group_c = UnserviceableInput();
  group_c.anchor_.group_id_ = "group-c";
  auto at_capacity = machine.Advance(group_c, 60'000);
  ASSERT_FALSE(at_capacity.ok());
  EXPECT_EQ(at_capacity.status().code(), absl::StatusCode::kResourceExhausted);

  machine.EraseGroup("group-a");
  EXPECT_EQ(machine.Snapshot().size(), 1u);
  EXPECT_TRUE(machine.Advance(group_c, 60'000).ok());
  machine.Clear();
  EXPECT_EQ(machine.Snapshot().size(), 0u);
  EXPECT_TRUE(machine.Snapshot().empty());
}

TEST(MetaAutomaticFailoverStateMachineTest,
     InvalidPolicyCutCannotCreateOrTriggerAGroup) {
  MetaAutomaticFailoverStateMachine machine;
  auto input = UnserviceableInput();
  input.suspect_after_ms_ = 0;
  auto invalid = machine.Advance(input, 10'000);
  ASSERT_FALSE(invalid.ok());
  EXPECT_EQ(invalid.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(machine.Snapshot().size(), 0u);
}

TEST(MetaAutomaticFailoverStateMachineTest, DiagnosticCodesAreStable) {
  EXPECT_EQ(
      MetaAutomaticFailoverStateName(MetaAutomaticFailoverState::kHealthy),
      "healthy");
  EXPECT_EQ(
      MetaAutomaticFailoverStateName(MetaAutomaticFailoverState::kSuspect),
      "suspect");
  EXPECT_EQ(
      MetaAutomaticFailoverStateName(MetaAutomaticFailoverState::kBlocked),
      "blocked");
  EXPECT_EQ(
      MetaAutomaticFailoverStateName(MetaAutomaticFailoverState::kTriggering),
      "triggering");

  EXPECT_EQ(
      MetaAutomaticFailoverBlockerName(MetaAutomaticFailoverBlocker::kNone),
      "none");
  EXPECT_EQ(MetaAutomaticFailoverBlockerName(
                MetaAutomaticFailoverBlocker::kLeaderIneligible),
            "leader_ineligible");
  EXPECT_EQ(MetaAutomaticFailoverBlockerName(
                MetaAutomaticFailoverBlocker::kLeadershipWarmup),
            "leadership_warmup");
  EXPECT_EQ(MetaAutomaticFailoverBlockerName(
                MetaAutomaticFailoverBlocker::kAuthorityHandoff),
            "authority_handoff");
  EXPECT_EQ(MetaAutomaticFailoverBlockerName(
                MetaAutomaticFailoverBlocker::kFailoverTransition),
            "failover_transition");
  EXPECT_EQ(MetaAutomaticFailoverBlockerName(
                MetaAutomaticFailoverBlocker::kStaleOwnerAnchor),
            "stale_owner_anchor");
  EXPECT_EQ(MetaAutomaticFailoverBlockerName(
                MetaAutomaticFailoverBlocker::kCausalLeasePending),
            "causal_lease_pending");
  EXPECT_EQ(MetaAutomaticFailoverBlockerName(
                MetaAutomaticFailoverBlocker::kIndeterminateEvidence),
            "indeterminate_evidence");
}

TEST(MetaAutomaticFailoverStateMachineTest,
     OwnerServiceabilityBlockersRemainStructured) {
  const std::array blocker_cases{
      std::pair{MetaOwnerServiceabilityBlocker::kLeaderIneligible,
                MetaAutomaticFailoverBlocker::kLeaderIneligible},
      std::pair{MetaOwnerServiceabilityBlocker::kLeadershipWarmup,
                MetaAutomaticFailoverBlocker::kLeadershipWarmup},
      std::pair{MetaOwnerServiceabilityBlocker::kAuthorityHandoff,
                MetaAutomaticFailoverBlocker::kAuthorityHandoff},
      std::pair{MetaOwnerServiceabilityBlocker::kFailoverTransition,
                MetaAutomaticFailoverBlocker::kFailoverTransition},
  };
  for (const auto& [serviceability_blocker, detector_blocker] : blocker_cases) {
    SCOPED_TRACE(static_cast<int>(serviceability_blocker));
    MetaAutomaticFailoverStateMachine machine;
    auto input = UnserviceableInput();
    input.owner_serviceability_ = {
        .state_ = MetaOwnerServiceabilityState::kBlocked,
        .blocker_ = serviceability_blocker,
    };
    auto update = machine.Advance(input, 10'000);
    ASSERT_TRUE(update.ok()) << update.status();
    EXPECT_EQ(update->status_.state_, MetaAutomaticFailoverState::kBlocked);
    EXPECT_EQ(update->status_.blocker_, detector_blocker);
    EXPECT_FALSE(update->trigger_now_);
  }

  MetaAutomaticFailoverStateMachine machine;
  auto causal = UnserviceableInput();
  causal.owner_serviceability_ = {
      .state_ = MetaOwnerServiceabilityState::kIndeterminate,
      .reason_ = MetaOwnerServiceabilityReason::kCausalLeasePending,
  };
  auto update = machine.Advance(causal, 10'000);
  ASSERT_TRUE(update.ok()) << update.status();
  EXPECT_EQ(update->status_.blocker_,
            MetaAutomaticFailoverBlocker::kCausalLeasePending);
}

}  // namespace
}  // namespace lavik::meta
