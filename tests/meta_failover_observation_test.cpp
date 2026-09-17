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
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "gtest/gtest.h"
#include "lavik/meta/observation_store.h"

namespace {

namespace meta = lavik::meta;

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

std::string Node(char value) { return std::string(40, value); }

class FailoverFacts final : public meta::MetaCommittedFacts {
 public:
  bool IsActiveNode(std::string_view node_id) const override {
    return active_.contains(std::string(node_id));
  }
  std::uint64_t CurrentGroupTerm(std::string_view group_id) const override {
    return group_id == "g" ? 7 : 0;
  }
  std::uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const override {
    return group_id == "g" ? 9 : 0;
  }
  meta::MetaHash256 CurrentPopulationManifestDigest(
      std::string_view group_id) const override {
    return group_id == "g" ? Bytes<32>(0x39) : meta::MetaHash256{};
  }
  std::uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const override {
    return group_id == "g" ? 4 : 0;
  }
  bool AssignmentMatches(
      std::string_view group_id, std::string_view node_id,
      const meta::MetaAssignmentId& assignment) const override {
    const auto it = assignments_.find(std::string(node_id));
    return group_id == "g" && it != assignments_.end() &&
           it->second == assignment;
  }
  bool IsOwnerAssignment(
      std::string_view group_id, std::string_view node_id,
      const meta::MetaAssignmentId& assignment) const override {
    return group_id == "g" && node_id == source_ &&
           AssignmentMatches(group_id, node_id, assignment);
  }
  std::optional<FailoverTransitionView> FailoverTransitionById(
      const meta::MetaFailoverTransitionId& id) const override {
    if (transition_.has_value() && transition_->transition_id_ == id) {
      return FailoverTransitionView{"g", *transition_};
    }
    return std::nullopt;
  }

  std::string source_ = Node('a');
  std::string candidate_ = Node('b');
  std::set<std::string> active_{source_, candidate_};
  std::map<std::string, meta::MetaAssignmentId> assignments_{
      {source_, Bytes<16>(0x11)}, {candidate_, Bytes<16>(0x22)}};
  std::optional<meta::MetaFailoverTransition> transition_;
};

meta::MetaFailoverTransition Controlled(const FailoverFacts& facts) {
  meta::MetaFailoverCandidateAction action;
  action.action_id_ = Bytes<16>(0x44);
  action.candidate_ = {facts.candidate_, Bytes<16>(0x22), Bytes<20>(0xb2)};
  action.domain_ = {
      7, facts.source_, Bytes<16>(0x11), Bytes<20>(0xa1), Bytes<20>(0xa2), 2};

  meta::MetaFailoverTransition transition;
  transition.transition_id_ = Bytes<16>(0x33);
  transition.revision_ = 20;
  transition.mode_ = meta::MetaFailoverMode::kControlled;
  transition.target_term_ = 8;
  transition.candidate_action_ = action;
  transition.controlled_ = meta::MetaControlledFailover{Bytes<16>(0x55), 9999};
  return transition;
}

meta::MetaObservationIdentity SourceIdentity(const FailoverFacts& facts) {
  return {facts.source_, Bytes<20>(0xa1), 1};
}

meta::MetaObservationIdentity CandidateIdentity(const FailoverFacts& facts) {
  return {facts.candidate_, Bytes<20>(0xb2), 1};
}

meta::MetaFailoverObservationObs SourcePaused(const FailoverFacts& facts) {
  return {.payload_ = meta::MetaSourcePausedObs{
              .group_id_ = "g",
              .transition_id_ = Bytes<16>(0x33),
              .source_node_id_ = facts.source_,
              .source_assignment_id_ = Bytes<16>(0x11),
              .source_boot_id_ = Bytes<20>(0xa1),
              .source_history_id_ = Bytes<20>(0xa2),
              .source_group_term_ = 7,
              .stable_next_lsns_ = {10, 20},
          }};
}

meta::MetaFailoverObservationObs CandidatePrepared(const FailoverFacts& facts) {
  return {.payload_ = meta::MetaCandidatePreparedObs{
              .group_id_ = "g",
              .transition_id_ = Bytes<16>(0x33),
              .action_id_ = Bytes<16>(0x44),
              .candidate_node_id_ = facts.candidate_,
              .candidate_assignment_id_ = Bytes<16>(0x22),
              .candidate_boot_id_ = Bytes<20>(0xb2),
              .prepared_context_id_ = Bytes<16>(0x66),
          }};
}

meta::MetaFailoverObservationObs ActionFailed(const FailoverFacts& facts) {
  return {.payload_ = meta::MetaActionFailedObs{
              .group_id_ = "g",
              .transition_id_ = Bytes<16>(0x33),
              .action_id_ = Bytes<16>(0x44),
              .candidate_node_id_ = facts.candidate_,
              .candidate_assignment_id_ = Bytes<16>(0x22),
              .candidate_boot_id_ = Bytes<20>(0xb2),
              .population_manifest_revision_ = 9,
              .population_manifest_digest_ = Bytes<32>(0x39),
              .partition_replication_epoch_ = 4,
              .failure_class_ = "watchdog-timeout",
              .failure_detail_ = "promotion did not become durable",
          }};
}

TEST(MetaFailoverObservationStore,
     SourceDisconnectRetainsExactObservationOnlyThroughIndependentGrace) {
  meta::MetaObservationStore::Limits limits;
  limits.ttl_ms_ = 30;
  meta::MetaObservationStore store(limits);
  FailoverFacts facts;
  facts.transition_ = Controlled(facts);
  const auto identity = SourceIdentity(facts);
  ASSERT_TRUE(store.AdoptSession(identity, 999).ok());
  ASSERT_TRUE(
      store
          .Ingest({.identity_ = identity, .payload_ = SourcePaused(facts)},
                  facts, 1000)
          .ok());

  store.InvalidateCandidateOnDisconnect(identity, 1001);
  EXPECT_TRUE(store.SourcePausedFor(Bytes<16>(0x33), facts, 1030).has_value());
  EXPECT_FALSE(store.SourcePausedFor(Bytes<16>(0x33), facts, 1031).has_value());
}

TEST(MetaFailoverObservationStore,
     CandidateDisconnectImmediatelyWithdrawsPreparedAndFailedFacts) {
  meta::MetaObservationStore store;
  FailoverFacts facts;
  facts.transition_ = Controlled(facts);
  const auto identity = CandidateIdentity(facts);
  ASSERT_TRUE(store.AdoptSession(identity, 999).ok());
  ASSERT_TRUE(
      store
          .Ingest({.identity_ = identity, .payload_ = CandidatePrepared(facts)},
                  facts, 1000)
          .ok());
  ASSERT_TRUE(
      store.CandidatePreparedFor(Bytes<16>(0x33), Bytes<16>(0x44), facts, 1001)
          .has_value());
  store.InvalidateCandidateOnDisconnect(identity, 1001);
  EXPECT_FALSE(
      store.CandidatePreparedFor(Bytes<16>(0x33), Bytes<16>(0x44), facts, 1001)
          .has_value());

  ASSERT_TRUE(
      store
          .Ingest({.identity_ = identity, .payload_ = ActionFailed(facts)},
                  facts, 1002)
          .ok());
  store.InvalidateCandidateOnDisconnect(identity, 1003);
  EXPECT_FALSE(
      store.ActionFailedFor(Bytes<16>(0x33), Bytes<16>(0x44), facts, 1003)
          .has_value());
}

TEST(MetaFailoverObservationStore,
     ReplacementClearsOldFactAndRejectsStaleActionWithoutRepair) {
  meta::MetaObservationStore store;
  FailoverFacts facts;
  facts.transition_ = Controlled(facts);
  const auto identity = CandidateIdentity(facts);
  ASSERT_TRUE(store.AdoptSession(identity, 999).ok());
  const meta::MetaNodeHealthObs health{.storage_ready_ = true,
                                       .population_ready_ = true};
  auto first = store.ReplaceHeartbeat(identity, health, std::nullopt,
                                      CandidatePrepared(facts), facts, 1000);
  ASSERT_TRUE(first.failover_status_.ok());

  auto stale = CandidatePrepared(facts);
  std::get<meta::MetaCandidatePreparedObs>(stale.payload_).action_id_ =
      Bytes<16>(0x45);
  auto replaced = store.ReplaceHeartbeat(identity, health, std::nullopt,
                                         std::move(stale), facts, 1001);
  EXPECT_FALSE(replaced.failover_status_.ok());
  EXPECT_FALSE(
      store.CandidatePreparedFor(Bytes<16>(0x33), Bytes<16>(0x44), facts, 1001)
          .has_value());
}

TEST(MetaFailoverObservationStore,
     CommitAndLeadershipChangesInvalidateSoftFacts) {
  meta::MetaObservationStore store;
  FailoverFacts facts;
  facts.transition_ = Controlled(facts);
  const auto identity = CandidateIdentity(facts);
  ASSERT_TRUE(store.AdoptSession(identity, 999).ok());
  ASSERT_TRUE(
      store
          .Ingest({.identity_ = identity, .payload_ = CandidatePrepared(facts)},
                  facts, 1000)
          .ok());

  facts.transition_->candidate_action_->action_id_ = Bytes<16>(0x45);
  store.RevalidateAll(facts, 1001);
  EXPECT_FALSE(
      store.CandidatePreparedFor(Bytes<16>(0x33), Bytes<16>(0x44), facts, 1001)
          .has_value());

  facts.transition_ = Controlled(facts);
  ASSERT_TRUE(
      store
          .Ingest({.identity_ = identity, .payload_ = CandidatePrepared(facts)},
                  facts, 1002)
          .ok());
  store.ResetForLeadershipChange();
  EXPECT_EQ(store.size(), 0u);
  EXPECT_FALSE(store.CurrentGeneration(facts.candidate_).has_value());
}

TEST(MetaFailoverObservationStore,
     ActionFailureRequiresExactCurrentPopulationAndBoundedDetail) {
  meta::MetaObservationStore store;
  FailoverFacts facts;
  facts.transition_ = Controlled(facts);
  const auto identity = CandidateIdentity(facts);
  ASSERT_TRUE(store.AdoptSession(identity, 999).ok());

  auto stale = ActionFailed(facts);
  std::get<meta::MetaActionFailedObs>(stale.payload_)
      .population_manifest_revision_ = 8;
  EXPECT_FALSE(
      store
          .Ingest({.identity_ = identity, .payload_ = std::move(stale)}, facts,
                  1000)
          .ok());
  EXPECT_TRUE(
      store
          .Ingest({.identity_ = identity, .payload_ = ActionFailed(facts)},
                  facts, 1001)
          .ok());
  EXPECT_TRUE(
      store.ActionFailedFor(Bytes<16>(0x33), Bytes<16>(0x44), facts, 1001)
          .has_value());
}

TEST(MetaFailoverObservationStore,
     FailoverLookupKeyIsChargedAgainstBothByteBudgets) {
  constexpr std::uint64_t kChargedBytes = 121;
  FailoverFacts facts;
  facts.transition_ = Controlled(facts);
  const auto identity = CandidateIdentity(facts);

  // CandidatePrepared retains three copies of the 40-byte node id (identity,
  // failover_by_node_ key, and payload) plus the one-byte group id.
  meta::MetaObservationStore::Limits per_node_limits;
  per_node_limits.max_retained_bytes_total_ = 1000;
  per_node_limits.max_retained_bytes_per_node_ = kChargedBytes - 1;
  meta::MetaObservationStore per_node_limited(per_node_limits);
  ASSERT_TRUE(per_node_limited.AdoptSession(identity, 999).ok());
  EXPECT_FALSE(
      per_node_limited
          .Ingest({.identity_ = identity, .payload_ = CandidatePrepared(facts)},
                  facts, 1000)
          .ok());
  EXPECT_EQ(per_node_limited.retained_bytes(), 0u);

  meta::MetaObservationStore::Limits total_limits;
  total_limits.max_retained_bytes_total_ = kChargedBytes - 1;
  total_limits.max_retained_bytes_per_node_ = 1000;
  meta::MetaObservationStore total_limited(total_limits);
  ASSERT_TRUE(total_limited.AdoptSession(identity, 999).ok());
  EXPECT_FALSE(
      total_limited
          .Ingest({.identity_ = identity, .payload_ = CandidatePrepared(facts)},
                  facts, 1000)
          .ok());
  EXPECT_EQ(total_limited.retained_bytes(), 0u);

  total_limits.max_retained_bytes_total_ = kChargedBytes;
  total_limits.max_retained_bytes_per_node_ = kChargedBytes;
  meta::MetaObservationStore exact_boundary(total_limits);
  ASSERT_TRUE(exact_boundary.AdoptSession(identity, 999).ok());
  ASSERT_TRUE(
      exact_boundary
          .Ingest({.identity_ = identity, .payload_ = CandidatePrepared(facts)},
                  facts, 1000)
          .ok());
  EXPECT_EQ(exact_boundary.retained_bytes(), kChargedBytes);
  EXPECT_EQ(exact_boundary.retained_bytes_for_node(facts.candidate_),
            kChargedBytes);
}

TEST(MetaFailoverObservationTest,
     RecoveryCompletionRequiresExactDeadlineAndCompleteVector) {
  FailoverFacts facts;
  facts.transition_ = Controlled(facts);
  facts.transition_->mode_ = meta::MetaFailoverMode::kUncontrolled;
  facts.transition_->controlled_.reset();
  facts.transition_->recovery_deadline_unix_ms_ = 3000;
  meta::MetaObservationStore store;
  const auto identity = CandidateIdentity(facts);
  ASSERT_TRUE(store.AdoptSession(identity, 999).ok());
  meta::MetaCandidateRecoveryCompleteObs complete{
      .group_id_ = "g",
      .transition_id_ = Bytes<16>(0x33),
      .action_id_ = Bytes<16>(0x44),
      .candidate_node_id_ = facts.candidate_,
      .candidate_assignment_id_ = Bytes<16>(0x22),
      .candidate_boot_id_ = Bytes<20>(0xb2),
      .recovery_deadline_unix_ms_ = 3000,
      .applied_next_lsns_ = {10, 20},
      .completion_reason_ = "deadline",
  };
  auto ingest = [&] {
    return store.Ingest(
        {.identity_ = identity,
         .payload_ = meta::MetaFailoverObservationObs{complete}},
        facts, 1000);
  };
  ASSERT_TRUE(ingest().ok());
  auto current = store.CandidateRecoveryCompleteFor(
      complete.transition_id_, complete.action_id_, facts, 1001);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(current->applied_next_lsns_, complete.applied_next_lsns_);
  EXPECT_EQ(current->session_generation_, identity.session_generation_);
  complete.recovery_deadline_unix_ms_ = 4000;
  EXPECT_FALSE(ingest().ok());
  complete.recovery_deadline_unix_ms_ = 3000;
  complete.applied_next_lsns_ = {10};
  EXPECT_FALSE(ingest().ok());
  complete.applied_next_lsns_ = {10, 0};
  EXPECT_FALSE(ingest().ok());
  complete.applied_next_lsns_ = {10, 20};
  complete.completion_reason_ = "still-receiving";
  EXPECT_FALSE(ingest().ok());
  facts.transition_->candidate_action_->action_id_ = Bytes<16>(0x45);
  EXPECT_FALSE(store
                   .CandidateRecoveryCompleteFor(complete.transition_id_,
                                                 complete.action_id_, facts,
                                                 1001)
                   .has_value());
}

}  // namespace
