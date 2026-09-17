#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/meta/candidate_plan.h"

namespace {

using keylane::meta::CandidateCompatibilityDomain;
using keylane::meta::CandidatePlanDisposition;
using keylane::meta::CandidatePlanFor;
using keylane::meta::CandidatePlanForDomain;
using keylane::meta::CandidateSelectionBasis;
using keylane::meta::MetaAssignmentId;
using keylane::meta::MetaBootIncarnation;
using keylane::meta::MetaCandidateProgressObs;
using keylane::meta::MetaCommittedFacts;
using keylane::meta::MetaFailoverCandidate;
using keylane::meta::MetaFailoverCandidateAction;
using keylane::meta::MetaHash256;
using keylane::meta::MetaNodeHealthObs;
using keylane::meta::MetaObservation;
using keylane::meta::MetaObservationIdentity;
using keylane::meta::MetaObservationStore;
using keylane::meta::MetaOperationId;
using keylane::meta::MetaReplicationHistoryId;
using keylane::meta::UncontrolledCandidatePlanFor;

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

std::string Node(char value) { return std::string(40, value); }

class PlanFacts final : public MetaCommittedFacts {
 public:
  bool IsActiveNode(std::string_view node_id) const override {
    return active_.contains(std::string(node_id));
  }
  std::uint64_t CurrentGroupTerm(std::string_view group_id) const override {
    return group_id == "g" ? 7 : 0;
  }
  std::uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const override {
    return group_id == "g" ? 11 : 0;
  }
  MetaHash256 CurrentPopulationManifestDigest(
      std::string_view group_id) const override {
    return group_id == "g" ? Bytes<32>(0x44) : MetaHash256{};
  }
  std::uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const override {
    return group_id == "g" ? 13 : 0;
  }
  bool AssignmentMatches(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment) const override {
    const auto it = assignments_.find(std::string(node_id));
    return group_id == "g" && it != assignments_.end() &&
           it->second == assignment;
  }
  bool IsOwnerAssignment(std::string_view, std::string_view,
                         const MetaAssignmentId&) const override {
    return false;
  }

  std::set<std::string> active_;
  std::map<std::string, MetaAssignmentId> assignments_;
};

MetaCandidateProgressObs Candidate(std::string node, std::uint8_t identity,
                                   std::vector<std::uint64_t> frontier) {
  return {
      .node_id_ = std::move(node),
      .boot_incarnation_ = Bytes<20>(identity),
      .session_generation_ = 1,
      .group_id_ = "g",
      .assignment_id_ = Bytes<16>(identity),
      .group_term_ = 7,
      .population_manifest_revision_ = 11,
      .population_manifest_digest_ = Bytes<32>(0x44),
      .partition_replication_epoch_ = 13,
      .replication_history_id_ = Bytes<20>(identity + 20),
      .source_group_term_ = 7,
      .source_node_id_ = Node('f'),
      .source_assignment_id_ = Bytes<16>(0xf1),
      .source_boot_incarnation_ = Bytes<20>(0xf2),
      .source_replication_history_id_ = Bytes<20>(0xf3),
      .applied_next_lsns_ = std::move(frontier),
      .storage_ready_ = true,
      .population_ready_ = true,
  };
}

void Admit(MetaObservationStore& store, PlanFacts& facts,
           MetaCandidateProgressObs candidate, std::int64_t now) {
  const MetaObservationIdentity identity{candidate.node_id_,
                                         candidate.boot_incarnation_,
                                         candidate.session_generation_};
  facts.active_.insert(candidate.node_id_);
  facts.assignments_[candidate.node_id_] = candidate.assignment_id_;
  ASSERT_TRUE(store.AdoptSession(identity, now - 1).ok());
  ASSERT_TRUE(store
                  .Ingest(MetaObservation{.identity_ = identity,
                                          .payload_ = std::move(candidate)},
                          facts, now)
                  .ok());
}

TEST(MetaCandidatePlanTest, SelectsUniqueGreatestAndNeverChoosesDominated) {
  MetaObservationStore store;
  PlanFacts facts;
  Admit(store, facts, Candidate(Node('a'), 1, {10, 10}), 1000);
  Admit(store, facts, Candidate(Node('b'), 2, {11, 10}), 1000);
  Admit(store, facts, Candidate(Node('c'), 3, {9, 9}), 1000);

  const auto plan = CandidatePlanFor("g", facts, store, 1001);
  ASSERT_EQ(plan.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('b'));
  EXPECT_EQ(plan.selection_basis_, CandidateSelectionBasis::kUniqueGreatest);
  EXPECT_EQ(plan.maximal_node_ids_, (std::vector<std::string>{Node('b')}));
}

TEST(MetaCandidatePlanTest, EqualGreatestUsesLowestNodeId) {
  MetaObservationStore store;
  PlanFacts facts;
  Admit(store, facts, Candidate(Node('b'), 2, {11, 12}), 1000);
  Admit(store, facts, Candidate(Node('a'), 1, {11, 12}), 1000);

  const auto plan = CandidatePlanFor("g", facts, store, 1001);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('a'));
  EXPECT_EQ(plan.selection_basis_,
            CandidateSelectionBasis::kEqualGreatestNodeTieBreak);
}

TEST(MetaCandidatePlanTest, IncomparableMaximaMinimizeEnvelopeDeficit) {
  MetaObservationStore store;
  PlanFacts facts;
  Admit(store, facts, Candidate(Node('a'), 1, {10, 1}), 1000);
  Admit(store, facts, Candidate(Node('b'), 2, {9, 9}), 1000);

  const auto plan = CandidatePlanFor("g", facts, store, 1001);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('b'));
  EXPECT_EQ(plan.selection_basis_,
            CandidateSelectionBasis::kIncomparableEnvelopeDeficit);
}

TEST(MetaCandidatePlanTest, RefusesMixedSourceLineages) {
  MetaObservationStore store;
  PlanFacts facts;
  Admit(store, facts, Candidate(Node('a'), 1, {10, 1}), 1000);
  auto other = Candidate(Node('b'), 2, {9, 9});
  other.source_replication_history_id_ = Bytes<20>(0xaa);
  Admit(store, facts, std::move(other), 1000);

  const auto plan = CandidatePlanFor("g", facts, store, 1001);
  EXPECT_EQ(plan.disposition_,
            CandidatePlanDisposition::kMultipleCompatibilityDomains);
  EXPECT_FALSE(plan.selected_.has_value());
}

TEST(MetaCandidatePlanTest,
     ControlledSelectionUsesOnlyTheRequiredCompatibilityDomain) {
  MetaObservationStore store;
  PlanFacts facts;
  auto required = Candidate(Node('a'), 1, {10, 10});
  const auto domain = CandidateCompatibilityDomain(required);
  Admit(store, facts, required, 1000);
  auto unrelated = Candidate(Node('b'), 2, {100, 100});
  unrelated.source_replication_history_id_ = Bytes<20>(0xaa);
  Admit(store, facts, std::move(unrelated), 1000);

  const auto plan = CandidatePlanForDomain("g", domain, facts, store, 1001);
  ASSERT_EQ(plan.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('a'));
}

TEST(MetaCandidatePlanTest,
     UncontrolledSelectionPrefersNewestDomainWithoutComparingLsns) {
  MetaObservationStore store;
  PlanFacts facts;
  auto older = Candidate(Node('a'), 1, {100, 100});
  older.source_group_term_ = 6;
  Admit(store, facts, std::move(older), 1000);
  auto newer = Candidate(Node('b'), 2, {1, 1});
  newer.source_group_term_ = 7;
  Admit(store, facts, std::move(newer), 1000);

  const auto plan = UncontrolledCandidatePlanFor("g", facts, store, 1001);
  ASSERT_EQ(plan.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('b'));
}

TEST(MetaCandidatePlanTest,
     UncontrolledSelectionFallsBackAfterNewestDomainDisappears) {
  MetaObservationStore store;
  PlanFacts facts;
  auto older = Candidate(Node('a'), 1, {50, 50});
  older.source_group_term_ = 6;
  Admit(store, facts, older, 1000);
  auto newer = Candidate(Node('b'), 2, {1, 1});
  newer.source_group_term_ = 7;
  Admit(store, facts, newer, 1000);

  store.InvalidateCandidateOnDisconnect(
      {newer.node_id_, newer.boot_incarnation_, newer.session_generation_},
      1001);
  const auto plan = UncontrolledCandidatePlanFor("g", facts, store, 1001);
  ASSERT_EQ(plan.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('a'));
}

TEST(MetaCandidatePlanTest,
     UncontrolledSameTermDomainsUseCanonicalDomainIdentity) {
  MetaObservationStore store;
  PlanFacts facts;
  auto later_domain = Candidate(Node('a'), 1, {100, 100});
  later_domain.source_node_id_ = Node('f');
  Admit(store, facts, std::move(later_domain), 1000);
  auto earlier_domain = Candidate(Node('b'), 2, {1, 1});
  earlier_domain.source_node_id_ = Node('e');
  Admit(store, facts, std::move(earlier_domain), 1000);

  const auto plan = UncontrolledCandidatePlanFor("g", facts, store, 1001);
  ASSERT_EQ(plan.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('b'));
}

TEST(MetaCandidatePlanTest,
     UncontrolledReplacementExcludesTheExactFailedActionPopulation) {
  MetaObservationStore store;
  PlanFacts facts;
  auto failed = Candidate(Node('a'), 1, {11, 11});
  const MetaFailoverCandidateAction failed_action{
      .action_id_ = Bytes<16>(0xa1),
      .candidate_ =
          MetaFailoverCandidate{failed.node_id_, failed.assignment_id_,
                                failed.boot_incarnation_},
      .domain_ = CandidateCompatibilityDomain(failed)};
  Admit(store, facts, failed, 1000);
  Admit(store, facts, Candidate(Node('b'), 2, {10, 10}), 1000);

  const auto plan =
      UncontrolledCandidatePlanFor("g", facts, store, 1001, failed_action);
  ASSERT_EQ(plan.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(plan.selected_.has_value());
  EXPECT_EQ(plan.selected_->node_id_, Node('b'));
}

TEST(MetaCandidatePlanTest, TtlIsAppliedAtTheFixedPlanningInstant) {
  MetaObservationStore::Limits limits;
  limits.ttl_ms_ = 30;
  MetaObservationStore store(limits);
  PlanFacts facts;
  Admit(store, facts, Candidate(Node('a'), 1, {10, 10}), 1000);

  EXPECT_EQ(CandidatePlanFor("g", facts, store, 1030).disposition_,
            CandidatePlanDisposition::kSelected);
  EXPECT_EQ(CandidatePlanFor("g", facts, store, 1031).disposition_,
            CandidatePlanDisposition::kNoEligibleCandidates);
}

TEST(MetaCandidatePlanTest, MissingHeartbeatCandidateWithdrawsBeforeTtl) {
  MetaObservationStore::Limits limits;
  limits.ttl_ms_ = 30;
  MetaObservationStore store(limits);
  PlanFacts facts;
  const auto slower = Candidate(Node('a'), 1, {10, 10});
  const auto greatest = Candidate(Node('b'), 2, {11, 10});
  const MetaNodeHealthObs healthy{
      .storage_ready_ = true, .population_ready_ = true, .active_groups_ = 1};

  for (const auto& candidate : {slower, greatest}) {
    const MetaObservationIdentity identity{candidate.node_id_,
                                           candidate.boot_incarnation_,
                                           candidate.session_generation_};
    facts.active_.insert(candidate.node_id_);
    facts.assignments_[candidate.node_id_] = candidate.assignment_id_;
    ASSERT_TRUE(store.AdoptSession(identity, 999).ok());
    const auto accepted =
        store.ReplaceHeartbeat(identity, healthy, candidate, facts, 1000);
    ASSERT_TRUE(accepted.boot_status_.ok());
    ASSERT_TRUE(accepted.health_status_.ok());
    ASSERT_TRUE(accepted.candidate_status_.ok());
  }

  const auto initial = CandidatePlanFor("g", facts, store, 1001);
  ASSERT_EQ(initial.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(initial.selected_.has_value());
  ASSERT_EQ(initial.selected_->node_id_, greatest.node_id_);
  ASSERT_GT(initial.selected_->expires_unix_ms_, 1002);

  const MetaObservationIdentity identity{greatest.node_id_,
                                         greatest.boot_incarnation_,
                                         greatest.session_generation_};
  // A healthy heartbeat that lacks a coherent frontier withdraws role
  // evidence immediately; the previous report's TTL cannot bridge a busy read.
  const auto omitted =
      store.ReplaceHeartbeat(identity, healthy, std::nullopt, facts, 1002);
  ASSERT_TRUE(omitted.boot_status_.ok());
  ASSERT_TRUE(omitted.health_status_.ok());
  ASSERT_TRUE(omitted.candidate_status_.ok());
  const auto fallback = CandidatePlanFor("g", facts, store, 1002);
  ASSERT_EQ(fallback.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(fallback.selected_.has_value());
  EXPECT_EQ(fallback.selected_->node_id_, slower.node_id_);
  EXPECT_EQ(fallback.maximal_node_ids_,
            (std::vector<std::string>{slower.node_id_}));

  const auto resumed =
      store.ReplaceHeartbeat(identity, healthy, greatest, facts, 1003);
  ASSERT_TRUE(resumed.boot_status_.ok());
  ASSERT_TRUE(resumed.health_status_.ok());
  ASSERT_TRUE(resumed.candidate_status_.ok());
  const auto restored = CandidatePlanFor("g", facts, store, 1003);
  ASSERT_EQ(restored.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(restored.selected_.has_value());
  EXPECT_EQ(restored.selected_->node_id_, greatest.node_id_);
  EXPECT_EQ(restored.selection_basis_,
            CandidateSelectionBasis::kUniqueGreatest);
}

}  // namespace
