#include "keylane/meta/candidate_plan.h"

#include <array>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {

using keylane::meta::CandidatePlanDisposition;
using keylane::meta::CandidatePlanFor;
using keylane::meta::CandidateSelectionBasis;
using keylane::meta::MetaAssignmentId;
using keylane::meta::MetaBootIncarnation;
using keylane::meta::MetaCandidateProgressObs;
using keylane::meta::MetaCommittedFacts;
using keylane::meta::MetaHash256;
using keylane::meta::MetaObservation;
using keylane::meta::MetaObservationIdentity;
using keylane::meta::MetaObservationStore;
using keylane::meta::MetaOperationId;
using keylane::meta::MetaReplicationHistoryId;

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
  bool OperationNonTerminal(const MetaOperationId&) const override {
    return false;
  }
  bool HistoryBoundToOperation(
      const MetaOperationId&,
      const MetaReplicationHistoryId&) const override {
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
  const MetaObservationIdentity identity{
      candidate.node_id_, candidate.boot_incarnation_,
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

}  // namespace
