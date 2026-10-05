/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include <array>
#include <csignal>
#include <string_view>
#include <utility>
#include <variant>

#include "gtest/gtest.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_machine.h"

namespace {
namespace meta = lavik::meta;

TEST(MetaProposalViewTest,
     AdmissionOwnsAuditAndIndicesAcrossAdvanceAndTeardown) {
  auto opened = meta::MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  auto machine = std::move(*opened);
  meta::RegisterNode node;
  node.request_id_.fill(1);
  node.node_id_ = std::string(40, 'a');
  node.principal_ = "lavik://node/" + node.node_id_;
  node.endpoints_ = {"tcp://127.0.0.1:6379"};
  auto encoded = meta::MetaStateMachine::EncodeCommand(node);
  ASSERT_TRUE(encoded.ok());
  machine->commit(1, **encoded);
  const auto captured = machine->CaptureProposal(node);
  EXPECT_EQ(captured.applied_index(), 1u);
  EXPECT_EQ(captured.state_change_index(), 1u);
  ASSERT_TRUE(captured.audit().Find(1).has_value());
  machine->Advance(2);
  const auto advanced = machine->CaptureProposal(node);
  EXPECT_EQ(advanced.applied_index(), 2u);
  EXPECT_EQ(advanced.state_change_index(), 1u);
  machine.reset();
  EXPECT_EQ(captured.applied_index(), 1u);
  EXPECT_EQ(captured.audit().size(), 1u);
  EXPECT_EQ(captured.audit().Find(1)->verdict_,
            meta::MetaAuditVerdict::kAccepted);
}

TEST(MetaProposalViewTest,
     OperationHeaderDistinguishesLiveTerminalAndArchived) {
  meta::MetaStores stores;
  meta::SubmitOperation submit;
  submit.operation_id_.fill(1);
  submit.kind_ = "maintenance";
  submit.intent_ = std::string(200'000, 'x');
  submit.intent_hash_ = meta::MetaSha256(submit.intent_);
  ASSERT_TRUE(stores.operation_.SubmitOperation(submit, 1).ok());
  meta::CompleteOperation complete;
  complete.operation_id_ = submit.operation_id_;
  auto captured = meta::MetaProposalView::FromStores(complete, stores, {10, 9});
  ASSERT_TRUE(captured.operation_header(submit.operation_id_).has_value());
  EXPECT_EQ(captured.operation_header(submit.operation_id_)->kind_,
            "maintenance");
  ASSERT_TRUE(stores.operation_.CompleteOperation(complete).ok());
  const auto terminal =
      meta::MetaProposalView::FromStores(complete, stores, {11, 11});
  EXPECT_TRUE(terminal.operation_header(submit.operation_id_).has_value());
  meta::ArchiveOperations archive;
  archive.operation_seqs_ = {1};
  ASSERT_TRUE(stores.operation_.ArchiveOperations(archive).ok());
  const auto archived =
      meta::MetaProposalView::FromStores(complete, stores, {12, 12});
  EXPECT_FALSE(archived.operation_header(submit.operation_id_).has_value());
  EXPECT_EQ(captured.operation_header(submit.operation_id_)->kind_,
            "maintenance");
  EXPECT_EQ(captured.applied_index(), 10u);
  EXPECT_EQ(captured.state_change_index(), 9u);
}

TEST(MetaProposalViewTest, FailoverRetainsGlobalFactsAndOnlyCurrentPolicies) {
  meta::MetaStores stores;
  meta::RegisterNode node;
  node.node_id_ = std::string(40, 'a');
  node.principal_ = "lavik://node/" + node.node_id_;
  node.endpoints_ = {"tcp://127.0.0.1:6379"};
  ASSERT_TRUE(stores.identity_.Apply(node).ok());
  for (const std::string id : {"target", "other"}) {
    meta::CreateGroup group;
    group.group_id_ = id;
    group.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
    ASSERT_TRUE(stores.topology_.Apply(group).ok());
  }
  meta::BeginGroupTerm term;
  term.group_id_ = "other";
  term.new_term_ = 1;
  ASSERT_TRUE(stores.topology_.BeginGroupTerm(term).ok());
  meta::PutPolicy policy;
  policy.policy_id_ = meta::kAuthorityLeasePolicyId;
  policy.version_ = 1;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  ASSERT_TRUE(stores.policy_.Apply(policy).ok());
  policy.version_ = 2;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":6000})";
  ASSERT_TRUE(stores.policy_.Apply(policy).ok());
  meta::BeginUncontrolledFailover begin;
  begin.group_id_ = "target";
  begin.trigger_reason_ = meta::MetaAutomaticFailoverReason::kSessionMissing;
  const auto captured =
      meta::MetaProposalView::FromStores(begin, stores, {10, 10});
  ASSERT_TRUE(captured.group("target").has_value());
  EXPECT_EQ(captured.facts().CurrentGroupTerm("other"), 1u);
  EXPECT_TRUE(captured.facts().IsActiveNode(node.node_id_));
  EXPECT_EQ(captured.automatic_policies().topology_epoch_, 2u);
  ASSERT_TRUE(captured.automatic_policies().lease_.has_value());
  EXPECT_EQ(captured.automatic_policies().lease_->version_, 2u);
  EXPECT_EQ(captured.automatic_policies().lease_->duration_ms_, 6000u);
  EXPECT_FALSE(captured.automatic_policies().automatic_.has_value());
  const auto raw_lease = stores.policy_.CurrentVersion(policy.policy_id_);
  ASSERT_TRUE(raw_lease.has_value());
  EXPECT_EQ(raw_lease->content_, policy.content_);
  policy.version_ = 3;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":7000})";
  ASSERT_TRUE(stores.policy_.Apply(policy).ok());
  policy.policy_id_ = meta::kAutomaticUncontrolledFailoverPolicyId;
  EXPECT_FALSE(stores.policy_.CurrentVersion(policy.policy_id_).has_value());
  policy.version_ = 1;
  policy.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
  ASSERT_TRUE(stores.policy_.Apply(policy).ok());
  const auto updated =
      meta::MetaProposalView::FromStores(begin, stores, {11, 11});
  EXPECT_EQ(updated.automatic_policies().lease_,
            stores.policy_.CurrentAuthorityLease());
  EXPECT_EQ(updated.automatic_policies().automatic_,
            stores.policy_.CurrentAutomaticUncontrolledFailover());
  EXPECT_EQ(raw_lease->version_, 2u);
  EXPECT_NE(
      raw_lease->content_,
      stores.policy_.CurrentVersion(std::string(meta::kAuthorityLeasePolicyId))
          ->content_);
  EXPECT_EQ(captured.automatic_policies().lease_->duration_ms_, 6000u);
  EXPECT_FALSE(captured.automatic_policies().automatic_.has_value());
  meta::RetireNode retire;
  retire.node_id_ = node.node_id_;
  retire.expected_revision_ = 1;
  ASSERT_TRUE(stores.identity_.Apply(retire).ok());
  EXPECT_TRUE(captured.facts().IsActiveNode(node.node_id_));
}

template <std::size_t... I>
auto EveryCommand(std::index_sequence<I...>) {
  return std::array<meta::MetaCommand, sizeof...(I)>{
      meta::MetaCommand(std::in_place_index<I>)...};
}

TEST(MetaProposalViewTest, EveryCommandPreservesEmptyStateHookVerdict) {
  auto opened = meta::MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  meta::MetaObservationStore observations;
  auto commands = EveryCommand(
      std::make_index_sequence<std::variant_size_v<meta::MetaCommand>>{});
  // These are the baseline hook's first rejection messages, independent of
  // capture classification. All other empty-state commands pass this hook;
  // their durable validation still belongs to deterministic apply.
  constexpr std::array<std::string_view, 35> errors{
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "controlled failover begin pre-state is stale",
      "uncontrolled failover begin pre-state is stale",
      "uncontrolled candidate set pre-state is stale",
      "failover authorization pre-state is stale",
      "",
      "controlled failover degrade pre-state is stale",
      "failover commit transition pre-state is stale",
      "failover commit transition pre-state is stale",
      "candidate recovery start pre-state is stale"};
  static_assert(errors.size() == std::variant_size_v<meta::MetaCommand>);
  for (auto& command : commands) {
    SCOPED_TRACE(command.index());
    if (auto* begin = std::get_if<meta::BeginControlledFailover>(&command)) {
      begin->absolute_deadline_unix_ms_ = 2000;
    }
    const auto view = (*opened)->CaptureProposal(command);
    const auto status =
        meta::ValidateFailoverProposal(command, view, observations, 1000);
    EXPECT_EQ(status.ok(), errors[command.index()].empty());
    EXPECT_EQ(status.message(), errors[command.index()]);
    if (!status.ok()) {
      EXPECT_EQ(meta::MetaFailureClassOf(status),
                meta::MetaFailureClass::kDomainReject);
    }
  }
}

TEST(MetaProposalViewDeathTest, UncapturedAndWrongKeyQueriesNeverMeanAbsent) {
  const auto previous_style = GTEST_FLAG_GET(death_test_style);
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  const meta::MetaStores stores;
  const auto ordinary =
      meta::MetaProposalView::FromStores(meta::RegisterNode{}, stores, {});
  EXPECT_EXIT((void)ordinary.facts(), ::testing::KilledBySignal(SIGABRT), "");
  EXPECT_EXIT((void)ordinary.operation_header({}),
              ::testing::KilledBySignal(SIGABRT), "");
  meta::CompleteOperation complete;
  complete.operation_id_.fill(1);
  const auto operation =
      meta::MetaProposalView::FromStores(complete, stores, {});
  EXPECT_FALSE(operation.operation_header(complete.operation_id_).has_value());
  EXPECT_EXIT((void)operation.operation_header({}),
              ::testing::KilledBySignal(SIGABRT), "");
  meta::BeginUncontrolledFailover begin;
  begin.group_id_ = "absent";
  const auto failover = meta::MetaProposalView::FromStores(begin, stores, {});
  EXPECT_FALSE(failover.group("absent").has_value());
  EXPECT_EXIT((void)failover.group("other"), ::testing::KilledBySignal(SIGABRT),
              "");
  EXPECT_EXIT((void)failover.automatic_policies(),
              ::testing::KilledBySignal(SIGABRT), "");
  GTEST_FLAG_SET(death_test_style, previous_style);
}

}  // namespace
