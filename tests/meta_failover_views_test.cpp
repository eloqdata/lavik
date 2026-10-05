/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include "gtest/gtest.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/failover_reconciler.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_machine.h"

namespace lavik::meta {
namespace {

TEST(MetaFailoverViewsTest, DiscoveryOwnsSelectedOperationsInSequenceOrder) {
  MetaStores stores;
  SubmitOperation request;
  request.operation_id_.fill(3);
  request.kind_ = kFailoverOperationKind;
  request.intent_ = *EncodeFailoverOperationIntent({"retained-group", 9999});
  request.intent_hash_ = MetaSha256(request.intent_);
  ASSERT_TRUE(stores.operation_.SubmitOperation(request, 10).ok());
  request.operation_id_.fill(1);
  ASSERT_TRUE(stores.operation_.SubmitOperation(request, 20).ok());
  request.operation_id_.fill(2);
  request.kind_ = "unrelated";
  ASSERT_TRUE(stores.operation_.SubmitOperation(request, 15).ok());
  auto opened = MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  auto machine = std::move(*opened);
  const auto image = stores.Serialize();
  ASSERT_TRUE(image.ok());
  const auto installed = machine->Install(30, *image);
  ASSERT_TRUE(installed.ok()) << installed;
  const auto captured = machine->CaptureFailoverDiscovery();
  machine->Advance(31);
  const auto advanced = machine->CaptureFailoverDiscovery();
  machine.reset();
  EXPECT_EQ(captured.cursor_.applied_index(), 30);
  EXPECT_EQ(advanced.cursor_.applied_index(), 31);
  EXPECT_EQ(advanced.cursor_.state_change_index(), 30);
  ASSERT_EQ(captured.operations_.size(), 2);
  EXPECT_EQ(captured.operations_[0].operation_seq_, 10);
  EXPECT_EQ(captured.operations_[1].operation_seq_, 20);
  EXPECT_EQ(captured.operations_[0].intent_, request.intent_);
}

TEST(MetaFailoverViewsTest, DiscoveryRaceRediscoverWithoutConsumingIds) {
  MetaStores stores;
  SubmitOperation request;
  request.operation_id_.fill(1);
  request.kind_ = kFailoverOperationKind;
  request.intent_ = *EncodeFailoverOperationIntent({"missing-group", 1000});
  request.intent_hash_ = MetaSha256(request.intent_);
  ASSERT_TRUE(stores.operation_.SubmitOperation(request, 1).ok());
  auto opened = MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  auto machine = std::move(*opened);
  ASSERT_TRUE(machine->Install(1, *stores.Serialize()).ok());
  auto discovery = machine->CaptureFailoverDiscovery();
  MetaObservationStore observations;
  int consumed = 0;
  MetaFailoverPlannerContext context{
      .now_unix_ms_ = 1000, .next_id_ = [&]() -> absl::StatusOr<MetaRequestId> {
        ++consumed;
        MetaRequestId id{};
        id.fill(3);
        return id;
      }};
  // No command event is required: a snapshot replaces the operation between
  // discovery and the target capture. A stale deadline must not allocate IDs.
  MetaStores empty;
  ASSERT_TRUE(machine->Install(2, *empty.Serialize()).ok());
  const auto plan = PlanFailoverStep(
      discovery,
      [&](const auto& group, auto operation) {
        return machine->CaptureFailoverPlanningView(discovery.cursor_, group,
                                                    operation);
      },
      observations, context);
  EXPECT_EQ(plan.status().code(), absl::StatusCode::kAborted);
  EXPECT_EQ(consumed, 0);
  discovery = machine->CaptureFailoverDiscovery();
  const auto retried = PlanFailoverStep(
      discovery,
      [&](const auto& group, auto operation) {
        return machine->CaptureFailoverPlanningView(discovery.cursor_, group,
                                                    operation);
      },
      observations, context);
  ASSERT_TRUE(retried.ok());
  EXPECT_FALSE(retried->has_value());
  EXPECT_EQ(consumed, 0);
}

TEST(MetaFailoverViewsTest, AutomaticPreemptionRequiresEveryPristineCondition) {
  MetaOperationRecord pristine;
  pristine.operation_id_.fill(1);
  pristine.operation_seq_ = 1;
  pristine.kind_ = kFailoverOperationKind;
  pristine.lifecycle_ = MetaOperationLifecycle::kSubmitted;
  pristine.intent_ = *EncodeFailoverOperationIntent({"g1", 9999});
  pristine.intent_hash_ = MetaSha256(pristine.intent_);
  using Mutation = std::function<void(MetaOperationRecord&)>;
  const std::vector<Mutation> invalid{
      [](auto& op) { op.kind_ = "other"; },
      [](auto& op) { op.lifecycle_ = MetaOperationLifecycle::kRunning; },
      [](auto& op) { op.revision_ = 1; },
      [](auto& op) { op.kind_phase_blob_ = "phase"; },
      [](auto& op) { op.current_directives_.emplace_back(); },
      [](auto& op) { op.terminal_receipts_.emplace_back(); },
      [](auto& op) { op.replication_history_id_.back() = 1; },
      [](auto& op) { op.intent_hash_.back() ^= 1; },
      [](auto& op) {
        op.intent_ = "invalid intent";
        op.intent_hash_ = MetaSha256(op.intent_);
      },
      [](auto& op) {
        op.intent_ = *EncodeFailoverOperationIntent({"other-group", 9999});
        op.intent_hash_ = MetaSha256(op.intent_);
      },
  };
  auto second = pristine;
  second.operation_id_.fill(2);
  second.operation_seq_ = 2;
  for (std::size_t i = 0; i < invalid.size(); ++i) {
    SCOPED_TRACE(i);
    MetaAutomaticTriggerView view{{}, {pristine, second}};
    EXPECT_EQ(view.PreemptableControlledRequest("g1"), &view.operations_[0]);
    invalid[i](view.operations_[0]);
    EXPECT_EQ(view.PreemptableControlledRequest("g1"), &view.operations_[1]);
    invalid[i](view.operations_[1]);
    EXPECT_EQ(view.PreemptableControlledRequest("g1"), nullptr);
  }
}

}  // namespace
}  // namespace lavik::meta
