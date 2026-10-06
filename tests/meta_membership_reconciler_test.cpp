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

#include "gtest/gtest.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/membership_reconciler.h"
#include "lavik/meta/state_machine.h"
#include "support/test_data_path.h"

namespace lavik::meta {
namespace {
class MembershipRecoveryTest : public testing::Test {
 protected:
  MetaMembershipPeer Peer(unsigned id) {
    return {
        .id_ = id,
        .endpoint_ = "127.0.0.1:" + std::to_string(7100 + id),
        .principal_ = "lavik://meta/" + std::to_string(id),
        .data_control_endpoint_ = "127.0.0.1:" + std::to_string(7300 + id),
        .ctl_endpoint_ = "127.0.0.1:" + std::to_string(7200 + id),
        .sentinel_endpoint_ =
            sentinel_routes_ ? (id == 1 ? "127.0.0.1:26379"
                                        : "tls://meta" + std::to_string(id) +
                                              ".example:26379")
                             : "",
    };
  }
  MetaMemberRecord Binding(unsigned id) {
    return {id,
            Peer(id).principal_,
            "127.0.0.1:" + std::to_string(7300 + id),
            "127.0.0.1:" + std::to_string(7200 + id),
            false,
            Peer(id).sentinel_endpoint_};
  }
  void Apply(MetaCommand c) {
    auto result =
        ApplyCommitted(stores_, ++index_, c, "lavik://operator/test", "now");
    ASSERT_EQ(result.verdict_, MetaAuditVerdict::kAccepted) << result.detail_;
    // Recover after every effect, including effect/checkpoint gaps.
    auto bytes = stores_.Serialize();
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    auto restored = MetaStores::Deserialize(*bytes);
    ASSERT_TRUE(restored.ok()) << restored.status();
    stores_ = std::move(*restored);
  }
  void Bind(unsigned id) {
    auto b = Binding(id);
    BindMetaMember c;
    c.server_id_ = id;
    c.principal_ = b.principal_;
    c.data_control_endpoint_ = b.data_control_endpoint_;
    c.ctl_endpoint_ = b.ctl_endpoint_;
    c.sentinel_endpoint_ = b.sentinel_endpoint_;
    Apply(c);
  }
  void Start(bool add) {
    Bind(1);
    Bind(2);
    config_ = {Peer(1), Peer(2)};
    intent_ = {add,
               Peer(add ? 3 : 2),
               Binding(add ? 3 : 2),
               config_,
               {Binding(1), Binding(2)}};
    auto bytes = EncodeMembershipIntent(intent_);
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    SubmitOperation c;
    id_.fill(1);
    c.operation_id_ = id_;
    c.kind_ = kMetaMembershipOperationKind;
    c.intent_ = *bytes;
    c.intent_hash_ = MetaSha256(*bytes);
    Apply(c);
  }
  absl::StatusOr<std::optional<MetaMembershipStep>> Plan(unsigned local = 1) {
    auto opened = MetaStateMachine::Open(
        lavik::test::TestDataPath("membership_reconciler_capture"));
    if (!opened.ok()) return opened.status();
    auto image = stores_.Serialize();
    if (!image.ok()) return image.status();
    if (auto status = (*opened)->Install(index_, *image); !status.ok())
      return status;
    const auto op = (*opened)->FindOperation(id_);
    std::vector<std::uint32_t> ids{intent_.target_.id_};
    for (const auto& binding : intent_.bindings_)
      ids.push_back(binding.server_id_);
    auto view = (*opened)->CaptureMembershipView(*op, ids);
    if (!view)
      return absl::InternalError("uncontended membership capture changed");
    return PlanMembershipStep(*view, config_, local);
  }
  void AdvanceToRaft() {
    for (unsigned i = 0; i < 10; ++i) {
      auto step = Plan();
      ASSERT_TRUE(step.ok() && step->has_value()) << step.status();
      if (std::holds_alternative<MetaMembershipRaftAction>(**step)) return;
      Apply(std::get<MetaCommand>(**step));
      ASSERT_FALSE(HasFatalFailure());
    }
    FAIL() << "did not reach Raft action";
  }
  void CommitConfig() {
    if (intent_.add_)
      config_.push_back(intent_.target_);
    else
      config_.pop_back();
  }
  void Finish() {
    for (unsigned i = 0; i < 10; ++i) {
      auto step = Plan();
      ASSERT_TRUE(step.ok()) << step.status();
      if (!step->has_value()) return;
      ASSERT_TRUE(std::holds_alternative<MetaCommand>(**step));
      Apply(std::get<MetaCommand>(**step));
      ASSERT_FALSE(HasFatalFailure());
    }
    FAIL() << "did not complete";
  }
  bool sentinel_routes_ = false;
  MetaStores stores_;
  std::uint64_t index_ = 0;
  MetaOperationId id_{};
  MetaMembershipIntent intent_;
  std::vector<MetaMembershipPeer> config_;
};

TEST_F(MembershipRecoveryTest, TaggedSentinelJoinRecoversEveryCheckpoint) {
  sentinel_routes_ = true;
  Start(true);
  AdvanceToRaft();
  CommitConfig();
  Finish();
  EXPECT_EQ(stores_.identity_.FindMetaMember(1)->sentinel_endpoint_,
            "127.0.0.1:26379");
  EXPECT_EQ(stores_.identity_.FindMetaMember(3)->sentinel_endpoint_,
            "tls://meta3.example:26379");
  const auto encoded = EncodeMembershipIntent(intent_);
  ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(*DecodeMembershipIntent(*encoded), intent_);
}

TEST_F(MembershipRecoveryTest,
       TaggedSentinelRetirementRecoversEveryCheckpoint) {
  sentinel_routes_ = true;
  Start(false);
  AdvanceToRaft();
  CommitConfig();
  Finish();
  const auto member = stores_.identity_.FindMetaMember(2);
  ASSERT_TRUE(member);
  EXPECT_TRUE(member->retired_);
  EXPECT_EQ(member->sentinel_endpoint_, "tls://meta2.example:26379");
}

TEST_F(MembershipRecoveryTest,
       InitialConfigBindingsResumeInIdOrderWithoutMembershipOperation) {
  config_ = {Peer(1), Peer(2), Peer(3)};
  for (unsigned expected_id = 1; expected_id <= 3; ++expected_id) {
    auto step =
        PlanInitialMetaBindings(stores_.identity_.MetaMembers(), config_,
                                /*initial_config=*/true);
    ASSERT_TRUE(step.ok()) << step.status();
    ASSERT_TRUE(step->has_value());
    EXPECT_EQ((*step)->server_id_, expected_id);
    Apply(MetaCommand(std::move(**step)));
  }

  auto complete =
      PlanInitialMetaBindings(stores_.identity_.MetaMembers(), config_,
                              /*initial_config=*/true);
  ASSERT_TRUE(complete.ok()) << complete.status();
  EXPECT_FALSE(complete->has_value());
  EXPECT_TRUE(stores_.operation_.LiveOperations().empty());
}

TEST_F(MembershipRecoveryTest,
       InitialConfigBindingConflictAndPostGenesisGapFailClosed) {
  config_ = {Peer(1), Peer(2)};
  Bind(1);
  BindMetaMember conflicting;
  conflicting.server_id_ = 2;
  conflicting.principal_ = Peer(2).principal_;
  conflicting.data_control_endpoint_ = "127.0.0.1:7999";
  conflicting.ctl_endpoint_ = Peer(2).ctl_endpoint_;
  Apply(MetaCommand(conflicting));

  EXPECT_EQ(PlanInitialMetaBindings(stores_.identity_.MetaMembers(), config_,
                                    /*initial_config=*/true)
                .status()
                .code(),
            absl::StatusCode::kFailedPrecondition);

  MetaStores empty;
  EXPECT_EQ(PlanInitialMetaBindings(empty.identity_.MetaMembers(), config_,
                                    /*initial_config=*/false)
                .status()
                .code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST_F(MembershipRecoveryTest,
       AdminCaptureSelectsOnlyActiveWorkflowAndKeepsIntent) {
  Start(true);
  SubmitOperation unrelated;
  unrelated.operation_id_.fill(99);
  unrelated.kind_ = "maintenance";
  unrelated.intent_ = std::string(256 * 1024, 'x');
  unrelated.intent_hash_ = MetaSha256(unrelated.intent_);
  Apply(unrelated);
  auto opened = MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  ASSERT_TRUE((*opened)->Install(index_, *stores_.Serialize()).ok());
  const auto captured = (*opened)->CaptureMembershipAdmin();
  EXPECT_EQ(captured.operation_, stores_.operation_.FindOperation(id_));
  EXPECT_EQ(captured.identity_.NodeCount(), 0);
  EXPECT_TRUE(captured.identity_.MetaMembers().empty());
  EXPECT_TRUE((*opened)->CaptureCreatePreflight().active_membership_);
  EXPECT_FALSE((*opened)->CaptureCreatePreflight().data_artifacts_);
  AdvanceToRaft();
  CommitConfig();
  Finish();
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_TRUE((*opened)->Install(index_, *stores_.Serialize()).ok());
  const auto completed = (*opened)->CaptureMembershipAdmin();
  EXPECT_FALSE(completed.operation_);
  EXPECT_EQ(completed.identity_.Serialize(), stores_.identity_.Serialize());
  EXPECT_FALSE((*opened)->CaptureCreatePreflight().active_membership_);
  ASSERT_TRUE(captured.operation_);
  EXPECT_EQ(*DecodeMembershipIntent(captured.operation_->intent_), intent_);
}

TEST_F(MembershipRecoveryTest, AddRestoresEveryCommittedPrefix) {
  Start(true);
  AdvanceToRaft();
  ASSERT_FALSE(HasFatalFailure());
  EXPECT_TRUE(stores_.identity_.IsActiveMetaMember(3, Peer(3).principal_));
  CommitConfig();
  Finish();
  ASSERT_FALSE(HasFatalFailure());
  EXPECT_EQ(stores_.operation_.FindOperation(id_)->terminal_result_,
            "member-added");
}

TEST_F(MembershipRecoveryTest,
       InitialCaptureRejectsBindingsOutsideConfiguration) {
  config_ = {Peer(1), Peer(2)};
  Bind(1);
  Bind(2);
  Bind(3);
  auto opened = MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  auto image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE((*opened)->Install(index_, *image).ok());
  const auto discovery = (*opened)->CaptureMembershipDiscovery();
  EXPECT_FALSE(discovery.operation_);
  EXPECT_EQ(discovery.initial_bindings_.size(), 3u);
  const auto step =
      PlanInitialMetaBindings(discovery.initial_bindings_, config_, true);
  EXPECT_EQ(step.status().message(),
            "initial identity binding is absent from Meta config");
}

TEST_F(MembershipRecoveryTest,
       CaptureOwnsSelectedBindingsAndRetriesOperationChange) {
  Start(true);
  Bind(4);  // Ordinary planning needs only the retained baseline and target.
  SubmitOperation unrelated;
  unrelated.operation_id_.fill(99);
  unrelated.kind_ = "unrelated";
  unrelated.intent_ = std::string(256 * 1024, 'x');
  unrelated.intent_hash_ = MetaSha256(unrelated.intent_);
  Apply(unrelated);
  auto opened = MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  auto machine = std::move(*opened);
  auto image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE(machine->Install(index_, *image).ok());
  const auto discovery = machine->CaptureMembershipDiscovery();
  ASSERT_TRUE(discovery.operation_);
  EXPECT_EQ(discovery.operation_->operation_id_, id_);
  EXPECT_TRUE(discovery.initial_bindings_.empty());
  const std::vector<std::uint32_t> ids{3, 2, 1, 2};
  const auto view = machine->CaptureMembershipView(*discovery.operation_, ids);
  ASSERT_TRUE(view);
  ASSERT_EQ(view->bindings_.size(), 2u);
  EXPECT_EQ(view->bindings_[0], Binding(1));
  EXPECT_EQ(view->bindings_[1], Binding(2));
  EXPECT_FALSE(FindMetaBinding(view->bindings_, 3));
  EXPECT_FALSE(FindMetaBinding(view->bindings_, 4));
  machine->Advance(++index_);
  auto advanced = machine->CaptureMembershipView(*discovery.operation_, ids);
  ASSERT_TRUE(advanced);
  EXPECT_EQ(advanced->cursor_.applied_index(), index_);
  EXPECT_EQ(advanced->cursor_.state_change_index(),
            view->cursor_.state_change_index());
  Bind(3);
  image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE(machine->Install(index_, *image).ok());
  auto bound = machine->CaptureMembershipView(*discovery.operation_, ids);
  ASSERT_TRUE(bound);
  EXPECT_EQ(bound->bindings_.size(), 3u);
  EXPECT_EQ(bound->cursor_.state_change_index(), index_);
  TransitionOperationPhase phase;
  phase.operation_id_ = id_;
  phase.expected_revision_ = discovery.operation_->revision_;
  phase.kind_phase_blob_ = "bind-member";
  Apply(phase);
  image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE(machine->Install(index_, *image).ok());
  EXPECT_FALSE(machine->CaptureMembershipView(*discovery.operation_, ids));
  machine.reset();
  EXPECT_EQ(view->operation_, discovery.operation_);
  EXPECT_EQ(view->bindings_.size(), 2u);
  EXPECT_EQ(bound->bindings_.back(), Binding(3));
}
TEST_F(MembershipRecoveryTest, RemoveRetiresOnlyAfterExactCommittedConfig) {
  Start(false);
  AdvanceToRaft();
  ASSERT_FALSE(HasFatalFailure());
  for (unsigned i = 0; i < 3; ++i) {
    auto next = Plan();
    ASSERT_TRUE(next.ok() && next->has_value());
    EXPECT_EQ(std::get<MetaMembershipRaftAction>(**next),
              MetaMembershipRaftAction::kRemove);
    EXPECT_FALSE(stores_.identity_.FindMetaMember(2)->retired_);
  }
  CommitConfig();
  Finish();
  ASSERT_FALSE(HasFatalFailure());
  EXPECT_TRUE(stores_.identity_.FindMetaMember(2)->retired_);
  EXPECT_EQ(stores_.operation_.FindOperation(id_)->terminal_result_,
            "member-removed");
}
TEST_F(MembershipRecoveryTest, MissingInviteResultKeepsOriginalIdentity) {
  Start(true);
  AdvanceToRaft();
  ASSERT_FALSE(HasFatalFailure());
  auto original = *stores_.operation_.FindOperation(id_);
  for (unsigned i = 0; i < 3; ++i) {
    auto next = Plan();
    ASSERT_TRUE(next.ok() && next->has_value());
    EXPECT_EQ(std::get<MetaMembershipRaftAction>(**next),
              MetaMembershipRaftAction::kAdd);
    EXPECT_EQ(stores_.operation_.FindOperation(id_), std::optional(original));
  }
  CommitConfig();
  Finish();
}
TEST_F(MembershipRecoveryTest, NewlyElectedRemovalTargetYieldsLeadership) {
  Start(false);
  AdvanceToRaft();
  ASSERT_FALSE(HasFatalFailure());
  auto next = Plan(2);
  ASSERT_TRUE(next.ok() && next->has_value());
  EXPECT_EQ(std::get<MetaMembershipRaftAction>(**next),
            MetaMembershipRaftAction::kYieldLeadership);
  EXPECT_FALSE(stores_.identity_.FindMetaMember(2)->retired_);
  next = Plan(1);
  ASSERT_TRUE(next.ok() && next->has_value());
  EXPECT_EQ(std::get<MetaMembershipRaftAction>(**next),
            MetaMembershipRaftAction::kRemove);
}
TEST_F(MembershipRecoveryTest, DoesNotUndoChangedConfiguration) {
  Start(true);
  AdvanceToRaft();
  ASSERT_FALSE(HasFatalFailure());
  config_[0].endpoint_ = "127.0.0.1:9999";
  EXPECT_FALSE(Plan().ok());
}
TEST_F(MembershipRecoveryTest, DoesNotRetireAfterUnrelatedRemoval) {
  Start(false);
  AdvanceToRaft();
  ASSERT_FALSE(HasFatalFailure());
  config_.erase(config_.begin());
  EXPECT_FALSE(Plan().ok());
  EXPECT_FALSE(stores_.identity_.FindMetaMember(2)->retired_);
}
TEST_F(MembershipRecoveryTest, RejectsConcurrentCreationAndMembershipAtApply) {
  Start(true);
  ASSERT_FALSE(HasFatalFailure());
  auto original = *stores_.operation_.FindOperation(id_);
  SubmitOperation c;
  c.operation_id_.fill(2);
  c.intent_ = original.intent_;
  c.intent_hash_ = original.intent_hash_;
  for (auto kind :
       {kMetaMembershipOperationKind, kMetaClusterCreateOperationKind}) {
    c.kind_ = kind;
    auto result =
        ApplyCommitted(stores_, ++index_, c, "lavik://operator/test", "now");
    EXPECT_EQ(result.verdict_, MetaAuditVerdict::kRejected);
  }
  c.kind_ = kMetaMembershipOperationKind;
  c.operation_id_ = id_;
  Apply(c);  // Exact replay still resolves the original admitted operation.
}
TEST_F(MembershipRecoveryTest, CodecIsBoundedStrictAndCanonical) {
  Start(true);
  ASSERT_FALSE(HasFatalFailure());
  auto bytes = EncodeMembershipIntent(intent_);
  ASSERT_TRUE(bytes.ok());
  EXPECT_EQ(*DecodeMembershipIntent(*bytes), intent_);
  ASSERT_GE(bytes->size(), 2u);
  EXPECT_EQ((*bytes)[0], '\x01');
  EXPECT_EQ((*bytes)[1], '\x00');
  auto unsupported = *bytes;
  unsupported[0] = '\x02';
  EXPECT_FALSE(DecodeMembershipIntent(unsupported).ok());
  EXPECT_FALSE(DecodeMembershipIntent(*bytes + "x").ok());
  EXPECT_FALSE(
      DecodeMembershipIntent(bytes->substr(0, bytes->size() - 1)).ok());
  std::swap(intent_.before_[0], intent_.before_[1]);
  EXPECT_FALSE(EncodeMembershipIntent(intent_).ok());
}
}  // namespace
}  // namespace lavik::meta
