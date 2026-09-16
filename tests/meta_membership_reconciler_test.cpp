#include "gtest/gtest.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/membership_reconciler.h"

namespace keylane::meta {
namespace {
class MembershipRecoveryTest : public testing::Test {
 protected:
  MetaMembershipPeer Peer(unsigned id) {
    return {
        .id_ = id,
        .endpoint_ = "127.0.0.1:" + std::to_string(7100 + id),
        .principal_ = "keylane://meta/" + std::to_string(id),
        .data_control_endpoint_ = "127.0.0.1:" + std::to_string(7300 + id),
        .ctl_endpoint_ = "127.0.0.1:" + std::to_string(7200 + id),
    };
  }
  MetaMemberRecord Binding(unsigned id) {
    return {id, Peer(id).principal_, "127.0.0.1:" + std::to_string(7300 + id),
            "127.0.0.1:" + std::to_string(7200 + id), false};
  }
  void Apply(MetaCommand c) {
    auto result =
        ApplyCommitted(stores_, ++index_, c, "keylane://operator/test", "now");
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
  auto Plan(unsigned local = 1) {
    return PlanMembershipStep(MetaCommittedView(stores_, index_),
                              *stores_.operation_.FindOperation(id_), config_,
                              local);
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
  MetaStores stores_;
  std::uint64_t index_ = 0;
  MetaOperationId id_{};
  MetaMembershipIntent intent_;
  std::vector<MetaMembershipPeer> config_;
};

TEST_F(MembershipRecoveryTest,
       InitialConfigBindingsResumeInIdOrderWithoutMembershipOperation) {
  config_ = {Peer(1), Peer(2), Peer(3)};
  for (unsigned expected_id = 1; expected_id <= 3; ++expected_id) {
    auto step =
        PlanInitialMetaBindings(MetaCommittedView(stores_, index_), config_,
                                /*initial_config=*/true);
    ASSERT_TRUE(step.ok()) << step.status();
    ASSERT_TRUE(step->has_value());
    EXPECT_EQ((*step)->server_id_, expected_id);
    Apply(MetaCommand(std::move(**step)));
  }

  auto complete =
      PlanInitialMetaBindings(MetaCommittedView(stores_, index_), config_,
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

  EXPECT_EQ(PlanInitialMetaBindings(MetaCommittedView(stores_, index_), config_,
                                    /*initial_config=*/true)
                .status()
                .code(),
            absl::StatusCode::kFailedPrecondition);

  MetaStores empty;
  EXPECT_EQ(PlanInitialMetaBindings(MetaCommittedView(empty, 0), config_,
                                    /*initial_config=*/false)
                .status()
                .code(),
            absl::StatusCode::kFailedPrecondition);
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
        ApplyCommitted(stores_, ++index_, c, "keylane://operator/test", "now");
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
}  // namespace keylane::meta
