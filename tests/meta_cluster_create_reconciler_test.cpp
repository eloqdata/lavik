#include "gtest/gtest.h"
#include "keylane/meta/cluster_create.h"
#include "keylane/meta/cluster_create_reconciler.h"
#include "keylane/meta/hash.h"

namespace keylane::meta {
namespace {
class ClusterCreateRecoveryTest : public testing::Test {
 protected:
  void Apply(MetaCommand command) {
    std::visit([&](auto& c) { c.request_id_.fill(1); }, command);
    const auto result = ApplyCommitted(stores_, ++index_, command,
                                       "keylane://operator/test", "now");
    ASSERT_EQ(result.verdict_, MetaAuditVerdict::kAccepted) << result.detail_;
    // Destroy/recover ALL stores after EVERY individual effect, including the
    // gap before its phase checkpoint. The planner has no in-memory cursor.
    auto bytes = stores_.Serialize();
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    auto restored = MetaStores::Deserialize(*bytes);
    ASSERT_TRUE(restored.ok()) << restored.status();
    stores_ = std::move(*restored);
  }
  void SetUp() override {
    BindMetaMember member;
    member.server_id_ = 1;
    member.principal_ = "keylane://meta/1";
    member.data_control_endpoint_ = "127.0.0.1:7301";
    member.ctl_endpoint_ = "127.0.0.1:7201";
    Apply(member);
    ClusterCreateManifestV1 manifest{1,
                                     1,
                                     std::string(40, '1'),
                                     "tcp://127.0.0.1:6379",
                                     "group-1",
                                     std::string(40, '1'),
                                     0,
                                     16383};
    auto intent = EncodeClusterCreateRequest(manifest, 1);
    ASSERT_TRUE(intent.ok()) << intent.status();
    SubmitOperation submit;
    root_.fill(2);
    submit.operation_id_ = root_;
    submit.kind_ = kMetaClusterCreateOperationKind;
    submit.intent_ = *intent;
    submit.intent_hash_ = MetaSha256(*intent);
    Apply(submit);
  }
  auto Plan() {
    return detail::PlanClusterCreateStep(
        MetaCommittedView(stores_, index_),
        *stores_.operation_.FindOperation(root_), runtime_);
  }
  void AdvanceToDataWait() {
    for (int i = 0; i < 30; ++i) {
      auto next = Plan();
      ASSERT_TRUE(next.ok()) << next.status();
      if (!next->has_value()) return;
      Apply(std::move(**next));
      ASSERT_FALSE(HasFatalFailure());
    }
    FAIL() << "creation did not reach Data wait";
  }
  void PublishRuntime() {
    auto group = stores_.topology_.FindGroup("group-1");
    auto grant = stores_.grant_.GroupState("group-1");
    ASSERT_TRUE(group.has_value());
    ASSERT_TRUE(grant.has_value() && grant->grant_.has_value());
    MetaDataControlRuntimeNode node;
    node.node_id_ = std::string(40, '1');
    node.boot_id_ = std::string(40, '3');
    node.replication_history_id_.fill(4);
    node.source_meta_applied_index_ = index_ - 1;
    node.validated_committed_high_water_ = index_;
    node.groups_.push_back(
        {group->group_id_, group->members_.front().assignment_id_,
         group->record_.group_term_, group->record_.authority_version_,
         grant->grant_->grant_revision_,
         group->record_.population_manifest_revision_,
         group->record_.population_manifest_digest_,
         group->record_.partition_replication_epoch_});
    runtime_.leader_authority_eligible_ = true;
    runtime_.nodes_ = {node};
  }
  MetaOperationRecord Child() {
    for (const auto& op : stores_.operation_.LiveOperations())
      if (op.operation_id_ != root_) return op;
    ADD_FAILURE() << "population child missing";
    return {};
  }
  void IssueInitialization() {
    AdvanceToDataWait();
    ASSERT_FALSE(HasFatalFailure());
    PublishRuntime();
    for (int i = 0; i < 3; ++i) {
      auto next = Plan();
      ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
      Apply(std::move(**next));
      ASSERT_FALSE(HasFatalFailure());
    }
    ASSERT_EQ(Child().current_directives_.size(), 1);
  }
  void Result(MetaDirectiveResultStatus status) {
    const auto child = Child();
    ASSERT_EQ(child.current_directives_.size(), 1);
    const auto& directive = child.current_directives_.front();
    CommitDirectiveResult c;
    c.operation_id_ = child.operation_id_;
    c.directive_id_ = directive.spec_.directive_id_;
    c.attempt_id_ = directive.spec_.attempt_id_;
    c.directive_revision_ = directive.directive_revision_;
    c.recipient_node_id_ = directive.spec_.recipient_node_id_;
    c.recipient_boot_id_ = directive.spec_.target_boot_id_;
    c.assignment_id_ = directive.spec_.assignment_id_;
    c.status_ = status;
    c.result_ =
        status == MetaDirectiveResultStatus::kSucceeded ? "ready" : "failed";
    c.result_hash_ = MetaSha256(c.result_);
    Apply(c);
  }
  MetaStores stores_;
  std::uint64_t index_ = 0;
  MetaOperationId root_{};
  MetaDataControlRuntimeSnapshot runtime_;
};

TEST_F(ClusterCreateRecoveryTest, RestoresEveryPrefixAndWaitsWithoutData) {
  EXPECT_EQ(stores_.identity_.NodeCount(), 0);
  EXPECT_EQ(stores_.topology_.GroupCount(), 0);
  AdvanceToDataWait();
  ASSERT_FALSE(HasFatalFailure());
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->kind_phase_blob_,
            "wait-data-projection");
  auto next = Plan();
  ASSERT_TRUE(next.ok());
  EXPECT_FALSE(next->has_value());
  EXPECT_EQ(stores_.operation_.LiveOperations().size(), 1);
}

TEST_F(ClusterCreateRecoveryTest, DurableReservationRejectsASecondCreation) {
  const auto original = *stores_.operation_.FindOperation(root_);
  SubmitOperation second;
  second.operation_id_.fill(7);
  second.kind_ = kMetaClusterCreateOperationKind;
  second.intent_ = original.intent_;
  second.intent_hash_ = original.intent_hash_;
  auto result = ApplyCommitted(stores_, ++index_, second,
                               "keylane://operator/test", "now");
  EXPECT_EQ(result.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_EQ(stores_.identity_.NodeCount(), 0);
  EXPECT_EQ(stores_.operation_.LiveOperations().size(), 1);
  AdvanceToDataWait();
  ASSERT_FALSE(HasFatalFailure());
  // An exact original-id replay is still legal after the topology exists.
  second.operation_id_ = root_;
  Apply(second);
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->operation_seq_,
            original.operation_seq_);
}

TEST_F(ClusterCreateRecoveryTest,
       OldSourceIndexCannotBlockValidatedProjection) {
  AdvanceToDataWait();
  ASSERT_FALSE(HasFatalFailure());
  PublishRuntime();
  runtime_.nodes_.front().validated_committed_high_water_ = index_ - 1;
  auto next = Plan();
  ASSERT_TRUE(next.ok());
  EXPECT_FALSE(next->has_value());
  runtime_.nodes_.front().validated_committed_high_water_ = index_;
  next = Plan();
  ASSERT_TRUE(next.ok() && next->has_value());
  EXPECT_TRUE(std::holds_alternative<SubmitOperation>(**next));
}

TEST_F(ClusterCreateRecoveryTest, MissingResultNeverMintsAnotherAttempt) {
  IssueInitialization();
  ASSERT_FALSE(HasFatalFailure());
  const auto original = Child();
  for (int i = 0; i < 3; ++i) {
    auto next = Plan();
    ASSERT_TRUE(next.ok()) << next.status();
    EXPECT_FALSE(next->has_value());
    EXPECT_EQ(Child(), original);
  }
}

TEST_F(ClusterCreateRecoveryTest, CommittedSuccessFinishesWithoutLiveData) {
  IssueInitialization();
  ASSERT_FALSE(HasFatalFailure());
  Result(MetaDirectiveResultStatus::kSucceeded);
  ASSERT_FALSE(HasFatalFailure());
  runtime_ = {};  // A restart has no old session/ACK/health observation.
  for (int i = 0; i < 3; ++i) {
    auto next = Plan();
    ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
    EXPECT_FALSE(std::holds_alternative<SubmitOperation>(**next));
    Apply(std::move(**next));
    ASSERT_FALSE(HasFatalFailure());
  }
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kCompleted);
  EXPECT_EQ(Child().lifecycle_, MetaOperationLifecycle::kCompleted);
  EXPECT_TRUE(Child().current_directives_.empty());
  EXPECT_EQ(Child().terminal_receipts_.size(), 1);
}

TEST_F(ClusterCreateRecoveryTest, CommittedFailureFencesBeforeAbort) {
  IssueInitialization();
  ASSERT_FALSE(HasFatalFailure());
  Result(MetaDirectiveResultStatus::kFailed);
  for (int i = 0; i < 3; ++i) {
    auto next = Plan();
    ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
    if (i == 0) EXPECT_TRUE(std::holds_alternative<FenceGroup>(**next));
    Apply(std::move(**next));
    ASSERT_FALSE(HasFatalFailure());
  }
  EXPECT_TRUE(stores_.grant_.GroupState("group-1")->fenced_);
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kAborted);
}

TEST_F(ClusterCreateRecoveryTest, DataRestartCannotRetargetDestructiveIntent) {
  IssueInitialization();
  ASSERT_FALSE(HasFatalFailure());
  runtime_.nodes_.front().boot_id_ = std::string(40, '5');
  auto next = Plan();
  EXPECT_FALSE(next.ok());
  EXPECT_NE(next.status().message().find("Data restarted"),
            std::string_view::npos);
}

TEST_F(ClusterCreateRecoveryTest, ChangedPopulationCannotBeResetByRecovery) {
  IssueInitialization();
  ASSERT_FALSE(HasFatalFailure());
  auto group = stores_.topology_.FindGroup("group-1");
  SetGroupReplicationState c;
  c.group_id_ = "group-1";
  c.expected_population_manifest_revision_ =
      c.new_population_manifest_revision_ = 1;
  c.expected_population_manifest_digest_ = c.new_population_manifest_digest_ =
      group->record_.population_manifest_digest_;
  c.expected_partition_replication_epoch_ = 1;
  c.new_partition_replication_epoch_ = 2;
  c.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
  Apply(c);
  ASSERT_FALSE(HasFatalFailure());
  EXPECT_FALSE(Plan().ok());
}
}  // namespace
}  // namespace keylane::meta
