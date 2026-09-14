#include <array>
#include <cstdint>
#include <string>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/cluster_create.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/failover.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/state_apply.h"

namespace {

namespace meta = keylane::meta;

constexpr std::string_view kActor = "keylane://operator/failover-test";
constexpr std::string_view kTime = "2026-09-13T00:00:00Z";

template <std::size_t N>
std::array<std::uint8_t, N> Filled(std::uint8_t seed) {
  std::array<std::uint8_t, N> value{};
  value.fill(seed);
  return value;
}

std::string NodeId(std::uint8_t suffix) {
  std::string id(40, '0');
  constexpr char kHex[] = "0123456789abcdef";
  id[38] = kHex[(suffix >> 4) & 0x0f];
  id[39] = kHex[suffix & 0x0f];
  return id;
}

void ExpectAccepted(meta::MetaStores& stores, std::uint64_t index,
                    const meta::MetaCommand& command) {
  const meta::MetaApplyResult result =
      meta::ApplyCommitted(stores, index, command, kActor, kTime);
  EXPECT_EQ(result.verdict_, meta::MetaAuditVerdict::kAccepted)
      << result.detail_;
}

meta::MetaApplyResult ExpectRejected(meta::MetaStores& stores,
                                     std::uint64_t index,
                                     const meta::MetaCommand& command) {
  const meta::MetaApplyResult result =
      meta::ApplyCommitted(stores, index, command, kActor, kTime);
  EXPECT_EQ(result.verdict_, meta::MetaAuditVerdict::kRejected);
  EXPECT_FALSE(result.detail_.empty());
  return result;
}

std::string DomainBytes(const meta::MetaStores& stores) {
  std::string bytes = stores.identity_.Serialize();
  bytes += stores.topology_.Serialize();
  bytes += stores.policy_.Serialize();
  bytes += stores.grant_.Serialize().value_or("invalid-grant");
  bytes += stores.operation_.Serialize().value_or("invalid-operation");
  bytes += stores.population_manifest_.Serialize();
  return bytes;
}

meta::SubmitOperation ClusterCreateRoot() {
  meta::ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101", "tcp://127.0.0.1:7301",
                             "tcp://127.0.0.1:7201"}};
  manifest.data_nodes_ = {{NodeId(1), "tcp://127.0.0.1:6379"}};
  manifest.groups_ = {{"g1", NodeId(1), {}}};
  manifest.slot_ranges_ = {{0, 16383, "g1"}};

  meta::SubmitOperation root;
  root.request_id_ = Filled<16>(0x01);
  root.operation_id_ = Filled<16>(0x02);
  root.kind_ = std::string(meta::kMetaClusterCreateOperationKind);
  const auto intent =
      meta::EncodeClusterCreateRequest(manifest, root.operation_id_);
  EXPECT_TRUE(intent.ok()) << intent.status();
  root.intent_ = intent.value_or("");
  root.intent_hash_ = meta::MetaSha256(root.intent_);
  return root;
}

meta::SubmitOperation FailoverSubmit(std::uint8_t seed) {
  meta::SubmitOperation submit;
  submit.request_id_ = Filled<16>(seed);
  submit.operation_id_ = Filled<16>(static_cast<std::uint8_t>(seed + 1));
  submit.kind_ = "failover";
  const auto intent = meta::EncodeFailoverOperationIntent(
      {.group_id_ = "g1", .absolute_deadline_unix_ms_ = 1000});
  EXPECT_TRUE(intent.ok()) << intent.status();
  submit.intent_ = intent.value_or("");
  submit.intent_hash_ = meta::MetaSha256(submit.intent_);
  return submit;
}

struct ActivatedGroupFixture {
  meta::MetaStores stores;
  std::string owner = NodeId(1);
  meta::MetaAssignmentId owner_assignment = Filled<16>(0x31);
  meta::MetaGrantSpec grant{5000, "p", 0};
};

void PopulateActivatedGroup(ActivatedGroupFixture& fixture,
                            std::uint64_t first_index) {
  meta::RegisterNode node;
  node.request_id_ = Filled<16>(0x04);
  node.node_id_ = fixture.owner;
  node.principal_ = absl::StrCat("keylane://node/", fixture.owner);
  node.endpoints_ = {"tcp://127.0.0.1:6379"};
  node.role_ = meta::MetaNodeRole::kPrimary;
  ExpectAccepted(fixture.stores, first_index, meta::MetaCommand{node});

  meta::CreateGroup group;
  group.request_id_ = Filled<16>(0x05);
  group.group_id_ = "g1";
  group.new_topology_epoch_ = 1;
  ExpectAccepted(fixture.stores, first_index + 1, meta::MetaCommand{group});

  meta::AssignNodeToGroup assign;
  assign.request_id_ = Filled<16>(0x06);
  assign.group_id_ = "g1";
  assign.node_id_ = fixture.owner;
  assign.assignment_id_ = fixture.owner_assignment;
  assign.role_ = meta::MetaNodeRole::kPrimary;
  assign.expected_revision_ = 1;
  assign.new_topology_epoch_ = 2;
  ExpectAccepted(fixture.stores, first_index + 2, meta::MetaCommand{assign});

  meta::PutPolicy policy;
  policy.request_id_ = Filled<16>(0x07);
  policy.policy_id_ = fixture.grant.policy_id_;
  policy.version_ = fixture.grant.policy_version_;
  policy.content_ = R"({"lease_ms":5000})";
  policy.content_hash_ = meta::MetaPolicyStore::ContentHash(policy.content_);
  ExpectAccepted(fixture.stores, first_index + 3, meta::MetaCommand{policy});

  meta::BeginGroupTerm begin;
  begin.request_id_ = Filled<16>(0x08);
  begin.group_id_ = "g1";
  begin.expected_term_ = 0;
  begin.new_term_ = 1;
  ExpectAccepted(fixture.stores, first_index + 4, meta::MetaCommand{begin});

  meta::ActivateAuthority activate;
  activate.request_id_ = Filled<16>(0x09);
  activate.group_id_ = "g1";
  activate.expected_term_ = 1;
  activate.new_owner_ = fixture.owner;
  activate.grant_ = fixture.grant;
  activate.new_authority_version_ = 1;
  activate.new_topology_epoch_ = 3;
  activate.new_config_epoch_ = 1;
  ExpectAccepted(fixture.stores, first_index + 5, meta::MetaCommand{activate});
}

ActivatedGroupFixture MakeActivatedGroup() {
  ActivatedGroupFixture fixture;
  meta::SubmitOperation root = ClusterCreateRoot();
  ExpectAccepted(fixture.stores, 1, meta::MetaCommand{root});

  meta::CompleteOperation complete;
  complete.request_id_ = Filled<16>(0x03);
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  ExpectAccepted(fixture.stores, 2, meta::MetaCommand{complete});

  PopulateActivatedGroup(fixture, 3);
  return fixture;
}

meta::BeginUncontrolledFailover MakeBeginUncontrolled(
    const ActivatedGroupFixture& fixture) {
  meta::BeginUncontrolledFailover begin;
  begin.request_id_ = Filled<16>(0x0a);
  begin.group_id_ = "g1";
  begin.transition_id_ = Filled<16>(0x41);
  begin.target_term_ = 2;
  begin.successor_grant_ = fixture.grant;
  begin.expected_owner_node_id_ = fixture.owner;
  begin.expected_owner_assignment_id_ = fixture.owner_assignment;
  begin.expected_membership_revision_ = 2;
  begin.expected_group_term_ = 1;
  begin.expected_authority_version_ = 1;
  begin.expected_grant_revision_ = 8;
  begin.expected_population_manifest_revision_ = 0;
  begin.expected_population_manifest_digest_.fill(0);
  begin.expected_partition_replication_epoch_ = 0;
  begin.expected_config_epoch_ = 1;
  return begin;
}

meta::MetaOperationId InstallCurrentAuthorityDirective(
    ActivatedGroupFixture& fixture) {
  meta::SubmitOperation submit;
  submit.operation_id_ = Filled<16>(0x81);
  submit.kind_ = "test-rebuild";
  submit.intent_ = "rebuild-current-authority";
  submit.intent_hash_ = meta::MetaSha256(submit.intent_);
  EXPECT_TRUE(fixture.stores.operation_.SubmitOperation(submit, 80).ok());

  meta::MetaDirectiveSpec directive;
  directive.directive_id_ = Filled<16>(0x82);
  directive.attempt_id_ = Filled<16>(0x83);
  directive.recipient_node_id_ = fixture.owner;
  directive.target_node_id_ = fixture.owner;
  directive.target_boot_id_ = Filled<meta::kMetaBootIncarnationBytes>(0x84);
  directive.assignment_id_ = fixture.owner_assignment;
  directive.source_node_id_ = fixture.owner;
  directive.source_assignment_id_ = fixture.owner_assignment;
  directive.source_boot_id_ = directive.target_boot_id_;
  directive.source_replication_history_id_ =
      Filled<meta::kMetaReplicationHistoryIdBytes>(0x85);
  directive.group_id_ = "g1";
  directive.group_term_ = 1;
  directive.authority_version_ = 1;
  directive.grant_revision_ = 8;
  directive.kind_ = std::string(meta::kMetaDirectiveRebuild);
  directive.payload_ = *keylane::cluster::control::EncodeRebuildRequest({3});
  directive.storage_mutating_ = true;

  meta::TransitionOperationPhase phase;
  phase.operation_id_ = submit.operation_id_;
  phase.current_directives_ = {directive};
  EXPECT_TRUE(
      fixture.stores.operation_.TransitionOperationPhase(phase, 81).ok());
  return submit.operation_id_;
}

void InstallUncontrolledPostStateDirectly(ActivatedGroupFixture& fixture,
                                          std::uint64_t revision) {
  meta::BeginGroupTerm begin_term;
  begin_term.group_id_ = "g1";
  begin_term.expected_term_ = 1;
  begin_term.new_term_ = 2;
  ASSERT_TRUE(fixture.stores.grant_.BeginGroupTerm(begin_term).ok());
  ASSERT_TRUE(fixture.stores.topology_.SetGroupTerm("g1", 2).ok());

  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  meta::MetaFailoverTransition transition;
  transition.transition_id_ = begin.transition_id_;
  transition.mode_ = meta::MetaFailoverMode::kUncontrolled;
  transition.target_term_ = begin.target_term_;
  transition.successor_grant_ = begin.successor_grant_;
  ASSERT_TRUE(fixture.stores.topology_
                  .InstallFailoverTransition("g1", transition, revision)
                  .ok());
}

void ExpectAggregateRestoreFails(const meta::MetaStores& stores) {
  const auto bytes = stores.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const auto restored = meta::MetaStores::Deserialize(*bytes);
  ASSERT_FALSE(restored.ok());
  EXPECT_EQ(meta::MetaFailureClassOf(restored.status()),
            meta::MetaFailureClass::kFailStop);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledWithoutCandidateAtomicallyFencesAndPersists) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);

  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{begin});

  const auto group = fixture.stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.owner_, fixture.owner);
  EXPECT_EQ(group->record_.group_term_, 2u);
  EXPECT_EQ(group->record_.authority_version_, 1u);
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(group->failover_transition_->transition_id_, begin.transition_id_);
  EXPECT_EQ(group->failover_transition_->revision_, 9u);
  EXPECT_EQ(group->failover_transition_->mode_,
            meta::MetaFailoverMode::kUncontrolled);
  EXPECT_EQ(group->failover_transition_->target_term_, 2u);
  EXPECT_EQ(group->failover_transition_->successor_grant_, fixture.grant);
  EXPECT_FALSE(group->failover_transition_->candidate_action_.has_value());
  EXPECT_FALSE(group->failover_transition_->controlled_.has_value());
  EXPECT_EQ(group->revision_, 2u);
  EXPECT_EQ(group->config_epoch_, 1u);
  EXPECT_EQ(fixture.stores.topology_.TopologyEpoch(), 3u);

  const auto grant = fixture.stores.grant_.GroupState("g1");
  ASSERT_TRUE(grant.has_value());
  EXPECT_EQ(grant->group_term_, 2u);
  EXPECT_EQ(grant->last_authority_version_, 1u);
  EXPECT_EQ(grant->last_grant_revision_, 8u);
  EXPECT_TRUE(grant->fenced_);
  EXPECT_FALSE(grant->grant_.has_value());

  const auto serialized = fixture.stores.Serialize();
  ASSERT_TRUE(serialized.ok()) << serialized.status();
  const auto restored = meta::MetaStores::Deserialize(*serialized);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->topology_.FindGroup("g1"), group);

  // The exact committed entry is a post-state no-op: term and transition
  // revision do not advance a second time, and the audit record is unchanged.
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{begin});
  EXPECT_EQ(fixture.stores.topology_.FindGroup("g1"), group);
  EXPECT_EQ(fixture.stores.audit_.size(), 9u);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsStaleGrantAnchorWithoutMutation) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  begin.expected_grant_revision_ = 7;
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledAtomicallyInvalidatesOldAuthorityDirectives) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::MetaOperationId operation_id =
      InstallCurrentAuthorityDirective(fixture);
  ASSERT_EQ(fixture.stores.operation_.FindOperation(operation_id)
                ->current_directives_.size(),
            1u);

  ExpectAccepted(fixture.stores, 9,
                 meta::MetaCommand{MakeBeginUncontrolled(fixture)});

  const auto operation = fixture.stores.operation_.FindOperation(operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_TRUE(operation->current_directives_.empty());
  EXPECT_EQ(operation->revision_, 2u);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsOperationCleanupPartialStateWithoutRepair) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::MetaOperationId operation_id =
      InstallCurrentAuthorityDirective(fixture);
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  InstallUncontrolledPostStateDirectly(fixture, 9);
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  const auto operation = fixture.stores.operation_.FindOperation(operation_id);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->current_directives_.size(), 1u);
  EXPECT_EQ(operation->revision_, 1u);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledSamePayloadAtLaterIndexIsNotReplay) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  ExpectAccepted(fixture.stores, 9, meta::MetaCommand{begin});
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 10, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(
      fixture.stores.topology_.FindGroup("g1")->failover_transition_->revision_,
      9u);
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsGrantOnlyPartialStateWithoutRepair) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  meta::BeginGroupTerm partial;
  partial.group_id_ = "g1";
  partial.expected_term_ = 1;
  partial.new_term_ = 2;
  ASSERT_TRUE(fixture.stores.grant_.BeginGroupTerm(partial).ok());
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.topology_.FindGroup("g1")->record_.group_term_, 1u);
  EXPECT_FALSE(fixture.stores.topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRejectsTopologyOnlyPartialStateWithoutRepair) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  const meta::BeginUncontrolledFailover begin = MakeBeginUncontrolled(fixture);
  ASSERT_TRUE(fixture.stores.topology_.SetGroupTerm("g1", 2).ok());
  meta::MetaFailoverTransition partial;
  partial.transition_id_ = begin.transition_id_;
  partial.mode_ = meta::MetaFailoverMode::kUncontrolled;
  partial.target_term_ = begin.target_term_;
  partial.successor_grant_ = begin.successor_grant_;
  ASSERT_TRUE(
      fixture.stores.topology_.InstallFailoverTransition("g1", partial, 9)
          .ok());
  const std::string before = DomainBytes(fixture.stores);

  ExpectRejected(fixture.stores, 9, meta::MetaCommand{begin});

  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_EQ(fixture.stores.grant_.GroupState("g1")->group_term_, 1u);
  EXPECT_TRUE(fixture.stores.grant_.GroupState("g1")->grant_.has_value());
}

TEST(MetaFailoverTransitionApply,
     SuccessorPolicyCannotRetireWhileTransitionIsActive) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  ExpectAccepted(fixture.stores, 9,
                 meta::MetaCommand{MakeBeginUncontrolled(fixture)});
  meta::RetirePolicy retire;
  retire.request_id_ = Filled<16>(0x0b);
  retire.policy_id_ = fixture.grant.policy_id_;
  retire.version_ = fixture.grant.policy_version_;
  const std::string before = DomainBytes(fixture.stores);

  const meta::MetaApplyResult result =
      ExpectRejected(fixture.stores, 10, meta::MetaCommand{retire});

  EXPECT_NE(result.detail_.find("failover transition"), std::string::npos);
  EXPECT_EQ(DomainBytes(fixture.stores), before);
  EXPECT_TRUE(fixture.stores.policy_.IsVersionActive(
      fixture.grant.policy_id_, fixture.grant.policy_version_));
}

TEST(MetaFailoverTransitionApply,
     FailoverSubmitRequiresCreatedClusterLifecycle) {
  {
    meta::MetaStores uninitialized;
    ExpectRejected(uninitialized, 1, meta::MetaCommand{FailoverSubmit(0x51)});
    EXPECT_FALSE(uninitialized.operation_.OperationKnown(Filled<16>(0x52)));
  }

  {
    meta::MetaStores creating;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(creating, 1, meta::MetaCommand{root});
    ExpectRejected(creating, 2, meta::MetaCommand{FailoverSubmit(0x53)});
    EXPECT_FALSE(creating.operation_.OperationKnown(Filled<16>(0x54)));
  }

  {
    meta::MetaStores failed;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(failed, 1, meta::MetaCommand{root});
    meta::AbortOperation abort;
    abort.request_id_ = Filled<16>(0x55);
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ExpectAccepted(failed, 2, meta::MetaCommand{abort});
    ASSERT_EQ(failed.topology_.ClusterLifecycle().state_,
              meta::MetaClusterLifecycle::kProvisioningFailed);
    ExpectRejected(failed, 3, meta::MetaCommand{FailoverSubmit(0x56)});
    EXPECT_FALSE(failed.operation_.OperationKnown(Filled<16>(0x57)));
  }

  {
    meta::MetaStores created;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(created, 1, meta::MetaCommand{root});
    meta::CompleteOperation complete;
    complete.request_id_ = Filled<16>(0x58);
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ExpectAccepted(created, 2, meta::MetaCommand{complete});
    const meta::SubmitOperation failover = FailoverSubmit(0x59);
    ExpectAccepted(created, 3, meta::MetaCommand{failover});
    EXPECT_TRUE(created.operation_.OperationKnown(failover.operation_id_));
  }
}

TEST(MetaFailoverTransitionApply,
     ActiveTransitionBlocksOrdinaryAuthorityAndAnchorMutations) {
  auto expect_blocked = [](const meta::MetaCommand& command) {
    ActivatedGroupFixture fixture = MakeActivatedGroup();
    ExpectAccepted(fixture.stores, 9,
                   meta::MetaCommand{MakeBeginUncontrolled(fixture)});
    const std::string before = DomainBytes(fixture.stores);
    ExpectRejected(fixture.stores, 10, command);
    EXPECT_EQ(DomainBytes(fixture.stores), before);
  };

  meta::ActivateAuthority activate;
  activate.request_id_ = Filled<16>(0x61);
  activate.group_id_ = "g1";
  activate.expected_term_ = 2;
  activate.new_owner_ = NodeId(1);
  activate.grant_ = meta::MetaGrantSpec{5000, "p", 0};
  activate.new_authority_version_ = 2;
  activate.new_topology_epoch_ = 4;
  activate.new_config_epoch_ = 2;
  expect_blocked(meta::MetaCommand{activate});

  meta::RemoveNodeFromGroup remove;
  remove.request_id_ = Filled<16>(0x62);
  remove.group_id_ = "g1";
  remove.node_id_ = NodeId(1);
  remove.expected_revision_ = 2;
  remove.new_topology_epoch_ = 4;
  expect_blocked(meta::MetaCommand{remove});

  meta::SetGroupReplicationState replication;
  replication.request_id_ = Filled<16>(0x63);
  replication.group_id_ = "g1";
  replication.expected_population_manifest_revision_ = 0;
  replication.expected_population_manifest_digest_.fill(0);
  replication.new_population_manifest_revision_ = 0;
  replication.new_population_manifest_digest_.fill(0);
  replication.expected_partition_replication_epoch_ = 0;
  replication.new_partition_replication_epoch_ = 1;
  replication.new_topology_epoch_ = 4;
  expect_blocked(meta::MetaCommand{replication});

  meta::BeginGroupTerm begin_term;
  begin_term.request_id_ = Filled<16>(0x64);
  begin_term.group_id_ = "g1";
  begin_term.expected_term_ = 2;
  begin_term.new_term_ = 3;
  expect_blocked(meta::MetaCommand{begin_term});

  meta::SetSlotMap slots;
  slots.request_id_ = Filled<16>(0x65);
  slots.ranges_ = {{0, 16383, "g1"}};
  slots.new_topology_epoch_ = 4;
  slots.config_epochs_ = {{"g1", 2}};
  expect_blocked(meta::MetaCommand{slots});
}

TEST(MetaFailoverTransitionApply,
     BeginUncontrolledRequiresCreatedClusterLifecycle) {
  const meta::BeginUncontrolledFailover command =
      MakeBeginUncontrolled(MakeActivatedGroup());

  {
    meta::MetaStores uninitialized;
    ExpectRejected(uninitialized, 1, meta::MetaCommand{command});
  }
  {
    meta::MetaStores creating;
    ExpectAccepted(creating, 1, meta::MetaCommand{ClusterCreateRoot()});
    ExpectRejected(creating, 2, meta::MetaCommand{command});
  }
  {
    meta::MetaStores failed;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(failed, 1, meta::MetaCommand{root});
    meta::AbortOperation abort;
    abort.request_id_ = Filled<16>(0x71);
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ExpectAccepted(failed, 2, meta::MetaCommand{abort});
    ExpectRejected(failed, 3, meta::MetaCommand{command});
  }
}

TEST(MetaFailoverTransitionApply,
     AggregateRestoreAllowsTransitionOnlyInCreatedLifecycle) {
  {
    ActivatedGroupFixture uninitialized;
    PopulateActivatedGroup(uninitialized, 1);
    InstallUncontrolledPostStateDirectly(uninitialized, 7);
    ExpectAggregateRestoreFails(uninitialized.stores);
  }
  {
    ActivatedGroupFixture creating;
    ExpectAccepted(creating.stores, 1, meta::MetaCommand{ClusterCreateRoot()});
    PopulateActivatedGroup(creating, 2);
    InstallUncontrolledPostStateDirectly(creating, 8);
    ExpectAggregateRestoreFails(creating.stores);
  }
  {
    ActivatedGroupFixture failed;
    const meta::SubmitOperation root = ClusterCreateRoot();
    ExpectAccepted(failed.stores, 1, meta::MetaCommand{root});
    meta::AbortOperation abort;
    abort.request_id_ = Filled<16>(0x72);
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ExpectAccepted(failed.stores, 2, meta::MetaCommand{abort});
    PopulateActivatedGroup(failed, 3);
    InstallUncontrolledPostStateDirectly(failed, 9);
    ExpectAggregateRestoreFails(failed.stores);
  }
  {
    ActivatedGroupFixture created = MakeActivatedGroup();
    InstallUncontrolledPostStateDirectly(created, 9);
    const auto bytes = created.stores.Serialize();
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    const auto restored = meta::MetaStores::Deserialize(*bytes);
    ASSERT_TRUE(restored.ok()) << restored.status();
    EXPECT_EQ(restored->topology_.FindGroup("g1"),
              created.stores.topology_.FindGroup("g1"));
  }
}

TEST(MetaFailoverTransitionApply,
     AggregateRestoreRejectsTransitionWhoseHistoricalOwnerWasRemoved) {
  ActivatedGroupFixture fixture = MakeActivatedGroup();
  InstallUncontrolledPostStateDirectly(fixture, 9);

  meta::RemoveNodeFromGroup remove;
  remove.group_id_ = "g1";
  remove.node_id_ = fixture.owner;
  remove.expected_revision_ = 2;
  remove.new_topology_epoch_ = 4;
  ASSERT_TRUE(fixture.stores.topology_.Apply(remove).ok());

  ExpectAggregateRestoreFails(fixture.stores);
}

}  // namespace
