/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include <array>
#include <atomic>
#include <thread>

#include "gtest/gtest.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_machine.h"

namespace lavik::meta {
namespace {
class MetaAdminViewsTest : public testing::Test {
 protected:
  void SetUp() override {
    auto opened = MetaStateMachine::Open("");
    ASSERT_TRUE(opened.ok());
    machine_ = std::move(*opened);
  }
  void Install(std::uint64_t index = 10) {
    const auto image = stores_.Serialize();
    ASSERT_TRUE(image.ok()) << image.status();
    ASSERT_TRUE(machine_->Install(index, *image).ok());
  }
  void Group(std::string id) {
    CreateGroup command;
    command.group_id_ = std::move(id);
    command.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
    ASSERT_TRUE(stores_.topology_.Apply(command).ok());
  }
  void Operation(std::uint8_t id, std::uint64_t seq) {
    SubmitOperation command;
    command.operation_id_.fill(id);
    command.kind_ = "maintenance";
    command.intent_ = std::string(1024, 'x');
    command.intent_hash_ = MetaSha256(command.intent_);
    ASSERT_TRUE(stores_.operation_.SubmitOperation(command, seq).ok());
  }
  MetaStores stores_;
  std::unique_ptr<MetaStateMachine> machine_;
};

TEST_F(MetaAdminViewsTest, GroupAndAbsoluteSlotsOwnTheirCutAcrossInstall) {
  Group("selected");
  Group("other");
  SetSlotMap slots;
  slots.new_topology_epoch_ = 3;
  slots.ranges_ = {{3, 7, "selected"}, {10, 12, "other"}};
  ASSERT_TRUE(stores_.topology_.Apply(slots).ok());
  Install();
  const auto group = machine_->CaptureAdminGroup("selected");
  const auto map = machine_->CaptureSlotMapCheck("selected");
  EXPECT_EQ(group.group_, stores_.topology_.FindGroup("selected"));
  EXPECT_EQ(group.authority_, stores_.topology_.AuthorityFor("selected"));
  EXPECT_EQ(group.lifecycle_, stores_.topology_.ClusterLifecycle());
  EXPECT_EQ(group.topology_epoch_, 3);
  EXPECT_EQ(group.cursor_.applied_index(), 10);
  EXPECT_EQ(group.cursor_.state_change_index(), 10);
  EXPECT_TRUE(map.group_exists_);
  for (std::uint32_t i = 0; i <= kMetaSlotCount; ++i) {
    const auto expected = stores_.topology_.SlotOwner(i);
    const auto actual = map.SlotOwner(i);
    EXPECT_EQ(actual.has_value(), expected.has_value());
    if (actual && expected) EXPECT_EQ(*actual, *expected);
  }
  machine_->Advance(11);
  const auto advanced = machine_->CaptureAdminGroup("selected");
  EXPECT_EQ(advanced.cursor_.applied_index(), 11);
  EXPECT_EQ(advanced.cursor_.state_change_index(), 10);
  stores_ = MetaStores();
  Install(12);
  EXPECT_FALSE(machine_->CaptureAdminGroup("selected").group_);
  EXPECT_FALSE(machine_->CaptureSlotMapCheck("selected").group_exists_);
  machine_.reset();
  ASSERT_TRUE(group.group_);
  EXPECT_EQ(group.group_->group_id_, "selected");
  EXPECT_EQ(map.SlotOwner(11), std::optional<std::string_view>("other"));
}

TEST_F(MetaAdminViewsTest, GroupAuthorityAndIndexStayPairedDuringCommits) {
  Group("g");
  Install();
  std::atomic<bool> done = false;
  std::thread writer([&] {
    for (std::uint64_t term = 1; term <= 100; ++term) {
      BeginGroupTerm command;
      command.request_id_.fill(static_cast<std::uint8_t>(term));
      command.group_id_ = "g";
      command.expected_term_ = term - 1;
      command.new_term_ = term;
      auto encoded = MetaStateMachine::EncodeCommand(command);
      EXPECT_TRUE(encoded.ok());
      if (!encoded.ok()) break;
      machine_->commit(10 + term, **encoded);
      std::this_thread::yield();
    }
    done.store(true);
  });
  do {
    const auto cut = machine_->CaptureAdminGroup("g");
    EXPECT_TRUE(cut.group_.has_value());
    EXPECT_TRUE(cut.authority_.has_value());
    if (cut.group_ && cut.authority_) {
      EXPECT_EQ(cut.group_->record_.group_term_, cut.authority_->group_term_);
      EXPECT_EQ(cut.group_->record_.group_term_,
                cut.cursor_.applied_index() - 10);
    }
  } while (!done.load());
  writer.join();
  EXPECT_EQ(machine_->CurrentGroupTerm("g"), 100);
  EXPECT_FALSE(machine_->CurrentGroupTerm("missing"));
}

TEST_F(MetaAdminViewsTest, OperationAndArchiveCutsRetainExactIdOrderAndBytes) {
  Operation(3, 1);
  Operation(1, 2);
  Operation(2, 3);
  MetaOperationId selected;
  selected.fill(3);
  Install();
  const auto live = machine_->CaptureOperationStatus(selected);
  ASSERT_TRUE(live.operation_);
  EXPECT_EQ(live.operation_, stores_.operation_.FindOperation(selected));
  EXPECT_FALSE(live.controlled_running_);
  for (auto id : {3, 1}) {
    CompleteOperation complete;
    complete.operation_id_.fill(id);
    complete.result_ = "retained result";
    ASSERT_TRUE(stores_.operation_.CompleteOperation(complete).ok());
  }
  ArchiveOperations archive;
  archive.operation_seqs_ = {1, 2};
  ASSERT_TRUE(stores_.operation_.ArchiveOperations(archive).ok());
  Install(11);
  const auto exported = machine_->CaptureOperationArchiveExport();
  ASSERT_EQ(exported.summaries_.size(), 2);
  EXPECT_EQ(exported.summaries_[0].operation_seq_, 2);
  EXPECT_EQ(exported.summaries_[1].operation_seq_, 1);
  EXPECT_EQ(exported.Encode(), stores_.operation_.ExportArchive());
  EXPECT_FALSE(machine_->CaptureOperationStatus(selected).operation_);
  EXPECT_TRUE(machine_->ArchivedOperationsExist(archive.operation_seqs_));
  const std::array<std::uint64_t, 2> partly_live{1, 3};
  EXPECT_FALSE(machine_->ArchivedOperationsExist(partly_live));
  PruneOperationArchive prune;
  prune.operation_seqs_ = {1};
  ASSERT_TRUE(stores_.operation_.PruneArchive(prune).ok());
  Install(12);
  EXPECT_FALSE(machine_->ArchivedOperationsExist(archive.operation_seqs_));
  machine_.reset();
  ASSERT_TRUE(exported.Encode().ok());
  EXPECT_EQ(DecodeMetaOperationArchiveExport(*exported.Encode())->summaries_,
            exported.summaries_);
  EXPECT_EQ(live.operation_->intent_, std::string(1024, 'x'));
}

TEST_F(MetaAdminViewsTest, AuditExportPreservesDroppedAndPrunedHistory) {
  stores_.audit_ = MetaAuditStore(3);
  for (std::uint64_t i = 1; i <= 6; ++i) {
    ASSERT_TRUE(stores_.audit_
                    .Append({i, "operator", "command",
                             MetaAuditVerdict::kAccepted, "", "time"})
                    .ok());
  }
  Install();
  const auto exported = machine_->CaptureAuditExport();
  const auto status = machine_->AuditStatus();
  EXPECT_EQ(exported.ExportThrough(6), stores_.audit_.ExportThrough(6));
  EXPECT_EQ(status.policy_, stores_.audit_.policy());
  EXPECT_EQ(status.size_, stores_.audit_.size());
  // Snapshot recovery restores the process capacity, not the fixture cap.
  EXPECT_EQ(status.capacity_, kMaxMetaAuditWindowRecords);
  EXPECT_EQ(status.dropped_total_, stores_.audit_.dropped_total());
  EXPECT_EQ(status.dropped_through_, stores_.audit_.dropped_through());
  const auto bytes = exported.ExportThrough(6);
  ASSERT_TRUE(stores_.audit_.PruneThrough(5).ok());
  Install(11);
  EXPECT_EQ(machine_->CaptureAuditExport().ExportThrough(6),
            stores_.audit_.ExportThrough(6));
  machine_.reset();
  EXPECT_EQ(exported.ExportThrough(6), bytes);
}

TEST_F(MetaAdminViewsTest, CurrentPolicyOwnsOnlyItsSelectedVersion) {
  PutPolicy policy;
  policy.policy_id_ = kAuthorityLeasePolicyId;
  policy.version_ = 1;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  ASSERT_TRUE(stores_.policy_.Apply(policy).ok());
  Install();
  const auto retained = machine_->CaptureCurrentPolicy(policy.policy_id_);
  EXPECT_EQ(retained.version_, 1);
  ASSERT_TRUE(retained.current_);
  EXPECT_EQ(retained.current_->content_, policy.content_);
  policy.version_ = 2;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":6000})";
  ASSERT_TRUE(stores_.policy_.Apply(policy).ok());
  Install(11);
  const auto current = machine_->CaptureCurrentPolicy(policy.policy_id_);
  EXPECT_EQ(current.version_, 2);
  EXPECT_EQ(current.current_->content_, policy.content_);
  EXPECT_FALSE(machine_->CaptureCurrentPolicy("missing").version_);
  machine_.reset();
  EXPECT_EQ(retained.current_->version_, 1);
}

TEST_F(MetaAdminViewsTest,
       CreatePreflightDetectsArtifactsAndOwnsMetaDirectory) {
  BindMetaMember bind;
  bind.server_id_ = 1;
  bind.principal_ = "lavik://meta/1";
  bind.data_control_endpoint_ = "127.0.0.1:7301";
  bind.ctl_endpoint_ = "127.0.0.1:7201";
  ASSERT_TRUE(stores_.identity_.Apply(bind).ok());
  Operation(1, 1);
  Install();
  const auto pristine = machine_->CaptureCreatePreflight();
  EXPECT_FALSE(pristine.data_artifacts_);
  EXPECT_FALSE(pristine.active_membership_);
  EXPECT_EQ(pristine.meta_members_, stores_.identity_.MetaMembers());
  Group("artifact");
  Install(11);
  EXPECT_TRUE(machine_->CaptureCreatePreflight().data_artifacts_);
  RetireMetaMember retire;
  retire.server_id_ = 1;
  ASSERT_TRUE(stores_.identity_.Apply(retire).ok());
  Install(12);
  const auto membership = machine_->CaptureMembershipAdmin();
  EXPECT_FALSE(membership.operation_);
  EXPECT_EQ(membership.identity_.Serialize(), stores_.identity_.Serialize());
  auto simulated = membership.identity_;
  EXPECT_FALSE(simulated.Apply(bind).ok());
  machine_.reset();
  ASSERT_EQ(pristine.meta_members_.size(), 1);
  EXPECT_FALSE(pristine.meta_members_[0].retired_);
}

TEST_F(MetaAdminViewsTest, PromoteRetainsCrossGroupFactsAndNoAuditDependency) {
  Group("target");
  Group("other");
  BeginGroupTerm term;
  term.group_id_ = "other";
  term.new_term_ = 1;
  ASSERT_TRUE(stores_.topology_.BeginGroupTerm(term).ok());
  Install();
  const auto view = machine_->CapturePromote("target");
  EXPECT_EQ(view.group_, stores_.topology_.FindGroup("target"));
  EXPECT_EQ(view.authority_, stores_.topology_.AuthorityFor("target"));
  EXPECT_EQ(view.facts_.CurrentGroupTerm("other"), 1);
  EXPECT_EQ(view.facts_.applied_index(), 10);
  BeginUncontrolledFailover begin;
  begin.group_id_ = "target";
  begin.trigger_reason_ = MetaAutomaticFailoverReason::kManual;
  MetaObservationStore observations;
  const auto proposal = machine_->CaptureProposal(begin);
  EXPECT_EQ(ValidateFailoverTransition(begin, view.group_, view.authority_,
                                       view.facts_, observations, 1000),
            ValidateFailoverProposal(begin, proposal, observations, 1000));
  EXPECT_FALSE(ValidateFailoverTransition(CreateGroup{}, view.group_,
                                          view.authority_, view.facts_,
                                          observations, 1000)
                   .ok());
  EXPECT_FALSE(view.facts_.AssignmentFor("target", "absent"));
  machine_.reset();
  EXPECT_EQ(view.facts_.CurrentGroupTerm("other"), 1);
}
}  // namespace
}  // namespace lavik::meta
