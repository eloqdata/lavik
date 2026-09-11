#include "keylane/replication_group.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "absl/status/status.h"

namespace {

std::vector<keylane::PopulationManifestEntry> FullPopulation(
    std::uint64_t epoch = 1) {
  std::vector<keylane::PopulationManifestEntry> entries;
  entries.reserve(keylane::kReplicationPartitionCount);
  for (std::uint32_t partition = 0;
       partition < keylane::kReplicationPartitionCount; ++partition) {
    entries.push_back({partition, epoch});
  }
  return entries;
}

const keylane::PopulationManifest& Manifest() {
  static const keylane::PopulationManifest manifest = [] {
    auto result = keylane::PopulationManifest::Create(FullPopulation());
    EXPECT_TRUE(result.ok()) << result.status();
    return std::move(*result);
  }();
  return manifest;
}

keylane::RebuildDirective DirectiveForManifest(
    const keylane::PopulationManifest& manifest,
    std::string group_id = "group-a", std::uint64_t term = 7,
    std::string attempt_id = "attempt-1", bool safe_source_active = true,
    std::uint32_t flow_count = 2) {
  return {
      .identity_ =
          {
              .group_id_ = std::move(group_id),
              .assignment_id_ = "assignment-1",
              .term_ = term,
              .directive_revision_ = 1,
              .authority_id_ = "authority-1",
              .source_node_id_ = "source-1",
              .source_assignment_id_ = "source-assignment-1",
              .source_boot_id_ = "source-boot-1",
              .source_history_id_ = "history-1",
              .target_node_id_ = "target-1",
              .target_boot_id_ = "target-boot-1",
              .operation_id_ = "operation-1",
              .directive_id_ = "directive-1",
              .attempt_id_ = std::move(attempt_id),
              .manifest_revision_ = 1,
              .manifest_id_ = manifest.id(),
              .partition_replication_epoch_ = 23,
          },
      .flow_count_ = flow_count,
      .safe_source_active_ = safe_source_active,
  };
}

keylane::RebuildDirective Directive(std::string group_id = "group-a",
                                    std::uint64_t term = 7,
                                    std::string attempt_id = "attempt-1",
                                    bool safe_source_active = true,
                                    std::uint32_t flow_count = 2) {
  return DirectiveForManifest(Manifest(), std::move(group_id), term,
                              std::move(attempt_id), safe_source_active,
                              flow_count);
}

void RecordCompleteManifestProof(keylane::ReplicationGroup& group,
                                 const keylane::RebuildIdentity& identity,
                                 const keylane::PopulationManifest& manifest) {
  for (std::uint32_t partition = 0;
       partition < keylane::kReplicationPartitionCount; ++partition) {
    const std::uint64_t target_local_epoch = partition + 1;
    ASSERT_TRUE(
        group.RecordPartitionReset(identity, partition, target_local_epoch)
            .ok());
    ASSERT_TRUE(
        group
            .RecordPartitionHandoff(identity, partition,
                                    manifest.logical_epochs()[partition],
                                    target_local_epoch)
            .ok());
  }
}

TEST(PopulationManifestTest,
     CanonicalizesSparseEntriesAndHasStableSha256Identity) {
  std::vector<keylane::PopulationManifestEntry> ascending{{42, 9},
                                                          {16'383, 11}};
  auto descending = ascending;
  std::reverse(descending.begin(), descending.end());

  auto first = keylane::PopulationManifest::Create(std::move(ascending));
  auto second = keylane::PopulationManifest::Create(std::move(descending));
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(second.ok()) << second.status();

  EXPECT_EQ(first->id(), second->id());
  EXPECT_EQ(first->id().Hex(),
            "16f307db5d0d887b9a020c5caf2ae46f65abd87f0dcf67a59943b32797d763de");
  EXPECT_EQ(first->logical_epochs()[0], 0);
  EXPECT_EQ(first->logical_epochs()[42], 9);
  EXPECT_EQ(first->logical_epochs()[16'383], 11);
}

TEST(PopulationManifestTest, SupportsEmptyAndFullDesiredPopulations) {
  auto empty = keylane::PopulationManifest::Create({});
  ASSERT_TRUE(empty.ok()) << empty.status();
  EXPECT_EQ(empty->id().Hex(),
            "4ab12ddad6a63f363ec2b647e875b799a284570e0ee038cb60bcceaf7300b2e0");
  EXPECT_TRUE(std::all_of(empty->logical_epochs().begin(),
                          empty->logical_epochs().end(),
                          [](std::uint64_t epoch) { return epoch == 0; }));

  auto full = keylane::PopulationManifest::Create(FullPopulation());
  ASSERT_TRUE(full.ok()) << full.status();
  EXPECT_EQ(full->id().Hex(),
            "9794b5fdf5b620bb895b544ecb9472797c225a1267b8b46a6602399d7dfe362b");
  EXPECT_TRUE(std::all_of(full->logical_epochs().begin(),
                          full->logical_epochs().end(),
                          [](std::uint64_t epoch) { return epoch == 1; }));
}

TEST(PopulationManifestTest, RejectsDuplicateAndInvalidEntries) {
  auto duplicate = FullPopulation();
  duplicate.back().partition_id_ =
      duplicate[duplicate.size() - 2].partition_id_;
  EXPECT_EQ(
      keylane::PopulationManifest::Create(std::move(duplicate)).status().code(),
      absl::StatusCode::kInvalidArgument);

  auto out_of_range = FullPopulation();
  out_of_range.back().partition_id_ = keylane::kReplicationPartitionCount;
  EXPECT_EQ(keylane::PopulationManifest::Create(std::move(out_of_range))
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);

  auto zero_epoch = FullPopulation();
  zero_epoch[42].logical_epoch_ = 0;
  EXPECT_EQ(keylane::PopulationManifest::Create(std::move(zero_epoch))
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ReplicationGroupTest, RequiresBootScopedSafeSourceAuthorization) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");

  auto missing_directive_id = Directive();
  missing_directive_id.identity_.directive_id_.clear();
  EXPECT_EQ(
      group.BeginRebuild(missing_directive_id, Manifest()).status().code(),
      absl::StatusCode::kInvalidArgument);

  auto missing_source_assignment = Directive();
  missing_source_assignment.identity_.source_assignment_id_.clear();
  EXPECT_EQ(
      group.BeginRebuild(missing_source_assignment, Manifest()).status().code(),
      absl::StatusCode::kInvalidArgument);

  auto missing_manifest_revision = Directive();
  missing_manifest_revision.identity_.manifest_revision_ = 0;
  EXPECT_EQ(
      group.BeginRebuild(missing_manifest_revision, Manifest()).status().code(),
      absl::StatusCode::kInvalidArgument);

  auto unsafe = Directive("group-a", 7, "attempt-1", false);
  EXPECT_EQ(group.BeginRebuild(unsafe, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kNotReady);

  auto wrong_boot = Directive();
  wrong_boot.identity_.target_boot_id_ = "previous-boot";
  EXPECT_EQ(group.BeginRebuild(wrong_boot, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);

  auto directive = Directive();
  auto authorization = group.BeginRebuild(directive, Manifest());
  ASSERT_TRUE(authorization.ok()) << authorization.status();
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kRebuilding);
}

TEST(ReplicationGroupTest, RejectsFlowLayoutOutsideTheWorkerDomain) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto oversized = Directive("group-a", 7, "attempt-1", true,
                             std::numeric_limits<std::uint32_t>::max());

  EXPECT_EQ(group.ValidateRebuild(oversized, Manifest()).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.BeginRebuild(oversized, Manifest()).status().code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kNotReady);
}

TEST(ReplicationGroupTest, ResetAuthorizationIsValidOnlyForTheActiveAttempt) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  auto first_authorization = group.BeginRebuild(first, Manifest());
  ASSERT_TRUE(first_authorization.ok()) << first_authorization.status();
  EXPECT_TRUE(group.ValidateResetAuthorization(*first_authorization).ok());

  keylane::ReplicationGroup other("target-1", "target-boot-1");
  auto wrong_attempt = Directive("group-a", 7, "attempt-other");
  auto wrong_authorization = other.BeginRebuild(wrong_attempt, Manifest());
  ASSERT_TRUE(wrong_authorization.ok()) << wrong_authorization.status();
  EXPECT_EQ(group.ValidateResetAuthorization(*wrong_authorization).code(),
            absl::StatusCode::kFailedPrecondition);

  ASSERT_TRUE(group.Abort(first.identity_).ok());
  auto second = Directive("group-a", 7, "attempt-2");
  auto second_authorization = group.BeginRebuild(second, Manifest());
  ASSERT_TRUE(second_authorization.ok()) << second_authorization.status();
  EXPECT_EQ(group.ValidateResetAuthorization(*first_authorization).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(group.ValidateResetAuthorization(*second_authorization).ok());

  ASSERT_TRUE(group.Abort(second.identity_).ok());
  EXPECT_EQ(group.ValidateResetAuthorization(*second_authorization).code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(ReplicationGroupTest, RejectsASecondGroupAssignmentForTheSameNode) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto second_group = Directive("group-b", 8, "attempt-2");
  EXPECT_EQ(group.BeginRebuild(second_group, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kNotReady);
}

TEST(ReplicationGroupTest, PublishesReadinessOnlyFromCompleteCurrentAttempt) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto directive = Directive();
  ASSERT_TRUE(group.BeginRebuild(directive, Manifest()).ok());

  RecordCompleteManifestProof(group, directive.identity_, Manifest());
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(directive.identity_).ok());
  EXPECT_EQ(group
                .RecordFlowCutVector(directive.identity_,
                                     std::vector<std::uint64_t>{101})
                .code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.MarkStoragePromoted(directive.identity_).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.PublishReady(directive.identity_).status().code(),
            absl::StatusCode::kFailedPrecondition);

  ASSERT_TRUE(group
                  .RecordFlowCutVector(directive.identity_,
                                       std::vector<std::uint64_t>{101, 202})
                  .ok());
  ASSERT_TRUE(group.MarkStoragePromoted(directive.identity_).ok());
  auto ready = group.PublishReady(directive.identity_);
  ASSERT_TRUE(ready.ok()) << ready.status();
  EXPECT_EQ(ready->identity(), directive.identity_);
  EXPECT_EQ(ready->identity().target_boot_id_, "target-boot-1");
  EXPECT_EQ(ready->identity().partition_replication_epoch_, 23U);
  EXPECT_EQ(std::vector<std::uint64_t>(ready->cut_vector().begin(),
                                       ready->cut_vector().end()),
            (std::vector<std::uint64_t>{101, 202}));
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kReady);

  auto wrong_population_epoch = directive.identity_;
  ++wrong_population_epoch.partition_replication_epoch_;
  EXPECT_EQ(group.PublishReady(wrong_population_epoch).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kReady);

  auto repeated = group.PublishReady(directive.identity_);
  ASSERT_TRUE(repeated.ok()) << repeated.status();
  EXPECT_TRUE(
      std::equal(repeated->cut_vector().begin(), repeated->cut_vector().end(),
                 ready->cut_vector().begin(), ready->cut_vector().end()));

  auto stale_retry = Directive("group-a", 7, "attempt-2");
  EXPECT_EQ(group.BeginRebuild(stale_retry, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kReady);
}

TEST(ReplicationGroupTest,
     EmptyPopulationPublishesReadinessWithoutASourceFlowCut) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto identity = Directive().identity_;
  identity.source_node_id_.clear();
  identity.source_assignment_id_.clear();
  identity.source_boot_id_.clear();
  identity.source_history_id_.clear();
  identity.target_history_id_ = "target-history-1";

  auto authorization = group.BeginEmptyPopulation(identity, Manifest());
  ASSERT_TRUE(authorization.ok()) << authorization.status();
  RecordCompleteManifestProof(group, identity, Manifest());
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(identity).ok());
  ASSERT_TRUE(group.MarkStoragePromoted(identity).ok());

  auto ready = group.PublishReady(identity);
  ASSERT_TRUE(ready.ok()) << ready.status();
  EXPECT_TRUE(ready->cut_vector().empty());
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kReady);
}

TEST(ReplicationGroupTest,
     RestartDropsEmptyPopulationProofAndRejectsThePriorBootDirective) {
  auto identity = Directive().identity_;
  identity.source_node_id_.clear();
  identity.source_assignment_id_.clear();
  identity.source_boot_id_.clear();
  identity.source_history_id_.clear();
  identity.target_history_id_ = "target-history-1";

  keylane::ReplicationGroup before_restart("target-1", "target-boot-1");
  ASSERT_TRUE(before_restart.BeginEmptyPopulation(identity, Manifest()).ok());
  RecordCompleteManifestProof(before_restart, identity, Manifest());
  ASSERT_TRUE(before_restart.MarkFunctionCatalogComplete(identity).ok());
  ASSERT_TRUE(before_restart.MarkStoragePromoted(identity).ok());
  ASSERT_TRUE(before_restart.PublishReady(identity).ok());

  keylane::ReplicationGroup restarted("target-1", "target-boot-2");
  EXPECT_EQ(restarted.state(), keylane::ReplicationGroupState::kNotReady);
  EXPECT_EQ(restarted.BeginEmptyPopulation(identity, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);

  identity.target_boot_id_ = "target-boot-2";
  EXPECT_TRUE(restarted.BeginEmptyPopulation(identity, Manifest()).ok());
  EXPECT_EQ(restarted.state(), keylane::ReplicationGroupState::kRebuilding);
}

TEST(ReplicationGroupTest, RejectsStaleIdentityAndAttemptReuse) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto second = Directive("group-a", 7, "attempt-2");
  ASSERT_TRUE(group.BeginRebuild(second, Manifest()).ok());
  EXPECT_EQ(group
                .RecordFlowCutVector(first.identity_,
                                     std::vector<std::uint64_t>{1, 2})
                .code(),
            absl::StatusCode::kFailedPrecondition);

  auto wrong_history = second.identity_;
  wrong_history.source_history_id_ = "stale-history";
  EXPECT_EQ(group.RecordPartitionReset(wrong_history, 0, 1).code(),
            absl::StatusCode::kFailedPrecondition);

  ASSERT_TRUE(group.Abort(second.identity_).ok());
  EXPECT_EQ(group.BeginRebuild(first, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(ReplicationGroupTest, RejectsTargetLocalEpochFromPreviousAttempt) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.RecordPartitionReset(first.identity_, 42, 701).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto second = Directive("group-a", 7, "attempt-2");
  ASSERT_TRUE(group.BeginRebuild(second, Manifest()).ok());
  ASSERT_TRUE(group.RecordPartitionReset(second.identity_, 42, 702).ok());
  EXPECT_EQ(group.RecordPartitionHandoff(second.identity_, 42, 1, 701).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(group.RecordPartitionHandoff(second.identity_, 42, 1, 702).ok());
}

TEST(ReplicationGroupTest, InvalidatePublishedProofRequiresFreshAttempt) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  const keylane::PopulationManifest manifest = Manifest();
  auto first = DirectiveForManifest(manifest);
  ASSERT_TRUE(group.BeginRebuild(first, manifest).ok());
  RecordCompleteManifestProof(group, first.identity_, manifest);
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(first.identity_).ok());
  ASSERT_TRUE(group
                  .RecordFlowCutVector(first.identity_,
                                       std::vector<std::uint64_t>{10, 11})
                  .ok());
  ASSERT_TRUE(group.MarkStoragePromoted(first.identity_).ok());
  ASSERT_TRUE(group.PublishReady(first.identity_).ok());

  EXPECT_TRUE(group.InvalidateProof(first.identity_).ok());
  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kNotReady);
  EXPECT_EQ(group.BeginRebuild(first, manifest).status().code(),
            absl::StatusCode::kFailedPrecondition);

  auto second = DirectiveForManifest(manifest, "group-a", 7, "attempt-2");
  EXPECT_TRUE(group.BeginRebuild(second, manifest).ok());
}

TEST(ReplicationGroupTest, RestartDropsProofAtEveryRebuildBoundary) {
  const keylane::PopulationManifest manifest = Manifest();
  auto directive = DirectiveForManifest(manifest);
  keylane::ReplicationGroup group("target-1", "target-boot-1");

  auto expect_restart_not_ready = [&] {
    keylane::ReplicationGroup restarted("target-1", "target-boot-2");
    EXPECT_EQ(restarted.state(), keylane::ReplicationGroupState::kNotReady);
    EXPECT_EQ(restarted.BeginRebuild(directive, manifest).status().code(),
              absl::StatusCode::kFailedPrecondition);

    auto fresh = directive;
    fresh.identity_.target_boot_id_ = "target-boot-2";
    EXPECT_TRUE(restarted.BeginRebuild(fresh, manifest).ok());
  };

  ASSERT_TRUE(group.BeginRebuild(directive, manifest).ok());
  expect_restart_not_ready();
  ASSERT_TRUE(group.RecordPartitionReset(directive.identity_, 0, 1).ok());
  expect_restart_not_ready();
  ASSERT_TRUE(group
                  .RecordPartitionHandoff(directive.identity_, 0,
                                          manifest.logical_epochs()[0], 1)
                  .ok());
  expect_restart_not_ready();

  RecordCompleteManifestProof(group, directive.identity_, manifest);
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(directive.identity_).ok());
  ASSERT_TRUE(group
                  .RecordFlowCutVector(directive.identity_,
                                       std::vector<std::uint64_t>{10, 11})
                  .ok());
  expect_restart_not_ready();
  ASSERT_TRUE(group.MarkStoragePromoted(directive.identity_).ok());
  expect_restart_not_ready();
  ASSERT_TRUE(group.PublishReady(directive.identity_).ok());
  expect_restart_not_ready();
}

TEST(ReplicationGroupTest,
     RejectsConflictingSameTermButAcceptsNewAuthorityTerm) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto conflicting = Directive("group-a", 7, "attempt-2");
  conflicting.identity_.source_history_id_ = "history-2";
  EXPECT_EQ(group.BeginRebuild(conflicting, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);

  conflicting.identity_.term_ = 8;
  conflicting.identity_.authority_id_ = "authority-2";
  ASSERT_TRUE(group.BeginRebuild(conflicting, Manifest()).ok());
}

TEST(ReplicationGroupTest,
     AcceptsOnlyMonotonicDirectiveRevisionsWithinAnAuthorityTerm) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto sparse_result = keylane::PopulationManifest::Create({{42, 9}});
  ASSERT_TRUE(sparse_result.ok()) << sparse_result.status();
  const keylane::PopulationManifest& sparse = *sparse_result;
  auto newer = DirectiveForManifest(sparse, "group-a", 7, "attempt-2");
  newer.identity_.directive_revision_ = 2;
  newer.identity_.source_node_id_ = "source-2";
  newer.identity_.source_assignment_id_ = "source-assignment-2";
  newer.identity_.source_boot_id_ = "source-boot-2";
  newer.identity_.source_history_id_ = "history-2";
  newer.identity_.operation_id_ = "operation-2";
  ASSERT_TRUE(group.BeginRebuild(newer, sparse).ok());
  ASSERT_TRUE(group.Abort(newer.identity_).ok());

  auto stale = Directive("group-a", 7, "attempt-3");
  EXPECT_EQ(group.BeginRebuild(stale, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);

  auto equal_but_conflicting = newer;
  equal_but_conflicting.identity_.attempt_id_ = "attempt-4";
  equal_but_conflicting.identity_.source_history_id_ = "history-3";
  EXPECT_EQ(group.BeginRebuild(equal_but_conflicting, sparse).status().code(),
            absl::StatusCode::kFailedPrecondition);

  auto equal_but_different_directive = newer;
  equal_but_different_directive.identity_.directive_id_ = "directive-2";
  equal_but_different_directive.identity_.attempt_id_ = "attempt-5";
  EXPECT_EQ(
      group.BeginRebuild(equal_but_different_directive, sparse).status().code(),
      absl::StatusCode::kFailedPrecondition);

  auto equal_but_different_manifest_revision = newer;
  equal_but_different_manifest_revision.identity_.attempt_id_ = "attempt-6";
  ++equal_but_different_manifest_revision.identity_.manifest_revision_;
  EXPECT_EQ(group.BeginRebuild(equal_but_different_manifest_revision, sparse)
                .status()
                .code(),
            absl::StatusCode::kFailedPrecondition);

  auto equal_but_different_population_epoch = newer;
  equal_but_different_population_epoch.identity_.attempt_id_ = "attempt-7";
  ++equal_but_different_population_epoch.identity_.partition_replication_epoch_;
  EXPECT_EQ(group.BeginRebuild(equal_but_different_population_epoch, sparse)
                .status()
                .code(),
            absl::StatusCode::kFailedPrecondition);

  auto equal_with_changed_flow_layout = newer;
  equal_with_changed_flow_layout.identity_.attempt_id_ = "attempt-8";
  equal_with_changed_flow_layout.flow_count_ = 3;
  EXPECT_EQ(group.BeginRebuild(equal_with_changed_flow_layout, sparse)
                .status()
                .code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(ReplicationGroupTest,
     ValidatesNewerActiveDirectiveWithoutRevokingOldAttempt) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  const keylane::PopulationManifest& first_manifest = Manifest();
  auto first = DirectiveForManifest(first_manifest);
  auto first_authorization = group.BeginRebuild(first, first_manifest);
  ASSERT_TRUE(first_authorization.ok()) << first_authorization.status();

  auto sparse_result = keylane::PopulationManifest::Create({{42, 9}});
  ASSERT_TRUE(sparse_result.ok()) << sparse_result.status();
  const keylane::PopulationManifest& sparse = *sparse_result;
  auto newer = DirectiveForManifest(sparse, "group-a", 7, "attempt-2");
  newer.identity_.directive_revision_ = 2;
  newer.identity_.source_node_id_ = "source-2";
  newer.identity_.source_assignment_id_ = "source-assignment-2";
  newer.identity_.source_boot_id_ = "source-boot-2";
  newer.identity_.source_history_id_ = "history-2";
  newer.identity_.operation_id_ = "operation-2";

  EXPECT_TRUE(group.ValidateRebuild(newer, sparse).ok());
  EXPECT_EQ(group.BeginRebuild(newer, sparse).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(group.ValidateResetAuthorization(*first_authorization).ok());

  auto stale = newer;
  stale.identity_.directive_revision_ = 1;
  EXPECT_EQ(group.ValidateRebuild(stale, sparse).code(),
            absl::StatusCode::kFailedPrecondition);

  ASSERT_TRUE(group.InvalidateProof(first.identity_).ok());
  auto newer_authorization = group.BeginRebuild(newer, sparse);
  ASSERT_TRUE(newer_authorization.ok()) << newer_authorization.status();
  EXPECT_EQ(group.ValidateResetAuthorization(*first_authorization).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(group.ValidateResetAuthorization(*newer_authorization).ok());
}

TEST(ReplicationGroupTest,
     RequiresResetAndEpochMatchingHandoffForEveryPhysicalPartition) {
  auto sparse_result =
      keylane::PopulationManifest::Create({{42, 9}, {16'383, 11}});
  ASSERT_TRUE(sparse_result.ok()) << sparse_result.status();
  const keylane::PopulationManifest& sparse = *sparse_result;
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto directive = DirectiveForManifest(sparse);
  ASSERT_TRUE(group.BeginRebuild(directive, sparse).ok());

  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 42, 9, 43).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group
                .RecordPartitionReset(directive.identity_,
                                      keylane::kReplicationPartitionCount, 1)
                .code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.RecordPartitionReset(directive.identity_, 42, 0).code(),
            absl::StatusCode::kInvalidArgument);
  ASSERT_TRUE(group.RecordPartitionReset(directive.identity_, 42, 43).ok());
  EXPECT_TRUE(group.RecordPartitionReset(directive.identity_, 42, 43).ok());
  EXPECT_EQ(group.RecordPartitionReset(directive.identity_, 42, 44).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 42, 9, 0).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 42, 9, 44).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 42, 8, 43).code(),
            absl::StatusCode::kFailedPrecondition);
  ASSERT_TRUE(
      group.RecordPartitionHandoff(directive.identity_, 42, 9, 43).ok());
  EXPECT_TRUE(
      group.RecordPartitionHandoff(directive.identity_, 42, 9, 43).ok());
  EXPECT_EQ(
      group.RecordPartitionHandoff(directive.identity_, 42, 10, 43).code(),
      absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 42, 9, 44).code(),
            absl::StatusCode::kFailedPrecondition);

  ASSERT_TRUE(group.RecordPartitionReset(directive.identity_, 0, 1).ok());
  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 0, 1, 1).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 0, 0, 0).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 0, 0, 2).code(),
            absl::StatusCode::kFailedPrecondition);
  ASSERT_TRUE(group.RecordPartitionHandoff(directive.identity_, 0, 0, 1).ok());
  EXPECT_TRUE(group.RecordPartitionHandoff(directive.identity_, 0, 0, 1).ok());

  for (std::uint32_t partition = 0;
       partition < keylane::kReplicationPartitionCount; ++partition) {
    const std::uint64_t target_local_epoch = partition + 1;
    ASSERT_TRUE(group
                    .RecordPartitionReset(directive.identity_, partition,
                                          target_local_epoch)
                    .ok());
    if (partition != keylane::kReplicationPartitionCount - 1) {
      ASSERT_TRUE(
          group
              .RecordPartitionHandoff(directive.identity_, partition,
                                      sparse.logical_epochs()[partition],
                                      target_local_epoch)
              .ok());
    }
  }
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(directive.identity_).ok());
  ASSERT_TRUE(group
                  .RecordFlowCutVector(directive.identity_,
                                       std::vector<std::uint64_t>{101, 202})
                  .ok());
  EXPECT_EQ(group.MarkStoragePromoted(directive.identity_).code(),
            absl::StatusCode::kFailedPrecondition);

  EXPECT_EQ(group
                .RecordPartitionHandoff(directive.identity_, 16'383, 0,
                                        keylane::kReplicationPartitionCount)
                .code(),
            absl::StatusCode::kFailedPrecondition);
  ASSERT_TRUE(group
                  .RecordPartitionHandoff(directive.identity_, 16'383, 11,
                                          keylane::kReplicationPartitionCount)
                  .ok());
  ASSERT_TRUE(group.MarkStoragePromoted(directive.identity_).ok());
}

TEST(ReplicationGroupTest, ConflictingFlowCutVectorCannotBeRewritten) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto directive = Directive();
  ASSERT_TRUE(group.BeginRebuild(directive, Manifest()).ok());
  ASSERT_TRUE(group
                  .RecordFlowCutVector(directive.identity_,
                                       std::vector<std::uint64_t>{101, 202})
                  .ok());
  EXPECT_TRUE(group
                  .RecordFlowCutVector(directive.identity_,
                                       std::vector<std::uint64_t>{101, 202})
                  .ok());
  EXPECT_EQ(group
                .RecordFlowCutVector(directive.identity_,
                                     std::vector<std::uint64_t>{101, 203})
                .code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group
                .RecordFlowCutVector(directive.identity_,
                                     std::vector<std::uint64_t>{101})
                .code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group
                .RecordFlowCutVector(directive.identity_,
                                     std::vector<std::uint64_t>{101, 0})
                .code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ReplicationGroupTest, FailureLatchPreventsEveryLaterAttempt) {
  keylane::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.FailStop(first.identity_).ok());

  EXPECT_EQ(group.state(), keylane::ReplicationGroupState::kFailedStopped);

  auto later = Directive("group-a", 8, "attempt-2");
  EXPECT_EQ(group.BeginRebuild(later, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.Abort(first.identity_).code(),
            absl::StatusCode::kFailedPrecondition);
}

}  // namespace
