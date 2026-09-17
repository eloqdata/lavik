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

#include "lavik/replication_group.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "absl/status/status.h"

namespace {

std::vector<lavik::PopulationManifestEntry> FullPopulation(
    std::uint64_t epoch = 1) {
  std::vector<lavik::PopulationManifestEntry> entries;
  entries.reserve(lavik::kReplicationPartitionCount);
  for (std::uint32_t partition = 0;
       partition < lavik::kReplicationPartitionCount; ++partition) {
    entries.push_back({partition, epoch});
  }
  return entries;
}

const lavik::PopulationManifest& Manifest() {
  static const lavik::PopulationManifest manifest = [] {
    auto result = lavik::PopulationManifest::Create(FullPopulation());
    EXPECT_TRUE(result.ok()) << result.status();
    return std::move(*result);
  }();
  return manifest;
}

lavik::RebuildDirective DirectiveForManifest(
    const lavik::PopulationManifest& manifest, std::string group_id = "group-a",
    std::uint64_t term = 7, std::string attempt_id = "attempt-1",
    bool safe_source_active = true, std::uint32_t flow_count = 2) {
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

lavik::RebuildDirective Directive(std::string group_id = "group-a",
                                  std::uint64_t term = 7,
                                  std::string attempt_id = "attempt-1",
                                  bool safe_source_active = true,
                                  std::uint32_t flow_count = 2) {
  return DirectiveForManifest(Manifest(), std::move(group_id), term,
                              std::move(attempt_id), safe_source_active,
                              flow_count);
}

void RecordCompleteManifestProof(lavik::ReplicationGroup& group,
                                 const lavik::RebuildIdentity& identity,
                                 const lavik::PopulationManifest& manifest) {
  for (std::uint32_t partition = 0;
       partition < lavik::kReplicationPartitionCount; ++partition) {
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
  std::vector<lavik::PopulationManifestEntry> ascending{{42, 9}, {16'383, 11}};
  auto descending = ascending;
  std::reverse(descending.begin(), descending.end());

  auto first = lavik::PopulationManifest::Create(std::move(ascending));
  auto second = lavik::PopulationManifest::Create(std::move(descending));
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(second.ok()) << second.status();

  EXPECT_EQ(first->id(), second->id());
  EXPECT_EQ(first->id().Hex(),
            "ac123ed6be15da1fa4285c66c6a52e604651587efbcd1be588e518c0de3f13e9");
  EXPECT_EQ(first->logical_epochs()[0], 0);
  EXPECT_EQ(first->logical_epochs()[42], 9);
  EXPECT_EQ(first->logical_epochs()[16'383], 11);
}

TEST(ReplicationGroupTest,
     HistorySwitchPreservesReadyAndChangesTheWholeLayout) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto parent = Directive();
  ASSERT_TRUE(group.BeginRebuild(parent, Manifest()).ok());
  RecordCompleteManifestProof(group, parent.identity_, Manifest());
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(parent.identity_).ok());
  ASSERT_TRUE(group
                  .RecordFlowCutVector(parent.identity_,
                                       std::vector<std::uint64_t>{10, 20})
                  .ok());
  ASSERT_TRUE(group.MarkStoragePromoted(parent.identity_).ok());
  auto ready = group.PublishReady(parent.identity_);
  ASSERT_TRUE(ready.ok());
  auto child = Directive("group-a", 8, "history-switch", true, 1);
  child.identity_.source_node_id_ = "new-owner";
  child.identity_.source_history_id_ = "child-history";
  const std::vector<std::uint64_t> boundary{11, 21}, origin{1};
  EXPECT_FALSE(group
                   .SwitchHistory(*ready, child, Manifest(),
                                  std::vector<std::uint64_t>{11, 20}, boundary,
                                  origin)
                   .ok());
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);
  EXPECT_TRUE(group.PublishReady(parent.identity_).ok());
  EXPECT_FALSE(group
                   .SwitchHistory(*ready, child, Manifest(),
                                  std::vector<std::uint64_t>{12, 21}, boundary,
                                  origin)
                   .ok());
  auto changed = child;
  ++changed.identity_.partition_replication_epoch_;
  EXPECT_FALSE(group
                   .SwitchHistory(*ready, changed, Manifest(), boundary,
                                  boundary, origin)
                   .ok());
  auto switched = group.SwitchHistory(*ready, child, Manifest(), boundary,
                                      boundary, origin);
  ASSERT_TRUE(switched.ok()) << switched.status();
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);
  EXPECT_EQ(switched->identity(), child.identity_);
  EXPECT_EQ(std::vector<std::uint64_t>(switched->cut_vector().begin(),
                                       switched->cut_vector().end()),
            origin);
  EXPECT_TRUE(
      group.SwitchHistory(*ready, child, Manifest(), boundary, boundary, origin)
          .ok());
  EXPECT_FALSE(group.InvalidateProof(parent.identity_).ok());
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);
}

TEST(ReplicationGroupTest, FencedLocalSourceBindingPreservesPopulationAnchors) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto identity = Directive().identity_;
  identity.source_node_id_.clear();
  identity.source_assignment_id_.clear();
  identity.source_boot_id_.clear();
  identity.source_history_id_.clear();
  identity.target_history_id_ = "initial-source-history";
  ASSERT_TRUE(group.BeginEmptyPopulation(identity, Manifest()).ok());
  RecordCompleteManifestProof(group, identity, Manifest());
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(identity).ok());
  ASSERT_TRUE(group.MarkStoragePromoted(identity).ok());
  auto ready = group.PublishReady(identity);
  ASSERT_TRUE(ready.ok());
  auto source = Directive();
  source.identity_.directive_revision_ = 2;
  source.identity_.attempt_id_ = "local-source";
  source.identity_.source_node_id_ = identity.target_node_id_;
  source.identity_.source_boot_id_ = identity.target_boot_id_;
  source.identity_.source_assignment_id_ = identity.assignment_id_;
  source.identity_.source_history_id_ = "live-source-history";
  const std::vector<std::uint64_t> cut{31, 42};
  auto invalid = source;
  invalid.identity_.source_node_id_ = "another-node";
  EXPECT_FALSE(
      group.BindLocalSourceHistory(*ready, invalid, Manifest(), cut).ok());
  invalid = source;
  ++invalid.identity_.partition_replication_epoch_;
  EXPECT_FALSE(
      group.BindLocalSourceHistory(*ready, invalid, Manifest(), cut).ok());
  auto bound = group.BindLocalSourceHistory(*ready, source, Manifest(), cut);
  ASSERT_TRUE(bound.ok()) << bound.status();
  EXPECT_EQ(bound->identity(), source.identity_);
  EXPECT_EQ(std::vector<std::uint64_t>(bound->cut_vector().begin(),
                                       bound->cut_vector().end()),
            cut);
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);
  auto regressed = source;
  ++regressed.identity_.directive_revision_;
  regressed.identity_.attempt_id_ = "regression";
  EXPECT_FALSE(group
                   .BindLocalSourceHistory(*bound, regressed, Manifest(),
                                           std::vector<std::uint64_t>{30, 42})
                   .ok());
  EXPECT_TRUE(group.PublishReady(source.identity_).ok());
}

TEST(PopulationManifestTest, SupportsEmptyAndFullDesiredPopulations) {
  auto empty = lavik::PopulationManifest::Create({});
  ASSERT_TRUE(empty.ok()) << empty.status();
  EXPECT_EQ(empty->id().Hex(),
            "0d1983359e59607d7e214525019af31ef19ac3ac9dea82799f9c0339149339b9");
  EXPECT_TRUE(std::all_of(empty->logical_epochs().begin(),
                          empty->logical_epochs().end(),
                          [](std::uint64_t epoch) { return epoch == 0; }));

  auto full = lavik::PopulationManifest::Create(FullPopulation());
  ASSERT_TRUE(full.ok()) << full.status();
  EXPECT_EQ(full->id().Hex(),
            "6acaf90cfce776b60edaaa6f3d5513b9339433e6b6eab61fe14a8bc34057b430");
  EXPECT_TRUE(std::all_of(full->logical_epochs().begin(),
                          full->logical_epochs().end(),
                          [](std::uint64_t epoch) { return epoch == 1; }));
}

TEST(PopulationManifestTest, RejectsDuplicateAndInvalidEntries) {
  auto duplicate = FullPopulation();
  duplicate.back().partition_id_ =
      duplicate[duplicate.size() - 2].partition_id_;
  EXPECT_EQ(
      lavik::PopulationManifest::Create(std::move(duplicate)).status().code(),
      absl::StatusCode::kInvalidArgument);

  auto out_of_range = FullPopulation();
  out_of_range.back().partition_id_ = lavik::kReplicationPartitionCount;
  EXPECT_EQ(lavik::PopulationManifest::Create(std::move(out_of_range))
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);

  auto zero_epoch = FullPopulation();
  zero_epoch[42].logical_epoch_ = 0;
  EXPECT_EQ(
      lavik::PopulationManifest::Create(std::move(zero_epoch)).status().code(),
      absl::StatusCode::kInvalidArgument);
}

TEST(ReplicationGroupTest, RequiresBootScopedSafeSourceAuthorization) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");

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
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kNotReady);

  auto wrong_boot = Directive();
  wrong_boot.identity_.target_boot_id_ = "previous-boot";
  EXPECT_EQ(group.BeginRebuild(wrong_boot, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);

  auto directive = Directive();
  auto authorization = group.BeginRebuild(directive, Manifest());
  ASSERT_TRUE(authorization.ok()) << authorization.status();
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kRebuilding);
}

TEST(ReplicationGroupTest, RejectsFlowLayoutOutsideTheWorkerDomain) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto oversized = Directive("group-a", 7, "attempt-1", true,
                             std::numeric_limits<std::uint32_t>::max());

  EXPECT_EQ(group.ValidateRebuild(oversized, Manifest()).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.BeginRebuild(oversized, Manifest()).status().code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kNotReady);
}

TEST(ReplicationGroupTest, ResetAuthorizationIsValidOnlyForTheActiveAttempt) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  auto first_authorization = group.BeginRebuild(first, Manifest());
  ASSERT_TRUE(first_authorization.ok()) << first_authorization.status();
  EXPECT_TRUE(group.ValidateResetAuthorization(*first_authorization).ok());

  lavik::ReplicationGroup other("target-1", "target-boot-1");
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
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto second_group = Directive("group-b", 8, "attempt-2");
  EXPECT_EQ(group.BeginRebuild(second_group, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kNotReady);
}

TEST(ReplicationGroupTest, PublishesReadinessOnlyFromCompleteCurrentAttempt) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
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
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);

  auto wrong_population_epoch = directive.identity_;
  ++wrong_population_epoch.partition_replication_epoch_;
  EXPECT_EQ(group.PublishReady(wrong_population_epoch).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);

  auto repeated = group.PublishReady(directive.identity_);
  ASSERT_TRUE(repeated.ok()) << repeated.status();
  EXPECT_TRUE(
      std::equal(repeated->cut_vector().begin(), repeated->cut_vector().end(),
                 ready->cut_vector().begin(), ready->cut_vector().end()));

  auto stale_retry = Directive("group-a", 7, "attempt-2");
  EXPECT_EQ(group.BeginRebuild(stale_retry, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);
}

TEST(ReplicationGroupTest,
     EmptyPopulationPublishesReadinessWithoutASourceFlowCut) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
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
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kReady);
}

TEST(ReplicationGroupTest,
     RestartDropsEmptyPopulationProofAndRejectsThePriorBootDirective) {
  auto identity = Directive().identity_;
  identity.source_node_id_.clear();
  identity.source_assignment_id_.clear();
  identity.source_boot_id_.clear();
  identity.source_history_id_.clear();
  identity.target_history_id_ = "target-history-1";

  lavik::ReplicationGroup before_restart("target-1", "target-boot-1");
  ASSERT_TRUE(before_restart.BeginEmptyPopulation(identity, Manifest()).ok());
  RecordCompleteManifestProof(before_restart, identity, Manifest());
  ASSERT_TRUE(before_restart.MarkFunctionCatalogComplete(identity).ok());
  ASSERT_TRUE(before_restart.MarkStoragePromoted(identity).ok());
  ASSERT_TRUE(before_restart.PublishReady(identity).ok());

  lavik::ReplicationGroup restarted("target-1", "target-boot-2");
  EXPECT_EQ(restarted.state(), lavik::ReplicationGroupState::kNotReady);
  EXPECT_EQ(
      restarted.BeginEmptyPopulation(identity, Manifest()).status().code(),
      absl::StatusCode::kFailedPrecondition);

  identity.target_boot_id_ = "target-boot-2";
  EXPECT_TRUE(restarted.BeginEmptyPopulation(identity, Manifest()).ok());
  EXPECT_EQ(restarted.state(), lavik::ReplicationGroupState::kRebuilding);
}

TEST(ReplicationGroupTest, RejectsStaleIdentityAndAttemptReuse) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
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

TEST(ReplicationGroupTest,
     DerivesFreshRevisionFromWatermarkAfterProofInvalidation) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto initial = group.NextDirectiveRevision(7);
  ASSERT_TRUE(initial.ok()) << initial.status();
  EXPECT_EQ(*initial, 1);

  auto first = Directive();
  first.identity_.directive_revision_ = 41;
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto same_term = group.NextDirectiveRevision(7);
  ASSERT_TRUE(same_term.ok()) << same_term.status();
  EXPECT_EQ(*same_term, 42);
  auto newer_term = group.NextDirectiveRevision(8);
  ASSERT_TRUE(newer_term.ok()) << newer_term.status();
  EXPECT_EQ(*newer_term, 1);
  EXPECT_EQ(group.NextDirectiveRevision(6).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.NextDirectiveRevision(0).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ReplicationGroupTest, RejectsTargetLocalEpochFromPreviousAttempt) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
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
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  const lavik::PopulationManifest manifest = Manifest();
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
  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kNotReady);
  EXPECT_EQ(group.BeginRebuild(first, manifest).status().code(),
            absl::StatusCode::kFailedPrecondition);

  auto second = DirectiveForManifest(manifest, "group-a", 7, "attempt-2");
  EXPECT_TRUE(group.BeginRebuild(second, manifest).ok());
}

TEST(ReplicationGroupTest, RestartDropsProofAtEveryRebuildBoundary) {
  const lavik::PopulationManifest manifest = Manifest();
  auto directive = DirectiveForManifest(manifest);
  lavik::ReplicationGroup group("target-1", "target-boot-1");

  auto expect_restart_not_ready = [&] {
    lavik::ReplicationGroup restarted("target-1", "target-boot-2");
    EXPECT_EQ(restarted.state(), lavik::ReplicationGroupState::kNotReady);
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
  lavik::ReplicationGroup group("target-1", "target-boot-1");
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
     ReadyPopulationCarriesForwardAcrossAuthorityTermAdvance) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto rebuild = Directive("group-a", 6);
  ASSERT_TRUE(group.BeginRebuild(rebuild, Manifest()).ok());
  RecordCompleteManifestProof(group, rebuild.identity_, Manifest());
  ASSERT_TRUE(group.MarkFunctionCatalogComplete(rebuild.identity_).ok());
  ASSERT_TRUE(group
                  .RecordFlowCutVector(rebuild.identity_,
                                       std::vector<std::uint64_t>{10, 11})
                  .ok());
  ASSERT_TRUE(group.MarkStoragePromoted(rebuild.identity_).ok());
  auto ready = group.PublishReady(rebuild.identity_);
  ASSERT_TRUE(ready.ok()) << ready.status();

  EXPECT_FALSE(ready->CanCarryForwardToTerm(5));
  EXPECT_TRUE(ready->CanCarryForwardToTerm(6));
  EXPECT_TRUE(ready->CanCarryForwardToTerm(7));
  EXPECT_EQ(ready->identity().term_, 6U);
}

TEST(ReplicationGroupTest,
     AcceptsOnlyMonotonicDirectiveRevisionsWithinAnAuthorityTerm) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.Abort(first.identity_).ok());

  auto sparse_result = lavik::PopulationManifest::Create({{42, 9}});
  ASSERT_TRUE(sparse_result.ok()) << sparse_result.status();
  const lavik::PopulationManifest& sparse = *sparse_result;
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
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  const lavik::PopulationManifest& first_manifest = Manifest();
  auto first = DirectiveForManifest(first_manifest);
  auto first_authorization = group.BeginRebuild(first, first_manifest);
  ASSERT_TRUE(first_authorization.ok()) << first_authorization.status();

  auto sparse_result = lavik::PopulationManifest::Create({{42, 9}});
  ASSERT_TRUE(sparse_result.ok()) << sparse_result.status();
  const lavik::PopulationManifest& sparse = *sparse_result;
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
      lavik::PopulationManifest::Create({{42, 9}, {16'383, 11}});
  ASSERT_TRUE(sparse_result.ok()) << sparse_result.status();
  const lavik::PopulationManifest& sparse = *sparse_result;
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto directive = DirectiveForManifest(sparse);
  ASSERT_TRUE(group.BeginRebuild(directive, sparse).ok());

  EXPECT_EQ(group.RecordPartitionHandoff(directive.identity_, 42, 9, 43).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group
                .RecordPartitionReset(directive.identity_,
                                      lavik::kReplicationPartitionCount, 1)
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
       partition < lavik::kReplicationPartitionCount; ++partition) {
    const std::uint64_t target_local_epoch = partition + 1;
    ASSERT_TRUE(group
                    .RecordPartitionReset(directive.identity_, partition,
                                          target_local_epoch)
                    .ok());
    if (partition != lavik::kReplicationPartitionCount - 1) {
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
                                        lavik::kReplicationPartitionCount)
                .code(),
            absl::StatusCode::kFailedPrecondition);
  ASSERT_TRUE(group
                  .RecordPartitionHandoff(directive.identity_, 16'383, 11,
                                          lavik::kReplicationPartitionCount)
                  .ok());
  ASSERT_TRUE(group.MarkStoragePromoted(directive.identity_).ok());
}

TEST(ReplicationGroupTest, ConflictingFlowCutVectorCannotBeRewritten) {
  lavik::ReplicationGroup group("target-1", "target-boot-1");
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
  lavik::ReplicationGroup group("target-1", "target-boot-1");
  auto first = Directive();
  ASSERT_TRUE(group.BeginRebuild(first, Manifest()).ok());
  ASSERT_TRUE(group.FailStop(first.identity_).ok());

  EXPECT_EQ(group.state(), lavik::ReplicationGroupState::kFailedStopped);

  auto later = Directive("group-a", 8, "attempt-2");
  EXPECT_EQ(group.BeginRebuild(later, Manifest()).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(group.Abort(first.identity_).code(),
            absl::StatusCode::kFailedPrecondition);
}

}  // namespace
