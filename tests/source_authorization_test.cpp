#include "../src/replication/source_authorization.h"

#include <cstdint>
#include <string>

#include "gtest/gtest.h"

namespace {

keylane::RebuildDirective Directive(std::uint64_t term, std::uint64_t revision,
                                    std::string target, std::string operation,
                                    std::string attempt) {
  keylane::PopulationManifestId manifest;
  manifest.bytes_[0] = 1;
  return keylane::RebuildDirective{
      .identity_ =
          {
              .group_id_ = "group-a",
              .assignment_id_ = "assignment-a",
              .term_ = term,
              .directive_revision_ = revision,
              .authority_id_ = "authority-a",
              .source_node_id_ = "source-a",
              .source_assignment_id_ = "source-assignment-a",
              .source_boot_id_ = "source-boot-a",
              .source_history_id_ = "source-history-a",
              .target_node_id_ = std::move(target),
              .target_boot_id_ = "target-boot-a",
              .operation_id_ = std::move(operation),
              .directive_id_ = "directive-a",
              .attempt_id_ = std::move(attempt),
              .manifest_revision_ = 19,
              .manifest_id_ = manifest,
              .partition_replication_epoch_ = 23,
          },
      .flow_count_ = 4,
      .safe_source_active_ = true,
  };
}

TEST(SourceAuthorizationLedgerTest,
     SameRevisionAllowsMultipleTargetsUntilRevocation) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective first =
      Directive(7, 11, "target-a", "operation-a", "attempt-a");
  keylane::RebuildDirective second =
      Directive(7, 11, "target-b", "operation-b", "attempt-b");
  second.identity_.assignment_id_ = "assignment-b";
  second.identity_.authority_id_ = "authority-b";
  second.identity_.target_boot_id_ = "target-boot-b";

  auto first_result = ledger.Authorize(first);
  ASSERT_TRUE(first_result.ok()) << first_result.status();
  EXPECT_EQ(*first_result,
            keylane::detail::SourceAuthorizationAction::kAuthorized);
  auto second_result = ledger.Authorize(second);
  ASSERT_TRUE(second_result.ok()) << second_result.status();
  EXPECT_EQ(*second_result,
            keylane::detail::SourceAuthorizationAction::kAuthorized);
  EXPECT_TRUE(ledger.IsAuthorized(first.identity_));
  EXPECT_TRUE(ledger.IsAuthorized(second.identity_));

  auto replay = ledger.Authorize(first);
  ASSERT_TRUE(replay.ok()) << replay.status();
  EXPECT_EQ(*replay, keylane::detail::SourceAuthorizationAction::kAuthorized);
}

TEST(SourceAuthorizationLedgerTest,
     RevocationRejectsTheOldRevisionButAllowsANewerCompleteIdentity) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective first =
      Directive(7, 11, "target-a", "operation-a", "attempt-a");
  ASSERT_TRUE(ledger.Authorize(first).ok());

  ledger.RevokeAll();
  EXPECT_FALSE(ledger.IsAuthorized(first.identity_));
  EXPECT_EQ(ledger.Authorize(first).status().code(),
            absl::StatusCode::kFailedPrecondition);

  const keylane::RebuildDirective same_revision =
      Directive(7, 11, "target-b", "operation-b", "attempt-b");
  EXPECT_EQ(ledger.Authorize(same_revision).status().code(),
            absl::StatusCode::kFailedPrecondition);

  keylane::RebuildDirective rebound =
      Directive(7, 12, "target-a", "operation-a", "attempt-a");
  auto rebound_result = ledger.Authorize(rebound);
  ASSERT_TRUE(rebound_result.ok()) << rebound_result.status();
  EXPECT_EQ(*rebound_result,
            keylane::detail::SourceAuthorizationAction::kAuthorized);
  EXPECT_TRUE(ledger.IsAuthorized(rebound.identity_));
}

TEST(SourceAuthorizationLedgerTest,
     SessionCleanupAllowsCurrentRevisionReplayWithoutErasingARevokeFloor) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective first =
      Directive(7, 11, "target-a", "operation-a", "attempt-a");
  keylane::RebuildDirective sibling =
      Directive(7, 11, "target-b", "operation-b", "attempt-b");
  sibling.identity_.target_boot_id_ = "target-boot-b";
  ASSERT_TRUE(ledger.Authorize(first).ok());
  ASSERT_TRUE(ledger.Authorize(sibling).ok());

  ledger.ClearActiveForSessionReplacement();
  EXPECT_FALSE(ledger.IsAuthorized(first.identity_));
  EXPECT_FALSE(ledger.IsAuthorized(sibling.identity_));
  auto replay = ledger.Authorize(first);
  ASSERT_TRUE(replay.ok()) << replay.status();
  EXPECT_EQ(*replay, keylane::detail::SourceAuthorizationAction::kAuthorized);
  EXPECT_TRUE(ledger.IsAuthorized(first.identity_));

  // A committed revocation remains authoritative even if a later transport
  // session performs its ordinary cleanup before replaying its FDS.
  ledger.RevokeAll();
  ledger.ClearActiveForSessionReplacement();
  EXPECT_EQ(ledger.Authorize(first).status().code(),
            absl::StatusCode::kFailedPrecondition);

  keylane::RebuildDirective newer =
      Directive(7, 12, "target-a", "operation-a", "attempt-new");
  auto advanced = ledger.Authorize(newer);
  ASSERT_TRUE(advanced.ok()) << advanced.status();
  EXPECT_EQ(*advanced, keylane::detail::SourceAuthorizationAction::kAuthorized);
}

TEST(SourceAuthorizationLedgerTest,
     NewRevisionRequiresWholeSessionRevocationBeforeInstallation) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective first =
      Directive(7, 11, "target-a", "operation-a", "attempt-a");
  const keylane::RebuildDirective newer =
      Directive(7, 12, "target-b", "operation-b", "attempt-b");
  ASSERT_TRUE(ledger.Authorize(first).ok());

  auto advance = ledger.Authorize(newer);
  ASSERT_TRUE(advance.ok()) << advance.status();
  EXPECT_EQ(*advance, keylane::detail::SourceAuthorizationAction::kRevokeOlder);
  EXPECT_TRUE(ledger.IsAuthorized(first.identity_));
  EXPECT_FALSE(ledger.IsAuthorized(newer.identity_));

  ledger.RevokeAll();
  auto installed = ledger.Authorize(newer);
  ASSERT_TRUE(installed.ok()) << installed.status();
  EXPECT_EQ(*installed,
            keylane::detail::SourceAuthorizationAction::kAuthorized);
  EXPECT_FALSE(ledger.IsAuthorized(first.identity_));
  EXPECT_TRUE(ledger.IsAuthorized(newer.identity_));

  const keylane::RebuildDirective stale =
      Directive(7, 10, "target-c", "operation-c", "attempt-c");
  EXPECT_EQ(ledger.Authorize(stale).status().code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(SourceAuthorizationLedgerTest, SameRevisionRejectsConflictingSourceScope) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective first =
      Directive(7, 11, "target-a", "operation-a", "attempt-a");
  ASSERT_TRUE(ledger.Authorize(first).ok());

  keylane::RebuildDirective conflicting =
      Directive(7, 11, "target-b", "operation-b", "attempt-b");
  conflicting.identity_.source_boot_id_ = "source-boot-b";
  EXPECT_EQ(ledger.Authorize(conflicting).status().code(),
            absl::StatusCode::kFailedPrecondition);

  conflicting = Directive(7, 11, "target-b", "operation-b", "attempt-b");
  conflicting.identity_.source_assignment_id_ = "source-assignment-b";
  EXPECT_EQ(ledger.Authorize(conflicting).status().code(),
            absl::StatusCode::kFailedPrecondition);

  conflicting = Directive(7, 11, "target-b", "operation-b", "attempt-b");
  ++conflicting.identity_.manifest_revision_;
  EXPECT_EQ(ledger.Authorize(conflicting).status().code(),
            absl::StatusCode::kFailedPrecondition);

  conflicting = Directive(7, 11, "target-b", "operation-b", "attempt-b");
  ++conflicting.identity_.partition_replication_epoch_;
  EXPECT_EQ(ledger.Authorize(conflicting).status().code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(SourceAuthorizationLedgerTest,
     AuthorizationIsBoundToTheExactDirectiveIdentity) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective directive =
      Directive(7, 11, "target-a", "operation-a", "attempt-a");
  ASSERT_TRUE(ledger.Authorize(directive).ok());

  auto different_directive = directive.identity_;
  different_directive.directive_id_ = "directive-b";
  EXPECT_FALSE(ledger.IsAuthorized(different_directive));
  EXPECT_TRUE(ledger.IsAuthorized(directive.identity_));
}

TEST(SourceAuthorizationLedgerTest,
     SiblingAuthorizationMatchesTheTargetsRebuildIdentity) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective authorize =
      Directive(7, 11, "target-a", "operation-a", "authorize-attempt");
  ASSERT_TRUE(ledger.Authorize(authorize).ok());

  keylane::RebuildIdentity rebuild = authorize.identity_;
  rebuild.directive_id_ = "rebuild-directive";
  rebuild.attempt_id_ = "rebuild-attempt";
  EXPECT_FALSE(ledger.IsAuthorized(rebuild));
  EXPECT_TRUE(ledger.MatchesAuthorizedRebuild(rebuild, authorize.flow_count_,
                                              authorize.safe_source_active_));

  auto wrong_manifest_revision = rebuild;
  ++wrong_manifest_revision.manifest_revision_;
  EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(wrong_manifest_revision,
                                               authorize.flow_count_, true));
  auto wrong_population_epoch = rebuild;
  ++wrong_population_epoch.partition_replication_epoch_;
  EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(wrong_population_epoch,
                                               authorize.flow_count_, true));
  auto wrong_target = rebuild;
  wrong_target.target_node_id_ = "target-b";
  EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(wrong_target,
                                               authorize.flow_count_, true));
  auto wrong_operation = rebuild;
  wrong_operation.operation_id_ = "operation-b";
  EXPECT_TRUE(ledger.MatchesAuthorizedRebuild(wrong_operation,
                                              authorize.flow_count_, true));
  auto stale_source_incarnation = rebuild;
  stale_source_incarnation.source_assignment_id_ = "source-assignment-b";
  EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(stale_source_incarnation,
                                               authorize.flow_count_, true));
  EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(
      rebuild, authorize.flow_count_ + 1, true));
  EXPECT_FALSE(
      ledger.MatchesAuthorizedRebuild(rebuild, authorize.flow_count_, false));
}

TEST(SourceAuthorizationLedgerTest,
     NativeHandshakeExportScopeExcludesControlDeliveryIdentity) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective authorize =
      Directive(7, 11, "target-a", "operation-a", "authorize-attempt");
  ASSERT_TRUE(ledger.Authorize(authorize).ok());

  const auto expect_match = [&](auto mutate) {
    keylane::RebuildIdentity requested = authorize.identity_;
    mutate(requested);
    EXPECT_TRUE(ledger.MatchesAuthorizedRebuild(
        requested, authorize.flow_count_, authorize.safe_source_active_));
  };

  expect_match([](auto& identity) { ++identity.term_; });
  expect_match([](auto& identity) { ++identity.directive_revision_; });
  expect_match([](auto& identity) { identity.authority_id_ = "authority-b"; });
  expect_match([](auto& identity) { identity.operation_id_ = "operation-b"; });
  expect_match([](auto& identity) { identity.directive_id_ = "directive-b"; });
  expect_match([](auto& identity) { identity.attempt_id_ = "attempt-b"; });
}

TEST(SourceAuthorizationLedgerTest,
     NativeHandshakeExportScopeIncludesDataSessionIdentity) {
  keylane::detail::SourceAuthorizationLedger ledger;
  const keylane::RebuildDirective authorize =
      Directive(7, 11, "target-a", "operation-a", "authorize-attempt");
  ASSERT_TRUE(ledger.Authorize(authorize).ok());

  const auto expect_mismatch = [&](auto mutate) {
    keylane::RebuildIdentity requested = authorize.identity_;
    mutate(requested);
    EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(
        requested, authorize.flow_count_, authorize.safe_source_active_));
  };

  expect_mismatch([](auto& identity) { identity.group_id_ = "group-b"; });
  expect_mismatch(
      [](auto& identity) { identity.source_node_id_ = "source-b"; });
  expect_mismatch([](auto& identity) {
    identity.source_assignment_id_ = "source-assignment-b";
  });
  expect_mismatch(
      [](auto& identity) { identity.source_boot_id_ = "source-boot-b"; });
  expect_mismatch(
      [](auto& identity) { identity.source_history_id_ = "source-history-b"; });
  expect_mismatch(
      [](auto& identity) { identity.target_node_id_ = "target-b"; });
  expect_mismatch(
      [](auto& identity) { identity.assignment_id_ = "assignment-b"; });
  expect_mismatch(
      [](auto& identity) { identity.target_boot_id_ = "target-boot-b"; });
  expect_mismatch([](auto& identity) { ++identity.manifest_revision_; });
  expect_mismatch([](auto& identity) { ++identity.manifest_id_.bytes_[0]; });
  expect_mismatch(
      [](auto& identity) { ++identity.partition_replication_epoch_; });

  EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(authorize.identity_,
                                               authorize.flow_count_ + 1,
                                               authorize.safe_source_active_));
  EXPECT_FALSE(ledger.MatchesAuthorizedRebuild(authorize.identity_,
                                               authorize.flow_count_,
                                               !authorize.safe_source_active_));
}

TEST(SourceAuthorizationLedgerTest, EmptyRevocationIsAnIdempotentNoOp) {
  keylane::detail::SourceAuthorizationLedger ledger;
  ledger.RevokeAll();
  ledger.RevokeAll();

  const keylane::RebuildDirective first =
      Directive(99, 99, "target-a", "operation-a", "attempt-a");
  auto accepted = ledger.Authorize(first);
  ASSERT_TRUE(accepted.ok()) << accepted.status();
  EXPECT_EQ(*accepted, keylane::detail::SourceAuthorizationAction::kAuthorized);
  EXPECT_TRUE(ledger.IsAuthorized(first.identity_));
}

}  // namespace
