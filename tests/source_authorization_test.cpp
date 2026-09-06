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
              .source_boot_id_ = "source-boot-a",
              .source_history_id_ = "source-history-a",
              .target_node_id_ = std::move(target),
              .target_boot_id_ = "target-boot-a",
              .operation_id_ = std::move(operation),
              .attempt_id_ = std::move(attempt),
              .manifest_id_ = manifest,
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
  conflicting.identity_.authority_id_ = "authority-b";
  EXPECT_EQ(ledger.Authorize(conflicting).status().code(),
            absl::StatusCode::kFailedPrecondition);
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
