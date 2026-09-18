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

#include "lavik/cluster/meta_client.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/cluster/node_control.h"

namespace lavik::cluster {
namespace {

TEST(MetaControlEndpointTest, ParsesOnlyNumericUnambiguousEndpoints) {
  auto ipv4 = ParseNumericControlEndpoint("127.0.0.1:7100");
  ASSERT_TRUE(ipv4.ok()) << ipv4.status();
  EXPECT_EQ(ipv4->host_, "127.0.0.1");
  EXPECT_EQ(ipv4->port_, 7100);

  auto ipv6 = ParseNumericControlEndpoint("[2001:db8::1]:7200");
  ASSERT_TRUE(ipv6.ok()) << ipv6.status();
  EXPECT_EQ(ipv6->host_, "2001:db8::1");
  EXPECT_EQ(ipv6->port_, 7200);

  auto normalized_ipv6 =
      ParseNumericControlEndpoint("[2001:0db8:0:0:0:0:0:1]:7200");
  ASSERT_TRUE(normalized_ipv6.ok()) << normalized_ipv6.status();
  EXPECT_EQ(normalized_ipv6->host_, "2001:db8::1");

  EXPECT_FALSE(ParseNumericControlEndpoint("meta.example:7100").ok());
  EXPECT_FALSE(ParseNumericControlEndpoint("2001:db8::1:7100").ok());
  EXPECT_FALSE(ParseNumericControlEndpoint("127.0.0.1:0").ok());
  EXPECT_FALSE(ParseNumericControlEndpoint("127.0.0.1:65536").ok());
}

TEST(MetaControlIdentityTest, RequiresExactlyOneMatchingUriSan) {
  const std::vector<std::string> matching{"lavik://meta/7"};
  const std::vector<std::string> mismatching{"lavik://meta/8"};
  const std::vector<std::string> duplicate{"lavik://meta/7",
                                           "lavik://operator/a"};
  EXPECT_TRUE(ValidateUniqueControlPrincipal(matching, "lavik://meta/7").ok());
  EXPECT_EQ(ValidateUniqueControlPrincipal({}, "lavik://meta/7").code(),
            absl::StatusCode::kUnauthenticated);
  EXPECT_EQ(
      ValidateUniqueControlPrincipal(mismatching, "lavik://meta/7").code(),
      absl::StatusCode::kPermissionDenied);
  EXPECT_EQ(ValidateUniqueControlPrincipal(duplicate, "lavik://meta/7").code(),
            absl::StatusCode::kUnauthenticated);
}

TEST(MetaControlIdentityTest, LearnedDialTargetPinsItsExactPrincipal) {
  const MetaControlEndpoint learned{
      .host_ = "127.0.0.1",
      .port_ = 7107,
      .server_id_ = 7,
      .principal_ = "lavik://meta/7",
  };
  control::WireMetaEndpoint hello_member{
      .server_id = 7,
      .host = "127.0.0.1",
      .port = 7107,
      .principal = "lavik://meta/7",
  };
  EXPECT_TRUE(ValidateDialedMetaIdentity(learned, hello_member).ok());

  hello_member.principal = "lavik://meta/8";
  EXPECT_EQ(ValidateDialedMetaIdentity(learned, hello_member).code(),
            absl::StatusCode::kPermissionDenied);
  hello_member.principal = "lavik://meta/7";
  hello_member.server_id = 8;
  EXPECT_EQ(ValidateDialedMetaIdentity(learned, hello_member).code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(MetaControlIdentityTest, UnresolvedSeedBootstrapsFromServerHello) {
  const MetaControlEndpoint seed{
      .host_ = "127.0.0.1",
      .port_ = 7107,
  };
  const control::WireMetaEndpoint hello_member{
      .server_id = 7,
      .host = "127.0.0.1",
      .port = 7107,
      .principal = "lavik://meta/7",
  };
  EXPECT_TRUE(ValidateDialedMetaIdentity(seed, hello_member).ok());
}

TEST(MetaReconnectPolicyTest, RepeatedFailuresKeepRetryWithinBoundedJitter) {
  EXPECT_EQ(MetaReconnectPolicy::MaximumDelay(),
            std::chrono::milliseconds(120));
  EXPECT_EQ(MetaReconnectPolicy::Next(0), std::chrono::milliseconds(80));
  EXPECT_EQ(MetaReconnectPolicy::Next(20), std::chrono::milliseconds(100));
  EXPECT_EQ(MetaReconnectPolicy::Next(40), std::chrono::milliseconds(120));
  for (std::uint64_t attempt = 0; attempt < 1000; ++attempt) {
    const auto delay = MetaReconnectPolicy::Next(attempt * 7919);
    EXPECT_GE(delay, std::chrono::milliseconds(80));
    EXPECT_LE(delay, MetaReconnectPolicy::MaximumDelay());
  }
  EXPECT_EQ(MetaReconnectPolicy::Next(0), std::chrono::milliseconds(80));
}

TEST(MetaSessionRunResultTest,
     StopRacingAFailedConnectionIgnoresTheOperationalError) {
  const detail::MetaSessionRunResult result(
      absl::UnavailableError("connect failed"));
  EXPECT_EQ(result.report_status().code(), absl::StatusCode::kUnavailable);
  EXPECT_TRUE(result.shutdown_status().ok());
}

TEST(MetaSessionRunResultTest,
     StopAfterSessionReturnStillPropagatesCleanupFailure) {
  const detail::MetaSessionRunResult result(
      absl::UnavailableError("control transport closed"),
      absl::InternalError("source authorization cleanup failed"));
  EXPECT_EQ(result.report_status().code(), absl::StatusCode::kInternal);
  EXPECT_EQ(result.shutdown_status().code(), absl::StatusCode::kInternal);
}

TEST(MetaEndpointDirectoryTest,
     OrdersLeaderThenDirectoryThenSeedsWithoutDupes) {
  auto seed_a = ParseNumericControlEndpoint("127.0.0.1:7101");
  auto seed_b = ParseNumericControlEndpoint("127.0.0.1:7102");
  ASSERT_TRUE(seed_a.ok() && seed_b.ok());
  MetaEndpointDirectory directory({*seed_a, *seed_b});

  std::vector<control::WireMetaEndpoint> learned{
      {.server_id = 2,
       .host = "127.0.0.1",
       .port = 7102,
       .principal = "lavik://meta/2"},
      {.server_id = 3,
       .host = "127.0.0.1",
       .port = 7103,
       .principal = "lavik://meta/3"},
  };
  ASSERT_TRUE(directory.Update(learned, 3).ok());
  const auto candidates = directory.Candidates();
  ASSERT_EQ(candidates.size(), 4U);
  EXPECT_EQ(candidates[0].port_, 7103);
  EXPECT_EQ(candidates[1].port_, 7102);
  EXPECT_EQ(candidates[2].port_, 7101);
  EXPECT_EQ(candidates[3].port_, 7102);
  EXPECT_EQ(candidates[3].server_id_, 0u);
}

TEST(MetaEndpointDirectoryTest,
     KeepsUnresolvedSeedFallbackForLegitimateEndpointReuse) {
  auto seed = ParseNumericControlEndpoint("127.0.0.1:7101");
  ASSERT_TRUE(seed.ok()) << seed.status();
  MetaEndpointDirectory directory({*seed});
  const std::vector<control::WireMetaEndpoint> learned{
      {.server_id = 1,
       .host = "127.0.0.1",
       .port = 7101,
       .principal = "lavik://meta/1"},
  };
  ASSERT_TRUE(directory.Update(learned, 1).ok());
  const auto candidates = directory.Candidates();
  ASSERT_EQ(candidates.size(), 2u);
  EXPECT_EQ(candidates[0].server_id_, 1u);
  EXPECT_EQ(candidates[1].server_id_, 0u);
  EXPECT_EQ(candidates[0].host_, candidates[1].host_);
  EXPECT_EQ(candidates[0].port_, candidates[1].port_);
}

TEST(MetaEndpointDirectoryTest, FullStateRefreshPreservesOnlyLiveLeaderHint) {
  auto seed = ParseNumericControlEndpoint("127.0.0.1:7101");
  ASSERT_TRUE(seed.ok()) << seed.status();
  MetaEndpointDirectory directory({*seed});

  const std::vector<control::WireMetaEndpoint> initial{
      {.server_id = 2,
       .host = "127.0.0.1",
       .port = 7102,
       .principal = "lavik://meta/2"},
      {.server_id = 3,
       .host = "127.0.0.1",
       .port = 7103,
       .principal = "lavik://meta/3"},
  };
  ASSERT_TRUE(directory.Update(initial, 3).ok());
  const std::vector<control::WireMetaEndpoint> refreshed{
      {.server_id = 3,
       .host = "127.0.0.1",
       .port = 7103,
       .principal = "lavik://meta/3"},
      {.server_id = 4,
       .host = "127.0.0.1",
       .port = 7104,
       .principal = "lavik://meta/4"},
  };
  ASSERT_TRUE(directory.Refresh(refreshed).ok());
  EXPECT_EQ(directory.Candidates().front().server_id_, 3u);

  const std::vector<control::WireMetaEndpoint> without_old_leader{
      {.server_id = 4,
       .host = "127.0.0.1",
       .port = 7104,
       .principal = "lavik://meta/4"},
  };
  ASSERT_TRUE(directory.Refresh(without_old_leader).ok());
  EXPECT_EQ(directory.Candidates().front().server_id_, 4u);

  const auto before_invalid = directory.Candidates();
  const std::vector<control::WireMetaEndpoint> invalid{
      {.server_id = 5,
       .host = "meta.example",
       .port = 7105,
       .principal = "lavik://meta/5"}};
  EXPECT_EQ(directory.Refresh(invalid).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(directory.Candidates(), before_invalid);
}

TEST(MetaEndpointDirectoryTest, RejectsMissingOrMismatchedPrincipalBinding) {
  MetaEndpointDirectory directory({});
  const std::vector<control::WireMetaEndpoint> missing{
      {.server_id = 7, .host = "127.0.0.1", .port = 7107}};
  EXPECT_EQ(directory.Update(missing, 7).code(),
            absl::StatusCode::kInvalidArgument);

  const std::vector<control::WireMetaEndpoint> mismatched{
      {.server_id = 7,
       .host = "127.0.0.1",
       .port = 7107,
       .principal = "lavik://meta/8"}};
  EXPECT_EQ(directory.Update(mismatched, 7).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_TRUE(directory.Candidates().empty());
}

TEST(MetaLeaseChallengeRotationTest, VisitsEveryLocallyOwnedActiveGrant) {
  constexpr char kLocal[] = "1111111111111111111111111111111111111111";
  constexpr char kRemote[] = "2222222222222222222222222222222222222222";
  control::WireId128 assignment{};
  assignment[0] = 1;
  const std::vector<control::WireDesiredGroup> groups{
      {.group_id = "local-a",
       .owner_node_id = kLocal,
       .owner_assignment_id = assignment,
       .grant_active = true},
      {.group_id = "remote",
       .owner_node_id = kRemote,
       .owner_assignment_id = assignment,
       .grant_active = true},
      {.group_id = "local-b",
       .owner_node_id = kLocal,
       .owner_assignment_id = assignment,
       .grant_active = true},
      {.group_id = "fenced-local",
       .owner_node_id = kLocal,
       .owner_assignment_id = assignment,
       .grant_active = false},
  };
  MetaLeaseChallengeRotation rotation;
  EXPECT_EQ(rotation.Next(groups, kLocal), 0u);
  EXPECT_EQ(rotation.Next(groups, kLocal), 2u);
  EXPECT_EQ(rotation.Next(groups, kLocal), 0u);
}

TEST(MetaLeaseChallengeRotationTest, HandlesProjectionReplacementAndNoOwner) {
  constexpr char kLocal[] = "1111111111111111111111111111111111111111";
  control::WireId128 assignment{};
  assignment[0] = 1;
  MetaLeaseChallengeRotation rotation;
  std::vector<control::WireDesiredGroup> initial{
      {.group_id = "a",
       .owner_node_id = kLocal,
       .owner_assignment_id = assignment,
       .grant_active = true},
      {.group_id = "b",
       .owner_node_id = kLocal,
       .owner_assignment_id = assignment,
       .grant_active = true},
  };
  EXPECT_EQ(rotation.Next(initial, kLocal), 0u);

  std::vector<control::WireDesiredGroup> replacement{
      {.group_id = "only",
       .owner_node_id = kLocal,
       .owner_assignment_id = assignment,
       .grant_active = true},
  };
  EXPECT_EQ(rotation.Next(replacement, kLocal), 0u);
  replacement[0].grant_active = false;
  EXPECT_EQ(rotation.Next(replacement, kLocal), std::nullopt);
  EXPECT_TRUE(
      MetaLeaseChallengeRotation::IsCommittedOwner(replacement, kLocal));
  EXPECT_FALSE(MetaLeaseChallengeRotation::IsCommittedOwner(
      replacement, "2222222222222222222222222222222222222222"));
}

TEST(MetaLeaseGrantValidationTest, RequiresExactResolvedFdsDuration) {
  EXPECT_TRUE(detail::ValidateResolvedLeaseGrantDuration(5000, 5000).ok());
  EXPECT_EQ(detail::ValidateResolvedLeaseGrantDuration(300, 5000).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(detail::ValidateResolvedLeaseGrantDuration(6000, 5000).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(detail::ValidateResolvedLeaseGrantDuration(0, 0).code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaLeaseChallengeRotationTest,
     FencedUncontrolledHistoricalOwnerMayReportCandidateProgress) {
  constexpr char kLocal[] = "1111111111111111111111111111111111111111";
  control::WireId128 assignment{};
  assignment[0] = 1;
  control::WireId128 transition_id{};
  transition_id[0] = 2;
  control::WireDesiredGroup group{
      .group_id = "group-a",
      .members = {{.node_id = kLocal, .assignment_id = assignment}},
      .owner_node_id = kLocal,
      .owner_assignment_id = assignment,
      .group_term = 8,
      .grant_active = false,
      .failover_transition =
          control::WireFailoverTransition{
              .transition_id = transition_id,
              .revision = 9,
              .mode = control::WireFailoverMode::kUncontrolled,
              .target_term = 8,
          },
  };
  const std::array groups{group};

  // BeginUncontrolled keeps the old owner as durable topology history while
  // fencing its grant. That history must not suppress the ordinary candidate
  // branch: the former owner may be the only surviving/latest population.
  EXPECT_FALSE(MetaLeaseChallengeRotation::IsCommittedOwner(groups, kLocal));

  // The exception is deliberately transition-scoped. A controlled or
  // target-term-mismatched projection must continue suppressing an owner from
  // the candidate role, as must any projection carrying active authority.
  group.failover_transition->mode = control::WireFailoverMode::kControlled;
  EXPECT_TRUE(
      MetaLeaseChallengeRotation::IsCommittedOwner(std::array{group}, kLocal));
  group.failover_transition->mode = control::WireFailoverMode::kUncontrolled;
  group.failover_transition->target_term = 9;
  EXPECT_TRUE(
      MetaLeaseChallengeRotation::IsCommittedOwner(std::array{group}, kLocal));
  group.failover_transition->target_term = 8;
  group.grant_active = true;
  EXPECT_TRUE(
      MetaLeaseChallengeRotation::IsCommittedOwner(std::array{group}, kLocal));
}

TEST(MetaCandidateProjectionTest,
     FencedHistoricalOwnerUsesAuthenticatedSelfOriginLineage) {
  constexpr char kLocal[] = "1111111111111111111111111111111111111111";
  constexpr char kBoot[] = "2222222222222222222222222222222222222222";
  constexpr char kHistory[] = "3333333333333333333333333333333333333333";
  const control::WireId128 assignment = [] {
    control::WireId128 value{};
    value[0] = 1;
    return value;
  }();
  const control::WireHash256 manifest = control::WireHash256{1};
  control::WireDesiredGroup group{
      .group_id = "group-a",
      .members = {{.node_id = kLocal, .assignment_id = assignment}},
      .owner_node_id = kLocal,
      .owner_assignment_id = assignment,
      .group_term = 8,
      .grant_active = false,
      .manifest_revision = 4,
      .manifest_digest = manifest,
      .partition_replication_epoch = 6,
      .failover_transition =
          control::WireFailoverTransition{
              .transition_id = {},
              .revision = 9,
              .mode = control::WireFailoverMode::kUncontrolled,
              .target_term = 8,
          },
  };
  const PopulationReadiness readiness{
      .group_id_ = group.group_id,
      .assignment_id_ = AssignmentId::FromBytes(assignment),
      .group_term_ = group.group_term,
      .manifest_revision_ = group.manifest_revision,
      .manifest_digest_ = group.manifest_digest,
      .partition_replication_epoch_ = group.partition_replication_epoch,
  };
  RebuildIdentity ready{
      .group_id_ = group.group_id,
      .assignment_id_ = readiness.assignment_id_.ToHexString(),
      .term_ = 7,
      .target_node_id_ = kLocal,
      .target_boot_id_ = kBoot,
      .manifest_revision_ = group.manifest_revision,
      .manifest_id_ = PopulationManifestId{.bytes_ = manifest},
      .partition_replication_epoch_ = group.partition_replication_epoch,
  };
  const ReplicationIdentity current{
      .local_node_id_ = kLocal,
      .boot_id_ = kBoot,
      .local_history_id_ = kHistory,
  };
  const std::array<std::uint64_t, 2> frontier{10, 20};

  auto projected = detail::ProjectReplicaCandidateProgress(
      std::array{group}, kLocal, current, readiness, ready, frontier,
      /*failover_candidate_eligible=*/true);
  ASSERT_TRUE(projected.ok()) << projected.status();
  ASSERT_TRUE(projected->has_value());
  EXPECT_EQ((*projected)->group_term, 8u);
  EXPECT_EQ((*projected)->source_group_term, 7u);
  EXPECT_EQ((*projected)->source_node_id, kLocal);
  EXPECT_EQ((*projected)->source_assignment_id, assignment);
  EXPECT_EQ((*projected)->source_boot_id, kBoot);
  EXPECT_EQ((*projected)->source_history_id, kHistory);
  EXPECT_EQ((*projected)->applied_next_lsns,
            (std::vector<std::uint64_t>{10, 20}));

  projected = detail::ProjectReplicaCandidateProgress(
      std::array{group}, kLocal, current, readiness, ready, frontier,
      /*failover_candidate_eligible=*/false);
  ASSERT_TRUE(projected.ok()) << projected.status();
  EXPECT_FALSE(projected->has_value());

  group.failover_transition->mode = control::WireFailoverMode::kControlled;
  projected = detail::ProjectReplicaCandidateProgress(
      std::array{group}, kLocal, current, readiness, ready, frontier,
      /*failover_candidate_eligible=*/true);
  ASSERT_TRUE(projected.ok()) << projected.status();
  EXPECT_FALSE(projected->has_value());
}

TEST(MetaHeartbeatProjectionGateTest,
     HoldsProducerAndConsumesOnlyTheOldOutstandingAck) {
  detail::MetaHeartbeatProjectionGate gate;
  EXPECT_FALSE(gate.pause_requested());
  EXPECT_FALSE(gate.quiesced());

  gate.RequestPause(41);
  EXPECT_TRUE(gate.pause_requested());
  gate.MarkQuiesced(true);
  EXPECT_TRUE(gate.quiesced());
  EXPECT_FALSE(gate.ConsumeSupersededAck(42));
  EXPECT_TRUE(gate.ConsumeSupersededAck(41));
  EXPECT_FALSE(gate.ConsumeSupersededAck(41));

  // Local FDS cleanup may span arbitrarily many heartbeat intervals. Only an
  // explicit post-apply resume reopens projection reads; quiescence itself
  // never does so.
  EXPECT_TRUE(gate.pause_requested());
  gate.Resume();
  gate.MarkQuiesced(false);
  EXPECT_FALSE(gate.pause_requested());
  EXPECT_FALSE(gate.quiesced());
}

TEST(MetaInboundTransferAbortTest,
     SupersededFullStateKeepsTheAuthenticatedSessionAndHeartbeat) {
  const control::TransferAbort superseded{
      .object_id = {},
      .reason = control::TransferAbortReason::kFullDesiredStateSuperseded,
  };

  EXPECT_EQ(
      detail::ClassifyMetaTransferAbort(
          superseded, control::TransferKind::kFullDesiredState),
      detail::MetaTransferAbortDisposition::kContinueAuthenticatedSession);

  // The reason is object-specific. It must not turn an interrupted Directive
  // into a session-local retry, and every unknown reason stays fail closed.
  EXPECT_EQ(detail::ClassifyMetaTransferAbort(
                superseded, control::TransferKind::kDirectivePayload),
            detail::MetaTransferAbortDisposition::kFailSession);
  control::TransferAbort unknown = superseded;
  unknown.reason = static_cast<control::TransferAbortReason>(99);
  EXPECT_EQ(detail::ClassifyMetaTransferAbort(
                unknown, control::TransferKind::kFullDesiredState),
            detail::MetaTransferAbortDisposition::kFailSession);
}

TEST(MetaDirectiveValidationTest,
     RequiresCurrentLocalAnchorsAndKindSpecificRecipient) {
  constexpr char kLocal[] = "1111111111111111111111111111111111111111";
  constexpr char kLocalBoot[] = "2222222222222222222222222222222222222222";
  constexpr char kRemote[] = "3333333333333333333333333333333333333333";
  constexpr char kRemoteBoot[] = "4444444444444444444444444444444444444444";
  control::WireProjectedDirective projected{
      .authority = {.group_id = "group-a",
                    .assignment_id = {},
                    .group_term = 3},
      .identity = {.operation_id = {},
                   .directive_id = {},
                   .attempt_id = {},
                   .directive_revision = 6},
      .recipient_node_id = kLocal,
      .recipient_boot_id = kLocalBoot,
      .target_node_id = kLocal,
      .target_boot_id = kLocalBoot,
      .source_node_id = kRemote,
      .source_assignment_id = {},
      .source_boot_id = kRemoteBoot,
      .source_replication_history_id = kRemote,
      .manifest_revision = 7,
      .manifest_digest = control::WireHash256{1},
      .partition_replication_epoch = 10,
      .kind = control::WireDirectiveKind::kRebuild,
      .payload = "payload",

  };
  projected.authority.assignment_id[0] = 1;
  projected.identity.operation_id[0] = 2;
  projected.identity.directive_id[0] = 3;
  projected.identity.attempt_id[0] = 4;
  projected.source_assignment_id[0] = 9;
  const auto live = [&] {
    return control::Directive{
        .session_id = {},
        .basis = {.control_revision = 9},
        .authority = projected.authority,
        .identity = projected.identity,
        .recipient_node_id = projected.recipient_node_id,
        .recipient_boot_id = projected.recipient_boot_id,
        .target_node_id = projected.target_node_id,
        .target_boot_id = projected.target_boot_id,
        .source_node_id = projected.source_node_id,
        .source_assignment_id = projected.source_assignment_id,
        .source_boot_id = projected.source_boot_id,
        .source_replication_history_id =
            projected.source_replication_history_id,
        .manifest_revision = projected.manifest_revision,
        .manifest_digest = projected.manifest_digest,
        .partition_replication_epoch = projected.partition_replication_epoch,
        .kind = projected.kind,
        .payload = projected.payload,

    };
  }();
  control::FullDesiredState desired;
  desired.control_revision = 9;
  control::WireId128 remote_assignment{};
  remote_assignment[0] = 9;
  desired.groups.push_back({
      .group_id = "group-a",
      .members = {{.node_id = kLocal,
                   .assignment_id = projected.authority.assignment_id},
                  {.node_id = kRemote, .assignment_id = remote_assignment}},
      .owner_node_id = kRemote,
      .owner_assignment_id = remote_assignment,
      .group_term = projected.authority.group_term,
      .partition_replication_epoch = projected.partition_replication_epoch,
  });
  desired.current_directives.push_back(projected);
  EXPECT_TRUE(ValidateLiveDirective(
                  live, control::SelectNodeControlState(desired, kLocal),
                  kLocal, kLocalBoot)
                  .ok());

  auto stale_basis = live;
  --stale_basis.basis.control_revision;
  EXPECT_EQ(ValidateLiveDirective(
                stale_basis, control::SelectNodeControlState(desired, kLocal),
                kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kFailedPrecondition);

  control::Directive stale_population = live;
  --stale_population.partition_replication_epoch;
  EXPECT_EQ(
      ValidateLiveDirective(stale_population,
                            control::SelectNodeControlState(desired, kLocal),
                            kLocal, kLocalBoot)
          .code(),
      absl::StatusCode::kFailedPrecondition);

  // Even if a stale envelope exactly matches a projected directive, the
  // installed group's current population epoch remains authoritative.
  desired.current_directives.front().partition_replication_epoch =
      stale_population.partition_replication_epoch;
  EXPECT_EQ(
      ValidateLiveDirective(stale_population,
                            control::SelectNodeControlState(desired, kLocal),
                            kLocal, kLocalBoot)
          .code(),
      absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().partition_replication_epoch =
      projected.partition_replication_epoch;

  control::Directive changed = live;
  changed.kind = control::WireDirectiveKind::kRevokeSources;
  EXPECT_EQ(ValidateLiveDirective(
                changed, control::SelectNodeControlState(desired, kLocal),
                kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kFailedPrecondition);

  control::Directive authorize = live;
  authorize.kind = control::WireDirectiveKind::kAuthorizeSource;
  authorize.recipient_node_id = kLocal;
  authorize.recipient_boot_id = kLocalBoot;
  authorize.source_node_id = kRemote;
  authorize.source_boot_id = kRemoteBoot;
  desired.current_directives.front().kind = authorize.kind;
  desired.current_directives.front().recipient_node_id =
      authorize.recipient_node_id;
  desired.current_directives.front().recipient_boot_id =
      authorize.recipient_boot_id;
  desired.current_directives.front().source_node_id = authorize.source_node_id;
  desired.current_directives.front().source_boot_id = authorize.source_boot_id;
  EXPECT_EQ(ValidateLiveDirective(
                authorize, control::SelectNodeControlState(desired, kLocal),
                kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kFailedPrecondition);

  authorize.source_node_id = kLocal;
  authorize.source_assignment_id = projected.authority.assignment_id;
  authorize.source_boot_id = kLocalBoot;
  authorize.target_node_id = kRemote;
  authorize.target_boot_id = kRemoteBoot;
  authorize.authority.assignment_id = remote_assignment;
  desired.current_directives.front().source_node_id = authorize.source_node_id;
  desired.current_directives.front().source_assignment_id =
      authorize.source_assignment_id;
  desired.current_directives.front().source_boot_id = authorize.source_boot_id;
  desired.current_directives.front().target_node_id = authorize.target_node_id;
  desired.current_directives.front().target_boot_id = authorize.target_boot_id;
  desired.current_directives.front().authority = authorize.authority;
  EXPECT_TRUE(ValidateLiveDirective(
                  authorize, control::SelectNodeControlState(desired, kLocal),
                  kLocal, kLocalBoot)
                  .ok());

  control::Directive stale_source = authorize;
  stale_source.source_assignment_id = remote_assignment;
  desired.current_directives.front().source_assignment_id =
      stale_source.source_assignment_id;
  EXPECT_EQ(ValidateLiveDirective(
                stale_source, control::SelectNodeControlState(desired, kLocal),
                kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().source_assignment_id =
      authorize.source_assignment_id;

  control::Directive stale_target = authorize;
  stale_target.authority.assignment_id = projected.authority.assignment_id;
  desired.current_directives.front().authority = stale_target.authority;
  EXPECT_EQ(ValidateLiveDirective(
                stale_target, control::SelectNodeControlState(desired, kLocal),
                kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().authority = authorize.authority;

  control::Directive revoke = authorize;
  revoke.kind = control::WireDirectiveKind::kRevokeSources;
  desired.current_directives.front().kind = revoke.kind;
  EXPECT_TRUE(ValidateLiveDirective(
                  revoke, control::SelectNodeControlState(desired, kLocal),
                  kLocal, kLocalBoot)
                  .ok());

  control::Directive initialize = live;
  initialize.kind = control::WireDirectiveKind::kInitializeEmptyPopulation;
  initialize.source_node_id = std::string(40, '0');
  initialize.source_assignment_id = {};
  initialize.source_boot_id = std::string(40, '0');
  initialize.source_replication_history_id = std::string(40, '0');
  initialize.payload = kRemote;

  initialize.authority.assignment_id = projected.authority.assignment_id;
  desired.current_directives.front() = projected;
  desired.current_directives.front().kind = initialize.kind;
  desired.current_directives.front().source_node_id = initialize.source_node_id;
  desired.current_directives.front().source_assignment_id = {};
  desired.current_directives.front().source_boot_id = initialize.source_boot_id;
  desired.current_directives.front().source_replication_history_id =
      initialize.source_replication_history_id;
  desired.current_directives.front().payload = initialize.payload;

  desired.current_directives.front().authority = initialize.authority;
  EXPECT_TRUE(ValidateLiveDirective(
                  initialize, control::SelectNodeControlState(desired, kLocal),
                  kLocal, kLocalBoot)
                  .ok());

  control::Directive stale_initialize_boot = initialize;
  stale_initialize_boot.recipient_boot_id = kRemoteBoot;
  stale_initialize_boot.target_boot_id = kRemoteBoot;
  desired.current_directives.front().recipient_boot_id = kRemoteBoot;
  desired.current_directives.front().target_boot_id = kRemoteBoot;
  EXPECT_EQ(
      ValidateLiveDirective(stale_initialize_boot,
                            control::SelectNodeControlState(desired, kLocal),
                            kLocal, kLocalBoot)
          .code(),
      absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().recipient_boot_id = kLocalBoot;
  desired.current_directives.front().target_boot_id = kLocalBoot;

  control::Directive stale_initialize_term = initialize;
  --stale_initialize_term.authority.group_term;
  desired.current_directives.front().authority =
      stale_initialize_term.authority;
  EXPECT_EQ(
      ValidateLiveDirective(stale_initialize_term,
                            control::SelectNodeControlState(desired, kLocal),
                            kLocal, kLocalBoot)
          .code(),
      absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().authority = initialize.authority;

  initialize.source_node_id = kRemote;
  EXPECT_EQ(ValidateLiveDirective(
                initialize, control::SelectNodeControlState(desired, kLocal),
                kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(MetaDirectiveResultTest, SeparatesRejectionsFromExecutionFailures) {
  EXPECT_EQ(ClassifyDirectiveResultStatus(absl::OkStatus(), true),
            control::DirectiveResultStatus::kSucceeded);
  EXPECT_EQ(ClassifyDirectiveResultStatus(
                absl::InvalidArgumentError("bad parameter"), false),
            control::DirectiveResultStatus::kRejected);
  EXPECT_EQ(ClassifyDirectiveResultStatus(
                absl::FailedPreconditionError("stale projection"), false),
            control::DirectiveResultStatus::kRejected);
  EXPECT_EQ(ClassifyDirectiveResultStatus(
                absl::OutOfRangeError("stale revision"), false),
            control::DirectiveResultStatus::kRejected);
  EXPECT_EQ(
      ClassifyDirectiveResultStatus(absl::UnavailableError("draining"), false),
      control::DirectiveResultStatus::kRejected);
  EXPECT_EQ(
      ClassifyDirectiveResultStatus(absl::AbortedError("cancelled"), true),
      control::DirectiveResultStatus::kFailed);
  EXPECT_EQ(ClassifyDirectiveResultStatus(
                absl::FailedPreconditionError("native state changed"), true),
            control::DirectiveResultStatus::kFailed);
  EXPECT_EQ(ClassifyDirectiveResultStatus(absl::InternalError("failure"), true),
            control::DirectiveResultStatus::kFailed);
}

TEST(MetaFailoverControlAdapterTest,
     TranslatesTheExactActionOnlyForItsCandidateIncarnation) {
  constexpr char kSource[] = "1111111111111111111111111111111111111111";
  constexpr char kCandidate[] = "2222222222222222222222222222222222222222";
  constexpr char kCandidateBoot[] = "3333333333333333333333333333333333333333";
  constexpr char kSourceBoot[] = "4444444444444444444444444444444444444444";
  constexpr char kSourceHistory[] = "5555555555555555555555555555555555555555";
  control::WireId128 transition_id{};
  transition_id[0] = 0x11;
  control::WireId128 action_id{};
  action_id[0] = 0x22;
  control::WireId128 active_grant_action_id{};
  active_grant_action_id[0] = 0x23;
  control::WireId128 source_assignment{};
  source_assignment[0] = 0x33;
  control::WireId128 candidate_assignment{};
  candidate_assignment[0] = 0x44;
  const control::WireHash256 manifest = control::WireHash256{2};
  DesiredClusterControl desired{
      .identity_ =
          {
              .group_id_ = "group-a",
              .group_term_ = 8,
              .manifest_revision_ = 18,
              .manifest_digest_ = manifest,
              .partition_replication_epoch_ = 20,
              .members_ = {{.node_id_ = *NodeId::Parse(kSource),
                            .assignment_id_ =
                                AssignmentId::FromBytes(source_assignment)},
                           {.node_id_ = *NodeId::Parse(kCandidate),
                            .assignment_id_ =
                                AssignmentId::FromBytes(candidate_assignment)}},
          },
      .owner_ =
          PreparedMemberAssignment{
              .node_id_ = *NodeId::Parse(kSource),
              .assignment_id_ = AssignmentId::FromBytes(source_assignment),
          },
      .grant_active_ = true,
      // A prior failover's action remains bound to the current owner's grant
      // while this new candidate action is prepared.
      .activation_action_id_ =
          FailoverActionId::FromBytes(active_grant_action_id),
      .failover_transition_ =
          PreparedFailoverTransition{
              .transition_id_ = FailoverTransitionId::FromBytes(transition_id),
              .revision_ = 23,
              .mode_ = PreparedFailoverMode::kControlled,
              .target_term_ = 9,
              .candidate_action_ =
                  PreparedFailoverAction{
                      .action_id_ = FailoverActionId::FromBytes(action_id),
                      .candidate_ =
                          {
                              .node_id_ = *NodeId::Parse(kCandidate),
                              .assignment_id_ =
                                  AssignmentId::FromBytes(candidate_assignment),
                              .boot_id_ = *NodeId::Parse(kCandidateBoot),
                          },
                      .domain_ =
                          {
                              .source_group_term_ = 8,
                              .source_node_id_ = *NodeId::Parse(kSource),
                              .source_assignment_id_ =
                                  AssignmentId::FromBytes(source_assignment),
                              .source_boot_id_ = *NodeId::Parse(kSourceBoot),
                              .source_history_id_ =
                                  *NodeId::Parse(kSourceHistory),
                              .flow_count_ = 2,
                          },
                      .authorization_ =
                          PreparedFailoverAuthorization{
                              .authorized_revision_ = 21,
                              .loss_if_cutover_ = PreparedFailoverLoss::kNone,
                          },
                  },
          },
  };

  auto translated = detail::TranslateClusterFailoverControl(
      desired, ReplicationIdentity{.local_node_id_ = kCandidate,
                                   .boot_id_ = kCandidateBoot,
                                   .local_history_id_ = kCandidate});
  ASSERT_TRUE(translated.ok()) << translated.status();
  ASSERT_TRUE(translated->candidate_action_.has_value());
  // The prior owner's grant provenance is not a pending activation on the new
  // candidate. Passing both ids to ReplicationManager is an impossible local
  // state even though they legally coexist in committed Group state.
  EXPECT_FALSE(translated->pending_activation_action_id_.has_value());
  const DesiredClusterFailoverAction& action = *translated->candidate_action_;
  EXPECT_EQ(action.transition_id_, transition_id);
  EXPECT_EQ(action.action_id_, action_id);
  EXPECT_EQ(action.transition_revision_, 23u);
  EXPECT_EQ(action.mode_, ClusterFailoverMode::kControlled);
  EXPECT_EQ(action.target_term_, 9u);
  EXPECT_EQ(action.committed_group_term_, 8u);
  EXPECT_TRUE(action.committed_grant_active_);
  EXPECT_EQ(action.authorized_revision_, 21u);
  EXPECT_EQ(action.group_id_, "group-a");
  EXPECT_EQ(action.candidate_node_id_, kCandidate);
  EXPECT_EQ(action.candidate_assignment_id_,
            AssignmentId::FromBytes(candidate_assignment).ToHexString());
  EXPECT_EQ(action.candidate_boot_id_, kCandidateBoot);
  EXPECT_EQ(action.domain_.source_group_term_, 8u);
  EXPECT_EQ(action.domain_.source_node_id_, kSource);
  EXPECT_EQ(action.domain_.source_assignment_id_,
            AssignmentId::FromBytes(source_assignment).ToHexString());
  EXPECT_EQ(action.domain_.source_boot_id_, kSourceBoot);
  EXPECT_EQ(action.domain_.source_history_id_, kSourceHistory);
  EXPECT_EQ(action.domain_.flow_count_, 2u);
  EXPECT_EQ(action.manifest_revision_, 18u);
  EXPECT_EQ(action.manifest_id_.bytes_, manifest);
  EXPECT_EQ(action.partition_replication_epoch_, 20u);

  translated = detail::TranslateClusterFailoverControl(
      desired, ReplicationIdentity{.local_node_id_ = kSource,
                                   .boot_id_ = kSourceBoot,
                                   .local_history_id_ = kSourceHistory});
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->candidate_action_.has_value());
  ASSERT_TRUE(translated->pending_activation_action_id_.has_value());
  EXPECT_EQ(*translated->pending_activation_action_id_, active_grant_action_id);
  ASSERT_TRUE(translated->source_pause_.has_value());
  const DesiredClusterSourcePause& pause = *translated->source_pause_;
  EXPECT_EQ(pause.transition_id_, transition_id);
  EXPECT_EQ(pause.transition_revision_, 23u);
  EXPECT_EQ(pause.group_id_, "group-a");
  EXPECT_EQ(pause.source_node_id_, kSource);
  EXPECT_EQ(pause.source_assignment_id_,
            AssignmentId::FromBytes(source_assignment).ToHexString());
  EXPECT_EQ(pause.source_boot_id_, kSourceBoot);
  EXPECT_EQ(pause.source_history_id_, kSourceHistory);
  EXPECT_EQ(pause.source_group_term_, 8u);
  EXPECT_EQ(pause.flow_count_, 2u);
  EXPECT_EQ(pause.manifest_revision_, 18u);
  EXPECT_EQ(pause.manifest_id_.bytes_, manifest);
  EXPECT_EQ(pause.partition_replication_epoch_, 20u);

  ReplicationIdentity mismatched_source{
      .local_node_id_ = kSource,
      .boot_id_ = kSourceBoot,
      .local_history_id_ = kSourceHistory,
  };
  mismatched_source.boot_id_ = kCandidateBoot;
  translated =
      detail::TranslateClusterFailoverControl(desired, mismatched_source);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->source_pause_.has_value());

  mismatched_source.boot_id_ = kSourceBoot;
  mismatched_source.local_history_id_ = kCandidate;
  translated =
      detail::TranslateClusterFailoverControl(desired, mismatched_source);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->source_pause_.has_value());

  desired.owner_->assignment_id_ =
      AssignmentId::FromBytes(candidate_assignment);
  translated = detail::TranslateClusterFailoverControl(
      desired, ReplicationIdentity{.local_node_id_ = kSource,
                                   .boot_id_ = kSourceBoot,
                                   .local_history_id_ = kSourceHistory});
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->source_pause_.has_value());

  desired.owner_->assignment_id_ = AssignmentId::FromBytes(source_assignment);
  desired.failover_transition_->mode_ = PreparedFailoverMode::kUncontrolled;
  translated = detail::TranslateClusterFailoverControl(
      desired, ReplicationIdentity{.local_node_id_ = kSource,
                                   .boot_id_ = kSourceBoot,
                                   .local_history_id_ = kSourceHistory});
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->source_pause_.has_value());
}

TEST(MetaFailoverControlAdapterTest,
     OmitsAnotherCandidateAndRetainsCutoverActivation) {
  constexpr char kCandidate[] = "1111111111111111111111111111111111111111";
  constexpr char kCandidateBoot[] = "2222222222222222222222222222222222222222";
  constexpr char kLocal[] = "3333333333333333333333333333333333333333";
  control::WireId128 transition_id{};
  transition_id[0] = 0x71;
  control::WireId128 action_id{};
  action_id[0] = 0x72;
  DesiredClusterControl desired{
      .failover_transition_ =
          PreparedFailoverTransition{
              .transition_id_ = FailoverTransitionId::FromBytes(transition_id),
              .revision_ = 1,
              .mode_ = PreparedFailoverMode::kUncontrolled,
              .target_term_ = 2,
              .candidate_action_ =
                  PreparedFailoverAction{
                      .action_id_ = FailoverActionId::FromBytes(action_id),
                      .candidate_ =
                          {
                              .node_id_ = *NodeId::Parse(kCandidate),
                              .boot_id_ = *NodeId::Parse(kCandidateBoot),
                          },
                  },
          },
  };
  const ReplicationIdentity local{
      .local_node_id_ = kLocal,
      .boot_id_ = kCandidateBoot,
      .local_history_id_ = kLocal,
  };

  auto translated = detail::TranslateClusterFailoverControl(desired, local);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->candidate_action_.has_value());
  EXPECT_FALSE(translated->source_pause_.has_value());
  EXPECT_FALSE(translated->pending_activation_action_id_.has_value());

  desired.failover_transition_.reset();
  desired.grant_active_ = true;
  desired.activation_action_id_ = FailoverActionId::FromBytes(action_id);
  desired.owner_ = PreparedMemberAssignment{
      .node_id_ = *NodeId::Parse(kLocal),
  };
  translated = detail::TranslateClusterFailoverControl(desired, local);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->candidate_action_.has_value());
  EXPECT_FALSE(translated->source_pause_.has_value());
  ASSERT_TRUE(translated->pending_activation_action_id_.has_value());
  EXPECT_EQ(*translated->pending_activation_action_id_, action_id);

  translated = detail::TranslateClusterFailoverControl(
      desired, ReplicationIdentity{.local_node_id_ = kCandidate,
                                   .boot_id_ = kCandidateBoot,
                                   .local_history_id_ = kCandidate});
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->pending_activation_action_id_.has_value());
}

TEST(MetaFailoverControlAdapterTest,
     RecoveryDirectoryUsesCurrentFdsMembersAndTransport) {
  const auto candidate = *NodeId::Parse(std::string(40, '1'));
  const auto donor = *NodeId::Parse(std::string(40, '2'));
  const auto boot = *NodeId::Parse(std::string(40, '3'));
  control::WireId128 id{};
  id[0] = 1;
  const auto assignment = AssignmentId::FromBytes(id);
  DesiredClusterControl desired;
  desired.identity_.group_id_ = "g";
  desired.identity_.group_term_ = 2;
  desired.identity_.members_ = {{candidate, assignment}, {donor, assignment}};
  desired.member_endpoints_ = {{candidate, "127.0.0.1", 6001, 7001},
                               {donor, "127.0.0.2", 6002, 7002}};
  PreparedFailoverTransition transition;
  transition.transition_id_ = FailoverTransitionId::FromBytes(id);
  transition.revision_ = 4;
  transition.mode_ = PreparedFailoverMode::kUncontrolled;
  transition.target_term_ = 2;
  PreparedFailoverAction action;
  action.action_id_ = FailoverActionId::FromBytes(id);
  action.candidate_ = {candidate, assignment, boot};
  action.domain_ = {1, donor, assignment, boot, boot, 2};
  transition.candidate_action_ = action;
  transition.recovery_deadline_unix_ms_ = 3000;
  desired.failover_transition_ = transition;
  ReplicationIdentity local{.local_node_id_ = donor.ToHexString(),
                            .boot_id_ = boot.ToHexString()};
  auto translated =
      detail::TranslateClusterFailoverControl(desired, local, true);
  ASSERT_TRUE(translated.ok()) << translated.status();
  ASSERT_TRUE(translated->recovery_.has_value());
  EXPECT_FALSE(translated->candidate_action_.has_value());
  EXPECT_EQ(translated->recovery_->local_node_id_, donor.ToHexString());
  ASSERT_EQ(translated->recovery_->members_.size(), 2);
  EXPECT_EQ(translated->recovery_->members_[1].endpoint_.port_, 7002);
  EXPECT_EQ(translated->recovery_->action_.recovery_deadline_unix_ms_, 3000);
  desired.failover_transition_->candidate_action_->authorization_ =
      PreparedFailoverAuthorization{5, PreparedFailoverLoss::kUnknown};
  translated = detail::TranslateClusterFailoverControl(desired, local, true);
  ASSERT_TRUE(translated.ok());
  EXPECT_FALSE(translated->recovery_.has_value());
}

TEST(MetaFailoverControlAdapterTest,
     ReconcilesSteadyOwnerOnlyAfterTransitionFinishes) {
  constexpr char kOwner[] = "1111111111111111111111111111111111111111";
  constexpr char kReplica[] = "2222222222222222222222222222222222222222";
  constexpr char kReplicaBoot[] = "3333333333333333333333333333333333333333";
  control::WireId128 owner_assignment{};
  owner_assignment[0] = 0x41;
  control::WireId128 replica_assignment{};
  replica_assignment[0] = 0x42;
  const auto manifest = PopulationManifest::Create({{3, 7}, {9, 11}});
  ASSERT_TRUE(manifest.ok()) << manifest.status();
  DesiredClusterControl desired{
      .identity_ =
          {
              .group_id_ = "group-a",
              .group_term_ = 8,
              .manifest_revision_ = 18,
              .manifest_digest_ = manifest->id().bytes_,
              .partition_replication_epoch_ = 20,
              .members_ =
                  {
                      {.node_id_ = *NodeId::Parse(kOwner),
                       .assignment_id_ =
                           AssignmentId::FromBytes(owner_assignment)},
                      {.node_id_ = *NodeId::Parse(kReplica),
                       .assignment_id_ =
                           AssignmentId::FromBytes(replica_assignment)},
                  },
          },
      .owner_ =
          PreparedMemberAssignment{
              .node_id_ = *NodeId::Parse(kOwner),
              .assignment_id_ = AssignmentId::FromBytes(owner_assignment),
          },
      .owner_endpoint_ =
          PreparedReplicationEndpoint{
              .node_id_ = *NodeId::Parse(kOwner),
              .host_ = "10.0.0.1",
              .port_ = 7001,
              .tls_port_ = 17001,
          },
      .manifest_entries_ = {{3, 7}, {9, 11}},
      .steady_replication_enabled_ = true,
  };
  const ReplicationIdentity replica{
      .local_node_id_ = kReplica,
      .boot_id_ = kReplicaBoot,
      .local_history_id_ = kReplica,
  };

  auto translated = detail::TranslateClusterFailoverControl(desired, replica,
                                                            /*use_tls=*/false);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_TRUE(translated->reconcile_follow_owner_);
  ASSERT_TRUE(translated->follow_owner_.has_value());
  EXPECT_EQ(translated->follow_owner_->local_node_id_, kReplica);
  EXPECT_EQ(translated->follow_owner_->local_assignment_id_,
            AssignmentId::FromBytes(replica_assignment).ToHexString());
  EXPECT_EQ(translated->follow_owner_->local_boot_id_, kReplicaBoot);
  EXPECT_EQ(translated->follow_owner_->owner_node_id_, kOwner);
  EXPECT_EQ(translated->follow_owner_->owner_assignment_id_,
            AssignmentId::FromBytes(owner_assignment).ToHexString());
  EXPECT_EQ(translated->follow_owner_->owner_endpoint_,
            (ReplicaOfConfig{"10.0.0.1", 7001}));
  EXPECT_EQ(translated->follow_owner_->manifest_entries_,
            (std::vector<PopulationManifestEntry>{{3, 7}, {9, 11}}));
  ASSERT_EQ(translated->follow_owner_->members_.size(), 2U);

  translated = detail::TranslateClusterFailoverControl(desired, replica,
                                                       /*use_tls=*/true);
  ASSERT_TRUE(translated.ok()) << translated.status();
  ASSERT_TRUE(translated->follow_owner_.has_value());
  EXPECT_EQ(translated->follow_owner_->owner_endpoint_,
            (ReplicaOfConfig{"10.0.0.1", 17001}));

  translated = detail::TranslateClusterFailoverControl(
      desired,
      ReplicationIdentity{.local_node_id_ = kOwner,
                          .boot_id_ = kReplicaBoot,
                          .local_history_id_ = kOwner},
      /*use_tls=*/false);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_TRUE(translated->reconcile_follow_owner_);
  ASSERT_TRUE(translated->follow_owner_.has_value());
  EXPECT_EQ(translated->follow_owner_->local_node_id_, kOwner);
  EXPECT_FALSE(translated->follow_owner_->owner_endpoint_.has_value());

  desired.population_transition_expected_ = true;
  translated = detail::TranslateClusterFailoverControl(desired, replica,
                                                       /*use_tls=*/false);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->reconcile_follow_owner_);
  EXPECT_FALSE(translated->follow_owner_.has_value());
  desired.population_transition_expected_ = false;

  desired.failover_transition_ = PreparedFailoverTransition{};
  translated = detail::TranslateClusterFailoverControl(desired, replica,
                                                       /*use_tls=*/false);
  ASSERT_TRUE(translated.ok()) << translated.status();
  EXPECT_FALSE(translated->reconcile_follow_owner_);
  EXPECT_FALSE(translated->follow_owner_.has_value());
}

TEST(MetaFailoverControlAdapterTest, TranslatesExactPromotionActivation) {
  constexpr char kCandidate[] = "1111111111111111111111111111111111111111";
  constexpr char kBoot[] = "2222222222222222222222222222222222222222";
  control::WireId128 action_id{};
  action_id[0] = 0x51;
  control::WireId128 assignment{};
  assignment[0] = 0x52;
  Sha256Digest manifest{};
  manifest[0] = 0x53;
  const PreparedFailoverActivation prepared{
      .action_id_ = FailoverActionId::FromBytes(action_id),
      .group_id_ = "group-a",
      .candidate_node_id_ = *NodeId::Parse(kCandidate),
      .candidate_assignment_id_ = AssignmentId::FromBytes(assignment),
      .candidate_boot_id_ = *NodeId::Parse(kBoot),
      .target_term_ = 9,
      .manifest_revision_ = 18,
      .manifest_digest_ = manifest,
      .partition_replication_epoch_ = 20,
  };

  EXPECT_EQ(detail::TranslateClusterFailoverActivation(prepared),
            (ClusterFailoverActivation{
                .action_id_ = action_id,
                .group_id_ = "group-a",
                .candidate_node_id_ = kCandidate,
                .candidate_assignment_id_ =
                    AssignmentId::FromBytes(assignment).ToHexString(),
                .candidate_boot_id_ = kBoot,
                .target_term_ = 9,
                .manifest_revision_ = 18,
                .manifest_id_ = PopulationManifestId{manifest},
                .partition_replication_epoch_ = 20,
            }));
}

TEST(MetaFailoverHeartbeatTest, ProjectsExactPreparedContext) {
  ClusterFailoverTransitionId transition_id{};
  transition_id[0] = 0x11;
  ClusterFailoverActionId action_id{};
  action_id[0] = 0x22;
  ClusterPreparedContextId context_id{};
  context_id[0] = 0x33;
  control::WireId128 assignment{};
  assignment[0] = 0x55;
  constexpr char kCandidate[] = "2222222222222222222222222222222222222222";
  constexpr char kCandidateBoot[] = "3333333333333333333333333333333333333333";
  constexpr char kSourceHistory[] = "4444444444444444444444444444444444444444";
  ClusterFailoverActionStatus status{
      .state_ = ClusterFailoverActionState::kPrepared,
      .action_ =
          DesiredClusterFailoverAction{
              .transition_id_ = transition_id,
              .action_id_ = action_id,
              .candidate_node_id_ = kCandidate,
              .candidate_assignment_id_ =
                  AssignmentId::FromBytes(assignment).ToHexString(),
              .candidate_boot_id_ = kCandidateBoot,
              .domain_ = {.source_history_id_ = kSourceHistory},
          },
      .prepared_ =
          ClusterFailoverPreparedContext{
              .transition_id_ = transition_id,
              .action_id_ = action_id,
              .context_id_ = context_id,
              .promotion_ = {.parent_history_id_ = kSourceHistory},
          },
  };

  auto observation = detail::ProjectClusterFailoverObservation(status);
  ASSERT_TRUE(observation.ok()) << observation.status();
  ASSERT_TRUE(observation->has_value());
  const auto* prepared =
      std::get_if<control::CandidatePrepared>(&**observation);
  ASSERT_NE(prepared, nullptr);
  EXPECT_EQ(prepared->transition_id, transition_id);
  EXPECT_EQ(prepared->action_id, action_id);
  EXPECT_EQ(prepared->candidate_node_id, kCandidate);
  EXPECT_EQ(prepared->candidate_assignment_id, assignment);
  EXPECT_EQ(prepared->candidate_boot_id, kCandidateBoot);
  EXPECT_EQ(prepared->prepared_context_id, context_id);
}

TEST(MetaFailoverHeartbeatTest, ProjectsOnlyAnExactStableSourcePause) {
  ClusterFailoverTransitionId transition_id{};
  transition_id[0] = 0x91;
  control::WireId128 assignment{};
  assignment[0] = 0x92;
  constexpr char kSource[] = "8888888888888888888888888888888888888888";
  constexpr char kSourceBoot[] = "9999999999999999999999999999999999999999";
  constexpr char kSourceHistory[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  ClusterSourcePauseStatus status{
      .desired_ =
          DesiredClusterSourcePause{
              .transition_id_ = transition_id,
              .transition_revision_ = 17,
              .group_id_ = "group-a",
              .source_node_id_ = kSource,
              .source_assignment_id_ =
                  AssignmentId::FromBytes(assignment).ToHexString(),
              .source_boot_id_ = kSourceBoot,
              .source_history_id_ = kSourceHistory,
              .source_group_term_ = 11,
              .flow_count_ = 2,
          },
      .stable_next_lsns_ = std::vector<std::uint64_t>{31, 47},
  };

  auto observation = detail::ProjectClusterSourcePauseObservation(status);
  ASSERT_TRUE(observation.ok()) << observation.status();
  ASSERT_TRUE(observation->has_value());
  const auto* paused = std::get_if<control::SourcePaused>(&**observation);
  ASSERT_NE(paused, nullptr);
  EXPECT_EQ(paused->transition_id, transition_id);
  EXPECT_EQ(paused->source_node_id, kSource);
  EXPECT_EQ(paused->source_assignment_id, assignment);
  EXPECT_EQ(paused->source_boot_id, kSourceBoot);
  EXPECT_EQ(paused->source_history_id, kSourceHistory);
  EXPECT_EQ(paused->source_group_term, 11u);
  EXPECT_EQ(paused->stable_next_lsns, (std::vector<std::uint64_t>{31, 47}));

  status.stable_next_lsns_.reset();
  observation = detail::ProjectClusterSourcePauseObservation(status);
  ASSERT_TRUE(observation.ok()) << observation.status();
  EXPECT_FALSE(observation->has_value());

  status.stable_next_lsns_ = std::vector<std::uint64_t>{31, 47};
  status.failure_detail_ = "source population identity changed";
  observation = detail::ProjectClusterSourcePauseObservation(status);
  ASSERT_TRUE(observation.ok()) << observation.status();
  EXPECT_FALSE(observation->has_value());

  status.failure_detail_.clear();
  status.stable_next_lsns_ = std::vector<std::uint64_t>{31};
  observation = detail::ProjectClusterSourcePauseObservation(status);
  ASSERT_TRUE(observation.ok()) << observation.status();
  EXPECT_FALSE(observation->has_value());

  status.stable_next_lsns_ = std::vector<std::uint64_t>{31, 47};
  status.desired_.reset();
  observation = detail::ProjectClusterSourcePauseObservation(status);
  ASSERT_TRUE(observation.ok()) << observation.status();
  EXPECT_FALSE(observation->has_value());
}

TEST(MetaFailoverHeartbeatTest, ProjectsFailureFromTheExactActionPopulation) {
  ClusterFailoverTransitionId transition_id{};
  transition_id[0] = 0x61;
  ClusterFailoverActionId action_id{};
  action_id[0] = 0x62;
  control::WireId128 assignment{};
  assignment[0] = 0x63;
  control::WireHash256 manifest{};
  manifest[0] = 0x64;
  constexpr char kCandidate[] = "6666666666666666666666666666666666666666";
  constexpr char kCandidateBoot[] = "7777777777777777777777777777777777777777";
  ClusterFailoverActionStatus status{
      .state_ = ClusterFailoverActionState::kFailed,
      .action_ =
          DesiredClusterFailoverAction{
              .transition_id_ = transition_id,
              .action_id_ = action_id,
              .group_id_ = "group-a",
              .candidate_node_id_ = kCandidate,
              .candidate_assignment_id_ =
                  AssignmentId::FromBytes(assignment).ToHexString(),
              .candidate_boot_id_ = kCandidateBoot,
              .manifest_revision_ = 71,
              .manifest_id_ = PopulationManifestId{manifest},
              .partition_replication_epoch_ = 72,
          },
      .prepared_ = std::nullopt,
      .failure_class_ = "prepare_durability",
      .failure_detail_ = "promotion base could not be persisted",
  };

  auto observation = detail::ProjectClusterFailoverObservation(status);
  ASSERT_TRUE(observation.ok()) << observation.status();
  ASSERT_TRUE(observation->has_value());
  const auto* failed = std::get_if<control::ActionFailed>(&**observation);
  ASSERT_NE(failed, nullptr);
  EXPECT_EQ(failed->transition_id, transition_id);
  EXPECT_EQ(failed->action_id, action_id);
  EXPECT_EQ(failed->candidate_node_id, kCandidate);
  EXPECT_EQ(failed->candidate_assignment_id, assignment);
  EXPECT_EQ(failed->candidate_boot_id, kCandidateBoot);
  EXPECT_EQ(failed->population_manifest_revision, 71u);
  EXPECT_EQ(failed->population_manifest_digest, manifest);
  EXPECT_EQ(failed->partition_replication_epoch, 72u);
  EXPECT_EQ(failed->failure_class, "prepare_durability");
  EXPECT_EQ(failed->failure_detail, "promotion base could not be persisted");
}

TEST(MetaFailoverHeartbeatTest,
     ExactPreparingAndPreparedActionsBridgeHistoryRotationUntilCutover) {
  const ReplicationIdentity established{
      .local_node_id_ = "1111111111111111111111111111111111111111",
      .boot_id_ = "2222222222222222222222222222222222222222",
      .local_history_id_ = "3333333333333333333333333333333333333333",
  };
  ReplicationIdentity child = established;
  child.local_history_id_ = "4444444444444444444444444444444444444444";

  EXPECT_TRUE(
      detail::ReplicationIdentityRequiresMetaReconnect(established, child));

  control::WireId128 transition_id{};
  transition_id[0] = 0x11;
  control::WireId128 action_id{};
  action_id[0] = 0x12;
  control::WireId128 candidate_assignment{};
  candidate_assignment[0] = 0x13;
  control::WireId128 source_assignment{};
  source_assignment[0] = 0x14;
  control::WireHash256 manifest{};
  manifest[0] = 0x15;
  constexpr char kSource[] = "5555555555555555555555555555555555555555";
  constexpr char kSourceBoot[] = "6666666666666666666666666666666666666666";
  constexpr char kSourceHistory[] = "7777777777777777777777777777777777777777";

  DesiredClusterFailoverAction action{
      .transition_id_ = transition_id,
      .action_id_ = action_id,
      .transition_revision_ = 7,
      .mode_ = ClusterFailoverMode::kControlled,
      .target_term_ = 9,
      .committed_group_term_ = 8,
      .committed_grant_active_ = true,
      .authorized_revision_ = 7,
      .group_id_ = "group-a",
      .candidate_node_id_ = child.local_node_id_,
      .candidate_assignment_id_ =
          AssignmentId::FromBytes(candidate_assignment).ToHexString(),
      .candidate_boot_id_ = child.boot_id_,
      .domain_ = {.source_group_term_ = 8,
                  .source_node_id_ = kSource,
                  .source_assignment_id_ =
                      AssignmentId::FromBytes(source_assignment).ToHexString(),
                  .source_boot_id_ = kSourceBoot,
                  .source_history_id_ = kSourceHistory,
                  .flow_count_ = 2},
      .manifest_revision_ = 10,
      .manifest_id_ = PopulationManifestId{manifest},
      .partition_replication_epoch_ = 11,
  };
  control::WireDesiredGroup group{
      .group_id = "group-a",
      .owner_node_id = std::string(kSource),
      .owner_assignment_id = source_assignment,
      .group_term = 8,
      .grant_active = true,
      .manifest_revision = 10,
      .manifest_digest = manifest,
      .partition_replication_epoch = 11,
      .failover_transition =
          control::WireFailoverTransition{
              .transition_id = transition_id,
              .revision = 7,
              .mode = control::WireFailoverMode::kControlled,
              .target_term = 9,
              .candidate_action =
                  control::WireFailoverCandidateAction{
                      .action_id = action_id,
                      .candidate = {.node_id = child.local_node_id_,
                                    .assignment_id = candidate_assignment,
                                    .boot_id = child.boot_id_},
                      .domain = {.source_group_term = 8,
                                 .source_node_id = kSource,
                                 .source_assignment_id = source_assignment,
                                 .source_boot_id = kSourceBoot,
                                 .source_history_id = kSourceHistory,
                                 .flow_count = 2},
                      .authorization =
                          control::WireFailoverAuthorization{
                              .authorized_revision = 7,
                              .loss_if_cutover =
                                  control::WireFailoverLoss::kNone}}},
  };
  std::array groups{group};

  ClusterFailoverActionStatus preparing{
      .state_ = ClusterFailoverActionState::kPreparing,
      .action_ = action,
  };
  EXPECT_TRUE(detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
      established, child, preparing, groups));
  EXPECT_EQ(
      detail::EvaluateMetaSessionReplicationIdentity(established, child,
                                                     preparing, groups),
      (detail::MetaSessionReplicationIdentityDecision{
          .requires_reconnect_ = false, .suppress_ordinary_role_ = true}));

  ClusterFailoverActionStatus prepared{
      .state_ = ClusterFailoverActionState::kPrepared,
      .action_ = action,
      .prepared_ =
          ClusterFailoverPreparedContext{
              .transition_id_ = transition_id,
              .action_id_ = action_id,
              .promotion_ = {.parent_history_id_ = kSourceHistory,
                             .child_history_id_ = child.local_history_id_}},
  };
  EXPECT_TRUE(detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
      established, child, prepared, groups));
  EXPECT_EQ(
      detail::EvaluateMetaSessionReplicationIdentity(established, child,
                                                     prepared, groups),
      (detail::MetaSessionReplicationIdentityDecision{
          .requires_reconnect_ = false, .suppress_ordinary_role_ = true}));

  auto wrong_child = prepared;
  wrong_child.prepared_->promotion_.child_history_id_ =
      established.local_history_id_;
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, child, wrong_child, groups));

  auto wrong_parent = prepared;
  wrong_parent.prepared_->promotion_.parent_history_id_ =
      established.local_history_id_;
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, child, wrong_parent, groups));
  auto wrong_parent_observation =
      detail::ProjectClusterFailoverObservation(wrong_parent);
  ASSERT_FALSE(wrong_parent_observation.ok());
  EXPECT_EQ(wrong_parent_observation.status().code(),
            absl::StatusCode::kFailedPrecondition);

  auto wrong_action = prepared;
  wrong_action.action_->action_id_[0] ^= 0xff;
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, child, wrong_action, groups));

  groups[0].failover_transition->revision++;
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, child, prepared, groups));
  groups[0] = group;

  groups[0]
      .failover_transition->candidate_action->authorization
      ->authorized_revision++;
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, child, prepared, groups));
  groups[0] = group;

  groups[0].failover_transition->candidate_action->authorization.reset();
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, child, prepared, groups));
  groups[0] = group;

  groups[0].failover_transition.reset();
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, child, prepared, groups));
  EXPECT_EQ(
      detail::EvaluateMetaSessionReplicationIdentity(established, child,
                                                     prepared, groups),
      (detail::MetaSessionReplicationIdentityDecision{
          .requires_reconnect_ = true, .suppress_ordinary_role_ = false}));

  ReplicationIdentity restarted = established;
  restarted.boot_id_ = "8888888888888888888888888888888888888888";
  EXPECT_TRUE(
      detail::ReplicationIdentityRequiresMetaReconnect(established, restarted));
  groups[0] = group;
  EXPECT_FALSE(
      detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, restarted, prepared, groups));
}

TEST(MetaFailoverHeartbeatTest, WaitingStatesAreNotWorkflowEvidence) {
  constexpr std::array states{
      ClusterFailoverActionState::kNone,
      ClusterFailoverActionState::kWaitingForAuthorization,
      ClusterFailoverActionState::kWaitingForPopulation,
      ClusterFailoverActionState::kPreparing,
      ClusterFailoverActionState::kRetrying,
  };
  for (const ClusterFailoverActionState state : states) {
    auto observation = detail::ProjectClusterFailoverObservation(
        ClusterFailoverActionStatus{.state_ = state});
    ASSERT_TRUE(observation.ok()) << observation.status();
    EXPECT_FALSE(observation->has_value());
  }
}

TEST(MetaCandidateProgressTest,
     HeartbeatPreservesWholeTypedCandidateWhileShorteningSummary) {
  control::WireId128 assignment{};
  assignment.fill(0x31);
  control::Heartbeat heartbeat{
      .session_id = {},
      .heartbeat_sequence = 1,
      .health = {.storage_ready = true,
                 .population_ready = true,
                 .draining = false,
                 .active_groups = 1,
                 .summary = std::string(control::kMaxFramePayloadBytes, 's')},
      .role_information =
          control::ReplicaCandidate{
              .progress =
                  {
                      .group_id = "group-a",
                      .assignment_id = assignment,
                      .group_term = 1,
                      .source_group_term = 1,
                      .manifest_revision = 1,
                      .manifest_digest = {},
                      .partition_replication_epoch = 2,
                      .source_node_id =
                          "1111111111111111111111111111111111111111",
                      .source_assignment_id = assignment,
                      .source_boot_id =
                          "2222222222222222222222222222222222222222",
                      .source_history_id =
                          "3333333333333333333333333333333333333333",
                      .applied_next_lsns = {10, 20},
                  },
          },
  };
  ASSERT_TRUE(FitHeartbeatToSingleFrame(heartbeat).ok());
  const auto* candidate =
      std::get_if<control::ReplicaCandidate>(&heartbeat.role_information);
  ASSERT_NE(candidate, nullptr);
  EXPECT_EQ(candidate->progress.assignment_id.front(), 0x31);
  EXPECT_EQ(candidate->progress.applied_next_lsns,
            (std::vector<std::uint64_t>{10, 20}));
  auto encoded = control::EncodeMessage(control::WireMessage(heartbeat));
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_LE(encoded->size(), control::kMaxFramePayloadBytes);
  EXPECT_LT(heartbeat.health.summary.size(), control::kMaxFramePayloadBytes);
}

TEST(MetaAuthorityIdentityTest, UsesVersionedUnambiguousEncoding) {
  AuthorityAnchor first{
      .group_id_ = "group:/with:separators",
      .assignment_id_ = AssignmentId::FromBytes({}),
      .group_term_ = 1,
  };
  AuthorityAnchor second = first;
  second.group_id_ = "group";
  EXPECT_NE(EncodeRebuildAuthorityIdentity(first),
            EncodeRebuildAuthorityIdentity(second));
  EXPECT_TRUE(EncodeRebuildAuthorityIdentity(first).starts_with("v1/"));
}

}  // namespace
}  // namespace lavik::cluster
