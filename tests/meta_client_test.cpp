#include "keylane/cluster/meta_client.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/cluster/node_control.h"

namespace keylane::cluster {
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
  const std::vector<std::string> matching{"keylane://meta/7"};
  const std::vector<std::string> mismatching{"keylane://meta/8"};
  const std::vector<std::string> duplicate{"keylane://meta/7",
                                           "keylane://operator/a"};
  EXPECT_TRUE(
      ValidateUniqueControlPrincipal(matching, "keylane://meta/7").ok());
  EXPECT_EQ(ValidateUniqueControlPrincipal({}, "keylane://meta/7").code(),
            absl::StatusCode::kUnauthenticated);
  EXPECT_EQ(
      ValidateUniqueControlPrincipal(mismatching, "keylane://meta/7").code(),
      absl::StatusCode::kPermissionDenied);
  EXPECT_EQ(
      ValidateUniqueControlPrincipal(duplicate, "keylane://meta/7").code(),
      absl::StatusCode::kUnauthenticated);
}

TEST(MetaControlIdentityTest, LearnedDialTargetPinsItsExactPrincipal) {
  const MetaControlEndpoint learned{
      .host_ = "127.0.0.1",
      .port_ = 7107,
      .server_id_ = 7,
      .principal_ = "keylane://meta/7",
  };
  control::WireMetaEndpoint hello_member{
      .server_id = 7,
      .host = "127.0.0.1",
      .port = 7107,
      .principal = "keylane://meta/7",
  };
  EXPECT_TRUE(ValidateDialedMetaIdentity(learned, hello_member).ok());

  hello_member.principal = "keylane://meta/8";
  EXPECT_EQ(ValidateDialedMetaIdentity(learned, hello_member).code(),
            absl::StatusCode::kPermissionDenied);
  hello_member.principal = "keylane://meta/7";
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
      .principal = "keylane://meta/7",
  };
  EXPECT_TRUE(ValidateDialedMetaIdentity(seed, hello_member).ok());
}

TEST(MetaReconnectBackoffTest, UsesFullJitterAndResetsOnlyExplicitly) {
  MetaReconnectBackoff backoff;
  EXPECT_EQ(backoff.window(), std::chrono::milliseconds(1000));
  EXPECT_EQ(backoff.Next(0), std::chrono::milliseconds(0));
  EXPECT_EQ(backoff.window(), std::chrono::milliseconds(2000));
  EXPECT_EQ(backoff.Next(2000), std::chrono::milliseconds(2000));
  EXPECT_EQ(backoff.window(), std::chrono::milliseconds(4000));
  (void)backoff.Next(999999);
  (void)backoff.Next(999999);
  EXPECT_EQ(backoff.window(), std::chrono::milliseconds(10000));
  (void)backoff.Next(999999);
  EXPECT_EQ(backoff.window(), std::chrono::milliseconds(10000));

  backoff.Reset();
  EXPECT_EQ(backoff.window(), std::chrono::milliseconds(1000));
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
       .principal = "keylane://meta/2"},
      {.server_id = 3,
       .host = "127.0.0.1",
       .port = 7103,
       .principal = "keylane://meta/3"},
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
       .principal = "keylane://meta/1"},
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
       .principal = "keylane://meta/2"},
      {.server_id = 3,
       .host = "127.0.0.1",
       .port = 7103,
       .principal = "keylane://meta/3"},
  };
  ASSERT_TRUE(directory.Update(initial, 3).ok());
  const std::vector<control::WireMetaEndpoint> refreshed{
      {.server_id = 3,
       .host = "127.0.0.1",
       .port = 7103,
       .principal = "keylane://meta/3"},
      {.server_id = 4,
       .host = "127.0.0.1",
       .port = 7104,
       .principal = "keylane://meta/4"},
  };
  ASSERT_TRUE(directory.Refresh(refreshed).ok());
  EXPECT_EQ(directory.Candidates().front().server_id_, 3u);

  const std::vector<control::WireMetaEndpoint> without_old_leader{
      {.server_id = 4,
       .host = "127.0.0.1",
       .port = 7104,
       .principal = "keylane://meta/4"},
  };
  ASSERT_TRUE(directory.Refresh(without_old_leader).ok());
  EXPECT_EQ(directory.Candidates().front().server_id_, 4u);

  const auto before_invalid = directory.Candidates();
  const std::vector<control::WireMetaEndpoint> invalid{
      {.server_id = 5,
       .host = "meta.example",
       .port = 7105,
       .principal = "keylane://meta/5"}};
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
       .principal = "keylane://meta/8"}};
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

TEST(MetaDirectiveValidationTest,
     RequiresExactInstalledDirectiveAndKindSpecificRecipient) {
  constexpr char kLocal[] = "1111111111111111111111111111111111111111";
  constexpr char kLocalBoot[] = "2222222222222222222222222222222222222222";
  constexpr char kRemote[] = "3333333333333333333333333333333333333333";
  constexpr char kRemoteBoot[] = "4444444444444444444444444444444444444444";
  control::WireProjectedDirective projected{
      .basis = {.source_meta_applied_index = 9,
                .projection_hash = control::ComputeSha256("projection")},
      .authority = {.group_id = "group-a",
                    .assignment_id = {},
                    .group_term = 3,
                    .authority_version = 4,
                    .grant_revision = 5},
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
      .manifest_digest = control::ComputeSha256("manifest"),
      .partition_replication_epoch = 10,
      .kind = control::WireDirectiveKind::kRebuild,
      .payload = "payload",
      .preconditions = "preconditions",
      .storage_mutating = true,
      .force = false,
  };
  projected.authority.assignment_id[0] = 1;
  projected.identity.operation_id[0] = 2;
  projected.identity.directive_id[0] = 3;
  projected.identity.attempt_id[0] = 4;
  projected.source_assignment_id[0] = 9;
  const auto live = [&] {
    return control::Directive{
        .session_id = {},
        .basis = projected.basis,
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
        .preconditions = projected.preconditions,
        .storage_mutating = projected.storage_mutating,
        .force = projected.force,
    };
  }();
  control::FullDesiredState desired;
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
      .authority_version = projected.authority.authority_version,
      .grant_revision = projected.authority.grant_revision,
      .partition_replication_epoch = projected.partition_replication_epoch,
  });
  desired.current_directives.push_back(projected);
  EXPECT_TRUE(ValidateLiveDirective(live, desired, kLocal, kLocalBoot).ok());

  control::Directive stale_population = live;
  --stale_population.partition_replication_epoch;
  EXPECT_EQ(ValidateLiveDirective(stale_population, desired, kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kFailedPrecondition);

  // Even if a stale envelope exactly matches a projected directive, the
  // installed group's current population epoch remains authoritative.
  desired.current_directives.front().partition_replication_epoch =
      stale_population.partition_replication_epoch;
  EXPECT_EQ(ValidateLiveDirective(stale_population, desired, kLocal, kLocalBoot)
                .code(),
            absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().partition_replication_epoch =
      projected.partition_replication_epoch;

  control::Directive changed = live;
  changed.force = true;
  EXPECT_EQ(ValidateLiveDirective(changed, desired, kLocal, kLocalBoot).code(),
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
  EXPECT_EQ(
      ValidateLiveDirective(authorize, desired, kLocal, kLocalBoot).code(),
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
  EXPECT_TRUE(
      ValidateLiveDirective(authorize, desired, kLocal, kLocalBoot).ok());

  control::Directive stale_source = authorize;
  stale_source.source_assignment_id = remote_assignment;
  desired.current_directives.front().source_assignment_id =
      stale_source.source_assignment_id;
  EXPECT_EQ(
      ValidateLiveDirective(stale_source, desired, kLocal, kLocalBoot).code(),
      absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().source_assignment_id =
      authorize.source_assignment_id;

  control::Directive stale_target = authorize;
  stale_target.authority.assignment_id = projected.authority.assignment_id;
  desired.current_directives.front().authority = stale_target.authority;
  EXPECT_EQ(
      ValidateLiveDirective(stale_target, desired, kLocal, kLocalBoot).code(),
      absl::StatusCode::kFailedPrecondition);
  desired.current_directives.front().authority = authorize.authority;

  control::Directive revoke = authorize;
  revoke.kind = control::WireDirectiveKind::kRevokeSources;
  desired.current_directives.front().kind = revoke.kind;
  EXPECT_TRUE(ValidateLiveDirective(revoke, desired, kLocal, kLocalBoot).ok());
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
      .authority_version_ = 23,
      .grant_revision_ = 4,
  };
  AuthorityAnchor second = first;
  second.group_id_ = "group";
  EXPECT_NE(EncodeRebuildAuthorityIdentity(first),
            EncodeRebuildAuthorityIdentity(second));
  EXPECT_TRUE(EncodeRebuildAuthorityIdentity(first).starts_with("v1/"));
}

}  // namespace
}  // namespace keylane::cluster
