#include <sys/types.h>

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/meta/automatic_failover_detector.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/ctl_server.h"
#include "keylane/meta/identity_store.h"
#include "keylane/meta/identity_verifier.h"

namespace {

constexpr std::string_view kNodeId = "0123456789abcdef0123456789abcdef01234567";

TEST(MetaClusterCreateStatus, DistinguishesUnobservedFromMissingSession) {
  using keylane::meta::detail::ClusterCreateMissingSessionBlocker;
  EXPECT_EQ(ClusterCreateMissingSessionBlocker(false, false),
            "data_unobserved");
  EXPECT_EQ(ClusterCreateMissingSessionBlocker(true, false),
            "data_session_missing");
  EXPECT_EQ(ClusterCreateMissingSessionBlocker(false, true),
            "data_session_missing");
}

TEST(MetaIdentitySecurity, ParsesCanonicalRoles) {
  auto node = keylane::meta::ParseMetaPrincipal(std::string("keylane://node/") +
                                                std::string(kNodeId));
  ASSERT_TRUE(node.ok()) << node.status();
  EXPECT_EQ(node->role_, keylane::meta::MetaPrincipalRole::kDataNode);
  EXPECT_EQ(node->subject_id_, kNodeId);

  auto member = keylane::meta::ParseMetaPrincipal("keylane://meta/17");
  ASSERT_TRUE(member.ok()) << member.status();
  EXPECT_EQ(member->role_, keylane::meta::MetaPrincipalRole::kMetaMember);
  EXPECT_EQ(member->subject_id_, "17");

  auto op = keylane::meta::ParseMetaPrincipal("keylane://operator/alice");
  ASSERT_TRUE(op.ok()) << op.status();
  EXPECT_EQ(op->role_, keylane::meta::MetaPrincipalRole::kOperator);
}

TEST(MetaIdentitySecurity, RejectsNonCanonicalPrincipals) {
  EXPECT_FALSE(keylane::meta::ParseMetaPrincipal(
                   "keylane://node/0123456789ABCDEF0123456789abcdef01234567")
                   .ok());
  EXPECT_FALSE(keylane::meta::ParseMetaPrincipal("keylane://meta/01").ok());
  EXPECT_FALSE(keylane::meta::ParseMetaPrincipal("keylane://operator/").ok());
  EXPECT_FALSE(keylane::meta::ParseMetaPrincipal("spiffe://node/1").ok());
}

TEST(MetaIdentitySecurity, CertificateMustCarryExactlyOneKeylanePrincipal) {
  EXPECT_FALSE(keylane::meta::AuthenticateMetaUriSans({}).ok());
  EXPECT_FALSE(keylane::meta::AuthenticateMetaUriSans(
                   std::vector<std::string>{"spiffe://unrelated/service"})
                   .ok());
  EXPECT_FALSE(
      keylane::meta::AuthenticateMetaUriSans(
          std::vector<std::string>{"keylane://meta/1", "keylane://meta/2"})
          .ok());
  auto identity =
      keylane::meta::AuthenticateMetaUriSans(std::vector<std::string>{
          "spiffe://unrelated/service", "keylane://meta/2"});
  ASSERT_TRUE(identity.ok()) << identity.status();
  EXPECT_EQ(identity->principal_, "keylane://meta/2");
}

TEST(MetaIdentitySecurity, RaftPeerClaimMatchesPersistedMemberBinding) {
  const keylane::meta::MetaMemberIdentity member{
      2, "keylane://meta/2", "10.0.0.2:7300", "10.0.0.2:7200"};
  const std::vector<std::string> sans{"keylane://meta/2"};
  EXPECT_TRUE(
      keylane::meta::VerifyRaftPeerIdentity(2, sans, member.EncodeAux()).ok());
  EXPECT_FALSE(
      keylane::meta::VerifyRaftPeerIdentity(3, sans, member.EncodeAux()).ok());
  EXPECT_FALSE(
      keylane::meta::VerifyRaftPeerIdentity(
          2, std::vector<std::string>{"keylane://meta/3"}, member.EncodeAux())
          .ok());
  EXPECT_FALSE(
      keylane::meta::VerifyRaftPeerIdentity(2, sans, "keylane://meta/2").ok());
}

TEST(MetaIdentitySecurity, MemberDescriptorRoundTrips) {
  const keylane::meta::MetaMemberIdentity member{
      7, "keylane://meta/7", "10.0.0.7:7300", "10.0.0.7:7200"};
  auto decoded =
      keylane::meta::MetaMemberIdentity::DecodeAux(member.EncodeAux());
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, member);
  EXPECT_TRUE(member.EncodeAux().starts_with("KMI2|"));
  EXPECT_FALSE(
      keylane::meta::MetaMemberIdentity::DecodeAux("KMI1|7|keylane://meta/7")
          .ok());
}

TEST(MetaIdentitySecurity, MemberDescriptorRejectsMissingOrNoncanonicalFields) {
  for (const keylane::meta::MetaMemberIdentity& invalid : {
           keylane::meta::MetaMemberIdentity{7, "keylane://meta/8",
                                             "10.0.0.7:7300", "10.0.0.7:7200"},
           keylane::meta::MetaMemberIdentity{7, "keylane://meta/7", "",
                                             "10.0.0.7:7200"},
           keylane::meta::MetaMemberIdentity{7, "keylane://meta/7",
                                             "localhost:7300", "10.0.0.7:7200"},
           keylane::meta::MetaMemberIdentity{7, "keylane://meta/7",
                                             "10.0.0.7:7300", ""},
       }) {
    EXPECT_FALSE(
        keylane::meta::MetaMemberIdentity::DecodeAux(invalid.EncodeAux()).ok());
  }
}

TEST(MetaIdentitySecurity, RbacKeepsDataNodeAtItsObservationBoundary) {
  auto node = keylane::meta::ParseMetaPrincipal(std::string("keylane://node/") +
                                                std::string(kNodeId));
  ASSERT_TRUE(node.ok());
  EXPECT_TRUE(keylane::meta::AuthorizeMetaAccess(
                  *node, keylane::meta::MetaAccess::kObservationWrite, kNodeId)
                  .ok());
  EXPECT_FALSE(keylane::meta::AuthorizeMetaAccess(
                   *node, keylane::meta::MetaAccess::kObservationWrite,
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                   .ok());
  EXPECT_FALSE(keylane::meta::AuthorizeMetaAccess(
                   *node, keylane::meta::MetaAccess::kPrivileged)
                   .ok());

  auto op = keylane::meta::ParseMetaPrincipal("keylane://operator/alice");
  ASSERT_TRUE(op.ok());
  EXPECT_TRUE(keylane::meta::AuthorizeMetaAccess(
                  *op, keylane::meta::MetaAccess::kPrivileged)
                  .ok());
}

TEST(MetaIdentitySecurity, UnixPeerMustBeOnTheExplicitUidAllowlist) {
  const std::vector<uid_t> allowed{1000, 1002};
  auto operator_identity =
      keylane::meta::AuthenticateLocalOperator(/*peer_uid=*/1002, allowed);
  ASSERT_TRUE(operator_identity.ok()) << operator_identity.status();
  EXPECT_EQ(operator_identity->role_,
            keylane::meta::MetaPrincipalRole::kOperator);
  EXPECT_EQ(operator_identity->principal_, "keylane://operator/uid-1002");

  auto rejected =
      keylane::meta::AuthenticateLocalOperator(/*peer_uid=*/1001, allowed);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), absl::StatusCode::kPermissionDenied);
}

TEST(MetaIdentitySecurity, TcpAdminSupportsPlaintextOrCompleteMtls) {
  keylane::meta::MetaCtlServerOptions options;
  options.transport_ =
      keylane::meta::MetaCtlServerOptions::Transport::kTcpPlaintext;
  options.bind_host_ = "127.0.0.1";
  options.port_ = 9000;
  options.local_ctl_endpoint_ = "127.0.0.1:9000";
  EXPECT_TRUE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());

  options.tls_ca_cert_file_ = "ca.pem";
  EXPECT_FALSE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());
  options.tls_cert_file_ = "server.pem";
  options.tls_key_file_ = "server.key";
  EXPECT_FALSE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());

  options.transport_ = keylane::meta::MetaCtlServerOptions::Transport::kTcpMtls;
  EXPECT_TRUE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());
  options.bind_host_ = "0.0.0.0";
  EXPECT_FALSE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());
  options.bind_host_ = "::";
  EXPECT_FALSE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());
  options.bind_host_ = "127.0.0.1";
  options.local_ctl_endpoint_ = "127.0.0.1:9001";
  EXPECT_FALSE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());
}

TEST(MetaIdentitySecurity, UnixAdminRequiresPathAndExplicitUid) {
  keylane::meta::MetaCtlServerOptions options;
  options.unix_socket_path_ = "/run/keylane/meta.sock";
  EXPECT_FALSE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());
  options.allowed_uids_.push_back(1000);
  EXPECT_TRUE(keylane::meta::MetaCtlServer::ValidateOptions(options).ok());
}

TEST(MetaCtlPolicyAdmin, PolicyVersionRequiresCanonicalPositiveDecimal) {
  std::uint64_t version = 0;
  EXPECT_TRUE(keylane::meta::detail::ParseAdminPolicyVersion("1", &version));
  EXPECT_EQ(version, 1u);
  EXPECT_TRUE(keylane::meta::detail::ParseAdminPolicyVersion(
      "18446744073709551615", &version));
  EXPECT_EQ(version, std::numeric_limits<std::uint64_t>::max());

  EXPECT_FALSE(keylane::meta::detail::ParseAdminPolicyVersion("0", &version));
  EXPECT_FALSE(keylane::meta::detail::ParseAdminPolicyVersion("01", &version));
  EXPECT_FALSE(
      keylane::meta::detail::ParseAdminPolicyVersion("0001", &version));
  EXPECT_FALSE(keylane::meta::detail::ParseAdminPolicyVersion(
      "18446744073709551616", &version));
}

TEST(MetaClusterStatusServiceTest, EnforcesSingleFlightAndRetainedBudget) {
  keylane::meta::MetaClusterStatusService service;
  EXPECT_TRUE(service.TryBeginCapture());
  EXPECT_FALSE(service.TryBeginCapture());
  service.EndCapture();
  EXPECT_TRUE(service.TryBeginCapture());
  service.EndCapture();

  constexpr std::size_t kTwoHundredMiB = 200u << 20;
  constexpr std::size_t kOneHundredMiB = 100u << 20;
  EXPECT_TRUE(service.TryRetain(kTwoHundredMiB));
  EXPECT_FALSE(service.TryBeginCapture());
  EXPECT_FALSE(service.TryRetain(kOneHundredMiB));
  service.Release(kTwoHundredMiB);
  EXPECT_TRUE(service.TryRetain(kOneHundredMiB));
  service.Release(kOneHundredMiB);
}

TEST(MetaClusterStatusRuntimeTest, HealthLossBeforeAckRemainsEncodable) {
  namespace control = keylane::cluster::control;
  using namespace keylane::meta;
  const std::string node_id(kNodeId);
  control::WireId128 assignment{};
  assignment[0] = 1;
  MetaCommittedStatusView view;
  MetaCommittedStatusGroup group;
  group.topology_.group_id_ = "group-a";
  group.topology_.record_.authority_version_ = 5;
  group.topology_.members_.push_back(
      {.node_id_ = node_id, .assignment_id_ = assignment});
  group.grant_.group_term_ = 4;
  group.grant_.grant_ = MetaGroupGrant{.owner_ = node_id,
                                       .term_ = 4,
                                       .authority_version_ = 5,
                                       .grant_revision_ = 6};
  view.groups_.push_back(std::move(group));
  const ClusterCaptureWireV1 capture{.responder_id_ = 1,
                                     .term_ = 7,
                                     .config_index_ = 8,
                                     .committed_index_ = 9,
                                     .topology_epoch_ = 3};
  MetaDataControlRuntimeNode runtime;
  runtime.node_id_ = node_id;
  runtime.boot_id_ = std::string(40, '2');
  runtime.leadership_generation_ = 11;
  runtime.health_ =
      control::HeartbeatHealth{.storage_ready = true, .population_ready = true};
  runtime.health_received_unix_ms_ = 1000;
  runtime.last_lease_decision_ =
      control::LeaseGranted{.leader_id = 1,
                            .raft_term = 7,
                            .leadership_generation = 11,
                            .data_boot_id = runtime.boot_id_,
                            .projection_hash = runtime.projection_hash_,
                            .group_id = "group-a",
                            .assignment_id = assignment,
                            .group_term = 4,
                            .authority_version = 5,
                            .grant_revision = 6,
                            .granted_duration_ms = 500};
  runtime.lease_decision_written_unix_ms_ = 1001;
  auto observe = [&](std::int64_t now_unix_ms) {
    ClusterDataNodeWireV1 node{.node_id_ = node_id,
                               .role_ = ClusterDataNodeRole::kPrimary,
                               .group_id_ = "group-a",
                               .current_session_ = true,
                               .projection_current_ = true};
    detail::ApplyClusterRuntimeObservation(node, runtime, view, capture,
                                           now_unix_ms, /*ttl_ms=*/500);
    return node;
  };
  EXPECT_EQ(observe(1001).lease_status_, ClusterLeaseStatus::kRecentlyGranted);
  auto expect_not_ready = [&](const ClusterDataNodeWireV1& node) {
    EXPECT_EQ(node.lease_status_, ClusterLeaseStatus::kUnknown);
    EXPECT_FALSE(node.population_current_);
    ClusterStatusWireV1 status;
    status.capture_ = capture;
    status.meta_available_ = true;
    status.meta_membership_stable_ = true;
    status.meta_members_.push_back({.server_id_ = 1, .is_leader_ = true});
    status.data_nodes_.push_back(node);
    status.groups_.push_back({.group_id_ = "group-a",
                              .term_ = 4,
                              .owner_node_id_ = node_id,
                              .config_epoch_ = 8,
                              .grant_revision_ = 6,
                              .effective_threshold_ms_ = 1'000});
    const auto encoded = EncodeClusterStatusReply(status);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    const auto decoded = DecodeClusterStatusReply(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_FALSE(decoded->cluster_ready_);
  };

  // These snapshots fall between receiving new health and finishing its Ack.
  // The previous granted decision is still present in each captured runtime.
  for (const auto health : {control::HeartbeatHealth{.storage_ready = false,
                                                     .population_ready = true},
                            control::HeartbeatHealth{.storage_ready = true,
                                                     .population_ready = false},
                            control::HeartbeatHealth{.storage_ready = true,
                                                     .population_ready = true,
                                                     .draining = true}}) {
    runtime.health_ = health;
    runtime.health_received_unix_ms_ = 1002;
    expect_not_ready(observe(1002));
  }
  // Ack completion can be newer than the health receipt. Health expiration
  // must also suppress the grant while its own freshness window still holds.
  runtime.health_ =
      control::HeartbeatHealth{.storage_ready = true, .population_ready = true};
  runtime.health_received_unix_ms_ = 1000;
  expect_not_ready(observe(1501));
}

TEST(MetaClusterStatusBracketTest, RejectsEveryMixedAuthorityCut) {
  using keylane::meta::detail::IsStableClusterStatusBracket;
  using keylane::meta::detail::MetaClusterStatusBracket;
  MetaClusterStatusBracket before{
      .is_leader_ = true,
      .leader_alive_ = true,
      .term_ = 7,
      .config_index_ = 11,
      .config_server_ids_ = {1, 2, 3},
      .active_meta_members_ =
          {
              {.server_id_ = 1,
               .principal_ = "keylane://meta/1",
               .data_control_endpoint_ = "127.0.0.1:7001",
               .ctl_endpoint_ = "127.0.0.1:7101"},
          },
      .leadership_ = {.leadership_generation_ = 5,
                      .leader_authority_eligible_ = true,
                      .leader_authority_eligibility_revision_ = 9},
  };
  EXPECT_TRUE(IsStableClusterStatusBracket(before, before));

  auto expect_changed = [&](auto mutate) {
    MetaClusterStatusBracket after = before;
    mutate(after);
    EXPECT_FALSE(IsStableClusterStatusBracket(before, after));
  };
  expect_changed([](auto& value) { value.is_leader_ = false; });
  expect_changed([](auto& value) { value.leader_alive_ = false; });
  expect_changed([](auto& value) { ++value.term_; });
  expect_changed([](auto& value) { ++value.config_index_; });
  expect_changed([](auto& value) { value.config_server_ids_.pop_back(); });
  expect_changed([](auto& value) {
    value.active_meta_members_.front().ctl_endpoint_ = "127.0.0.1:7199";
  });
  expect_changed(
      [](auto& value) { ++value.leadership_.leadership_generation_; });
  expect_changed([](auto& value) {
    value.leadership_.leader_authority_eligible_ = false;
  });
  expect_changed([](auto& value) {
    ++value.leadership_.leader_authority_eligibility_revision_;
  });
}

TEST(MetaClusterStatusBracketTest,
     RejectsDetectorStateFromBeforeAnEligibilityAba) {
  keylane::meta::MetaDataControlRuntimeSnapshot runtime{
      .leadership_generation_ = 5,
      .leader_authority_eligible_ = true,
      .leader_authority_eligibility_revision_ = 3,
  };
  keylane::meta::MetaAutomaticFailoverDiagnosticsSnapshot detector{
      .leadership_generation_ = 5,
      .leader_authority_eligibility_revision_ = 1,
      .evaluated_applied_index_ = 17,
  };
  EXPECT_FALSE(keylane::meta::detail::IsCurrentAutomaticFailoverDiagnostics(
      runtime, detector, /*committed_applied_index=*/17));
  detector.leader_authority_eligibility_revision_ = 3;
  EXPECT_TRUE(keylane::meta::detail::IsCurrentAutomaticFailoverDiagnostics(
      runtime, detector, /*committed_applied_index=*/17));
  ++detector.evaluated_applied_index_;
  EXPECT_FALSE(keylane::meta::detail::IsCurrentAutomaticFailoverDiagnostics(
      runtime, detector, /*committed_applied_index=*/17));
}

TEST(MetaIdentitySecurity, RegistrationRejectsPrincipalForAnotherNode) {
  keylane::meta::MetaIdentityStore store;
  keylane::meta::RegisterNode command;
  command.node_id_ = std::string(kNodeId);
  command.principal_ =
      "keylane://node/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  EXPECT_FALSE(store.Apply(command).ok());
  EXPECT_EQ(store.NodeCount(), 0);
}

TEST(MetaIdentitySecurity,
     MetaMemberBindingIsCommittedAndRetirementIsTerminal) {
  keylane::meta::MetaIdentityStore store;
  keylane::meta::BindMetaMember bind;
  bind.server_id_ = 7;
  bind.principal_ = "keylane://meta/7";
  bind.data_control_endpoint_ = "10.0.0.7:7100";
  bind.ctl_endpoint_ = "10.0.0.7:7200";
  ASSERT_TRUE(store.Apply(bind).ok());

  auto member = store.FindMetaMember(7);
  ASSERT_TRUE(member.has_value());
  EXPECT_EQ(member->principal_, "keylane://meta/7");
  EXPECT_EQ(member->data_control_endpoint_, "10.0.0.7:7100");
  EXPECT_EQ(member->ctl_endpoint_, std::optional<std::string>("10.0.0.7:7200"));
  EXPECT_FALSE(member->retired_);
  EXPECT_TRUE(store.Apply(bind).ok());

  keylane::meta::RetireMetaMember retire;
  retire.server_id_ = 7;
  ASSERT_TRUE(store.Apply(retire).ok());
  member = store.FindMetaMember(7);
  ASSERT_TRUE(member.has_value());
  EXPECT_TRUE(member->retired_);
  EXPECT_TRUE(store.Apply(retire).ok());

  EXPECT_FALSE(store.Apply(bind).ok());
  keylane::meta::BindMetaMember reused = bind;
  reused.server_id_ = 8;
  EXPECT_FALSE(store.Apply(reused).ok());
}

TEST(MetaIdentitySecurity, MetaMemberBindingSurvivesSnapshotRoundTrip) {
  keylane::meta::MetaIdentityStore store;
  keylane::meta::BindMetaMember bind;
  bind.server_id_ = 3;
  bind.principal_ = "keylane://meta/3";
  bind.data_control_endpoint_ = "10.0.0.3:7100";
  bind.ctl_endpoint_ = "10.0.0.3:7200";
  ASSERT_TRUE(store.Apply(bind).ok());

  auto restored =
      keylane::meta::MetaIdentityStore::Deserialize(store.Serialize());
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->FindMetaMember(3), store.FindMetaMember(3));
}

TEST(MetaIdentitySecurity, SoleMemberCtlEndpointCanOnlyBeCompletedOnce) {
  keylane::meta::MetaIdentityStore store;
  keylane::meta::BindMetaMember bind;
  bind.server_id_ = 1;
  bind.principal_ = "keylane://meta/1";
  bind.data_control_endpoint_ = "10.0.0.1:7100";
  ASSERT_TRUE(store.Apply(bind).ok());

  bind.ctl_endpoint_ = "10.0.0.1:7200";
  ASSERT_TRUE(store.Apply(bind).ok());
  ASSERT_TRUE(store.FindMetaMember(1).has_value());
  EXPECT_EQ(store.FindMetaMember(1)->ctl_endpoint_, bind.ctl_endpoint_);

  bind.ctl_endpoint_ = "10.0.0.1:7300";
  EXPECT_FALSE(store.Apply(bind).ok());
  EXPECT_EQ(store.FindMetaMember(1)->ctl_endpoint_,
            std::optional<std::string>("10.0.0.1:7200"));
}

}  // namespace
