#include <sys/types.h>

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/ctl_server.h"
#include "keylane/meta/identity_store.h"
#include "keylane/meta/identity_verifier.h"

namespace {

constexpr std::string_view kNodeId = "0123456789abcdef0123456789abcdef01234567";

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
  const keylane::meta::MetaMemberIdentity member{2, "keylane://meta/2"};
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
  const keylane::meta::MetaMemberIdentity member{7, "keylane://meta/7"};
  auto decoded =
      keylane::meta::MetaMemberIdentity::DecodeAux(member.EncodeAux());
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->server_id_, 7);
  EXPECT_EQ(decoded->principal_, "keylane://meta/7");
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

TEST(MetaClusterStatusBracketTest, RejectsEveryMixedAuthorityCut) {
  using keylane::meta::detail::IsStableClusterStatusBracket;
  using keylane::meta::detail::MetaClusterStatusBracket;
  MetaClusterStatusBracket before{
      .is_leader_ = true,
      .leader_alive_ = true,
      .term_ = 7,
      .config_index_ = 11,
      .config_server_ids_ = {1, 2, 3},
      .active_meta_members_ = {
          {.server_id_ = 1,
           .principal_ = "keylane://meta/1",
           .data_control_endpoint_ = "127.0.0.1:7001",
           .ctl_endpoint_ = "127.0.0.1:7101"},
      },
      .leadership_ = {.leadership_generation_ = 5,
                      .leader_authority_eligible_ = true},
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
  EXPECT_EQ(member->ctl_endpoint_,
            std::optional<std::string>("10.0.0.7:7200"));
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
