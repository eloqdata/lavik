#include "keylane/meta/nuraft_asio_transport.h"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/identity_verifier.h"
#include "keylane/meta/nuraft_state_mgr.h"
#include "keylane/meta/state_machine.h"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/msg_type.hxx"
#include "libnuraft/srv_config.hxx"
#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {

bool IsPristineJoinExchange(int type) {
  return type == static_cast<int>(nuraft::msg_type::join_cluster_request) ||
         type == static_cast<int>(nuraft::msg_type::join_cluster_response);
}

struct SslContexts {
  ~SslContexts() {
    if (server_ != nullptr) SSL_CTX_free(server_);
    if (client_ != nullptr) SSL_CTX_free(client_);
  }
  SSL_CTX* server_ = nullptr;
  SSL_CTX* client_ = nullptr;
};

std::string OpenSslError(std::string_view operation) {
  const unsigned long code = ERR_get_error();
  if (code == 0) return std::string(operation);
  std::array<char, 256> text{};
  ERR_error_string_n(code, text.data(), text.size());
  return absl::StrCat(operation, ": ", text.data());
}

absl::StatusOr<SSL_CTX*> MakeSslContext(const MetaAsioTransportConfig& config,
                                        bool server) {
  SSL_CTX* ctx =
      SSL_CTX_new(server ? TLS_server_method() : TLS_client_method());
  if (ctx == nullptr) return absl::InternalError(OpenSslError("SSL_CTX_new"));
  const auto fail =
      [&](std::string_view operation) -> absl::StatusOr<SSL_CTX*> {
    const std::string message = OpenSslError(operation);
    SSL_CTX_free(ctx);
    return absl::InvalidArgumentError(message);
  };
  if (SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION) != 1) {
    return fail("set TLS minimum version");
  }
  if (SSL_CTX_use_certificate_chain_file(ctx, config.tls_cert_file_.c_str()) !=
      1) {
    return fail("load TLS certificate chain");
  }
  if (SSL_CTX_use_PrivateKey_file(ctx, config.tls_key_file_.c_str(),
                                  SSL_FILETYPE_PEM) != 1) {
    return fail("load TLS private key");
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    return fail("check TLS private key");
  }
  if (SSL_CTX_load_verify_locations(ctx, config.tls_ca_cert_file_.c_str(),
                                    nullptr) != 1) {
    return fail("load TLS CA");
  }
  SSL_CTX_set_verify(
      ctx, SSL_VERIFY_PEER | (server ? SSL_VERIFY_FAIL_IF_NO_PEER_CERT : 0),
      nullptr);
  return ctx;
}

std::optional<MetaMemberIdentity> FindConfiguredMember(
    const nuraft::ptr<NuraftStateMgr>& state_mgr, std::int32_t server_id) {
  const nuraft::ptr<nuraft::cluster_config> config = state_mgr->load_config();
  if (config == nullptr) return std::nullopt;
  for (const nuraft::ptr<nuraft::srv_config>& member : config->get_servers()) {
    if (member == nullptr || member->get_id() != server_id) continue;
    auto decoded = MetaMemberIdentity::DecodeAux(member->get_aux());
    if (!decoded.ok()) return std::nullopt;
    return *decoded;
  }
  return std::nullopt;
}

absl::StatusOr<bool> ConfigBindingsConverged(
    const nuraft::ptr<nuraft::cluster_config>& config,
    const MetaStores& stores) {
  if (config == nullptr) return false;
  const auto members = stores.identity_.MetaMembers();
  const std::size_t active_bindings = std::count_if(
      members.begin(), members.end(),
      [](const auto& member) { return !member.retired_; });
  if (active_bindings != config->get_servers().size()) {
    return false;
  }
  for (const auto& member : config->get_servers()) {
    if (member == nullptr) {
      return absl::DataLossError("null member in durable Raft config");
    }
    auto descriptor = MetaMemberIdentity::DecodeAux(member->get_aux());
    if (!descriptor.ok() || descriptor->server_id_ != member->get_id()) {
      return absl::DataLossError("invalid descriptor in durable Raft config");
    }
    const auto binding = stores.identity_.FindMetaMember(
        static_cast<std::uint32_t>(member->get_id()));
    if (!binding.has_value()) return false;
    if (binding->retired_ || binding->principal_ != descriptor->principal_ ||
        binding->data_control_endpoint_ !=
            descriptor->data_control_endpoint_ ||
        binding->ctl_endpoint_ !=
            std::optional<std::string>(descriptor->ctl_endpoint_)) {
      return absl::PermissionDeniedError(
          "Raft config descriptor conflicts with identity binding");
    }
  }
  return true;
}

absl::Status VerifyPeer(const nuraft::asio_service::meta_cb_params& params,
                        std::span<const std::string> uri_sans,
                        const nuraft::ptr<NuraftStateMgr>& state_mgr,
                        const nuraft::ptr<MetaStateMachine>& state_machine,
                        bool tls_enabled) {
  if (params.src_id_ <= 0 || params.dst_id_ != state_mgr->server_id()) {
    return absl::PermissionDeniedError(
        "Raft RPC source/destination is invalid");
  }
  const auto configured = FindConfiguredMember(state_mgr, params.src_id_);
  if (!configured.has_value()) {
    if (!IsPristineJoinExchange(params.msg_type_)) {
      return absl::PermissionDeniedError("Raft RPC source is not configured");
    }
    const MetaStores stores = state_machine->StoresSnapshot();
    const auto pending = stores.identity_.FindMetaMember(
        static_cast<std::uint32_t>(params.src_id_));
    if (pending.has_value() && !pending->retired_) {
      if (tls_enabled) {
        if (!pending->ctl_endpoint_.has_value()) {
          return absl::PermissionDeniedError(
              "joining Raft source has an incomplete identity binding");
        }
        const MetaMemberIdentity expected{params.src_id_, pending->principal_,
                                          pending->data_control_endpoint_,
                                          *pending->ctl_endpoint_};
        const absl::Status certificate = VerifyRaftPeerIdentity(
            params.src_id_, uri_sans, expected.EncodeAux());
        if (!certificate.ok()) return certificate;
      }
      return absl::OkStatus();
    }
    if (!state_mgr->waiting_joiner_catchup_pending() ||
        state_machine->last_commit_index() != 0) {
      return absl::PermissionDeniedError(
          "joining Raft source has no committed identity binding");
    }
    if (tls_enabled) {
      auto identity = AuthenticateMetaUriSans(uri_sans);
      if (!identity.ok()) return identity.status();
      if (identity->role_ != MetaPrincipalRole::kMetaMember ||
          identity->subject_id_ != std::to_string(params.src_id_)) {
        return absl::PermissionDeniedError(
            "join certificate does not match Raft source id");
      }
    }
    return absl::OkStatus();
  }
  if (tls_enabled) {
    const absl::Status certificate = VerifyRaftPeerIdentity(
        params.src_id_, uri_sans, configured->EncodeAux());
    if (!certificate.ok()) return certificate;
  }
  const MetaStores stores = state_machine->StoresSnapshot();
  if (state_mgr->initial_bindings_pending() ||
      state_mgr->waiting_joiner_catchup_pending()) {
    const auto durable_config = state_mgr->load_config();
    auto converged = ConfigBindingsConverged(durable_config, stores);
    if (!converged.ok()) return converged.status();
    if (*converged) {
      if (state_mgr->initial_bindings_pending()) {
        if (absl::Status status = state_mgr->CompleteInitialBindings(
                state_machine->last_commit_index());
            !status.ok()) {
          return status;
        }
      }
      if (state_mgr->waiting_joiner_catchup_pending() &&
          durable_config != nullptr &&
          durable_config->get_server(state_mgr->server_id()) != nullptr &&
          state_machine->last_commit_index() >=
              durable_config->get_log_idx()) {
        if (absl::Status status = state_mgr->CompleteWaitingJoinerCatchup(
                state_machine->last_commit_index());
            !status.ok()) {
          return status;
        }
      }
    }
  }
  const auto committed = stores.identity_.FindMetaMember(
      static_cast<std::uint32_t>(params.src_id_));
  if (!committed.has_value()) {
    // Lifecycle markers bridge first convergence. On ordinary restart, the
    // reusable baseline authorizes only the exact descriptor set that had
    // already converged, and only below its durable replay watermark.
    const auto config = state_mgr->load_config();
    if (config != nullptr &&
        (state_mgr->initial_bindings_pending() ||
         (state_mgr->waiting_joiner_catchup_pending() &&
          state_machine->last_commit_index() < config->get_log_idx()) ||
         state_mgr->transport_binding_replay_pending(
             state_machine->last_commit_index()))) {
      return absl::OkStatus();
    }
    return absl::PermissionDeniedError("Raft member has no committed binding");
  }
  if (committed->retired_ || committed->principal_ != configured->principal_ ||
      committed->data_control_endpoint_ != configured->data_control_endpoint_ ||
      committed->ctl_endpoint_ !=
          std::optional<std::string>(configured->ctl_endpoint_)) {
    return absl::PermissionDeniedError(
        "Raft member binding is retired or differs from configuration");
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<nuraft::asio_service::options> BuildMetaAsioOptions(
    const MetaAsioTransportConfig& config,
    nuraft::ptr<NuraftStateMgr> state_mgr,
    nuraft::ptr<MetaStateMachine> state_machine) {
  if (config.bind_address_.empty() || config.io_threads_ == 0 ||
      config.max_rpc_payload_bytes_ == 0) {
    return absl::InvalidArgumentError("invalid Meta Asio transport options");
  }
  nuraft::asio_service::options options;
  options.thread_pool_size_ = config.io_threads_;
  options.listen_address_ = config.bind_address_;
  options.max_rpc_payload_bytes_ = config.max_rpc_payload_bytes_;
  options.enable_ssl_ = config.TlsEnabled();
  options.skip_verification_ = false;

  if (config.TlsEnabled()) {
    auto contexts = std::make_shared<SslContexts>();
    auto server = MakeSslContext(config, /*server=*/true);
    if (!server.ok()) return server.status();
    contexts->server_ = *server;
    auto client = MakeSslContext(config, /*server=*/false);
    if (!client.ok()) return client.status();
    contexts->client_ = *client;
    options.ssl_context_provider_server_ = [contexts] {
      return std::exchange(contexts->server_, nullptr);
    };
    options.ssl_context_provider_client_ = [contexts] {
      return std::exchange(contexts->client_, nullptr);
    };
  }

  options.verify_rpc_peer_ =
      [state_mgr, state_machine, tls_enabled = config.TlsEnabled()](
          const auto& params, const std::vector<std::string>& uri_sans) {
        const absl::Status status =
            VerifyPeer(params, uri_sans, state_mgr, state_machine, tls_enabled);
        if (!status.ok()) {
          spdlog::warn("rejected Raft peer: {}", status.ToString());
        }
        return status.ok();
      };
  return options;
}

}  // namespace keylane::meta
