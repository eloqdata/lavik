#include "meta/nuraft_rpc_listener.h"

#include <cstddef>
#include <span>
#include <utility>
#include <vector>

#include "celer/io/storage.h"
#include "celer/net/connection.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/net/tls.h"
#include "celer/runtime/worker.h"
// NuRaft's headers are not -Wpedantic-clean; see nuraft_scheduler.h.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/req_msg.hxx"
#include "libnuraft/resp_msg.hxx"
#pragma GCC diagnostic pop

namespace keylane::meta {

// Worker-affined listener state. All non-atomic members are touched only by
// worker coroutines; the shared_ptr is what lets sessions outlive the
// NuRaft-facing listener object.
struct NuraftRpcListener::ListenerCore {
  MetaTransportConfig config_;
  std::shared_ptr<celer::TlsContext> tls_context_;  // nullptr = plaintext
  IdentityVerifier identity_verifier_;
  std::string bind_host_;
  std::uint16_t port_ = 0;

  mutable std::mutex status_mu_;
  absl::Status listen_status_ = absl::Status(
      absl::StatusCode::kUnavailable, "bind has not run on the worker yet");

  // Worker-thread only below.
  celer::Worker* worker_ = nullptr;
  celer::TcpListener listener_;
  bool listening_ = false;
  nuraft::ptr<nuraft::msg_handler> handler_;
  // Live session connections, so shutdown() can close them. Entries are
  // removed by the session coroutines themselves when they exit.
  std::vector<celer::Connection*> sessions_;
};

celer::Task<absl::Status> NuraftRpcListener::SessionLoop(
    ListenerCorePtr core, celer::TcpStream stream,
    celer::Connection* connection, std::vector<std::string> uri_sans) {
  std::byte chunk[16384];
  std::vector<std::byte> pending;
  bool identity_verified = false;
  bool schema_negotiated = false;
  wire::SchemaRange peer_schema;
  while (true) {
    auto read = co_await stream.ReadSome(chunk);
    if (!read.ok() || *read == 0) {
      break;  // peer EOF, shutdown() close, or worker teardown
    }
    pending.insert(pending.end(), chunk, chunk + *read);

    // Answer every complete frame, strictly in order: clients match responses
    // to requests FIFO, so responses on one connection must not be reordered.
    std::size_t pos = 0;
    bool drop = false;
    while (pending.size() - pos >= wire::kHeaderBytes) {
      auto header = wire::ParseHeader(
          std::span<const std::byte>(pending.data() + pos, wire::kHeaderBytes));
      if (!header.ok() ||
          header->payload_bytes_ > core->config_.max_frame_bytes_) {
        drop = true;  // framing desync is unrecoverable
        break;
      }
      if (pending.size() - pos < wire::kHeaderBytes + header->payload_bytes_) {
        break;  // incomplete frame; read more
      }
      const std::span<const std::byte> payload(
          pending.data() + pos + wire::kHeaderBytes, header->payload_bytes_);
      if (!schema_negotiated) {
        if (header->kind_ != wire::FrameKind::kSchemaHello) {
          drop = true;
          break;
        }
        auto range = wire::DecodeSchemaRange(payload);
        if (!range.ok() || range->min_schema_ > core->config_.max_schema_ ||
            range->max_schema_ < core->config_.min_schema_) {
          drop = true;
          break;
        }
        auto ack = wire::EncodeSchemaRange(
            wire::FrameKind::kSchemaAck,
            wire::SchemaRange{core->config_.min_schema_,
                              core->config_.max_schema_});
        if (!ack.ok()) {
          drop = true;
          break;
        }
        const absl::Status ack_written = co_await stream.WriteAll(*ack);
        if (!ack_written.ok()) {
          drop = true;
          break;
        }
        peer_schema = *range;
        schema_negotiated = true;
        pos += wire::kHeaderBytes + header->payload_bytes_;
        continue;
      }
      if (header->kind_ != wire::FrameKind::kRequest) {
        drop = true;
        break;
      }
      auto req = wire::DecodeRequest(payload);
      if (!req.ok() || core->handler_ == nullptr) {
        drop = true;  // corrupt payload, or shutdown() released the handler
        break;
      }
      pos += wire::kHeaderBytes + header->payload_bytes_;

      if (!identity_verified && core->identity_verifier_) {
        const absl::Status identity =
            core->identity_verifier_((*req)->get_src(), uri_sans, peer_schema);
        if (!identity.ok()) {
          drop = true;
          break;
        }
        identity_verified = true;
      }

      // Synchronous, on the worker thread: process_req only takes NuRaft's
      // internal locks. Same threading model as the asio listener.
      nuraft::ptr<nuraft::resp_msg> resp =
          raft_server_handler::process_req(core->handler_.get(), **req);
      if (resp == nullptr) {
        drop = true;
        break;
      }
      if (resp->has_cb()) {
        resp = resp->call_cb(resp);
      }
      if (resp->has_async_cb()) {
        // Only reachable with raft auto-forwarding, which keylane_meta keeps
        // off; blocking the worker on cmd_result::get() would stall every
        // session, so drop instead.
        drop = true;
        break;
      }
      auto frame = wire::EncodeResponse(*resp);
      if (!frame.ok()) {
        drop = true;
        break;
      }
      const absl::Status written = co_await stream.WriteAll(*frame);
      if (!written.ok()) {
        drop = true;
        break;
      }
    }
    if (drop) {
      break;
    }
    pending.erase(pending.begin(),
                  pending.begin() + static_cast<std::ptrdiff_t>(pos));
  }

  std::vector<celer::Connection*>& sessions = core->sessions_;
  for (auto it = sessions.begin(); it != sessions.end(); ++it) {
    if (*it == connection) {
      *it = sessions.back();
      sessions.pop_back();
      break;
    }
  }
  (void)stream.Close();
  co_return absl::OkStatus();
}

celer::Task<absl::Status> NuraftRpcListener::AcceptLoop(ListenerCorePtr core) {
  celer::Worker& worker = *core->worker_;
  while (core->listening_) {
    auto accepted = co_await core->listener_.Accept();
    if (!accepted.ok()) {
      // A closed listener fails the pending accept; that is the stop() path.
      if (!core->listening_) {
        break;
      }
      // Transient accept failure (e.g. EMFILE): back off so the loop cannot
      // spin, then retry — the accept state re-arms itself.
      co_await celer::SleepFor(worker, std::chrono::milliseconds(10));
      continue;
    }
    celer::Connection* connection = *accepted;
    celer::TcpStream stream(connection);
    std::vector<std::string> uri_sans;
    // Bounded ingress: without read-ahead, kernel socket buffers are the
    // backpressure mechanism for slow/flooding peers. Must precede the first
    // read, including the TLS handshake's.
    (void)stream.SetReadAhead(false);
    if (core->tls_context_ != nullptr) {
      const absl::Status tls =
          co_await stream.StartTls(core->tls_context_, /*server=*/true);
      if (!tls.ok()) {
        (void)stream.Close();
        continue;
      }
      auto sans = stream.PeerCertificateUriSans();
      if (!sans.ok()) {
        (void)stream.Close();
        continue;
      }
      uri_sans = std::move(*sans);
    }
    core->sessions_.push_back(connection);
    worker.Spawn(
        SessionLoop(core, std::move(stream), connection, std::move(uri_sans)));
  }
  co_return absl::OkStatus();
}

void NuraftRpcListener::CloseListener(ListenerCore& core) {
  core.listening_ = false;
  (void)core.listener_.Close();
}

// static
absl::StatusOr<std::shared_ptr<NuraftRpcListener>> NuraftRpcListener::Create(
    std::shared_ptr<MetaCelerBridge> bridge, MetaTransportConfig config,
    std::string bind_host, std::uint16_t port) {
  if (bridge == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "bridge must not be null");
  }
  if (port == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "listener port must be non-zero");
  }
  std::shared_ptr<celer::TlsContext> tls_context;
  if (config.TlsEnabled()) {
    celer::TlsServerOptions options;
    options.cert_file_ = config.tls_cert_file_;
    options.key_file_ = config.tls_key_file_;
    options.ca_cert_file_ = config.tls_ca_cert_file_;
    // mTLS: peers must present a certificate chaining to the same CA.
    options.client_auth_ = celer::TlsClientAuth::kRequired;
    auto context = celer::TlsContext::CreateServer(options);
    if (!context.ok()) {
      return context.status();
    }
    tls_context = std::move(*context);
  }
  auto core = std::make_shared<ListenerCore>();
  core->config_ = std::move(config);
  core->tls_context_ = std::move(tls_context);
  core->bind_host_ = std::move(bind_host);
  core->port_ = port;
  return std::shared_ptr<NuraftRpcListener>(
      new NuraftRpcListener(std::move(bridge), std::move(core)));
}

NuraftRpcListener::~NuraftRpcListener() { PostShutdown(); }

void NuraftRpcListener::listen(nuraft::ptr<nuraft::msg_handler>& handler) {
  ListenerCorePtr core = core_;
  bridge_->Post([core, handler](celer::Worker& worker) mutable {
    if (core->listening_) {
      return;
    }
    core->worker_ = &worker;
    core->handler_ = std::move(handler);
    absl::Status bound = core->listener_.Bind(&worker, core->bind_host_,
                                              core->port_, /*backlog=*/128,
                                              /*reuse_port=*/false);
    {
      std::lock_guard<std::mutex> lock(core->status_mu_);
      core->listen_status_ = bound;
    }
    if (!bound.ok()) {
      return;
    }
    core->listening_ = true;
    worker.Spawn(AcceptLoop(core));
  });
}

void NuraftRpcListener::stop() {
  ListenerCorePtr core = core_;
  bridge_->Post([core](celer::Worker& /*worker*/) {
    // asio parity: stop() closes the acceptor; live sessions run on.
    CloseListener(*core);
  });
}

void NuraftRpcListener::shutdown() { PostShutdown(); }

void NuraftRpcListener::PostShutdown() {
  ListenerCorePtr core = core_;
  bridge_->Post([core](celer::Worker& worker) {
    CloseListener(*core);
    // Close live sessions; their coroutines resume with I/O errors and
    // remove themselves from sessions_ as they exit.
    for (celer::Connection* connection : core->sessions_) {
      worker.BeginClose(
          connection,
          absl::Status(absl::StatusCode::kCancelled, "listener shutdown"),
          celer::CloseMode::kLocalClose);
    }
    core->handler_.reset();
  });
}

absl::Status NuraftRpcListener::listen_status() const {
  std::lock_guard<std::mutex> lock(core_->status_mu_);
  return core_->listen_status_;
}

void NuraftRpcListener::SetIdentityVerifier(IdentityVerifier verifier) {
  core_->identity_verifier_ = std::move(verifier);
}

}  // namespace keylane::meta
