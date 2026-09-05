#pragma once

// NuRaft rpc_listener on celer's TcpListener (+ optional mTLS).
//
// THREAD MODEL
//
// listen()/stop()/shutdown() are called from user/NuRaft threads (the raft
// launcher and raft_server teardown), never from a celer worker. Each of them
// only posts a closure through the MetaCelerBridge (see nuraft_scheduler.h
// for the bridge contract); the actual bind/accept/close happens on the
// worker. The bridge's FIFO inbox preserves the caller's stop()->listen()
// ordering.
//
// One accept loop coroutine runs on the worker; each accepted connection gets
// one session coroutine. A session reads request frames, calls
// raft_server_handler::process_req() — synchronously, on the celer worker
// thread (asio does the same on its io_service threads; process_req only
// takes NuRaft's internal locks) — and writes the serialized response back.
// Requests on one connection are answered strictly in order because clients
// match responses to requests FIFO. resp_msg::has_cb() transforms are applied
// synchronously like asio does. resp_msg::has_async_cb() (only reachable with
// raft auto-forwarding, which keylane_meta keeps off) is NOT supported: the
// session is closed instead of stalling the worker on cmd_result::get().
//
// INGRESS BACKPRESSURE
//
// Accepted streams run with SetReadAhead(false): celer then does not prefetch
// receive buffers while the session is not reading, so a slow or flooding
// peer is throttled by TCP flow control instead of piling bytes into
// per-connection buffers. Frames larger than
// MetaTransportConfig::max_frame_bytes_ are rejected and the session is
// dropped — a framing desync is never recoverable.
//
// LIFECYCLE AND OWNERSHIP
//
// The NuRaft-facing NuraftRpcListener is a thin handle; all state lives in a
// heap ListenerCore shared with the worker coroutines via shared_ptr, so
// destroying the listener (after shutdown(), like asio's listener teardown)
// cannot strand running sessions. stop() mirrors asio_rpc_listener::stop():
// it closes the acceptor and lets in-flight sessions finish. shutdown()
// additionally closes every active session and releases the msg_handler —
// the release runs on the worker thread, so if the listener held the last
// raft_server reference, ~raft_server runs there (NuRaft teardown only
// touches its own members plus scheduler cancels; both are safe here).
// Because stop() closes the acceptor for good, a later listen() re-binds the
// port (NuRaft never re-listens; asio silently cannot).

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
// NuRaft's headers are not -Wpedantic-clean; see nuraft_scheduler.h.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/ptr.hxx"
#include "libnuraft/raft_server_handler.hxx"
#include "libnuraft/rpc_listener.hxx"
#pragma GCC diagnostic pop

#include "meta/nuraft_rpc_client.h"

namespace celer {
class Connection;
class TcpStream;
}  // namespace celer

namespace keylane::meta {

class NuraftRpcListener : public nuraft::rpc_listener,
                          public nuraft::raft_server_handler {
 public:
  // Builds the TLS server context (client certs required) when TLS is
  // configured, so bad cert paths fail at startup. bind_host must be numeric
  // ("0.0.0.0", "::", ...) or empty for the wildcard address.
  static absl::StatusOr<std::shared_ptr<NuraftRpcListener>> Create(
      std::shared_ptr<MetaCelerBridge> bridge, MetaTransportConfig config,
      std::string bind_host, std::uint16_t port);

  // Posts shutdown() if it never happened, so sessions cannot outlive the
  // NuRaft-facing object with a dangling handler.
  ~NuraftRpcListener() override;

  // Connections are accepted only after listen(). The bind itself runs on the
  // worker asynchronously; check listen_status() afterwards.
  void listen(nuraft::ptr<nuraft::msg_handler>& handler) override;
  void stop() override;
  void shutdown() override;

  // Result of the asynchronous bind: kUnavailable until the worker reports.
  absl::Status listen_status() const;

  // Assembly-time hook invoked once, on the first decoded request of each
  // session. It binds the request's claimed source id, certificate URI SANs
  // when TLS is enabled, and transport-advertised binary schema range to the
  // latest committed member configuration before process_req.
  using IdentityVerifier = std::function<absl::Status(
      std::int32_t, std::span<const std::string>, wire::SchemaRange)>;
  void SetIdentityVerifier(IdentityVerifier verifier);

 private:
  struct ListenerCore;
  using ListenerCorePtr = std::shared_ptr<ListenerCore>;

  NuraftRpcListener(std::shared_ptr<MetaCelerBridge> bridge,
                    ListenerCorePtr core)
      : bridge_(std::move(bridge)), core_(std::move(core)) {}

  // SessionLoop must be a member: raft_server_handler::process_req is a
  // protected static, reachable only from derived-class members.
  static celer::Task<absl::Status> AcceptLoop(ListenerCorePtr core);
  static celer::Task<absl::Status> SessionLoop(
      ListenerCorePtr core, celer::TcpStream stream,
      celer::Connection* connection, std::vector<std::string> uri_sans);
  static void CloseListener(ListenerCore& core);

  void PostShutdown();

  std::shared_ptr<MetaCelerBridge> bridge_;
  ListenerCorePtr core_;
};

}  // namespace keylane::meta
