#include "meta/nuraft_rpc_client.h"

#include <arpa/inet.h>

#include <charconv>
#include <cstring>
#include <deque>
#include <optional>
#include <utility>

#include "celer/io/storage.h"
#include "celer/net/tcp_stream.h"
#include "celer/net/tls.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "meta/meta_identity_verifier.h"
// NuRaft's headers are not -Wpedantic-clean; see nuraft_scheduler.h.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/async.hxx"
#include "libnuraft/buffer.hxx"
#include "libnuraft/log_entry.hxx"
#include "libnuraft/msg_type.hxx"
#include "libnuraft/rpc_exception.hxx"
#pragma GCC diagnostic pop

namespace keylane::meta {
namespace {

// ---------------------------------------------------------------------------
// Little-endian codec helpers. Both ends run this adapter, so LE is a fixed
// property of the format rather than a negotiated one.
// ---------------------------------------------------------------------------

void PutU8(std::vector<std::byte>& out, std::uint64_t value) {
  out.push_back(static_cast<std::byte>(value & 0xffu));
}

void PutU16(std::vector<std::byte>& out, std::uint64_t value) {
  for (unsigned shift = 0; shift < 16; shift += 8) {
    out.push_back(static_cast<std::byte>((value >> shift) & 0xffu));
  }
}

void PutU32(std::vector<std::byte>& out, std::uint64_t value) {
  for (unsigned shift = 0; shift < 32; shift += 8) {
    out.push_back(static_cast<std::byte>((value >> shift) & 0xffu));
  }
}

void PutU64(std::vector<std::byte>& out, std::uint64_t value) {
  for (unsigned shift = 0; shift < 64; shift += 8) {
    out.push_back(static_cast<std::byte>((value >> shift) & 0xffu));
  }
}

void PutBytes(std::vector<std::byte>& out, const void* data, std::size_t size) {
  const auto* bytes = static_cast<const std::byte*>(data);
  out.insert(out.end(), bytes, bytes + size);
}

// Bounds-checking cursor over one payload. Every failed read latches the
// error so callers can chain reads and check once at the end.
class Reader {
 public:
  explicit Reader(std::span<const std::byte> data) : data_(data) {}

  bool GetU64(std::uint64_t* out) {
    if (!Check(8)) return false;
    std::uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) {
      value |= static_cast<std::uint64_t>(data_[pos_]) << shift;
      ++pos_;
    }
    *out = value;
    return true;
  }

  bool GetU32(std::uint32_t* out) {
    std::uint64_t value = 0;
    if (!GetU64N(4, &value)) return false;
    *out = static_cast<std::uint32_t>(value);
    return true;
  }

  bool GetU16(std::uint16_t* out) {
    std::uint64_t value = 0;
    if (!GetU64N(2, &value)) return false;
    *out = static_cast<std::uint16_t>(value);
    return true;
  }

  bool GetU8(std::uint8_t* out) {
    if (!Check(1)) return false;
    *out = static_cast<std::uint8_t>(data_[pos_]);
    ++pos_;
    return true;
  }

  bool GetBytes(std::size_t count, std::span<const std::byte>* out) {
    if (!Check(count)) return false;
    *out = data_.subspan(pos_, count);
    pos_ += count;
    return true;
  }

  bool ok() const { return !error_; }
  bool exhausted() const { return pos_ == data_.size(); }

 private:
  bool GetU64N(std::size_t bytes, std::uint64_t* out) {
    if (!Check(bytes)) return false;
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes; ++i) {
      value |= static_cast<std::uint64_t>(data_[pos_]) << (8 * i);
      ++pos_;
    }
    *out = value;
    return true;
  }

  bool Check(std::size_t count) {
    if (error_ || data_.size() - pos_ < count) {
      error_ = true;
      return false;
    }
    return true;
  }

  std::span<const std::byte> data_;
  std::size_t pos_ = 0;
  bool error_ = false;
};

absl::Status CorruptFrame(const char* what) {
  return absl::Status(absl::StatusCode::kInvalidArgument,
                      std::string("nuraft wire decode: ") + what);
}

// ---------------------------------------------------------------------------
// Client core. Constructed on a NuRaft thread; every non-atomic member is
// worker-thread only afterwards. Coroutines hold shared_ptrs so the core
// outlives the NuRaft-facing client whenever NuRaft drops it first.
// ---------------------------------------------------------------------------

struct PendingRequest {
  nuraft::ptr<nuraft::req_msg> req_;
  nuraft::rpc_handler handler_;
  std::chrono::steady_clock::time_point deadline_;
  std::vector<std::byte> frame_;  // cleared once written
};

struct RpcClientCore {
  std::string host_;
  std::uint16_t port_ = 0;
  MetaTransportConfig config_;
  std::shared_ptr<celer::TlsContext> tls_context_;
  MetaPeerSchemaVerifier peer_schema_verifier_;
  std::uint64_t id_ = 0;
  // Read from NuRaft threads via is_abandoned(); set exactly once on the
  // worker (or at client destruction) and never cleared.
  std::atomic<bool> abandoned_{false};

  // Worker-thread only below.
  celer::Worker* worker_ = nullptr;
  bool driver_started_ = false;
  bool stream_open_ = false;
  celer::TcpStream stream_;
  std::int32_t peer_id_ = 0;
  bool schema_handshake_complete_ = false;
  celer::AsyncNotification schema_handshake_changed_;
  // Connect phase: ConnectTcp is handed connect_timeout_ and enforces the
  // deadline itself (its loser-cancel retires the timeout SQE before the
  // frame resumes, so no adapter-side deadline coroutine is needed).
  bool connect_done_ = false;
  std::optional<absl::StatusOr<celer::TcpStream>> connect_outcome_;
  celer::AsyncNotification connect_changed_;  // connect resolved or failed
  std::deque<PendingRequest> egress_;         // queued for transport write
  std::deque<PendingRequest> outstanding_;    // written, awaiting response
  std::size_t egress_bytes_ = 0;
  celer::AsyncNotification egress_ready_;     // wakes the writer
  celer::AsyncNotification watchdog_kick_;    // wakes an idle watchdog
  celer::TimerCancelHandle watchdog_cancel_;  // wakes an armed watchdog
  std::chrono::steady_clock::time_point watchdog_armed_ =
      std::chrono::steady_clock::time_point::max();
};

void FailClient(RpcClientCore& core, const std::string& reason);

// Exactly-once failure of a single request that never entered the queues.
// Runs on the worker thread.
void FailOne(nuraft::ptr<nuraft::req_msg>& req, nuraft::rpc_handler& handler,
             const std::string& reason) {
  nuraft::ptr<nuraft::resp_msg> no_resp;
  nuraft::ptr<nuraft::rpc_exception> err =
      nuraft::cs_new<nuraft::rpc_exception>(reason, req);
  handler(no_resp, err);
}

celer::Task<absl::Status> RunReader(std::shared_ptr<RpcClientCore> core);
celer::Task<absl::Status> RunWatchdog(std::shared_ptr<RpcClientCore> core);

// Lazy connect with connect_timeout_ carried by ConnectTcp itself: the
// operation arms its own deadline and cancels the loser SQE before resuming,
// so an expired or refused connect resolves here as an ordinary error status.
// A client abandoned while the connect is still in flight closes the fresh
// stream instead of handing it to the writer.
celer::Task<absl::Status> RunConnect(std::shared_ptr<RpcClientCore> core) {
  celer::Worker& worker = *core->worker_;
  auto connected = co_await celer::ConnectTcp(worker, core->host_, core->port_,
                                              core->config_.connect_timeout_);
  if (core->abandoned_.load(std::memory_order_acquire)) {
    if (connected.ok()) {
      celer::TcpStream late = std::move(*connected);
      (void)late.Close();
    }
    co_return absl::OkStatus();
  }
  core->connect_outcome_.emplace(std::move(connected));
  core->connect_done_ = true;
  core->connect_changed_.NotifyAll(worker);
  co_return absl::OkStatus();
}

celer::Task<absl::Status> RunWriter(std::shared_ptr<RpcClientCore> core) {
  celer::Worker& worker = *core->worker_;

  // Lazy connect on first use; create_client() itself never blocks.
  if (!core->stream_open_) {
    worker.Spawn(RunWatchdog(core));
    worker.Spawn(RunConnect(core));
    while (!core->connect_done_ &&
           !core->abandoned_.load(std::memory_order_acquire)) {
      co_await core->connect_changed_.Wait();
    }
    if (core->abandoned_.load(std::memory_order_acquire)) {
      co_return absl::OkStatus();  // FailClient already ran
    }
    if (!core->connect_outcome_->ok()) {
      FailClient(*core,
                 "connect to " + core->host_ + " failed: " +
                     std::string(core->connect_outcome_->status().message()));
      co_return absl::OkStatus();
    }
    core->stream_ = std::move(**core->connect_outcome_);
    core->connect_outcome_.reset();
    core->stream_open_ = true;
    if (core->egress_.empty()) {
      FailClient(*core, "schema handshake has no target Raft request");
      co_return absl::OkStatus();
    }
    core->peer_id_ = core->egress_.front().req_->get_dst();
    if (core->tls_context_ != nullptr) {
      // peer_name = endpoint host: the certificate must cover it (mTLS).
      const absl::Status tls = co_await core->stream_.StartTls(
          core->tls_context_, /*server=*/false, core->host_);
      if (!tls.ok()) {
        FailClient(*core, "TLS handshake with " + core->host_ +
                              " failed: " + std::string(tls.message()));
        co_return absl::OkStatus();
      }
      auto sans = core->stream_.PeerCertificateUriSans();
      if (!sans.ok()) {
        FailClient(*core, "TLS peer identity unavailable for " + core->host_);
        co_return absl::OkStatus();
      }
      auto peer_identity = AuthenticateMetaUriSans(*sans);
      if (!peer_identity.ok() ||
          peer_identity->role_ != MetaPrincipalRole::kMetaMember ||
          peer_identity->subject_id_ != std::to_string(core->peer_id_)) {
        FailClient(*core, "TLS peer URI identity does not match Raft target " +
                              std::to_string(core->peer_id_));
        co_return absl::OkStatus();
      }
    }
    // Negotiate the actual binary schema range before any NuRaft message.
    // The request watchdog is already live, so a peer that never acks fails
    // through the same bounded client lifecycle as any other stalled send.
    worker.Spawn(RunReader(core));
    auto hello =
        wire::EncodeSchemaRange(wire::FrameKind::kSchemaHello,
                                wire::SchemaRange{core->config_.min_schema_,
                                                  core->config_.max_schema_});
    if (!hello.ok()) {
      FailClient(*core, std::string(hello.status().message()));
      co_return absl::OkStatus();
    }
    const absl::Status hello_written = co_await core->stream_.WriteAll(*hello);
    if (!hello_written.ok()) {
      FailClient(*core, "schema hello to " + core->host_ +
                            " failed: " + std::string(hello_written.message()));
      co_return absl::OkStatus();
    }
    while (!core->schema_handshake_complete_ &&
           !core->abandoned_.load(std::memory_order_acquire)) {
      co_await core->schema_handshake_changed_.Wait();
    }
    if (core->abandoned_.load(std::memory_order_acquire)) {
      co_return absl::OkStatus();
    }
  }

  while (!core->abandoned_.load(std::memory_order_acquire)) {
    if (core->egress_.empty()) {
      co_await core->egress_ready_.Wait();
      continue;
    }
    PendingRequest pending = std::move(core->egress_.front());
    core->egress_.pop_front();
    core->egress_bytes_ -= pending.frame_.size();
    if (pending.req_->get_dst() != core->peer_id_) {
      core->egress_.push_front(std::move(pending));
      FailClient(*core, "Raft client reused for a different target member");
      co_return absl::OkStatus();
    }
    const std::size_t frame_bytes = pending.frame_.size();
    const absl::Status written =
        co_await core->stream_.WriteAll(pending.frame_);
    if (!written.ok()) {
      // Put the request back so FailClient's drain reports every handler
      // exactly once, in original order.
      core->egress_.push_front(std::move(pending));
      core->egress_bytes_ += frame_bytes;
      FailClient(*core, "write to " + core->host_ +
                            " failed: " + std::string(written.message()));
      co_return absl::OkStatus();
    }
    core->outstanding_.push_back(std::move(pending));
  }
  co_return absl::OkStatus();
}

// Parse and dispatch every complete frame in `pending`. Returns false when it
// failed the client (protocol violation); the caller must exit its loop.
bool DrainResponseFrames(RpcClientCore& core, std::vector<std::byte>& pending) {
  std::size_t pos = 0;
  while (pending.size() - pos >= wire::kHeaderBytes) {
    auto header = wire::ParseHeader(
        std::span<const std::byte>(pending.data() + pos, wire::kHeaderBytes));
    if (!header.ok()) {
      FailClient(core, std::string(header.status().message()));
      return false;
    }
    if (header->payload_bytes_ > core.config_.max_frame_bytes_) {
      FailClient(core, "oversize response frame");
      return false;
    }
    if (pending.size() - pos < wire::kHeaderBytes + header->payload_bytes_) {
      break;  // incomplete frame; wait for more bytes
    }
    const std::span<const std::byte> payload(
        pending.data() + pos + wire::kHeaderBytes, header->payload_bytes_);
    if (!core.schema_handshake_complete_) {
      if (header->kind_ != wire::FrameKind::kSchemaAck) {
        FailClient(core, "expected schema ack before Raft response");
        return false;
      }
      auto peer = wire::DecodeSchemaRange(payload);
      if (!peer.ok() || peer->min_schema_ > core.config_.max_schema_ ||
          peer->max_schema_ < core.config_.min_schema_) {
        FailClient(core, "Raft peer schema range does not overlap this binary");
        return false;
      }
      if (core.peer_schema_verifier_) {
        const absl::Status verified =
            core.peer_schema_verifier_(core.peer_id_, *peer);
        if (!verified.ok()) {
          FailClient(core, "Raft peer schema attestation failed: " +
                               std::string(verified.message()));
          return false;
        }
      }
      pos += wire::kHeaderBytes + header->payload_bytes_;
      core.schema_handshake_complete_ = true;
      if (core.worker_ != nullptr) {
        core.schema_handshake_changed_.NotifyAll(*core.worker_);
      }
      continue;
    }
    if (header->kind_ != wire::FrameKind::kResponse) {
      FailClient(core, "unexpected frame kind on a client connection");
      return false;
    }
    auto resp = wire::DecodeResponse(payload);
    if (!resp.ok()) {
      FailClient(core, std::string(resp.status().message()));
      return false;
    }
    if (core.outstanding_.empty()) {
      FailClient(core, "unsolicited response (no matching request)");
      return false;
    }
    PendingRequest done = std::move(core.outstanding_.front());
    core.outstanding_.pop_front();
    pos += wire::kHeaderBytes + header->payload_bytes_;
    // The request left every queue before its handler runs: exactly-once.
    // The handler (peer::handle_rpc_result) takes NuRaft locks and may post
    // fresh sends back through the bridge — both are safe from this worker
    // coroutine.
    nuraft::ptr<nuraft::resp_msg>& resp_ref = *resp;
    nuraft::ptr<nuraft::rpc_exception> no_err;
    done.handler_(resp_ref, no_err);
  }
  pending.erase(pending.begin(),
                pending.begin() + static_cast<std::ptrdiff_t>(pos));
  return true;
}

celer::Task<absl::Status> RunReader(std::shared_ptr<RpcClientCore> core) {
  std::byte chunk[16384];
  std::vector<std::byte> pending;
  while (!core->abandoned_.load(std::memory_order_acquire)) {
    auto read = co_await core->stream_.ReadSome(chunk);
    if (!read.ok() || *read == 0) {
      FailClient(*core, "connection to " + core->host_ + " closed: " +
                            (read.ok() ? std::string("peer EOF")
                                       : std::string(read.status().message())));
      co_return absl::OkStatus();
    }
    pending.insert(pending.end(), chunk, chunk + *read);
    if (!DrainResponseFrames(*core, pending)) {
      co_return absl::OkStatus();
    }
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> RunWatchdog(std::shared_ptr<RpcClientCore> core) {
  celer::Worker& worker = *core->worker_;
  while (!core->abandoned_.load(std::memory_order_acquire)) {
    // Both queues are FIFO and deadlines are assigned at enqueue, so the
    // earliest deadline overall is the earlier of the two fronts.
    auto earliest = std::chrono::steady_clock::time_point::max();
    if (!core->egress_.empty()) {
      earliest = std::min(earliest, core->egress_.front().deadline_);
    }
    if (!core->outstanding_.empty()) {
      earliest = std::min(earliest, core->outstanding_.front().deadline_);
    }
    if (earliest == std::chrono::steady_clock::time_point::max()) {
      core->watchdog_armed_ = earliest;
      co_await core->watchdog_kick_.Wait();
      continue;
    }
    const auto now = std::chrono::steady_clock::now();
    if (earliest <= now) {
      FailClient(*core, "request to " + core->host_ + " timed out");
      co_return absl::OkStatus();
    }
    core->watchdog_armed_ = earliest;
    celer::CancellableTimerAwaitable timer =
        celer::CancellableSleepFor(worker, earliest - now);
    core->watchdog_cancel_ = timer.CancelHandle();
    const absl::Status fired = co_await timer;
    core->watchdog_cancel_ = celer::TimerCancelHandle{};
    // kCancelled means the deadline set changed; recompute. An actual fire
    // means the head request exceeded its overall timeout: this client is
    // single-use, so fail it and let NuRaft recreate one with backoff.
    if (fired.ok()) {
      FailClient(*core, "request to " + core->host_ + " timed out");
      co_return absl::OkStatus();
    }
  }
  co_return absl::OkStatus();
}

// Single-use failure: invoke every queued handler exactly once with an
// rpc_exception, close the connection, wake the driver coroutines so they
// exit, and mark the client abandoned. Idempotent. Worker thread only.
void FailClient(RpcClientCore& core, const std::string& reason) {
  bool expected = false;
  if (!core.abandoned_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel)) {
    return;
  }
  std::deque<PendingRequest> failed;
  while (!core.outstanding_.empty()) {
    failed.push_back(std::move(core.outstanding_.front()));
    core.outstanding_.pop_front();
  }
  while (!core.egress_.empty()) {
    failed.push_back(std::move(core.egress_.front()));
    core.egress_.pop_front();
  }
  core.egress_bytes_ = 0;
  if (core.stream_open_) {
    (void)core.stream_.Close();
    core.stream_open_ = false;
  }
  core.watchdog_cancel_.Cancel();
  if (core.worker_ != nullptr) {
    core.egress_ready_.NotifyAll(*core.worker_);
    core.watchdog_kick_.NotifyAll(*core.worker_);
    core.connect_changed_.NotifyAll(*core.worker_);
    core.schema_handshake_changed_.NotifyAll(*core.worker_);
  }
  for (PendingRequest& pending : failed) {
    FailOne(pending.req_, pending.handler_, reason);
  }
}

// Marshalled onto the worker by NuraftRpcClient::send().
void EnqueueSend(std::shared_ptr<RpcClientCore> core, celer::Worker& worker,
                 nuraft::ptr<nuraft::req_msg> req, nuraft::rpc_handler handler,
                 std::uint64_t timeout_ms) {
  core->worker_ = &worker;
  if (core->abandoned_.load(std::memory_order_acquire)) {
    FailOne(req, handler, "client to " + core->host_ + " is abandoned");
    return;
  }
  auto frame = wire::EncodeRequest(*req);
  if (!frame.ok()) {
    FailOne(req, handler,
            "encode failed: " + std::string(frame.status().message()));
    return;
  }
  if (frame->size() > core->config_.max_frame_bytes_ ||
      core->egress_.size() >= core->config_.max_egress_messages_ ||
      core->egress_bytes_ + frame->size() > core->config_.max_egress_bytes_) {
    // Bounded egress: never pile up unboundedly behind a dead peer. Failing
    // the client makes NuRaft drop and recreate it with backoff.
    FailClient(*core, "egress limit exceeded");
    FailOne(req, handler, "egress limit exceeded");
    return;
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  const bool queues_were_empty =
      core->egress_.empty() && core->outstanding_.empty();
  core->egress_bytes_ += frame->size();
  core->egress_.push_back(PendingRequest{std::move(req), std::move(handler),
                                         deadline, std::move(*frame)});
  if (!core->driver_started_) {
    core->driver_started_ = true;
    worker.Spawn(RunWriter(core));
  } else {
    core->egress_ready_.NotifyAll(worker);
  }
  // The watchdog only needs a kick when it may be idle or armed past this
  // request's earlier deadline; both primitives are no-ops otherwise.
  if (queues_were_empty || deadline < core->watchdog_armed_) {
    core->watchdog_cancel_.Cancel();
    core->watchdog_kick_.NotifyAll(worker);
  }
}

class NuraftRpcClient : public nuraft::rpc_client {
 public:
  NuraftRpcClient(std::shared_ptr<MetaCelerBridge> bridge,
                  std::shared_ptr<RpcClientCore> core)
      : bridge_(std::move(bridge)), core_(std::move(core)) {}

  // NuRaft drops failed/replaced clients; make sure the connection does not
  // linger on the worker. If the bridge is already stopped the post is
  // dropped and the worker's shutdown teardown reclaims everything.
  ~NuraftRpcClient() override {
    std::shared_ptr<RpcClientCore> core = core_;
    bridge_->Post([core](celer::Worker& /*worker*/) {
      FailClient(*core, "client destroyed");
    });
  }

  void send(nuraft::ptr<nuraft::req_msg>& req, nuraft::rpc_handler& when_done,
            uint64_t send_timeout_ms = 0) override {
    std::shared_ptr<RpcClientCore> core = core_;
    // The core raft path always passes 0 (peer::send_req); fall back to the
    // configured request timeout, honoring an explicit value when given.
    const std::uint64_t timeout_ms =
        send_timeout_ms != 0 ? send_timeout_ms
                             : static_cast<std::uint64_t>(
                                   core->config_.request_timeout_.count());
    bridge_->Post(
        [core, req, when_done, timeout_ms](celer::Worker& worker) mutable {
          EnqueueSend(std::move(core), worker, std::move(req),
                      std::move(when_done), timeout_ms);
        });
  }

  std::uint64_t get_id() const override { return core_->id_; }

  bool is_abandoned() const override {
    return core_->abandoned_.load(std::memory_order_acquire);
  }

 private:
  std::shared_ptr<MetaCelerBridge> bridge_;
  std::shared_ptr<RpcClientCore> core_;
};

// "host:port", "[v6-host]:port", optionally with a "tcp://" scheme prefix.
// Host must be numeric (celer::ConnectTcp does no DNS); validated here so a
// bad endpoint fails at create_client, like the asio factory returning nil.
absl::StatusOr<std::pair<std::string, std::uint16_t>> ParseEndpoint(
    const std::string& endpoint) {
  std::string_view view(endpoint);
  const std::size_t scheme = view.rfind("://");
  if (scheme != std::string_view::npos) {
    view.remove_prefix(scheme + 3);
  }
  const std::size_t colon = view.rfind(':');
  if (colon == std::string_view::npos || colon == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "endpoint lacks host:port");
  }
  std::string_view host = view.substr(0, colon);
  const std::string_view port_text = view.substr(colon + 1);
  if (host.front() == '[') {
    if (host.size() < 3 || host.back() != ']') {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "malformed bracketed IPv6 host");
    }
    host = host.substr(1, host.size() - 2);
  }
  std::uint32_t port = 0;
  const char* begin = port_text.data();
  const char* end = begin + port_text.size();
  const auto parsed = std::from_chars(begin, end, port);
  if (parsed.ec != std::errc{} || parsed.ptr != end || port == 0 ||
      port > 65535) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid endpoint port");
  }
  in_addr addr4{};
  in6_addr addr6{};
  const std::string host_text(host);
  if (::inet_pton(AF_INET, host_text.c_str(), &addr4) != 1 &&
      ::inet_pton(AF_INET6, host_text.c_str(), &addr6) != 1) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "endpoint host is not a numeric IPv4/IPv6 address");
  }
  return std::make_pair(host_text, static_cast<std::uint16_t>(port));
}

}  // namespace

namespace wire {

absl::StatusOr<std::vector<std::byte>> EncodeRequest(nuraft::req_msg& req) {
  std::vector<std::byte> payload;
  payload.reserve(45);
  PutU8(payload, static_cast<std::uint8_t>(req.get_type()));
  PutU32(payload, static_cast<std::uint32_t>(req.get_src()));
  PutU32(payload, static_cast<std::uint32_t>(req.get_dst()));
  PutU64(payload, req.get_term());
  PutU64(payload, req.get_last_log_term());
  PutU64(payload, req.get_last_log_idx());
  PutU64(payload, req.get_commit_idx());
  PutU64(payload, req.get_extra_flags());
  PutU32(payload, static_cast<std::uint32_t>(req.log_entries().size()));
  for (nuraft::ptr<nuraft::log_entry>& entry : req.log_entries()) {
    PutU64(payload, entry->get_term());
    PutU8(payload, static_cast<std::uint8_t>(entry->get_val_type()));
    PutU64(payload, entry->get_timestamp());
    PutU8(payload, entry->has_crc32() ? 1 : 0);
    PutU32(payload, entry->get_crc32());
    if (entry->is_buf_null()) {
      PutU32(payload, 0xffffffffu);  // -1: null buffer marker
      continue;
    }
    nuraft::buffer& data = entry->get_buf();
    PutU32(payload, static_cast<std::uint32_t>(data.size()));
    PutBytes(payload, data.data_begin(), data.size());
  }

  std::vector<std::byte> frame;
  frame.reserve(kHeaderBytes + payload.size());
  PutU32(frame, static_cast<std::uint32_t>(payload.size()));
  PutU8(frame, kVersion);
  PutU8(frame, static_cast<std::uint8_t>(FrameKind::kRequest));
  PutU16(frame, 0);
  frame.insert(frame.end(), payload.begin(), payload.end());
  return frame;
}

absl::StatusOr<std::vector<std::byte>> EncodeResponse(nuraft::resp_msg& resp) {
  std::vector<std::byte> payload;
  payload.reserve(45);
  PutU8(payload, static_cast<std::uint8_t>(resp.get_type()));
  PutU32(payload, static_cast<std::uint32_t>(resp.get_src()));
  PutU32(payload, static_cast<std::uint32_t>(resp.get_dst()));
  PutU64(payload, resp.get_term());
  PutU64(payload, resp.get_next_idx());
  PutU8(payload, resp.get_accepted() ? 1 : 0);
  PutU64(payload,
         static_cast<std::uint64_t>(resp.get_next_batch_size_hint_in_bytes()));
  PutU64(payload, resp.get_extra_flags());
  PutU32(payload, static_cast<std::uint32_t>(
                      static_cast<std::int32_t>(resp.get_result_code())));
  const nuraft::ptr<nuraft::buffer> ctx = resp.get_ctx();
  const std::uint32_t ctx_bytes =
      ctx ? static_cast<std::uint32_t>(ctx->size()) : 0;
  PutU32(payload, ctx_bytes);
  if (ctx_bytes != 0) {
    PutBytes(payload, ctx->data_begin(), ctx->size());
  }

  std::vector<std::byte> frame;
  frame.reserve(kHeaderBytes + payload.size());
  PutU32(frame, static_cast<std::uint32_t>(payload.size()));
  PutU8(frame, kVersion);
  PutU8(frame, static_cast<std::uint8_t>(FrameKind::kResponse));
  PutU16(frame, 0);
  frame.insert(frame.end(), payload.begin(), payload.end());
  return frame;
}

absl::StatusOr<std::vector<std::byte>> EncodeSchemaRange(FrameKind kind,
                                                         SchemaRange range) {
  if (kind != FrameKind::kSchemaHello && kind != FrameKind::kSchemaAck) {
    return CorruptFrame("schema range requires hello or ack frame kind");
  }
  if (range.min_schema_ == 0 || range.min_schema_ > range.max_schema_) {
    return CorruptFrame("invalid schema range");
  }
  std::vector<std::byte> frame;
  frame.reserve(kHeaderBytes + 4);
  PutU32(frame, 4);
  PutU8(frame, kVersion);
  PutU8(frame, static_cast<std::uint8_t>(kind));
  PutU16(frame, 0);
  PutU16(frame, range.min_schema_);
  PutU16(frame, range.max_schema_);
  return frame;
}

absl::StatusOr<FrameHeader> ParseHeader(std::span<const std::byte> header) {
  if (header.size() != kHeaderBytes) {
    return CorruptFrame("header length mismatch");
  }
  Reader reader(header);
  std::uint32_t payload_bytes = 0;
  std::uint8_t version = 0;
  std::uint8_t kind = 0;
  std::uint16_t reserved = 0xffff;
  if (!reader.GetU32(&payload_bytes) || !reader.GetU8(&version) ||
      !reader.GetU8(&kind) || !reader.GetU16(&reserved)) {
    return CorruptFrame("truncated header");
  }
  // The reserved field is currently always zero; tolerate any value for
  // forward compatibility.
  (void)reserved;
  if (version != kVersion) {
    return CorruptFrame("unsupported wire version");
  }
  if (kind < static_cast<std::uint8_t>(FrameKind::kRequest) ||
      kind > static_cast<std::uint8_t>(FrameKind::kSchemaAck)) {
    return CorruptFrame("unknown frame kind");
  }
  return FrameHeader{static_cast<FrameKind>(kind), payload_bytes};
}

absl::StatusOr<SchemaRange> DecodeSchemaRange(
    std::span<const std::byte> payload) {
  Reader reader(payload);
  SchemaRange range;
  if (!reader.GetU16(&range.min_schema_) ||
      !reader.GetU16(&range.max_schema_) || !reader.exhausted()) {
    return CorruptFrame("malformed schema range");
  }
  if (range.min_schema_ == 0 || range.min_schema_ > range.max_schema_) {
    return CorruptFrame("invalid schema range");
  }
  return range;
}

absl::StatusOr<nuraft::ptr<nuraft::req_msg>> DecodeRequest(
    std::span<const std::byte> payload) {
  Reader reader(payload);
  std::uint8_t type = 0;
  std::uint32_t src = 0;
  std::uint32_t dst = 0;
  std::uint64_t term = 0;
  std::uint64_t last_log_term = 0;
  std::uint64_t last_log_idx = 0;
  std::uint64_t commit_idx = 0;
  std::uint64_t extra_flags = 0;
  std::uint32_t log_count = 0;
  if (!reader.GetU8(&type) || !reader.GetU32(&src) || !reader.GetU32(&dst) ||
      !reader.GetU64(&term) || !reader.GetU64(&last_log_term) ||
      !reader.GetU64(&last_log_idx) || !reader.GetU64(&commit_idx) ||
      !reader.GetU64(&extra_flags) || !reader.GetU32(&log_count)) {
    return CorruptFrame("truncated request header");
  }
  auto req = nuraft::cs_new<nuraft::req_msg>(
      term, static_cast<nuraft::msg_type>(type), static_cast<std::int32_t>(src),
      static_cast<std::int32_t>(dst), last_log_term, last_log_idx, commit_idx);
  req->set_extra_flags(extra_flags);
  for (std::uint32_t i = 0; i < log_count; ++i) {
    std::uint64_t entry_term = 0;
    std::uint8_t value_type = 0;
    std::uint64_t timestamp = 0;
    std::uint8_t has_crc32 = 0;
    std::uint32_t crc32 = 0;
    std::uint32_t data_bytes = 0;
    if (!reader.GetU64(&entry_term) || !reader.GetU8(&value_type) ||
        !reader.GetU64(&timestamp) || !reader.GetU8(&has_crc32) ||
        !reader.GetU32(&crc32) || !reader.GetU32(&data_bytes)) {
      return CorruptFrame("truncated log entry");
    }
    nuraft::ptr<nuraft::buffer> data;
    if (data_bytes != 0xffffffffu) {
      std::span<const std::byte> bytes;
      if (!reader.GetBytes(data_bytes, &bytes)) {
        return CorruptFrame("truncated log entry data");
      }
      data = nuraft::buffer::alloc(data_bytes);
      if (data_bytes != 0) {
        std::memcpy(data->data_begin(), bytes.data(), data_bytes);
      }
    }
    req->log_entries().push_back(nuraft::cs_new<nuraft::log_entry>(
        entry_term, data, static_cast<nuraft::log_val_type>(value_type),
        timestamp, has_crc32 != 0, crc32, /*compute_crc=*/false));
  }
  if (!reader.exhausted()) {
    return CorruptFrame("trailing bytes after request");
  }
  return req;
}

absl::StatusOr<nuraft::ptr<nuraft::resp_msg>> DecodeResponse(
    std::span<const std::byte> payload) {
  Reader reader(payload);
  std::uint8_t type = 0;
  std::uint32_t src = 0;
  std::uint32_t dst = 0;
  std::uint64_t term = 0;
  std::uint64_t next_idx = 0;
  std::uint8_t accepted = 0;
  std::uint64_t hint = 0;
  std::uint64_t extra_flags = 0;
  std::uint32_t result_code = 0;
  std::uint32_t ctx_bytes = 0;
  if (!reader.GetU8(&type) || !reader.GetU32(&src) || !reader.GetU32(&dst) ||
      !reader.GetU64(&term) || !reader.GetU64(&next_idx) ||
      !reader.GetU8(&accepted) || !reader.GetU64(&hint) ||
      !reader.GetU64(&extra_flags) || !reader.GetU32(&result_code) ||
      !reader.GetU32(&ctx_bytes)) {
    return CorruptFrame("truncated response");
  }
  auto resp = nuraft::cs_new<nuraft::resp_msg>(
      term, static_cast<nuraft::msg_type>(type), static_cast<std::int32_t>(src),
      static_cast<std::int32_t>(dst), next_idx, accepted != 0);
  resp->set_next_batch_size_hint_in_bytes(static_cast<std::int64_t>(hint));
  resp->set_extra_flags(extra_flags);
  resp->set_result_code(static_cast<nuraft::cmd_result_code>(
      static_cast<std::int32_t>(result_code)));
  if (ctx_bytes != 0) {
    std::span<const std::byte> bytes;
    if (!reader.GetBytes(ctx_bytes, &bytes)) {
      return CorruptFrame("truncated response ctx");
    }
    nuraft::ptr<nuraft::buffer> ctx = nuraft::buffer::alloc(ctx_bytes);
    std::memcpy(ctx->data_begin(), bytes.data(), ctx_bytes);
    resp->set_ctx(ctx);
  }
  if (!reader.exhausted()) {
    return CorruptFrame("trailing bytes after response");
  }
  return resp;
}

}  // namespace wire

// static
absl::StatusOr<std::shared_ptr<NuraftRpcClientFactory>>
NuraftRpcClientFactory::Create(std::shared_ptr<MetaCelerBridge> bridge,
                               MetaTransportConfig config) {
  if (bridge == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "bridge must not be null");
  }
  std::shared_ptr<celer::TlsContext> tls_context;
  if (config.TlsEnabled()) {
    celer::TlsClientOptions options;
    options.ca_cert_file_ = config.tls_ca_cert_file_;
    options.cert_file_ = config.tls_cert_file_;
    options.key_file_ = config.tls_key_file_;
    auto context = celer::TlsContext::CreateClient(options);
    if (!context.ok()) {
      return context.status();
    }
    tls_context = std::move(*context);
  }
  return std::shared_ptr<NuraftRpcClientFactory>(new NuraftRpcClientFactory(
      std::move(bridge), std::move(config), std::move(tls_context)));
}

nuraft::ptr<nuraft::rpc_client> NuraftRpcClientFactory::create_client(
    const std::string& endpoint) {
  auto parsed = ParseEndpoint(endpoint);
  if (!parsed.ok()) {
    return nuraft::ptr<nuraft::rpc_client>();
  }
  auto core = std::make_shared<RpcClientCore>();
  core->host_ = std::move(parsed->first);
  core->port_ = parsed->second;
  core->config_ = config_;
  core->tls_context_ = tls_context_;
  core->peer_schema_verifier_ = peer_schema_verifier_;
  core->id_ = next_client_id_.fetch_add(1, std::memory_order_relaxed);
  return nuraft::cs_new<NuraftRpcClient>(bridge_, std::move(core));
}

}  // namespace keylane::meta
