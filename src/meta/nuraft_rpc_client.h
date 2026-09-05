#pragma once

// NuRaft rpc_client / rpc_client_factory on celer TCP (+ optional mTLS).
//
// THREAD MODEL
//
// NuRaft calls rpc_client_factory::create_client() and rpc_client::send()
// from its own threads (peer::send_req runs on the bg append/commit threads),
// never from a celer worker. Every send() is marshalled through the
// MetaCelerBridge (see nuraft_scheduler.h for the bridge's threading
// contract): send() only enqueues a closure and returns; the closure runs on
// the worker and drives the connection. Handlers (rpc_handler) are always
// invoked on the celer worker thread, matching how asio_rpc_client invokes
// them on its io_service threads; NuRaft's handlers (peer::handle_rpc_result)
// only take NuRaft's internal locks, so this is safe — and send() never
// invokes the handler inline, which matters because callers may hold NuRaft
// locks that the handler would re-acquire.
//
// CLIENT LIFECYCLE (contract with NuRaft's reconnect logic)
//
// A client owns ONE connection and is single-use: any failure (connect
// refused/timeout, TLS failure, write/read error, request timeout, egress
// overflow) fails the whole client — every queued and in-flight request's
// handler is invoked exactly once with an rpc_exception, the connection is
// closed, and is_abandoned() becomes true. NuRaft then drops the client and
// re-creates one through the factory with its own exponential backoff
// (peer::recreate_rpc). create_client() is therefore deliberately cheap and
// non-blocking: it parses the endpoint and allocates state only; the TCP
// connect happens lazily on the first send, inside the writer coroutine.
//
// EXACTLY-ONCE HANDLER GUARANTEE
//
// Each request lives in exactly one place: the egress deque (not yet
// written), the outstanding deque (written, awaiting response), or a local
// being handled. Responses are matched FIFO to outstanding (TCP order), and
// the failure path drains both deques. The core's worker-side state is
// touched only by worker coroutines, so no locks are needed there;
// abandoned_ is atomic because is_abandoned() is read from NuRaft threads.
//
// TIMEOUTS
//
// NuRaft's raft-to-peer path always passes send_timeout_ms = 0
// (peer::send_req), so this adapter enforces its own timeouts: the connect
// deadline is connect_timeout_, passed straight to celer::ConnectTcp, which
// enforces it natively (the Wave-1 ConnectOperation timeout-SQE
// use-after-free that once forced an adapter-side deadline coroutine is fixed
// in celer and regression-covered by celer_connect_timer_check), and a
// per-request overall timeout (send-to-response) driven by a single watchdog
// coroutine that sleeps until the earliest deadline of the egress/outstanding
// front entries. A non-zero send_timeout_ms from NuRaft (auto-forwarding
// path) overrides the configured request timeout for that request, matching
// asio semantics.
//
// BACKPRESSURE
//
// Egress is bounded by message count AND bytes; overflow fails the client so
// NuRaft recreates it — nothing piles up unboundedly while a peer is down.
// The corresponding listener-side ingress bound is SetReadAhead(false) on
// accepted streams.
//
// WIRE FORMAT (both ends are this adapter; versioned for future drift)
//
// All integers are little-endian. One frame per message:
//   u32 payload_bytes   (excludes this 8-byte header)
//   u8  version         (= kVersion)
//   u8  kind            (1 = request, 2 = response, 3 = schema hello,
//                        4 = schema ack)
//   u16 reserved        (= 0)
//
// Each connection starts with a schema hello/ack carrying the binary's real
// readable range (u16 min, u16 max). Raft request frames are rejected until
// that exchange succeeds. Request payload (full req_msg coverage):
//   u8 msg_type, i32 src, i32 dst, u64 term, u64 last_log_term,
//   u64 last_log_idx, u64 commit_idx, u64 extra_flags, u32 log_count,
//   then per log entry (full log_entry coverage):
//     u64 term, u8 value_type, u64 timestamp_us, u8 has_crc32, u32 crc32,
//     i32 data_bytes (-1 = null buffer), u8[data_bytes] data
//   Entry data is opaque: for conf/cluster_server entries it carries NuRaft's
//   cluster_config serialization, which this transport never interprets.
//
// Response payload (full resp_msg coverage):
//   u8 msg_type, i32 src, i32 dst, u64 term, u64 next_idx, u8 accepted,
//   i64 next_batch_size_hint_in_bytes, u64 extra_flags, i32 result_code,
//   i32 ctx_bytes (0 = no ctx), u8[ctx_bytes] ctx
//
// Decode rejects: wrong version/kind, payload_bytes > max_frame_bytes_,
// truncated or trailing data, out-of-range counts/lengths. Any violation
// closes the connection (client: fails the client; listener: drops the
// session) — a framing desync is never recoverable.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
// NuRaft's headers are not -Wpedantic-clean; see nuraft_scheduler.h.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/ptr.hxx"
#include "libnuraft/req_msg.hxx"
#include "libnuraft/resp_msg.hxx"
#include "libnuraft/rpc_cli.hxx"
#include "libnuraft/rpc_cli_factory.hxx"
#pragma GCC diagnostic pop

#include "meta/meta_encoding.h"
#include "meta/nuraft_scheduler.h"

namespace celer {
class TlsContext;
}

namespace keylane::meta {

// Transport tuning for one meta Raft instance. TLS is mTLS: when
// tls_ca_cert_file_ is set, clients present tls_cert_file_/tls_key_file_ and
// verify the peer against the endpoint host, and the listener requires a
// client certificate (TlsClientAuth::kRequired). Empty ca path means
// plaintext only when the executable's explicit unsafe test flag permits it.
struct MetaTransportConfig {
  std::string tls_ca_cert_file_;
  std::string tls_cert_file_;
  std::string tls_key_file_;

  std::chrono::milliseconds connect_timeout_{2000};
  std::chrono::milliseconds request_timeout_{3000};

  // Bounded client egress; overflow fails the client (see above).
  std::size_t max_egress_messages_ = 256;
  std::size_t max_egress_bytes_ = 8 * 1024 * 1024;

  // Decode-side frame cap. NuRaft bounds append batches via raft_params, but
  // a corrupt or hostile peer must not be able to force giant allocations.
  std::uint32_t max_frame_bytes_ = 64 * 1024 * 1024;

  // Binary capability advertised by the transport handshake. Production
  // leaves these at the compile-time constants; explicit fields keep wire
  // tests able to exercise non-overlap without pretending an aux value is a
  // different binary.
  std::uint16_t min_schema_ = kMetaMinReadableSchemaVersion;
  std::uint16_t max_schema_ = kMetaCurrentSchemaVersion;

  bool TlsEnabled() const { return !tls_ca_cert_file_.empty(); }
};

namespace wire {

inline constexpr std::uint8_t kVersion = 1;
enum class FrameKind : std::uint8_t {
  kRequest = 1,
  kResponse = 2,
  kSchemaHello = 3,
  kSchemaAck = 4,
};
inline constexpr std::size_t kHeaderBytes = 8;

struct FrameHeader {
  FrameKind kind_;
  std::uint32_t payload_bytes_;
};

struct SchemaRange {
  std::uint16_t min_schema_ = 0;
  std::uint16_t max_schema_ = 0;
  bool operator==(const SchemaRange&) const = default;
};

// Encode a full frame (header + payload), ready for one WriteAll.
absl::StatusOr<std::vector<std::byte>> EncodeRequest(nuraft::req_msg& req);
absl::StatusOr<std::vector<std::byte>> EncodeResponse(nuraft::resp_msg& resp);
absl::StatusOr<std::vector<std::byte>> EncodeSchemaRange(FrameKind kind,
                                                         SchemaRange range);

// Parse the fixed 8-byte frame header; validates version and kind.
absl::StatusOr<FrameHeader> ParseHeader(std::span<const std::byte> header);

// Decode one payload (header already stripped by the caller).
absl::StatusOr<nuraft::ptr<nuraft::req_msg>> DecodeRequest(
    std::span<const std::byte> payload);
absl::StatusOr<nuraft::ptr<nuraft::resp_msg>> DecodeResponse(
    std::span<const std::byte> payload);
absl::StatusOr<SchemaRange> DecodeSchemaRange(
    std::span<const std::byte> payload);

}  // namespace wire

// Assembly-time validation of the remote hello acknowledgement against the
// target member's committed binding. The target id comes from the first Raft
// request queued on that per-peer client.
using MetaPeerSchemaVerifier =
    std::function<absl::Status(std::int32_t, wire::SchemaRange)>;

// Creates lazy single-use clients. Thread-safe: create_client() may be called
// from any NuRaft thread and only allocates + records state.
class NuraftRpcClientFactory : public nuraft::rpc_client_factory {
 public:
  // Builds the shared TLS client context upfront when TLS is configured, so a
  // bad cert path fails at startup instead of on the first connect.
  static absl::StatusOr<std::shared_ptr<NuraftRpcClientFactory>> Create(
      std::shared_ptr<MetaCelerBridge> bridge, MetaTransportConfig config);

  // Install before NuRaft starts and can create clients. Production uses this
  // to reject a peer whose actual binary range differs from its first-stage
  // committed member binding.
  void SetPeerSchemaVerifier(MetaPeerSchemaVerifier verifier) {
    peer_schema_verifier_ = std::move(verifier);
  }

  // Cheap and non-blocking; returns nullptr on a malformed endpoint (the
  // asio factory does the same). The endpoint must be a numeric
  // IPv4/IPv6 "host:port" (celer::ConnectTcp does no DNS).
  nuraft::ptr<nuraft::rpc_client> create_client(
      const std::string& endpoint) override;

 private:
  NuraftRpcClientFactory(std::shared_ptr<MetaCelerBridge> bridge,
                         MetaTransportConfig config,
                         std::shared_ptr<celer::TlsContext> tls_context)
      : bridge_(std::move(bridge)),
        config_(std::move(config)),
        tls_context_(std::move(tls_context)) {}

  std::shared_ptr<MetaCelerBridge> bridge_;
  MetaTransportConfig config_;
  std::shared_ptr<celer::TlsContext> tls_context_;  // nullptr = plaintext
  MetaPeerSchemaVerifier peer_schema_verifier_;
  // Monotonic per-process ids; NuRaft compares get_id() to discard late
  // responses from connections it already replaced.
  std::atomic<std::uint64_t> next_client_id_{1};
};

}  // namespace keylane::meta
