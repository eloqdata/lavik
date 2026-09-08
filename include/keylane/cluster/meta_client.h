#pragma once

// Process-level outbound Meta control client for a Data Node. The client owns
// only volatile discovery/session state: a restart begins fenced and learns a
// complete projection from the current Meta leader before acquiring a lease.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/net/service.h"
#include "keylane/cluster/control_protocol.h"

namespace celer {
class TlsContext;
}

namespace keylane {
class ReplicationManager;
}

namespace keylane::cluster {

class NodeControlActions;
class NodeControlInstaller;
class TopologyCache;
struct AuthorityAnchor;

struct MetaControlEndpoint {
  std::string host_;
  std::uint16_t port_ = 0;
  std::uint32_t server_id_ = 0;  // zero for an unresolved seed
  // Empty only for unresolved static seeds. Learned entries retain the
  // committed identity binding so TLS verification cannot trust a principal
  // synthesized solely from an untrusted ServerHello.
  std::optional<std::string> principal_;

  friend bool operator==(const MetaControlEndpoint&,
                         const MetaControlEndpoint&) = default;
};

// Accepts only numeric IPv4 `a.b.c.d:port` or bracketed IPv6
// `[address]:port`. DNS is deliberately outside the trust model: plaintext
// mode trusts configured/committed numeric endpoints, while mTLS additionally
// verifies the dialed IP SAN.
absl::StatusOr<MetaControlEndpoint> ParseNumericControlEndpoint(
    std::string_view endpoint);

// The peer certificate must carry one and only one URI SAN, and it must equal
// the identity committed/configured for that connection. IP SAN verification
// is performed by Celer/OpenSSL during StartTls using the dialed numeric host.
absl::Status ValidateUniqueControlPrincipal(
    std::span<const std::string> uri_sans, std::string_view expected);

// Validates the local recipient/incarnation, the directive-kind role, and an
// exact field-for-field match with the installed FDS directive set. The live
// session id is intentionally excluded because FDS is session independent.
absl::Status ValidateLiveDirective(const control::Directive& directive,
                                   const control::FullDesiredState& desired,
                                   std::string_view local_node_id,
                                   std::string_view local_boot_id);

// Maps local validation rejection separately from an execution that started
// and then failed. Once `started` is true, every non-success outcome is a
// failed execution even when its native status code is commonly associated
// with admission rejection. Protocol/session failures are handled by the
// caller and do not become directive results.
control::DirectiveResultStatus ClassifyDirectiveResultStatus(
    const absl::Status& status, bool started) noexcept;

// Canonical decimal encoding: `<count>:<lsn0>,...`. The count makes empty and
// truncated vectors unambiguous; oversized observations are rejected before
// entering a heartbeat frame.
absl::StatusOr<std::string> EncodeCandidateFlowVector(
    std::span<const std::uint64_t> cut_vector);

// Fits the soft health/candidate observation into the protocol's mandatory
// single-frame heartbeat. Human-readable summary text is shortened first; an
// indivisible candidate proof is omitted rather than truncated or falsified.
absl::Status FitHeartbeatToSingleFrame(control::Heartbeat& heartbeat);

// Versioned, delimiter-safe identity used by the native rebuild adapter for
// the complete authority anchor.
std::string EncodeRebuildAuthorityIdentity(const AuthorityAnchor& anchor);

// Full-jitter reconnect policy: draw uniformly from [0, current_window], then
// double the window up to 10 seconds. Call Reset only after an accepted
// session has produced a valid HeartbeatAck.
class MetaReconnectBackoff {
 public:
  std::chrono::milliseconds Next(std::uint64_t entropy) noexcept;
  void Reset() noexcept { window_ = std::chrono::milliseconds(1000); }
  std::chrono::milliseconds window() const noexcept { return window_; }

 private:
  std::chrono::milliseconds window_{1000};
};

// Volatile discovery directory. A known leader is tried first, then the
// committed in-memory directory, then configured seeds. Entries are
// de-duplicated within the same identity; an unresolved static seed is kept
// even when its address matches a learned member, so legitimate endpoint
// reuse can recover from a stale learned server id. Nothing is persisted by
// the Data Node.
class MetaEndpointDirectory {
 public:
  explicit MetaEndpointDirectory(std::vector<MetaControlEndpoint> seeds);

  // Replaces the committed directory and consumes an explicit leader hint
  // from ServerHello. Learned entries require the canonical committed
  // `keylane://meta/<server-id>` binding. A hint absent from the replacement
  // is discarded.
  absl::Status Update(
      std::span<const control::WireMetaEndpoint> committed_directory,
      std::optional<std::uint32_t> leader_id);

  // Replaces the committed directory while preserving the last in-memory
  // leader hint when that server is still present. FullDesiredState carries
  // the directory but deliberately does not repeat leader-local state.
  absl::Status Refresh(
      std::span<const control::WireMetaEndpoint> committed_directory);
  std::vector<MetaControlEndpoint> Candidates() const;

 private:
  std::vector<MetaControlEndpoint> seeds_;
  std::vector<MetaControlEndpoint> learned_;
  std::optional<std::uint32_t> leader_id_;
};

// Picks at most one challenge per heartbeat while giving every locally owned
// active grant a turn. The cursor is volatile session state; replacing a full
// projection may change the vector, but repeated calls still cannot pin all
// renewals to the first group.
class MetaLeaseChallengeRotation {
 public:
  std::optional<std::size_t> Next(
      std::span<const control::WireDesiredGroup> groups,
      std::string_view local_node_id) noexcept;
  void Reset() noexcept { next_index_ = 0; }

 private:
  std::size_t next_index_ = 0;
};

namespace detail {

// Separates a retriable connection/session failure from failure of the local
// authority and population cleanup that followed an accepted session. A stop
// may race any operational return; only cleanup_status() is allowed to make a
// graceful shutdown unsafe.
class MetaSessionRunResult {
 public:
  explicit MetaSessionRunResult(absl::Status operational_status);
  MetaSessionRunResult(absl::Status operational_status,
                       absl::Status cleanup_status);

  // Prefers cleanup failure when reporting an ordinary reconnect attempt.
  const absl::Status& report_status() const noexcept;
  // Ignores transport/protocol failures that merely happened adjacent to
  // Stop, while preserving every failure of the cleanup barrier itself.
  const absl::Status& shutdown_status() const noexcept {
    return cleanup_status_;
  }

 private:
  absl::Status operational_status_;
  absl::Status cleanup_status_;
};

// Worker-confined state machine separating an FDS replacement from heartbeat
// projection reads. The producer owns quiescence; the session reader owns
// pause/resume and consumes at most one response already written for the old
// projection. Stop-and-wait heartbeat sequencing is what makes one retained
// sequence sufficient.
class MetaHeartbeatProjectionGate {
 public:
  void RequestPause(std::optional<std::uint64_t> outstanding_sequence) noexcept;
  void MarkQuiesced(bool value) noexcept { quiesced_ = value; }
  void Resume() noexcept { pause_requested_ = false; }
  bool ConsumeSupersededAck(std::uint64_t sequence) noexcept;

  bool pause_requested() const noexcept { return pause_requested_; }
  bool quiesced() const noexcept { return quiesced_; }

 private:
  std::optional<std::uint64_t> superseded_ack_;
  bool pause_requested_ = false;
  bool quiesced_ = false;
};

}  // namespace detail

struct MetaControlClientOptions {
  std::vector<std::string> seeds_;
  std::string node_id_;
  unsigned request_worker_count_ = 0;
  // Null means plaintext. When present, the same CA/client identity used for
  // Data-to-Data replication is reused for Meta control mTLS.
  std::shared_ptr<celer::TlsContext> tls_context_;
};

class MetaControlClientService final : public celer::Service {
 public:
  static absl::StatusOr<std::unique_ptr<MetaControlClientService>> Create(
      MetaControlClientOptions options, NodeControlInstaller& installer,
      TopologyCache& topology, ReplicationManager& replication);
  ~MetaControlClientService() override;

  MetaControlClientService(const MetaControlClientService&) = delete;
  MetaControlClientService& operator=(const MetaControlClientService&) = delete;

  void Prepare(unsigned thread_count) override;
  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext context) override;
  void Stop() noexcept override;

  // Joins the worker-0 session, including any directive execution and the
  // final fail-closed NodeControl transition. Call after Stop() and before a
  // graceful storage checkpoint; the hosting Runtime must still be running.
  // Failure means native replication cleanup is uncertain and the caller must
  // not publish a normal shutdown checkpoint.
  absl::Status WaitUntilQuiesced();

 private:
  struct Impl;
  explicit MetaControlClientService(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

// Process-lifetime adapter used by NodeControlInstaller. It dispatches only
// normalized directives through ReplicationManager on worker 0; the installer
// remains the sole component allowed to invoke it.
std::unique_ptr<NodeControlActions> CreateReplicationNodeControlActions(
    ReplicationManager& replication);

}  // namespace keylane::cluster
