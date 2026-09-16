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

#pragma once

// Process-level outbound Meta control client for a Data Node. The client owns
// only volatile discovery/session state: a restart begins fenced and learns a
// complete projection from the current Meta leader before acquiring a lease.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/net/service.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/replication.h"

namespace bycorf {
class TlsContext;
}

namespace lavik {
class ReplicationManager;
}

namespace lavik::cluster {

class NodeControlActions;
class NodeControlInstaller;
class TopologyCache;
struct AuthorityAnchor;
struct DesiredClusterControl;
struct PopulationReadiness;
struct PreparedFailoverActivation;

struct MetaControlEndpoint {
  std::string host_;
  std::uint16_t port_ = 0;
  std::uint32_t server_id_ = 0;  // zero for an unresolved seed
  // Empty only for unresolved configured seeds. Learned entries retain the
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
// is performed by Bycorf/OpenSSL during StartTls using the dialed numeric host.
absl::Status ValidateUniqueControlPrincipal(
    std::span<const std::string> uri_sans, std::string_view expected);

// Pins a learned dial target to the exact committed member identity retained
// from the previous directory. An unresolved configured seed may bootstrap from
// the authenticated ServerHello member; a learned endpoint may not replace
// its principal merely by echoing a different value in that Hello.
absl::Status ValidateDialedMetaIdentity(
    const MetaControlEndpoint& dialed,
    const control::WireMetaEndpoint& hello_member);

// Validates the local recipient/incarnation, the directive-kind role, and an
// exact field-for-field match with the installed FDS directive set. The live
// session id is intentionally excluded because FDS is session independent.
absl::Status ValidateLiveDirective(const control::Directive& directive,
                                   const control::NodeControlState& desired,
                                   std::string_view local_node_id,
                                   std::string_view local_boot_id);

// Maps local validation rejection separately from an execution that started
// and then failed. Once `started` is true, every non-success outcome is a
// failed execution even when its native status code is commonly associated
// with admission rejection. Protocol/session failures are handled by the
// caller and do not become directive results.
control::DirectiveResultStatus ClassifyDirectiveResultStatus(
    const absl::Status& status, bool started) noexcept;

// Fits the soft health and single role payload into the mandatory one-frame
// heartbeat. Human-readable summary text is the only truncatable field; an
// indivisible candidate is omitted only as a defensive last resort.
absl::Status FitHeartbeatToSingleFrame(control::Heartbeat& heartbeat);

// Versioned, delimiter-safe identity used by the native rebuild adapter for
// the complete authority anchor.
std::string EncodeRebuildAuthorityIdentity(const AuthorityAnchor& anchor);

// Full-jitter reconnect policy: draw uniformly from [0, current_window], then
// double the window up to 10 seconds. Call Reset only after an accepted
// session has produced a valid HeartbeatAck.
class MetaReconnectBackoff {
 public:
  // Shared with Meta's new-leader observation warmup so a live Data process
  // cannot be declared absent while still inside a permitted reconnect sleep.
  static constexpr std::chrono::milliseconds MaximumWindow() noexcept {
    return std::chrono::seconds(10);
  }

  std::chrono::milliseconds Next(std::uint64_t entropy) noexcept;
  void Reset() noexcept { window_ = std::chrono::milliseconds(1000); }
  std::chrono::milliseconds window() const noexcept { return window_; }

 private:
  std::chrono::milliseconds window_{1000};
};

// Volatile discovery directory. A known leader is tried first, then the
// committed in-memory directory, then configured seeds. Entries are
// de-duplicated within the same identity; an unresolved configured seed is kept
// even when its address matches a learned member, so legitimate endpoint
// reuse can recover from a stale learned server id. Nothing is persisted by
// the Data Node.
class MetaEndpointDirectory {
 public:
  explicit MetaEndpointDirectory(std::vector<MetaControlEndpoint> seeds);

  // Replaces the committed directory and consumes an explicit leader hint
  // from ServerHello. Learned entries require the canonical committed
  // `lavik://meta/<server-id>` binding. A hint absent from the replacement
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
  // Owner role is normally independent of whether its current grant is
  // renewable. The only exception is an exact uncontrolled target-term fence:
  // its retained owner is historical topology, not active authority, and may
  // report its boot-local population as a recovery candidate.
  static bool IsCommittedOwner(
      std::span<const control::WireDesiredGroup> groups,
      std::string_view local_node_id) noexcept;

  std::optional<std::size_t> Next(
      std::span<const control::WireDesiredGroup> groups,
      std::string_view local_node_id) noexcept;
  void Reset() noexcept { next_index_ = 0; }

 private:
  std::size_t next_index_ = 0;
};

namespace detail {

// A lease challenge names the exact resolved duration in the installed FDS.
// Meta must echo that scalar unchanged; accepting a shorter value would make
// the installed projection and Data's finite authority describe different
// contracts.
absl::Status ValidateResolvedLeaseGrantDuration(
    std::uint32_t granted_duration_ms, std::uint32_t challenged_duration_ms);

enum class MetaTransferAbortDisposition : std::uint8_t {
  kFailSession,
  kContinueAuthenticatedSession,
};

// Called only after LargeObjectReassembler has validated the active object id
// and consumed the abort. The narrow named reason is object-local; every other
// reason/type pair remains a terminal protocol event.
MetaTransferAbortDisposition ClassifyMetaTransferAbort(
    const control::TransferAbort& abort,
    std::optional<control::TransferKind> active_kind) noexcept;

// Exact native-manager input derived from one normalized FDS Group and the
// current Data incarnation. An action belongs only to its named candidate, a
// source pause only to the exact controlled Owner, and a grant activation id
// becomes a local pending activation only for the committed Owner.
struct ClusterFailoverReconcileInput {
  std::optional<DesiredClusterFailoverAction> candidate_action_;
  std::optional<DesiredClusterSourcePause> source_pause_;
  std::optional<ClusterFailoverActionId> pending_activation_action_id_;
  // An active transition owns candidate catch-up and deliberately leaves the
  // ordinary relationship untouched. Once the transition is absent, true
  // means follow_owner_ (including nullopt for removal) must be reconciled.
  bool reconcile_follow_owner_ = false;
  std::optional<DesiredClusterUpstream> follow_owner_;

  friend bool operator==(const ClusterFailoverReconcileInput&,
                         const ClusterFailoverReconcileInput&) = default;
};

// Converts one normalized group projection into native replication intents.
// `use_tls` selects the committed owner's TLS replication port for follow-owner
// relationships; false selects its plaintext port. It does not configure the
// Meta control connection itself.
absl::StatusOr<ClusterFailoverReconcileInput> TranslateClusterFailoverControl(
    const DesiredClusterControl& desired,
    const ReplicationIdentity& local_identity, bool use_tls = false);

ClusterFailoverActivation TranslateClusterFailoverActivation(
    const PreparedFailoverActivation& activation);

// Projects only exact terminal native outcomes. Waiting/retrying states remain
// local diagnostics and therefore produce no Meta observation.
absl::StatusOr<std::optional<control::FailoverObservation>>
ProjectClusterFailoverObservation(const ClusterFailoverActionStatus& status);

// Emits SourcePaused only after the native pause owns an exact desired
// incarnation and has captured its complete stable frontier.
absl::StatusOr<std::optional<control::FailoverObservation>>
ProjectClusterSourcePauseObservation(const ClusterSourcePauseStatus& status);

// A boot or history change ordinarily replaces the session. The caller
// separately recognizes the narrow exact FDS-owned failover bridge needed to
// publish a completed rotation safely.
bool ReplicationIdentityRequiresMetaReconnect(
    const ReplicationIdentity& established,
    const ReplicationIdentity& latest) noexcept;

// Native failover preparation rotates history before its level-triggered
// Prepared observation is publishable. The authenticated session may bridge
// only that exact action while it remains in the installed FDS; removal at
// Cutover (or any identity/action mismatch) forces the normal reconnect.
bool FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
    const ReplicationIdentity& established, const ReplicationIdentity& latest,
    const ClusterFailoverActionStatus& status,
    std::span<const control::WireDesiredGroup> groups) noexcept;

// One heartbeat must either reconnect, use its ordinary role projection, or
// remain on the old authenticated history solely to publish failover progress.
// The last case suppresses ReplicaCandidate and lease claims until the FDS
// removes the action and the client reauthenticates with the child history.
struct MetaSessionReplicationIdentityDecision {
  bool requires_reconnect_ = false;
  bool suppress_ordinary_role_ = false;

  friend bool operator==(const MetaSessionReplicationIdentityDecision&,
                         const MetaSessionReplicationIdentityDecision&) =
      default;
};

MetaSessionReplicationIdentityDecision EvaluateMetaSessionReplicationIdentity(
    const ReplicationIdentity& established, const ReplicationIdentity& latest,
    const ClusterFailoverActionStatus& status,
    std::span<const control::WireDesiredGroup> groups) noexcept;

// Builds ordinary replica progress from the current Ready population. During
// an exact uncontrolled fence, the retained historical owner reports a
// boot-local self-origin lineage. A clean recovered population instead keeps
// the certificate's historical source lineage; active owners and terminally
// failed populations remain ineligible.
absl::StatusOr<std::optional<control::CandidateProgress>>
ProjectReplicaCandidateProgress(
    std::span<const control::WireDesiredGroup> groups,
    std::string_view local_node_id, const ReplicationIdentity& current_identity,
    const PopulationReadiness& readiness, const RebuildIdentity& ready_identity,
    std::span<const std::uint64_t> applied_next_lsns,
    bool failover_candidate_eligible, bool recovered = false);

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
  std::shared_ptr<bycorf::TlsContext> tls_context_;
};

class MetaControlClientService final : public bycorf::Service {
 public:
  // The installer, topology cache, and replication manager are retained by
  // reference and must outlive Run, Stop, and the final WaitUntilQuiesced.
  static absl::StatusOr<std::unique_ptr<MetaControlClientService>> Create(
      MetaControlClientOptions options, NodeControlInstaller& installer,
      TopologyCache& topology, ReplicationManager& replication);
  ~MetaControlClientService() override;

  MetaControlClientService(const MetaControlClientService&) = delete;
  MetaControlClientService& operator=(const MetaControlClientService&) = delete;

  void Prepare(unsigned thread_count) override;
  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext context) override;
  void Stop() noexcept override;

  // Joins the worker-0 session, including any directive execution and the
  // final fail-closed NodeControl transition. Call after Stop() and before a
  // graceful storage checkpoint. This call blocks and must run outside worker
  // 0 while that worker and the hosting Runtime can still make progress.
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
// remains the sole component allowed to invoke it. `use_tls` fixes whether
// follow-owner intents select committed TLS or plaintext replication endpoints
// and must match the Data-to-Data replication listener configuration.
std::unique_ptr<NodeControlActions> CreateReplicationNodeControlActions(
    ReplicationManager& replication, bool use_tls);

}  // namespace lavik::cluster
