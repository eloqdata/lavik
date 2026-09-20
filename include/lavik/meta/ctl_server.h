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

// MetaCtlServer: authorized line-protocol administration and observation
// surface of lavik-meta, served on its Bycorf control worker. Raft's Go
// peer transport has an independent runtime.
//
// Local administration defaults to a mode-0600 AF_UNIX socket and derives
// its actor from SO_PEERCRED. Remote TCP administration supports plaintext on
// explicitly trusted networks, or mutual TLS with a canonical Lavik URI
// SAN. Plaintext peers share the deliberately unauthenticated
// `lavik://operator/plaintext` actor; anyone who can reach that listener has
// operator authority, so network isolation is part of that mode's security
// boundary.
//
// Protocol: one command per line (LF-terminated, CR tolerated), exactly one
// reply line per command, processed strictly in order per connection.
// Committed writes use the metadata command schema
// (commands.h); generic KV verbs are not part of this surface:
//   submitop <id32hex> <kind> <payload> [<history40hex>]
//                          -> propose SubmitOperation (intent = payload,
//                             intent_hash = SHA-256(payload)): "OK <log_idx>"
//                             once committed AND the effect verified in the
//                             local committed state; "ERR rejected" when
//                             apply consumed the index but refused the
//                             command; "ERR not-leader" on a follower;
//                             otherwise "ERR <code>" (replication timeout is
//                             Raft's client_req_timeout_).
//   completeop <id32hex> [<result>]
//                          -> propose CompleteOperation with the record's
//                             current committed revision as the CAS token;
//                             same reply shape as submitop. "ERR not-found"
//                             (unknown operation) and "ERR terminal" (already
//                             Completed/Aborted) short-circuit without
//                             proposing. Submitting and immediately
//                             completing keeps the non-terminal operation set
//                             tiny and below max_active_operations.
//   abortop <id32hex> [<reason>]
//                          -> propose AbortOperation with the same committed
//                             revision/CAS and effect-verification rules.
//                             Empty result/reason forms are the zero-growth
//                             terminalization step admitted by the durability
//                             fail-safe before archival.
//   getop <id32hex>        -> "OK submitted" / "OK running" /
//                             "OK completed <result>" / "OK aborted <reason>"
//                             / "ERR not-found".
//                             Non-terminal creation/membership workflows
//                             additionally include phase=<durable
//                             phase/recovery reason>. Failover running state is
//                             derived from the operation and matching topology
//                             transition in one committed snapshot. This is NOT
//                             a linearizable read (no read-index round or
//                             leader lease check), so a stale follower may
//                             answer from an older commit index.
//   registernode <node_id40hex> <principal> <primary|replica>
//                <data-endpoint> [<data-endpoint>]
//                          -> propose RegisterNode;
//                             reply shape of submitop. Data endpoints are
//                             numeric host:port values optionally tagged with
//                             tcp:// or tls://; an active node needs one or
//                             two before its desired state can be projected.
//   getnode <node_id>      -> "OK principal=<p> role=<primary|replica>
//                             revision=<n> retired=<0|1>" / "ERR not-found";
//                             same non-linearizable read semantics as getop.
//   putpolicy <policy_id> <version> <content>
//                          -> commit one immutable policy version; content is
//                             a strict compact JSON token in a registered
//                             family.
//   getpolicy <policy_id>  -> leader-only current raw Policy as
//                             "OK version=<n> content=<json>" or not-found.
//   setslotmap <first> <last> <group_id>
//                          -> replace the absolute slot map with one inclusive
//                             range. This deliberately narrow bootstrap form
//                             does not imply incremental slot mutation.
//   activateauthority <group_id> <expected_term> <owner_node_id>
//                          -> atomically activate the committed owner/grant;
//                             the topology epoch is derived from the local
//                             committed snapshot. A term can acquire at most
//                             one Grant. Normal Owner changes use typed
//                             failover; this primitive requires a previously
//                             reserved grantless term.
//   fencegroup <group_id> <expected_term>
//                          -> atomically fence the current Grant and advance
//                             the Group Term by one. Data sessions fence and
//                             drain the superseded anchor before acknowledging
//                             its replacement FDS.
//   status                 -> "OK leader=<0|1> id=<n> committed=<idx>
//                             snapshot_idx=<idx> term=<n>".
//   clusterhead 1          -> bounded versioned responder/role/term/leader/
//                             committed-Meta-directory discovery response.
//                             Any Meta member may answer this operator-only,
//                             read-only verb.
//   clusterstatus 1        -> bounded versioned full cluster-status cut from
//                             the current caught-up Leader, or a typed
//                             retryable error. Capture is single-flight and
//                             never probes followers.
//   clustercreate 1 <hex>  -> leader-owned v1 multi-Meta/multi-Group Genesis.
//                             The bounded payload contains the normalized
//                             topology and caller-generated root operation id.
//                             Success confirms the atomic root-operation plus
//                             Creating lifecycle commit and returns immediately
//                             with its index and operation id. All later
//                             creates are already-created; failures name a
//                             stage and stable code. Creation and membership
//                             changes share admission across all Admin
//                             listeners.
//   failover 1 <hex>       -> submit one bounded canonical controlled-failover
//                             request with its caller-generated operation id
//                             and absolute deadline. Success confirms only the
//                             request commit; getop observes later transition
//                             progress and completion. Typed errors distinguish
//                             definite rejection from uncertain submission.
//   addsrv <id> <raft-ip:port> <data-control-ip:port> <ctl-ip:port>
//          [<lavik://meta/id>]
//                          -> persists a membership workflow before binding
//                             identity or invoking Raft. "OK" means the
//                             exact configuration and identity are committed.
//                             A wait timeout returns uncertain-outcome with
//                             an operation id; the leader keeps retrying.
//                             Identical retries attach to the same task. The
//                             optional principal defaults to the canonical
//                             identity for that member id.
//   removesrv <id>         -> the same durable workflow/wait contract. Only a
//                             committed removal retires the committed
//                             member identity.
//   exportaudit <through>  -> "OK <hex>" versioned, ordered record export.
//   pruneaudit <through>   -> replicated prefix prune; callers must durably
//                             store the matching export first.
//   exportoperations      -> "OK <hex>" versioned archived-operation export.
//   archiveoperations <seq>... -> move the named terminal live records into
//                                  bounded archive summaries.
//   pruneoperations <seq>... -> replicated tombstone prune; callers must
//                                durably store the export first.
//   snapshot               -> "OK <idx>" / "ERR snapshot-failed"; wraps
//                             raft_server::create_snapshot with
//                             serialize_commit_=true: the manual capture is
//                             serialized against the commit thread, as defined
//                             by raft_server.hxx create_snapshot_options.
//                             The durable write then runs asynchronously on
//                             the state machine's writer thread, so OK means
//                             the exact-cut capture at <idx> was taken; an
//                             in-flight earlier round fails fast with 0 and
//                             a later asynchronous write failure only skips
//                             this compaction round.
//
// creategroup, assignnode, begingroupterm, and transitionop are also typed,
// committed-state drivers used by operators and gates to build the anchors
// against which observation freshness is checked.
//
// Observation surface: MetaObservationStore is volatile and leader-local, so
// this whole verb family manipulates process-local state — nothing here is
// replicated:
//   adoptsession <node_id> <boot_hex40> <gen>
//                          -> MetaObservationStore::AdoptSession with the
//                             transport-authorized session identity; "OK" /
//                             "ERR <detail>".
//   obs boot <node_id> <boot_hex40> <gen>
//   obs health <node_id> <boot_hex40> <gen> <health>
//   obs candidate <node_id> <boot_hex40> <gen> <group> <term> <manifest>
//                 <partition_epoch> <history40hex>
//   observations           -> "OK total=<n>"; observations <group_id> ->
//                             "OK candidates=<n>" plus one
//                             node=<n>,assignment=<a>,term=<t>,manifest=<m>,
//                             partition_epoch=<p>,history=<h>,storage_ready=<s>,population_ready=<r>
//                             token per fresh candidate (read paths re-filter
//                             against the current committed snapshot).
//   obsaudit               -> "OK events=<n>" plus one
//                             kind=<k>,node=<id>,detail=<d>,ts=<ms> token
//                             per audit-ring event, oldest first.
// The coordinator revalidates volatile observations after every committed
// batch, including batches proposed by reconcilers rather than this surface.
// Read paths also filter against one committed MetaStores snapshot.
// Payloads and principals are whitespace-free single tokens; anything else
// is a protocol error and closes the connection after an "ERR bad-request".
//
// ACTOR INJECTION: the ctl surface is a trusted entry, so
// IT injects the transport-derived actor of every command it proposes. UDS
// sessions derive `lavik://operator/uid-N` from SO_PEERCRED and an explicit
// uid allowlist; remote mTLS sessions derive one canonical URI SAN from the
// client certificate, while plaintext TCP sessions use the fixed
// unauthenticated actor described above.
// The coordinator stamps readable_time immediately before proposal; apply
// only copies both values into replicated audit state.
//
// THREAD MODEL
//
// Start()/Shutdown()/status() may be called from any thread. Lifecycle work
// enters the owning worker through bycorf::ForeignExecutor. The accept loop
// and one session coroutine per connection run on that worker. Committed
// model mutations go through
// MetaCoordinator; a bounded proposal executor invokes membership and
// snapshot lifecycle APIs away from the worker. Background-thread completions
// use ForeignExecutor::Resume() to resume the parked session coroutine.
//
// LIFECYCLE
//
// The core keeps its own shared_ptr references to the raft_server and the
// state machine, so a session in flight always sees live objects; Shutdown()
// releases them on the worker thread (the adapter teardown order in
// app/lavik_meta.cpp guarantees ~raft_server runs after
// raft_server::shutdown(),
// on whichever thread drops the last reference — both are safe).

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/runtime/foreign_executor.h"
#include "bycorf/runtime/task.h"
#include "lavik/meta/cluster_status.h"
#include "lavik/meta/committed_status_view.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/meta/raft.h"

namespace bycorf {
struct Connection;
class TcpStream;
class Worker;
}  // namespace bycorf

namespace lavik::meta {

class MetaAutomaticFailoverDiagnosticsRegistry;
struct MetaAutomaticFailoverDiagnosticsSnapshot;

class MetaMembershipGate;
class MetaProposalExecutor;

class MetaObservationStore;
class MetaCoordinator;
class MetaStateMachine;
namespace detail {

// Pure representation of both sides of the server's clusterstatus bracket.
// Keeping comparison free of Raft calls makes every invalidating transition
// directly testable while the capture path remains the sole owner of reads.
struct MetaClusterStatusBracket {
  bool is_leader_ = false;
  bool leader_alive_ = false;
  std::uint64_t term_ = 0;
  std::uint64_t config_index_ = 0;
  std::vector<std::uint32_t> config_server_ids_;
  std::vector<MetaMemberRecord> active_meta_members_;
  MetaDataControlLeadershipState leadership_;
};

bool IsStableClusterStatusBracket(const MetaClusterStatusBracket& before,
                                  const MetaClusterStatusBracket& after);

// Requires detector diagnostics, volatile authority continuity, and the
// committed status view to describe one evaluation cut before they are joined
// into clusterstatus.
bool IsCurrentAutomaticFailoverDiagnostics(
    const MetaDataControlRuntimeSnapshot& runtime,
    const MetaAutomaticFailoverDiagnosticsSnapshot& detector,
    std::uint64_t committed_applied_index);

// Parses the canonical positive decimal representation accepted by the
// putpolicy Admin Adapter. Leading zeroes are rejected so one version has one
// wire spelling; overflow and a null output are also rejected.
bool ParseAdminPolicyVersion(std::string_view text, std::uint64_t* version);

// Classifies a registered node with no current session. Prior parsed Hello or
// accepted-session evidence distinguishes a missing session from a process
// this leadership generation has never observed.
std::string_view ClusterCreateMissingSessionBlocker(bool retry_observed,
                                                    bool session_observed);

// Projects the captured heartbeat and last written lease onto a node whose
// session/projection fields already describe this committed cut. Health can
// arrive before its Ack is written; only a healthy, current population may
// expose a recent grant. All freshness checks use the supplied capture time.
void ApplyClusterRuntimeObservation(
    ClusterDataNodeWireV1& node, const MetaDataControlRuntimeNode& runtime_node,
    const MetaCommittedStatusView& view, const ClusterCaptureWireV1& capture,
    std::int64_t now_unix_ms, std::uint32_t observation_ttl_ms);

}  // namespace detail

// Shared by every configured Admin listener. Capture is single-flight across
// UDS and TCP, so a second expensive status build fails retryably instead of
// delaying Data-control heartbeat work on their shared worker.
class MetaClusterStatusService {
 public:
  bool TryBeginCapture() noexcept {
    if (capture_in_progress_.test_and_set(std::memory_order_acquire)) {
      return false;
    }
    // A response can be almost the full process-wide retained budget. Keep
    // capture admission closed while any prior reply is queued or sending so
    // the next build cannot temporarily allocate a second 256 MiB payload.
    if (retained_reply_bytes_.load(std::memory_order_acquire) != 0) {
      capture_in_progress_.clear(std::memory_order_release);
      return false;
    }
    return true;
  }
  void EndCapture() noexcept {
    capture_in_progress_.clear(std::memory_order_release);
  }
  bool TryRetain(std::size_t bytes) noexcept {
    std::size_t current = retained_reply_bytes_.load(std::memory_order_relaxed);
    while (current <=
           kMaxRetainedReplyBytes - std::min(bytes, kMaxRetainedReplyBytes)) {
      if (bytes > kMaxRetainedReplyBytes) return false;
      if (retained_reply_bytes_.compare_exchange_weak(
              current, current + bytes, std::memory_order_acquire,
              std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }
  void Release(std::size_t bytes) noexcept {
    retained_reply_bytes_.fetch_sub(bytes, std::memory_order_release);
  }

 private:
  static constexpr std::size_t kMaxRetainedReplyBytes = 256u << 20;
  std::atomic_flag capture_in_progress_ = ATOMIC_FLAG_INIT;
  std::atomic<std::size_t> retained_reply_bytes_{0};
};

class MetaClusterCreateReconciler;

struct MetaCtlServerOptions {
  enum class Transport : std::uint8_t { kUnix, kTcpPlaintext, kTcpMtls };
  Transport transport_ = Transport::kUnix;

  std::string unix_socket_path_;
  std::vector<uid_t> allowed_uids_;

  std::string bind_host_;
  std::uint16_t port_ = 0;
  std::string tls_ca_cert_file_;
  std::string tls_cert_file_;
  std::string tls_key_file_;

  // Process-local Data Node control listener. Dynamic remove parsing retains
  // this value only as a syntactic placeholder; durable advertised routes
  // come from the committed member descriptor.
  std::string local_data_control_endpoint_;
  // Process-local administrative TCP listener, which may sit behind the
  // durable advertised ctl route.
  std::string local_ctl_endpoint_;
  std::shared_ptr<MetaClusterStatusService> cluster_status_service_;
  std::shared_ptr<MetaDataControlRuntimeStatus> data_control_runtime_status_;
  // Complete, generation-bracketed detector cuts published by the
  // leader-scoped automatic failover reconciler. The Admin server owns no
  // detector timers and exposes no Policy contents through status.
  std::shared_ptr<MetaAutomaticFailoverDiagnosticsRegistry>
      automatic_failover_diagnostics_;
  // Shared background owner; listeners submit durable intent and only wait.
  std::shared_ptr<MetaClusterCreateReconciler> cluster_create_reconciler_;
  std::shared_ptr<class MetaMembershipReconciler> membership_reconciler_;
  std::uint32_t observation_ttl_ms_ = 30000;
};

class MetaCtlServer {
 public:
  // Pure validation seam used by startup and security tests. TCP is either
  // explicitly plaintext with no TLS inputs, or mTLS with a complete identity;
  // partial TLS configuration never silently downgrades to plaintext.
  static absl::Status ValidateOptions(const MetaCtlServerOptions& options);

  static absl::StatusOr<std::shared_ptr<MetaCtlServer>> Create(
      bycorf::ForeignExecutor foreign_executor,
      std::shared_ptr<MetaRaft> server,
      std::shared_ptr<MetaStateMachine> state_machine,
      std::shared_ptr<MetaCoordinator> coordinator,
      std::shared_ptr<MetaObservationStore> obs_store,
      // Non-owning: process assembly must keep the executor alive until the
      // Bycorf worker and all ctl session coroutines have stopped.
      MetaProposalExecutor& proposal_executor,
      // All listeners for this Meta process must share the same gate, covering
      // both membership changes and the complete cluster-create workflow.
      std::shared_ptr<MetaMembershipGate> membership_gate,
      MetaCtlServerOptions options);

  // Shuts down if needed, so sessions cannot outlive the handle while holding
  // Raft references. Process assembly destroys this while the worker is live.
  ~MetaCtlServer();

  MetaCtlServer(const MetaCtlServer&) = delete;
  MetaCtlServer& operator=(const MetaCtlServer&) = delete;

  // Connections are accepted only after Start(). The bind itself runs on the
  // worker asynchronously; check status() afterwards.
  void Start();
  // Cancels result waits and drains local sessions; committed background
  // workflows are not aborted or rolled back when an Admin waiter goes away.
  void Shutdown();

  // Result of the asynchronous bind: kUnavailable until the worker reports.
  absl::Status status() const;

 private:
  struct Core;
  class SessionConnectionBorrow;
  using CorePtr = std::shared_ptr<Core>;

  explicit MetaCtlServer(CorePtr core) : core_(std::move(core)) {}

  static bycorf::Task<absl::Status> AcceptLoop(CorePtr core);
  static bycorf::Task<absl::Status> SessionLoop(CorePtr core,
                                                bycorf::TcpStream stream,
                                                bycorf::Connection* connection,
                                                SessionConnectionBorrow borrow);

  CorePtr core_;
};

}  // namespace lavik::meta
