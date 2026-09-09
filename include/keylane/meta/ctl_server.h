#pragma once

// MetaCtlServer: authorized line-protocol administration and observation
// surface of keylane-meta, served on the celer worker that also drives the
// meta Raft transport.
//
// Local administration defaults to a mode-0600 AF_UNIX socket and derives
// its actor from SO_PEERCRED. Remote TCP administration supports plaintext on
// explicitly trusted networks, or mutual TLS with a canonical Keylane URI
// SAN. Plaintext peers share the deliberately unauthenticated
// `keylane://operator/plaintext` actor; anyone who can reach that listener has
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
//                             NuRaft's client_req_timeout_).
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
//                             / "ERR not-found". Reads the state machine's
//                             committed operation journal directly; this is
//                             NOT a linearizable read (no read-index round or
//                             leader lease check), so a stale follower
//                             may answer from an older commit index.
//   registernode <node_id40hex> <principal> <primary|replica>
//                <data-endpoint> [<data-endpoint>]
//                          -> propose RegisterNode with zero capability mask;
//                             reply shape of submitop. Data endpoints are
//                             numeric host:port values optionally tagged with
//                             tcp:// or tls://; an active node needs one or
//                             two before its desired state can be projected.
//   getnode <node_id>      -> "OK principal=<p> role=<primary|replica>
//                             revision=<n> retired=<0|1>" / "ERR not-found";
//                             same non-linearizable read semantics as getop.
//   putpolicy <policy_id> <version> <content>
//                          -> commit one immutable policy version; content is
//                             a whitespace-free token and its SHA-256 is
//                             computed by this trusted proposer.
//   setslotmap <first> <last> <group_id> <config_epoch>
//                          -> replace the absolute slot map with one inclusive
//                             range and set that group's absolute config
//                             epoch. This deliberately narrow bootstrap form
//                             does not imply incremental slot mutation.
//   activateauthority <group_id> <expected_term> <owner_node_id>
//                     <lease_ms> <policy_id> <policy_version>
//                     <new_authority_version> <new_config_epoch>
//                          -> atomically activate the committed owner/grant;
//                             the topology epoch is derived from the local
//                             committed snapshot and every other CAS/absolute
//                             value remains explicit operator input.
//   fencegroup <group_id> <expected_term>
//                          -> commit FenceGroup under the current term. Data
//                             sessions fence and drain the superseded anchor
//                             before acknowledging its replacement FDS.
//   status                 -> "OK leader=<0|1> id=<n> committed=<idx>
//                             snapshot_idx=<idx> term=<n>".
//   addsrv <id> <raft-ip:port> <data-control-ip:port>
//          [<keylane://meta/id>]
//                          -> first commits the member identity, then returns
//                             "OK" / "ERR <code>" from NuRaft add_srv. The
//                             optional principal defaults to the canonical
//                             identity for that member id.
//   removesrv <id>         -> "OK" / "ERR <code>" from NuRaft remove_srv.
//                             A successful removal then retires the committed
//                             member identity.
//   exportaudit <through>  -> "OK <hex>" versioned, hash-chained export.
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
// against which observation freshness is checked. transitionop appends an
// evidence summary whose replication_history_id is what later anchors
// `obs evidence` (HistoryBoundToOperation).
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
//                 <partition_epoch> <history40hex> <flow> <backlog> <readiness>
//   obs evidence <node_id> <boot_hex40> <gen> <op32hex> <phase> <evidence>
//                <group> <term> <manifest> <partition_epoch> <history40hex>
//                          -> one Ingest each (fields map 1:1 onto the
//                             envelope payloads of observation_store.h;
//                             candidate reporter/assignment identity is
//                             derived from the trusted session identity and
//                             the same committed snapshot used for admission;
//                             evidence_hash is computed as SHA-256(evidence)
//                             by the ctl, not taken from the wire). "OK" on
//                             admission, "ERR <detail>" on rejection — every
//                             rejection also lands in the audit ring.
//   observations           -> "OK total=<n>"; observations <group_id> ->
//                             "OK candidates=<n>" plus one
//                             node=<n>,assignment=<a>,term=<t>,manifest=<m>,
//                             partition_epoch=<p>,history=<h>,readiness=<r>
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
// sessions derive `keylane://operator/uid-N` from SO_PEERCRED and an explicit
// uid allowlist; remote mTLS sessions derive one canonical URI SAN from the
// client certificate, while plaintext TCP sessions use the fixed
// unauthenticated actor described above.
// The coordinator stamps readable_time immediately before proposal; apply
// only copies both values into replicated audit state.
//
// THREAD MODEL
//
// Start()/Shutdown()/status() may be called from any thread. Lifecycle work
// enters the owning worker through celer::ForeignExecutor. The accept loop
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
// app/keylane_meta.cpp guarantees ~raft_server runs after
// raft_server::shutdown(),
// on whichever thread drops the last reference — both are safe).

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/foreign_executor.h"
#include "celer/runtime/task.h"
// NuRaft's headers are not -Wpedantic-clean.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/ptr.hxx"
#pragma GCC diagnostic pop

namespace celer {
struct Connection;
class TcpStream;
class Worker;
}  // namespace celer

namespace nuraft {
class raft_server;
}  // namespace nuraft

namespace keylane::meta {

class MetaMembershipGate;
class MetaProposalExecutor;

class MetaObservationStore;
class MetaCoordinator;
class MetaStateMachine;

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

  // Advertised endpoint of this process's Data Node control listener. It is
  // used when the first membership change binds the bootstrap Raft member
  // into the committed Meta directory.
  std::string local_data_control_endpoint_;
};

class MetaCtlServer {
 public:
  // Pure validation seam used by startup and security tests. TCP is either
  // explicitly plaintext with no TLS inputs, or mTLS with a complete identity;
  // partial TLS configuration never silently downgrades to plaintext.
  static absl::Status ValidateOptions(const MetaCtlServerOptions& options);

  static absl::StatusOr<std::shared_ptr<MetaCtlServer>> Create(
      celer::ForeignExecutor foreign_executor,
      nuraft::ptr<nuraft::raft_server> server,
      nuraft::ptr<MetaStateMachine> state_machine,
      std::shared_ptr<MetaCoordinator> coordinator,
      std::shared_ptr<MetaObservationStore> obs_store,
      // Non-owning: process assembly must keep the executor alive until the
      // Celer worker and all ctl session coroutines have stopped.
      MetaProposalExecutor& proposal_executor,
      std::shared_ptr<MetaMembershipGate> membership_gate,
      MetaCtlServerOptions options);

  // Posts shutdown() if it never happened, so sessions cannot outlive the
  // handle while holding raft references.
  ~MetaCtlServer();

  MetaCtlServer(const MetaCtlServer&) = delete;
  MetaCtlServer& operator=(const MetaCtlServer&) = delete;

  // Connections are accepted only after Start(). The bind itself runs on the
  // worker asynchronously; check status() afterwards.
  void Start();
  void Shutdown();

  // Result of the asynchronous bind: kUnavailable until the worker reports.
  absl::Status status() const;

 private:
  struct Core;
  using CorePtr = std::shared_ptr<Core>;

  explicit MetaCtlServer(CorePtr core) : core_(std::move(core)) {}

  static celer::Task<absl::Status> AcceptLoop(CorePtr core);
  static celer::Task<absl::Status> SessionLoop(CorePtr core,
                                               celer::TcpStream stream,
                                               celer::Connection* connection);

  void PostShutdown();

  CorePtr core_;
};

}  // namespace keylane::meta
