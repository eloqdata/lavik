#include "meta/meta_ctl_server.h"

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "celer/io/storage.h"
#include "celer/net/connection.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/net/tls.h"
#include "celer/runtime/worker.h"
#include "spdlog/spdlog.h"
// NuRaft's headers are not -Wpedantic-clean; see nuraft_scheduler.h.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/async.hxx"
#include "libnuraft/buffer.hxx"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/raft_server.hxx"
#include "libnuraft/srv_config.hxx"
#pragma GCC diagnostic pop

#include "meta/meta_commands.h"
#include "meta/meta_coordinator.h"
#include "meta/meta_hash.h"
#include "meta/meta_identity_verifier.h"
#include "meta/meta_observation_store.h"
#include "meta/meta_state_machine.h"
#include "meta/nuraft_scheduler.h"

namespace keylane::meta {

// All Core members below the bind status are worker-thread only; the Core
// outlives individual sessions via shared_ptr.
struct MetaCtlServer::Core {
  std::shared_ptr<MetaCelerBridge> bridge_;
  nuraft::ptr<nuraft::raft_server> server_;
  nuraft::ptr<MetaStateMachine> state_machine_;
  std::shared_ptr<MetaCoordinator> coordinator_;
  // Leader-local observation store (plan §4). Internally serialized because
  // ctl ingestion and commit-driven revalidation run on different threads.
  std::shared_ptr<MetaObservationStore> obs_store_;
  MetaCtlServerOptions options_;
  std::shared_ptr<celer::TlsContext> tls_context_;

  mutable std::mutex status_mu_;
  absl::Status status_ = absl::Status(absl::StatusCode::kUnavailable,
                                      "bind has not run on the worker yet");

  // Worker-thread only below.
  celer::Worker* worker_ = nullptr;
  celer::TcpListener listener_;
  bool listening_ = false;
  std::vector<celer::Connection*> sessions_;
};

namespace {

// One oversized partial line already proves a broken or hostile peer; the
// Control-plane payloads are bounded, so cap the assembly buffer hard.
constexpr std::size_t kMaxLineBytes = 64 * 1024;

// Reply tokens for the NuRaft result codes the gate can plausibly hit;
// kept whitespace-free so a reply line always parses as "ERR <token>".
std::string CmdResultToken(nuraft::cmd_result_code code) {
  switch (code) {
    case nuraft::cmd_result_code::OK:
      return "ok";
    case nuraft::cmd_result_code::CANCELLED:
      return "cancelled";
    case nuraft::cmd_result_code::TIMEOUT:
      return "timeout";
    case nuraft::cmd_result_code::NOT_LEADER:
      return "not-leader";
    case nuraft::cmd_result_code::BAD_REQUEST:
      return "bad-request";
    case nuraft::cmd_result_code::SERVER_ALREADY_EXISTS:
      return "already-exists";
    case nuraft::cmd_result_code::CONFIG_CHANGING:
      return "config-changing";
    case nuraft::cmd_result_code::SERVER_IS_JOINING:
      return "joining";
    case nuraft::cmd_result_code::SERVER_NOT_FOUND:
      return "not-found";
    case nuraft::cmd_result_code::CANNOT_REMOVE_LEADER:
      return "cannot-remove-leader";
    case nuraft::cmd_result_code::SERVER_IS_LEAVING:
      return "leaving";
    case nuraft::cmd_result_code::TERM_MISMATCH:
      return "term-mismatch";
    default:
      return "code-" + std::to_string(static_cast<int>(code));
  }
}

// Parking state for one asynchronous NuRaft round trip (append_entries,
// add_srv, remove_srv). The session coroutine stores its handle in waiter_
// (worker thread); the NuRaft completion callback — which fires on a NuRaft
// background thread or, for an already-completed cmd_result, inline on the
// worker — fills reply_ and hops through the bridge to resume the waiter.
// The worker's single-threaded drain loop guarantees the resume closure
// cannot run before the coroutine has parked itself: between when_ready()
// and await_suspend() the session coroutine never yields.
struct AsyncReply {
  std::coroutine_handle<> waiter_{};
  std::string reply_;
};

class AsyncReplyAwaiter {
 public:
  explicit AsyncReplyAwaiter(std::shared_ptr<AsyncReply> state) noexcept
      : state_(std::move(state)) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
    state_->waiter_ = awaiting;
    return true;
  }
  std::string await_resume() noexcept { return std::move(state_->reply_); }

 private:
  std::shared_ptr<AsyncReply> state_;
};

void CompleteAsyncReply(const std::shared_ptr<MetaCelerBridge>& bridge,
                        std::shared_ptr<AsyncReply> state, std::string reply) {
  // Written before Post: the bridge inbox mutex publishes it to the drain
  // loop, which runs the resume on the worker thread.
  state->reply_ = std::move(reply);
  bridge->Post(
      [state](celer::Worker& worker) { worker.Enqueue(state->waiter_); });
}

// Opaque request_id for audit correlation (§2): 8B LE per-process counter ||
// 8B random boot salt. Its representation carries no ordering contract.
MetaRequestId MakeRequestId() {
  static const std::uint64_t boot_salt = [] {
    std::random_device rd;
    std::uint64_t salt = 0;
    for (int ii = 0; ii < 8; ++ii) {
      salt = (salt << 8) | (rd() & 0xffu);
    }
    return salt;
  }();
  static std::atomic<std::uint64_t> counter{0};
  const std::uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
  MetaRequestId id{};
  for (int ii = 0; ii < 8; ++ii) {
    id[ii] = static_cast<std::uint8_t>(seq >> (8 * ii));
    id[8 + ii] = static_cast<std::uint8_t>(boot_salt >> (8 * ii));
  }
  return id;
}

int HexNybble(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

// Exactly `hex_chars` hex digits -> bytes; false otherwise.
bool ParseHexBytes(const std::string& text, std::size_t hex_chars,
                   std::uint8_t* out) {
  if (text.size() != hex_chars || hex_chars % 2 != 0) {
    return false;
  }
  for (std::size_t ii = 0; ii < hex_chars / 2; ++ii) {
    const int hi = HexNybble(text[2 * ii]);
    const int lo = HexNybble(text[2 * ii + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    out[ii] = static_cast<std::uint8_t>((hi << 4) | lo);
  }
  return true;
}

bool ParseOperationId(const std::string& text, MetaOperationId& out) {
  return ParseHexBytes(text, 32, out.data());
}

std::string HexEncode(std::string_view bytes) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (const unsigned char byte : bytes) {
    out.push_back(kHex[byte >> 4]);
    out.push_back(kHex[byte & 0x0f]);
  }
  return out;
}

// node_id on the ctl surface: the topology convention's 40 hex chars
// (kMetaNodeIdBytes), kept as text in the command.
bool IsNodeId(const std::string& text) {
  if (text.size() != kMetaNodeIdBytes) {
    return false;
  }
  for (const char c : text) {
    if (HexNybble(c) < 0) {
      return false;
    }
  }
  return true;
}

const char* LifecycleName(MetaOperationLifecycle lifecycle) {
  switch (lifecycle) {
    case MetaOperationLifecycle::kSubmitted:
      return "submitted";
    case MetaOperationLifecycle::kRunning:
      return "running";
    case MetaOperationLifecycle::kCompleted:
      return "completed";
    case MetaOperationLifecycle::kAborted:
      return "aborted";
  }
  return "unknown";
}

bool IsTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}

const char* RoleName(MetaNodeRole role) {
  return role == MetaNodeRole::kPrimary ? "primary" : "replica";
}

// ---------------------------------------------------------------------------
// Observation-surface helpers (plan §4; see the header's verb reference).
// ---------------------------------------------------------------------------

// Wall clock for the VOLATILE observation store (receive time / TTL). The
// committed side never reads a clock — ApplyCommitted is clock-free by
// contract; this stamp is only used by the leader-local obs store.
std::int64_t NowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// MetaCommittedFacts over ONE committed MetaStores snapshot, so all five
// freshness checks of a single obs command see one consistent cut instead of
// tearing across per-call reads. StoresSnapshot() deep-copies the aggregate —
// KB-scale and fine at ctl command frequency; high-frequency coordinator
// callers must build their facts from a CommittedView instead.
class SnapshotCommittedFacts : public MetaCommittedFacts {
 public:
  explicit SnapshotCommittedFacts(MetaStores stores)
      : stores_(std::move(stores)) {}

  bool IsActiveNode(std::string_view node_id) const override {
    return stores_.identity_.IsActiveNode(std::string(node_id));
  }
  std::uint64_t CurrentGroupTerm(std::string_view group_id) const override {
    // 0 when the group does not exist: unknown committed state rejects.
    return stores_.grant_.CurrentGroupTerm(std::string(group_id)).value_or(0);
  }
  std::uint64_t CurrentPopulationManifestId(
      std::string_view group_id) const override {
    const std::optional<MetaTopologyGroupView> group =
        stores_.topology_.FindGroup(std::string(group_id));
    return group.has_value() ? group->record_.population_manifest_id_ : 0;
  }
  bool OperationNonTerminal(const MetaOperationId& id) const override {
    const std::optional<MetaOperationRecord> record =
        stores_.operation_.FindOperation(id);
    return record.has_value() && !IsTerminal(record->lifecycle_);
  }
  bool HistoryBoundToOperation(const MetaOperationId& id,
                               std::uint64_t history_id) const override {
    const std::optional<MetaOperationRecord> record =
        stores_.operation_.FindOperation(id);
    if (!record.has_value()) {
      return false;
    }
    return record->replication_history_id_ != 0 &&
           record->replication_history_id_ == history_id;
  }

 private:
  MetaStores stores_;
};

// Strict decimal u64 ("0" allowed, no signs/padding games, overflow rejects).
bool ParseU64(const std::string& text, std::uint64_t& out) {
  if (text.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  for (const char c : text) {
    if (c < '0' || c > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(c - '0');
    if (value > (UINT64_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  out = value;
  return true;
}

// The trusted {node_id, boot_incarnation, session_generation} triple of the
// obs verbs: node id as 40 hex chars, boot as 32 hex chars (16B), generation
// as decimal u64.
bool ParseObsIdentity(const std::vector<std::string>& tokens, std::size_t base,
                      MetaObservationIdentity& out) {
  if (!IsNodeId(tokens[base])) {
    return false;
  }
  out.node_id_ = tokens[base];
  if (!ParseHexBytes(tokens[base + 1], 2 * out.boot_incarnation_.size(),
                     out.boot_incarnation_.data())) {
    return false;
  }
  return ParseU64(tokens[base + 2], out.session_generation_);
}

const char* ObsAuditKindName(MetaObsAuditKind kind) {
  switch (kind) {
    case MetaObsAuditKind::kRejected:
      return "rejected";
    case MetaObsAuditKind::kStalePurged:
      return "stale-purged";
    case MetaObsAuditKind::kTtlExpired:
      return "ttl-expired";
  }
  return "unknown";
}

using CmdResult = nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>;

// Proposes one encoded meta command and resolves to "OK <log_idx>" once the
// entry commits (the commit result payload is the state machine commit()'s
// return, so OK also means THIS leader has applied it), or "ERR <token>".
// The apply verdict is a separate question — callers verify the effect.
celer::Task<std::string> ProposeCommand(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, MetaCommand command) {
  auto result =
      co_await coordinator->Propose(std::move(command), std::move(principal));
  if (!result.ok()) {
    if (result.status().code() == absl::StatusCode::kFailedPrecondition &&
        result.status().message().find("not leader") !=
            std::string_view::npos) {
      co_return "ERR not-leader";
    }
    if (result.status().code() == absl::StatusCode::kResourceExhausted) {
      co_return "ERR resource-exhausted";
    }
    if (result.status().code() == absl::StatusCode::kDeadlineExceeded) {
      co_return "ERR timeout";
    }
    if (result.status().code() == absl::StatusCode::kCancelled) {
      co_return "ERR cancelled";
    }
    co_return "ERR propose-failed";
  }
  if (result->verdict_ != MetaAuditVerdict::kAccepted) {
    co_return "ERR rejected";
  }
  co_return "OK " + std::to_string(result->log_index_);
}

celer::Task<std::string> HandleSubmitOp(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    nuraft::ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const MetaOperationId& id,
    const std::string& kind, const std::string& payload,
    std::uint64_t replication_history_id) {
  SubmitOperation command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = id;
  command.kind_ = kind;
  command.intent_ = payload;
  command.intent_hash_ = MetaSha256(payload);
  command.replication_history_id_ = replication_history_id;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  // OK never reports an apply-level rejection: verify the committed effect.
  // An idempotent duplicate submit (same id, same intent) verifies
  // identically; a payload-reuse rejection leaves a mismatched intent hash.
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value() || record->intent_hash_ != command.intent_hash_) {
    co_return "ERR rejected";
  }
  co_return reply;
}

celer::Task<std::string> HandleCompleteOp(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    nuraft::ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const MetaOperationId& id,
    const std::string& result) {
  // The CAS token comes from the local committed state: on the leader that
  // accepted the submit, the record is visible at its post-submit revision.
  // A freshly elected leader may legitimately lag behind the submit's OK —
  // "ERR not-found" is the uncertain-outcome signal (plan §3), never a
  // false success.
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value()) {
    co_return "ERR not-found";
  }
  if (IsTerminal(record->lifecycle_)) {
    co_return "ERR terminal";
  }
  CompleteOperation command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = id;
  command.expected_revision_ = record->revision_;
  command.result_ = result;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  const std::optional<MetaOperationRecord> after =
      state_machine->FindOperation(id);
  if (!after.has_value() ||
      after->lifecycle_ != MetaOperationLifecycle::kCompleted ||
      after->terminal_result_ != result) {
    co_return "ERR rejected";
  }
  co_return reply;
}

// Non-linearizable read of the committed journal (see the header).
std::string HandleGetOp(nuraft::ptr<MetaStateMachine> state_machine,
                        const MetaOperationId& id) {
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value()) {
    return "ERR not-found";
  }
  if (IsTerminal(record->lifecycle_)) {
    return std::string("OK ") + LifecycleName(record->lifecycle_) + " " +
           record->terminal_result_;
  }
  return std::string("OK ") + LifecycleName(record->lifecycle_);
}

celer::Task<std::string> HandleRegisterNode(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    nuraft::ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal authenticated, const std::string& node_id,
    const std::string& principal, MetaNodeRole role) {
  RegisterNode command;
  command.request_id_ = MakeRequestId();
  command.node_id_ = node_id;
  command.principal_ = principal;
  command.role_ = role;  // endpoints empty, capability mask 0
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(authenticated), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  // Same effect-verification as submitop: an idempotent re-register with
  // identical content verifies; a principal conflict does not.
  const std::optional<MetaNodeRecord> record = state_machine->FindNode(node_id);
  if (!record.has_value() || record->principal_ != principal ||
      record->role_ != role || record->retired_) {
    co_return "ERR rejected";
  }
  co_return reply;
}

std::string HandleGetNode(nuraft::ptr<MetaStateMachine> state_machine,
                          const std::string& node_id) {
  const std::optional<MetaNodeRecord> record = state_machine->FindNode(node_id);
  if (!record.has_value()) {
    return "ERR not-found";
  }
  return "OK principal=" + record->principal_ +
         " role=" + RoleName(record->role_) +
         " revision=" + std::to_string(record->revision_) +
         (record->retired_ ? " retired=1" : " retired=0");
}

// creategroup <group_id>: the topology epoch is absolute (current + 1), read
// from a committed snapshot. Effect-verified like submitop.
celer::Task<std::string> HandleCreateGroup(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    nuraft::ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& group_id) {
  CreateGroup command;
  command.request_id_ = MakeRequestId();
  command.group_id_ = group_id;
  command.new_topology_epoch_ =
      state_machine->StoresSnapshot().topology_.TopologyEpoch() + 1;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  if (!state_machine->StoresSnapshot().topology_.GroupExists(group_id)) {
    co_return "ERR rejected";
  }
  co_return reply;
}

// begingroupterm <group_id> <expected> <new>: promotes the committed
// group_term (and fences the group), which is what term-bound observations
// anchor to.
celer::Task<std::string> HandleBeginGroupTerm(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    nuraft::ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& group_id,
    std::uint64_t expected, std::uint64_t next) {
  BeginGroupTerm command;
  command.request_id_ = MakeRequestId();
  command.group_id_ = group_id;
  command.expected_term_ = expected;
  command.new_term_ = next;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  const std::optional<std::uint64_t> term =
      state_machine->StoresSnapshot().grant_.CurrentGroupTerm(group_id);
  if (!term.has_value() || *term != next) {
    co_return "ERR rejected";
  }
  co_return reply;
}

// transitionop <id32hex> <phase> <history>: moves the operation to Running.
// The history argument must match the anchor committed by submitop; it is a
// ctl-side consistency check and is not fabricated into evidence.
celer::Task<std::string> HandleTransitionOp(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    nuraft::ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const MetaOperationId& id,
    const std::string& phase, std::uint64_t history) {
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value()) {
    co_return "ERR not-found";
  }
  if (IsTerminal(record->lifecycle_)) {
    co_return "ERR terminal";
  }
  TransitionOperationPhase command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = id;
  command.expected_revision_ = record->revision_;
  command.kind_phase_blob_ = phase;
  if (record->replication_history_id_ != history) {
    co_return "ERR history-mismatch";
  }
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  const std::optional<MetaOperationRecord> after =
      state_machine->FindOperation(id);
  if (!after.has_value() ||
      after->lifecycle_ != MetaOperationLifecycle::kRunning ||
      after->kind_phase_blob_ != phase) {
    co_return "ERR rejected";
  }
  co_return reply;
}

celer::Task<std::string> HandleSetSchema(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, std::uint16_t schema,
    const std::string& attestation) {
  SetSchemaVersion command;
  command.request_id_ = MakeRequestId();
  command.new_active_write_schema_ = schema;
  command.attestation_ = attestation;
  co_return co_await ProposeCommand(coordinator, std::move(principal), command);
}

celer::Task<std::string> HandlePruneAudit(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, std::uint64_t through) {
  PruneAudit command;
  command.request_id_ = MakeRequestId();
  command.through_log_index_ = through;
  co_return co_await ProposeCommand(coordinator, std::move(principal), command);
}

celer::Task<std::string> HandlePruneOperationArchive(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, std::vector<std::uint64_t> seqs) {
  PruneOperationArchive command;
  command.request_id_ = MakeRequestId();
  command.operation_seqs_ = std::move(seqs);
  co_return co_await ProposeCommand(coordinator, std::move(principal), command);
}

// adoptsession: the trusted session layer's stand-in until #20 (plan §4 —
// "本期由 ctl 注入路径模拟可信 generation"). No facts needed: adopting a
// session for an unregistered node is harmless because Ingest re-checks
// registration on every observation.
std::string HandleAdoptSession(
    const std::shared_ptr<MetaObservationStore>& obs_store,
    const MetaObservationIdentity& identity) {
  const absl::Status status = obs_store->AdoptSession(identity, NowUnixMs());
  if (!status.ok()) {
    return "ERR " + std::string(status.message());
  }
  return "OK";
}

// One obs ingest: sweep expired entries first (ctl-frequency TTL hygiene; the
// coordinator also sweeps on its own tick), then admit against a single
// committed snapshot.
std::string HandleObsIngest(
    const std::shared_ptr<MetaObservationStore>& obs_store,
    nuraft::ptr<MetaStateMachine> state_machine, MetaObservation observation) {
  const std::int64_t now = NowUnixMs();
  obs_store->SweepExpired(now);
  const SnapshotCommittedFacts facts(state_machine->StoresSnapshot());
  const absl::Status status =
      obs_store->Ingest(std::move(observation), facts, now);
  if (!status.ok()) {
    return "ERR " + std::string(status.message());
  }
  return "OK";
}

std::string HandleObservations(
    const std::shared_ptr<MetaObservationStore>& obs_store,
    nuraft::ptr<MetaStateMachine> state_machine,
    const std::optional<std::string>& group_id) {
  obs_store->SweepExpired(NowUnixMs());
  if (!group_id.has_value()) {
    return "OK total=" + std::to_string(obs_store->size());
  }
  const SnapshotCommittedFacts facts(state_machine->StoresSnapshot());
  const std::vector<MetaCandidateProgressObs> candidates =
      obs_store->CandidateProgressFor(*group_id, facts);
  std::string reply = "OK candidates=" + std::to_string(candidates.size());
  for (const MetaCandidateProgressObs& candidate : candidates) {
    reply += " term=" + std::to_string(candidate.group_term_) +
             ",manifest=" + std::to_string(candidate.population_manifest_id_) +
             ",history=" + std::to_string(candidate.replication_history_id_) +
             ",readiness=" + candidate.readiness_;
  }
  return reply;
}

std::string HandleObsAudit(
    const std::shared_ptr<MetaObservationStore>& obs_store) {
  const std::vector<MetaObsAuditEvent> events = obs_store->AuditRing();
  std::string reply = "OK events=" + std::to_string(events.size());
  for (const MetaObsAuditEvent& event : events) {
    // Details are whitespace-free single tokens by construction
    // (meta_observation_store.cpp), so the line protocol can dump them raw.
    reply += std::string(" kind=") + ObsAuditKindName(event.kind_) +
             ",node=" + event.node_id_ + ",detail=" + event.detail_ +
             ",ts=" + std::to_string(event.unix_ms_);
  }
  return reply;
}

celer::Task<std::string> HandleConfigChange(
    nuraft::ptr<nuraft::raft_server> server,
    nuraft::ptr<MetaStateMachine> state_machine,
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, std::shared_ptr<MetaCelerBridge> bridge,
    bool add, int server_id, const std::string& endpoint,
    const std::string& member_principal, std::uint16_t min_schema,
    std::uint16_t max_schema) {
  if (!server->is_leader()) {
    co_return "ERR not-leader";
  }
  // Migrate the bootstrap member (and any pre-formal config) into the
  // committed identity store before the next membership mutation. Permission
  // always requires BOTH this binding and NuRaft's committed config aux.
  const nuraft::ptr<nuraft::cluster_config> current_config =
      server->get_config();
  if (current_config == nullptr) {
    co_return "ERR no-config";
  }
  for (const nuraft::ptr<nuraft::srv_config>& member :
       current_config->get_servers()) {
    if (member == nullptr) continue;
    auto descriptor = MetaMemberIdentity::DecodeAux(member->get_aux());
    if (!descriptor.ok()) co_return "ERR bad-member-identity";
    const auto existing =
        state_machine->StoresSnapshot().identity_.FindMetaMember(
            static_cast<std::uint32_t>(member->get_id()));
    if (existing.has_value() && !existing->retired_) continue;
    if (existing.has_value() && existing->retired_) {
      // NuRaft may briefly expose the just-removed member while its leave
      // bookkeeping drains. Do not try to reactivate the terminal binding;
      // report the same retryable state as a concurrent config mutation.
      co_return "ERR config-changing";
    }
    BindMetaMember bind;
    bind.request_id_ = MakeRequestId();
    bind.server_id_ = static_cast<std::uint32_t>(descriptor->server_id_);
    bind.principal_ = descriptor->principal_;
    bind.min_schema_ = descriptor->min_schema_;
    bind.max_schema_ = descriptor->max_schema_;
    std::string bound = co_await ProposeCommand(coordinator, principal, bind);
    if (bound.rfind("OK ", 0) != 0) co_return bound;
  }
  nuraft::ptr<CmdResult> result;
  if (add) {
    const MetaMemberIdentity identity{server_id, member_principal, min_schema,
                                      max_schema};
    auto canonical = MetaMemberIdentity::DecodeAux(identity.EncodeAux());
    if (!canonical.ok()) {
      co_return "ERR bad-member-identity";
    }
    const std::uint16_t active_schema =
        state_machine->StoresSnapshot().active_write_schema_;
    if (min_schema > active_schema || max_schema < active_schema) {
      co_return "ERR schema-incompatible";
    }
    BindMetaMember bind;
    bind.request_id_ = MakeRequestId();
    bind.server_id_ = static_cast<std::uint32_t>(server_id);
    bind.principal_ = member_principal;
    bind.min_schema_ = min_schema;
    bind.max_schema_ = max_schema;
    std::string bound = co_await ProposeCommand(coordinator, principal, bind);
    if (bound.rfind("OK ", 0) != 0) co_return bound;
    nuraft::srv_config config(server_id, /*dc_id=*/0, endpoint,
                              identity.EncodeAux(), /*learner=*/false);
    result = server->add_srv(config);
  } else {
    result = server->remove_srv(server_id);
  }
  if (result == nullptr) {
    co_return "ERR no-result";
  }
  std::shared_ptr<AsyncReply> state = std::make_shared<AsyncReply>();
  CmdResult::handler_type2 handler =
      [bridge, state](CmdResult& completed,
                      nuraft::ptr<std::exception>& err) mutable {
        std::string reply;
        if (err != nullptr) {
          reply = "ERR exception";
        } else if (completed.get_result_code() == nuraft::cmd_result_code::OK &&
                   completed.get_accepted()) {
          reply = "OK";
        } else {
          reply = "ERR " + CmdResultToken(completed.get_result_code());
        }
        CompleteAsyncReply(bridge, std::move(state), std::move(reply));
      };
  result->when_ready(handler);
  std::string reply = co_await AsyncReplyAwaiter(std::move(state));
  if (add || (reply != "OK" && reply != "ERR not-found")) {
    co_return reply;
  }
  RetireMetaMember retire;
  retire.request_id_ = MakeRequestId();
  retire.server_id_ = static_cast<std::uint32_t>(server_id);
  std::string retired =
      co_await ProposeCommand(coordinator, std::move(principal), retire);
  if (retired.rfind("OK ", 0) != 0) co_return retired;
  co_return "OK";
}

std::vector<std::string> SplitTokens(std::string_view line) {
  std::vector<std::string> tokens;
  std::size_t pos = 0;
  while (pos < line.size()) {
    while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) {
      ++pos;
    }
    const std::size_t begin = pos;
    while (pos < line.size() && line[pos] != ' ' && line[pos] != '\t') {
      ++pos;
    }
    if (begin < pos) {
      tokens.emplace_back(line.substr(begin, pos - begin));
    }
  }
  return tokens;
}

bool ParseServerId(const std::string& text, int& out) {
  try {
    std::size_t used = 0;
    const int value = std::stoi(text, &used);
    if (used != text.size() || value <= 0) {
      return false;
    }
    out = value;
    return true;
  } catch (...) {
    return false;
  }
}

// Committed-mutation verbs: everything that proposes onto the raft log.
// Split from DispatchCommand so the caller can run the observation
// revalidation pass once per successful commit (see DispatchCommand).
celer::Task<std::string> DispatchMutationVerb(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    nuraft::ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& command,
    const std::vector<std::string>& tokens) {
  if (command == "submitop" || command == "completeop") {
    const bool submit = command == "submitop";
    // submitop <id> <kind> <payload> [replication_history_id];
    // completeop <id> <result>.
    if ((!submit && tokens.size() != 3u) ||
        (submit && tokens.size() != 4u && tokens.size() != 5u)) {
      co_return "ERR bad-request";
    }
    MetaOperationId id{};
    if (!ParseOperationId(tokens[1], id)) {
      co_return "ERR bad-request";
    }
    if (submit) {
      std::uint64_t history = 0;
      if (tokens.size() == 5u && !ParseU64(tokens[4], history)) {
        co_return "ERR bad-request";
      }
      co_return co_await HandleSubmitOp(coordinator, std::move(state_machine),
                                        std::move(principal), id, tokens[2],
                                        tokens[3], history);
    }
    co_return co_await HandleCompleteOp(coordinator, std::move(state_machine),
                                        std::move(principal), id, tokens[2]);
  }
  if (command == "registernode") {
    if (tokens.size() != 4 || !IsNodeId(tokens[1])) {
      co_return "ERR bad-request";
    }
    MetaNodeRole role;
    if (tokens[3] == "primary") {
      role = MetaNodeRole::kPrimary;
    } else if (tokens[3] == "replica") {
      role = MetaNodeRole::kReplica;
    } else {
      co_return "ERR bad-request";
    }
    co_return co_await HandleRegisterNode(coordinator, std::move(state_machine),
                                          std::move(principal), tokens[1],
                                          tokens[2], role);
  }
  if (command == "creategroup") {
    if (tokens.size() != 2 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleCreateGroup(coordinator, std::move(state_machine),
                                         std::move(principal), tokens[1]);
  }
  if (command == "begingroupterm") {
    if (tokens.size() != 4 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes) {
      co_return "ERR bad-request";
    }
    std::uint64_t expected = 0;
    std::uint64_t next = 0;
    if (!ParseU64(tokens[2], expected) || !ParseU64(tokens[3], next)) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleBeginGroupTerm(
        coordinator, std::move(state_machine), std::move(principal), tokens[1],
        expected, next);
  }
  if (command == "transitionop") {
    if (tokens.size() != 4) {
      co_return "ERR bad-request";
    }
    MetaOperationId id{};
    std::uint64_t history = 0;
    if (!ParseOperationId(tokens[1], id) || !ParseU64(tokens[3], history)) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleTransitionOp(coordinator, std::move(state_machine),
                                          std::move(principal), id, tokens[2],
                                          history);
  }
  if (command == "setschema") {
    std::uint64_t schema = 0;
    if (tokens.size() != 3 || !ParseU64(tokens[1], schema) ||
        schema > UINT16_MAX) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleSetSchema(coordinator, std::move(principal),
                                       static_cast<std::uint16_t>(schema),
                                       tokens[2]);
  }
  if (command == "pruneaudit") {
    std::uint64_t through = 0;
    if (tokens.size() != 2 || !ParseU64(tokens[1], through) || through == 0) {
      co_return "ERR bad-request";
    }
    co_return co_await HandlePruneAudit(coordinator, std::move(principal),
                                        through);
  }
  if (command == "pruneoperations") {
    if (tokens.size() < 2 ||
        tokens.size() - 1 > kMaxMetaArchivedOperationSummaries) {
      co_return "ERR bad-request";
    }
    std::vector<std::uint64_t> seqs;
    seqs.reserve(tokens.size() - 1);
    for (std::size_t i = 1; i < tokens.size(); ++i) {
      std::uint64_t seq = 0;
      if (!ParseU64(tokens[i], seq) || seq == 0) {
        co_return "ERR bad-request";
      }
      seqs.push_back(seq);
    }
    co_return co_await HandlePruneOperationArchive(
        coordinator, std::move(principal), std::move(seqs));
  }
  co_return "ERR unknown-command";
}

// Runs on the celer worker thread and suspends only on bridge round trips. The
// shared references keep command dependencies alive if teardown releases the
// core's references mid-command. Authentication has already resolved the
// trusted principal; actor fields never come from command text. Observation
// access is internally serialized because commit-driven revalidation can run
// concurrently with this worker.
celer::Task<std::string> DispatchCommand(
    nuraft::ptr<nuraft::raft_server> server,
    nuraft::ptr<MetaStateMachine> state_machine,
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaObservationStore> obs_store,
    std::shared_ptr<MetaCelerBridge> bridge,
    const MetaPrincipalIdentity& identity, AuthenticatedPrincipal principal,
    std::string_view line) {
  const std::vector<std::string> tokens = SplitTokens(line);
  if (tokens.empty()) {
    co_return "ERR bad-request";
  }
  const std::string& command = tokens[0];
  MetaAccess access = MetaAccess::kPrivileged;
  std::string_view target_node_id;
  if (command == "status") {
    access = MetaAccess::kStatus;
  } else if (command == "adoptsession" && tokens.size() >= 2) {
    access = MetaAccess::kObservationWrite;
    target_node_id = tokens[1];
  } else if (command == "obs" && tokens.size() >= 3) {
    access = MetaAccess::kObservationWrite;
    target_node_id = tokens[2];
  }
  if (!AuthorizeMetaAccess(identity, access, target_node_id).ok()) {
    co_return "ERR forbidden";
  }
  if (command == "submitop" || command == "completeop" ||
      command == "registernode" || command == "creategroup" ||
      command == "begingroupterm" || command == "transitionop" ||
      command == "setschema" || command == "pruneaudit" ||
      command == "pruneoperations") {
    std::string reply = co_await DispatchMutationVerb(
        coordinator, state_machine, std::move(principal), command, tokens);
    co_return reply;
  }
  if (command == "getop") {
    if (tokens.size() != 2) {
      co_return "ERR bad-request";
    }
    MetaOperationId id{};
    if (!ParseOperationId(tokens[1], id)) {
      co_return "ERR bad-request";
    }
    co_return HandleGetOp(std::move(state_machine), id);
  }
  if (command == "getnode") {
    if (tokens.size() != 2 || !IsNodeId(tokens[1])) {
      co_return "ERR bad-request";
    }
    co_return HandleGetNode(std::move(state_machine), tokens[1]);
  }
  if (command == "adoptsession") {
    if (tokens.size() != 4) {
      co_return "ERR bad-request";
    }
    MetaObservationIdentity identity;
    if (!ParseObsIdentity(tokens, 1, identity)) {
      co_return "ERR bad-request";
    }
    co_return HandleAdoptSession(obs_store, identity);
  }
  if (command == "obs") {
    // obs <kind> <node> <boot> <gen> <kind fields...>; see the header.
    if (tokens.size() < 5) {
      co_return "ERR bad-request";
    }
    const std::string& kind = tokens[1];
    MetaObservationIdentity identity;
    if (!ParseObsIdentity(tokens, 2, identity)) {
      co_return "ERR bad-request";
    }
    MetaObservation observation;
    observation.identity_ = std::move(identity);
    if (kind == "boot" && tokens.size() == 5) {
      observation.payload_ = MetaNodeBootObs{};
    } else if (kind == "health" && tokens.size() == 6) {
      MetaNodeHealthObs payload;
      payload.health_ = tokens[5];
      observation.payload_ = std::move(payload);
    } else if (kind == "candidate" && tokens.size() == 12) {
      MetaCandidateProgressObs payload;
      payload.group_id_ = tokens[5];
      if (!ParseU64(tokens[6], payload.group_term_) ||
          !ParseU64(tokens[7], payload.population_manifest_id_) ||
          !ParseU64(tokens[8], payload.replication_history_id_)) {
        co_return "ERR bad-request";
      }
      payload.applied_flow_vector_ = tokens[9];
      payload.backlog_coverage_ = tokens[10];
      payload.readiness_ = tokens[11];
      observation.payload_ = std::move(payload);
    } else if (kind == "evidence" && tokens.size() == 12) {
      MetaOperationEvidenceObs payload;
      if (!ParseOperationId(tokens[5], payload.operation_id_)) {
        co_return "ERR bad-request";
      }
      payload.kind_phase_ = tokens[6];
      payload.evidence_ = tokens[7];
      // The digest of the normalized payload is computed at ingestion; the
      // wire never carries a self-reported hash.
      payload.evidence_hash_ = MetaSha256(payload.evidence_);
      payload.group_id_ = tokens[8];
      if (!ParseU64(tokens[9], payload.group_term_) ||
          !ParseU64(tokens[10], payload.population_manifest_id_) ||
          !ParseU64(tokens[11], payload.replication_history_id_)) {
        co_return "ERR bad-request";
      }
      observation.payload_ = std::move(payload);
    } else {
      co_return "ERR bad-request";
    }
    co_return HandleObsIngest(obs_store, std::move(state_machine),
                              std::move(observation));
  }
  if (command == "observations") {
    if (tokens.size() == 1) {
      co_return HandleObservations(obs_store, std::move(state_machine),
                                   std::nullopt);
    }
    if (tokens.size() == 2 && !tokens[1].empty() &&
        tokens[1].size() <= kMaxMetaGroupIdBytes) {
      co_return HandleObservations(obs_store, std::move(state_machine),
                                   tokens[1]);
    }
    co_return "ERR bad-request";
  }
  if (command == "obsaudit") {
    if (tokens.size() != 1) {
      co_return "ERR bad-request";
    }
    co_return HandleObsAudit(obs_store);
  }
  if (command == "exportaudit") {
    std::uint64_t through = 0;
    if (tokens.size() != 2 || !ParseU64(tokens[1], through) || through == 0) {
      co_return "ERR bad-request";
    }
    auto exported =
        state_machine->StoresSnapshot().audit_.ExportThrough(through);
    if (!exported.ok()) co_return "ERR rejected";
    co_return "OK " + HexEncode(*exported);
  }
  if (command == "exportoperations") {
    if (tokens.size() != 1) co_return "ERR bad-request";
    auto exported = state_machine->StoresSnapshot().operation_.ExportArchive();
    if (!exported.ok()) co_return "ERR rejected";
    co_return "OK " + HexEncode(*exported);
  }
  if (command == "status") {
    co_return "OK leader=" + std::to_string(server->is_leader() ? 1 : 0) +
        " id=" + std::to_string(server->get_id()) +
        " committed=" + std::to_string(server->get_committed_log_idx()) +
        " snapshot_idx=" + std::to_string(server->get_last_snapshot_idx()) +
        " term=" + std::to_string(server->get_term()) +
        " schema=" + std::to_string(state_machine->active_write_schema());
  }
  if (command == "addsrv" || command == "removesrv") {
    const bool add = command == "addsrv";
    if ((!add && tokens.size() != 2u) ||
        (add && tokens.size() != 3u && tokens.size() != 6u)) {
      co_return "ERR bad-request";
    }
    int server_id = 0;
    if (!ParseServerId(tokens[1], server_id)) {
      co_return "ERR bad-request";
    }
    std::string member_principal =
        "keylane://meta/" + std::to_string(server_id);
    std::uint64_t min_schema = kMetaMinReadableSchemaVersion;
    std::uint64_t max_schema = kMetaCurrentSchemaVersion;
    if (add && tokens.size() == 6u) {
      member_principal = tokens[3];
      if (!ParseU64(tokens[4], min_schema) ||
          !ParseU64(tokens[5], max_schema) || min_schema > UINT16_MAX ||
          max_schema > UINT16_MAX) {
        co_return "ERR bad-request";
      }
    }
    co_return co_await HandleConfigChange(
        std::move(server), std::move(state_machine), coordinator,
        std::move(principal), std::move(bridge), add, server_id,
        add ? tokens[2] : std::string(), member_principal,
        static_cast<std::uint16_t>(min_schema),
        static_cast<std::uint16_t>(max_schema));
  }
  if (command == "snapshot") {
    // Plan §3 快照切点: a manual snapshot must serialize against the commit
    // thread — serialize_commit_ blocks the background commit until the
    // state machine's exact-cut capture returns (NuRaft semantics per
    // raft_server.hxx create_snapshot_options). The capture is synchronous
    // and KB-scale on this worker thread; the durability write is handed to
    // the state machine's writer thread, so the reply only guarantees the
    // cut point, and compaction completes asynchronously. A round already
    // in flight fails fast (returns 0).
    nuraft::raft_server::create_snapshot_options options;
    options.serialize_commit_ = true;
    const std::uint64_t idx = server->create_snapshot(options);
    if (idx == 0) {
      co_return "ERR snapshot-failed";
    }
    co_return "OK " + std::to_string(idx);
  }
  co_return "ERR unknown-command";
}

}  // namespace

// static
absl::Status MetaCtlServer::ValidateOptions(
    const MetaCtlServerOptions& options) {
  if (options.transport_ == MetaCtlServerOptions::Transport::kUnix) {
    if (options.unix_socket_path_.empty()) {
      return absl::InvalidArgumentError(
          "Unix ctl socket path must not be empty");
    }
    if (options.allowed_uids_.empty()) {
      return absl::InvalidArgumentError(
          "Unix ctl socket requires at least one allowed uid");
    }
    if (options.port_ != 0 || !options.bind_host_.empty() ||
        !options.tls_ca_cert_file_.empty() || !options.tls_cert_file_.empty() ||
        !options.tls_key_file_.empty()) {
      return absl::InvalidArgumentError(
          "Unix ctl options cannot be mixed with TCP or TLS options");
    }
    return absl::OkStatus();
  }

  if (options.port_ == 0) {
    return absl::InvalidArgumentError("TCP ctl port must be non-zero");
  }
  if (options.tls_ca_cert_file_.empty() || options.tls_cert_file_.empty() ||
      options.tls_key_file_.empty()) {
    return absl::InvalidArgumentError(
        "TCP ctl requires --ctl-tls-ca, --ctl-tls-cert and --ctl-tls-key; "
        "plaintext TCP administration is forbidden");
  }
  if (!options.unix_socket_path_.empty() || !options.allowed_uids_.empty()) {
    return absl::InvalidArgumentError(
        "TCP ctl options cannot be mixed with Unix socket options");
  }
  return absl::OkStatus();
}

// static
absl::StatusOr<std::shared_ptr<MetaCtlServer>> MetaCtlServer::Create(
    std::shared_ptr<MetaCelerBridge> bridge,
    nuraft::ptr<nuraft::raft_server> server,
    nuraft::ptr<MetaStateMachine> state_machine,
    std::shared_ptr<MetaCoordinator> coordinator,
    std::shared_ptr<MetaObservationStore> obs_store,
    MetaCtlServerOptions options) {
  if (bridge == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "bridge must not be null");
  }
  if (server == nullptr || state_machine == nullptr || coordinator == nullptr) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "server, state machine, and coordinator must not be null");
  }
  if (obs_store == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "observation store must not be null");
  }
  const absl::Status valid = ValidateOptions(options);
  if (!valid.ok()) return valid;
  auto core = std::make_shared<Core>();
  core->bridge_ = std::move(bridge);
  core->server_ = std::move(server);
  core->state_machine_ = std::move(state_machine);
  core->coordinator_ = std::move(coordinator);
  core->obs_store_ = std::move(obs_store);
  core->options_ = std::move(options);
  if (core->options_.transport_ == MetaCtlServerOptions::Transport::kTcpMtls) {
    celer::TlsServerOptions tls;
    tls.cert_file_ = core->options_.tls_cert_file_;
    tls.key_file_ = core->options_.tls_key_file_;
    tls.ca_cert_file_ = core->options_.tls_ca_cert_file_;
    tls.client_auth_ = celer::TlsClientAuth::kRequired;
    auto context = celer::TlsContext::CreateServer(tls);
    if (!context.ok()) return context.status();
    core->tls_context_ = std::move(*context);
  }
  return std::shared_ptr<MetaCtlServer>(new MetaCtlServer(std::move(core)));
}

MetaCtlServer::~MetaCtlServer() { PostShutdown(); }

void MetaCtlServer::Start() {
  CorePtr core = core_;
  core->bridge_->Post([core](celer::Worker& worker) mutable {
    if (core->listening_) {
      return;
    }
    core->worker_ = &worker;
    absl::Status bound;
    if (core->options_.transport_ == MetaCtlServerOptions::Transport::kUnix) {
      bound = core->listener_.BindUnix(
          &worker, core->options_.unix_socket_path_, /*backlog=*/128,
          /*mode=*/0600);
    } else {
      bound = core->listener_.Bind(&worker, core->options_.bind_host_,
                                   core->options_.port_, /*backlog=*/128,
                                   /*reuse_port=*/false);
    }
    {
      std::lock_guard<std::mutex> lock(core->status_mu_);
      core->status_ = bound;
    }
    if (!bound.ok()) {
      return;
    }
    core->listening_ = true;
    worker.Spawn(AcceptLoop(core));
  });
}

void MetaCtlServer::Shutdown() { PostShutdown(); }

absl::Status MetaCtlServer::status() const {
  std::lock_guard<std::mutex> lock(core_->status_mu_);
  return core_->status_;
}

void MetaCtlServer::PostShutdown() {
  CorePtr core = core_;
  core->bridge_->Post([core](celer::Worker& worker) {
    core->listening_ = false;
    (void)core->listener_.Close();
    // Fail the pending session reads; each session coroutine removes itself
    // from sessions_ as it exits.
    for (celer::Connection* connection : core->sessions_) {
      worker.BeginClose(
          connection,
          absl::Status(absl::StatusCode::kCancelled, "ctl server shutdown"),
          celer::CloseMode::kLocalClose);
    }
    // Drop the shared references on the worker; a parked propose/addsrv holds
    // its own copy until its reply lands. meta_main also keeps the observation
    // store alive through worker shutdown.
    core->server_.reset();
    core->state_machine_.reset();
    core->coordinator_.reset();
    core->obs_store_.reset();
    core->tls_context_.reset();
  });
}

celer::Task<absl::Status> MetaCtlServer::AcceptLoop(CorePtr core) {
  celer::Worker& worker = *core->worker_;
  while (core->listening_) {
    auto accepted = co_await core->listener_.Accept();
    if (!accepted.ok()) {
      // A closed listener fails the pending accept; that is the
      // Shutdown() path.
      if (!core->listening_) {
        break;
      }
      co_await celer::SleepFor(worker, std::chrono::milliseconds(10));
      continue;
    }
    celer::Connection* connection = *accepted;
    core->sessions_.push_back(connection);
    worker.Spawn(SessionLoop(core, celer::TcpStream(connection), connection));
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> MetaCtlServer::SessionLoop(
    CorePtr core, celer::TcpStream stream, celer::Connection* connection) {
  const auto remove_session = [&] {
    std::vector<celer::Connection*>& sessions = core->sessions_;
    for (auto it = sessions.begin(); it != sessions.end(); ++it) {
      if (*it == connection) {
        *it = sessions.back();
        sessions.pop_back();
        return;
      }
    }
  };
  absl::StatusOr<MetaPrincipalIdentity> identity =
      absl::UnauthenticatedError("ctl session was not authenticated");
  if (core->options_.transport_ == MetaCtlServerOptions::Transport::kUnix) {
    ucred credentials{};
    socklen_t size = sizeof(credentials);
    if (::getsockopt(stream.NativeFd(), SOL_SOCKET, SO_PEERCRED, &credentials,
                     &size) != 0 ||
        size != sizeof(credentials)) {
      spdlog::warn("ctl rejected Unix peer: SO_PEERCRED failed: {}",
                   std::strerror(errno));
      (void)stream.Close();
      remove_session();
      co_return absl::UnauthenticatedError("SO_PEERCRED failed");
    }
    identity = AuthenticateLocalOperator(credentials.uid,
                                         core->options_.allowed_uids_);
  } else {
    const absl::Status tls =
        co_await stream.StartTls(core->tls_context_, /*server=*/true);
    if (!tls.ok()) {
      spdlog::warn("ctl rejected TCP peer during mTLS handshake: {}",
                   tls.message());
      (void)stream.Close();
      remove_session();
      co_return tls;
    }
    auto sans = stream.PeerCertificateUriSans();
    if (!sans.ok()) {
      (void)stream.Close();
      remove_session();
      co_return sans.status();
    }
    identity = AuthenticateMetaUriSans(*sans);
    if (identity.ok() && identity->role_ == MetaPrincipalRole::kMetaMember) {
      identity = absl::PermissionDeniedError(
          "Meta member certificate is not an admin/control identity");
    }
  }
  if (!identity.ok()) {
    spdlog::warn("ctl rejected unauthenticated peer: {}",
                 identity.status().message());
    (void)stream.Close();
    remove_session();
    co_return identity.status();
  }
  AuthenticatedPrincipal authenticated(identity->principal_,
                                       MetaPrincipalPasskey{});
  std::byte chunk[4096];
  std::string pending;
  bool drop = false;
  while (!drop) {
    auto read = co_await stream.ReadSome(chunk);
    if (!read.ok() || *read == 0) {
      break;  // peer EOF, Shutdown() close, or worker teardown
    }
    pending.append(reinterpret_cast<const char*>(chunk), *read);

    std::size_t newline = std::string::npos;
    while ((newline = pending.find('\n')) != std::string::npos) {
      std::string line = pending.substr(0, newline);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      pending.erase(0, newline + 1);
      // One in-flight command per connection: replies stay FIFO and the
      // gate driver is synchronous per connection anyway.
      std::string reply = co_await DispatchCommand(
          core->server_, core->state_machine_, core->coordinator_,
          core->obs_store_, core->bridge_, *identity, authenticated, line);
      reply.push_back('\n');
      const absl::Status written =
          co_await stream.WriteAll(std::span<const std::byte>(
              reinterpret_cast<const std::byte*>(reply.data()), reply.size()));
      if (!written.ok()) {
        drop = true;
        break;
      }
    }
    if (!drop && pending.size() > kMaxLineBytes) {
      const std::string reply = "ERR bad-request\n";
      (void)co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(reply.data()), reply.size()));
      break;  // framing abuse: close without reading further
    }
  }

  remove_session();
  (void)stream.Close();
  co_return absl::OkStatus();
}

}  // namespace keylane::meta
