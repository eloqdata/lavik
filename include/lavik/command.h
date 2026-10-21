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

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/runtime/task.h"
#include "lavik/cluster/topology.h"
#include "lavik/command_args.h"
#include "lavik/read_trace.h"
#include "lavik/resp_version.h"
#include "lavik/retained_allocator.h"
#include "lavik/set_trace.h"
#include "lavik/storage/engine.h"

namespace lavik {

class BlockingWakeCascade;
class CapturedPubSubPublication;

namespace cluster {
class AuthorityAdmission;
}

namespace tx {
class Transaction;
}

struct RespCommand;
struct ReplicatedCommand;
class ReplyBuilder;
class ReplicationManager;

using bycorf::Task;

struct CapturedReplicationCommand {
  CapturedReplicationCommand() = default;
  CapturedReplicationCommand(std::uint8_t db_id, std::vector<std::string> args);
  CapturedReplicationCommand(std::uint8_t db_id,
                             std::initializer_list<std::string_view> args)
      : CapturedReplicationCommand(db_id, CopyCommandArgs(args)) {}
  CapturedReplicationCommand(const CapturedReplicationCommand& other)
      : CapturedReplicationCommand(other.db_id_, other.args_) {}
  CapturedReplicationCommand& operator=(
      const CapturedReplicationCommand& other) {
    if (this != &other) *this = CapturedReplicationCommand(other);
    return *this;
  }
  CapturedReplicationCommand(CapturedReplicationCommand&&) noexcept = default;
  CapturedReplicationCommand& operator=(CapturedReplicationCommand&&) noexcept =
      default;

  // Actual retained argument storage follows effects through EXEC/Lua. The
  // enclosing write is already admitted, so this charge may cross the soft
  // budget. Argument buffers die before their accounting owner.
  std::shared_ptr<RetainedMemoryCharge> retained_charge_;
  std::uint8_t db_id_ = 0;
  std::vector<std::string> args_;
};

// Accepted effects may grow beyond the soft budget. Their vector allocation
// is accounted independently of each argument owner, including when effects
// move from one child capture into a long-lived Lua or EXEC aggregate.
template <typename T>
class ReplicationCaptureAllocator : public RetainedAllocator<T> {
 public:
  using propagate_on_container_swap = std::true_type;
  ReplicationCaptureAllocator() noexcept
      : RetainedAllocator<T>(
            RetainedAllocationDomain{.externally_admitted_ = true}) {}
  template <typename U>
  ReplicationCaptureAllocator(
      const ReplicationCaptureAllocator<U>& other) noexcept
      : RetainedAllocator<T>(other.domain()) {}
  template <typename U>
  struct rebind {
    using other = ReplicationCaptureAllocator<U>;
  };
};

using CapturedReplicationCommands =
    std::vector<CapturedReplicationCommand,
                ReplicationCaptureAllocator<CapturedReplicationCommand>>;

struct CapturedReplicationEffects {
  bool handled_ = false;
  CapturedReplicationCommands commands_;
};

// EXEC handlers record outcome-dependent commands here (absolute deadlines,
// selected random members, generated stream IDs, consumer-group after-images).
// A command normally writes from one worker, while XREADGROUP may visit
// several owners, so recording is safe from any participant worker.
class ReplicationCommandCapture {
 public:
  // Reserve effect slots without a second soft-budget admission. The retained
  // allocator charges slots immediately; each recorded command owns its args.
  absl::Status ReserveAdditionalCommands(std::size_t count);
  // Release unused slots after a command finishes without effects.
  void ReleaseUnusedPreparation();
  // Preserve argument accounting through nested EVAL/EXEC captures.
  void Record(CapturedReplicationCommand command);
  void MarkHandled();
  void Record(std::uint8_t db_id, std::vector<std::string> args);
  CapturedReplicationEffects Take();
  void SetCapturedPubSubPublication(
      std::shared_ptr<CapturedPubSubPublication> publication);
  std::shared_ptr<CapturedPubSubPublication> TakeCapturedPubSubPublication();

 private:
  mutable std::mutex mutex_;
  bool handled_ = false;
  CapturedReplicationCommands commands_;
  // A source-side EXEC must publish its replication envelope before local
  // subscriber-visible delivery. Keep the command-time recipient snapshot
  // beside the corresponding replication effects so every failure path drops
  // both together without changing CommandRequest's fixed-size hot layout.
  std::shared_ptr<CapturedPubSubPublication> pubsub_publication_;
};

struct CapturedBlockingNotification {
  std::uint8_t db_id_ = 0;
  std::string key_;
  storage::ValueType value_type_ = storage::ValueType::kNone;
};

// EXEC handlers can run on several workers and must not expose readiness from
// an intermediate command. They record candidate keys here; the coordinator
// filters them by the transaction's final value type before waking waiters.
class BlockingNotificationCapture {
 public:
  explicit BlockingNotificationCapture(unsigned worker_count);

  void Record(std::uint8_t db_id, std::string key,
              storage::ValueType value_type);
  std::vector<CapturedBlockingNotification> Take();

 private:
  // Each runtime worker only appends to its own slot. EXEC consumes the slots
  // after all shard callbacks have joined, so this needs no cross-worker lock.
  std::vector<std::vector<CapturedBlockingNotification>> per_worker_;
};

enum class CommandKind {
  kPing,
  kEcho,
  kPublish,
  kPubSub,
  kPSubscribe,
  kPUnsubscribe,
  kSubscribe,
  kUnsubscribe,
  kQuit,
  kReset,
  kAuth,
  kHello,
  kDbSize,
  kDel,
  kUnlink,
  kRename,
  kRenameNx,
  kCopy,
  kExists,
  kTouch,
  kRandomKey,
  kFlushDb,
  kFlushAll,
  kGet,
  kGetDel,
  kGetEx,
  kGetRange,
  kGetSet,
  kAppend,
  kGetBit,
  kSetBit,
  kBitCount,
  kBitPos,
  kBitField,
  kBitFieldRo,
  kBitOp,
  kStrlen,
  kIncr,
  kIncrBy,
  kIncrByFloat,
  kDecr,
  kDecrBy,
  kSetEx,
  kPSetEx,
  kSetNx,
  kSetRange,
  kSubstr,
  kLcs,
  kExpire,
  kPExpire,
  kExpireAt,
  kPExpireAt,
  kPersist,
  kTtl,
  kPttl,
  kExpireTime,
  kPExpireTime,
  kScan,
  kType,
  kDump,
  kRestore,
  kSort,
  kSortRo,
  kSelect,
  kEval,
  kEvalSha,
  kEvalRo,
  kEvalShaRo,
  kScript,
  kFCall,
  kFCallRo,
  kFunction,
  kSet,
  kLPush,
  kLPushX,
  kRPush,
  kRPushX,
  kLPop,
  kRPop,
  kLLen,
  kLIndex,
  kLRange,
  kLSet,
  kLInsert,
  kLRem,
  kLTrim,
  kLPos,
  kLMove,
  kRPopLPush,
  kLMPop,
  kBLPop,
  kBRPop,
  kBLMove,
  kBRPopLPush,
  kBLMPop,
  kHSet,
  kHMSet,
  kHReplace,
  kHSetNx,
  kHGet,
  kHMGet,
  kHDel,
  kHLen,
  kHExists,
  kHGetAll,
  kHKeys,
  kHVals,
  kHStrlen,
  kHIncrBy,
  kHIncrByFloat,
  kHRandField,
  kHScan,
  kSAdd,
  kSCard,
  kSDiff,
  kSDiffStore,
  kSInter,
  kSInterCard,
  kSInterStore,
  kSIsMember,
  kSMembers,
  kSMIsMember,
  kSMove,
  kSPop,
  kSRandMember,
  kSRem,
  kSScan,
  kSUnion,
  kSUnionStore,
  kBZMPop,
  kBZPopMax,
  kBZPopMin,
  kZAdd,
  kZCard,
  kZCount,
  kZIncrBy,
  kZLexCount,
  kZMPop,
  kZMScore,
  kZPopMax,
  kZPopMin,
  kZRandMember,
  kZRange,
  kZRangeStore,
  kZRangeByLex,
  kZRangeByScore,
  kZRank,
  kZRem,
  kZRemRangeByLex,
  kZRemRangeByRank,
  kZRemRangeByScore,
  kZRevRange,
  kZRevRangeByLex,
  kZRevRangeByScore,
  kZRevRank,
  kZScan,
  kZScore,
  kZDiff,
  kZDiffStore,
  kZInter,
  kZInterCard,
  kZInterStore,
  kZUnion,
  kZUnionStore,
  kGeoAdd,
  kGeoDist,
  kGeoHash,
  kGeoPos,
  kGeoRadius,
  kGeoRadiusRo,
  kGeoRadiusByMember,
  kGeoRadiusByMemberRo,
  kGeoSearch,
  kGeoSearchStore,
  kXAdd,
  kXDel,
  kXLen,
  kXRange,
  kXRevRange,
  kXTrim,
  kXSetId,
  kXGroup,
  kXAck,
  kXPending,
  kXClaim,
  kXAutoClaim,
  kXInfo,
  kXRead,
  kXReadGroup,
  kMSet,
  kMSetNx,
  kMGet,
  kMulti,
  kExec,
  kDiscard,
  kWatch,
  kUnwatch,
  kClient,
  kReplicaOf,
  kLavikReplicaOf,
  kAddReplicaOf,
  kConfig,
  kInfo,
  kRole,
  kWait,
  kCluster,
  kCommand,
  kReadOnly,
  kReadWrite,
  kKeys,
  kSave,
  kBgSave,
  kLastSave,
  kMonitor,
  kSlowLog,
  kTombRaider,
  kDefrag,
  kUnknown,
  kCount,
};

struct CommandSpec;

// CommandRequest is stored directly in Bycorf coroutine frames. Their allocator
// guarantees max_align_t, so stronger type alignment would let GCC emit aligned
// SIMD stores that the frame cannot honor. Do not over-align the type without
// extending the coroutine allocation contract and measuring its cost on
// ordinary commands.
struct alignas(std::max_align_t) CommandRequest {
  CommandKind kind_ = CommandKind::kUnknown;
  std::uint8_t db_id_ = 0;
  // Reply protocol for every nested/cross-core execution path. Replication
  // and internal callers naturally default to RESP2 because their replies are
  // discarded; client dispatch overwrites this from the connection.
  RespVersion resp_version_ = RespVersion::k2;
  // These values share one byte: they are request-local control state, not a
  // durable or wire representation. Replication-origin commands bypass
  // replica read-only checks and must not be published again. TLS controls
  // which advertised port cluster replies select. The slot count needs only
  // 0, 1, or 2; two distinct slots are already a terminal CROSSSLOT witness.
  std::uint8_t replication_origin_ : 1 = false;
  std::uint8_t connection_tls_ : 1 = false;
  std::uint8_t cluster_slot_sample_count_ : 2 = 0;
  std::uint8_t write_admission_role_epoch_valid_ : 1 = false;
  std::uint8_t serving_generation_valid_ : 1 = false;
  // Source-side EXEC captures PUBLISH for ordered replication and delays its
  // local delivery until that publication commits.
  std::uint8_t defer_pubsub_delivery_ : 1 = false;
  // EXEC with dynamic SORT_RO pattern reads owns an exclusive database cut.
  // Nested handlers may read derived keys without extending a held lock set.
  std::uint8_t exclusive_db_access_ : 1 = false;
  // Captured when a client write chooses its source-publication path. A DB
  // gate that reopens under a different role must reject the stale request
  // before mutation, including when writable replicas are enabled. Its valid
  // bit shares the control byte above, retaining every uint64_t epoch value
  // while avoiding optional<uint64_t>'s extra word.
  std::uint64_t write_admission_role_epoch_ = 0;
  // External data commands capture the currently open dataset generation at
  // dispatch. They revalidate after acquiring either ordinary database
  // admission or a self-managed exclusive database cut. A zero token records
  // admission while serving was closed; internal/nested execution is unscoped
  // because its outer command already owns the fence. Its valid bit shares the
  // compact control byte above.
  std::uint64_t serving_generation_ = 0;
  const CommandSpec* spec_ = nullptr;
  // Cluster admission needs the first slot plus at most one different-slot
  // witness: two distinct slots already make the request terminally
  // CROSSSLOT, while every admitted request carries exactly one slot. Keeping
  // only those samples avoids embedding a general vector in every request.
  // For a single-key request, the first sample also stores the owner route;
  // routed_key_argument_ identifies when that value is reusable even while
  // cluster mode is disabled. KeyIndexView already represents key positions
  // as uint16_t, and argument zero is the command name, so zero is an exact
  // no-route sentinel rather than a narrower limit.
  std::array<std::uint16_t, 2> cluster_slot_samples_{};
  std::uint16_t routed_key_argument_ = 0;

  // A route is reusable only for the exact argument from which its slot was
  // computed; this prevents nested and rewritten requests from applying a
  // stale route to another key.
  bool HasRoutedPartitionFor(std::uint16_t argument) const noexcept {
    return argument != 0 && routed_key_argument_ == argument;
  }
  // Requires HasRoutedPartitionFor() for the argument being routed.
  std::uint16_t RoutedPartitionId() const noexcept {
    return cluster_slot_samples_[0];
  }
  void SetRoutedPartition(std::uint16_t slot, std::uint16_t argument) noexcept {
    assert(argument != 0);
    cluster_slot_samples_[0] = slot;
    routed_key_argument_ = argument;
  }
  void ClearRoutedPartition() noexcept { routed_key_argument_ = 0; }
  void ClearClusterSlots() noexcept { cluster_slot_sample_count_ = 0; }
  void AddClusterSlot(std::uint16_t slot) noexcept {
    if (cluster_slot_sample_count_ == 0) {
      cluster_slot_samples_[0] = slot;
      cluster_slot_sample_count_ = 1;
    } else if (cluster_slot_samples_[0] != slot &&
               cluster_slot_sample_count_ == 1) {
      cluster_slot_samples_[1] = slot;
      cluster_slot_sample_count_ = 2;
    }
  }
  std::span<const std::uint16_t> ClusterSlots() const noexcept {
    return {cluster_slot_samples_.data(), cluster_slot_sample_count_};
  }
  // Mutable: an owner-side re-check may re-arm a request after an unrelated
  // control-plane change. The admission is deliberately opaque here: only
  // AuthorityGuard may interpret its topology, session, lease deadline, and
  // generation proof. ExecuteCommandBody takes the request as const, while
  // refreshing this proof does not change the request's logical contents.
  mutable std::shared_ptr<const cluster::AuthorityAdmission>
      cluster_authority_admission_;
  CommandArgsHandle args_;
  std::shared_ptr<ReplicationCommandCapture> replication_capture_;
  std::shared_ptr<BlockingNotificationCapture> blocking_notification_capture_;
  // Non-owning: the dispatch coroutine keeps the cascade alive until every
  // waiter transitively woken by this command has finished its ready attempt.
  BlockingWakeCascade* blocking_wake_cascade_ = nullptr;

  // Copies execution metadata with independently owned arguments. Rewriters
  // can start with an empty handle instead of copying arguments they replace.
  [[nodiscard]] CommandRequest WithArgs(CommandArgsHandle args) const {
    return CommandRequest{
        .kind_ = kind_,
        .db_id_ = db_id_,
        .resp_version_ = resp_version_,
        .replication_origin_ = replication_origin_,
        .connection_tls_ = connection_tls_,
        .cluster_slot_sample_count_ = cluster_slot_sample_count_,
        .write_admission_role_epoch_valid_ = write_admission_role_epoch_valid_,
        .serving_generation_valid_ = serving_generation_valid_,
        .defer_pubsub_delivery_ = defer_pubsub_delivery_,
        .exclusive_db_access_ = exclusive_db_access_,
        .write_admission_role_epoch_ = write_admission_role_epoch_,
        .serving_generation_ = serving_generation_,
        .spec_ = spec_,
        .cluster_slot_samples_ = cluster_slot_samples_,
        .routed_key_argument_ = routed_key_argument_,
        .cluster_authority_admission_ = cluster_authority_admission_,
        .args_ = std::move(args),
        .replication_capture_ = replication_capture_,
        .blocking_notification_capture_ = blocking_notification_capture_,
        .blocking_wake_cascade_ = blocking_wake_cascade_};
  }
  // Explicit deep copy for a nested execution that needs independent arguments.
  [[nodiscard]] CommandRequest Clone() const { return WithArgs(args_.Clone()); }
};

// Requests move through coroutine frames and MULTI vectors; their arguments
// stay at a fixed address behind a single owning pointer. Preserve the original
// two-cache-line budget so unrelated fields cannot silently add a third line.
static_assert(alignof(CommandRequest) == alignof(std::max_align_t));
static_assert(sizeof(CommandRequest) <= 128);

struct ReplicaOfRequest {
  // Empty for REPLICAOF NO ONE; otherwise identifies the requested upstream.
  std::optional<std::string> host_;
  std::uint16_t port_ = 0;
};

// Parses REPLICAOF <host> <port> and REPLICAOF NO ONE without changing role.
// Command dispatch passes the validated request to ReplicationManager.
absl::StatusOr<ReplicaOfRequest> ParseReplicaOfRequest(CommandArgsView args);

// Pulls the next chunk of a streamed reply; an empty chunk ends the stream.
// Lets unbounded replies (KEYS) reach the socket in bounded memory.
using ReplyChunkSource = std::function<Task<absl::StatusOr<std::string>>()>;

// Owns independently encoded fragments until the socket has consumed them.
// Fragments follow CommandReply::encoded_; an optional lazy source follows the
// fragments. Keeping this state indirect leaves ordinary replies compact.
struct ReplyContinuation {
  explicit ReplyContinuation(ReplyChunkSource source)
      : source_(std::move(source)) {}
  explicit ReplyContinuation(std::vector<std::string> fragments)
      : fragments_(std::move(fragments)) {}

  std::vector<std::string> fragments_;
  ReplyChunkSource source_;
};

struct CommandReply {
  // Points into the connection's ReplyBuilder and remains valid until the
  // current socket write completes. DiskValue keeps the specialized
  // direct-from-read-buffer GET path.
  std::string_view encoded_;
  // DiskValue's lease already has an exact empty state; wrapping it in
  // optional duplicates that state and enlarges every command result.
  storage::DiskValue disk_value_;
  // Composite and streamed replies own their fragments/continuation here.
  // The connection sends encoded_ first and retains this state across every
  // partial write. Single-buffer and direct GET replies need no allocation.
  std::unique_ptr<ReplyContinuation> continuation_;
  bool close_connection_ = false;
  bool start_monitoring_ = false;
  ReadLatencyTrace read_trace_;
  SetLatencyTrace set_trace_;
  std::optional<std::uint8_t> selected_db_;
};

// The direct GET result is moved through several coroutine promises. Keep its
// common representation at 72 bytes so adding rare reply state cannot silently
// restore the former larger coroutine frames. Diagnostic builds deliberately
// carry stage timestamps; this size contract applies only with tracing off.
#if !LAVIK_ENABLE_TRACE
static_assert(sizeof(CommandReply) == 72);
#endif

absl::StatusOr<CommandRequest> BuildCommandRequest(RespCommand command,
                                                   std::uint8_t db_id);

struct ConnectionContext;

// Connection-level dispatch: intercepts MULTI/EXEC/DISCARD and queueing;
// everything else falls through to ExecuteCommand. The caller must retain
// `request` until the returned task completes. Redis sessions directly
// co_await dispatch from the frame that owns the request, avoiding a second
// 112-byte CommandRequest in this child coroutine frame.
// Writes into ctx.reply_builder_. The caller must reset it before dispatch
// and consume the reply before resetting or reusing the builder.
Task<CommandReply> DispatchCommand(ConnectionContext& ctx,
                                   CommandRequest& request);

// Revalidates an external data command after it obtains ordinary database
// admission or drains a self-managed exclusive database cut.
// Replication-origin and internally nested requests are intentionally
// unscoped. A mismatch returns a pointer to a static Redis wire error body
// (without RESP framing), while the common success path returns nullptr. The
// pointer representation keeps this check cheap enough for every data command;
// callers must not take ownership of the returned string.
const char* CommandServingGenerationError(
    const CommandRequest& request) noexcept;

// Unregisters every WATCH this connection holds (connection close, UNWATCH,
// DISCARD, and the end of every EXEC).
Task<absl::Status> ReleaseConnectionWatches(ConnectionContext& ctx);

// Bind command routing to the disk engine. Call once before the server starts.
void InitStorage(storage::StorageEngine* engine,
                 ReplicationManager* replication = nullptr);

// Reserves replication capacity before a top-level blocking write attempt
// takes DB/key holds and releases it once all owner hops settle. The callback
// must not wait for data or escape those hops. EXEC/Lua children instead use
// their enclosing publisher admission and must call their attempt directly.
Task<absl::Status> RunReplicationAdmittedAttempt(
    const CommandRequest& request, std::function<Task<absl::Status>()> attempt);

// Builds an owned command for the current worker's replication journal. The
// storage mutation consumes it at the same ordering point that assigns the
// partition mutation sequence. Returns null for replayed commands or while
// the online replication log is inactive. An empty canonical_args vector
// means to journal the original request arguments.
std::optional<storage::ReplicationCommandAppend> PrepareReplicationCommand(
    const CommandRequest& request,
    std::vector<std::string> canonical_args = {});

// Transfers owned deterministic effects into an EXEC/Lua capture, if present.
// Check replication_capture_ before constructing capture-only arguments: this
// function's null check runs after by-value arguments have already been built.
void CaptureReplicationCommand(const CommandRequest& request,
                               std::vector<std::string> canonical_args);
void CaptureReplicationCommand(const CommandRequest& request,
                               std::uint8_t db_id,
                               std::vector<std::string> canonical_args);
// Borrowed canonical literals/views are copied only when a capture is active.
inline void CaptureReplicationCommand(
    const CommandRequest& request,
    std::initializer_list<std::string_view> canonical_args) {
  if (request.replication_capture_ != nullptr)
    CaptureReplicationCommand(request, CopyCommandArgs(canonical_args));
}
inline void CaptureReplicationCommand(
    const CommandRequest& request, std::uint8_t db_id,
    std::initializer_list<std::string_view> canonical_args) {
  if (request.replication_capture_ != nullptr)
    CaptureReplicationCommand(request, db_id, CopyCommandArgs(canonical_args));
}
std::optional<storage::ReplicationCommandAppend> PrepareReplicationCommand(
    const CommandRequest& request,
    std::initializer_list<std::string_view> canonical_args);
void MarkReplicationCommandHandled(const CommandRequest& request);

// Encodes several deterministic write effects as one replication command.
// Standalone multi-key commands use the same envelope as replicated EXEC so
// all effects are replayed atomically by every participant flow.
std::vector<std::string> EncodeReplicationCommandEffects(
    CapturedReplicationCommands commands);

// Reserves one ordered replication marker on every shard participating in a
// standalone cross-key command. Callers must check status() for representation
// limits before scheduling the transaction. The accepted write may exceed the
// soft memory budget; destruction aborts an unresolved marker.
class ReplicationTransactionGuard {
 public:
  using ParticipantsEnteredHook = void (*)(void*) noexcept;

  // Additional participants carry no storage keys and therefore have no
  // transaction entry hook; the caller must enter each marker explicitly.
  ReplicationTransactionGuard(
      const CommandRequest& request, tx::Transaction* transaction,
      std::vector<std::string> canonical_args = {},
      std::vector<unsigned> additional_participants = {});
  ReplicationTransactionGuard(const CommandRequest& request,
                              std::vector<unsigned> participants,
                              std::vector<std::string> canonical_args = {});
  ReplicationTransactionGuard(const ReplicationTransactionGuard&) = delete;
  ReplicationTransactionGuard& operator=(const ReplicationTransactionGuard&) =
      delete;
  ~ReplicationTransactionGuard();

  // Returns false when a participant already made publication impossible.
  bool Commit() noexcept;
  // Move a canonical body into the pending transaction and account its actual
  // storage. Soft memory pressure cannot reject an already admitted write.
  absl::Status TrySetCommandArgs(
      std::vector<std::string> canonical_args) noexcept;
  void SetCommandArgs(std::vector<std::string> canonical_args) noexcept;
  void SetFinalExpirations(
      std::span<const storage::TxShardWrites> shard_writes) noexcept;
  // Runs exactly once, on the worker that enqueues the final participant
  // marker. The hook must be nonblocking and remain alive until the first
  // transaction hop completes.
  void SetParticipantsEnteredHook(ParticipantsEnteredHook hook,
                                  void* context) noexcept;
  void EnterCurrentShard() noexcept;
  bool active() const noexcept { return transaction_ != nullptr; }
  const absl::Status& status() const noexcept { return status_; }

 private:
  void Initialize(const CommandRequest& request,
                  std::vector<unsigned> participants,
                  std::vector<std::string> canonical_args);
  void InvalidatePayload() noexcept;
  void EnterShard(unsigned shard_id) noexcept;
  static void EnterShardHook(void* context, unsigned shard_id);

  std::shared_ptr<storage::ReplicationTransaction> transaction_;
  std::atomic<unsigned> entered_participants_{0};
  ParticipantsEnteredHook participants_entered_hook_ = nullptr;
  void* participants_entered_context_ = nullptr;
  int uncaught_exceptions_ = 0;
  absl::Status status_ = absl::OkStatus();
};

// Static facts INFO reports. Call once before the server starts.
void SetServerInfo(std::string bind_ip, std::uint16_t port,
                   unsigned thread_count, std::string config_file);

// Connection accounting for INFO's Clients section.
void ConnectionOpened() noexcept;
void ConnectionClosed() noexcept;
// CLIENT metadata is worker-local. Replication socket handoff unregisters on
// the accepting worker and registers the same identity on the owning worker.
// context is borrowed on the owning worker until UnregisterClientConnection.
void RegisterClientConnection(std::uint64_t id, int fd, std::string address,
                              bool tls, bool replica = false,
                              std::uint64_t replication_session_id = 0,
                              ConnectionContext* context = nullptr);
// Notifies each socket-owning worker to close all ordinary/PubSub connections
// present when it handles the notification, including recent reconnects.
// Established replication/donor sessions are excluded. Call after closing data
// admission; asynchronous socket cleanup does not replace internal mutation
// drain.
void RetireClientConnections() noexcept;
void SetClientReplicationSession(std::uint64_t id,
                                 std::uint64_t replication_session_id) noexcept;
void SetClientName(std::uint64_t id, std::string name) noexcept;
void SetClientRespVersion(std::uint64_t id, RespVersion version) noexcept;
void SetClientPubSubCounts(std::uint64_t id, std::size_t subscriptions,
                           std::size_t pattern_subscriptions) noexcept;
// Updates CLIENT state. After registering a waiter, setting blocked also
// cancels it if EOF or connection retirement already occurred.
void SetClientBlocked(std::uint64_t id, bool blocked) noexcept;
void UnregisterClientConnection(std::uint64_t id) noexcept;

// Commands marked kCmdMayBlock hold the database gate only while performing
// one concrete attempt. Their potentially unbounded wait must not prevent
// FLUSHDB from draining in-flight database operations.
bool TryBeginCommandDbOperation(std::uint8_t db_id) noexcept;
void EndCommandDbOperation(std::uint8_t db_id) noexcept;
// True unless a client write crossed a replication role transition after its
// publication decision. Replication-origin and read-only requests have no
// captured epoch and therefore remain valid.
bool CommandWriteAdmissionIsCurrent(const CommandRequest& request) noexcept;
bool CloseAllCommandDbGates() noexcept;
void OpenAllCommandDbGates() noexcept;
bool CommandDbOperationsActive() noexcept;

// Full-sync snapshot handoff uses the same sharded gate shape as FLUSHDB:
// commands update only their coordinator worker's counter, while the rare
// snapshot cut closes and scans every worker. These functions are internal to
// command dispatch and ReplicationManager.
bool TryBeginSnapshotTransaction() noexcept;
void EndSnapshotTransaction() noexcept;
bool CloseSnapshotTransactionGate() noexcept;
void OpenSnapshotTransactionGate() noexcept;
bool SnapshotTransactionsActive() noexcept;

// Replication flows send bounded batches, then wait for complete-event ACKs
// before advancing. Keep cross-flow transactions in one global source order
// so overlapping flow subsets cannot form an arrival/ACK cycle on the
// replica. Requests that are proven to touch at most one shard skip this gate
// (see RequestSpansMultipleShards): a single-flow marker never joins a
// cross-flow rendezvous cycle.
bool TryBeginReplicationTransactionOrder() noexcept;
void EndReplicationTransactionOrder() noexcept;

// Conservative admission test for the transaction order gate above: returns
// true (keep taking the gate) unless the command kind carries
// kCmdKeyViewComplete AND every key in this request's DetermineKeys view maps
// to one shard. Unknown commands, unproven key views, arity failures, and
// empty views all answer true; the gate fails safe, never unsafe.
bool RequestSpansMultipleShards(const CommandRequest& request);

// Route `request` to the worker owning its Redis hash-slot partition. Async
// disk operations use SubmitTaskTo and return on the connection's original
// worker.
Task<CommandReply> ExecuteCommand(CommandRequest& request,
                                  ReplyBuilder& reply_builder,
                                  std::uint64_t client_id = 0,
                                  ConnectionContext* connection = nullptr);

// RDB loaders validate complete FUNCTION2 catalogs before applying keys, then
// install the same source set at the dataset cut.
Task<absl::Status> ValidateLuaFunctionCatalog(
    const std::vector<std::string>& library_codes);
Task<absl::Status> ReplaceLuaFunctionCatalog(
    const std::vector<std::string>& library_codes);

// Replays one trusted canonical command from the native replication stream.
// Transaction envelopes rendezvous on every source flow before this primitive
// applies their ordered effects atomically on the replica.
Task<absl::Status> ApplyReplicatedCommand(const ReplicatedCommand& command);

// Replays a FULL mutation after checking which effects are already covered by
// newer/equal per-key snapshot versions. The caller owns the partition's
// BeginReplicaTailCommand/EndReplicaTailCommand context and serializes its
// flow, so all coverage decisions precede every command/TTL effect.
Task<absl::Status> ApplyFullSyncCommand(const ReplicatedCommand& command,
                                        std::uint64_t session_id,
                                        std::uint16_t partition_id,
                                        std::uint64_t partition_sequence);

// Replays Redis's ordinary single-connection replication stream. Redis emits
// raw FLUSH commands and groups MULTI/EXEC commands without Lavik's native
// epoch/envelope metadata, so these need a distinct trusted apply path.
// The single-command caller builds metadata once and retains the request and
// its argument owner until this await completes, including cross-worker work.
Task<absl::Status> ApplyRedisReplicatedCommand(CommandRequest& request);
Task<absl::Status> ApplyRedisReplicatedTransaction(
    std::span<const ReplicatedCommand> commands);

}  // namespace lavik
