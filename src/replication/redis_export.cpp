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

#include "../redis/stream_command.h"
#include "lavik/replication.h"
#include "replication_internal.h"

namespace lavik {
namespace replication_internal {
struct RedisExportAckState {
  std::atomic<bool> done_{false};
  std::atomic<bool> failed_{false};
  std::atomic<std::uint64_t> offset_{0};
};

Task<absl::Status> ConsumeRedisExportAcks(
    TcpStream* stream, std::shared_ptr<RedisExportAckState> state) {
  RedisCommandStream commands(stream);
  absl::Status status;
  while (status.ok()) {
    auto wire = co_await commands.Next();
    if (!wire.ok()) {
      status = wire.status();
      break;
    }
    const auto& args = wire->command_.args_;
    std::uint64_t offset = 0;
    if (args.size() != 3 || !EqualCaseInsensitive(args[0], "REPLCONF") ||
        !EqualCaseInsensitive(args[1], "ACK") ||
        !ParseUnsigned(args[2], &offset)) {
      status = absl::InvalidArgumentError(
          "unexpected command from Redis export replica");
      break;
    }
    state->offset_.store(offset, std::memory_order_release);
  }
  state->failed_.store(true, std::memory_order_release);
  state->done_.store(true, std::memory_order_release);
  (void)::shutdown(stream->NativeFd(), SHUT_RDWR);
  co_return status;
}

// Multiple storage workers encode the point-in-time image concurrently, but
// the Redis wire remains one ordered byte stream. Queue pressure suspends only
// these background scanners; it never holds command admission closed.
class RedisRdbStreamQueue
    : public std::enable_shared_from_this<RedisRdbStreamQueue> {
 public:
  RedisRdbStreamQueue(storage::StorageEngine* storage, std::uint64_t session_id)
      : storage_(storage), session_id_(session_id), producer_(queue_) {}

  void Start() {
    remaining_.store(storage_->worker_count(), std::memory_order_release);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto context = std::make_unique<std::shared_ptr<RedisRdbStreamQueue>>(
          shared_from_this());
      bycorf::PostNotification(
          bycorf::ThisWorker().cross_core_, worker,
          bycorf::RemoteNotification{
              .context_ = context.release(),
              .value_ = worker,
              .run_fn_ =
                  [](void* raw, std::uint64_t worker_id) noexcept {
                    std::unique_ptr<std::shared_ptr<RedisRdbStreamQueue>> queue(
                        static_cast<std::shared_ptr<RedisRdbStreamQueue>*>(
                            raw));
                    (*queue)->SpawnWorker(static_cast<unsigned>(worker_id));
                  },
          });
    }
  }

  bool TryPop(std::string* fragment) {
    if (!queue_.try_dequeue(*fragment)) return false;
    queued_bytes_.fetch_sub(fragment->size(), std::memory_order_acq_rel);
    return true;
  }

  bool done() const noexcept {
    return remaining_.load(std::memory_order_acquire) == 0;
  }
  bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }
  void Abort(absl::Status status) {
    Fail(std::move(status));
    aborted_.store(true, std::memory_order_release);
  }
  absl::Status status() {
    absl::Status status;
    if (failures_.try_dequeue(status)) return status;
    return failed() ? absl::InternalError("Redis RDB stream failed")
                    : absl::OkStatus();
  }

 private:
  static Task<absl::Status> RunOwned(std::shared_ptr<RedisRdbStreamQueue> queue,
                                     unsigned worker_id) {
    // Keep this coroutine frame: it owns queue until ScanWorker completes.
    co_return co_await queue->ScanWorker(worker_id);
  }

  void SpawnWorker(unsigned worker_id) {
    bycorf::ThisWorker().self_->SpawnBackground(
        RunOwned(shared_from_this(), worker_id));
  }

  bool TryBeginEntry(unsigned owner) {
    unsigned available = std::numeric_limits<unsigned>::max();
    return entry_owner_.compare_exchange_strong(
        available, owner, std::memory_order_acquire, std::memory_order_relaxed);
  }

  bool TryPush(std::string* fragment, unsigned owner) {
    assert(entry_owner_.load(std::memory_order_relaxed) == owner);
    if (aborted_.load(std::memory_order_relaxed)) return false;
    const std::size_t bytes = fragment->size();
    std::size_t occupied = queued_bytes_.load(std::memory_order_acquire);
    for (;;) {
      // A single value may exceed the normal bound only as the sole item.
      if ((bytes > kMaximumQueuedBytes && occupied != 0) ||
          (bytes <= kMaximumQueuedBytes &&
           occupied > kMaximumQueuedBytes - bytes)) {
        return false;
      }
      if (queued_bytes_.compare_exchange_weak(occupied, occupied + bytes,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
        break;
      }
    }
    if (aborted_.load(std::memory_order_acquire)) {
      queued_bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
      return false;
    }
    if (!queue_.enqueue(producer_, std::move(*fragment))) {
      queued_bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
      Fail(absl::ResourceExhaustedError(
          "failed to allocate Redis RDB stream queue entry"));
      aborted_.store(true, std::memory_order_release);
      return false;
    }
    return true;
  }

  Task<absl::Status> PushEntrySpan(unsigned owner, std::string_view bytes) {
    while (!bytes.empty()) {
      const auto piece = bytes.substr(0, 1024 * 1024);
      std::string fragment(piece);
      while (!TryPush(&fragment, owner)) {
        if (aborted_.load(std::memory_order_acquire))
          co_return absl::CancelledError("Redis RDB export cancelled");
        auto status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                std::chrono::milliseconds(1));
        if (!status.ok()) co_return status;
      }
      bytes.remove_prefix(piece.size());
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> WriteCollection(unsigned owner,
                                     const storage::RdbSnapshotValue& value) {
    auto encoder = rdb::CollectionFileEncoder::Create(
        value.db_id_, value.key_, value.value_.value_type_,
        value.value_.logical_size_, value.value_.expire_at_ms_);
    if (!encoder.ok()) co_return encoder.status();
    auto drain = [&]() -> Task<absl::Status> {
      while (auto span = encoder->Next()) {
        auto status = co_await PushEntrySpan(owner, *span);
        if (!status.ok()) co_return status;
      }
      co_return absl::OkStatus();
    };
    auto status = co_await drain();
    if (!status.ok()) co_return status;
    std::uint64_t cursor = 0;
    for (;;) {
      if (aborted_.load(std::memory_order_acquire))
        co_return absl::CancelledError("Redis RDB export cancelled");
      auto page = co_await storage_->ReadRdbCollectionPage(
          session_id_, value.collection_token_, cursor);
      if (!page.ok()) co_return page.status();
      status = encoder->StartPage(*page);
      if (!status.ok()) co_return status;
      // The admitted page owns all borrowed strings until network queue
      // backpressure has accepted every span; no whole-object copy is made.
      status = co_await drain();
      if (!status.ok()) co_return status;
      cursor = page->next_cursor_;
      if (page->done_) break;
    }
    status = encoder->Finish();
    if (!status.ok()) co_return status;
    co_return co_await storage_->FinishRdbCollection(session_id_,
                                                     value.collection_token_);
  }

  Task<absl::Status> ScanWorker(unsigned worker_id) {
    absl::Status status;
    storage::RdbSnapshotCursor cursor;
    unsigned reads_since_yield = 0;
    try {
      while (status.ok() && !aborted_.load(std::memory_order_acquire)) {
        auto batch = co_await storage_->ReadRdbSnapshotBatch(
            session_id_, cursor, 1, 8ULL * 1024 * 1024);
        if (!batch.ok()) {
          status = batch.status();
          break;
        }
        cursor = batch->cursor_;
        for (storage::RdbSnapshotValue& value : batch->values_) {
          while (!TryBeginEntry(worker_id)) {
            if (aborted_.load(std::memory_order_acquire)) {
              status = absl::CancelledError("Redis RDB export cancelled");
              break;
            }
            status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                               std::chrono::milliseconds(1));
            if (!status.ok()) break;
          }
          if (!status.ok()) break;
          struct EntryLease {
            std::atomic<unsigned>* owner;
            ~EntryLease() {
              owner->store(std::numeric_limits<unsigned>::max(),
                           std::memory_order_release);
            }
          } lease{&entry_owner_};
          if (value.collection_token_ != 0) {
            status = co_await WriteCollection(worker_id, value);
            if (!status.ok()) break;
            continue;
          }
          auto fragment =
              rdb::EncodeFileEntry(value.db_id_, value.key_, value.value_);
          std::string().swap(value.value_.encoded_);
          std::string().swap(value.key_);
          if (!fragment.ok()) {
            status = fragment.status();
            break;
          }
          status = co_await PushEntrySpan(worker_id, *fragment);
          if (!status.ok() || aborted_.load(std::memory_order_acquire)) break;
        }
        if (!status.ok() || batch->done_) break;
        if (++reads_since_yield == 64) {
          reads_since_yield = 0;
          co_await bycorf::Yield(*bycorf::ThisWorker().self_);
        }
      }
    } catch (const std::bad_alloc&) {
      // Entry leases and admitted pages unwind before cancelling the retained
      // snapshot. Never let a background allocation failure strand its pins.
      RecordMemoryRejection();
      status = absl::ResourceExhaustedError("OOM Redis RDB export");
    }
    absl::Status ended = co_await storage_->EndRdbSnapshot(session_id_);
    if (status.ok() && !aborted_.load(std::memory_order_acquire)) {
      status = std::move(ended);
    }
    if (!status.ok()) Fail(status);
    remaining_.fetch_sub(1, std::memory_order_acq_rel);
    co_return absl::OkStatus();
  }

  void Fail(absl::Status status) {
    if (status.ok()) return;
    bool expected = false;
    if (failed_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
      (void)failures_.enqueue(std::move(status));
    }
  }

  static constexpr std::size_t kMaximumQueuedBytes = 64ULL * 1024 * 1024;
  storage::StorageEngine* storage_;
  std::uint64_t session_id_;
  moodycamel::ConcurrentQueue<std::string> queue_;
  // ConcurrentQueue orders within one producer, not between producers. A
  // whole-key lease serializes this shared token across workers, preserving
  // contiguous entry fragments even when another worker acquires the lease
  // before the consumer has drained the previous key.
  moodycamel::ProducerToken producer_;
  std::atomic<unsigned> entry_owner_{std::numeric_limits<unsigned>::max()};
  std::atomic<std::size_t> queued_bytes_{0};
  moodycamel::ConcurrentQueue<absl::Status> failures_;
  std::atomic<unsigned> remaining_{0};
  std::atomic<bool> failed_{false};
  std::atomic<bool> aborted_{false};
};

struct RedisExportEvent {
  unsigned worker_ = 0;
  storage::ReplicationEventKind kind_ =
      storage::ReplicationEventKind::kMutation;
  storage::ReplicationLogCursor next_{};
  ReplicatedCommand command_;
};

constexpr std::string_view kRedisExportBacklogGapMessage =
    "Redis export cursor fell behind the online-write backlog";

struct RedisExportTransaction {
  std::uint64_t id_ = 0;
  std::vector<unsigned> participants_;
  unsigned payload_flow_ = 0;
  bool has_payload_ = false;
  ReplicatedCommand command_;
};

struct RedisExportBacklogState {
  enum class Phase : std::uint8_t {
    kFill,
    kMutation,
    kTransaction,
    kControl,
    kIdle,
  };
  explicit RedisExportBacklogState(
      std::vector<storage::ReplicationLogCursor> initial)
      : cursors_(std::move(initial)), heads_(cursors_.size()) {}

  RedisExportContext* context_ = nullptr;
  std::vector<storage::ReplicationLogCursor> cursors_;
  std::vector<std::optional<RedisExportEvent>> heads_;
  unsigned next_worker_ = 0;
  std::uint8_t selected_db_ = 0;
  std::chrono::steady_clock::time_point last_write_ =
      std::chrono::steady_clock::now();
  Phase phase_ = Phase::kFill;
  std::optional<std::vector<storage::ReplicationLogCursor>> stop_cursors_;
  bool abort_ = false;
  bool done_ = false;
  absl::Status status_;
};

// The connection owner is the sole writer. During RDB, completed commands
// enter one disk stream; after the handoff the same merger writes to the
// socket. Source workers never allocate export blocks or own the Redis fd.
class RedisExportSink {
 public:
  virtual ~RedisExportSink() = default;
  virtual Task<absl::Status> Write(std::string_view bytes) = 0;
};

class RedisExportDiskSink final : public RedisExportSink {
 public:
  RedisExportDiskSink(storage::StorageEngine* storage, std::uint64_t session_id)
      : storage_(storage), session_id_(session_id) {}
  Task<absl::Status> Write(std::string_view bytes) override {
    co_return co_await storage_->AppendRedisExportDiskBytes(session_id_, bytes);
  }

 private:
  storage::StorageEngine* storage_;
  std::uint64_t session_id_;
};

class RedisExportSocketSink final : public RedisExportSink {
 public:
  explicit RedisExportSocketSink(TcpStream* stream, RedisExportContext* context)
      : stream_(stream), context_(context) {}
  Task<absl::Status> Write(std::string_view bytes) override {
    if (context_->cancelled_.load(std::memory_order_acquire) ||
        (context_->lease_ && !context_->lease_->valid_at(
                                 cluster::LeaseClockNow().time_since_epoch())))
      co_return absl::CancelledError("Redis export authority ended");
    auto status = co_await WriteText(*stream_, bytes);
    if (status.ok()) context_->progress_.offset_ += bytes.size();
    co_return status;
  }

 private:
  TcpStream* stream_;
  RedisExportContext* context_;
};

Task<absl::StatusOr<std::optional<RedisExportEvent>>> ReadLocalRedisExportEvent(
    storage::StorageEngine* storage, unsigned worker,
    storage::ReplicationLogCursor cursor) {
  std::string encoded;
  std::uint64_t lsn = 0;
  storage::ReplicationEventKind kind = storage::ReplicationEventKind::kMutation;
  std::uint32_t next_fragment = 0;
  while (true) {
    auto batch = co_await storage->ReadReplicationLog(
        cursor, storage::kReplicationTransferBytes, 1);
    if (!batch.ok()) {
      // Only an online-write backlog gap is classified as a slow Redis
      // replica. RDB producer pressure is handled separately by suspending the
      // snapshot scanner until the bounded MPSC queue has room.
      if (batch.status().code() == absl::StatusCode::kOutOfRange) {
        co_return absl::ResourceExhaustedError(kRedisExportBacklogGapMessage);
      }
      co_return batch.status();
    }
    if (batch->frames_.empty()) {
      if (!encoded.empty()) {
        co_return absl::InternalError(
            "Redis export observed a truncated replication event");
      }
      co_return std::optional<RedisExportEvent>{};
    }
    const storage::ReplicationLogFrame& frame = batch->frames_.front();
    const bool first =
        (frame.header_.flags_ &
         static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kFirst)) != 0;
    const bool last =
        (frame.header_.flags_ &
         static_cast<std::uint8_t>(storage::ReplicationFrameFlag::kLast)) != 0;
    if (encoded.empty()) {
      if (!first || frame.header_.fragment_index_ != 0) {
        co_return absl::InternalError(
            "Redis export cursor did not start at an event boundary");
      }
      lsn = frame.header_.lsn_;
      kind = frame.header_.kind_;
    }
    if (frame.header_.lsn_ != lsn ||
        frame.header_.fragment_index_ != next_fragment ||
        frame.header_.kind_ != kind || (next_fragment != 0 && first)) {
      co_return absl::InternalError(
          "Redis export replication fragments are out of order");
    }
    absl::Status appended = AppendReplicationString(&encoded, frame.payload_);
    if (!appended.ok()) co_return appended;
    cursor = batch->next_;
    ++next_fragment;
    if (!last) continue;
    auto command = DecodeReplicationCommand(encoded);
    if (!command.ok()) co_return command.status();
    co_return std::optional<RedisExportEvent>(RedisExportEvent{
        .worker_ = worker,
        .kind_ = kind,
        .next_ = cursor,
        .command_ = std::move(*command),
    });
  }
}

absl::StatusOr<RedisExportTransaction> ParseRedisExportTransaction(
    const RedisExportEvent& event) {
  const auto& args = event.command_.args_;
  if (args.empty() || !IsReplicationTransactionEnvelope(args[0])) {
    return absl::InvalidArgumentError(
        "malformed Redis export transaction envelope");
  }
  auto metadata = DecodeReplicationTransactionEnvelope(args[0]);
  if (!metadata.ok()) return metadata.status();
  RedisExportTransaction transaction;
  transaction.id_ = metadata->id_;
  transaction.command_.db_id_ = event.command_.db_id_;
  transaction.payload_flow_ = metadata->payload_flow_;
  transaction.participants_ = std::move(metadata->participants_);
  transaction.has_payload_ = args.size() > 1;
  const bool event_is_payload = event.worker_ == transaction.payload_flow_;
  if (event_is_payload != transaction.has_payload_) {
    return absl::InvalidArgumentError(
        "Redis export transaction payload arrived on the wrong flow");
  }
  transaction.command_.args_.assign(args.begin() + 1, args.end());
  return transaction;
}

absl::StatusOr<std::string> EncodeRedisExportCommand(
    const ReplicatedCommand& command, bool transactional,
    std::uint8_t* selected_db) {
  if (command.args_.empty()) {
    return absl::InvalidArgumentError("empty Redis export command");
  }
  struct Child {
    std::uint8_t db_ = 0;
    std::vector<std::string> args_;
  };
  std::vector<Child> children;
  if (command.args_[0] == kReplicatedExecCommand) {
    if (command.args_.size() < 2) {
      return absl::InvalidArgumentError("truncated replicated EXEC");
    }
    unsigned count = 0;
    if (!ParseUnsigned(command.args_[1], &count) || count == 0) {
      return absl::InvalidArgumentError("invalid replicated EXEC count");
    }
    std::size_t position = 2;
    children.reserve(count);
    for (unsigned index = 0; index < count; ++index) {
      unsigned db = 0;
      unsigned argc = 0;
      if (position + 2 > command.args_.size() ||
          !ParseUnsigned(command.args_[position], &db) ||
          db >= storage::kLogicalDatabaseCount ||
          !ParseUnsigned(command.args_[position + 1], &argc) || argc == 0 ||
          position + 2 + argc > command.args_.size()) {
        return absl::InvalidArgumentError("malformed replicated EXEC child");
      }
      position += 2;
      Child child{.db_ = static_cast<std::uint8_t>(db), .args_ = {}};
      child.args_.assign(command.args_.begin() + position,
                         command.args_.begin() + position + argc);
      position += argc;
      children.push_back(std::move(child));
    }
    if (position != command.args_.size()) {
      return absl::InvalidArgumentError("trailing replicated EXEC arguments");
    }
    transactional = true;
  } else {
    children.push_back(Child{.db_ = command.db_id_, .args_ = command.args_});
  }

  std::vector<Child> expanded;
  for (const auto& child : children) {
    auto commands = RedisExportStreamGroup(child.args_);
    if (!commands.ok()) return commands.status();
    if (commands->size() > 1) transactional = true;
    for (auto& args : *commands)
      expanded.push_back({.db_ = child.db_, .args_ = std::move(args)});
  }
  children = std::move(expanded);

  // Redis does not know Lavik extensions. Export a successful whole-Hash
  // replacement as DEL + HSET inside the enclosing atomic envelope. Source
  // append/transaction settlement already supplies the final absolute expiry
  // effect, so this neither reads old fields nor extends the key's lifetime.
  for (const auto& child : children) {
    if (!child.args_.empty() &&
        EqualCaseInsensitive(child.args_[0], "LAVIK.HREPLACE")) {
      if (child.args_.size() < 4 || child.args_.size() % 2 != 0)
        return absl::InvalidArgumentError("malformed Hash replacement export");
      transactional = true;
    }
  }

  std::string output;
  std::uint8_t current_db = *selected_db;
  if (current_db != children.front().db_) {
    output += EncodeRespCommand(std::vector<std::string>{
        "SELECT", std::to_string(children.front().db_)});
    current_db = children.front().db_;
  }
  if (transactional) {
    output += EncodeRespCommand(std::vector<std::string>{"MULTI"});
  }
  for (Child& child : children) {
    if (current_db != child.db_) {
      output += EncodeRespCommand(
          std::vector<std::string>{"SELECT", std::to_string(child.db_)});
      current_db = child.db_;
    }
    if (!child.args_.empty() &&
        EqualCaseInsensitive(child.args_[0], "LAVIK.HREPLACE")) {
      output +=
          EncodeRespCommand(std::vector<std::string>{"DEL", child.args_[1]});
      child.args_[0] = "HSET";
    }
    output += EncodeRespCommand(child.args_);
  }
  if (transactional) {
    output += EncodeRespCommand(std::vector<std::string>{"EXEC"});
  }
  *selected_db = current_db;
  return output;
}

Task<absl::Status> FillRedisExportHeads(storage::StorageEngine* storage,
                                        RedisExportBacklogState* state) {
  for (unsigned worker = 0; worker < state->heads_.size(); ++worker) {
    if (state->heads_[worker].has_value()) continue;
    if (state->stop_cursors_.has_value() &&
        state->cursors_[worker].lsn_ >= (*state->stop_cursors_)[worker].lsn_) {
      continue;
    }
    auto event = co_await bycorf::SubmitTaskTo(
        worker, [storage, worker, cursor = state->cursors_[worker]] {
          return ReadLocalRedisExportEvent(storage, worker, cursor);
        });
    if (!event.ok()) co_return event.status();
    if (event->has_value()) state->heads_[worker] = std::move(**event);
  }
  // Ready cross-worker barriers take priority over unrelated local mutations;
  // otherwise a continuously busy worker could starve a transaction forever.
  state->phase_ = RedisExportBacklogState::Phase::kTransaction;
  co_return absl::OkStatus();
}

Task<absl::Status> AdvanceRedisExportCursor(
    storage::StorageEngine* storage, bool backpressure, unsigned worker,
    std::uint64_t session_id, storage::ReplicationLogCursor cursor,
    RedisExportContext* context) {
  if (!backpressure) co_return absl::OkStatus();
  co_return co_await bycorf::SubmitTo(
      worker, [storage, session_id, cursor, context] {
        if (context->cancelled_.load(std::memory_order_acquire))
          return absl::CancelledError("Redis export retention cancelled");
        return storage->RetainReplicationLog(session_id, cursor.lsn_);
      });
}

Task<absl::Status> SendRedisExportMutation(RedisExportSink& sink,
                                           storage::StorageEngine* storage,
                                           bool backpressure,
                                           std::uint64_t session_id,
                                           RedisExportBacklogState* state) {
  const unsigned workers = state->heads_.size();
  for (unsigned offset = 0; offset < workers; ++offset) {
    const unsigned worker = (state->next_worker_ + offset) % workers;
    if (!state->heads_[worker].has_value() ||
        (state->heads_[worker]->kind_ !=
             storage::ReplicationEventKind::kMutation &&
         state->heads_[worker]->kind_ !=
             storage::ReplicationEventKind::kEphemeral &&
         state->heads_[worker]->kind_ !=
             storage::ReplicationEventKind::kCatalogMutation)) {
      continue;
    }
    if (!state->heads_[worker]->command_.args_.empty() &&
        IsReplicationTransactionEnvelope(
            state->heads_[worker]->command_.args_[0])) {
      co_return absl::InternalError(
          "transaction envelope was journaled as a mutation");
    }
    auto encoded = EncodeRedisExportCommand(state->heads_[worker]->command_,
                                            false, &state->selected_db_);
    if (!encoded.ok()) co_return encoded.status();
    absl::Status sent = co_await sink.Write(*encoded);
    if (!sent.ok()) co_return sent;
    state->last_write_ = std::chrono::steady_clock::now();
    state->cursors_[worker] = state->heads_[worker]->next_;
    state->heads_[worker].reset();
    state->next_worker_ = (worker + 1) % workers;
    absl::Status advanced = co_await AdvanceRedisExportCursor(
        storage, backpressure, worker, session_id, state->cursors_[worker],
        state->context_);
    if (!advanced.ok()) co_return advanced;
    state->phase_ = RedisExportBacklogState::Phase::kFill;
    co_return absl::OkStatus();
  }
  state->phase_ = RedisExportBacklogState::Phase::kIdle;
  co_return absl::NotFoundError("no Redis export mutation is ready");
}

Task<absl::Status> SendRedisExportTransaction(RedisExportSink& sink,
                                              storage::StorageEngine* storage,
                                              bool backpressure,
                                              std::uint64_t session_id,
                                              RedisExportBacklogState* state) {
  const unsigned workers = state->heads_.size();
  for (unsigned offset = 0; offset < workers; ++offset) {
    const unsigned worker = (state->next_worker_ + offset) % workers;
    if (!state->heads_[worker].has_value() ||
        state->heads_[worker]->kind_ !=
            storage::ReplicationEventKind::kTransaction) {
      continue;
    }
    auto transaction = ParseRedisExportTransaction(*state->heads_[worker]);
    if (!transaction.ok()) co_return transaction.status();
    bool ready = true;
    std::vector<std::uint8_t> included(workers, 0);
    std::optional<ReplicatedCommand> payload;
    for (unsigned participant : transaction->participants_) {
      if (participant >= workers || included[participant]) {
        co_return absl::InvalidArgumentError(
            "invalid Redis export transaction participant set");
      }
      included[participant] = 1;
      if (!state->heads_[participant].has_value() ||
          state->heads_[participant]->kind_ !=
              storage::ReplicationEventKind::kTransaction) {
        ready = false;
        continue;
      }
      auto peer = ParseRedisExportTransaction(*state->heads_[participant]);
      if (!peer.ok()) co_return peer.status();
      if (peer->id_ != transaction->id_) {
        ready = false;
        continue;
      }
      if (peer->participants_ != transaction->participants_ ||
          peer->payload_flow_ != transaction->payload_flow_ ||
          peer->command_.db_id_ != transaction->command_.db_id_) {
        co_return absl::InvalidArgumentError(
            "conflicting Redis export transaction envelope");
      }
      if (peer->has_payload_) {
        if (payload.has_value()) {
          co_return absl::InvalidArgumentError(
              "duplicate Redis export transaction payload");
        }
        payload = peer->command_;
      }
    }
    if (!included[worker]) {
      co_return absl::InvalidArgumentError(
          "transaction does not name its source worker");
    }
    if (!ready) continue;
    if (!payload.has_value()) {
      co_return absl::InvalidArgumentError(
          "Redis export transaction payload is missing");
    }
    auto encoded =
        EncodeRedisExportCommand(*payload, true, &state->selected_db_);
    if (!encoded.ok()) co_return encoded.status();
    absl::Status sent = co_await sink.Write(*encoded);
    if (!sent.ok()) co_return sent;
    state->last_write_ = std::chrono::steady_clock::now();
    for (unsigned participant : transaction->participants_) {
      state->cursors_[participant] = state->heads_[participant]->next_;
      state->heads_[participant].reset();
      absl::Status advanced = co_await AdvanceRedisExportCursor(
          storage, backpressure, participant, session_id,
          state->cursors_[participant], state->context_);
      if (!advanced.ok()) co_return advanced;
    }
    state->next_worker_ = (worker + 1) % workers;
    state->phase_ = RedisExportBacklogState::Phase::kFill;
    co_return absl::OkStatus();
  }
  state->phase_ = RedisExportBacklogState::Phase::kControl;
  co_return absl::NotFoundError("no Redis export transaction is ready");
}

Task<absl::Status> SendRedisExportControl(RedisExportSink& sink,
                                          storage::StorageEngine* storage,
                                          bool backpressure,
                                          std::uint64_t session_id,
                                          RedisExportBacklogState* state) {
  const unsigned workers = state->heads_.size();
  for (unsigned worker = 0; worker < workers; ++worker) {
    if (!state->heads_[worker].has_value() ||
        state->heads_[worker]->kind_ !=
            storage::ReplicationEventKind::kControl) {
      continue;
    }
    const auto& args = state->heads_[worker]->command_.args_;
    if (args.size() < 2 || (args[0] != "FLUSHDB" && args[0] != "FLUSHALL")) {
      co_return absl::InvalidArgumentError(
          "unsupported Redis export control event");
    }
    bool ready = true;
    for (unsigned peer = 0; peer < workers; ++peer) {
      if (!state->heads_[peer].has_value() ||
          state->heads_[peer]->kind_ !=
              storage::ReplicationEventKind::kControl ||
          state->heads_[peer]->command_.args_.size() < 2 ||
          state->heads_[peer]->command_.args_[0] != args[0] ||
          state->heads_[peer]->command_.args_[1] != args[1]) {
        ready = false;
        break;
      }
    }
    if (!ready) continue;
    ReplicatedCommand control{.db_id_ = state->heads_[worker]->command_.db_id_,
                              .args_ = {args[0]}};
    auto encoded =
        EncodeRedisExportCommand(control, false, &state->selected_db_);
    if (!encoded.ok()) co_return encoded.status();
    absl::Status sent = co_await sink.Write(*encoded);
    if (!sent.ok()) co_return sent;
    state->last_write_ = std::chrono::steady_clock::now();
    for (unsigned peer = 0; peer < workers; ++peer) {
      state->cursors_[peer] = state->heads_[peer]->next_;
      state->heads_[peer].reset();
      absl::Status advanced = co_await AdvanceRedisExportCursor(
          storage, backpressure, peer, session_id, state->cursors_[peer],
          state->context_);
      if (!advanced.ok()) co_return advanced;
    }
    state->phase_ = RedisExportBacklogState::Phase::kFill;
    co_return absl::OkStatus();
  }
  state->phase_ = RedisExportBacklogState::Phase::kMutation;
  co_return absl::NotFoundError("no Redis export control is ready");
}

Task<absl::Status> PingRedisExportBacklog(RedisExportSink& sink,
                                          RedisExportBacklogState* state) {
  absl::Status status = co_await sink.Write("*1\r\n$4\r\nPING\r\n");
  if (status.ok()) state->last_write_ = std::chrono::steady_clock::now();
  if (status.ok()) state->phase_ = RedisExportBacklogState::Phase::kFill;
  co_return status;
}

Task<absl::Status> SleepRedisExportBacklog(RedisExportBacklogState* state) {
  absl::Status status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                  std::chrono::milliseconds(1));
  if (status.ok()) state->phase_ = RedisExportBacklogState::Phase::kFill;
  co_return status;
}

Task<absl::Status> IdleRedisExportBacklog(RedisExportSink& sink,
                                          bool allow_ping,
                                          RedisExportBacklogState* state) {
  if (allow_ping && std::chrono::steady_clock::now() - state->last_write_ >=
                        std::chrono::seconds(1)) {
    return PingRedisExportBacklog(sink, state);
  }
  return SleepRedisExportBacklog(state);
}

Task<absl::Status> DispatchRedisExportBacklogPhase(
    RedisExportSink& sink, storage::StorageEngine* storage, bool backpressure,
    std::uint64_t session_id, RedisExportBacklogState* state, bool allow_ping) {
  switch (state->phase_) {
    case RedisExportBacklogState::Phase::kFill:
      return FillRedisExportHeads(storage, state);
    case RedisExportBacklogState::Phase::kMutation:
      return SendRedisExportMutation(sink, storage, backpressure, session_id,
                                     state);
    case RedisExportBacklogState::Phase::kTransaction:
      return SendRedisExportTransaction(sink, storage, backpressure, session_id,
                                        state);
    case RedisExportBacklogState::Phase::kControl:
      return SendRedisExportControl(sink, storage, backpressure, session_id,
                                    state);
    case RedisExportBacklogState::Phase::kIdle:
      return IdleRedisExportBacklog(sink, allow_ping, state);
  }
  return []() -> Task<absl::Status> {
    co_return absl::InternalError("invalid Redis export backlog phase");
  }();
}

Task<absl::Status> RunRedisExportBacklogLoop(TcpStream& stream,
                                             storage::StorageEngine* storage,
                                             bool backpressure,
                                             std::uint64_t session_id,
                                             RedisExportBacklogState* state,
                                             RedisExportContext* context) {
  RedisExportSocketSink sink(&stream, context);
  while (stream.IsOpen() && !context->cancelled_.load()) {
    absl::Status iteration = co_await DispatchRedisExportBacklogPhase(
        sink, storage, backpressure, session_id, state, true);
    context->progress_.sent_next_lsns_.clear();
    for (auto cursor : state->cursors_)
      context->progress_.sent_next_lsns_.push_back(cursor.lsn_);
    if (!iteration.ok() && iteration.code() != absl::StatusCode::kNotFound) {
      co_return iteration;
    }
  }
  co_return absl::UnavailableError("Redis export connection closed");
}

Task<absl::Status> CaptureRedisExportDiskBacklog(
    storage::StorageEngine* storage, std::uint64_t session_id,
    std::shared_ptr<RedisExportBacklogState> state,
    std::shared_ptr<RedisExportContext> context) {
  RedisExportDiskSink sink(storage, session_id);
  while (!state->abort_ && !context->cancelled_.load()) {
    if (state->stop_cursors_.has_value() &&
        std::equal(state->cursors_.begin(), state->cursors_.end(),
                   state->stop_cursors_->begin())) {
      state->status_ = absl::OkStatus();
      state->done_ = true;
      co_return absl::OkStatus();
    }
    absl::Status iteration = co_await DispatchRedisExportBacklogPhase(
        sink, storage, true, session_id, state.get(), false);
    if (!iteration.ok() && iteration.code() != absl::StatusCode::kNotFound) {
      // Disk exhaustion ends only this Redis session. Drop its pins now so a
      // writer at the memory-log limit need not wait for the RDB sender to
      // observe the failed capture and finish snapshot cleanup.
      for (unsigned worker = 0; worker < storage->worker_count(); ++worker) {
        (void)co_await bycorf::SubmitTo(worker, [storage, session_id] {
          storage->ReleaseReplicationLogRetention(session_id);
          return true;
        });
      }
      spdlog::warn("Redis export {} disk capture failed: {}", session_id,
                   iteration.message());
      context->cancelled_.store(true, std::memory_order_release);
      state->status_ = iteration;
      state->done_ = true;
      co_return iteration;
    }
  }
  state->status_ = absl::CancelledError("Redis export disk capture aborted");
  state->done_ = true;
  co_return state->status_;
}

}  // namespace replication_internal
}  // namespace lavik

namespace lavik {
using namespace replication_internal;

auto ReplicationManager::ReplicationGroup::ServeRedisExportConnection(
    TcpStream& stream, std::vector<std::string> args, std::uint64_t client_id,
    std::string client_address, bool tls, bool eof_capable)
    -> Task<absl::Status> {
  if (!source_sockets_.Add(stream.NativeFd())) {
    co_return absl::CancelledError(
        "Redis replication export stopped for process shutdown");
  }
  ScopedSocketSetMembership source_socket(&source_sockets_, stream.NativeFd());
  if (args.size() != 3 || !EqualCaseInsensitive(args[0], "PSYNC")) {
    co_return absl::InvalidArgumentError("invalid Redis PSYNC handshake");
  }
  if (!eof_capable) {
    absl::Status sent = co_await WriteText(
        stream, "-ERR diskless PSYNC requires REPLCONF capa eof\r\n");
    co_return sent.ok() ? absl::FailedPreconditionError(
                              "Redis replica did not advertise EOF capability")
                        : sent;
  }
  if (is_loading()) {
    absl::Status sent = co_await WriteText(
        stream,
        "-LOADING node has no valid Redis replication source state\r\n");
    co_return sent.ok() ? absl::FailedPreconditionError(
                              "a fenced node cannot export Redis PSYNC")
                        : sent;
  }
  if (is_replica()) {
    absl::Status sent = co_await WriteText(
        stream, "-ERR detach this Lavik replica before Redis export\r\n");
    co_return sent.ok() ? absl::FailedPreconditionError(
                              "a Lavik replica cannot export Redis PSYNC")
                        : sent;
  }
  bool expected = false;
  if (!redis_export_active_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    absl::Status sent = co_await WriteText(
        stream, "-ERR only one Redis PSYNC export is supported\r\n");
    co_return sent.ok()
        ? absl::AlreadyExistsError("a Redis PSYNC export is already active")
        : sent;
  }
  redis_export_fd_.store(stream.NativeFd(), std::memory_order_release);
  struct ActiveGuard {
    std::atomic<bool>* active_;
    std::atomic<int>* fd_;
    ~ActiveGuard() {
      fd_->store(-1, std::memory_order_release);
      active_->store(false, std::memory_order_release);
    }
  } active_guard{&redis_export_active_, &redis_export_fd_};
  // CAS-before-check pairs with DrainSourceEgress's active observation. If
  // shutdown won first, this handler exits before any await/history setup;
  // otherwise the barrier sees active=true and joins the whole export.
  if (replication_shutdown_requested_.load(std::memory_order_acquire)) {
    co_return absl::CancelledError(
        "Redis replication export stopped for process shutdown");
  }
  if (is_replica() || is_loading()) {
    absl::Status sent = co_await WriteText(
        stream,
        "-LOADING node has no valid Redis replication source state\r\n");
    co_return sent.ok() ? absl::FailedPreconditionError(
                              "node lost valid source state during PSYNC setup")
                        : sent;
  }
  absl::Status configured = ConfigureConnectedFd(stream.NativeFd());
  if (!configured.ok()) co_return configured;
  const std::uint64_t session_id =
      next_master_session_id_.fetch_add(1, std::memory_order_relaxed);
  SetClientReplicationSession(client_id, session_id);
  RegisterClientConnection(client_id, stream.NativeFd(),
                           std::move(client_address), tls, true, session_id);
  struct ClientGuard {
    std::uint64_t id_;
    ~ClientGuard() { UnregisterClientConnection(id_); }
  } client_guard{client_id};

  auto context = std::make_shared<RedisExportContext>();
  context->worker_ = bycorf::ThisWorker().id_;
  context->id_ = session_id;
  context->disk_capacity_ = redis_export_disk_backlog_size();
  context->progress_.session_id_ = session_id;
  context->progress_.disk_capacity_ = context->disk_capacity_;
  context->progress_.phase_ = "admission";
  redis_export_context_.store(context, std::memory_order_release);
  absl::Status status = co_await PrepareRedisExport(context);
  if (status.ok()) {
    context->admitted_.store(true, std::memory_order_release);
    bycorf::ThisWorker().self_->Spawn(MonitorRedisExport(&stream, context));
    status = co_await RunRedisExportSession(stream, context);
    context->cancelled_.store(true, std::memory_order_release);
    while (!context->monitor_done_) {
      (void)co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                      std::chrono::milliseconds(1));
    }
  }
  if (!context->admitted_.load(std::memory_order_acquire)) {
    (void)co_await WriteText(
        stream,
        "-ERR Redis export requires current serving Primary authority\r\n");
  }
  // Every inner early-return and cancellation releases only this session's
  // claims. Do not retire or reset the shared native history here.
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    (void)co_await bycorf::SubmitTo(worker, [this, session_id] {
      storage_->ReleaseReplicationLogRetention(session_id);
      return true;
    });
  }
  auto released = co_await storage_->ReleaseRedisExportDiskBacklog(session_id);
  if (status.ok() && !released.ok()) status = released;
  redis_export_context_.store(nullptr, std::memory_order_release);
  if (!status.ok())
    spdlog::warn("Redis export {} ended: {}", session_id, status.message());
  co_return status;
}

auto ReplicationManager::ReplicationGroup::RunRedisExportSession(
    TcpStream& stream, std::shared_ptr<RedisExportContext> context)
    -> Task<absl::Status> {
  const auto session_id = context->id_;
  absl::Status status;
  while (!CloseAllCommandDbGates()) {
    if (!RedisExportValid(*context)) {
      co_return absl::CancelledError(
          "source role changed before Redis export snapshot admission");
    }
    status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                       std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }
  bool gates_open = false;
  struct GateGuard {
    bool* open_;
    ~GateGuard() {
      if (!*open_) OpenAllCommandDbGates();
    }
  } gate_guard{&gates_open};
  while (CommandDbOperationsActive()) {
    if (!RedisExportValid(*context))
      co_return absl::CancelledError("Redis export admission cancelled");
    status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                       std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }

  if (!RedisExportValid(*context))
    co_return absl::CancelledError("Redis export admission revoked");
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  const std::uint64_t snapshot_time_ms =
      now > 0 ? static_cast<std::uint64_t>(now) : 1;
  unsigned snapshots_begun = 0;
  for (; snapshots_begun < storage_->worker_count(); ++snapshots_begun) {
    status = co_await bycorf::SubmitTo(
        snapshots_begun, [this, session_id, snapshot_time_ms] {
          return storage_->BeginRdbSnapshot(session_id, snapshot_time_ms);
        });
    if (!status.ok()) break;
  }
  if (!status.ok()) {
    for (unsigned worker = 0; worker < snapshots_begun; ++worker) {
      (void)co_await bycorf::SubmitTaskTo(worker, [this, session_id] {
        return storage_->EndRdbSnapshot(session_id);
      });
    }
    co_return status;
  }

  std::vector<storage::ReplicationLogCursor> cursors(storage_->worker_count());
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    auto fenced = co_await bycorf::SubmitTaskTo(worker, [this, context]() {
      return storage_->FenceReplicationLog(
          [context] { return context->cancelled_.load(); });
    });
    if (!fenced.ok()) {
      status = fenced.status();
      break;
    }
    cursors[worker] =
        storage::ReplicationLogCursor{.lsn_ = *fenced, .fragment_index_ = 0};
    {
      status = co_await bycorf::SubmitTo(
          worker, [this, session_id, cursor = cursors[worker], context] {
            if (context->cancelled_.load(std::memory_order_acquire))
              return absl::CancelledError("Redis export retention cancelled");
            return storage_->RetainReplicationLog(session_id, cursor.lsn_);
          });
      if (!status.ok()) break;
    }
  }
  if (!status.ok()) {
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      (void)co_await bycorf::SubmitTaskTo(worker, [this, session_id] {
        return storage_->EndRdbSnapshot(session_id);
      });
      (void)co_await bycorf::SubmitTo(worker, [this, session_id] {
        storage_->ReleaseReplicationLogRetention(session_id);
        return true;
      });
    }
    co_return status;
  }
  // The connection owner is the only worker that allocates export blocks.
  // Its merger drains each source flow into a single Redis command stream
  // while snapshot scanners independently feed the same socket with RDB.
  status = co_await storage_->StartRedisExportDiskBacklog(
      session_id, context->disk_capacity_);
  if (!status.ok()) {
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      (void)co_await bycorf::SubmitTaskTo(worker, [this, session_id] {
        return storage_->EndRdbSnapshot(session_id);
      });
      (void)co_await bycorf::SubmitTo(worker, [this, session_id] {
        storage_->ReleaseReplicationLogRetention(session_id);
        return true;
      });
    }
    co_return status;
  }
  auto capture = std::make_shared<RedisExportBacklogState>(cursors);
  capture->context_ = context.get();
  bycorf::ThisWorker().self_->Spawn(
      CaptureRedisExportDiskBacklog(storage_, session_id, capture, context));
  const std::vector<LuaFunctionLibrary> function_libraries =
      SnapshotLuaFunctionLibraries();
  OpenAllCommandDbGates();
  gates_open = true;

  const std::string eof_token = NewReplicationId();
  auto rdb_queue = std::make_shared<RedisRdbStreamQueue>(storage_, session_id);
  context->progress_.phase_ = "rdb";
  context->abort_snapshot_ = [rdb_queue] {
    rdb_queue->Abort(absl::CancelledError("Redis export cancelled"));
  };
  // Start all storage workers even if the socket fails immediately: every
  // producer owns the matching EndRdbSnapshot cleanup.
  rdb_queue->Start();
  spdlog::info("Redis PSYNC export {} began RDB and disk capture", session_id);
  const std::string full_resync_header = absl::StrCat(
      "+FULLRESYNC ", context->history_, " 1\r\n$EOF:", eof_token, "\r\n");
  status = co_await WriteText(stream, full_resync_header);
  if (status.ok()) {
    // RDB v10 is accepted by Redis 7.0 and later. Lavik's value opcodes
    // do not require the v11 metadata additions used by backup files.
    rdb::StreamEncoder encoder(10);
    status = co_await WriteText(stream, encoder.Header());
    if (status.ok()) {
      for (const LuaFunctionLibrary& library : function_libraries) {
        const std::string fragment =
            rdb::EncodeFunctionLibraryEntry(library.code_);
        status = co_await WriteText(stream, fragment);
        if (!status.ok()) break;
        encoder.Account(fragment);
      }
    }
    if (status.ok()) {
      bool empty_race_injected = false;
      while (status.ok()) {
        if (!RedisExportValid(*context)) {
          status = absl::CancelledError("Redis export revoked");
          break;
        }
        if (capture->done_ && !capture->status_.ok()) {
          status = capture->status_;
          break;
        }
        std::string fragment;
        bool observe_empty = false;
        LAVIK_FAULT_INJECT(
            if (!empty_race_injected &&
                LAVIK_FAULT_MATCHES("LAVIK_REDIS_EXPORT_FINAL_QUEUE_RACE",
                                    "1")) {
              empty_race_injected = true;
              observe_empty = true;
              // Model an empty dequeue immediately before the final producer
              // publishes. Tiny fixtures fit the queue while we await done.
              while (!rdb_queue->done()) {
                (void)co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                std::chrono::milliseconds(1));
              }
            });
        (void)empty_race_injected;
        if (!observe_empty && rdb_queue->TryPop(&fragment)) {
          status = co_await WriteText(stream, fragment);
          if (status.ok()) encoder.Account(fragment);
          continue;
        }
        if (rdb_queue->done()) {
          // Completion is acquired after the first empty dequeue: a final
          // producer may have published a fragment between those observations.
          if (rdb_queue->TryPop(&fragment)) {
            status = co_await WriteText(stream, fragment);
            if (status.ok()) encoder.Account(fragment);
            continue;
          }
          break;
        }
        if (rdb_queue->failed()) {
          status = rdb_queue->status();
          break;
        }
        status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                           std::chrono::milliseconds(1));
      }
      if (status.ok() && rdb_queue->failed()) status = rdb_queue->status();
      if (status.ok()) {
        const std::string trailer = encoder.Finish();
        status = co_await WriteText(stream, trailer);
      }
      if (status.ok()) status = co_await WriteText(stream, eof_token);
    }
  }
  if (!status.ok() && !rdb_queue->done()) rdb_queue->Abort(status);
  while (!rdb_queue->done()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) break;
  }
  if (status.ok()) {
    // A brief second gate/fence establishes an exact handoff. Commands
    // before it finish the disk stream; later commands remain in the normal
    // memory backlog and are backpressured until disk replay catches up.
    while (!CloseAllCommandDbGates()) {
      if (!RedisExportValid(*context)) {
        status = absl::CancelledError("Redis export cut cancelled");
        break;
      }
      status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                         std::chrono::milliseconds(1));
      if (!status.ok()) break;
    }
    if (status.ok()) {
      gates_open = false;
      while (CommandDbOperationsActive()) {
        if (!RedisExportValid(*context)) {
          status = absl::CancelledError("Redis export cut cancelled");
          break;
        }
        status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                           std::chrono::milliseconds(1));
        if (!status.ok()) break;
      }
    }
    if (status.ok()) {
      std::vector<storage::ReplicationLogCursor> end_cursors(
          storage_->worker_count());
      for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
        auto fenced = co_await bycorf::SubmitTaskTo(worker, [this, context] {
          return storage_->FenceReplicationLog(
              [context] { return context->cancelled_.load(); });
        });
        if (!fenced.ok()) {
          status = fenced.status();
          break;
        }
        end_cursors[worker] = {.lsn_ = *fenced, .fragment_index_ = 0};
      }
      if (status.ok()) {
        capture->stop_cursors_ = end_cursors;
        cursors = std::move(end_cursors);
      }
    }
    if (!gates_open) {
      OpenAllCommandDbGates();
      gates_open = true;
    }
  }
  if (!status.ok()) capture->abort_ = true;
  while (!capture->done_) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      capture->abort_ = true;
    }
  }
  if (status.ok()) status = capture->status_;
  std::shared_ptr<RedisExportAckState> ack_state;
  if (status.ok()) {
    ack_state = std::make_shared<RedisExportAckState>();
    bycorf::ThisWorker().self_->Spawn(
        ConsumeRedisExportAcks(&stream, ack_state));
  }
  if (status.ok()) {
    context->progress_.phase_ = "waiting-ack";
    while (ack_state->offset_.load(std::memory_order_acquire) < 1) {
      if (!RedisExportValid(*context) || ack_state->done_.load()) {
        status = absl::CancelledError("Redis export initial ACK interrupted");
        break;
      }
      status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                         std::chrono::milliseconds(1));
      if (!status.ok()) break;
    }
  }
  if (status.ok()) {
    context->progress_.phase_ = "disk-replay";
    auto stopped = co_await storage_->StopRedisExportDiskBacklog(session_id);
    if (!stopped.ok()) {
      status = stopped.status();
    } else {
      storage::ReplicationLogCursor disk_cursor{.lsn_ = 1,
                                                .fragment_index_ = 0};
      while (status.ok() && disk_cursor.lsn_ < *stopped) {
        auto batch = co_await storage_->ReadRedisExportDiskBacklog(
            session_id, disk_cursor, storage::kReplicationTransferBytes, 64);
        if (!batch.ok()) {
          status = batch.status();
          break;
        }
        if (batch->frames_.empty()) {
          status = absl::DataLossError("Redis export disk stream ended early");
          break;
        }
        for (const auto& frame : batch->frames_) {
          RedisExportSocketSink sink(&stream, context.get());
          status = co_await sink.Write(frame.payload_);
          if (!status.ok()) break;
        }
        disk_cursor = batch->next_;
      }
    }
  }
  absl::Status released_disk =
      co_await storage_->ReleaseRedisExportDiskBacklog(session_id);
  if (status.ok() && !released_disk.ok()) status = released_disk;
  if (status.ok()) {
    spdlog::info("Redis PSYNC export {} completed diskless RDB cut",
                 session_id);
    auto backlog_state =
        std::make_shared<RedisExportBacklogState>(std::move(cursors));
    backlog_state->context_ = context.get();
    backlog_state->selected_db_ = capture->selected_db_;
    context->progress_.phase_ = "online";
    for (auto cursor : backlog_state->cursors_)
      context->progress_.sent_next_lsns_.push_back(cursor.lsn_);
    status = co_await RunRedisExportBacklogLoop(
        stream, storage_, true, session_id, backlog_state.get(), context.get());
  }
  if (ack_state != nullptr &&
      !ack_state->done_.load(std::memory_order_acquire)) {
    (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
    while (!ack_state->done_.load(std::memory_order_acquire)) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) break;
    }
  }
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    (void)co_await bycorf::SubmitTo(worker, [this, session_id] {
      storage_->ReleaseReplicationLogRetention(session_id);
      return true;
    });
  }
  if (status.code() == absl::StatusCode::kResourceExhausted &&
      status.message() == kRedisExportBacklogGapMessage) {
    spdlog::warn(
        "Redis PSYNC export {} failed: Redis replica fell behind online "
        "writes; disconnecting and requiring a new full sync",
        session_id);
  } else if (!status.ok()) {
    spdlog::warn("Redis PSYNC export {} ended: {}", session_id,
                 status.message());
  }
  co_return status;
}

auto ReplicationManager::ReplicationGroup::PrepareRedisExport(
    std::shared_ptr<RedisExportContext> context) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, context] { return PrepareRedisExport(context); });
  }
  if (is_replica() || is_loading() || replication_shutdown_requested_.load())
    co_return absl::FailedPreconditionError(
        "Redis export requires a servable Primary");
  if (meta_managed_) {
    if (cluster_control_stopping_ || cluster_source_revocations_in_flight_ ||
        !native_dataset_valid_.load() || !cluster_rebuild_ ||
        cluster_rebuild_->state_.load() != ReplicationGroupState::kReady ||
        !cluster_rebuild_->ready_token_ ||
        !source_authorizations_.LeaseAdmissionOpen(
            cluster::LeaseClockNow().time_since_epoch()))
      co_return absl::FailedPreconditionError(
          "Redis export requires current Owner authority");
    context->lease_ = source_authorizations_.lease();
    context->term_ = owner_source_term_;
    context->population_ = cluster_rebuild_->ready_token_->identity();
    context->progress_.group_id_ = context->population_->group_id_;
    context->progress_.term_ = context->term_;
    context->progress_.node_id_ = node_id_;
    context->progress_.boot_id_ = boot_id_;
  } else {
    // Standalone has no population-owned publisher until its first consumer.
    // Use the common history initialization, never a Redis-only history reset.
    auto ready = co_await EnsureReplicationHistoryReady();
    if (!ready.ok()) co_return ready;
    StartIdleReplicationHistoryMonitor();
  }
  context->history_ = history_id_;
  context->progress_.history_id_ = history_id_;
  context->generation_ = serving_generation_->load(std::memory_order_acquire);
  context->role_epoch_ = role_epoch_.load(std::memory_order_acquire);
  context->progress_.generation_ = context->generation_;
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    auto info = co_await bycorf::SubmitTo(
        worker, [this] { return storage_->LocalReplicationLogInfo(); });
    if (meta_managed_) {
      if (info.state_ != storage::ReplicationLogState::kActive)
        co_return absl::FailedPreconditionError(
            "Owner replication history is not active");
    } else {
      auto enabled = co_await bycorf::SubmitTaskTo(worker, [this, worker,
                                                            context] {
        return storage_->EnableReplicationLog(
            context->id_, BacklogCapacityForFlow(worker, backlog_size_bytes()));
      });
      if (!enabled.ok()) co_return enabled;
    }
  }
  if (!RedisExportValid(*context))
    co_return absl::CancelledError(
        "Redis export authority changed during admission");
  co_return absl::OkStatus();
}

bool ReplicationManager::ReplicationGroup::RedisExportValid(
    const RedisExportContext& context) const {
  if (context.cancelled_.load(std::memory_order_acquire) ||
      replication_shutdown_requested_.load(std::memory_order_acquire) ||
      is_replica() || is_loading() ||
      context.role_epoch_ != role_epoch_.load(std::memory_order_acquire) ||
      context.generation_ !=
          serving_generation_->load(std::memory_order_acquire))
    return false;
  if (meta_managed_ &&
      (!context.lease_ ||
       !context.lease_->valid_at(cluster::LeaseClockNow().time_since_epoch())))
    return false;
  const auto current = published_heartbeat_.load(std::memory_order_acquire);
  return !meta_managed_ ||
         (current && current->observation_.identity_.local_history_id_ ==
                         context.history_);
}

void ReplicationManager::ReplicationGroup::CancelRedisExport() noexcept {
  if (auto context = redis_export_context_.load(std::memory_order_acquire))
    context->cancelled_.store(true, std::memory_order_release);
}

auto ReplicationManager::ReplicationGroup::MonitorRedisExport(
    TcpStream* stream, std::shared_ptr<RedisExportContext> context)
    -> Task<absl::Status> {
  while (RedisExportValid(*context)) {
    {
      const bool current = co_await bycorf::SubmitTo(0, [this, context] {
        if (history_id_ != context->history_) return false;
        if (!meta_managed_) return true;
        return !cluster_control_stopping_ &&
               owner_source_term_ == context->term_ && cluster_rebuild_ &&
               cluster_rebuild_->ready_token_ &&
               cluster_rebuild_->ready_token_->identity() ==
                   *context->population_;
      });
      if (!current) {
        spdlog::info("Redis export {} population binding changed",
                     context->id_);
        break;
      }
    }
    if (meta_managed_) {
      const bool paused = co_await bycorf::SubmitTo(
          0, [this] { return cluster_source_pause_ != nullptr; });
      // Publication can reach capacity after request drain, while the pause
      // watermark is fencing asynchronous publishers. Relieve that boundary
      // as well as NodeControl's earlier admitted-request drain.
      if (paused) (void)co_await RelieveRedisExportBackpressure();
    }
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(5));
    if (!waited.ok()) break;
  }
  spdlog::info(
      "Redis export {} revoked: cancelled={} role_epoch={}/{} generation={}/{} "
      "lease_valid={}",
      context->id_, context->cancelled_.load(), context->role_epoch_,
      role_epoch_.load(), context->generation_, serving_generation_->load(),
      !context->lease_ || context->lease_->valid_at(
                              cluster::LeaseClockNow().time_since_epoch()));
  context->cancelled_.store(true, std::memory_order_release);
  (void)::shutdown(stream->NativeFd(), SHUT_RDWR);
  if (context->abort_snapshot_) context->abort_snapshot_();
  // Release pins before joining work that may itself await a blocked publisher.
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    (void)co_await bycorf::SubmitTo(worker, [this, context] {
      storage_->ReleaseReplicationLogRetention(context->id_);
      return true;
    });
  }
  context->monitor_done_ = true;
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RedisExportStatus() const
    -> Task<RedisExportProgress> {
  auto context = redis_export_context_.load(std::memory_order_acquire);
  if (!context || !context->admitted_.load(std::memory_order_acquire) ||
      !RedisExportValid(*context))
    co_return RedisExportProgress{};
  std::vector<std::uint64_t> source;
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    auto fence = co_await bycorf::SubmitTaskTo(worker, [this, context] {
      return storage_->FenceReplicationLog(
          [context] { return context->cancelled_.load(); });
    });
    if (!fence.ok()) co_return RedisExportProgress{};
    source.push_back(*fence);
  }
  auto result = co_await bycorf::SubmitTo(
      context->worker_, [context] { return context->progress_; });
  if (!RedisExportValid(*context) || redis_export_context_.load() != context)
    co_return RedisExportProgress{};
  result.source_next_lsns_ = std::move(source);
  co_return result;
}

auto ReplicationManager::ReplicationGroup::RelieveRedisExportBackpressure()
    -> Task<absl::Status> {
  auto context = redis_export_context_.load(std::memory_order_acquire);
  if (!context) co_return absl::OkStatus();
  bool blocked = false;
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    blocked |= co_await bycorf::SubmitTo(worker, [this] {
      return storage_->LocalReplicationLogInfo().capacity_backpressured_;
    });
  }
  if (blocked) {
    context->cancelled_.store(true, std::memory_order_release);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      (void)co_await bycorf::SubmitTo(worker, [this, context] {
        storage_->ReleaseReplicationLogRetention(context->id_);
        return true;
      });
    }
  }
  co_return absl::OkStatus();
}

bycorf::Task<RedisExportProgress> ReplicationManager::ObserveRedisExport()
    const {
  return group_->RedisExportStatus();
}

bycorf::Task<absl::Status>
ReplicationManager::RelieveRedisExportBackpressure() {
  return group_->RelieveRedisExportBackpressure();
}

}  // namespace lavik
