#include "list_command.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "keylane/redis_parse.h"
#include "keylane/resp.h"
#include "keylane/storage/engine.h"
#include "keylane/tx/transaction.h"
#include "keylane/tx/tx_shard.h"

namespace keylane {
using namespace celer;

namespace {

storage::StorageEngine* g_storage = nullptr;

struct BlockingKey {
  std::uint8_t db_id_ = 0;
  std::string key_;
  std::string lane_;

  bool operator==(const BlockingKey&) const = default;
};

struct BlockingKeyView {
  std::uint8_t db_id_ = 0;
  std::string_view key_;
  std::string_view lane_;
};

bool operator==(const BlockingKey& left, const BlockingKeyView& right) {
  return left.db_id_ == right.db_id_ && left.key_ == right.key_ &&
         left.lane_ == right.lane_;
}

bool operator==(const BlockingKeyView& left, const BlockingKey& right) {
  return right == left;
}

template <typename H>
H AbslHashValue(H hash, const BlockingKey& value) {
  return H::combine(std::move(hash), value.db_id_, value.key_, value.lane_);
}

template <typename H>
H AbslHashValue(H hash, const BlockingKeyView& value) {
  return H::combine(std::move(hash), value.db_id_, value.key_, value.lane_);
}

struct BlockingKeyHash {
  using is_transparent = void;

  std::size_t operator()(const BlockingKey& value) const {
    return absl::HashOf(value.db_id_, value.key_, value.lane_);
  }
  std::size_t operator()(const BlockingKeyView& value) const {
    return absl::HashOf(value.db_id_, value.key_, value.lane_);
  }
};

struct BlockingKeyEqual {
  using is_transparent = void;

  bool operator()(const BlockingKey& left, const BlockingKey& right) const {
    return left == right;
  }
  bool operator()(const BlockingKey& left, const BlockingKeyView& right) const {
    return left == right;
  }
  bool operator()(const BlockingKeyView& left, const BlockingKey& right) const {
    return left == right;
  }
};

struct WaitRegistration {
  unsigned owner_ = 0;
  BlockingKey key_;
  BlockingQueuePolicy policy_ = BlockingQueuePolicy::kFifo;
  std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_after_;
};

using WakeReason = BlockingWakeReason;

// A waiter is simultaneously referenced by the command worker, its timer,
// and worker-local registries on every key owner. Those references can cross
// cores, so this is one of the cases where atomic shared_ptr ownership is
// required rather than worker-local ownership.
class BlockingWaiter : public std::enable_shared_from_this<BlockingWaiter> {
 public:
  BlockingWaiter(celer::Worker* worker, std::uint64_t ticket)
      : worker_(worker), ticket_(ticket) {}

  class Awaiter {
   public:
    explicit Awaiter(std::shared_ptr<BlockingWaiter> waiter)
        : waiter_(std::move(waiter)) {}
    Awaiter(const Awaiter&) = delete;
    Awaiter& operator=(const Awaiter&) = delete;
    ~Awaiter() {
      if (handle_ && !resumed_) waiter_->Cancel();
    }

    bool await_ready() const noexcept {
      return waiter_->reason() != WakeReason::kWaiting;
    }

    bool await_suspend(std::coroutine_handle<> handle) {
      handle_ = handle;
      return waiter_->Suspend(handle);
    }

    WakeReason await_resume() noexcept {
      resumed_ = true;
      return waiter_->reason();
    }

   private:
    std::shared_ptr<BlockingWaiter> waiter_;
    std::coroutine_handle<> handle_{};
    bool resumed_ = false;
  };

  Awaiter Wait() { return Awaiter(shared_from_this()); }

  std::uint64_t ticket() const noexcept { return ticket_; }
  unsigned worker_id() const noexcept { return worker_->id(); }

  WakeReason reason() const noexcept {
    return reason_;
  }

  bool ResetReady() noexcept {
    if (reason_ != WakeReason::kReady) return false;
    reason_ = WakeReason::kWaiting;
    return true;
  }

  bool Signal(WakeReason reason) noexcept {
    if (reason_ != WakeReason::kWaiting) return false;
    reason_ = reason;
    std::coroutine_handle<> handle = std::exchange(handle_, {});
    if (handle) worker_->Enqueue(handle);
    return true;
  }

  void SignalTimeout() noexcept {
    if (reason_ != WakeReason::kWaiting && reason_ != WakeReason::kReady) {
      return;
    }
    // Deadline is terminal even if readiness was already latched. If the
    // woken attempt loses the element to an earlier waiter, it must observe
    // the expired deadline instead of sleeping after its timer has exited.
    reason_ = WakeReason::kTimeout;
    std::coroutine_handle<> handle = std::exchange(handle_, {});
    if (handle) worker_->Enqueue(handle);
  }

  void Cancel() noexcept {
    if (reason_ == WakeReason::kWaiting || reason_ == WakeReason::kReady) {
      reason_ = WakeReason::kCancelled;
    }
    handle_ = {};
  }

 private:
  bool Suspend(std::coroutine_handle<> handle) noexcept {
    if (reason_ != WakeReason::kWaiting) return false;
    handle_ = handle;
    return true;
  }

  celer::Worker* worker_ = nullptr;
  std::uint64_t ticket_ = 0;
  WakeReason reason_ = WakeReason::kWaiting;
  std::coroutine_handle<> handle_{};
};

void RunBlockingReadyNotification(void* context, std::uint64_t) noexcept {
  std::unique_ptr<std::shared_ptr<BlockingWaiter>> waiter(
      static_cast<std::shared_ptr<BlockingWaiter>*>(context));
  (void)(*waiter)->Signal(WakeReason::kReady);
}

void SignalBlockingReady(const std::shared_ptr<BlockingWaiter>& waiter) {
  const celer::CurrentWorker& current = celer::ThisWorker();
  if (waiter->worker_id() == current.id_) {
    (void)waiter->Signal(WakeReason::kReady);
    return;
  }
  auto context = std::make_unique<std::shared_ptr<BlockingWaiter>>(waiter);
  celer::PostNotification(
      current.cross_core_, waiter->worker_id(),
      celer::RemoteNotification{.context_ = context.release(),
                                .value_ = 0,
                                .run_fn_ = &RunBlockingReadyNotification});
}

class BlockingWaitRegistry {
 public:
  void Register(const WaitRegistration& registration,
                const std::shared_ptr<BlockingWaiter>& waiter) {
    auto& state = queues_[registration.key_];
    auto& queue = state.entries_;
    std::erase_if(queue,
                  [](const Entry& current) { return current.waiter_.expired(); });
    const auto position =
        std::find_if(queue.begin(), queue.end(), [&](const Entry& current) {
          const std::shared_ptr<BlockingWaiter> value = current.waiter_.lock();
          return value != nullptr && value->ticket() > waiter->ticket();
        });
    queue.insert(position,
                 Entry{waiter, registration.policy_,
                       registration.stream_after_});
  }

  void Unregister(const BlockingKey& key, std::uint64_t ticket) {
    Erase(key, ticket);
  }

  // Pass FIFO ownership only within the lane whose active waiter completed.
  // A physical-key notification here would spuriously wake every private
  // XREAD broadcast lane whenever an unrelated waiter timed out.
  void NotifyLane(const BlockingKey& key) {
    auto found = queues_.find(key);
    if (found == queues_.end()) return;
    auto& queue = found->second.entries_;
    std::erase_if(queue,
                  [](const Entry& entry) { return entry.waiter_.expired(); });
    if (queue.empty()) {
      queues_.erase(found);
      return;
    }
    if (queue.front().policy_ == BlockingQueuePolicy::kBroadcast) {
      for (const Entry& entry : queue) {
        if (const auto waiter = entry.waiter_.lock()) {
          SignalBlockingReady(waiter);
        }
      }
      return;
    }
    if (const auto waiter = queue.front().waiter_.lock()) {
      SignalBlockingReady(waiter);
    }
  }

  void Notify(
      std::uint8_t db_id, std::string_view key,
      std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_id =
          std::nullopt) {
    for (auto found = queues_.begin(); found != queues_.end();) {
      if (found->first.db_id_ != db_id || found->first.key_ != key) {
        ++found;
        continue;
      }
      auto& state = found->second;
      auto& queue = state.entries_;
      std::erase_if(queue,
                    [](const Entry& entry) { return entry.waiter_.expired(); });
      if (queue.empty()) {
        auto empty = found++;
        queues_.erase(empty);
        continue;
      }
      auto matches = [&](const Entry& entry) {
        return !entry.stream_after_.has_value() || !stream_id.has_value() ||
               *stream_id > *entry.stream_after_;
      };
      if (queue.front().policy_ == BlockingQueuePolicy::kBroadcast) {
        for (const Entry& entry : queue) {
          if (!matches(entry)) continue;
          if (const auto waiter = entry.waiter_.lock())
            SignalBlockingReady(waiter);
        }
      } else {
        for (const Entry& entry : queue) {
          if (!matches(entry)) continue;
          if (const auto waiter = entry.waiter_.lock()) {
            SignalBlockingReady(waiter);
            break;
          }
        }
      }
      ++found;
    }
  }

 private:
  struct Entry {
    std::weak_ptr<BlockingWaiter> waiter_;
    BlockingQueuePolicy policy_ = BlockingQueuePolicy::kFifo;
    std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_after_;
  };

  struct QueueState {
    std::deque<Entry> entries_;
  };

 public:
  void NotifyDb(std::uint8_t db_id) {
    for (auto& [key, state] : queues_) {
      if (key.db_id_ != db_id) continue;
      for (const Entry& entry : state.entries_) {
        if (const auto waiter = entry.waiter_.lock()) {
          SignalBlockingReady(waiter);
          if (entry.policy_ == BlockingQueuePolicy::kFifo) break;
        }
      }
    }
  }

 private:

  void Erase(const BlockingKey& key, std::uint64_t ticket) {
    auto found = queues_.find(key);
    if (found == queues_.end()) return;
    auto& state = found->second;
    auto& queue = state.entries_;
    for (auto it = queue.begin(); it != queue.end();) {
      const std::shared_ptr<BlockingWaiter> value = it->waiter_.lock();
      if (value == nullptr || value->ticket() == ticket) {
        it = queue.erase(it);
      } else {
        ++it;
      }
    }
    if (queue.empty()) queues_.erase(found);
  }

  absl::flat_hash_map<BlockingKey, QueueState, BlockingKeyHash,
                      BlockingKeyEqual>
      queues_;
};

BlockingWaitRegistry& LocalBlockingWaiters() {
  static thread_local BlockingWaitRegistry registry;
  return registry;
}

Task<absl::Status> TimeoutBlockingWaiter(
    std::shared_ptr<BlockingWaiter> waiter,
    std::chrono::steady_clock::time_point deadline) {
  constexpr auto kCancellationGranularity = std::chrono::seconds(1);
  for (;;) {
    if (waiter->reason() == WakeReason::kCancelled) {
      co_return absl::OkStatus();
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) break;
    absl::Status slept = co_await celer::SleepFor(
        *celer::ThisWorker().self_,
        std::min(
            deadline - now,
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                kCancellationGranularity)));
    if (!slept.ok()) co_return slept;
  }
  waiter->SignalTimeout();
  co_return absl::OkStatus();
}

CommandReply BuiltReply(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

unsigned ShardForKey(std::string_view key) {
  return g_storage->OwnerForKey(key);
}

struct WaiterCleanup {
  BlockingKey key_;
  std::uint64_t ticket_ = 0;
};

void RunWaiterCleanup(void* context, std::uint64_t) noexcept {
  std::unique_ptr<WaiterCleanup> cleanup(static_cast<WaiterCleanup*>(context));
  LocalBlockingWaiters().Unregister(cleanup->key_, cleanup->ticket_);
  LocalBlockingWaiters().NotifyLane(cleanup->key_);
}

void PostWaiterCleanup(const WaitRegistration& registration,
                       std::uint64_t ticket) noexcept {
  auto cleanup =
      std::make_unique<WaiterCleanup>(WaiterCleanup{registration.key_, ticket});
  const celer::CurrentWorker& current = celer::ThisWorker();
  if (registration.owner_ == current.id_) {
    RunWaiterCleanup(cleanup.release(), 0);
    return;
  }
  celer::PostNotification(current.cross_core_, registration.owner_,
                          celer::RemoteNotification{
                              .context_ = cleanup.release(),
                              .value_ = 0,
                              .run_fn_ = &RunWaiterCleanup,
                          });
}

class BlockingRegistrationGuard {
 public:
  BlockingRegistrationGuard(std::shared_ptr<BlockingWaiter> waiter,
                            const std::vector<WaitRegistration>* registrations)
      : waiter_(std::move(waiter)), registrations_(registrations) {}
  BlockingRegistrationGuard(const BlockingRegistrationGuard&) = delete;
  BlockingRegistrationGuard& operator=(const BlockingRegistrationGuard&) =
      delete;
  ~BlockingRegistrationGuard() {
    if (!active_) return;
    waiter_->Cancel();
    for (const WaitRegistration& registration : *registrations_) {
      PostWaiterCleanup(registration, waiter_->ticket());
    }
  }

  void Release() noexcept { active_ = false; }

 private:
  std::shared_ptr<BlockingWaiter> waiter_;
  const std::vector<WaitRegistration>* registrations_ = nullptr;
  bool active_ = true;
};

Task<absl::Status> RegisterBlockingWaiter(
    const std::shared_ptr<BlockingWaiter>& waiter,
    const std::vector<WaitRegistration>& registrations) {
  for (const WaitRegistration& registration : registrations) {
    (void)co_await celer::SubmitTo(
        registration.owner_, [registration, waiter] {
          LocalBlockingWaiters().Register(registration, waiter);
          return true;
        });
  }
  co_return absl::OkStatus();
}

void UnregisterBlockingWaiter(
    const std::shared_ptr<BlockingWaiter>& waiter,
    const std::vector<WaitRegistration>& registrations) {
  waiter->Cancel();
  for (const WaitRegistration& registration : registrations) {
    PostWaiterCleanup(registration, waiter->ticket());
  }
}

struct BlockingKeyNotification {
  BlockingKey key_;
  std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_id_;
};

void RunBlockingKeyNotification(void* context, std::uint64_t) noexcept {
  std::unique_ptr<BlockingKeyNotification> notification(
      static_cast<BlockingKeyNotification*>(context));
  LocalBlockingWaiters().Notify(notification->key_.db_id_,
                                notification->key_.key_,
                                notification->stream_id_);
}

void NotifyBlockingKey(
    std::uint8_t db_id, std::string_view key,
    std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_id =
        std::nullopt) {
  const unsigned owner = ShardForKey(key);
  const celer::CurrentWorker& current = celer::ThisWorker();
  if (owner == current.id_) {
    LocalBlockingWaiters().Notify(db_id, key, stream_id);
    return;
  }
  auto notification = std::make_unique<BlockingKeyNotification>(
      BlockingKeyNotification{BlockingKey{db_id, std::string(key), {}},
                              stream_id});
  celer::PostNotification(current.cross_core_, owner,
                          celer::RemoteNotification{
                              .context_ = notification.release(),
                              .value_ = 0,
                              .run_fn_ = &RunBlockingKeyNotification,
                          });
}

bool CmpCaseInsensitive(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    unsigned char ca = static_cast<unsigned char>(a[i]);
    unsigned char cb = static_cast<unsigned char>(b[i]);
    if (ca >= 'A' && ca <= 'Z') ca += 'a' - 'A';
    if (cb >= 'A' && cb <= 'Z') cb += 'a' - 'A';
    if (ca != cb) return false;
  }
  return true;
}

bool ParseInt64(std::string_view text, std::int64_t* value) {
  if (value == nullptr || text.empty()) return false;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, *value);
  return error == std::errc{} && parsed_end == end;
}

bool ParseNonNegative(std::string_view text, std::uint64_t* value) {
  std::int64_t parsed = 0;
  if (!ParseInt64(text, &parsed) || parsed < 0) return false;
  *value = static_cast<std::uint64_t>(parsed);
  return true;
}

bool ParseRespCount(std::string_view encoded, std::size_t* offset, char prefix,
                    std::uint64_t* value) {
  if (*offset >= encoded.size() || encoded[*offset] != prefix) return false;
  const std::size_t begin = ++*offset;
  const std::size_t end = encoded.find("\r\n", begin);
  if (end == std::string_view::npos || end == begin) return false;
  const auto parsed =
      std::from_chars(encoded.data() + begin, encoded.data() + end, *value);
  if (parsed.ec != std::errc{} || parsed.ptr != encoded.data() + end) {
    return false;
  }
  *offset = end + 2;
  return true;
}

bool ParseRespBulk(std::string_view encoded, std::size_t* offset,
                   std::string_view* value) {
  std::uint64_t bytes = 0;
  if (!ParseRespCount(encoded, offset, '$', &bytes) ||
      bytes > encoded.size() - *offset ||
      encoded.size() - *offset - static_cast<std::size_t>(bytes) < 2) {
    return false;
  }
  *value = encoded.substr(*offset, static_cast<std::size_t>(bytes));
  *offset += static_cast<std::size_t>(bytes);
  if (encoded.substr(*offset, 2) != "\r\n") return false;
  *offset += 2;
  return true;
}

std::string_view AppendStorageError(ReplyBuilder& reply_builder,
                                    const absl::Status& status) {
  return status.message().starts_with("WRONGTYPE ")
             ? reply_builder.AppendError(status.message())
             : reply_builder.AppendError("ERR ", status.message());
}

void AppendBulkArray(ReplyBuilder& builder,
                     const std::vector<std::string>& values) {
  builder.AppendArrayHeader(values.size());
  for (const std::string& value : values) builder.AppendBulkString(value);
}

Task<absl::Status> ListLockHoldCallback(void*, const tx::ShardSlice&) {
  co_return absl::OkStatus();
}

absl::StatusOr<bool> ParseListLeft(std::string_view value) {
  if (CmpCaseInsensitive(value, "left")) return true;
  if (CmpCaseInsensitive(value, "right")) return false;
  return absl::Status(absl::StatusCode::kInvalidArgument, "syntax error");
}

struct SingleShardListOutcome {
  explicit SingleShardListOutcome(absl::Status status, std::string key = {},
                                  std::vector<std::string> values = {})
      : status_(std::move(status)),
        key_(std::move(key)),
        values_(std::move(values)) {}

  absl::Status status_;
  std::string key_;
  std::vector<std::string> values_;
};

Task<SingleShardListOutcome> ExecuteSingleShardListMulti(
    std::uint8_t db_id, std::vector<std::string> keys, bool move,
    bool source_left, bool destination_left, bool pop_left,
    std::uint64_t pop_count) {
  std::vector<tx::KeyRef> locks;
  locks.reserve(keys.size());
  for (const std::string& key : keys) {
    const tx::LockFp fingerprint =
        tx::FingerprintOf(storage::ComputeDigest(key));
    bool duplicate = false;
    for (const tx::KeyRef& lock : locks) {
      if (lock.db_ == db_id && lock.fp_ == fingerprint) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      locks.push_back(tx::KeyRef{fingerprint, tx::LockMode::kExclusive, db_id});
    }
  }
  auto guard = co_await tx::CurrentTxShard().AcquireKeys(locks);

  if (!move) {
    for (const std::string& key : keys) {
      storage::ListOperation pop;
      pop.kind_ = pop_left ? storage::ListOperationKind::kPopLeft
                           : storage::ListOperationKind::kPopRight;
      pop.count_ = pop_count;
      pop.count_provided_ = true;
      auto result = co_await g_storage->ExecuteListLocked(
          db_id, key, storage::ComputeDigest(key), pop);
      if (!result.ok()) {
        co_return SingleShardListOutcome(result.status());
      }
      if (!result->values_.empty()) {
        co_return SingleShardListOutcome(absl::OkStatus(), key,
                                         std::move(result->values_));
      }
    }
    co_return SingleShardListOutcome(absl::OkStatus());
  }

  const std::string& source = keys[0];
  const std::string& destination = keys[1];
  if (source == destination) {
    storage::ListOperation operation;
    operation.kind_ = storage::ListOperationKind::kMoveWithin;
    operation.first_ = source_left ? 1 : 0;
    operation.second_ = destination_left ? 1 : 0;
    auto result = co_await g_storage->ExecuteListLocked(
        db_id, source, storage::ComputeDigest(source), operation);
    if (!result.ok()) {
      co_return SingleShardListOutcome(result.status());
    }
    co_return SingleShardListOutcome(absl::OkStatus(), {},
                                     std::move(result->values_));
  }

  const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
  storage::TxShardWrites writes;
  writes.txid_ = txid;
  writes.collect_undo_ = true;
  storage::ListOperation pop;
  pop.kind_ = source_left ? storage::ListOperationKind::kPopLeft
                          : storage::ListOperationKind::kPopRight;
  pop.count_ = 1;
  auto popped = co_await g_storage->ExecuteListLocked(
      db_id, source, storage::ComputeDigest(source), pop, &writes);
  if (!popped.ok()) {
    co_return SingleShardListOutcome(popped.status());
  }
  if (popped->values_.empty()) {
    co_return SingleShardListOutcome(absl::OkStatus());
  }

  storage::ListOperation push;
  push.kind_ = destination_left ? storage::ListOperationKind::kPushLeft
                                : storage::ListOperationKind::kPushRight;
  push.values_.push_back(popped->values_.front());
  auto pushed = co_await g_storage->ExecuteListLocked(
      db_id, destination, storage::ComputeDigest(destination), push, &writes);
  if (!pushed.ok()) {
    (void)co_await g_storage->RollbackTxLocal(txid);
    co_return SingleShardListOutcome(pushed.status());
  }
  std::vector<storage::TxShardWrites*> write_refs{&writes};
  absl::Status committed =
      co_await g_storage->CommitTxWrites(txid, std::move(write_refs));
  if (!committed.ok()) {
    (void)co_await g_storage->RollbackTxLocal(txid);
    co_return SingleShardListOutcome(std::move(committed));
  }
  (void)co_await g_storage->DiscardTxUndoLocal(txid);
  co_return SingleShardListOutcome(absl::OkStatus(), {},
                                   std::move(popped->values_));
}

}  // namespace

struct BlockingWaitHandle::Impl {
  std::shared_ptr<BlockingWaiter> waiter_;
  std::vector<WaitRegistration> registrations_;
  bool active_ = true;
};

BlockingWaitHandle::BlockingWaitHandle(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

BlockingWaitHandle::BlockingWaitHandle(BlockingWaitHandle&&) noexcept =
    default;

BlockingWaitHandle& BlockingWaitHandle::operator=(
    BlockingWaitHandle&& other) noexcept {
  if (this == &other) return *this;
  FinishBlockingWait(*this);
  impl_ = std::move(other.impl_);
  return *this;
}

BlockingWaitHandle::~BlockingWaitHandle() { FinishBlockingWait(*this); }

Task<absl::StatusOr<std::unique_ptr<BlockingWaitHandle>>>
RegisterBlockingWait(
    std::uint8_t db_id, std::vector<BlockingWaitSpec> specs,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
  auto impl = std::make_unique<BlockingWaitHandle::Impl>();
  impl->waiter_ = std::make_shared<BlockingWaiter>(
      celer::ThisWorker().self_, storage::StorageEngine::AllocateWriteTxid());
  impl->registrations_.reserve(specs.size());
  for (BlockingWaitSpec& spec : specs) {
    WaitRegistration registration{
        ShardForKey(spec.key_),
        BlockingKey{db_id, std::move(spec.key_), std::move(spec.lane_)},
        spec.policy_, spec.stream_after_};
    const bool duplicate =
        std::any_of(impl->registrations_.begin(),
                    impl->registrations_.end(),
                    [&](const WaitRegistration& existing) {
                      return existing.key_ == registration.key_;
                    });
    if (!duplicate) impl->registrations_.push_back(std::move(registration));
  }
  if (impl->registrations_.empty()) {
    co_return absl::InvalidArgumentError("blocking wait has no keys");
  }
  absl::Status registered =
      co_await RegisterBlockingWaiter(impl->waiter_, impl->registrations_);
  if (!registered.ok()) co_return registered;
  if (deadline.has_value()) {
    celer::SpawnOnCurrentWorker(
        TimeoutBlockingWaiter(impl->waiter_, *deadline));
  }
  co_return std::unique_ptr<BlockingWaitHandle>(
      new BlockingWaitHandle(std::move(impl)));
}

Task<BlockingWakeReason> WaitForBlockingReady(BlockingWaitHandle& handle) {
  if (!handle.impl_ || !handle.impl_->active_) {
    co_return BlockingWakeReason::kCancelled;
  }
  co_return co_await handle.impl_->waiter_->Wait();
}

BlockingWakeReason BlockingWaitState(const BlockingWaitHandle& handle) {
  if (!handle.impl_ || !handle.impl_->active_) {
    return BlockingWakeReason::kCancelled;
  }
  return handle.impl_->waiter_->reason();
}

bool ResetBlockingReady(BlockingWaitHandle& handle) {
  return handle.impl_ && handle.impl_->active_ &&
         handle.impl_->waiter_->ResetReady();
}

void FinishBlockingWait(BlockingWaitHandle& handle) {
  if (!handle.impl_ || !handle.impl_->active_) return;
  handle.impl_->active_ = false;
  UnregisterBlockingWaiter(handle.impl_->waiter_,
                           handle.impl_->registrations_);
}

void InitListCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

void NotifyListBlockingKey(std::uint8_t db_id, std::string_view key) {
  NotifyBlockingKey(db_id, key);
}

void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key,
                             std::uint64_t id_ms,
                             std::uint64_t id_seq) {
  NotifyBlockingKey(db_id, key, std::pair{id_ms, id_seq});
}

void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key) {
  NotifyBlockingKey(db_id, key);
}

Task<absl::Status> NotifyBlockingDb(std::uint8_t db_id) {
  for (unsigned worker = 0; worker < g_storage->worker_count(); ++worker) {
    (void)co_await celer::SubmitTo(worker, [db_id] {
      LocalBlockingWaiters().NotifyDb(db_id);
      return true;
    });
  }
  co_return absl::OkStatus();
}

Task<CommandReply> ExecuteSingleListCommandImpl(const CommandRequest& request,
                                                const storage::Digest* digest,
                                                storage::TxShardWrites* tx,
                                                ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  storage::ListOperation op;
  switch (request.kind_) {
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX: {
      op.kind_ = request.kind_ == CommandKind::kLPush
                     ? storage::ListOperationKind::kPushLeft
                 : request.kind_ == CommandKind::kLPushX
                     ? storage::ListOperationKind::kPushLeftIfExists
                 : request.kind_ == CommandKind::kRPush
                     ? storage::ListOperationKind::kPushRight
                     : storage::ListOperationKind::kPushRightIfExists;
      op.values_.reserve(args.size() - 2);
      for (std::size_t i = 2; i < args.size(); ++i) {
        op.values_.push_back(args[i]);
      }
      break;
    }
    case CommandKind::kLPop:
    case CommandKind::kRPop:
      op.kind_ = request.kind_ == CommandKind::kLPop
                     ? storage::ListOperationKind::kPopLeft
                     : storage::ListOperationKind::kPopRight;
      op.count_provided_ = args.size() == 3;
      op.count_ = 1;
      if (op.count_provided_ && !ParseNonNegative(args[2], &op.count_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is out of range, must be positive"));
      }
      break;
    case CommandKind::kLLen:
      op.kind_ = storage::ListOperationKind::kLength;
      break;
    case CommandKind::kLIndex:
      op.kind_ = storage::ListOperationKind::kIndex;
      if (!ParseInt64(args[2], &op.first_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      break;
    case CommandKind::kLRange:
    case CommandKind::kLTrim:
      op.kind_ = request.kind_ == CommandKind::kLRange
                     ? storage::ListOperationKind::kRange
                     : storage::ListOperationKind::kTrim;
      if (!ParseInt64(args[2], &op.first_) ||
          !ParseInt64(args[3], &op.second_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      break;
    case CommandKind::kLSet:
      op.kind_ = storage::ListOperationKind::kSet;
      if (!ParseInt64(args[2], &op.first_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      op.value_ = args[3];
      break;
    case CommandKind::kLInsert:
      if (CmpCaseInsensitive(args[2], "before")) {
        op.kind_ = storage::ListOperationKind::kInsertBefore;
      } else if (CmpCaseInsensitive(args[2], "after")) {
        op.kind_ = storage::ListOperationKind::kInsertAfter;
      } else {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
      op.pivot_ = args[3];
      op.value_ = args[4];
      break;
    case CommandKind::kLRem:
      op.kind_ = storage::ListOperationKind::kRemove;
      if (!ParseInt64(args[2], &op.first_)) {
        co_return BuiltReply(reply_builder.AppendError(
            "ERR value is not an integer or out of range"));
      }
      op.value_ = args[3];
      break;
    case CommandKind::kLPos: {
      op.kind_ = storage::ListOperationKind::kPosition;
      op.value_ = args[2];
      for (std::size_t i = 3; i < args.size();) {
        if (CmpCaseInsensitive(args[i], "rank")) {
          if (++i >= args.size() || !ParseInt64(args[i], &op.rank_)) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR value is not an integer or out of range"));
          }
          if (op.rank_ == std::numeric_limits<std::int64_t>::min()) {
            co_return BuiltReply(
                reply_builder.AppendError("ERR value is out of range"));
          }
          if (op.rank_ == 0) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR RANK can't be zero: use 1 to start from the first "
                "match, 2 from the second, ..."));
          }
          ++i;
        } else if (CmpCaseInsensitive(args[i], "count")) {
          op.count_provided_ = true;
          if (++i >= args.size() || !ParseNonNegative(args[i], &op.count_)) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR value is out of range, must be positive"));
          }
          ++i;
        } else if (CmpCaseInsensitive(args[i], "maxlen")) {
          op.max_length_provided_ = true;
          if (++i >= args.size() ||
              !ParseNonNegative(args[i], &op.max_length_)) {
            co_return BuiltReply(reply_builder.AppendError(
                "ERR value is out of range, must be positive"));
          }
          ++i;
        } else {
          co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
        }
      }
      break;
    }
    default:
      co_return BuiltReply(
          reply_builder.AppendError("ERR unsupported List command path"));
  }

  absl::StatusOr<storage::ListResult> result;
  if (digest == nullptr) {
    result = co_await g_storage->ExecuteList(request.db_id_, args[1], op);
  } else {
    result = co_await g_storage->ExecuteListLocked(request.db_id_, args[1],
                                                   *digest, op, tx);
  }
  if (!result.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, result.status()));
  }
  switch (request.kind_) {
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLPop:
    case CommandKind::kRPop:
    case CommandKind::kLSet:
    case CommandKind::kLInsert:
    case CommandKind::kLRem:
    case CommandKind::kLTrim:
      if (result->length_ != 0) {
        NotifyBlockingKey(request.db_id_, args[1]);
      }
      break;
    default:
      break;
  }
  switch (request.kind_) {
    case CommandKind::kLPush:
    case CommandKind::kLPushX:
    case CommandKind::kRPush:
    case CommandKind::kRPushX:
    case CommandKind::kLLen:
      co_return BuiltReply(
          reply_builder.AppendInteger(static_cast<long long>(result->length_)));
    case CommandKind::kLPop:
    case CommandKind::kRPop:
      if (op.count_provided_) {
        if (!result->key_exists_) {
          co_return BuiltReply(reply_builder.AppendRaw("*-1\r\n"));
        }
        AppendBulkArray(reply_builder, result->values_);
        co_return BuiltReply(reply_builder.View());
      }
      co_return BuiltReply(
          result->values_.empty()
              ? reply_builder.AppendNullBulkString()
              : reply_builder.AppendBulkString(result->values_.front()));
    case CommandKind::kLIndex:
      co_return BuiltReply(
          result->values_.empty()
              ? reply_builder.AppendNullBulkString()
              : reply_builder.AppendBulkString(result->values_.front()));
    case CommandKind::kLRange:
      AppendBulkArray(reply_builder, result->values_);
      co_return BuiltReply(reply_builder.View());
    case CommandKind::kLSet:
    case CommandKind::kLTrim:
      co_return BuiltReply(reply_builder.AppendSimpleString("OK"));
    case CommandKind::kLInsert:
      co_return BuiltReply(reply_builder.AppendInteger(
          result->integer_ == -1 ? -1
                                 : static_cast<long long>(result->length_)));
    case CommandKind::kLRem:
      co_return BuiltReply(reply_builder.AppendInteger(result->integer_));
    case CommandKind::kLPos:
      if (op.count_provided_) {
        reply_builder.AppendArrayHeader(result->positions_.size());
        for (std::int64_t position : result->positions_) {
          reply_builder.AppendInteger(position);
        }
        co_return BuiltReply(reply_builder.View());
      }
      co_return BuiltReply(
          result->positions_.empty()
              ? reply_builder.AppendNullBulkString()
              : reply_builder.AppendInteger(result->positions_.front()));
    default:
      break;
  }
  co_return BuiltReply(reply_builder.AppendError("ERR unreachable List reply"));
}

Task<CommandReply> ExecuteSingleListCommand(const CommandRequest& request,
                                            ReplyBuilder& reply_builder) {
  co_return co_await ExecuteSingleListCommandImpl(request, nullptr, nullptr,
                                                  reply_builder);
}

Task<CommandReply> ExecuteSingleListCommandLocked(const CommandRequest& request,
                                                  const storage::Digest& digest,
                                                  storage::TxShardWrites* tx,
                                                  ReplyBuilder& reply_builder) {
  co_return co_await ExecuteSingleListCommandImpl(request, &digest, tx,
                                                  reply_builder);
}
Task<CommandReply> ExecuteListMultiKey(const CommandRequest& request,
                                       ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  const bool move = request.kind_ == CommandKind::kLMove ||
                    request.kind_ == CommandKind::kRPopLPush;
  std::vector<std::size_t> key_args;
  bool source_left = false;
  bool destination_left = true;
  bool pop_left = true;
  std::uint64_t pop_count = 1;

  if (move) {
    key_args = {1, 2};
    if (request.kind_ == CommandKind::kLMove) {
      auto source = ParseListLeft(args[3]);
      auto destination = ParseListLeft(args[4]);
      if (!source.ok() || !destination.ok()) {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
      source_left = *source;
      destination_left = *destination;
    }
  } else {
    std::int64_t numkeys = 0;
    if (args.size() < 4 || !ParseInt64(args[1], &numkeys) || numkeys <= 0) {
      co_return BuiltReply(
          reply_builder.AppendError("ERR numkeys should be greater than 0"));
    }
    const std::size_t count = static_cast<std::size_t>(numkeys);
    if (count > args.size() - 3) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    for (std::size_t i = 0; i < count; ++i) key_args.push_back(2 + i);
    const std::size_t direction_arg = 2 + count;
    auto direction = ParseListLeft(args[direction_arg]);
    if (!direction.ok()) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    pop_left = *direction;
    std::size_t next = direction_arg + 1;
    if (next < args.size()) {
      if (!CmpCaseInsensitive(args[next], "count") || next + 2 != args.size() ||
          !ParseNonNegative(args[next + 1], &pop_count) || pop_count == 0) {
        co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
      }
    }
  }

  const unsigned first_owner = ShardForKey(args[key_args.front()]);
  bool single_shard = true;
  std::vector<std::string> keys;
  keys.reserve(key_args.size());
  for (std::size_t arg : key_args) {
    single_shard &= ShardForKey(args[arg]) == first_owner;
    keys.push_back(args[arg]);
  }
  if (single_shard) {
    SingleShardListOutcome outcome = co_await celer::SubmitTaskTo(
        first_owner,
        [db_id = request.db_id_, keys = std::move(keys), move, source_left,
         destination_left, pop_left, pop_count]() mutable {
          return ExecuteSingleShardListMulti(db_id, std::move(keys), move,
                                             source_left, destination_left,
                                             pop_left, pop_count);
        });
    if (!outcome.status_.ok()) {
      co_return BuiltReply(AppendStorageError(reply_builder, outcome.status_));
    }
    if (outcome.values_.empty()) {
      co_return BuiltReply(move ? reply_builder.AppendNullBulkString()
                                : reply_builder.AppendRaw("*-1\r\n"));
    }
    for (std::size_t arg : key_args) {
      NotifyBlockingKey(request.db_id_, args[arg]);
    }
    if (move) {
      co_return BuiltReply(
          reply_builder.AppendBulkString(outcome.values_.front()));
    }
    reply_builder.AppendArrayHeader(2);
    reply_builder.AppendBulkString(outcome.key_);
    AppendBulkArray(reply_builder, outcome.values_);
    co_return BuiltReply(reply_builder.View());
  }

  tx::Transaction txn;
  for (std::size_t arg : key_args) {
    txn.AddKey(ShardForKey(args[arg]), request.db_id_,
               storage::ComputeDigest(args[arg]),
               static_cast<std::uint32_t>(arg), tx::LockMode::kExclusive);
  }
  txn.Seal();
  absl::Status status = co_await txn.Schedule();
  if (!status.ok()) {
    co_return BuiltReply(reply_builder.AppendError("ERR ", status.message()));
  }
  status = co_await txn.Execute(&ListLockHoldCallback, nullptr, false);
  if (!status.ok()) {
    co_return BuiltReply(reply_builder.AppendError("ERR ", status.message()));
  }

  auto release = [&]() -> Task<absl::Status> {
    co_return co_await txn.Execute(&ListLockHoldCallback, nullptr, true);
  };

  if (!move) {
    for (std::size_t arg : key_args) {
      storage::ListOperation op;
      op.kind_ = pop_left ? storage::ListOperationKind::kPopLeft
                          : storage::ListOperationKind::kPopRight;
      op.count_ = pop_count;
      op.count_provided_ = true;
      const storage::Digest digest = storage::ComputeDigest(args[arg]);
      auto popped = co_await celer::SubmitTaskTo(
          ShardForKey(args[arg]),
          [db = request.db_id_, key = std::string(args[arg]), digest,
           op]() mutable {
            return g_storage->ExecuteListLocked(db, key, digest, op);
          });
      if (!popped.ok()) {
        (void)co_await release();
        co_return BuiltReply(
            AppendStorageError(reply_builder, popped.status()));
      }
      if (!popped->values_.empty()) {
        status = co_await release();
        if (!status.ok()) {
          co_return BuiltReply(
              reply_builder.AppendError("ERR ", status.message()));
        }
        reply_builder.AppendArrayHeader(2);
        reply_builder.AppendBulkString(args[arg]);
        AppendBulkArray(reply_builder, popped->values_);
        for (std::size_t key_arg : key_args) {
          NotifyBlockingKey(request.db_id_, args[key_arg]);
        }
        co_return BuiltReply(reply_builder.View());
      }
    }
    (void)co_await release();
    co_return BuiltReply(reply_builder.AppendRaw("*-1\r\n"));
  }

  const std::string_view source_key = args[1];
  const std::string_view destination_key = args[2];
  if (source_key == destination_key) {
    storage::ListOperation op;
    op.kind_ = storage::ListOperationKind::kMoveWithin;
    op.first_ = source_left ? 1 : 0;
    op.second_ = destination_left ? 1 : 0;
    const storage::Digest digest = storage::ComputeDigest(source_key);
    auto moved = co_await celer::SubmitTaskTo(
        ShardForKey(source_key),
        [db = request.db_id_, key = std::string(source_key), digest, op] {
          return g_storage->ExecuteListLocked(db, key, digest, op);
        });
    (void)co_await release();
    if (!moved.ok()) {
      co_return BuiltReply(AppendStorageError(reply_builder, moved.status()));
    }
    co_return BuiltReply(
        moved->values_.empty()
            ? reply_builder.AppendNullBulkString()
            : reply_builder.AppendBulkString(moved->values_.front()));
  }

  const std::uint64_t txid = storage::StorageEngine::AllocateWriteTxid();
  std::vector<storage::TxShardWrites> writes(g_storage->worker_count());
  for (auto& write : writes) {
    write.txid_ = txid;
    write.collect_undo_ = true;
  }
  storage::ListOperation pop;
  pop.kind_ = source_left ? storage::ListOperationKind::kPopLeft
                          : storage::ListOperationKind::kPopRight;
  pop.count_ = 1;
  const storage::Digest source_digest = storage::ComputeDigest(source_key);
  const unsigned source_owner = ShardForKey(source_key);
  auto popped = co_await celer::SubmitTaskTo(
      source_owner, [db = request.db_id_, key = std::string(source_key),
                     source_digest, pop, write = &writes[source_owner]] {
        return g_storage->ExecuteListLocked(db, key, source_digest, pop, write);
      });
  if (!popped.ok() || popped->values_.empty()) {
    (void)co_await release();
    if (!popped.ok()) {
      co_return BuiltReply(AppendStorageError(reply_builder, popped.status()));
    }
    co_return BuiltReply(reply_builder.AppendNullBulkString());
  }

  storage::ListOperation push;
  push.kind_ = destination_left ? storage::ListOperationKind::kPushLeft
                                : storage::ListOperationKind::kPushRight;
  push.values_.push_back(popped->values_.front());
  const storage::Digest destination_digest =
      storage::ComputeDigest(destination_key);
  const unsigned destination_owner = ShardForKey(destination_key);
  auto pushed = co_await celer::SubmitTaskTo(
      destination_owner,
      [db = request.db_id_, key = std::string(destination_key),
       destination_digest, push, write = &writes[destination_owner]] {
        return g_storage->ExecuteListLocked(db, key, destination_digest, push,
                                            write);
      });
  if (!pushed.ok()) {
    for (const unsigned owner : {source_owner, destination_owner}) {
      (void)co_await celer::SubmitTaskTo(
          owner, [txid] { return g_storage->RollbackTxLocal(txid); });
    }
    (void)co_await release();
    co_return BuiltReply(AppendStorageError(reply_builder, pushed.status()));
  }

  std::vector<storage::TxShardWrites*> write_ptrs;
  for (auto& write : writes) write_ptrs.push_back(&write);
  status = co_await g_storage->CommitTxWrites(txid, std::move(write_ptrs));
  if (!status.ok()) {
    for (const unsigned owner : {source_owner, destination_owner}) {
      (void)co_await celer::SubmitTaskTo(
          owner, [txid] { return g_storage->RollbackTxLocal(txid); });
    }
  } else {
    for (const unsigned owner : {source_owner, destination_owner}) {
      (void)co_await celer::SubmitTaskTo(
          owner, [txid] { return g_storage->DiscardTxUndoLocal(txid); });
    }
  }
  absl::Status released = co_await release();
  if (!status.ok()) {
    co_return BuiltReply(AppendStorageError(reply_builder, status));
  }
  if (!released.ok()) {
    co_return BuiltReply(reply_builder.AppendError("ERR ", released.message()));
  }
  NotifyBlockingKey(request.db_id_, source_key);
  NotifyBlockingKey(request.db_id_, destination_key);
  co_return BuiltReply(reply_builder.AppendBulkString(popped->values_.front()));
}

Task<CommandReply> ExecuteBlockingListCommand(const CommandRequest& request,
                                              ReplyBuilder& reply_builder) {
  const auto& args = request.args_;
  const std::size_t timeout_arg = request.kind_ == CommandKind::kBLMPop   ? 1
                                  : request.kind_ == CommandKind::kBLMove ? 5
                                  : request.kind_ == CommandKind::kBRPopLPush
                                      ? 3
                                      : args.size() - 1;
  double timeout_seconds = 0;
  if (!ParseRedisDouble(args[timeout_arg], &timeout_seconds)) {
    co_return BuiltReply(reply_builder.AppendError(
        "ERR timeout is not a float or out of range"));
  }
  if (timeout_seconds < 0) {
    co_return BuiltReply(
        reply_builder.AppendError("ERR timeout is negative"));
  }

  CommandRequest nonblocking = request;
  if (request.kind_ == CommandKind::kBLPop ||
      request.kind_ == CommandKind::kBRPop) {
    nonblocking.kind_ = CommandKind::kLMPop;
    nonblocking.args_.clear();
    nonblocking.args_.push_back("LMPOP");
    nonblocking.args_.push_back(std::to_string(args.size() - 2));
    for (std::size_t i = 1; i + 1 < args.size(); ++i) {
      nonblocking.args_.push_back(args[i]);
    }
    nonblocking.args_.push_back(request.kind_ == CommandKind::kBLPop ? "LEFT"
                                                                     : "RIGHT");
  } else if (request.kind_ == CommandKind::kBLMPop) {
    nonblocking.kind_ = CommandKind::kLMPop;
    nonblocking.args_.clear();
    nonblocking.args_.push_back("LMPOP");
    nonblocking.args_.insert(nonblocking.args_.end(), args.begin() + 2,
                             args.end());
  } else if (request.kind_ == CommandKind::kBLMove) {
    nonblocking.kind_ = CommandKind::kLMove;
    nonblocking.args_.assign(args.begin(), args.begin() + 5);
    nonblocking.args_[0] = "LMOVE";
  } else {
    nonblocking.kind_ = CommandKind::kRPopLPush;
    nonblocking.args_.assign(args.begin(), args.begin() + 3);
    nonblocking.args_[0] = "RPOPLPUSH";
  }

  const bool infinite = timeout_seconds == 0;
  std::chrono::steady_clock::time_point deadline{};
  if (!infinite) {
    constexpr long double kNanosecondsPerSecond = 1'000'000'000.0L;
    const long double timeout_nanoseconds =
        static_cast<long double>(timeout_seconds) * kNanosecondsPerSecond;
    const auto now = std::chrono::steady_clock::now();
    const auto maximum_delay =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::time_point::max() - now);
    if (timeout_nanoseconds >
        static_cast<long double>(maximum_delay.count())) {
      co_return BuiltReply(reply_builder.AppendError(
          "ERR timeout is out of range"));
    }
    const auto timeout = std::chrono::nanoseconds(
        static_cast<std::int64_t>(timeout_nanoseconds));
    deadline = now + timeout;
  }

  auto is_empty = [](const CommandReply& reply) {
    return reply.encoded_.starts_with("*-1\r\n") ||
           reply.encoded_.starts_with("$-1\r\n");
  };
  auto finish_attempt = [&](const CommandReply& attempt) {
    if (attempt.encoded_.starts_with("-") ||
        (request.kind_ != CommandKind::kBLPop &&
         request.kind_ != CommandKind::kBRPop)) {
      return BuiltReply(reply_builder.AppendRaw(attempt.encoded_));
    }
    // LMPOP returns [key, [value]]. Decode by RESP lengths so arbitrary
    // binary key/value bytes cannot be mistaken for framing delimiters.
    const std::string_view encoded = attempt.encoded_;
    std::size_t offset = 0;
    std::uint64_t outer_count = 0;
    std::uint64_t inner_count = 0;
    std::string_view selected_key;
    std::string_view selected_value;
    if (!ParseRespCount(encoded, &offset, '*', &outer_count) ||
        outer_count != 2 || !ParseRespBulk(encoded, &offset, &selected_key) ||
        !ParseRespCount(encoded, &offset, '*', &inner_count) ||
        inner_count != 1 || !ParseRespBulk(encoded, &offset, &selected_value) ||
        offset != encoded.size()) {
      return BuiltReply(
          reply_builder.AppendError("ERR invalid internal blocking pop reply"));
    }
    reply_builder.AppendArrayHeader(2);
    reply_builder.AppendBulkString(selected_key);
    reply_builder.AppendBulkString(selected_value);
    return BuiltReply(reply_builder.View());
  };
  auto timeout_reply = [&] {
    return BuiltReply((request.kind_ == CommandKind::kBLMove ||
                       request.kind_ == CommandKind::kBRPopLPush)
                          ? reply_builder.AppendNullBulkString()
                          : reply_builder.AppendRaw("*-1\r\n"));
  };
  bool gate_deadline_reached = false;
  auto execute_attempt = [&](ReplyBuilder& attempt_builder)
      -> Task<CommandReply> {
    // FLUSHDB closes the gate only for its short detach window. A blocked
    // command must not count as an in-flight database operation while it is
    // waiting, but every actual read/modify/write attempt still participates
    // in the gate so it cannot race the detach.
    while (!TryBeginCommandDbOperation(request.db_id_)) {
      if (!infinite && std::chrono::steady_clock::now() >= deadline) {
        gate_deadline_reached = true;
        co_return BuiltReply(
            (request.kind_ == CommandKind::kBLMove ||
             request.kind_ == CommandKind::kBRPopLPush)
                ? attempt_builder.AppendNullBulkString()
                : attempt_builder.AppendRaw("*-1\r\n"));
      }
      absl::Status slept =
          co_await celer::SleepFor(*celer::ThisWorker().self_,
                                   std::chrono::milliseconds(1));
      if (!slept.ok()) {
        co_return BuiltReply(attempt_builder.AppendError(
            "ERR ", slept.message()));
      }
    }
    struct AttemptDbGuard {
      explicit AttemptDbGuard(std::uint8_t db) : db_(db) {}
      ~AttemptDbGuard() { EndCommandDbOperation(db_); }
      std::uint8_t db_;
    } db_guard(request.db_id_);
    co_return co_await ExecuteListMultiKey(nonblocking, attempt_builder);
  };

  // Preserve Redis's immediate path: a command that can consume now never
  // enters the waiter queue.
  {
    ReplyBuilder attempt_builder;
    CommandReply attempt = co_await execute_attempt(attempt_builder);
    if (gate_deadline_reached) co_return timeout_reply();
    if (!is_empty(attempt)) co_return finish_attempt(attempt);
    if (!infinite && std::chrono::steady_clock::now() >= deadline)
      co_return timeout_reply();
  }

  std::vector<WaitRegistration> registrations;
  auto register_key = [&](std::string_view key) {
    const BlockingKey blocking_key{request.db_id_, std::string(key), {}};
    for (const WaitRegistration& registration : registrations) {
      if (registration.key_ == blocking_key) return;
    }
    registrations.push_back(WaitRegistration{
        ShardForKey(key), std::move(blocking_key),
        BlockingQueuePolicy::kFifo, std::nullopt});
  };
  if (nonblocking.kind_ == CommandKind::kLMPop) {
    std::int64_t key_count = 0;
    if (!ParseInt64(nonblocking.args_[1], &key_count) || key_count <= 0) {
      co_return BuiltReply(reply_builder.AppendError("ERR syntax error"));
    }
    registrations.reserve(static_cast<std::size_t>(key_count));
    for (std::size_t i = 0; i < static_cast<std::size_t>(key_count); ++i) {
      register_key(nonblocking.args_[2 + i]);
    }
  } else {
    // BLMOVE/BRPOPLPUSH block only on their source. Registering the
    // destination would put this waiter ahead of legitimate BLPOP waiters
    // even though a destination push cannot make the move executable.
    register_key(nonblocking.args_[1]);
  }

  auto waiter = std::make_shared<BlockingWaiter>(
      celer::ThisWorker().self_, storage::StorageEngine::AllocateWriteTxid());
  BlockingRegistrationGuard registration_guard(waiter, &registrations);
  (void)co_await RegisterBlockingWaiter(waiter, registrations);
  if (!infinite) {
    celer::SpawnOnCurrentWorker(TimeoutBlockingWaiter(waiter, deadline));
  }

  // Recheck after registration to close the empty-check/register race. A
  // readiness signal received while this attempt is running remains latched.
  for (;;) {
    const WakeReason before = waiter->reason();
    if (before == WakeReason::kTimeout) {
      UnregisterBlockingWaiter(waiter, registrations);
      registration_guard.Release();
      co_return timeout_reply();
    }
    if (before == WakeReason::kReady) (void)waiter->ResetReady();

    ReplyBuilder attempt_builder;
    CommandReply attempt = co_await execute_attempt(attempt_builder);
    if (gate_deadline_reached) {
      UnregisterBlockingWaiter(waiter, registrations);
      registration_guard.Release();
      co_return timeout_reply();
    }
    if (!is_empty(attempt)) {
      UnregisterBlockingWaiter(waiter, registrations);
      registration_guard.Release();
      co_return finish_attempt(attempt);
    }

    const WakeReason woke = co_await waiter->Wait();
    if (woke == WakeReason::kTimeout) {
      UnregisterBlockingWaiter(waiter, registrations);
      registration_guard.Release();
      co_return timeout_reply();
    }
    if (woke == WakeReason::kCancelled) {
      UnregisterBlockingWaiter(waiter, registrations);
      registration_guard.Release();
      co_return BuiltReply(
          reply_builder.AppendError("ERR blocking List wait cancelled"));
    }
  }
}

}  // namespace keylane
