#include "blocking_wait.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "keylane/metrics.h"
#include "keylane/storage/engine.h"

namespace keylane {
using namespace celer;

void BlockingWakeCascade::Done() noexcept {
  const std::uint64_t previous =
      pending_.fetch_sub(1, std::memory_order_acq_rel);
  assert(previous != 0);
}

Task<absl::Status> DrainBlockingWakeCascade(BlockingWakeCascade& cascade) {
  while (!cascade.empty()) {
    co_await celer::Yield(*celer::ThisWorker().self_);
  }
  co_return absl::OkStatus();
}

BlockingNotificationCapture::BlockingNotificationCapture(
    unsigned worker_count)
    : per_worker_(worker_count) {}

void BlockingNotificationCapture::Record(std::uint8_t db_id, std::string key,
                                         storage::ValueType value_type) {
  const unsigned worker_id = celer::ThisWorker().id_;
  assert(worker_id < per_worker_.size());
  per_worker_[worker_id].push_back(
      CapturedBlockingNotification{db_id, std::move(key), value_type});
}

std::vector<CapturedBlockingNotification>
BlockingNotificationCapture::Take() {
  std::size_t count = 0;
  for (const auto& slot : per_worker_) count += slot.size();

  std::vector<CapturedBlockingNotification> notifications;
  notifications.reserve(count);
  for (auto& slot : per_worker_) {
    notifications.insert(notifications.end(),
                         std::make_move_iterator(slot.begin()),
                         std::make_move_iterator(slot.end()));
    slot.clear();
  }
  return notifications;
}

namespace {

storage::StorageEngine* g_storage = nullptr;

// The registry is worker-local; cross-worker registration and wakeup use
// runtime notifications instead of a process-wide mutex.
struct BlockingKey {
  std::uint8_t db_id_ = 0;
  std::string key_;
  std::string lane_;
  BlockingValueType value_type_ = BlockingValueType::kList;

  bool operator==(const BlockingKey&) const = default;
};

struct BlockingKeyView {
  std::uint8_t db_id_ = 0;
  std::string_view key_;
  std::string_view lane_;
  BlockingValueType value_type_ = BlockingValueType::kList;
};

bool operator==(const BlockingKey& left, const BlockingKeyView& right) {
  return left.db_id_ == right.db_id_ && left.key_ == right.key_ &&
         left.lane_ == right.lane_ && left.value_type_ == right.value_type_;
}

bool operator==(const BlockingKeyView& left, const BlockingKey& right) {
  return right == left;
}

template <typename H>
H AbslHashValue(H hash, const BlockingKey& value) {
  return H::combine(std::move(hash), value.db_id_, value.key_, value.lane_,
                    value.value_type_);
}

template <typename H>
H AbslHashValue(H hash, const BlockingKeyView& value) {
  return H::combine(std::move(hash), value.db_id_, value.key_, value.lane_,
                    value.value_type_);
}

struct BlockingKeyHash {
  using is_transparent = void;

  std::size_t operator()(const BlockingKey& value) const {
    return absl::HashOf(value.db_id_, value.key_, value.lane_,
                        value.value_type_);
  }
  std::size_t operator()(const BlockingKeyView& value) const {
    return absl::HashOf(value.db_id_, value.key_, value.lane_,
                        value.value_type_);
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
      if (handle_ && !resumed_) {
        if (BlockingWakeCascade* cascade = waiter_->Cancel()) cascade->Done();
      }
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

  WakeReason reason() const noexcept { return reason_; }

  BlockingWakeCascade* ResetReady() noexcept {
    if (reason_ != WakeReason::kReady) return nullptr;
    reason_ = WakeReason::kWaiting;
    return std::exchange(cascade_, nullptr);
  }

  bool Signal(WakeReason reason,
              BlockingWakeCascade* cascade = nullptr) noexcept {
    if (reason_ != WakeReason::kWaiting) return false;
    reason_ = reason;
    cascade_ = cascade;
    std::coroutine_handle<> handle = std::exchange(handle_, {});
    if (handle) worker_->Enqueue(handle);
    return true;
  }

  bool SignalTerminal(WakeReason reason) noexcept {
    if (reason_ != WakeReason::kWaiting && reason_ != WakeReason::kReady) {
      return false;
    }
    if (cascade_ != nullptr) {
      cascade_->Done();
      cascade_ = nullptr;
    }
    reason_ = reason;
    std::coroutine_handle<> handle = std::exchange(handle_, {});
    if (handle) worker_->Enqueue(handle);
    return true;
  }

  void SignalTimeout() noexcept {
    // Deadline is terminal even if readiness was already latched. If the
    // woken attempt loses the element to an earlier waiter, it must observe
    // the expired deadline instead of sleeping after its timer has exited.
    (void)SignalTerminal(WakeReason::kTimeout);
  }

  BlockingWakeCascade* Cancel() noexcept {
    // Cancellation is the final lifecycle state, including after a terminal
    // timeout or CLIENT UNBLOCK wake. This also lets the detached deadline
    // task retire instead of sleeping until its original (possibly distant)
    // deadline after the command has already replied.
    BlockingWakeCascade* cascade = std::exchange(cascade_, nullptr);
    reason_ = WakeReason::kCancelled;
    handle_ = {};
    return cascade;
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
  BlockingWakeCascade* cascade_ = nullptr;
  std::coroutine_handle<> handle_{};
};

struct BlockingReadyNotification {
  std::shared_ptr<BlockingWaiter> waiter_;
  BlockingWakeCascade* cascade_ = nullptr;
};

void RunBlockingReadyNotification(void* context, std::uint64_t) noexcept {
  std::unique_ptr<BlockingReadyNotification> notification(
      static_cast<BlockingReadyNotification*>(context));
  if (!notification->waiter_->Signal(WakeReason::kReady,
                                     notification->cascade_) &&
      notification->cascade_ != nullptr) {
    notification->cascade_->Done();
  }
}

void SignalBlockingReady(const std::shared_ptr<BlockingWaiter>& waiter,
                         BlockingWakeCascade* cascade) {
  if (cascade != nullptr) cascade->Add();
  const celer::CurrentWorker& current = celer::ThisWorker();
  if (waiter->worker_id() == current.id_) {
    if (!waiter->Signal(WakeReason::kReady, cascade) && cascade != nullptr) {
      cascade->Done();
    }
    return;
  }
  auto context = std::make_unique<BlockingReadyNotification>(
      BlockingReadyNotification{waiter, cascade});
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
    std::erase_if(
        queue, [](const Entry& current) { return current.waiter_.expired(); });
    const auto position =
        std::find_if(queue.begin(), queue.end(), [&](const Entry& current) {
          const std::shared_ptr<BlockingWaiter> value = current.waiter_.lock();
          return value != nullptr && value->ticket() > waiter->ticket();
        });
    queue.insert(position, Entry{waiter, registration.policy_,
                                 registration.stream_after_});
  }

  void Unregister(const BlockingKey& key, std::uint64_t ticket) {
    Erase(key, ticket);
  }

  // Pass FIFO ownership only within the lane whose active waiter completed.
  // A physical-key notification here would spuriously wake every private
  // XREAD broadcast lane whenever an unrelated waiter timed out.
  void NotifyLane(const BlockingKey& key,
                  BlockingWakeCascade* cascade = nullptr) {
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
          SignalBlockingReady(waiter, cascade);
        }
      }
      return;
    }
    if (const auto waiter = queue.front().waiter_.lock()) {
      SignalBlockingReady(waiter, cascade);
    }
  }

  void Notify(std::uint8_t db_id, std::string_view key,
              BlockingValueType value_type,
              std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_id =
                  std::nullopt,
              BlockingWakeCascade* cascade = nullptr) {
    for (auto found = queues_.begin(); found != queues_.end();) {
      if (found->first.db_id_ != db_id || found->first.key_ != key) {
        ++found;
        continue;
      }
      if (found->first.value_type_ != value_type) {
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
            SignalBlockingReady(waiter, cascade);
        }
      } else {
        for (const Entry& entry : queue) {
          if (!matches(entry)) continue;
          if (const auto waiter = entry.waiter_.lock()) {
            SignalBlockingReady(waiter, cascade);
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
          SignalBlockingReady(waiter, nullptr);
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

absl::flat_hash_map<std::uint64_t, std::weak_ptr<BlockingWaiter>>&
LocalBlockedClients() {
  static thread_local absl::flat_hash_map<
      std::uint64_t, std::weak_ptr<BlockingWaiter>>
      clients;
  return clients;
}

void RegisterBlockedClient(
    std::uint64_t client_id,
    const std::shared_ptr<BlockingWaiter>& waiter) {
  if (client_id == 0) return;
  LocalBlockedClients()[client_id] = waiter;
  SetClientBlocked(client_id, true);
}

void UnregisterBlockedClient(
    std::uint64_t client_id,
    const std::shared_ptr<BlockingWaiter>& waiter) noexcept {
  if (client_id == 0) return;
  auto& clients = LocalBlockedClients();
  const auto found = clients.find(client_id);
  bool cleared = false;
  if (found != clients.end()) {
    const std::shared_ptr<BlockingWaiter> current = found->second.lock();
    if (current == nullptr || current == waiter) {
      clients.erase(found);
      cleared = true;
    }
  }
  if (cleared) SetClientBlocked(client_id, false);
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

unsigned ShardForKey(std::string_view key) {
  return g_storage->OwnerForKey(key);
}

struct WaiterCleanup {
  BlockingKey key_;
  std::uint64_t ticket_ = 0;
  BlockingWakeCascade* cascade_ = nullptr;
};

void RunWaiterCleanup(void* context, std::uint64_t) noexcept {
  std::unique_ptr<WaiterCleanup> cleanup(static_cast<WaiterCleanup*>(context));
  LocalBlockingWaiters().Unregister(cleanup->key_, cleanup->ticket_);
  LocalBlockingWaiters().NotifyLane(cleanup->key_, cleanup->cascade_);
  if (cleanup->cascade_ != nullptr) cleanup->cascade_->Done();
}

void PostWaiterCleanup(const WaitRegistration& registration,
                       std::uint64_t ticket,
                       BlockingWakeCascade* cascade) noexcept {
  if (cascade != nullptr) cascade->Add();
  auto cleanup = std::make_unique<WaiterCleanup>(
      WaiterCleanup{registration.key_, ticket, cascade});
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

Task<absl::Status> RegisterBlockingWaiter(
    const std::shared_ptr<BlockingWaiter>& waiter,
    const std::vector<WaitRegistration>& registrations) {
  for (const WaitRegistration& registration : registrations) {
    (void)co_await celer::SubmitTo(registration.owner_, [registration, waiter] {
      LocalBlockingWaiters().Register(registration, waiter);
      return true;
    });
  }
  co_return absl::OkStatus();
}

void UnregisterBlockingWaiter(
    const std::shared_ptr<BlockingWaiter>& waiter,
    const std::vector<WaitRegistration>& registrations) {
  BlockingWakeCascade* cascade = waiter->Cancel();
  for (const WaitRegistration& registration : registrations) {
    PostWaiterCleanup(registration, waiter->ticket(), cascade);
  }
  if (cascade != nullptr) cascade->Done();
}

struct BlockingKeyNotification {
  BlockingKey key_;
  std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_id_;
  BlockingWakeCascade* cascade_ = nullptr;
};

void RunBlockingKeyNotification(void* context, std::uint64_t) noexcept {
  std::unique_ptr<BlockingKeyNotification> notification(
      static_cast<BlockingKeyNotification*>(context));
  LocalBlockingWaiters().Notify(
      notification->key_.db_id_, notification->key_.key_,
      notification->key_.value_type_, notification->stream_id_,
      notification->cascade_);
  if (notification->cascade_ != nullptr) notification->cascade_->Done();
}

void NotifyBlockingKey(std::uint8_t db_id, std::string_view key,
                       BlockingValueType value_type,
                       std::optional<std::pair<std::uint64_t, std::uint64_t>>
                           stream_id = std::nullopt,
                       BlockingWakeCascade* cascade = nullptr) {
  const unsigned owner = ShardForKey(key);
  const celer::CurrentWorker& current = celer::ThisWorker();
  if (owner == current.id_) {
    LocalBlockingWaiters().Notify(db_id, key, value_type, stream_id, cascade);
    return;
  }
  if (cascade != nullptr) cascade->Add();
  auto notification =
      std::make_unique<BlockingKeyNotification>(BlockingKeyNotification{
          BlockingKey{db_id, std::string(key), {}, value_type}, stream_id,
          cascade});
  celer::PostNotification(current.cross_core_, owner,
                          celer::RemoteNotification{
                              .context_ = notification.release(),
                              .value_ = 0,
                              .run_fn_ = &RunBlockingKeyNotification,
                          });
}

}  // namespace

bool UnblockClientOnCurrentWorker(std::uint64_t client_id,
                                  ClientUnblockMode mode) noexcept {
  auto& clients = LocalBlockedClients();
  const auto found = clients.find(client_id);
  if (found == clients.end()) return false;
  const std::shared_ptr<BlockingWaiter> waiter = found->second.lock();
  if (waiter == nullptr) {
    clients.erase(found);
    SetClientBlocked(client_id, false);
    return false;
  }
  const WakeReason reason = mode == ClientUnblockMode::kTimeout
                                ? WakeReason::kTimeout
                                : WakeReason::kUnblockedError;
  return waiter->SignalTerminal(reason);
}

bool CancelBlockedClientOnCurrentWorker(std::uint64_t client_id) noexcept {
  auto& clients = LocalBlockedClients();
  const auto found = clients.find(client_id);
  if (found == clients.end()) return false;
  const std::shared_ptr<BlockingWaiter> waiter = found->second.lock();
  if (waiter == nullptr) {
    clients.erase(found);
    SetClientBlocked(client_id, false);
    return false;
  }
  return waiter->SignalTerminal(WakeReason::kCancelled);
}

struct BlockingWaitHandle::Impl {
  std::shared_ptr<BlockingWaiter> waiter_;
  std::vector<WaitRegistration> registrations_;
  std::uint64_t client_id_ = 0;
  bool active_ = true;
};

BlockingWaitHandle::BlockingWaitHandle(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

BlockingWaitHandle::BlockingWaitHandle(BlockingWaitHandle&&) noexcept = default;

BlockingWaitHandle& BlockingWaitHandle::operator=(
    BlockingWaitHandle&& other) noexcept {
  if (this == &other) return *this;
  FinishBlockingWait(*this);
  impl_ = std::move(other.impl_);
  return *this;
}

BlockingWaitHandle::~BlockingWaitHandle() { FinishBlockingWait(*this); }

Task<absl::StatusOr<std::unique_ptr<BlockingWaitHandle>>> RegisterBlockingWait(
    std::uint8_t db_id, std::vector<BlockingWaitSpec> specs,
    std::uint64_t client_id,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
  auto impl = std::make_unique<BlockingWaitHandle::Impl>();
  impl->client_id_ = client_id;
  impl->waiter_ = std::make_shared<BlockingWaiter>(
      celer::ThisWorker().self_, storage::StorageEngine::AllocateWriteTxid());
  impl->registrations_.reserve(specs.size());
  for (BlockingWaitSpec& spec : specs) {
    WaitRegistration registration{
        ShardForKey(spec.key_),
        BlockingKey{db_id, std::move(spec.key_), std::move(spec.lane_),
                    spec.value_type_},
        spec.policy_, spec.stream_after_};
    const bool duplicate =
        std::any_of(impl->registrations_.begin(), impl->registrations_.end(),
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
  RegisterBlockedClient(impl->client_id_, impl->waiter_);
  RecordClientBlocked();
  if (deadline.has_value()) {
    celer::SpawnOnCurrentWorker(
        TimeoutBlockingWaiter(impl->waiter_, *deadline));
  }
  co_return std::unique_ptr<BlockingWaitHandle>(
      new BlockingWaitHandle(std::move(impl)));
}

Task<absl::StatusOr<std::unique_ptr<BlockingWaitHandle>>>
RegisterClientBlockingWait(
    std::uint64_t client_id,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
  auto impl = std::make_unique<BlockingWaitHandle::Impl>();
  impl->client_id_ = client_id;
  impl->waiter_ = std::make_shared<BlockingWaiter>(
      celer::ThisWorker().self_, storage::StorageEngine::AllocateWriteTxid());
  RegisterBlockedClient(impl->client_id_, impl->waiter_);
  RecordClientBlocked();
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

BlockingWakeCascade* ResetBlockingReady(BlockingWaitHandle& handle) {
  return handle.impl_ && handle.impl_->active_
             ? handle.impl_->waiter_->ResetReady()
             : nullptr;
}

void FinishBlockingWait(BlockingWaitHandle& handle) {
  if (!handle.impl_ || !handle.impl_->active_) return;
  handle.impl_->active_ = false;
  RecordClientUnblocked();
  UnregisterBlockedClient(handle.impl_->client_id_, handle.impl_->waiter_);
  UnregisterBlockingWaiter(handle.impl_->waiter_, handle.impl_->registrations_);
}

Task<CommandReply> ExecuteBlockingWaitLoop(
    std::uint64_t client_id, std::uint8_t db_id,
    std::vector<BlockingWaitSpec> specs,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::string cancellation_message, BlockingAttempt attempt,
    BlockingReplyFactory timeout_reply,
    BlockingReplyFactory unblock_error_reply,
    BlockingStatusReplyFactory status_reply, bool yield_before_retry) {
  class AttemptDbGuard {
   public:
    explicit AttemptDbGuard(std::uint8_t db_id) : db_id_(db_id) {}
    ~AttemptDbGuard() { Release(); }

    void Release() {
      if (!active_) return;
      EndCommandDbOperation(db_id_);
      active_ = false;
    }

   private:
    std::uint8_t db_id_;
    bool active_ = true;
  };

  std::unique_ptr<BlockingWaitHandle> waiter;
  for (;;) {
    BlockingWakeCascade* attempt_cascade = nullptr;
    if (waiter) {
      const BlockingWakeReason state = BlockingWaitState(*waiter);
      if (state == BlockingWakeReason::kTimeout) co_return timeout_reply();
      if (state == BlockingWakeReason::kUnblockedError) {
        co_return unblock_error_reply();
      }
      if (state == BlockingWakeReason::kCancelled) {
        co_return status_reply(absl::CancelledError(cancellation_message));
      }
      if (state == BlockingWakeReason::kReady) {
        attempt_cascade = ResetBlockingReady(*waiter);
      }
    }

    struct CascadeCompletion {
      BlockingWakeCascade* cascade_ = nullptr;
      ~CascadeCompletion() { Finish(); }
      void Finish() noexcept {
        if (cascade_ == nullptr) return;
        cascade_->Done();
        cascade_ = nullptr;
      }
    } cascade_completion{attempt_cascade};

    while (!TryBeginCommandDbOperation(db_id)) {
      if (deadline && std::chrono::steady_clock::now() >= *deadline) {
        co_return timeout_reply();
      }
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return status_reply(slept);
    }
    AttemptDbGuard gate(db_id);

    BlockingAttemptResult result = co_await attempt(attempt_cascade);
    cascade_completion.Finish();
    if (result.state_ == BlockingAttemptState::kComplete) {
      co_return std::move(result.reply_);
    }
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      co_return timeout_reply();
    }

    if (!waiter) {
      gate.Release();
      auto registered =
          co_await RegisterBlockingWait(db_id, std::move(specs), client_id,
                                        deadline);
      if (!registered.ok()) co_return status_reply(registered.status());
      waiter = std::move(*registered);
      continue;  // closes the unavailable-check/register race
    }

    gate.Release();
    const BlockingWakeReason woke = co_await WaitForBlockingReady(*waiter);
    if (woke == BlockingWakeReason::kTimeout) co_return timeout_reply();
    if (woke == BlockingWakeReason::kUnblockedError) {
      co_return unblock_error_reply();
    }
    if (woke == BlockingWakeReason::kCancelled) {
      co_return status_reply(absl::CancelledError(cancellation_message));
    }
    // Readiness is not an ownership handoff. A blocking move may write a
    // destination unrelated to the key that woke it. Let requests already
    // admitted on other connections establish their destination lock queue
    // positions before the move retries. This is deliberately restricted to
    // moves and costs no timer sleep or ordinary-command latency.
    if (yield_before_retry) {
      co_await celer::Yield(*celer::ThisWorker().self_);
    }
  }
}

absl::StatusOr<std::optional<std::chrono::steady_clock::time_point>>
BlockingDeadlineFromSeconds(double timeout_seconds) {
  if (timeout_seconds == 0) return std::nullopt;
  constexpr long double kNanosecondsPerSecond = 1'000'000'000.0L;
  const long double nanoseconds =
      static_cast<long double>(timeout_seconds) * kNanosecondsPerSecond;
  const auto now = std::chrono::steady_clock::now();
  const auto maximum = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::time_point::max() - now);
  if (!std::isfinite(timeout_seconds) || nanoseconds < 0 ||
      nanoseconds > static_cast<long double>(maximum.count())) {
    return absl::InvalidArgumentError("timeout is out of range");
  }
  return now +
         std::chrono::nanoseconds(static_cast<std::int64_t>(nanoseconds));
}


void InitBlockingWaitStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

void NotifyListBlockingKey(std::uint8_t db_id, std::string_view key,
                           BlockingWakeCascade* cascade) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kList, std::nullopt,
                    cascade);
}

void NotifyListBlockingKey(const CommandRequest& request,
                           std::string_view key) {
  if (request.blocking_notification_capture_ != nullptr) {
    request.blocking_notification_capture_->Record(
        request.db_id_, std::string(key), storage::ValueType::kList);
    return;
  }
  NotifyListBlockingKey(request.db_id_, key,
                        request.blocking_wake_cascade_);
}

void NotifyZSetBlockingKey(std::uint8_t db_id, std::string_view key,
                           BlockingWakeCascade* cascade) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kSortedSet, std::nullopt,
                    cascade);
}

void NotifyZSetBlockingKey(const CommandRequest& request,
                           std::string_view key) {
  if (request.blocking_notification_capture_ != nullptr) {
    request.blocking_notification_capture_->Record(
        request.db_id_, std::string(key), storage::ValueType::kSortedSet);
    return;
  }
  NotifyZSetBlockingKey(request.db_id_, key,
                        request.blocking_wake_cascade_);
}

void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key,
                             std::uint64_t id_ms, std::uint64_t id_seq,
                             BlockingWakeCascade* cascade) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kStream,
                    std::pair{id_ms, id_seq}, cascade);
}

void NotifyStreamBlockingKey(const CommandRequest& request,
                             std::string_view key, std::uint64_t id_ms,
                             std::uint64_t id_seq) {
  if (request.blocking_notification_capture_ != nullptr) {
    request.blocking_notification_capture_->Record(
        request.db_id_, std::string(key), storage::ValueType::kStream);
    return;
  }
  NotifyStreamBlockingKey(request.db_id_, key, id_ms, id_seq,
                          request.blocking_wake_cascade_);
}

void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key,
                             BlockingWakeCascade* cascade) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kStream, std::nullopt,
                    cascade);
}

void NotifyStreamBlockingKey(const CommandRequest& request,
                             std::string_view key) {
  if (request.blocking_notification_capture_ != nullptr) {
    request.blocking_notification_capture_->Record(
        request.db_id_, std::string(key), storage::ValueType::kStream);
    return;
  }
  NotifyStreamBlockingKey(request.db_id_, key,
                          request.blocking_wake_cascade_);
}

Task<absl::Status> FlushBlockingNotifications(
    BlockingNotificationCapture& capture, BlockingWakeCascade* cascade) {
  std::vector<CapturedBlockingNotification> notifications = capture.Take();
  std::sort(notifications.begin(), notifications.end(),
            [](const CapturedBlockingNotification& left,
               const CapturedBlockingNotification& right) {
              return std::tie(left.db_id_, left.key_, left.value_type_) <
                     std::tie(right.db_id_, right.key_, right.value_type_);
            });
  notifications.erase(
      std::unique(notifications.begin(), notifications.end(),
                  [](const CapturedBlockingNotification& left,
                     const CapturedBlockingNotification& right) {
                    return left.db_id_ == right.db_id_ &&
                           left.key_ == right.key_ &&
                           left.value_type_ == right.value_type_;
                  }),
      notifications.end());

  for (std::size_t begin = 0; begin < notifications.size();) {
    std::size_t end = begin + 1;
    while (end < notifications.size() &&
           notifications[end].db_id_ == notifications[begin].db_id_ &&
           notifications[end].key_ == notifications[begin].key_) {
      ++end;
    }
    const std::uint8_t db_id = notifications[begin].db_id_;
    const std::string& key = notifications[begin].key_;
    const storage::ExpirationInfo info = co_await celer::SubmitTaskTo(
        ShardForKey(key), [db_id, key] {
          return g_storage->GetExpiration(db_id, key);
        });
    if (info.exists_) {
      const auto matching = std::find_if(
          notifications.begin() + static_cast<std::ptrdiff_t>(begin),
          notifications.begin() + static_cast<std::ptrdiff_t>(end),
          [&](const CapturedBlockingNotification& notification) {
            return notification.value_type_ == info.value_type_;
          });
      if (matching !=
          notifications.begin() + static_cast<std::ptrdiff_t>(end)) {
        if (info.value_type_ == storage::ValueType::kList) {
          NotifyListBlockingKey(db_id, key, cascade);
        } else if (info.value_type_ == storage::ValueType::kSortedSet) {
          NotifyZSetBlockingKey(db_id, key, cascade);
        } else if (info.value_type_ == storage::ValueType::kStream) {
          NotifyStreamBlockingKey(db_id, key, cascade);
        }
      }
    }
    begin = end;
  }
  co_return absl::OkStatus();
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


}  // namespace keylane
