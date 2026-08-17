#include "blocking_wait.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
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
#include "keylane/storage/engine.h"

namespace keylane {
using namespace celer;

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

  WakeReason reason() const noexcept { return reason_; }

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

  void Notify(std::uint8_t db_id, std::string_view key,
              BlockingValueType value_type,
              std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_id =
                  std::nullopt) {
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
  LocalBlockingWaiters().Notify(
      notification->key_.db_id_, notification->key_.key_,
      notification->key_.value_type_, notification->stream_id_);
}

void NotifyBlockingKey(std::uint8_t db_id, std::string_view key,
                       BlockingValueType value_type,
                       std::optional<std::pair<std::uint64_t, std::uint64_t>>
                           stream_id = std::nullopt) {
  const unsigned owner = ShardForKey(key);
  const celer::CurrentWorker& current = celer::ThisWorker();
  if (owner == current.id_) {
    LocalBlockingWaiters().Notify(db_id, key, value_type, stream_id);
    return;
  }
  auto notification =
      std::make_unique<BlockingKeyNotification>(BlockingKeyNotification{
          BlockingKey{db_id, std::string(key), {}, value_type}, stream_id});
  celer::PostNotification(current.cross_core_, owner,
                          celer::RemoteNotification{
                              .context_ = notification.release(),
                              .value_ = 0,
                              .run_fn_ = &RunBlockingKeyNotification,
                          });
}


}  // namespace

struct BlockingWaitHandle::Impl {
  std::shared_ptr<BlockingWaiter> waiter_;
  std::vector<WaitRegistration> registrations_;
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
    std::optional<std::chrono::steady_clock::time_point> deadline) {
  auto impl = std::make_unique<BlockingWaitHandle::Impl>();
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
  UnregisterBlockingWaiter(handle.impl_->waiter_, handle.impl_->registrations_);
}

Task<CommandReply> ExecuteBlockingWaitLoop(
    std::uint8_t db_id, std::vector<BlockingWaitSpec> specs,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::string cancellation_message, BlockingAttempt attempt,
    BlockingReplyFactory timeout_reply,
    BlockingStatusReplyFactory status_reply) {
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
    if (waiter) {
      const BlockingWakeReason state = BlockingWaitState(*waiter);
      if (state == BlockingWakeReason::kTimeout) co_return timeout_reply();
      if (state == BlockingWakeReason::kCancelled) {
        co_return status_reply(absl::CancelledError(cancellation_message));
      }
      if (state == BlockingWakeReason::kReady) {
        (void)ResetBlockingReady(*waiter);
      }
    }

    while (!TryBeginCommandDbOperation(db_id)) {
      if (deadline && std::chrono::steady_clock::now() >= *deadline) {
        co_return timeout_reply();
      }
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return status_reply(slept);
    }
    AttemptDbGuard gate(db_id);

    BlockingAttemptResult result = co_await attempt();
    if (result.state_ == BlockingAttemptState::kComplete) {
      co_return std::move(result.reply_);
    }
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      co_return timeout_reply();
    }

    if (!waiter) {
      gate.Release();
      auto registered =
          co_await RegisterBlockingWait(db_id, std::move(specs), deadline);
      if (!registered.ok()) co_return status_reply(registered.status());
      waiter = std::move(*registered);
      continue;  // closes the unavailable-check/register race
    }

    gate.Release();
    const BlockingWakeReason woke = co_await WaitForBlockingReady(*waiter);
    if (woke == BlockingWakeReason::kTimeout) co_return timeout_reply();
    if (woke == BlockingWakeReason::kCancelled) {
      co_return status_reply(absl::CancelledError(cancellation_message));
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

void NotifyListBlockingKey(std::uint8_t db_id, std::string_view key) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kList);
}

void NotifyZSetBlockingKey(std::uint8_t db_id, std::string_view key) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kSortedSet);
}

void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key,
                             std::uint64_t id_ms, std::uint64_t id_seq) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kStream,
                    std::pair{id_ms, id_seq});
}

void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key) {
  NotifyBlockingKey(db_id, key, BlockingValueType::kStream);
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
