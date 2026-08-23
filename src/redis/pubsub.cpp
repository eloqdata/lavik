#include "keylane/pubsub.h"

#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/glob.h"
#include "keylane/resp.h"

namespace keylane {
using namespace celer;

namespace {

constexpr std::size_t kWorkerPubSubBufferLimit = 128ULL * 1024 * 1024;
constexpr std::size_t kPubSubQueueLimit = 10'000;

struct WorkerPubSubRegistry;

struct QueuedFrame {
  std::shared_ptr<const std::string> encoded_;
  bool exit_ = false;
};

}  // namespace

class PubSubSession : public std::enable_shared_from_this<PubSubSession> {
 public:
  PubSubSession(WorkerPubSubRegistry* registry, Worker* worker, int fd)
      : registry_(registry), worker_(worker), fd_(fd) {}

  bool Subscribe(std::string_view channel);
  bool Unsubscribe(std::string_view channel);
  void UnsubscribeAll();
  bool PSubscribe(std::string_view pattern);
  bool PUnsubscribe(std::string_view pattern);
  void PUnsubscribeAll();
  bool subscribed_to(std::string_view channel) const {
    return channels_.contains(channel);
  }
  bool subscribed_to_pattern(std::string_view pattern) const {
    return patterns_.contains(pattern);
  }
  std::size_t subscription_count() const noexcept {
    return channels_.size() + patterns_.size();
  }
  const std::vector<std::string>& channel_order() const noexcept {
    return channel_order_;
  }
  const std::vector<std::string>& pattern_order() const noexcept {
    return pattern_order_;
  }

  bool Enqueue(const std::shared_ptr<const std::string>& encoded);
  void EnqueueExit();
  Task<QueuedFrame> Next();

  void Close() noexcept {
    if (closed_) return;
    closed_ = true;
    output_ready_.NotifyAll(*worker_);
  }

  void ReaderStarted() noexcept {
    reader_started_ = true;
    reader_done_ = false;
  }
  void ReaderDone() noexcept {
    reader_done_ = true;
    reader_done_ready_.NotifyAll(*worker_);
  }
  Task<absl::Status> WaitReaderDone() {
    while (reader_started_ && !reader_done_) {
      co_await reader_done_ready_.Wait();
    }
    co_return absl::OkStatus();
  }

 private:
  friend void UnregisterPubSubSession(
      const std::shared_ptr<PubSubSession>& session);

  void RemoveFromChannel(std::string_view channel);
  void RemoveFromPattern(std::string_view pattern);

  WorkerPubSubRegistry* registry_ = nullptr;
  Worker* worker_ = nullptr;
  int fd_ = -1;
  absl::flat_hash_set<std::string> channels_;
  std::vector<std::string> channel_order_;
  absl::flat_hash_set<std::string> patterns_;
  std::vector<std::string> pattern_order_;
  std::deque<QueuedFrame> queue_;
  std::size_t pending_bytes_ = 0;
  AsyncNotification output_ready_;
  AsyncNotification reader_done_ready_;
  bool registered_ = true;
  bool closed_ = false;
  bool exit_enqueued_ = false;
  bool reader_started_ = false;
  bool reader_done_ = true;
};

namespace {

struct WorkerPubSubRegistry {
  absl::flat_hash_map<std::string, std::vector<std::shared_ptr<PubSubSession>>>
      channels_;
  absl::flat_hash_map<std::string, std::vector<std::shared_ptr<PubSubSession>>>
      patterns_;
  std::size_t pending_bytes_ = 0;
};

std::unique_ptr<WorkerPubSubRegistry[]> g_registries;
unsigned g_worker_count = 0;

WorkerPubSubRegistry& LocalRegistry() {
  assert(g_registries != nullptr);
  assert(ThisWorker().id_ < g_worker_count);
  return g_registries[ThisWorker().id_];
}

void AppendSubscriptionFrame(ReplyBuilder* builder, std::string_view kind,
                             const std::string* channel,
                             std::size_t subscription_count) {
  builder->AppendArrayHeader(3);
  builder->AppendBulkString(kind);
  if (channel == nullptr) {
    builder->AppendNullBulkString();
  } else {
    builder->AppendBulkString(*channel);
  }
  builder->AppendInteger(static_cast<long long>(subscription_count));
}

std::shared_ptr<const std::string> EncodeMessage(std::string_view channel,
                                                 std::string_view payload) {
  ReplyBuilder builder;
  builder.Reserve(channel.size() + payload.size() + 48);
  builder.AppendArrayHeader(3);
  builder.AppendBulkString("message");
  builder.AppendBulkString(channel);
  builder.AppendBulkString(payload);
  return std::make_shared<const std::string>(std::move(builder).Release());
}

std::shared_ptr<const std::string> EncodePatternMessage(
    std::string_view pattern, std::string_view channel,
    std::string_view payload) {
  ReplyBuilder builder;
  builder.Reserve(pattern.size() + channel.size() + payload.size() + 64);
  builder.AppendArrayHeader(4);
  builder.AppendBulkString("pmessage");
  builder.AppendBulkString(pattern);
  builder.AppendBulkString(channel);
  builder.AppendBulkString(payload);
  return std::make_shared<const std::string>(std::move(builder).Release());
}

std::uint64_t DeliverLocal(std::string_view channel, std::string_view payload,
                           const std::shared_ptr<const std::string>& encoded) {
  WorkerPubSubRegistry& registry = LocalRegistry();
  std::uint64_t receivers = 0;
  if (auto found = registry.channels_.find(channel);
      found != registry.channels_.end()) {
    for (const auto& session : found->second) {
      // A notification posted before UNSUBSCRIBE may run afterward.
      // Membership is checked at the final worker-local enqueue point.
      if (session->subscribed_to(channel) && session->Enqueue(encoded)) {
        ++receivers;
      }
    }
  }
  for (const auto& [pattern, sessions] : registry.patterns_) {
    if (!RedisGlobMatch(pattern, channel)) continue;
    auto pattern_message = EncodePatternMessage(pattern, channel, payload);
    for (const auto& session : sessions) {
      if (session->subscribed_to_pattern(pattern) &&
          session->Enqueue(pattern_message)) {
        ++receivers;
      }
    }
  }
  return receivers;
}

class PublishOperation : public std::enable_shared_from_this<PublishOperation> {
 public:
  PublishOperation(Worker* origin, unsigned participants,
                   std::shared_ptr<const std::string> channel,
                   std::shared_ptr<const std::string> payload,
                   std::shared_ptr<const std::string> encoded)
      : origin_(origin),
        remaining_(participants),
        channel_(std::move(channel)),
        payload_(std::move(payload)),
        encoded_(std::move(encoded)) {}

  class Awaiter {
   public:
    explicit Awaiter(std::shared_ptr<PublishOperation> operation)
        : operation_(std::move(operation)) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> handle) {
      operation_->handle_ = handle;
      operation_->Dispatch();
      return true;
    }
    std::uint64_t await_resume() const noexcept {
      return operation_->receivers_.load(std::memory_order_acquire);
    }

   private:
    std::shared_ptr<PublishOperation> operation_;
  };

  Awaiter Wait() { return Awaiter(shared_from_this()); }

  void Complete(std::uint64_t receivers) noexcept {
    receivers_.fetch_add(receivers, std::memory_order_relaxed);
    if (remaining_.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
    if (ThisWorker().id_ == origin_->id()) {
      origin_->Enqueue(handle_);
      return;
    }
    auto context =
        std::make_unique<std::shared_ptr<PublishOperation>>(shared_from_this());
    PostNotification(
        ThisWorker().cross_core_, origin_->id(),
        RemoteNotification{
            .context_ = context.release(),
            .value_ = 0,
            .run_fn_ =
                [](void* raw, std::uint64_t) noexcept {
                  std::unique_ptr<std::shared_ptr<PublishOperation>> operation(
                      static_cast<std::shared_ptr<PublishOperation>*>(raw));
                  (*operation)->origin_->Enqueue((*operation)->handle_);
                },
        });
  }

 private:
  struct Delivery {
    std::shared_ptr<PublishOperation> operation_;
  };

  void Dispatch() {
    const CurrentWorker& current = ThisWorker();
    for (unsigned worker = 0; worker < g_worker_count; ++worker) {
      if (worker == current.id_) {
        Complete(DeliverLocal(*channel_, *payload_, encoded_));
        continue;
      }
      auto delivery = std::make_unique<Delivery>(
          Delivery{.operation_ = shared_from_this()});
      PostNotification(current.cross_core_, worker,
                       RemoteNotification{
                           .context_ = delivery.release(),
                           .value_ = 0,
                           .run_fn_ =
                               [](void* raw, std::uint64_t) noexcept {
                                 std::unique_ptr<Delivery> delivery(
                                     static_cast<Delivery*>(raw));
                                 auto& operation = delivery->operation_;
                                 operation->Complete(DeliverLocal(
                                     *operation->channel_, *operation->payload_,
                                     operation->encoded_));
                               },
                       });
    }
  }

  Worker* origin_ = nullptr;
  std::atomic<unsigned> remaining_;
  std::atomic<std::uint64_t> receivers_{0};
  std::coroutine_handle<> handle_{};
  std::shared_ptr<const std::string> channel_;
  std::shared_ptr<const std::string> payload_;
  std::shared_ptr<const std::string> encoded_;
};

}  // namespace

bool PubSubSession::Subscribe(std::string_view channel) {
  auto [found, inserted] = channels_.insert(std::string(channel));
  if (!inserted) return false;
  channel_order_.push_back(*found);
  registry_->channels_[*found].push_back(shared_from_this());
  return true;
}

void PubSubSession::RemoveFromChannel(std::string_view channel) {
  auto found = registry_->channels_.find(channel);
  if (found == registry_->channels_.end()) return;
  std::erase(found->second, shared_from_this());
  if (found->second.empty()) registry_->channels_.erase(found);
}

bool PubSubSession::Unsubscribe(std::string_view channel) {
  auto found = channels_.find(channel);
  if (found == channels_.end()) return false;
  const std::string owned = *found;
  RemoveFromChannel(owned);
  channels_.erase(found);
  std::erase(channel_order_, owned);
  return true;
}

void PubSubSession::UnsubscribeAll() {
  const std::vector<std::string> channels = channel_order_;
  for (const std::string& channel : channels) {
    (void)Unsubscribe(channel);
  }
}

bool PubSubSession::PSubscribe(std::string_view pattern) {
  auto [found, inserted] = patterns_.insert(std::string(pattern));
  if (!inserted) return false;
  pattern_order_.push_back(*found);
  registry_->patterns_[*found].push_back(shared_from_this());
  return true;
}

void PubSubSession::RemoveFromPattern(std::string_view pattern) {
  auto found = registry_->patterns_.find(pattern);
  if (found == registry_->patterns_.end()) return;
  std::erase(found->second, shared_from_this());
  if (found->second.empty()) registry_->patterns_.erase(found);
}

bool PubSubSession::PUnsubscribe(std::string_view pattern) {
  auto found = patterns_.find(pattern);
  if (found == patterns_.end()) return false;
  const std::string owned = *found;
  RemoveFromPattern(owned);
  patterns_.erase(found);
  std::erase(pattern_order_, owned);
  return true;
}

void PubSubSession::PUnsubscribeAll() {
  const std::vector<std::string> patterns = pattern_order_;
  for (const std::string& pattern : patterns) {
    (void)PUnsubscribe(pattern);
  }
}

bool PubSubSession::Enqueue(const std::shared_ptr<const std::string>& encoded) {
  if (closed_ || exit_enqueued_) return false;
  const std::size_t bytes = encoded->size();
  if (queue_.size() >= kPubSubQueueLimit ||
      bytes > kWorkerPubSubBufferLimit - std::min(registry_->pending_bytes_,
                                                  kWorkerPubSubBufferLimit)) {
    closed_ = true;
    output_ready_.NotifyAll(*worker_);
    (void)::shutdown(fd_, SHUT_RDWR);
    return false;
  }
  queue_.push_back(QueuedFrame{.encoded_ = encoded});
  pending_bytes_ += bytes;
  registry_->pending_bytes_ += bytes;
  output_ready_.NotifyAll(*worker_);
  return true;
}

void PubSubSession::EnqueueExit() {
  if (closed_ || exit_enqueued_) return;
  exit_enqueued_ = true;
  queue_.push_back(QueuedFrame{.encoded_ = nullptr, .exit_ = true});
  output_ready_.NotifyAll(*worker_);
}

Task<QueuedFrame> PubSubSession::Next() {
  while (queue_.empty() && !closed_) {
    co_await output_ready_.Wait();
  }
  if (closed_ || queue_.empty()) co_return QueuedFrame{};
  QueuedFrame frame = std::move(queue_.front());
  queue_.pop_front();
  if (frame.encoded_ != nullptr) {
    assert(pending_bytes_ >= frame.encoded_->size());
    assert(registry_->pending_bytes_ >= frame.encoded_->size());
    pending_bytes_ -= frame.encoded_->size();
    registry_->pending_bytes_ -= frame.encoded_->size();
  }
  co_return frame;
}

void PreparePubSub(unsigned worker_count) {
  g_worker_count = worker_count;
  g_registries = std::make_unique<WorkerPubSubRegistry[]>(worker_count);
}

std::shared_ptr<PubSubSession> RegisterPubSubSession(int fd) {
  return std::make_shared<PubSubSession>(&LocalRegistry(), ThisWorker().self_,
                                         fd);
}

void UnregisterPubSubSession(const std::shared_ptr<PubSubSession>& session) {
  if (session == nullptr || !session->registered_) return;
  session->registered_ = false;
  session->Close();
  session->UnsubscribeAll();
  session->PUnsubscribeAll();
  assert(session->registry_->pending_bytes_ >= session->pending_bytes_);
  session->registry_->pending_bytes_ -= session->pending_bytes_;
  session->pending_bytes_ = 0;
  session->queue_.clear();
}

std::size_t PubSubSubscriptionCount(
    const std::shared_ptr<PubSubSession>& session) noexcept {
  return session == nullptr ? 0 : session->subscription_count();
}

std::string SubscribeChannels(const std::shared_ptr<PubSubSession>& session,
                              std::span<const std::string> channels) {
  ReplyBuilder builder;
  for (const std::string& channel : channels) {
    (void)session->Subscribe(channel);
    AppendSubscriptionFrame(&builder, "subscribe", &channel,
                            session->subscription_count());
  }
  return std::move(builder).Release();
}

std::string UnsubscribeChannels(const std::shared_ptr<PubSubSession>& session,
                                std::span<const std::string> channels) {
  ReplyBuilder builder;
  if (channels.empty()) {
    const std::vector<std::string> current = session->channel_order();
    if (current.empty()) {
      AppendSubscriptionFrame(&builder, "unsubscribe", nullptr,
                              session->subscription_count());
      return std::move(builder).Release();
    }
    for (const std::string& channel : current) {
      (void)session->Unsubscribe(channel);
      AppendSubscriptionFrame(&builder, "unsubscribe", &channel,
                              session->subscription_count());
    }
    return std::move(builder).Release();
  }
  for (const std::string& channel : channels) {
    (void)session->Unsubscribe(channel);
    AppendSubscriptionFrame(&builder, "unsubscribe", &channel,
                            session->subscription_count());
  }
  return std::move(builder).Release();
}

std::string PSubscribePatterns(const std::shared_ptr<PubSubSession>& session,
                               std::span<const std::string> patterns) {
  ReplyBuilder builder;
  for (const std::string& pattern : patterns) {
    (void)session->PSubscribe(pattern);
    AppendSubscriptionFrame(&builder, "psubscribe", &pattern,
                            session->subscription_count());
  }
  return std::move(builder).Release();
}

std::string PUnsubscribePatterns(const std::shared_ptr<PubSubSession>& session,
                                 std::span<const std::string> patterns) {
  ReplyBuilder builder;
  if (patterns.empty()) {
    const std::vector<std::string> current = session->pattern_order();
    if (current.empty()) {
      AppendSubscriptionFrame(&builder, "punsubscribe", nullptr,
                              session->subscription_count());
      return std::move(builder).Release();
    }
    for (const std::string& pattern : current) {
      (void)session->PUnsubscribe(pattern);
      AppendSubscriptionFrame(&builder, "punsubscribe", &pattern,
                              session->subscription_count());
    }
    return std::move(builder).Release();
  }
  for (const std::string& pattern : patterns) {
    (void)session->PUnsubscribe(pattern);
    AppendSubscriptionFrame(&builder, "punsubscribe", &pattern,
                            session->subscription_count());
  }
  return std::move(builder).Release();
}

void ResetPubSubSubscriptions(const std::shared_ptr<PubSubSession>& session) {
  if (session == nullptr) return;
  session->UnsubscribeAll();
  session->PUnsubscribeAll();
}

Task<std::vector<std::string>> PubSubChannels(
    std::optional<std::string> pattern) {
  absl::flat_hash_set<std::string> unique;
  for (unsigned worker = 0; worker < g_worker_count; ++worker) {
    auto collect = [pattern]() -> Task<std::vector<std::string>> {
      std::vector<std::string> local;
      for (const auto& [channel, sessions] : LocalRegistry().channels_) {
        if (!sessions.empty() &&
            (!pattern.has_value() || RedisGlobMatch(*pattern, channel))) {
          local.push_back(channel);
        }
      }
      co_return local;
    };
    std::vector<std::string> local;
    if (worker == ThisWorker().id_) {
      local = co_await collect();
    } else {
      local = co_await SubmitTaskTo(worker, collect);
    }
    unique.insert(std::make_move_iterator(local.begin()),
                  std::make_move_iterator(local.end()));
  }
  std::vector<std::string> channels(unique.begin(), unique.end());
  std::sort(channels.begin(), channels.end());
  co_return channels;
}

Task<std::vector<std::uint64_t>> PubSubNumSub(
    std::span<const std::string> channels) {
  std::vector<std::string> owned(channels.begin(), channels.end());
  std::vector<std::uint64_t> counts(owned.size(), 0);
  for (unsigned worker = 0; worker < g_worker_count; ++worker) {
    auto collect = [owned]() -> Task<std::vector<std::uint64_t>> {
      std::vector<std::uint64_t> local(owned.size(), 0);
      for (std::size_t index = 0; index < owned.size(); ++index) {
        auto found = LocalRegistry().channels_.find(owned[index]);
        if (found != LocalRegistry().channels_.end()) {
          local[index] = found->second.size();
        }
      }
      co_return local;
    };
    std::vector<std::uint64_t> local;
    if (worker == ThisWorker().id_) {
      local = co_await collect();
    } else {
      local = co_await SubmitTaskTo(worker, collect);
    }
    for (std::size_t index = 0; index < counts.size(); ++index) {
      counts[index] += local[index];
    }
  }
  co_return counts;
}

Task<std::uint64_t> PubSubNumPat() {
  absl::flat_hash_set<std::string> unique;
  for (unsigned worker = 0; worker < g_worker_count; ++worker) {
    auto collect = []() -> Task<std::vector<std::string>> {
      std::vector<std::string> local;
      local.reserve(LocalRegistry().patterns_.size());
      for (const auto& [pattern, sessions] : LocalRegistry().patterns_) {
        if (!sessions.empty()) local.push_back(pattern);
      }
      co_return local;
    };
    std::vector<std::string> local;
    if (worker == ThisWorker().id_) {
      local = co_await collect();
    } else {
      local = co_await SubmitTaskTo(worker, collect);
    }
    unique.insert(std::make_move_iterator(local.begin()),
                  std::make_move_iterator(local.end()));
  }
  co_return unique.size();
}

Task<std::uint64_t> PublishChannel(std::string_view channel,
                                   std::string_view payload) {
  auto owned_channel = std::make_shared<const std::string>(channel);
  auto owned_payload = std::make_shared<const std::string>(payload);
  auto operation = std::make_shared<PublishOperation>(
      ThisWorker().self_, g_worker_count, owned_channel, owned_payload,
      EncodeMessage(channel, payload));
  co_return co_await operation->Wait();
}

void EnqueuePubSubReply(const std::shared_ptr<PubSubSession>& session,
                        std::string encoded) {
  (void)session->Enqueue(
      std::make_shared<const std::string>(std::move(encoded)));
}

void ExitPubSubMode(const std::shared_ptr<PubSubSession>& session) {
  session->EnqueueExit();
}

void ClosePubSubSession(const std::shared_ptr<PubSubSession>& session) {
  if (session != nullptr) session->Close();
}

void MarkPubSubReaderStarted(const std::shared_ptr<PubSubSession>& session) {
  session->ReaderStarted();
}

void MarkPubSubReaderDone(const std::shared_ptr<PubSubSession>& session) {
  session->ReaderDone();
}

Task<absl::Status> WaitPubSubReaderDone(
    const std::shared_ptr<PubSubSession>& session) {
  co_return co_await session->WaitReaderDone();
}

Task<absl::Status> StreamPubSubMessages(
    TcpStream& stream, const std::shared_ptr<PubSubSession>& session) {
  while (stream.IsOpen()) {
    QueuedFrame frame = co_await session->Next();
    if (frame.exit_) co_return absl::OkStatus();
    if (frame.encoded_ == nullptr) co_return absl::OkStatus();
    absl::Status written = co_await stream.WriteAll(std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(frame.encoded_->data()),
        frame.encoded_->size()));
    if (!written.ok()) {
      session->Close();
      co_return written;
    }
  }
  co_return absl::OkStatus();
}

}  // namespace keylane
