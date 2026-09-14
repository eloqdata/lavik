#include "keylane/pubsub.h"

#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
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
#include "keylane/memory.h"
#include "keylane/resp.h"

namespace keylane {
using namespace celer;

namespace {

constexpr std::size_t kWorkerPubSubBufferLimit = 128ULL * 1024 * 1024;
constexpr std::size_t kPubSubQueueLimit = 10'000;
std::atomic<std::uint64_t> g_resp2_subscribers{0};
std::atomic<std::uint64_t> g_resp3_subscribers{0};

std::atomic<std::uint64_t>& SubscriberCount(RespVersion version) {
  return version == RespVersion::k3 ? g_resp3_subscribers : g_resp2_subscribers;
}

struct WorkerPubSubRegistry;

struct QueuedFrame {
  std::shared_ptr<const std::string> encoded_;
  bool exit_ = false;
};

struct EncodedFrames {
  std::shared_ptr<const std::string> resp2_;
  std::shared_ptr<const std::string> resp3_;
};

}  // namespace

class PubSubSession : public std::enable_shared_from_this<PubSubSession> {
 public:
  PubSubSession(WorkerPubSubRegistry* registry, Worker* worker, int fd,
                RespVersion version)
      : registry_(registry), worker_(worker), fd_(fd), version_(version) {}

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
  bool live() const noexcept {
    return registered_ && !closed_ && !exit_enqueued_;
  }
  RespVersion version() const noexcept { return version_; }
  void SetVersion(RespVersion version) noexcept {
    if (version == version_) return;
    if (subscription_count() != 0) {
      SubscriberCount(version_).fetch_sub(1, std::memory_order_relaxed);
      SubscriberCount(version).fetch_add(1, std::memory_order_relaxed);
    }
    version_ = version;
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
  RespVersion version_ = RespVersion::k2;
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

// This command-time snapshot intentionally keeps only weak session references:
// delaying publication must not extend a disconnected connection's lifetime.
// Frames retain the RESP version observed at PUBLISH, while the destination
// worker remains the sole owner allowed to inspect or enqueue to each session.
// Per-worker charges bound snapshots that accumulate across a large EXEC.
class CapturedPubSubPublication {
 public:
  struct Recipient {
    std::weak_ptr<PubSubSession> session_;
    std::shared_ptr<const std::string> encoded_;
  };

  explicit CapturedPubSubPublication(unsigned worker_count)
      : per_worker_(worker_count), charges_(worker_count) {}

  std::vector<std::vector<Recipient>> per_worker_;
  std::vector<RetainedMemoryCharge> charges_;
  std::atomic<std::uint64_t> receiver_count_{0};
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
  builder->AppendPushHeader(3);
  builder->AppendBulkString(kind);
  if (channel == nullptr) {
    builder->AppendNull();
  } else {
    builder->AppendBulkString(*channel);
  }
  builder->AppendInteger(static_cast<long long>(subscription_count));
}

std::shared_ptr<const std::string> EncodeMessage(RespVersion version,
                                                 std::string_view channel,
                                                 std::string_view payload) {
  ReplyBuilder builder(version);
  builder.Reserve(channel.size() + payload.size() + 48);
  builder.AppendPushHeader(3);
  builder.AppendBulkString("message");
  builder.AppendBulkString(channel);
  builder.AppendBulkString(payload);
  return std::make_shared<const std::string>(std::move(builder).Release());
}

std::shared_ptr<const std::string> EncodePatternMessage(
    RespVersion version, std::string_view pattern, std::string_view channel,
    std::string_view payload) {
  ReplyBuilder builder(version);
  builder.Reserve(pattern.size() + channel.size() + payload.size() + 64);
  builder.AppendPushHeader(4);
  builder.AppendBulkString("pmessage");
  builder.AppendBulkString(pattern);
  builder.AppendBulkString(channel);
  builder.AppendBulkString(payload);
  return std::make_shared<const std::string>(std::move(builder).Release());
}

std::shared_ptr<const EncodedFrames> EncodeMessages(std::string_view channel,
                                                    std::string_view payload) {
  const bool need_resp2 =
      g_resp2_subscribers.load(std::memory_order_relaxed) != 0;
  const bool need_resp3 =
      g_resp3_subscribers.load(std::memory_order_relaxed) != 0;
  return std::make_shared<const EncodedFrames>(EncodedFrames{
      .resp2_ = need_resp2 ? EncodeMessage(RespVersion::k2, channel, payload)
                           : nullptr,
      .resp3_ = need_resp3 ? EncodeMessage(RespVersion::k3, channel, payload)
                           : nullptr,
  });
}

const std::shared_ptr<const std::string>& SelectFrame(
    const EncodedFrames& frames, RespVersion version) {
  return version == RespVersion::k3 ? frames.resp3_ : frames.resp2_;
}

std::uint64_t DeliverLocal(
    std::string_view channel, std::string_view payload,
    const std::shared_ptr<const EncodedFrames>& encoded) {
  WorkerPubSubRegistry& registry = LocalRegistry();
  std::uint64_t receivers = 0;
  if (auto found = registry.channels_.find(channel);
      found != registry.channels_.end()) {
    for (const auto& session : found->second) {
      // A notification posted before UNSUBSCRIBE may run afterward.
      // Membership is checked at the final worker-local enqueue point.
      const auto& frame = SelectFrame(*encoded, session->version());
      if (frame != nullptr && session->subscribed_to(channel) &&
          session->Enqueue(frame)) {
        ++receivers;
      }
    }
  }
  for (const auto& [pattern, sessions] : registry.patterns_) {
    if (!RedisGlobMatch(pattern, channel)) continue;
    const EncodedFrames pattern_message{
        .resp2_ = encoded->resp2_ == nullptr
                      ? nullptr
                      : EncodePatternMessage(RespVersion::k2, pattern, channel,
                                             payload),
        .resp3_ = encoded->resp3_ == nullptr
                      ? nullptr
                      : EncodePatternMessage(RespVersion::k3, pattern, channel,
                                             payload),
    };
    for (const auto& session : sessions) {
      const auto& frame = SelectFrame(pattern_message, session->version());
      if (frame != nullptr && session->subscribed_to_pattern(pattern) &&
          session->Enqueue(frame)) {
        ++receivers;
      }
    }
  }
  return receivers;
}

struct LocalPubSubCapture {
  std::vector<CapturedPubSubPublication::Recipient> recipients_;
  RetainedMemoryCharge charge_;
};

bool AddCaptureBytes(std::size_t increment, std::size_t* bytes) {
  if (increment > std::numeric_limits<std::size_t>::max() - *bytes) {
    return false;
  }
  *bytes += increment;
  return true;
}

absl::StatusOr<LocalPubSubCapture> CaptureLocal(
    std::string_view channel, std::string_view payload,
    const std::shared_ptr<const EncodedFrames>& encoded) {
  WorkerPubSubRegistry& registry = LocalRegistry();
  std::size_t recipient_count = 0;
  std::size_t retained_bytes = 128;
  bool exact_resp2 = false;
  bool exact_resp3 = false;
  if (auto found = registry.channels_.find(channel);
      found != registry.channels_.end()) {
    for (const auto& session : found->second) {
      const auto& frame = SelectFrame(*encoded, session->version());
      if (frame != nullptr && session->live() &&
          session->subscribed_to(channel)) {
        ++recipient_count;
        if (session->version() == RespVersion::k3)
          exact_resp3 = true;
        else
          exact_resp2 = true;
      }
    }
  }
  const auto add_encoded_frame = [&](std::size_t pattern_bytes) {
    std::size_t bytes = channel.size();
    if (!AddCaptureBytes(payload.size(), &bytes) ||
        !AddCaptureBytes(pattern_bytes, &bytes) ||
        !AddCaptureBytes(128, &bytes)) {
      return false;
    }
    return AddCaptureBytes(bytes, &retained_bytes);
  };
  if ((exact_resp2 && !add_encoded_frame(0)) ||
      (exact_resp3 && !add_encoded_frame(0))) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM deferred Pub/Sub snapshot size overflow");
  }
  for (const auto& [pattern, sessions] : registry.patterns_) {
    if (!RedisGlobMatch(pattern, channel)) continue;
    bool pattern_resp2 = false;
    bool pattern_resp3 = false;
    for (const auto& session : sessions) {
      const auto& frame = SelectFrame(*encoded, session->version());
      if (frame != nullptr && session->live() &&
          session->subscribed_to_pattern(pattern)) {
        ++recipient_count;
        if (session->version() == RespVersion::k3)
          pattern_resp3 = true;
        else
          pattern_resp2 = true;
      }
    }
    if ((pattern_resp2 && !add_encoded_frame(pattern.size())) ||
        (pattern_resp3 && !add_encoded_frame(pattern.size()))) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM deferred Pub/Sub snapshot size overflow");
    }
  }
  if (recipient_count == 0) return LocalPubSubCapture{};
  if (recipient_count > std::numeric_limits<std::size_t>::max() /
                            sizeof(CapturedPubSubPublication::Recipient) ||
      !AddCaptureBytes(
          recipient_count * sizeof(CapturedPubSubPublication::Recipient),
          &retained_bytes)) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM deferred Pub/Sub snapshot size overflow");
  }
  auto reservation = TryReserveMemory(retained_bytes);
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM preparing deferred Pub/Sub snapshot");
  }

  try {
    LocalPubSubCapture capture;
    capture.recipients_.reserve(recipient_count);
    if (auto found = registry.channels_.find(channel);
        found != registry.channels_.end()) {
      for (const auto& session : found->second) {
        const auto& frame = SelectFrame(*encoded, session->version());
        if (frame != nullptr && session->live() &&
            session->subscribed_to(channel)) {
          capture.recipients_.push_back(
              {.session_ = session, .encoded_ = frame});
        }
      }
    }
    for (const auto& [pattern, sessions] : registry.patterns_) {
      if (!RedisGlobMatch(pattern, channel)) continue;
      bool pattern_resp2 = false;
      bool pattern_resp3 = false;
      for (const auto& session : sessions) {
        if (!session->live() || !session->subscribed_to_pattern(pattern)) {
          continue;
        }
        if (session->version() == RespVersion::k3)
          pattern_resp3 = true;
        else
          pattern_resp2 = true;
      }
      const EncodedFrames pattern_message{
          .resp2_ = pattern_resp2
                        ? EncodePatternMessage(RespVersion::k2, pattern,
                                               channel, payload)
                        : nullptr,
          .resp3_ = pattern_resp3
                        ? EncodePatternMessage(RespVersion::k3, pattern,
                                               channel, payload)
                        : nullptr,
      };
      for (const auto& session : sessions) {
        const auto& frame = SelectFrame(pattern_message, session->version());
        if (frame != nullptr && session->live() &&
            session->subscribed_to_pattern(pattern)) {
          capture.recipients_.push_back(
              {.session_ = session, .encoded_ = frame});
        }
      }
    }
    assert(capture.recipients_.size() == recipient_count);
    capture.charge_.Adopt(&*reservation, retained_bytes);
    return capture;
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM allocating deferred Pub/Sub snapshot");
  }
}

class CapturePubSubOperation
    : public std::enable_shared_from_this<CapturePubSubOperation> {
 public:
  CapturePubSubOperation(Worker* origin, unsigned participants,
                         std::shared_ptr<const std::string> channel,
                         std::shared_ptr<const std::string> payload,
                         std::shared_ptr<const EncodedFrames> encoded)
      : origin_(origin),
        remaining_(participants),
        channel_(std::move(channel)),
        payload_(std::move(payload)),
        encoded_(std::move(encoded)),
        publication_(std::make_shared<CapturedPubSubPublication>(participants)),
        errors_(participants, absl::OkStatus()) {}

  class Awaiter {
   public:
    explicit Awaiter(std::shared_ptr<CapturePubSubOperation> operation)
        : operation_(std::move(operation)) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> handle) {
      operation_->handle_ = handle;
      operation_->Dispatch();
      return true;
    }
    absl::StatusOr<std::shared_ptr<CapturedPubSubPublication>> await_resume()
        const {
      for (const absl::Status& error : operation_->errors_) {
        if (!error.ok()) return error;
      }
      return operation_->publication_;
    }

   private:
    std::shared_ptr<CapturePubSubOperation> operation_;
  };

  Awaiter Wait() { return Awaiter(shared_from_this()); }

  void Complete(unsigned worker,
                absl::StatusOr<LocalPubSubCapture> capture) noexcept {
    if (!capture.ok()) {
      errors_[worker] = capture.status();
    } else {
      publication_->receiver_count_.fetch_add(capture->recipients_.size(),
                                              std::memory_order_relaxed);
      publication_->per_worker_[worker] = std::move(capture->recipients_);
      publication_->charges_[worker] = std::move(capture->charge_);
    }
    if (remaining_.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
    if (ThisWorker().id_ == origin_->id()) {
      origin_->Enqueue(handle_);
      return;
    }
    auto context = std::make_unique<std::shared_ptr<CapturePubSubOperation>>(
        shared_from_this());
    PostNotification(
        ThisWorker().cross_core_, origin_->id(),
        RemoteNotification{
            .context_ = context.release(),
            .value_ = 0,
            .run_fn_ =
                [](void* raw, std::uint64_t) noexcept {
                  std::unique_ptr<std::shared_ptr<CapturePubSubOperation>>
                      operation(
                          static_cast<std::shared_ptr<CapturePubSubOperation>*>(
                              raw));
                  (*operation)->origin_->Enqueue((*operation)->handle_);
                },
        });
  }

 private:
  struct Capture {
    std::shared_ptr<CapturePubSubOperation> operation_;
    unsigned worker_ = 0;
  };

  void Dispatch() {
    const CurrentWorker& current = ThisWorker();
    for (unsigned worker = 0; worker < g_worker_count; ++worker) {
      if (worker == current.id_) {
        Complete(worker, CaptureLocal(*channel_, *payload_, encoded_));
        continue;
      }
      auto capture = std::make_unique<Capture>(
          Capture{.operation_ = shared_from_this(), .worker_ = worker});
      PostNotification(
          current.cross_core_, worker,
          RemoteNotification{
              .context_ = capture.release(),
              .value_ = 0,
              .run_fn_ =
                  [](void* raw, std::uint64_t) noexcept {
                    std::unique_ptr<Capture> capture(
                        static_cast<Capture*>(raw));
                    auto& operation = capture->operation_;
                    operation->Complete(
                        capture->worker_,
                        CaptureLocal(*operation->channel_, *operation->payload_,
                                     operation->encoded_));
                  },
          });
    }
  }

  Worker* origin_ = nullptr;
  std::atomic<unsigned> remaining_;
  std::coroutine_handle<> handle_{};
  std::shared_ptr<const std::string> channel_;
  std::shared_ptr<const std::string> payload_;
  std::shared_ptr<const EncodedFrames> encoded_;
  std::shared_ptr<CapturedPubSubPublication> publication_;
  std::vector<absl::Status> errors_;
};

class DeliverCapturedPubSubOperation
    : public std::enable_shared_from_this<DeliverCapturedPubSubOperation> {
 public:
  DeliverCapturedPubSubOperation(
      Worker* origin, unsigned participants,
      std::shared_ptr<CapturedPubSubPublication> publication)
      : origin_(origin),
        remaining_(participants),
        publication_(std::move(publication)) {}

  class Awaiter {
   public:
    explicit Awaiter(std::shared_ptr<DeliverCapturedPubSubOperation> operation)
        : operation_(std::move(operation)) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> handle) {
      operation_->handle_ = handle;
      operation_->Dispatch();
      return true;
    }
    void await_resume() const noexcept {}

   private:
    std::shared_ptr<DeliverCapturedPubSubOperation> operation_;
  };

  Awaiter Wait() { return Awaiter(shared_from_this()); }

  void Complete() noexcept {
    if (remaining_.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
    if (ThisWorker().id_ == origin_->id()) {
      origin_->Enqueue(handle_);
      return;
    }
    auto context =
        std::make_unique<std::shared_ptr<DeliverCapturedPubSubOperation>>(
            shared_from_this());
    PostNotification(
        ThisWorker().cross_core_, origin_->id(),
        RemoteNotification{
            .context_ = context.release(),
            .value_ = 0,
            .run_fn_ =
                [](void* raw, std::uint64_t) noexcept {
                  std::unique_ptr<
                      std::shared_ptr<DeliverCapturedPubSubOperation>>
                      operation(
                          static_cast<
                              std::shared_ptr<DeliverCapturedPubSubOperation>*>(
                              raw));
                  (*operation)->origin_->Enqueue((*operation)->handle_);
                },
        });
  }

 private:
  struct Delivery {
    std::shared_ptr<DeliverCapturedPubSubOperation> operation_;
    unsigned worker_ = 0;
  };

  void DeliverLocalSnapshot(unsigned worker) {
    for (const auto& recipient : publication_->per_worker_[worker]) {
      if (auto session = recipient.session_.lock(); session != nullptr) {
        // Membership is intentionally not rechecked: it was frozen at the
        // PUBLISH position. Enqueue still rejects dead or backpressured
        // sessions through the ordinary bounded-delivery path.
        (void)session->Enqueue(recipient.encoded_);
      }
    }
  }

  void Dispatch() {
    const CurrentWorker& current = ThisWorker();
    for (unsigned worker = 0; worker < g_worker_count; ++worker) {
      if (worker == current.id_) {
        DeliverLocalSnapshot(worker);
        Complete();
        continue;
      }
      auto delivery = std::make_unique<Delivery>(
          Delivery{.operation_ = shared_from_this(), .worker_ = worker});
      PostNotification(current.cross_core_, worker,
                       RemoteNotification{
                           .context_ = delivery.release(),
                           .value_ = 0,
                           .run_fn_ =
                               [](void* raw, std::uint64_t) noexcept {
                                 std::unique_ptr<Delivery> delivery(
                                     static_cast<Delivery*>(raw));
                                 delivery->operation_->DeliverLocalSnapshot(
                                     delivery->worker_);
                                 delivery->operation_->Complete();
                               },
                       });
    }
  }

  Worker* origin_ = nullptr;
  std::atomic<unsigned> remaining_;
  std::coroutine_handle<> handle_{};
  std::shared_ptr<CapturedPubSubPublication> publication_;
};

class PublishOperation : public std::enable_shared_from_this<PublishOperation> {
 public:
  PublishOperation(Worker* origin, unsigned participants,
                   std::shared_ptr<const std::string> channel,
                   std::shared_ptr<const std::string> payload,
                   std::shared_ptr<const EncodedFrames> encoded)
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
  std::shared_ptr<const EncodedFrames> encoded_;
};

}  // namespace

bool PubSubSession::Subscribe(std::string_view channel) {
  const bool was_empty = subscription_count() == 0;
  auto [found, inserted] = channels_.insert(std::string(channel));
  if (!inserted) return false;
  channel_order_.push_back(*found);
  registry_->channels_[*found].push_back(shared_from_this());
  if (was_empty)
    SubscriberCount(version_).fetch_add(1, std::memory_order_relaxed);
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
  if (subscription_count() == 0)
    SubscriberCount(version_).fetch_sub(1, std::memory_order_relaxed);
  return true;
}

void PubSubSession::UnsubscribeAll() {
  const std::vector<std::string> channels = channel_order_;
  for (const std::string& channel : channels) {
    (void)Unsubscribe(channel);
  }
}

bool PubSubSession::PSubscribe(std::string_view pattern) {
  const bool was_empty = subscription_count() == 0;
  auto [found, inserted] = patterns_.insert(std::string(pattern));
  if (!inserted) return false;
  pattern_order_.push_back(*found);
  registry_->patterns_[*found].push_back(shared_from_this());
  if (was_empty)
    SubscriberCount(version_).fetch_add(1, std::memory_order_relaxed);
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
  if (subscription_count() == 0)
    SubscriberCount(version_).fetch_sub(1, std::memory_order_relaxed);
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
  g_resp2_subscribers.store(0, std::memory_order_relaxed);
  g_resp3_subscribers.store(0, std::memory_order_relaxed);
  g_worker_count = worker_count;
  g_registries = std::make_unique<WorkerPubSubRegistry[]>(worker_count);
}

void SetPubSubRespVersion(const std::shared_ptr<PubSubSession>& session,
                          RespVersion version) {
  if (session != nullptr) session->SetVersion(version);
}

std::shared_ptr<PubSubSession> RegisterPubSubSession(int fd,
                                                     RespVersion version) {
  return std::make_shared<PubSubSession>(&LocalRegistry(), ThisWorker().self_,
                                         fd, version);
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

std::size_t PubSubPatternSubscriptionCount(
    const std::shared_ptr<PubSubSession>& session) noexcept {
  return session == nullptr ? 0 : session->pattern_order().size();
}

std::string SubscribeChannels(const std::shared_ptr<PubSubSession>& session,
                              std::span<const std::string> channels) {
  ReplyBuilder builder(session->version());
  for (const std::string& channel : channels) {
    (void)session->Subscribe(channel);
    AppendSubscriptionFrame(&builder, "subscribe", &channel,
                            session->subscription_count());
  }
  return std::move(builder).Release();
}

std::string UnsubscribeChannels(const std::shared_ptr<PubSubSession>& session,
                                std::span<const std::string> channels) {
  ReplyBuilder builder(session->version());
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
  ReplyBuilder builder(session->version());
  for (const std::string& pattern : patterns) {
    (void)session->PSubscribe(pattern);
    AppendSubscriptionFrame(&builder, "psubscribe", &pattern,
                            session->subscription_count());
  }
  return std::move(builder).Release();
}

std::string PUnsubscribePatterns(const std::shared_ptr<PubSubSession>& session,
                                 std::span<const std::string> patterns) {
  ReplyBuilder builder(session->version());
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
      EncodeMessages(channel, payload));
  co_return co_await operation->Wait();
}

Task<absl::StatusOr<std::shared_ptr<CapturedPubSubPublication>>>
CapturePubSubPublication(std::string_view channel, std::string_view payload) {
  auto owned_channel = std::make_shared<const std::string>(channel);
  auto owned_payload = std::make_shared<const std::string>(payload);
  auto operation = std::make_shared<CapturePubSubOperation>(
      ThisWorker().self_, g_worker_count, owned_channel, owned_payload,
      EncodeMessages(channel, payload));
  co_return co_await operation->Wait();
}

std::uint64_t CapturedPubSubReceiverCount(
    const std::shared_ptr<CapturedPubSubPublication>& publication) noexcept {
  return publication == nullptr
             ? 0
             : publication->receiver_count_.load(std::memory_order_acquire);
}

Task<absl::Status> DeliverCapturedPubSubPublication(
    std::shared_ptr<CapturedPubSubPublication> publication) {
  if (publication == nullptr) co_return absl::OkStatus();
  auto operation = std::make_shared<DeliverCapturedPubSubOperation>(
      ThisWorker().self_, g_worker_count, std::move(publication));
  co_await operation->Wait();
  co_return absl::OkStatus();
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
  return session->WaitReaderDone();
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
