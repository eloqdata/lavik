#include "keylane/monitor.h"

#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/command.h"
#include "keylane/command_table.h"
#include "keylane/session.h"

namespace keylane {
using namespace celer;

namespace {

// These protect a worker from aggregate queued monitor output and a single
// connection from an excessive number of tiny messages. They intentionally
// mirror the two-dimensional shape of Keylane's other bounded queues.
constexpr std::size_t kWorkerMonitorBufferLimit = 128ULL * 1024 * 1024;
constexpr std::size_t kMonitorQueueLimit = 10'000;
constexpr auto kPeerCheckInterval = std::chrono::milliseconds(250);

struct WorkerMonitorRegistry;

std::unique_ptr<WorkerMonitorRegistry[]> g_registries;
unsigned g_worker_count = 0;
std::atomic<unsigned> g_monitor_count{0};

}  // namespace

class MonitorSession {
 public:
  MonitorSession(WorkerMonitorRegistry* registry, Worker* worker, int fd)
      : registry_(registry), worker_(worker), fd_(fd) {}

  void Enqueue(const std::shared_ptr<const std::string>& message);
  Task<std::shared_ptr<const std::string>> Next();

  void Close() noexcept {
    if (closed_) return;
    closed_ = true;
    ready_.NotifyAll(*worker_);
  }

  void Stop() noexcept {
    active_ = false;
    Close();
  }

  bool active() const noexcept { return active_; }
  int fd() const noexcept { return fd_; }

 private:
  friend void UnregisterMonitorSession(
      const std::shared_ptr<MonitorSession>& session);

  WorkerMonitorRegistry* registry_ = nullptr;
  Worker* worker_ = nullptr;
  int fd_ = -1;
  std::deque<std::shared_ptr<const std::string>> queue_;
  std::size_t pending_bytes_ = 0;
  AsyncNotification ready_;
  bool active_ = true;
  bool closed_ = false;
  bool registered_ = true;
};

namespace {

struct WorkerMonitorRegistry {
  std::vector<std::shared_ptr<MonitorSession>> sessions_;
  std::size_t pending_bytes_ = 0;
};

WorkerMonitorRegistry& LocalRegistry() {
  assert(g_registries != nullptr);
  assert(ThisWorker().id_ < g_worker_count);
  return g_registries[ThisWorker().id_];
}

void AppendQuoted(std::string_view input, std::string* output) {
  constexpr char kHex[] = "0123456789abcdef";
  output->push_back('"');
  for (const unsigned char value : input) {
    switch (value) {
      case '\\':
        output->append("\\\\");
        break;
      case '"':
        output->append("\\\"");
        break;
      case '\n':
        output->append("\\n");
        break;
      case '\r':
        output->append("\\r");
        break;
      case '\t':
        output->append("\\t");
        break;
      case '\a':
        output->append("\\a");
        break;
      case '\b':
        output->append("\\b");
        break;
      default:
        if (value >= 0x20 && value <= 0x7e) {
          output->push_back(static_cast<char>(value));
        } else {
          output->append("\\x");
          output->push_back(kHex[value >> 4]);
          output->push_back(kHex[value & 0x0f]);
        }
        break;
    }
  }
  output->push_back('"');
}

bool ValidMonitorCommand(const CommandRequest& request) {
  const CommandSpec* spec = request.spec_;
  if (spec == nullptr || (spec->flags_ & (kCmdAdmin | kCmdSkipMonitor)) != 0) {
    return false;
  }
  const std::size_t argc = request.args_.size();
  return argc >= spec->min_args_ &&
         (spec->max_args_ == 0 || argc <= spec->max_args_);
}

void DeliverMonitorMessage(const std::shared_ptr<const std::string>& message) {
  // Registry membership and session queues are worker-local. Cross-worker
  // notifications arrive here before touching either, so no mutex is needed.
  for (const auto& session : LocalRegistry().sessions_) {
    session->Enqueue(message);
  }
}

void RunMonitorDelivery(void* context, std::uint64_t) noexcept {
  std::unique_ptr<std::shared_ptr<const std::string>> message(
      static_cast<std::shared_ptr<const std::string>*>(context));
  DeliverMonitorMessage(*message);
}

Task<absl::Status> WatchMonitorPeer(std::shared_ptr<MonitorSession> session) {
  while (session->active()) {
    pollfd descriptor{
        .fd = session->fd(),
        .events = static_cast<short>(POLLERR | POLLHUP | POLLRDHUP),
        .revents = 0,
    };
    const int result = ::poll(&descriptor, 1, 0);
    if (result > 0 &&
        (descriptor.revents & (POLLERR | POLLHUP | POLLRDHUP)) != 0) {
      session->Close();
      co_return absl::OkStatus();
    }
    absl::Status slept =
        co_await SleepFor(*ThisWorker().self_, kPeerCheckInterval);
    if (!slept.ok()) co_return absl::OkStatus();
  }
  co_return absl::OkStatus();
}

}  // namespace

void MonitorSession::Enqueue(
    const std::shared_ptr<const std::string>& message) {
  if (closed_) return;
  if (registry_->pending_bytes_ >= kWorkerMonitorBufferLimit ||
      queue_.size() >= kMonitorQueueLimit) {
    closed_ = true;
    ready_.NotifyAll(*worker_);
    // This also breaks a write already parked in io_uring. Cleanup and byte
    // accounting remain on the owning worker in UnregisterMonitorSession.
    (void)::shutdown(fd_, SHUT_RDWR);
    return;
  }
  queue_.push_back(message);
  pending_bytes_ += message->size();
  registry_->pending_bytes_ += message->size();
  ready_.NotifyAll(*worker_);
}

Task<std::shared_ptr<const std::string>> MonitorSession::Next() {
  while (queue_.empty() && !closed_) {
    co_await ready_.Wait();
  }
  if (queue_.empty()) co_return nullptr;

  std::shared_ptr<const std::string> message = std::move(queue_.front());
  queue_.pop_front();
  assert(pending_bytes_ >= message->size());
  assert(registry_->pending_bytes_ >= message->size());
  pending_bytes_ -= message->size();
  registry_->pending_bytes_ -= message->size();
  co_return message;
}

void PrepareMonitor(unsigned worker_count) {
  assert(g_monitor_count.load(std::memory_order_relaxed) == 0);
  g_worker_count = worker_count;
  g_registries = std::make_unique<WorkerMonitorRegistry[]>(worker_count);
}

bool HasMonitorSessions() noexcept {
  return g_monitor_count.load(std::memory_order_acquire) != 0;
}

std::shared_ptr<const std::string> PrepareMonitorMessage(
    std::uint8_t db_id, std::string_view endpoint,
    std::span<const std::string> args, const CommandRequest* request) {
  if (args.empty() || (request != nullptr && !ValidMonitorCommand(*request))) {
    return nullptr;
  }

  timeval now{};
  gettimeofday(&now, nullptr);
  std::string output = absl::StrCat("+", now.tv_sec, ".");
  const std::string micros = std::to_string(now.tv_usec);
  output.append(6 - std::min<std::size_t>(6, micros.size()), '0');
  output.append(micros);
  absl::StrAppend(&output, " [", static_cast<unsigned>(db_id), " ",
                  endpoint.empty() ? std::string_view("?:0") : endpoint, "] ");

  const bool redact = absl::EqualsIgnoreCase(args.front(), "AUTH");
  for (std::size_t index = 0; index < args.size(); ++index) {
    if (index != 0) output.push_back(' ');
    AppendQuoted(redact && index != 0 ? std::string_view("(redacted)")
                                      : std::string_view(args[index]),
                 &output);
  }
  output.append("\r\n");
  return std::make_shared<const std::string>(std::move(output));
}

void PublishMonitorMessage(std::shared_ptr<const std::string> message) {
  if (message == nullptr) return;
  const CurrentWorker& current = ThisWorker();
  for (unsigned worker = 0; worker < g_worker_count; ++worker) {
    if (worker == current.id_) {
      DeliverMonitorMessage(message);
      continue;
    }
    auto context =
        std::make_unique<std::shared_ptr<const std::string>>(message);
    PostNotification(current.cross_core_, worker,
                     RemoteNotification{
                         .context_ = context.release(),
                         .value_ = 0,
                         .run_fn_ = &RunMonitorDelivery,
                     });
  }
}

void PublishExecMonitorCommands(const ConnectionContext& context,
                                std::span<const CommandRequest> commands) {
  for (const CommandRequest& command : commands) {
    PublishMonitorMessage(PrepareMonitorMessage(
        command.db_id_, context.peer_address_, command.args_, &command));
  }
}

std::shared_ptr<MonitorSession> RegisterMonitorSession(int fd) {
  WorkerMonitorRegistry& registry = LocalRegistry();
  auto session =
      std::make_shared<MonitorSession>(&registry, ThisWorker().self_, fd);
  registry.sessions_.push_back(session);
  g_monitor_count.fetch_add(1, std::memory_order_release);
  ThisWorker().self_->Spawn(WatchMonitorPeer(session));
  return session;
}

void UnregisterMonitorSession(const std::shared_ptr<MonitorSession>& session) {
  if (session == nullptr || !session->registered_) return;
  session->registered_ = false;
  session->Stop();

  WorkerMonitorRegistry& registry = LocalRegistry();
  assert(registry.pending_bytes_ >= session->pending_bytes_);
  registry.pending_bytes_ -= session->pending_bytes_;
  session->pending_bytes_ = 0;
  session->queue_.clear();
  std::erase(registry.sessions_, session);
  const unsigned previous =
      g_monitor_count.fetch_sub(1, std::memory_order_acq_rel);
  assert(previous != 0);
  (void)previous;
}

Task<absl::Status> StreamMonitorMessages(
    TcpStream& stream, const std::shared_ptr<MonitorSession>& session) {
  while (stream.IsOpen()) {
    std::shared_ptr<const std::string> message = co_await session->Next();
    if (message == nullptr) co_return absl::OkStatus();
    absl::Status written = co_await stream.WriteAll(std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(message->data()), message->size()));
    if (!written.ok()) {
      session->Close();
      co_return written;
    }
  }
  co_return absl::OkStatus();
}

}  // namespace keylane
