#include "meta/nuraft_scheduler.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <string>

#include "celer/net/connection.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/worker.h"

namespace keylane::meta {
namespace {

absl::Status SocketpairStatus(int error, const char* operation) {
  return absl::Status(absl::StatusCode::kInternal,
                      std::string(operation) + ": " + std::strerror(error));
}

}  // namespace

// static
absl::StatusOr<std::shared_ptr<MetaCelerBridge>> MetaCelerBridge::Create() {
  // Both ends non-blocking: the read end is registered as a celer Connection
  // (which assumes nonblocking semantics) and foreign-thread writers must
  // never block on a full pipe.
  int fds[2] = {-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                   fds) != 0) {
    return SocketpairStatus(errno, "socketpair failed");
  }
  return std::shared_ptr<MetaCelerBridge>(new MetaCelerBridge(fds[0], fds[1]));
}

MetaCelerBridge::~MetaCelerBridge() {
  // The read fd is owned by the worker's connection table once Run()
  // registered it (wake_read_fd_ reset to -1 there); the drain loop closes it
  // via TcpStream::Close() on exit, or the worker closes it during shutdown.
  if (wake_read_fd_ >= 0) {
    ::close(wake_read_fd_);
  }
  if (wake_write_fd_ >= 0) {
    ::close(wake_write_fd_);
  }
}

void MetaCelerBridge::Wake() noexcept {
  const int fd = wake_write_fd_;
  if (fd < 0) {
    return;
  }
  // EAGAIN means the pipe is full of wakeups, i.e. one is already pending and
  // the drain loop will empty the whole inbox when it fires — dropping this
  // byte loses nothing.
  const std::uint64_t one = 1;
  const ssize_t written = ::write(fd, &one, sizeof(one));
  (void)written;
}

void MetaCelerBridge::Post(WorkItem work) {
  {
    std::lock_guard<std::mutex> lock(inbox_mu_);
    if (stopped_.load(std::memory_order_acquire)) {
      return;  // teardown race, same as posting into a stopped asio io_svc
    }
    inbox_.push_back(std::move(work));
  }
  Wake();
}

void MetaCelerBridge::Stop() noexcept {
  bool expected = false;
  if (!stopped_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel)) {
    return;
  }
  Wake();  // make a suspended drain loop observe the stop
}

void MetaCelerBridge::DrainInbox(celer::Worker& worker) {
  std::deque<WorkItem> work;
  {
    std::lock_guard<std::mutex> lock(inbox_mu_);
    work.swap(inbox_);
  }
  for (WorkItem& item : work) {
    item(worker);
  }
}

celer::Task<absl::Status> MetaCelerBridge::Run(celer::Worker& worker) {
  // Keep the bridge alive while the loop is suspended; without this the
  // frame could outlive the last external shared_ptr.
  std::shared_ptr<MetaCelerBridge> self = shared_from_this();

  // Register the read end with the worker's connection table, mirroring the
  // accept-side registration flow in ConnectTcp.
  celer::Connection connection;
  connection.worker_ = &worker;
  connection.file_.fd_ = wake_read_fd_;
  connection.closed_ = false;
  celer::Connection* registered = worker.AddConnection(std::move(connection));
  if (registered == nullptr) {
    ::close(wake_read_fd_);
    wake_read_fd_ = -1;
    co_return absl::Status(absl::StatusCode::kInternal,
                           "failed to register bridge wake fd");
  }
  wake_read_fd_ = -1;  // ownership moved to the worker
  celer::TcpStream stream(registered);

  std::byte chunk[256];
  while (true) {
    auto read = co_await stream.ReadSome(chunk);
    // A read error means the worker is shutting down (it closes every
    // registered connection); 0 means the write end was closed, which only
    // happens at bridge destruction. Either way the loop's work is over.
    if (!read.ok() || *read == 0) {
      break;
    }
    DrainInbox(worker);
    if (stopped_.load(std::memory_order_acquire)) {
      break;
    }
  }
  (void)stream.Close();
  co_return absl::OkStatus();
}

}  // namespace keylane::meta
