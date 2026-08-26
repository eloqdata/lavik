#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>

#include "absl/status/status.h"
#include "celer/net/server.h"
#include "celer/net/service.h"
#include "celer/net/tcp_listener.h"
#include "celer/runtime/task.h"
#include "celer/runtime/worker.h"
#include "gtest/gtest.h"

namespace celer {
namespace {

// Serves one blocking client connect with AcceptUnregistered() and records the
// TCP_NODELAY value found on the accepted fd, before the connection is
// registered with or handed to any worker.
class AcceptNoDelayService final : public Service {
 public:
  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  Task<absl::Status> Run(Worker& worker, ServiceContext) override {
    if (!prepared_) {
      status_ = absl::FailedPreconditionError(
          "accept nodelay test requires one worker");
      bound_port_.store(-1, std::memory_order_release);
      worker.RequestStop();
      co_return status_;
    }

    status_ = listener_.Bind(&worker, "127.0.0.1", 0);
    if (status_.ok()) {
      sockaddr_in bound{};
      socklen_t bound_length = sizeof(bound);
      if (::getsockname(listener_.NativeFd(),
                        reinterpret_cast<sockaddr*>(&bound),
                        &bound_length) != 0) {
        status_ = absl::UnknownError(std::string("getsockname failed: ") +
                                     std::strerror(errno));
      } else {
        bound_port_.store(ntohs(bound.sin_port), std::memory_order_release);
      }
    }
    if (!status_.ok()) {
      bound_port_.store(-1, std::memory_order_release);
      worker.RequestStop();
      co_return status_;
    }

    auto accepted = co_await listener_.AcceptUnregistered();
    if (!accepted.ok()) {
      status_ = accepted.status();
    } else {
      const int fd = accepted->file_.fd_;
      socklen_t option_length = sizeof(tcp_nodelay_);
      if (::getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay_,
                       &option_length) != 0) {
        status_ = absl::UnknownError(
            std::string("getsockopt(TCP_NODELAY) failed: ") +
            std::strerror(errno));
      }
      ::close(fd);
    }
    listener_.Close().IgnoreError();
    worker.RequestStop();
    co_return status_;
  }

  void Stop() noexcept override { listener_.Close().IgnoreError(); }

  int bound_port() const noexcept {
    return bound_port_.load(std::memory_order_acquire);
  }
  const absl::Status& status() const noexcept { return status_; }
  int tcp_nodelay() const noexcept { return tcp_nodelay_; }

 private:
  TcpListener listener_;
  std::atomic<int> bound_port_{0};
  int tcp_nodelay_ = -1;
  bool prepared_ = false;
  absl::Status status_ =
      absl::UnknownError("accept nodelay service did not run");
};

TEST(TcpListenerAcceptTest, AcceptedConnectionDisablesNagle) {
  AcceptNoDelayService service;
  Server server;
  server.AddService(&service);
  ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());

  // The worker publishes the ephemeral port once the listener is bound; the
  // connect below stays blocking on purpose (setup path only).
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  int port = 0;
  while ((port = service.bound_port()) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_GT(port, 0) << "listener failed to bind: " << service.status();

  const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(client, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<std::uint16_t>(port));
  ASSERT_EQ(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  ASSERT_EQ(
      ::connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0)
      << std::strerror(errno);

  server.WaitUntilStopped();
  ::close(client);

  ASSERT_TRUE(service.status().ok()) << service.status();
  EXPECT_EQ(service.tcp_nodelay(), 1);
}

}  // namespace
}  // namespace celer
