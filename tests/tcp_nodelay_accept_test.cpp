#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "absl/status/status.h"
#include "bycorf/net/server.h"
#include "bycorf/net/service.h"
#include "bycorf/net/tcp_listener.h"
#include "bycorf/runtime/task.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "support/test_data_path.h"

namespace bycorf {
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
        status_ =
            absl::UnknownError(std::string("getsockopt(TCP_NODELAY) failed: ") +
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

class AcceptUnixService final : public Service {
 public:
  explicit AcceptUnixService(std::string path,
                             bool replace_before_close = false)
      : path_(std::move(path)), replace_before_close_(replace_before_close) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  Task<absl::Status> Run(Worker& worker, ServiceContext) override {
    if (!prepared_) {
      status_ =
          absl::FailedPreconditionError("Unix accept test requires one worker");
    } else {
      status_ = listener_.BindUnix(&worker, path_);
    }
    ready_.store(true, std::memory_order_release);
    if (!status_.ok()) {
      worker.RequestStop();
      co_return status_;
    }
    auto accepted = co_await listener_.AcceptUnregistered();
    if (!accepted.ok()) {
      status_ = accepted.status();
    } else {
      ucred credentials{};
      socklen_t size = sizeof(credentials);
      if (::getsockopt(accepted->file_.fd_, SOL_SOCKET, SO_PEERCRED,
                       &credentials, &size) != 0) {
        status_ = absl::UnknownError(std::string("SO_PEERCRED failed: ") +
                                     std::strerror(errno));
      } else {
        peer_uid_ = credentials.uid;
      }
      ::close(accepted->file_.fd_);
    }
    if (status_.ok() && replace_before_close_) {
      std::filesystem::remove(path_);
      std::ofstream replacement(path_);
      replacement << "replacement";
      replacement.close();
    }
    close_status_ = listener_.Close();
    if (!replace_before_close_ && status_.ok() && !close_status_.ok()) {
      status_ = close_status_;
    }
    worker.RequestStop();
    co_return status_;
  }

  void Stop() noexcept override { listener_.Close().IgnoreError(); }
  bool ready() const noexcept { return ready_.load(std::memory_order_acquire); }
  const absl::Status& status() const noexcept { return status_; }
  uid_t peer_uid() const noexcept { return peer_uid_; }
  const absl::Status& close_status() const noexcept { return close_status_; }

 private:
  std::string path_;
  TcpListener listener_;
  std::atomic<bool> ready_{false};
  uid_t peer_uid_ = static_cast<uid_t>(-1);
  bool replace_before_close_ = false;
  bool prepared_ = false;
  absl::Status status_ = absl::UnknownError("Unix accept test did not run");
  absl::Status close_status_ = absl::UnknownError("listener did not close");
};

std::filesystem::path MakeSecureUnixTestDirectory(std::string_view suffix) {
  const std::filesystem::path directory =
      keylane::test::TestDataDirectory() /
      ("keylane-bycorf-uds-" + std::to_string(::getpid()) + "-" +
       std::string(suffix));
  std::filesystem::remove_all(directory);
  std::filesystem::create_directory(directory);
  std::filesystem::permissions(directory, std::filesystem::perms::owner_all,
                               std::filesystem::perm_options::replace);
  return directory;
}

TEST(TcpListenerAcceptTest, UnixSocketAcceptsAndExposesPeerCredentials) {
  const std::filesystem::path directory =
      MakeSecureUnixTestDirectory("credentials");
  const std::filesystem::path path = directory / "listener.sock";
  AcceptUnixService service(path.string());
  Server server;
  server.AddService(&service);
  ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!service.ready() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(service.ready());
  ASSERT_TRUE(service.status().ok()) << service.status();

  const int client = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(client, 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, path.c_str(), path.string().size() + 1);
  ASSERT_EQ(
      ::connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0)
      << std::strerror(errno);

  server.WaitUntilStopped();
  ::close(client);
  ASSERT_TRUE(service.status().ok()) << service.status();
  EXPECT_EQ(service.peer_uid(), ::getuid());
  EXPECT_FALSE(std::filesystem::exists(path));
  std::filesystem::remove_all(directory);
}

TEST(TcpListenerAcceptTest, UnixClosePreservesReplacementPath) {
  const std::filesystem::path directory =
      MakeSecureUnixTestDirectory("replacement");
  const std::filesystem::path path = directory / "listener.sock";
  AcceptUnixService service(path.string(), /*replace_before_close=*/true);
  Server server;
  server.AddService(&service);
  ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!service.ready() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(service.ready());
  ASSERT_TRUE(service.status().ok()) << service.status();

  const int client = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(client, 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, path.c_str(), path.string().size() + 1);
  ASSERT_EQ(
      ::connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0)
      << std::strerror(errno);
  server.WaitUntilStopped();
  ::close(client);

  EXPECT_EQ(service.close_status().code(),
            absl::StatusCode::kFailedPrecondition);
  std::ifstream replacement(path);
  std::string contents;
  replacement >> contents;
  EXPECT_EQ(contents, "replacement");
  std::filesystem::remove_all(directory);
}

}  // namespace
}  // namespace bycorf
