#include "tests/support/process.h"
#include "tests/support/test_data_path.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "gtest/gtest.h"

namespace keylane::test {
namespace {

class ScopedEnvironment {
 public:
  ScopedEnvironment(std::string name, std::string value)
      : name_(std::move(name)) {
    if (const char* previous = std::getenv(name_.c_str());
        previous != nullptr) {
      previous_ = previous;
    }
    if (::setenv(name_.c_str(), value.c_str(), 1) != 0) {
      throw std::runtime_error("setenv failed");
    }
  }

  ~ScopedEnvironment() {
    if (previous_.has_value()) {
      (void)::setenv(name_.c_str(), previous_->c_str(), 1);
    } else {
      (void)::unsetenv(name_.c_str());
    }
  }

 private:
  std::string name_;
  std::optional<std::string> previous_;
};

TEST(ProcessSupportTest, AcceptsConfiguredRootWithTrailingSeparator) {
  TempDirectory root("process-support-root");
  {
    ScopedEnvironment environment("KEYLANE_TEST_DATA_DIR",
                                  root.path().string() + "/");
    TempDirectory directory("configured-root");
    EXPECT_TRUE(std::filesystem::equivalent(directory.path().parent_path(),
                                           root.path()));
  }
  EXPECT_TRUE(std::filesystem::is_empty(root.path()));
}

TEST(ProcessSupportTest, DefaultsToTmpForEmptyConfiguredRoot) {
  ScopedEnvironment environment("KEYLANE_TEST_DATA_DIR", "");
  TempDirectory directory("empty-root");
  EXPECT_TRUE(std::filesystem::equivalent(directory.path().parent_path(),
                                         "/tmp"));
}

TEST(ProcessSupportTest, SpawnsWithEnvironmentAndControlsLifecycle) {
  TempDirectory directory("process-support");
  const std::filesystem::path log = directory.path() / "child.log";
  ChildProcess child(
      {"/bin/sh", "-c", "printf '%s\\n' \"$KEYLANE_PROCESS_TEST\"; sleep 60"},
      log, {{"KEYLANE_PROCESS_TEST", "ready"}});

  WaitUntil("child output", std::chrono::seconds(2), [&] {
    return std::filesystem::exists(log) &&
           ReadFile(log).find("ready") != std::string::npos;
  });
  child.Pause();
  child.Resume();
  child.Stop(SIGTERM);
  EXPECT_EQ(child.pid(), -1);
}

TEST(ProcessSupportTest, CreatesTemporaryDirectoriesUnderTestDataRoot) {
  TempDirectory directory("configured-root");
  EXPECT_TRUE(std::filesystem::equivalent(directory.path().parent_path(),
                                         TestDataDirectory()));
}

TEST(ProcessSupportTest, HoldsPortUntilExplicitRelease) {
  PortReservation reservation;
  EXPECT_NE(reservation.port(), 0);

  const int contender = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(contender, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(reservation.port());
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  EXPECT_EQ(::bind(contender, reinterpret_cast<const sockaddr*>(&address),
                   sizeof(address)),
            -1);
  EXPECT_EQ(errno, EADDRINUSE);
  EXPECT_EQ(reservation.ReleaseForSpawn(), reservation.port());
  EXPECT_EQ(::bind(contender, reinterpret_cast<const sockaddr*>(&address),
                   sizeof(address)),
            0);
  EXPECT_EQ(::close(contender), 0);
}

TEST(ProcessSupportTest, RejectsOversizedAndDeepRespReplies) {
  auto expect_rejected = [](std::string_view reply) {
    int sockets[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets), 0);
    ASSERT_EQ(::send(sockets[1], reply.data(), reply.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(reply.size()));
    {
      RespClient client(sockets[0]);
      EXPECT_THROW((void)client.Command({"PING"}), std::runtime_error);
    }
    EXPECT_EQ(::close(sockets[1]), 0);
  };

  expect_rejected("$16777217\r\n");
  std::string deeply_nested;
  for (std::size_t depth = 0; depth < 33; ++depth) deeply_nested += "*1\r\n";
  deeply_nested += "+OK\r\n";
  expect_rejected(deeply_nested);
}

}  // namespace
}  // namespace keylane::test
