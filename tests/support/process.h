#pragma once

#include <sys/types.h>

#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace keylane::test {

// Test-support failures throw std::runtime_error so callers can retain logs and
// let the test framework report a single contextual failure.
[[noreturn]] void Fail(std::string message);

// File creators use O_EXCL and never overwrite existing fixtures. All helpers
// throw through Fail on I/O errors.
void WriteFile(const std::filesystem::path& path, std::string_view contents);
void CreateDataFile(const std::filesystem::path& path, std::uint64_t bytes);
std::string ReadFile(const std::filesystem::path& path);

class TempDirectory {
 public:
  // Creates a uniquely named directory under TMPDIR (or /tmp when unset).
  // Normal destruction recursively removes it; Preserve or stack unwinding
  // retains it for failure diagnosis.
  explicit TempDirectory(std::string_view label);
  TempDirectory(const TempDirectory&) = delete;
  TempDirectory& operator=(const TempDirectory&) = delete;
  ~TempDirectory();

  const std::filesystem::path& path() const noexcept { return path_; }
  void Preserve() noexcept { preserve_ = true; }

 private:
  std::filesystem::path path_;
  bool preserve_ = false;
};

// Holds a bound loopback socket while test configuration is assembled. Most
// current servers cannot inherit a listener, so ReleaseForSpawn narrows but
// cannot eliminate the final bind race; callers must still diagnose startup
// failure and may retry the whole fixture with fresh reservations.
class PortReservation {
 public:
  PortReservation();
  PortReservation(const PortReservation&) = delete;
  PortReservation& operator=(const PortReservation&) = delete;
  PortReservation(PortReservation&& other) noexcept;
  PortReservation& operator=(PortReservation&& other) noexcept;
  ~PortReservation();

  std::uint16_t port() const noexcept { return port_; }
  std::uint16_t ReleaseForSpawn();

 private:
  int fd_ = -1;
  std::uint16_t port_ = 0;
};

class ChildProcess {
 public:
  // Starts arguments[0] in a new process group and redirects stdout/stderr to
  // log_path. The destructor force-stops the whole group without throwing.
  ChildProcess(
      std::vector<std::string> arguments, const std::filesystem::path& log_path,
      const std::vector<std::pair<std::string, std::string>>& environment = {});
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;
  ChildProcess(ChildProcess&& other) noexcept;
  ChildProcess& operator=(ChildProcess&& other) noexcept;
  ~ChildProcess();

  pid_t pid() const noexcept { return pid_; }
  void Pause();
  void Resume();
  // Stop signals the process group and may block until it exits; use Wait when
  // the test needs a bounded expectation. Wait returns the waitpid status.
  void Stop(int signal = SIGINT);
  int Wait(std::chrono::milliseconds timeout);

 private:
  void KillProcessGroup(int signal) const;
  void StopNoThrow() noexcept;

  pid_t pid_ = -1;
  bool stopped_ = false;
};

class RespClient {
 public:
  // Owns a connected socket and exchanges one RESP2 command/reply at a time.
  // Replies are limited to 16 MiB on the wire, 64 KiB per line, and 32 nested
  // arrays. Protocol, I/O, and limit failures throw via Fail.
  explicit RespClient(int fd) : fd_(fd) {}
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;
  RespClient(RespClient&& other) noexcept;
  RespClient& operator=(RespClient&& other) noexcept;
  ~RespClient();

  std::string Command(const std::vector<std::string_view>& arguments);

 private:
  static std::size_t Length(std::string_view line);
  void SendAll(std::string_view bytes);
  void ReadExact(char* output, std::size_t bytes);
  std::string ReadLine();
  std::string ReadReply();
  std::string ReadReply(std::size_t depth, std::size_t* wire_bytes);

  int fd_ = -1;
};

RespClient Connect(std::uint16_t port, std::chrono::milliseconds timeout =
                                           std::chrono::seconds(20));

// Retries ready until timeout. Exceptions from ready are treated as transient
// failures so startup races do not discard the last useful test log.
void WaitUntil(
    std::string_view label, std::chrono::milliseconds timeout,
    const std::function<bool()>& ready,
    std::chrono::milliseconds poll_interval = std::chrono::milliseconds(100));

}  // namespace keylane::test
