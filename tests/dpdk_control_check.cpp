// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <span>
#include <thread>

#include "bycorf/io/backend_options.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/server.h"
#include "bycorf/net/socket_ops.h"
#include "bycorf/net/tcp_service.h"

namespace {
using namespace std::chrono_literals;

class DataService : public bycorf::TcpService {
 public:
  DataService() : TcpService(16404) { SetWorkers({0, 1}); }
  bycorf::Task<absl::Status> Serve(bycorf::TcpStream stream) override {
    std::array<std::byte, 1> id{std::byte(bycorf::ThisWorker().id_)};
    co_return co_await stream.WriteAll(id);
  }
};

class ControlService : public bycorf::Service {
 public:
  std::atomic<int> result{-1};
  ControlService() { SetWorkers({2}); }
  void Prepare(unsigned) override {}
  void Stop() noexcept override {}
  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    auto status = co_await Check(worker);
    if (!status.ok()) std::cerr << status << '\n';
    result.store(status.ok() ? 0 : 1);
    co_return status;
  }

 private:
  bycorf::Task<absl::Status> CheckLocal(bycorf::Worker& worker) {
    // Worker 2 has no data listener. Native local routing must honor service
    // placement and return replies to this worker's client-port stripe.
    for (unsigned n = 0; n < 16; ++n) {
      auto connected =
          co_await bycorf::ConnectTcp(worker, "198.18.0.2", 16404, 1s);
      if (!connected.ok()) co_return connected.status();
      auto stream = std::move(*connected);
      auto borrow = stream.BorrowStorage();
      if (!bycorf::detail::IsDpdkSocket(stream.NativeFd()))
        co_return absl::InternalError("local connection used a kernel socket");
      std::array<std::byte, 1> owner{};
      auto read = co_await stream.ReadSome(owner);
      stream.Close().IgnoreError();
      if (!read.ok()) co_return read.status();
      if (*read != 1 || std::to_integer<unsigned>(owner[0]) >= 2)
        co_return absl::InternalError("local connection missed data placement");
    }
    auto refused = co_await bycorf::ConnectTcp(worker, "198.18.0.2", 16406, 1s);
    if (refused.ok() ||
        refused.status().code() == absl::StatusCode::kDeadlineExceeded)
      co_return absl::InternalError("local refused connect did not complete");
    std::cout << "PASS DPDK local control-to-data connects and refusal\n";
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> Check(bycorf::Worker& worker) {
    const auto until = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < until) {
      auto connected =
          co_await bycorf::ConnectTcp(worker, "198.18.0.1", 16405, 300ms);
      if (!connected.ok()) {
        auto slept = co_await bycorf::SleepFor(worker, 100ms);
        if (!slept.ok()) co_return slept;
        continue;
      }
      auto stream = std::move(*connected);
      if (!bycorf::detail::IsDpdkSocket(stream.NativeFd())) {
        co_return absl::InternalError(
            "outbound control connection used a kernel socket");
      }
      std::array<std::byte, 4096> sent{}, received{};
      sent.fill(std::byte{0x5a});
      auto written = co_await stream.WriteAll(sent);
      if (!written.ok()) co_return written;
      std::size_t used = 0;
      while (used < received.size()) {
        auto read = co_await stream.ReadSome(std::span(received).subspan(used));
        if (!read.ok()) co_return read.status();
        if (*read == 0) co_return absl::InternalError("early control EOF");
        used += *read;
      }
      stream.Close().IgnoreError();
      if (sent != received)
        co_return absl::DataLossError("control echo mismatch");
      auto refused =
          co_await bycorf::ConnectTcp(worker, "198.18.0.1", 16406, 1s);
      if (refused.ok())
        co_return absl::InternalError("refused connect succeeded");
      auto timeout =
          co_await bycorf::ConnectTcp(worker, "198.18.0.99", 16405, 50ms);
      if (timeout.ok() ||
          timeout.status().code() != absl::StatusCode::kDeadlineExceeded) {
        co_return absl::InternalError(
            "connect deadline did not retire DPDK operation");
      }
      auto local = co_await CheckLocal(worker);
      if (!local.ok()) co_return local;
      std::cout
          << "PASS DPDK control connect, echo, refusal, timeout on worker 2\n";
      co_return absl::OkStatus();
    }
    co_return absl::DeadlineExceededError("TAP peer did not become ready");
  }
};
}  // namespace

int main() {
  auto selected = bycorf::ConfigureIoBackends({.dpdk_network = true});
  if (!selected.ok()) return 2;
  DataService data;
  ControlService control;
  bycorf::Server server;
  server.AddService(&data);
  server.AddService(&control);
  bycorf::ServerOptions options;
  options.bind_ip_ = "198.18.0.2";
  options.thread_count_ = 3;
  const auto started = server.Start(options);
  if (!started.ok()) {
    std::cerr << started;
    return 3;
  }
  const auto until = std::chrono::steady_clock::now() + 40s;
  while (control.result.load() < 0 && !server.stopped() &&
         std::chrono::steady_clock::now() < until)
    std::this_thread::sleep_for(10ms);
  server.RequestStop();
  server.WaitUntilStopped();
  return control.result.load() == 0 ? 0 : 4;
}
