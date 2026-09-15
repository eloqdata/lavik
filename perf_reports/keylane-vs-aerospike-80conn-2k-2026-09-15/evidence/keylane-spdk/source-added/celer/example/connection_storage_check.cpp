#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <iostream>
#include <memory>
#include <utility>

#include "celer/io/storage.h"
#include "celer/net/connection.h"
#include "celer/runtime/runtime.h"

namespace {

using namespace std::chrono_literals;
int failures = 0;

void Check(bool passed, const char* message) {
  std::cout << (passed ? "PASS " : "FAIL ") << message << '\n';
  if (!passed) ++failures;
}

celer::Task<absl::Status> CheckStorage(celer::Worker& worker) {
  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                 sockets) != 0) {
    Check(false, "create socket pair");
    worker.RequestStop();
    co_return absl::InternalError("socketpair failed");
  }

  celer::Connection initial;
  initial.worker_ = &worker;
  initial.file_.fd_ = sockets[0];
  initial.closed_ = false;
  // An aliasing shared_ptr lets the test observe destruction without reading
  // potentially reclaimed memory. No TLS state is used by this raw connection.
  auto sentinel = std::make_shared<int>(0);
  std::weak_ptr<int> storage_alive = sentinel;
  initial.tls_state_ =
      std::shared_ptr<celer::TlsState>(std::move(sentinel), nullptr);
  auto* connection = worker.AddConnection(std::move(initial));
  celer::BorrowConnectionStorage(connection);
  celer::BorrowConnectionStorage(connection);
  Check(worker.EnsureRecvArmed(connection).ok(), "arm real pending receive");
  worker.BeginClose(connection, absl::CancelledError("test close"),
                    celer::CloseMode::kLocalClose);
  Check(fcntl(sockets[0], F_GETFD) == -1 && errno == EBADF,
        "storage borrow allows immediate transport close");
  close(sockets[1]);

  // Returning to the event loop lets cancellation completions and reclamation
  // run while this session is suspended on a timer instead of socket I/O.
  (void)co_await celer::SleepFor(worker, 20ms);
  Check(!storage_alive.expired(), "closed storage survives suspended session");
  if (!storage_alive.expired()) {
    Check(connection->closed_ && !connection->recv_armed_,
          "receive cancellation drains while storage stays borrowed");
    celer::ReleaseConnectionStorage(connection);
    (void)co_await celer::SleepFor(worker, 20ms);
    Check(!storage_alive.expired(), "one remaining borrower prevents reclaim");
    if (!storage_alive.expired()) {
      celer::ReleaseConnectionStorage(connection);
      (void)co_await celer::SleepFor(worker, 20ms);
      Check(storage_alive.expired(), "last release permits worker reclamation");
    }
  }
  worker.RequestStop();
  co_return absl::OkStatus();
}

}  // namespace

int main() {
  celer::Runtime runtime;
  runtime.Start(1, [](unsigned, celer::Worker& worker) {
    if (!worker.Init().ok()) return 1;
    worker.Spawn(CheckStorage(worker));
    worker.Run();
    worker.Shutdown();
    return failures == 0 ? 0 : 1;
  });
  runtime.WaitUntilStopped();
  return runtime.exit_code();
}
