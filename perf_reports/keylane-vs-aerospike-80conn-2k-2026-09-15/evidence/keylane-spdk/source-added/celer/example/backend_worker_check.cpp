#include <arpa/inet.h>

#include <atomic>
#include <cerrno>
#include <iostream>
#include <latch>
#include <set>
#include <string>
#include <vector>

#include "celer/net/socket_ops.h"
#include "celer/runtime/runtime.h"

// Run on a virtual PMD. Every owner must have a positive, distinct BSD handle,
// including worker IDs above 63; rejecting a foreign handle must not close the
// owner's listener. Oversubscription is intentional for capacity checks.
int main(int argc, char** argv) {
  const unsigned workers = argc > 1 ? std::stoul(argv[1]) : 128;
  const bool expect_rejection =
      argc > 2 && std::string_view(argv[2]) == "reject";
  if (!workers || !celer::ConfigureIoBackends({.dpdk_network = true}).ok())
    return 2;
  celer::Runtime runtime;
  std::latch ready(workers);
  std::vector<int> handles(workers, -1);
  std::atomic<unsigned> passed{0};
  try {
    runtime.Start(
        workers,
        [&](unsigned id, celer::Worker& worker) {
          const auto status = worker.Init({});
          if (!status.ok()) {
            std::cerr << status << '\n';
            ready.count_down();
            return 1;
          }
          sockaddr_in address{};
          address.sin_family = AF_INET;
          address.sin_port = htons(16391);
          inet_pton(AF_INET, "198.18.0.2", &address.sin_addr);
          handles[id] = celer::DpdkBackend::Listen(
              reinterpret_cast<sockaddr*>(&address), sizeof(address), 16);
          ready.arrive_and_wait();
          bool ok = celer::detail::IsDpdkSocket(handles[id]);
          if (id == 0)
            ok = ok && std::set<int>(handles.begin(), handles.end()).size() ==
                           workers;
          if (workers > 1) {
            const int other = handles[(id + 1) % workers];
            ok = ok && celer::DpdkBackend::Close(other) == -1 && errno == EBADF;
          }
          // Kernel close() must never see this namespace, even at the highest
          // ID.
          ok = ok && celer::detail::CloseSocket(handles[id]) == 0;
          if (ok) ++passed;
          worker.Shutdown();
          return ok ? 0 : 1;
        },
        false);
    runtime.WaitUntilStopped();
  } catch (const std::exception& error) {
    std::cout << error.what() << '\n';
    return expect_rejection && std::string_view(error.what())
                                   .starts_with("DPDK build supports ")
               ? 0
               : 1;
  }
  if (expect_rejection || runtime.exit_code() || passed != workers) return 1;
  std::cout << workers
            << " workers: initialization, distinct handles, foreign "
               "handle rejection and shutdown: PASS\n";
}
