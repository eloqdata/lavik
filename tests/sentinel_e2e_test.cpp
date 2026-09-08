#include <signal.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "tests/support/process.h"

namespace {
using namespace std::chrono_literals;
using keylane::test::ChildProcess;
using keylane::test::Connect;
using keylane::test::CreateDataFile;
using keylane::test::Fail;
using keylane::test::PortReservation;
using keylane::test::ReadFile;
using keylane::test::RespClient;
using keylane::test::TempDirectory;
using keylane::test::WaitUntil;
using keylane::test::WriteFile;

std::vector<std::string> KeylaneArgs(const std::string& binary,
                                     const std::string& config,
                                     const std::string& data) {
  return {binary,
          config,
          "--data-file",
          data,
          "--threads",
          "1",
          "--no-pin-workers",
          "--logtostderr",
          "--recv-buffers-per-worker",
          "0"};
}

std::string Bulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value);
}

std::size_t Count(std::string_view haystack, std::string_view needle) {
  std::size_t count = 0;
  for (std::size_t offset = 0;
       (offset = haystack.find(needle, offset)) != std::string_view::npos;
       offset += needle.size()) {
    ++count;
  }
  return count;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 3) {
      Fail("usage: sentinel_e2e_test KEYLANE_BINARY REDIS_SERVER_BINARY");
    }
    TempDirectory root("sentinel-e2e");
    PortReservation master_reservation;
    PortReservation preferred_reservation;
    PortReservation other_reservation;
    std::array<PortReservation, 3> sentinel_reservations;
    const std::uint16_t master_port = master_reservation.port();
    const std::uint16_t preferred_port = preferred_reservation.port();
    const std::uint16_t other_port = other_reservation.port();
    const std::array<std::uint16_t, 3> sentinel_ports{
        sentinel_reservations[0].port(), sentinel_reservations[1].port(),
        sentinel_reservations[2].port()};

    const std::string master_config = (root.path() / "master.conf").string();
    const std::string preferred_config =
        (root.path() / "preferred.conf").string();
    const std::string other_config = (root.path() / "other.conf").string();
    WriteFile(master_config, "port " + std::to_string(master_port) + "\n");
    WriteFile(preferred_config, "port " + std::to_string(preferred_port) +
                                    "\nreplicaof 127.0.0.1 " +
                                    std::to_string(master_port) +
                                    "\nreplica-priority 10\n");
    WriteFile(other_config,
              "port " + std::to_string(other_port) + "\nreplicaof 127.0.0.1 " +
                  std::to_string(master_port) + "\nreplica-priority 100\n");
    const std::string master_data = (root.path() / "master.data").string();
    const std::string preferred_data =
        (root.path() / "preferred.data").string();
    const std::string other_data = (root.path() / "other.data").string();
    CreateDataFile(master_data, 128ULL * 1024 * 1024);
    CreateDataFile(preferred_data, 128ULL * 1024 * 1024);
    CreateDataFile(other_data, 128ULL * 1024 * 1024);

    (void)master_reservation.ReleaseForSpawn();
    ChildProcess master(KeylaneArgs(argv[1], master_config, master_data),
                        root.path() / "master.log");
    (void)preferred_reservation.ReleaseForSpawn();
    ChildProcess preferred(
        KeylaneArgs(argv[1], preferred_config, preferred_data),
        root.path() / "preferred.log",
        {
#if KEYLANE_TEST_FAULTS_AVAILABLE
            {"KEYLANE_REPLICATION_PAUSE_FULLSYNC_AFTER_RESET_MS", "1000"}
#endif
        });
    (void)other_reservation.ReleaseForSpawn();
    ChildProcess other(KeylaneArgs(argv[1], other_config, other_data),
                       root.path() / "other.log");

    WaitUntil("both replicas online", 60s, [&] {
      RespClient client = Connect(master_port);
      const std::string info = client.Command({"INFO", "replication"});
      return info.find("connected_slaves:2") != std::string::npos;
    });
    {
      RespClient client = Connect(master_port);
      if (client.Command({"SET", "sentinel-key", "before"}) != "+OK") {
        Fail("failed to write pre-failover value");
      }
    }
    WaitUntil("pre-failover value on both replicas", 30s, [&] {
      for (const std::uint16_t port : {preferred_port, other_port}) {
        RespClient client = Connect(port);
        (void)client.Command({"READONLY"});
        if (client.Command({"GET", "sentinel-key"}) != Bulk("before")) {
          return false;
        }
      }
      return true;
    });

    std::array<std::string, 3> sentinel_configs;
    for (std::size_t index = 0; index < sentinel_ports.size(); ++index) {
      sentinel_configs[index] =
          (root.path() / ("sentinel-" + std::to_string(index) + ".conf"))
              .string();
      WriteFile(sentinel_configs[index],
                "port " + std::to_string(sentinel_ports[index]) + "\n" +
                    "dir " + root.path().string() + "\n" +
                    "sentinel monitor keylane 127.0.0.1 " +
                    std::to_string(master_port) + " 2\n" +
                    "sentinel down-after-milliseconds keylane 1000\n" +
                    "sentinel failover-timeout keylane 15000\n" +
                    "sentinel parallel-syncs keylane 1\n");
    }
    (void)sentinel_reservations[0].ReleaseForSpawn();
    ChildProcess sentinel0({argv[2], sentinel_configs[0], "--sentinel"},
                           root.path() / "sentinel-0.log");
    (void)sentinel_reservations[1].ReleaseForSpawn();
    ChildProcess sentinel1({argv[2], sentinel_configs[1], "--sentinel"},
                           root.path() / "sentinel-1.log");
    (void)sentinel_reservations[2].ReleaseForSpawn();
    ChildProcess sentinel2({argv[2], sentinel_configs[2], "--sentinel"},
                           root.path() / "sentinel-2.log");
    WaitUntil("all Sentinels discover replicas and each other", 45s, [&] {
      for (const std::uint16_t sentinel_port : sentinel_ports) {
        RespClient client = Connect(sentinel_port);
        const std::string replicas =
            client.Command({"SENTINEL", "REPLICAS", "keylane"});
        if (replicas.find(std::to_string(preferred_port)) ==
                std::string::npos ||
            replicas.find(std::to_string(other_port)) == std::string::npos ||
            Count(replicas, "$18\r\nmaster-link-status\r\n$2\r\nok") != 2) {
          return false;
        }
        const std::string peers =
            client.Command({"SENTINEL", "SENTINELS", "keylane"});
        if (!peers.starts_with("*2\r\n")) return false;
      }
      return true;
    });
    WaitUntil("three Sentinel named pubsub connections", 30s, [&] {
      RespClient client = Connect(master_port);
      const std::string list =
          client.Command({"CLIENT", "LIST", "TYPE", "pubsub"});
      return Count(list, "name=sentinel-") == 3 && Count(list, "flags=P") == 3;
    });

    // Exercise actual quorum-based failure detection, not just the manual
    // SENTINEL FAILOVER path. Two of three Sentinels must agree the primary is
    // objectively down before one can win the leader election.
    master.Stop(SIGKILL);
    const std::string preferred_address =
        "*2\r\n$9\r\n127.0.0.1\r\n$" +
        std::to_string(std::to_string(preferred_port).size()) + "\r\n" +
        std::to_string(preferred_port);
    WaitUntil("quorum priority-selected master", 60s, [&] {
      for (const std::uint16_t sentinel_port : sentinel_ports) {
        RespClient client = Connect(sentinel_port);
        if (client.Command({"SENTINEL", "GET-MASTER-ADDR-BY-NAME",
                            "keylane"}) != preferred_address) {
          return false;
        }
      }
      return true;
    });
    WaitUntil("promoted Keylane role", 30s, [&] {
      RespClient client = Connect(preferred_port);
      return client.Command({"ROLE"}).starts_with("*3\r\n$6\r\nmaster");
    });
    WaitUntil("remaining replica follows promoted master", 60s, [&] {
      // Keep PUBLISH active while the remaining replica reconnects. A
      // Debug/fault server additionally pauses after its first 64-partition
      // reset batch: this channel's slot 7127 then exercises an epoch that is
      // not installed yet. Ordinary Release still runs the full quorum,
      // promotion and replication checks, without claiming that pause window.
      RespClient promoted = Connect(preferred_port);
      if (!promoted.Command({"PUBLISH", "fullsync-epoch-race", "probe"})
               .starts_with(':')) {
        return false;
      }
      RespClient client = Connect(other_port);
      const std::string info = client.Command({"INFO", "replication"});
      return info.find("master_port:" + std::to_string(preferred_port)) !=
                 std::string::npos &&
             info.find("master_link_status:up") != std::string::npos;
    });
    {
      RespClient promoted = Connect(preferred_port);
      if (promoted.Command({"GET", "sentinel-key"}) != Bulk("before")) {
        Fail("promoted replica lost pre-failover data");
      }
      if (promoted.Command({"SET", "sentinel-after", "ok"}) != "+OK") {
        Fail("promoted replica did not accept writes");
      }
    }
    WaitUntil("post-failover replication", 30s, [&] {
      RespClient client = Connect(other_port);
      (void)client.Command({"READONLY"});
      return client.Command({"GET", "sentinel-after"}) == Bulk("ok");
    });

    sentinel2.Stop();
    sentinel1.Stop();
    sentinel0.Stop();
    other.Stop();
    preferred.Stop();
    master.Stop();
    if (ReadFile(root.path() / "other.log")
            .find("malformed full-sync published command") !=
        std::string::npos) {
      Fail("PUBLISH raced ahead of its full-sync partition reset");
    }
#if !KEYLANE_TEST_FAULTS_AVAILABLE
    std::cout << "sentinel reset-pause injection not exercised: requires a "
                 "Debug/fault server; quorum and replication checks passed\n";
#endif
    std::cout << "sentinel e2e passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "sentinel e2e failed: " << error.what() << '\n';
    return 1;
  }
}
