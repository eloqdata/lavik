#pragma once

// ClusterControlPort is the synchronous target-state source used by the static
// file and in-memory test adapters. Both hand complete states to the shared
// NodeControlInstaller. Meta control has an asynchronous session lifecycle and
// therefore calls that installer directly after decoding its wire projection;
// neither path exposes Raft phases to request routing.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/cluster/node_control.h"
#include "keylane/cluster/topology.h"

namespace keylane::cluster {

class ClusterControlPort {
 public:
  virtual ~ClusterControlPort() = default;

  // Reads the current target state from the control source and hands it to the
  // unique installer. On any source/parse error the previously published
  // state stays in effect (fencing transitions are only ever installed
  // complete).
  virtual absl::Status RefreshTarget(NodeControlInstaller& installer) = 0;
};

// Loads a Redis nodes.conf-format file as the static topology:
//
//   <id> <host>:<port>@<cport> <flags> <master-id> <ping> <pong> <epoch>
//       <link-state> [<slot>|<first>-<last>]...
//   vars currentEpoch <n> lastVoteEpoch <n>   (ignored)
//
// The same file is shared by every node; it carries no `myself` mark, so the
// local entry is identified by matching (host, port) — wildcard binds match
// on port alone. Migration markers ([slot-<-id], [slot->-id]) are rejected
// outright: v1 has no importing/migrating compatibility flow and must not
// silently misparse one.
//
// Grant semantics: statically configured primaries hold a permanent grant;
// readiness follows SetStorageReady (the server flips it after storage
// recovery completes), so before that point every non-whitelisted command is
// LOADING.
class StaticClusterControl final : public ClusterControlPort {
 public:
  struct SelfMatch {
    std::string host_;        // bind host; "0.0.0.0"/"::" match by port only
    std::uint16_t port_ = 0;  // this process's data port
  };

  // `cluster_tls_port` is the cluster-wide uniform TLS port announced for
  // every node (0 = no TLS). `worker_count` sizes each group's in-flight
  // stripes so Celer worker ids index them directly. Non-uniform TLS ports are
  // out of v1 scope.
  StaticClusterControl(std::string path, SelfMatch self,
                       std::uint16_t cluster_tls_port,
                       std::size_t worker_count = 1);

  absl::Status RefreshTarget(NodeControlInstaller& installer) override;

  // Marks storage readiness; takes effect on the next RefreshTarget.
  void SetStorageReady(bool ready);

  // Serializes terminal storage loss with SIGHUP/startup RefreshTarget across
  // the whole asynchronous NodeControl transition. The mutex is intentionally
  // held across suspension: reload runs on the process main thread, while the
  // transition and every continuation run on worker zero.
  celer::Task<absl::Status> LoseStorageReadinessTransition(
      NodeControlInstaller& installer);

  // Parses nodes.conf content into a ServingState without touching any cache.
  // Exposed for unit tests and for startup validation (fail fast on a file
  // that will never parse).
  static absl::StatusOr<std::shared_ptr<const ServingState>> Parse(
      std::string_view content, const SelfMatch& self,
      std::uint16_t cluster_tls_port, bool storage_ready,
      std::size_t worker_count = 1);

 private:
  std::string path_;
  SelfMatch self_;
  std::uint16_t cluster_tls_port_;
  std::size_t worker_count_;
  std::atomic<bool> storage_ready_{false};
  // Startup readiness is published by worker 0, while SIGHUP reload runs on
  // the process main thread. Serialize the complete parse/install boundary so
  // an older not-ready parse cannot overtake the readiness publication and so
  // NodeControlInstaller remains a single-writer seam in static mode.
  std::mutex refresh_mutex_;
  std::uint64_t refresh_revision_ = 0;
};

// Test adapter: tests build ServingStates directly (including fenced or
// not-ready states the static adapter never produces) and publish them
// through the same seam.
class InMemoryClusterControl final : public ClusterControlPort {
 public:
  void SetTarget(std::shared_ptr<const ServingState> state);
  absl::Status RefreshTarget(NodeControlInstaller& installer) override;

 private:
  std::shared_ptr<const ServingState> pending_;
  std::uint64_t refresh_revision_ = 0;
};

}  // namespace keylane::cluster
