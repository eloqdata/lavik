#pragma once

// ClusterControlPort: the seam through which a control plane
// supplies the target serving state. The static file adapter and the
// in-memory test adapter ship with the open-source data plane; the Meta/Raft
// adapter (follow-up work) implements the same interface without the data plane
// ever learning about Raft phases, transport, or licensing.

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/cluster/topology.h"

namespace keylane::cluster {

class ClusterControlPort {
 public:
  virtual ~ClusterControlPort() = default;

  // Reads the current target state from the control source and publishes it
  // into `cache` when the content changed. On any error the previously
  // published state stays in effect (fencing transitions are only ever
  // published complete).
  virtual absl::Status RefreshTarget(TopologyCache& cache) = 0;
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
  // every node (0 = no TLS). Non-uniform TLS ports are out of v1 scope.
  StaticClusterControl(std::string path, SelfMatch self,
                       std::uint16_t cluster_tls_port);

  absl::Status RefreshTarget(TopologyCache& cache) override;

  // Marks storage readiness; takes effect on the next RefreshTarget.
  void SetStorageReady(bool ready);

  // Parses nodes.conf content into a ServingState without touching any cache.
  // Exposed for unit tests and for startup validation (fail fast on a file
  // that will never parse).
  static absl::StatusOr<std::shared_ptr<const ServingState>> Parse(
      std::string_view content, const SelfMatch& self,
      std::uint16_t cluster_tls_port, bool storage_ready);

 private:
  std::string path_;
  SelfMatch self_;
  std::uint16_t cluster_tls_port_;
  std::atomic<bool> storage_ready_{false};
};

// Test adapter: tests build ServingStates directly (including fenced or
// not-ready states the static adapter never produces) and publish them
// through the same seam.
class InMemoryClusterControl final : public ClusterControlPort {
 public:
  void SetTarget(std::shared_ptr<const ServingState> state);
  absl::Status RefreshTarget(TopologyCache& cache) override;

 private:
  std::shared_ptr<const ServingState> pending_;
};

}  // namespace keylane::cluster
