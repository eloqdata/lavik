#pragma once

// Process-wide cluster data-plane runtime. Installed once at
// startup when `cluster-enabled yes`, before any client connection is
// served; nullptr in standalone mode. The object graph and advertised
// endpoints are immutable after installation; the cache, authority guard, and
// node controller own their documented synchronized/worker-affine state.

#include <cstdint>
#include <memory>
#include <string>

#include "keylane/cluster/node_control.h"
#include "keylane/cluster/topology.h"

namespace keylane::cluster {

struct ClusterRuntime {
  explicit ClusterRuntime(
      std::unique_ptr<NodeControlActions> actions = nullptr);

  TopologyCache topology_cache_;
  NullNodeControlActions null_control_actions_;
  // Production owns the ReplicationManager adapter for exactly as long as the
  // installer can dispatch into it. Focused tests may use the no-op adapter.
  std::unique_ptr<NodeControlActions> control_actions_;
  AuthorityGuard authority_guard_;
  NodeControlInstaller node_control_installer_;
  // Advertised address of this node for discovery self entries (the
  // cluster-announce-* values after defaults resolve). An empty host keeps
  // the existing wildcard-bind convention: clients dial the startup node's
  // address (see ClusterSlotsHost in src/redis/command.cpp).
  std::string announce_ip_;
  std::uint16_t announce_port_ = 0;
  std::uint16_t announce_tls_port_ = 0;
};

ClusterRuntime* GetClusterRuntime() noexcept;
bool ClusterEnabled() noexcept;

// Installs the runtime during startup. Not thread-safe by design: call it
// before worker threads begin serving.
void InstallClusterRuntime(std::unique_ptr<ClusterRuntime> runtime) noexcept;

}  // namespace keylane::cluster
