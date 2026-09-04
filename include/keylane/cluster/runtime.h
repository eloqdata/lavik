#pragma once

// Process-wide cluster data-plane runtime. Installed once at
// startup when `cluster-enabled yes`, before any client connection is
// served; nullptr in standalone mode. Everything except the TopologyCache is
// immutable after installation, so readers need no synchronization.

#include <cstdint>
#include <memory>
#include <string>

#include "keylane/cluster/topology.h"

namespace keylane::cluster {

struct ClusterRuntime {
  TopologyCache topology_cache_;
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
