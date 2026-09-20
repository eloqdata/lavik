/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

// Process-wide cluster data-plane runtime. Installed once at
// startup when `meta-managed yes`, before any client connection is
// served; nullptr when not Meta-managed. The object graph and advertised
// endpoints are immutable after installation; the cache, authority guard, and
// node controller own their documented synchronized/worker-affine state.

#include <cstdint>
#include <memory>
#include <string>

#include "lavik/client_mode.h"
#include "lavik/cluster/node_control.h"
#include "lavik/cluster/topology.h"

namespace lavik::cluster {

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
  // announce-* values after defaults resolve). An empty host keeps
  // the existing wildcard-bind convention: clients dial the startup node's
  // address (see ClusterSlotsHost in src/redis/command.cpp).
  std::string announce_ip_;
  std::uint16_t announce_port_ = 0;
  std::uint16_t announce_tls_port_ = 0;
};

ClusterRuntime* GetClusterRuntime() noexcept;
// Client protocol semantics never depend on whether a Meta runtime exists.
ClientMode GetClientMode() noexcept;
// Convenience predicate for Cluster-only Redis behavior.
bool IsClusterClientMode() noexcept;
// A managed process installs its runtime before any worker starts serving.
bool MetaManaged() noexcept;
// Startup-only; tests must restore the default after tearing down their
// runtime.
void SetClientMode(ClientMode mode) noexcept;

// Installs the runtime during startup. Not thread-safe by design: call it
// before worker threads begin serving.
void InstallClusterRuntime(std::unique_ptr<ClusterRuntime> runtime) noexcept;

}  // namespace lavik::cluster
