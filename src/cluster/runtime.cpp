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

#include "lavik/cluster/runtime.h"

namespace lavik::cluster {

namespace {
std::unique_ptr<ClusterRuntime> g_cluster_runtime;
ClientMode g_client_mode = ClientMode::kSingle;
}  // namespace

ClusterRuntime::ClusterRuntime(
    std::unique_ptr<NodeControlActions> actions,
    AuthorityGuard::RetirementCallback retirement_callback)
    : control_actions_(std::move(actions)),
      authority_guard_(topology_cache_, retirement_callback),
      node_control_installer_(
          topology_cache_, authority_guard_,
          control_actions_ == nullptr
              ? static_cast<NodeControlActions&>(null_control_actions_)
              : *control_actions_) {}

ClusterRuntime* GetClusterRuntime() noexcept { return g_cluster_runtime.get(); }

ClientMode GetClientMode() noexcept { return g_client_mode; }

bool IsClusterClientMode() noexcept {
  return g_client_mode == ClientMode::kCluster;
}

bool MetaManaged() noexcept { return g_cluster_runtime != nullptr; }

void SetClientMode(ClientMode mode) noexcept { g_client_mode = mode; }

void InstallClusterRuntime(std::unique_ptr<ClusterRuntime> runtime) noexcept {
  g_cluster_runtime = std::move(runtime);
}

}  // namespace lavik::cluster
