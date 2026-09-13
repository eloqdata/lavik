#include "keylane/cluster/runtime.h"

namespace keylane::cluster {

namespace {
std::unique_ptr<ClusterRuntime> g_cluster_runtime;
}

ClusterRuntime::ClusterRuntime(std::unique_ptr<NodeControlActions> actions)
    : control_actions_(std::move(actions)),
      authority_guard_(topology_cache_),
      node_control_installer_(
          topology_cache_, authority_guard_,
          control_actions_ == nullptr
              ? static_cast<NodeControlActions&>(null_control_actions_)
              : *control_actions_) {}

ClusterRuntime* GetClusterRuntime() noexcept { return g_cluster_runtime.get(); }

bool ClusterEnabled() noexcept { return g_cluster_runtime != nullptr; }

void InstallClusterRuntime(std::unique_ptr<ClusterRuntime> runtime) noexcept {
  g_cluster_runtime = std::move(runtime);
}

}  // namespace keylane::cluster
