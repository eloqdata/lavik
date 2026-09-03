#include "keylane/cluster/runtime.h"

namespace keylane::cluster {

namespace {
std::unique_ptr<ClusterRuntime> g_cluster_runtime;
}

ClusterRuntime* GetClusterRuntime() noexcept { return g_cluster_runtime.get(); }

bool ClusterEnabled() noexcept { return g_cluster_runtime != nullptr; }

void InstallClusterRuntime(std::unique_ptr<ClusterRuntime> runtime) noexcept {
  g_cluster_runtime = std::move(runtime);
}

}  // namespace keylane::cluster
