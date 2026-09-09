#pragma once

// Data-side adapter from authenticated control-protocol values into the
// immutable serving domain. Decoding a frame is intentionally insufficient:
// this boundary validates node/group relationships and builds the complete
// snapshot before NodeControlInstaller can publish anything.

#include <cstddef>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/cluster/node_control.h"

namespace keylane::cluster {

absl::StatusOr<PreparedFullState> PrepareMetaFullState(
    const control::FullDesiredState& desired, std::string_view local_node_id,
    std::size_t request_worker_count);

}  // namespace keylane::cluster
