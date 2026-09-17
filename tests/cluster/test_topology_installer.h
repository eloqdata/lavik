#pragma once

// Test-only adapter for installing programmatic topology. It deliberately
// drives the same FDS projection and finite, session-scoped lease boundaries
// as Meta control; production targets neither compile nor link this header.

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>

#include "absl/status/status.h"
#include "keylane/cluster/node_control.h"

namespace keylane::cluster::testing {

class TestTopologyInstaller {
 public:
  TestTopologyInstaller(NodeControlInstaller& installer,
                        TopologyCache& topology)
      : installer_(installer), topology_(topology) {
    std::array<std::uint8_t, SessionId::kByteSize> session_bytes{};
    session_bytes.back() = 1;
    session_ = SessionIdentity{
        .session_id_ = SessionId::FromBytes(session_bytes),
        .generation_ = 1,
        .data_boot_id_ =
            *NodeId::Parse("dddddddddddddddddddddddddddddddddddddddd"),
    };
  }

  // Installs one complete state under a fresh synthetic Meta projection and
  // grants finite authority only to committed, locally primary groups. A
  // successful state publication advances the projection even if a later
  // lease grant fails; callers may then inspect the installed fail-closed
  // state or submit another transition. Storage readiness is process-local and
  // remains true after any subsequent failure.
  absl::Status Install(
      std::shared_ptr<const ServingState> state, MonotonicTime now,
      MonotonicDuration lease_duration = std::chrono::hours(1)) {
    if (state == nullptr) {
      return absl::InvalidArgumentError("test topology is empty");
    }
    if (absl::Status ready = installer_.SetStorageReady(true); !ready.ok()) {
      return ready;
    }
    const ProjectionBasis projection{
        .control_revision_ = next_source_index_,
    };
    if (absl::Status installed = installer_.InstallFullState(
            PreparedFullState{
                .serving_state_ = std::move(state),
            },
            projection);
        !installed.ok()) {
      return installed;
    }
    ++next_source_index_;

    const std::shared_ptr<const ServingState> installed = topology_.Current();
    for (const GroupView& group : installed->Groups()) {
      if (!group.granted_ ||
          group.primary_node_index_ != installed->SelfNodeIndex()) {
        continue;
      }
      const AuthorityAnchor anchor{
          .group_id_ = group.group_id_,
          .assignment_id_ = group.assignment_id_,
          .group_term_ = group.group_term_,
      };
      if (absl::Status granted = installer_.ApplyAuthority(
              AuthorityMessage{
                  .kind_ = AuthorityMessage::Kind::kLeaseGrant,
                  .session_ = session_,
                  .projection_ = projection,
                  .anchor_ = anchor,
                  .sent_at_ = now,
                  .granted_duration_ = lease_duration,
              },
              now);
          !granted.ok()) {
        return granted;
      }
    }
    return absl::OkStatus();
  }

 private:
  NodeControlInstaller& installer_;
  TopologyCache& topology_;
  SessionIdentity session_;
  std::uint64_t next_source_index_ = 1;
};

}  // namespace keylane::cluster::testing
