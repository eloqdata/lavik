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

#include "lavik/meta/ctl_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <exception>
#include <future>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/connection.h"
#include "bycorf/net/tcp_listener.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/net/tls.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/cluster/control_transport.h"
#include "lavik/meta/automatic_failover_detector.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/cluster_create_reconciler.h"
#include "lavik/meta/cluster_status.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/coordinator.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/failover_admin.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/identity_verifier.h"
#include "lavik/meta/membership_reconciler.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/population_manifest_store.h"
#include "lavik/meta/proposal_executor.h"
#include "lavik/meta/raft.h"
#include "lavik/meta/state_machine.h"
#include "lavik/numeric_endpoint.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {

// All Core members below the bind status are worker-thread only; the Core
// outlives individual sessions via shared_ptr.
struct MetaCtlServer::Core {
  bycorf::ForeignExecutor foreign_executor_;
  std::shared_ptr<MetaRaft> server_;
  std::shared_ptr<MetaStateMachine> state_machine_;
  std::shared_ptr<MetaCoordinator> coordinator_;
  // Leader-local observation store. Internally serialized because
  // ctl ingestion and commit-driven revalidation run on different threads.
  std::shared_ptr<MetaObservationStore> obs_store_;
  // Non-owning. Process assembly keeps the executor alive until after the
  // Bycorf worker and all session coroutines have stopped.
  MetaProposalExecutor* proposal_executor_ = nullptr;
  std::shared_ptr<MetaMembershipGate> membership_gate_;
  MetaCtlServerOptions options_;
  std::shared_ptr<bycorf::TlsContext> tls_context_;

  mutable std::mutex status_mu_;
  absl::Status status_ = absl::Status(absl::StatusCode::kUnavailable,
                                      "bind has not run on the worker yet");

  // Worker-thread only below.
  bycorf::Worker* worker_ = nullptr;
  bycorf::TcpListener listener_;
  bool listening_ = false;
  bool shutdown_ = false;
  bool accept_loop_running_ = false;
  int shutdown_accept_wake_fd_ = -1;
  std::vector<bycorf::Connection*> sessions_;
  std::vector<std::shared_ptr<std::promise<void>>> shutdown_drain_waiters_;
  std::atomic<bool> shutdown_complete_{false};
};

bool detail::IsStableClusterStatusBracket(
    const MetaClusterStatusBracket& before,
    const MetaClusterStatusBracket& after) {
  return before.is_leader_ && before.leader_alive_ &&
         before.leadership_.leader_authority_eligible_ && after.is_leader_ &&
         after.leader_alive_ && after.leadership_.leader_authority_eligible_ &&
         after.term_ == before.term_ &&
         after.config_index_ == before.config_index_ &&
         after.config_server_ids_ == before.config_server_ids_ &&
         after.active_meta_members_ == before.active_meta_members_ &&
         after.leadership_.leadership_generation_ ==
             before.leadership_.leadership_generation_ &&
         after.leadership_.leader_authority_eligibility_revision_ ==
             before.leadership_.leader_authority_eligibility_revision_;
}

bool detail::IsCurrentAutomaticFailoverDiagnostics(
    const MetaDataControlRuntimeSnapshot& runtime,
    const MetaAutomaticFailoverDiagnosticsSnapshot& detector,
    std::uint64_t committed_applied_index) {
  return detector.leadership_generation_ == runtime.leadership_generation_ &&
         detector.leader_authority_eligibility_revision_ ==
             runtime.leader_authority_eligibility_revision_ &&
         detector.evaluated_applied_index_ == committed_applied_index;
}

bool detail::ParseAdminPolicyVersion(std::string_view text,
                                     std::uint64_t* version) {
  if (version == nullptr || text.empty() || text == "0" ||
      text.front() == '0') {
    return false;
  }
  std::uint64_t value = 0;
  for (const char c : text) {
    if (c < '0' || c > '9') return false;
    const std::uint64_t digit = static_cast<std::uint64_t>(c - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  *version = value;
  return true;
}

std::string_view detail::ClusterCreateMissingSessionBlocker(
    bool retry_observed, bool session_observed) {
  return retry_observed || session_observed ? "data_session_missing"
                                            : "data_unobserved";
}

void detail::ApplyClusterRuntimeObservation(
    ClusterDataNodeWireV1& node, const MetaDataControlRuntimeNode& runtime_node,
    const MetaCommittedStatusView& view, const ClusterCaptureWireV1& capture,
    std::int64_t now_unix_ms, std::uint32_t observation_ttl_ms) {
  node.lease_status_ = ClusterLeaseStatus::kUnknown;
  node.health_fresh_ = runtime_node.health_.has_value() &&
                       runtime_node.health_received_unix_ms_ <= now_unix_ms &&
                       now_unix_ms - runtime_node.health_received_unix_ms_ <=
                           observation_ttl_ms &&
                       runtime_node.health_->storage_ready &&
                       !runtime_node.health_->draining;
  node.population_current_ = node.projection_current_ && node.health_fresh_ &&
                             runtime_node.health_->population_ready;
  if (runtime_node.last_lease_decision_.has_value()) {
    if (const auto* granted = std::get_if<cluster::control::LeaseGranted>(
            &*runtime_node.last_lease_decision_)) {
      const auto committed_group = std::find_if(
          view.groups_.begin(), view.groups_.end(), [&](const auto& group) {
            return group.topology_.group_id_ == granted->group_id;
          });
      const std::uint64_t freshness_ms = std::min<std::uint64_t>(
          granted->granted_duration_ms, observation_ttl_ms);
      const bool recent =
          runtime_node.lease_decision_written_unix_ms_ <= now_unix_ms &&
          static_cast<std::uint64_t>(
              now_unix_ms - runtime_node.lease_decision_written_unix_ms_) <=
              freshness_ms;
      bool current_assignment = false;
      if (committed_group != view.groups_.end() &&
          committed_group->grant_.grant_.has_value() &&
          committed_group->grant_.grant_->owner_ == node.node_id_) {
        const auto owner_member =
            std::find_if(committed_group->topology_.members_.begin(),
                         committed_group->topology_.members_.end(),
                         [&](const MetaGroupMember& member) {
                           return member.node_id_ == node.node_id_;
                         });
        current_assignment =
            owner_member != committed_group->topology_.members_.end() &&
            owner_member->assignment_id_ == granted->assignment_id;
      }
      const bool current_grant =
          committed_group != view.groups_.end() &&
          committed_group->grant_.grant_.has_value() && current_assignment &&
          granted->leader_id == capture.responder_id_ &&
          granted->raft_term == capture.term_ &&
          granted->leadership_generation ==
              runtime_node.leadership_generation_ &&
          granted->data_boot_id == runtime_node.boot_id_ &&
          granted->control_revision == runtime_node.control_revision_ &&
          granted->group_term == committed_group->grant_.group_term_;
      // Health arrives before the corresponding Ack finishes writing. An old
      // successful grant is not current readiness evidence after health drops,
      // even while that Ack is queued or when heartbeat freshness expires.
      node.lease_status_ =
          recent && current_grant && node.projection_current_ &&
                  node.health_fresh_ && node.population_current_
              ? ClusterLeaseStatus::kRecentlyGranted
              : ClusterLeaseStatus::kUnknown;
    } else if (!std::holds_alternative<cluster::control::NoChallenge>(
                   *runtime_node.last_lease_decision_)) {
      const bool recent =
          runtime_node.lease_decision_written_unix_ms_ <= now_unix_ms &&
          now_unix_ms - runtime_node.lease_decision_written_unix_ms_ <=
              observation_ttl_ms;
      node.lease_status_ =
          recent ? ClusterLeaseStatus::kDenied : ClusterLeaseStatus::kUnknown;
    }
  }
}

namespace {

// One oversized partial line already proves a broken or hostile peer; the
// Control-plane payloads are bounded, so cap the assembly buffer hard.
constexpr std::size_t kMaxLineBytes = 64 * 1024;
constexpr auto kClusterStatusSendDeadline = std::chrono::seconds(5);

std::string HexEncode(std::string_view bytes);

void NotifyCtlShutdownDrained(auto& core) {
  if (!core.shutdown_ || core.accept_loop_running_ || !core.sessions_.empty()) {
    return;
  }
  // Session coroutines retain their own dependencies while parked on a
  // proposal. Releasing the Core copies only after the accept/session drain
  // makes Admin shutdown a real lifecycle barrier before Data-control stops.
  core.server_.reset();
  core.state_machine_.reset();
  core.coordinator_.reset();
  core.obs_store_.reset();
  core.tls_context_.reset();
  for (const auto& waiter : core.shutdown_drain_waiters_) waiter->set_value();
  core.shutdown_drain_waiters_.clear();
}

absl::StatusOr<int> OpenCtlShutdownAcceptWakeSocket(
    const MetaCtlServerOptions& options) {
  sockaddr_storage destination{};
  socklen_t destination_size = 0;
  int family = AF_UNSPEC;
  int protocol = 0;

  if (options.transport_ == MetaCtlServerOptions::Transport::kUnix) {
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (options.unix_socket_path_.size() >= sizeof(address.sun_path)) {
      return absl::InvalidArgumentError("ctl shutdown wake path is too long");
    }
    std::memcpy(address.sun_path, options.unix_socket_path_.c_str(),
                options.unix_socket_path_.size() + 1);
    family = AF_UNIX;
    destination_size = sizeof(address);
    std::memcpy(&destination, &address, sizeof(address));
  } else {
    sockaddr_in address4{};
    address4.sin_family = AF_INET;
    address4.sin_port = htons(options.port_);
    if (::inet_pton(AF_INET, options.bind_host_.c_str(), &address4.sin_addr) ==
        1) {
      family = AF_INET;
      protocol = IPPROTO_TCP;
      destination_size = sizeof(address4);
      std::memcpy(&destination, &address4, sizeof(address4));
    } else {
      sockaddr_in6 address6{};
      address6.sin6_family = AF_INET6;
      address6.sin6_port = htons(options.port_);
      if (::inet_pton(AF_INET6, options.bind_host_.c_str(),
                      &address6.sin6_addr) != 1) {
        return absl::InvalidArgumentError(
            "ctl shutdown wake host is not numeric");
      }
      family = AF_INET6;
      protocol = IPPROTO_TCP;
      destination_size = sizeof(address6);
      std::memcpy(&destination, &address6, sizeof(address6));
    }
  }

  const int fd =
      ::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, protocol);
  if (fd < 0) {
    return absl::ErrnoToStatus(errno, "create ctl shutdown wake socket");
  }
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&destination),
                destination_size) != 0 &&
      errno != EINPROGRESS && errno != EALREADY && errno != EISCONN) {
    const absl::Status status =
        absl::ErrnoToStatus(errno, "connect ctl shutdown wake socket");
    (void)::close(fd);
    return status;
  }
  return fd;
}

std::vector<std::uint32_t> ConfigServerIds(
    const std::shared_ptr<MetaRaftConfig>& config) {
  std::vector<std::uint32_t> ids;
  if (config == nullptr) return ids;
  for (const std::shared_ptr<MetaRaftMember>& member : config->get_servers()) {
    if (member != nullptr && member->get_id() > 0) {
      ids.push_back(static_cast<std::uint32_t>(member->get_id()));
    }
  }
  std::sort(ids.begin(), ids.end());
  return ids;
}

std::vector<MetaMemberRecord> ActiveMetaMembers(
    const MetaCommittedStatusView& view) {
  std::vector<MetaMemberRecord> members;
  for (const MetaMemberRecord& member : view.meta_members_) {
    if (!member.retired_) members.push_back(member);
  }
  std::sort(members.begin(), members.end(),
            [](const auto& left, const auto& right) {
              return left.server_id_ < right.server_id_;
            });
  return members;
}

std::vector<ClusterMetaMemberWireV1> StatusMembers(
    const std::vector<MetaMemberRecord>& members, std::int32_t leader_id) {
  std::vector<ClusterMetaMemberWireV1> wire;
  wire.reserve(members.size());
  for (const auto& member : members) {
    wire.push_back({.server_id_ = member.server_id_,
                    .ctl_endpoint_ = member.ctl_endpoint_,
                    .is_leader_ = leader_id > 0 &&
                                  member.server_id_ ==
                                      static_cast<std::uint32_t>(leader_id)});
  }
  return wire;
}

std::string BuildClusterHeadReply(
    const std::shared_ptr<MetaRaft>& server,
    const std::shared_ptr<MetaStateMachine>& state_machine) {
  const MetaCommittedStatusView view = state_machine->StatusSnapshot();
  const auto members = ActiveMetaMembers(view);
  const std::shared_ptr<MetaRaftConfig> config = server->get_config();
  if (config == nullptr || members.empty()) return "ERR leader_not_caught_up";
  // Use one role observation for both fields. A promotion can otherwise land
  // between two is_leader() reads and create a structurally corrupt head that
  // the client must treat as fatal instead of retrying ordinary term churn.
  const bool responder_is_leader = server->is_leader();
  const std::int32_t leader_id =
      responder_is_leader ? server->get_id() : server->get_leader();
  if (leader_id <= 0) return "ERR leader_unknown";
  if ((leader_id == server->get_id()) != responder_is_leader) {
    return "ERR cut_changed";
  }
  const auto responder = std::find_if(
      members.begin(), members.end(), [&](const MetaMemberRecord& member) {
        return member.server_id_ ==
               static_cast<std::uint32_t>(server->get_id());
      });
  const auto leader = std::find_if(
      members.begin(), members.end(), [&](const MetaMemberRecord& member) {
        return member.server_id_ == static_cast<std::uint32_t>(leader_id);
      });
  if (responder == members.end() || leader == members.end()) {
    return "ERR leader_not_caught_up";
  }
  ClusterHeadWireV1 head;
  head.responder_id_ = static_cast<std::uint32_t>(server->get_id());
  head.role_ = responder_is_leader ? ClusterMetaRole::kLeader
                                   : ClusterMetaRole::kFollower;
  head.term_ = server->get_term();
  if (leader_id > 0) head.leader_id_ = static_cast<std::uint32_t>(leader_id);
  head.config_index_ = config->get_log_idx();
  head.meta_members_ = StatusMembers(members, leader_id);
  auto encoded = EncodeClusterHeadReply(head);
  return encoded.ok() ? std::move(*encoded) : "ERR state_corrupt";
}

std::string BuildClusterStatusReply(
    const std::shared_ptr<MetaRaft>& server,
    const std::shared_ptr<MetaStateMachine>& state_machine,
    const std::shared_ptr<MetaDataControlRuntimeStatus>& runtime_status,
    const std::shared_ptr<MetaAutomaticFailoverDiagnosticsRegistry>&
        automatic_failover_diagnostics,
    std::uint32_t observation_ttl_ms) {
  const bool before_is_leader = server->is_leader();
  const bool before_leader_alive = server->is_leader_alive();
  if (!before_is_leader) return "ERR not_leader";
  if (!before_leader_alive) return "ERR leader_not_caught_up";

  const std::uint64_t before_term = server->get_term();
  const std::shared_ptr<MetaRaftConfig> before_config = server->get_config();
  if (before_config == nullptr) return "ERR leader_not_caught_up";
  const std::uint64_t before_config_index = before_config->get_log_idx();
  const std::vector<std::uint32_t> before_config_ids =
      ConfigServerIds(before_config);

  // Capture volatile session facts first, then take one atomic compact
  // committed view. Compatibility checks below admit only runtime facts whose
  // FDS and authority anchors still describe that committed cut.
  const MetaDataControlRuntimeSnapshot runtime = runtime_status->Snapshot();
  if (!runtime.leader_authority_eligible_ ||
      runtime.leadership_generation_ == 0) {
    return "ERR leader_not_caught_up";
  }
  const MetaAutomaticFailoverDiagnosticsSnapshot detector =
      automatic_failover_diagnostics->Snapshot();
  const MetaCommittedStatusView view = state_machine->StatusSnapshot();
  if (view.applied_index_ < server->get_committed_log_idx()) {
    return "ERR leader_not_caught_up";
  }
  if (!detail::IsCurrentAutomaticFailoverDiagnostics(runtime, detector,
                                                     view.applied_index_)) {
    return "ERR cut_changed";
  }
  const auto active_meta_members = ActiveMetaMembers(view);
  if (std::none_of(active_meta_members.begin(), active_meta_members.end(),
                   [&](const MetaMemberRecord& member) {
                     return member.server_id_ ==
                            static_cast<std::uint32_t>(server->get_id());
                   })) {
    return "ERR leader_not_caught_up";
  }
  const detail::MetaClusterStatusBracket before_bracket{
      .is_leader_ = before_is_leader,
      .leader_alive_ = before_leader_alive,
      .term_ = before_term,
      .config_index_ = before_config_index,
      .config_server_ids_ = before_config_ids,
      .active_meta_members_ = active_meta_members,
      .leadership_ = {.leadership_generation_ = runtime.leadership_generation_,
                      .leader_authority_eligible_ =
                          runtime.leader_authority_eligible_,
                      .leader_authority_eligibility_revision_ =
                          runtime.leader_authority_eligibility_revision_},
  };

  ClusterStatusWireV1 status;
  status.capture_ = {
      .responder_id_ = static_cast<std::uint32_t>(server->get_id()),
      .term_ = before_term,
      .config_index_ = before_config_index,
      .committed_index_ = view.applied_index_,
      .topology_epoch_ = view.topology_epoch_,
  };
  status.lifecycle_revision_ = view.cluster_lifecycle_.Revision();
  switch (view.cluster_lifecycle_.state_) {
    case MetaClusterLifecycle::kUninitialized:
      status.cluster_state_ = view.cluster_non_pristine_
                                  ? ClusterStateWireV1::kNonPristine
                                  : ClusterStateWireV1::kUninitialized;
      break;
    case MetaClusterLifecycle::kCreating:
      status.cluster_state_ = ClusterStateWireV1::kCreating;
      break;
    case MetaClusterLifecycle::kCreated:
      status.cluster_state_ = ClusterStateWireV1::kCreated;
      break;
    case MetaClusterLifecycle::kProvisioningFailed:
      status.cluster_state_ = ClusterStateWireV1::kProvisioningFailed;
      break;
  }
  if (view.cluster_lifecycle_.state_ != MetaClusterLifecycle::kUninitialized) {
    status.root_operation_id_ = HexEncode(
        std::string_view(reinterpret_cast<const char*>(
                             view.cluster_lifecycle_.root_operation_id_.data()),
                         view.cluster_lifecycle_.root_operation_id_.size()));
    status.genesis_commit_index_ =
        view.cluster_lifecycle_.genesis_commit_index_;
  }
  if (view.cluster_lifecycle_.state_ == MetaClusterLifecycle::kCreating) {
    status.cluster_create_phase_ = view.active_cluster_create_phase_.empty()
                                       ? std::string("submitted")
                                       : view.active_cluster_create_phase_;
  }
  if (view.cluster_lifecycle_.state_ ==
      MetaClusterLifecycle::kProvisioningFailed) {
    status.provisioning_failure_summary_ =
        view.cluster_lifecycle_.failure_summary_;
  }
  status.meta_available_ = true;
  status.meta_members_ = StatusMembers(active_meta_members, server->get_id());
  if (view.active_cluster_create_operation_) {
    status.blockers_.push_back(
        {.code_ = std::string(kClusterCreateActiveBlockerCode),
         .scope_ = "cluster",
         .detail_ = "non_terminal_cluster_create_operation_exists"});
    if (view.active_cluster_create_phase_.empty() ||
        view.active_cluster_create_phase_ == "wait-meta-barrier") {
      status.blockers_.push_back(
          {.code_ = "meta_catching_up",
           .scope_ = "meta",
           .detail_ = "initial_meta_members_have_not_applied_create_barrier"});
    }
    for (const std::string& node_id : view.active_cluster_create_data_nodes_) {
      const auto identity = std::find_if(
          view.data_nodes_.begin(), view.data_nodes_.end(),
          [&](const MetaNodeRecord& node) { return node.node_id_ == node_id; });
      const bool retrying =
          std::find(runtime.unregistered_retries_.begin(),
                    runtime.unregistered_retries_.end(),
                    node_id) != runtime.unregistered_retries_.end();
      if (identity == view.data_nodes_.end()) {
        status.blockers_.push_back(
            {.code_ =
                 retrying ? "data_unregistered_retrying" : "data_unregistered",
             .scope_ = "node:" + node_id,
             .detail_ = retrying ? "hello_rejected_until_identity_commits"
                                 : "declared_identity_not_committed"});
        continue;
      }
      const bool observed =
          std::any_of(runtime.nodes_.begin(), runtime.nodes_.end(),
                      [&](const MetaDataControlRuntimeNode& node) {
                        return node.node_id_ == node_id;
                      });
      if (!observed) {
        const bool session_observed =
            std::binary_search(runtime.observed_nodes_.begin(),
                               runtime.observed_nodes_.end(), node_id);
        const std::string_view blocker =
            detail::ClusterCreateMissingSessionBlocker(retrying,
                                                       session_observed);
        status.blockers_.push_back(
            {.code_ = std::string(blocker),
             .scope_ = "node:" + node_id,
             .detail_ = blocker == "data_session_missing"
                            ? "observed_node_has_no_current_session"
                            : "registered_node_has_not_contacted_this_leader"});
      }
    }
  }

  std::vector<std::uint32_t> committed_ids;
  std::vector<std::string> ctl_endpoints;
  bool complete_ctl_directory = active_meta_members.size() <= 1;
  for (const auto& member : active_meta_members) {
    committed_ids.push_back(member.server_id_);
    if (member.ctl_endpoint_.has_value()) {
      ctl_endpoints.push_back(*member.ctl_endpoint_);
    } else if (active_meta_members.size() > 1) {
      complete_ctl_directory = false;
    }
  }
  std::sort(committed_ids.begin(), committed_ids.end());
  std::sort(ctl_endpoints.begin(), ctl_endpoints.end());
  const bool unique_ctl_endpoints =
      std::adjacent_find(ctl_endpoints.begin(), ctl_endpoints.end()) ==
      ctl_endpoints.end();
  if (active_meta_members.size() > 1 &&
      ctl_endpoints.size() == active_meta_members.size()) {
    complete_ctl_directory = true;
  }
  status.meta_membership_stable_ = committed_ids == before_config_ids &&
                                   complete_ctl_directory &&
                                   unique_ctl_endpoints;
  if (!status.meta_membership_stable_) {
    status.blockers_.push_back(
        {.code_ = "meta_membership_unstable",
         .scope_ = "meta",
         .detail_ = "committed_identity_or_ctl_directory_mismatch"});
  }

  for (const MetaNodeRecord& record : view.data_nodes_) {
    ClusterDataNodeWireV1 node;
    node.node_id_ = record.node_id_;
    node.role_ = record.role_ == MetaNodeRole::kPrimary
                     ? ClusterDataNodeRole::kPrimary
                     : ClusterDataNodeRole::kReplica;
    node.retired_ = record.retired_;
    const auto runtime_node = std::find_if(
        runtime.nodes_.begin(), runtime.nodes_.end(),
        [&](const auto& item) { return item.node_id_ == record.node_id_; });
    node.current_session_ =
        !record.retired_ && runtime_node != runtime.nodes_.end();
    for (const auto& group : view.groups_) {
      const auto membership = std::find_if(
          group.topology_.members_.begin(), group.topology_.members_.end(),
          [&](const MetaGroupMember& item) {
            return item.node_id_ == record.node_id_;
          });
      if (membership != group.topology_.members_.end()) {
        node.group_id_ = group.topology_.group_id_;
        // Node registration role is only a bootstrap hint. Once a Group
        // exists, its committed Owner is the sole role authority; otherwise
        // cluster-status would continue labelling the original primary as
        // primary after a successful cutover.
        node.role_ = group.topology_.record_.owner_ == record.node_id_
                         ? ClusterDataNodeRole::kPrimary
                         : ClusterDataNodeRole::kReplica;
        break;
      }
    }
    if (runtime_node != runtime.nodes_.end()) {
      node.projection_current_ =
          runtime_node->validated_committed_high_water_ >=
              view.applied_index_ &&
          runtime_node->topology_epoch_ == view.topology_epoch_;
      if (node.group_id_.has_value()) {
        const auto committed_group = std::find_if(
            view.groups_.begin(), view.groups_.end(), [&](const auto& group) {
              return group.topology_.group_id_ == *node.group_id_;
            });
        const auto projected_group =
            std::find_if(runtime_node->groups_.begin(),
                         runtime_node->groups_.end(), [&](const auto& group) {
                           return group.group_id_ == *node.group_id_;
                         });
        const auto committed_member =
            committed_group == view.groups_.end()
                ? std::vector<MetaGroupMember>::const_iterator{}
                : std::find_if(committed_group->topology_.members_.begin(),
                               committed_group->topology_.members_.end(),
                               [&](const MetaGroupMember& member) {
                                 return member.node_id_ == record.node_id_;
                               });
        const bool anchors_match =
            committed_group != view.groups_.end() &&
            projected_group != runtime_node->groups_.end() &&
            committed_member != committed_group->topology_.members_.end() &&
            projected_group->assignment_id_ ==
                committed_member->assignment_id_ &&
            projected_group->group_term_ ==
                committed_group->grant_.group_term_ &&
            projected_group->manifest_revision_ ==
                committed_group->topology_.record_
                    .population_manifest_revision_ &&
            projected_group->manifest_digest_ ==
                committed_group->topology_.record_
                    .population_manifest_digest_ &&
            projected_group->partition_replication_epoch_ ==
                committed_group->topology_.record_.partition_replication_epoch_;
        node.projection_current_ = node.projection_current_ && anchors_match;
      }
      detail::ApplyClusterRuntimeObservation(
          node, *runtime_node, view, status.capture_,
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count(),
          observation_ttl_ms);
    }
    status.data_nodes_.push_back(std::move(node));
  }

  status.topology_converged_ = true;
  for (const MetaCommittedStatusGroup& source : view.groups_) {
    ClusterGroupWireV1 group;
    group.group_id_ = source.topology_.group_id_;
    group.term_ = source.grant_.group_term_;
    if (!source.topology_.record_.owner_.empty()) {
      group.owner_node_id_ = source.topology_.record_.owner_;
    }
    group.effective_threshold_ms_ = view.automatic_failover_threshold_ms_;

    const auto detector_status =
        std::find_if(detector.statuses_.begin(), detector.statuses_.end(),
                     [&](const MetaAutomaticFailoverStatus& item) {
                       return item.anchor_.group_id_ == group.group_id_;
                     });
    if (detector_status != detector.statuses_.end()) {
      group.blocked_reason_.reset();
      switch (detector_status->state_) {
        case MetaAutomaticFailoverState::kHealthy:
          group.automatic_failover_state_ =
              ClusterAutomaticFailoverState::kHealthy;
          break;
        case MetaAutomaticFailoverState::kSuspect:
          group.automatic_failover_state_ =
              ClusterAutomaticFailoverState::kSuspect;
          break;
        case MetaAutomaticFailoverState::kBlocked:
          group.automatic_failover_state_ =
              ClusterAutomaticFailoverState::kBlocked;
          break;
        case MetaAutomaticFailoverState::kTriggering:
          group.automatic_failover_state_ =
              ClusterAutomaticFailoverState::kTriggering;
          break;
      }
      if (detector_status->current_reason_ !=
          MetaOwnerServiceabilityReason::kNone) {
        group.current_reason_ = std::string(MetaOwnerServiceabilityReasonName(
            detector_status->current_reason_));
      }
      group.suspect_elapsed_ms_ = detector_status->accumulated_suspect_ms_;
      group.effective_threshold_ms_ = detector_status->effective_threshold_ms_;
      if (detector_status->blocker_ != MetaAutomaticFailoverBlocker::kNone) {
        group.blocked_reason_ = std::string(
            MetaAutomaticFailoverBlockerName(detector_status->blocker_));
      }
    } else {
      // A newly opened diagnostics bracket may precede its first complete
      // detector publication. Missing policies before Genesis also block
      // evaluation; automatic failover has no administrative off state.
      group.automatic_failover_state_ = ClusterAutomaticFailoverState::kBlocked;
      group.blocked_reason_ = "indeterminate_evidence";
    }
    group.topology_converged_ = true;
    for (const MetaGroupMember& member : source.topology_.members_) {
      const auto identity =
          std::find_if(view.data_nodes_.begin(), view.data_nodes_.end(),
                       [&](const MetaNodeRecord& node) {
                         return node.node_id_ == member.node_id_;
                       });
      // Retired identities remain visible for diagnosis but no longer
      // participate in convergence of the active committed topology.
      if (identity != view.data_nodes_.end() && identity->retired_) continue;
      const auto runtime =
          std::find_if(status.data_nodes_.begin(), status.data_nodes_.end(),
                       [&](const ClusterDataNodeWireV1& node) {
                         return node.node_id_ == member.node_id_;
                       });
      std::vector<std::string_view> missing;
      if (runtime == status.data_nodes_.end() || !runtime->current_session_)
        missing.push_back("session");
      if (runtime == status.data_nodes_.end() || !runtime->projection_current_)
        missing.push_back("projection");
      if (runtime == status.data_nodes_.end() || !runtime->health_fresh_)
        missing.push_back("health");
      if (runtime == status.data_nodes_.end() || !runtime->population_current_)
        missing.push_back("population");
      if (!missing.empty()) {
        group.topology_converged_ = false;
        std::string detail = "group=" + group.group_id_ + ";missing=";
        for (std::size_t index = 0; index < missing.size(); ++index) {
          if (index != 0) detail.push_back(',');
          detail.append(missing[index]);
        }
        status.blockers_.push_back({.code_ = "node_runtime_not_ready",
                                    .scope_ = "node:" + member.node_id_,
                                    .detail_ = std::move(detail)});
      }
    }
    group.serving_ready_ =
        source.grant_.grant_.has_value() && source.manifest_present_ &&
        source.policy_active_ && group.owner_node_id_.has_value() &&
        std::any_of(
            status.data_nodes_.begin(), status.data_nodes_.end(),
            [&](const ClusterDataNodeWireV1& node) {
              return node.node_id_ == *group.owner_node_id_ &&
                     node.current_session_ && node.projection_current_ &&
                     node.health_fresh_ && node.population_current_ &&
                     node.lease_status_ == ClusterLeaseStatus::kRecentlyGranted;
            });
    if (!group.topology_converged_) {
      status.topology_converged_ = false;
      status.blockers_.push_back(
          {.code_ = "group_runtime_not_converged",
           .scope_ = "group:" + group.group_id_,
           .detail_ = "current_projection_health_or_population_missing"});
    }
    const bool owns_slots =
        std::any_of(view.slot_ranges_.begin(), view.slot_ranges_.end(),
                    [&](const MetaCommittedStatusSlotRange& range) {
                      return range.group_id_ == group.group_id_;
                    });
    if (owns_slots && !group.serving_ready_) {
      status.blockers_.push_back(
          {.code_ = "group_not_serving",
           .scope_ = "group:" + group.group_id_,
           .detail_ = "authority_or_recent_lease_missing"});
    }
    status.groups_.push_back(std::move(group));
  }

  bool full_slot_coverage = !view.slot_ranges_.empty();
  std::uint32_t expected_first = 0;
  for (const MetaCommittedStatusSlotRange& source : view.slot_ranges_) {
    status.slot_ranges_.push_back({.first_ = source.first_,
                                   .last_ = source.last_,
                                   .group_id_ = source.group_id_});
    if (source.first_ != expected_first) full_slot_coverage = false;
    expected_first = source.last_ + 1;
  }
  full_slot_coverage = full_slot_coverage && expected_first == kMetaSlotCount;
  status.serving_ready_ =
      full_slot_coverage && !status.groups_.empty() &&
      std::all_of(
          status.slot_ranges_.begin(), status.slot_ranges_.end(),
          [&](const ClusterSlotRangeWireV1& range) {
            const auto group =
                std::find_if(status.groups_.begin(), status.groups_.end(),
                             [&](const ClusterGroupWireV1& item) {
                               return item.group_id_ == range.group_id_;
                             });
            return group != status.groups_.end() && group->serving_ready_;
          });
  if (!full_slot_coverage) {
    status.blockers_.push_back({.code_ = "slots_unassigned",
                                .scope_ = "cluster",
                                .detail_ = "coverage_is_not_0_through_16383"});
  }
  status.cluster_ready_ = status.meta_available_ &&
                          status.meta_membership_stable_ &&
                          status.serving_ready_ && status.topology_converged_;

  auto encoded = EncodeClusterStatusReply(status);
  if (!encoded.ok()) return "ERR state_corrupt";

  // Leadership/config/directory bracket: ordinary topology commits after the
  // compact snapshot do not invalidate that snapshot, but a leadership or
  // routing-identity change would make the response a mixed authority cut.
  const std::shared_ptr<MetaRaftConfig> after_config = server->get_config();
  const MetaCommittedStatusView after_view = state_machine->StatusSnapshot();
  const MetaDataControlLeadershipState after_leadership =
      runtime_status->LeadershipState();
  detail::MetaClusterStatusBracket after_bracket{
      .is_leader_ = server->is_leader(),
      .leader_alive_ = server->is_leader_alive(),
      .term_ = server->get_term(),
      .config_index_ = 0,
      .config_server_ids_ = {},
      .active_meta_members_ = {},
      .leadership_ = after_leadership,
  };
  if (after_config != nullptr) {
    after_bracket.config_index_ = after_config->get_log_idx();
    after_bracket.config_server_ids_ = ConfigServerIds(after_config);
  }
  after_bracket.active_meta_members_ = ActiveMetaMembers(after_view);
  if (after_config == nullptr ||
      !detail::IsStableClusterStatusBracket(before_bracket, after_bracket)) {
    return "ERR cut_changed";
  }
  return *encoded;
}

const char* AuditPolicyName(MetaAuditPolicy policy) {
  switch (policy) {
    case MetaAuditPolicy::kDisabled:
      return "disabled";
    case MetaAuditPolicy::kBoundedRotate:
      return "bounded-rotate";
    case MetaAuditPolicy::kStrictExport:
      return "strict-export";
  }
  return "unknown";
}

// Parking state for one asynchronous Raft round trip (append_entries,
// add_srv, remove_srv). Raft may complete inline before await_suspend(), so
// ready_ and waiter_ form a small handshake independent of mailbox timing.
struct AsyncReply {
  ~AsyncReply() {
    if (retained_status_service_ != nullptr && retained_status_bytes_ != 0) {
      retained_status_service_->Release(retained_status_bytes_);
    }
  }

  std::mutex mutex_;
  std::coroutine_handle<> waiter_{};
  std::string reply_;
  std::shared_ptr<MetaClusterStatusService> retained_status_service_;
  std::size_t retained_status_bytes_ = 0;
  bool ready_ = false;
  bool detached_ = false;
};

class AsyncReplyAwaiter {
 public:
  explicit AsyncReplyAwaiter(
      std::shared_ptr<AsyncReply> state,
      std::size_t* retained_status_bytes = nullptr) noexcept
      : state_(std::move(state)),
        retained_status_bytes_(retained_status_bytes) {}

  ~AsyncReplyAwaiter() {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    state_->detached_ = true;
    state_->waiter_ = {};
  }

  bool await_ready() const noexcept {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    return state_->ready_;
  }
  bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    if (state_->ready_) {
      return false;
    }
    state_->waiter_ = awaiting;
    return true;
  }
  std::string await_resume() noexcept {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    if (retained_status_bytes_ != nullptr) {
      *retained_status_bytes_ = state_->retained_status_bytes_;
      state_->retained_status_bytes_ = 0;
      state_->retained_status_service_.reset();
    }
    return std::move(state_->reply_);
  }

 private:
  std::shared_ptr<AsyncReply> state_;
  std::size_t* retained_status_bytes_;
};

void CompleteAsyncReply(
    bycorf::ForeignExecutor foreign_executor, std::shared_ptr<AsyncReply> state,
    std::string reply,
    std::shared_ptr<MetaClusterStatusService> retained_status_service = nullptr,
    std::size_t retained_status_bytes = 0) {
  std::coroutine_handle<> waiter;
  {
    std::lock_guard<std::mutex> lock(state->mutex_);
    if (state->detached_ || state->ready_) {
      if (retained_status_service != nullptr && retained_status_bytes != 0) {
        retained_status_service->Release(retained_status_bytes);
      }
      return;
    }
    state->reply_ = std::move(reply);
    state->retained_status_service_ = std::move(retained_status_service);
    state->retained_status_bytes_ = retained_status_bytes;
    state->ready_ = true;
    waiter = state->waiter_;
  }
  // An empty handle means completion won the race with await_suspend(); the
  // coroutine observes ready_ and continues without a mailbox round trip.
  if (waiter && !foreign_executor.Resume(waiter)) {
    // Runtime teardown starts only after Raft and the proposal executor are
    // quiescent. Rejection here therefore indicates a lifecycle violation
    // that would otherwise leave a session suspended forever.
    std::terminate();
  }
}

// Ids are generated before proposal; apply never manufactures randomness,
// preserving deterministic replay. A CSPRNG failure is a process-safety
// failure because continuing with a guessed or reused id would break the
// idempotency boundary.
cluster::control::WireId128 MakeRequiredId(std::string_view purpose) {
  auto id = cluster::control::GenerateId128();
  if (!id.ok()) {
    spdlog::critical("OS CSPRNG failed while generating {}: {}", purpose,
                     id.status().message());
    std::terminate();
  }
  return *id;
}

MetaRequestId MakeRequestId() { return MakeRequiredId("Meta request id"); }

MetaAssignmentId MakeAssignmentId() {
  return MakeRequiredId("membership assignment id");
}

MetaOperationId MakeOperationId() { return MakeRequiredId("operation id"); }

int HexNybble(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

// Exactly `hex_chars` hex digits -> bytes; false otherwise.
bool ParseHexBytes(const std::string& text, std::size_t hex_chars,
                   std::uint8_t* out) {
  if (text.size() != hex_chars || hex_chars % 2 != 0) {
    return false;
  }
  for (std::size_t ii = 0; ii < hex_chars / 2; ++ii) {
    const int hi = HexNybble(text[2 * ii]);
    const int lo = HexNybble(text[2 * ii + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    out[ii] = static_cast<std::uint8_t>((hi << 4) | lo);
  }
  return true;
}

bool ParseOperationId(const std::string& text, MetaOperationId& out) {
  return ParseHexBytes(text, 32, out.data());
}

bool ParseReplicationHistoryId(const std::string& text,
                               MetaReplicationHistoryId& out) {
  if (text.size() != 2 * out.size() ||
      std::any_of(text.begin(), text.end(), [](char c) {
        return !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
      })) {
    return false;
  }
  return ParseHexBytes(text, text.size(), out.data());
}

std::string ReplicationHistoryIdText(
    const MetaReplicationHistoryId& history_id) {
  return HexEncode(std::string_view(
      reinterpret_cast<const char*>(history_id.data()), history_id.size()));
}

std::string AssignmentIdText(const MetaAssignmentId& assignment_id) {
  return HexEncode(
      std::string_view(reinterpret_cast<const char*>(assignment_id.data()),
                       assignment_id.size()));
}

std::string HexEncode(std::string_view bytes) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (const unsigned char byte : bytes) {
    out.push_back(kHex[byte >> 4]);
    out.push_back(kHex[byte & 0x0f]);
  }
  return out;
}

// node_id on the ctl surface: the topology convention's 40 hex chars
// (kMetaNodeIdBytes), kept as text in the command.
bool IsNodeId(const std::string& text) {
  if (text.size() != kMetaNodeIdBytes) {
    return false;
  }
  for (const char c : text) {
    if (HexNybble(c) < 0) {
      return false;
    }
  }
  return true;
}

const char* LifecycleName(MetaOperationLifecycle lifecycle) {
  switch (lifecycle) {
    case MetaOperationLifecycle::kSubmitted:
      return "submitted";
    case MetaOperationLifecycle::kRunning:
      return "running";
    case MetaOperationLifecycle::kCompleted:
      return "completed";
    case MetaOperationLifecycle::kAborted:
      return "aborted";
  }
  return "unknown";
}

bool IsTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}

const char* RoleName(MetaNodeRole role) {
  return role == MetaNodeRole::kPrimary ? "primary" : "replica";
}

// ---------------------------------------------------------------------------
// Observation-surface helpers; see the header's verb reference.
// ---------------------------------------------------------------------------

// Wall clock for the VOLATILE observation store (receive time / TTL). The
// committed side never reads a clock — ApplyCommitted is clock-free by
// contract; this stamp is only used by the leader-local obs store.
std::int64_t NowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// Strict decimal u64 ("0" allowed, no signs/padding games, overflow rejects).
bool ParseU64(const std::string& text, std::uint64_t& out) {
  if (text.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  for (const char c : text) {
    if (c < '0' || c > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(c - '0');
    if (value > (UINT64_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  out = value;
  return true;
}

// The trusted {node_id, boot_incarnation, session_generation} triple of the
// obs verbs: node id and boot incarnation as 40 hex chars (20B), generation
// as decimal u64.
bool ParseObsIdentity(const std::vector<std::string>& tokens, std::size_t base,
                      MetaObservationIdentity& out) {
  if (!IsNodeId(tokens[base])) {
    return false;
  }
  out.node_id_ = tokens[base];
  if (!ParseHexBytes(tokens[base + 1], 2 * out.boot_incarnation_.size(),
                     out.boot_incarnation_.data())) {
    return false;
  }
  return ParseU64(tokens[base + 2], out.session_generation_);
}

const char* ObsAuditKindName(MetaObsAuditKind kind) {
  switch (kind) {
    case MetaObsAuditKind::kRejected:
      return "rejected";
    case MetaObsAuditKind::kStalePurged:
      return "stale-purged";
    case MetaObsAuditKind::kTtlExpired:
      return "ttl-expired";
  }
  return "unknown";
}

using CmdResult = MetaRaftResult;

// Proposes one encoded meta command and resolves to "OK <log_idx>" only when
// the entry commits and this leader's state machine reports an accepted apply
// verdict. Some callers additionally verify a stable post-state when their
// effect cannot be removed by a later valid command.
bycorf::Task<std::string> ProposeCommand(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, MetaCommand command) {
  auto result =
      co_await coordinator->Propose(std::move(command), std::move(principal));
  if (!result.ok()) {
    if (result.status().code() == absl::StatusCode::kFailedPrecondition &&
        result.status().message().find("not leader") !=
            std::string_view::npos) {
      co_return "ERR not-leader";
    }
    if (result.status().code() == absl::StatusCode::kResourceExhausted) {
      co_return "ERR resource-exhausted";
    }
    if (result.status().code() == absl::StatusCode::kDeadlineExceeded) {
      co_return "ERR timeout";
    }
    if (result.status().code() == absl::StatusCode::kCancelled) {
      co_return "ERR cancelled";
    }
    co_return "ERR propose-failed";
  }
  if (result->verdict_ != MetaAuditVerdict::kAccepted) {
    co_return "ERR rejected";
  }
  co_return "OK " + std::to_string(result->log_index_);
}

bycorf::Task<std::string> HandleSubmitOp(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const MetaOperationId& id,
    const std::string& kind, const std::string& payload,
    const MetaReplicationHistoryId& replication_history_id) {
  if (kind == kMetaMembershipOperationKind ||
      kind == kMetaClusterCreateOperationKind ||
      kind == kMetaClusterCreateV1GroupOperationKind ||
      kind == kFailoverOperationKind) {
    co_return "ERR workflow-owned";
  }
  SubmitOperation command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = id;
  command.kind_ = kind;
  command.intent_ = payload;
  command.intent_hash_ = MetaSha256(payload);
  command.replication_history_id_ = replication_history_id;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  // OK never reports an apply-level rejection: verify the committed effect.
  // An idempotent duplicate submit (same id, same intent) verifies
  // identically; a payload-reuse rejection leaves a mismatched intent hash.
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value() || record->intent_hash_ != command.intent_hash_) {
    co_return "ERR rejected";
  }
  co_return reply;
}

bycorf::Task<std::string> HandleCompleteOp(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const MetaOperationId& id,
    const std::string& result) {
  const auto lifecycle = state_machine->ClusterLifecycle();
  if (lifecycle.state_ != MetaClusterLifecycle::kUninitialized &&
      lifecycle.root_operation_id_ == id) {
    co_return "ERR workflow-owned";
  }
  // The CAS token comes from the local committed state: on the leader that
  // accepted the submit, the record is visible at its post-submit revision.
  // A freshly elected leader may legitimately lag behind the submit's OK —
  // "ERR not-found" is the uncertain-outcome signal, never a
  // false success.
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value()) {
    co_return "ERR not-found";
  }
  // Releasing this reservation while Raft still owns an accepted invite or
  // leave would allow a second workflow to overtake its uncertain outcome.
  if (record->kind_ == kMetaMembershipOperationKind ||
      record->kind_ == kMetaClusterCreateOperationKind ||
      record->kind_ == kMetaClusterCreateV1GroupOperationKind ||
      record->kind_ == kFailoverOperationKind)
    co_return "ERR workflow-owned";
  if (IsTerminal(record->lifecycle_)) {
    co_return "ERR terminal";
  }
  CompleteOperation command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = id;
  command.expected_revision_ = record->revision_;
  command.result_ = result;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  const std::optional<MetaOperationRecord> after =
      state_machine->FindOperation(id);
  if (!after.has_value() ||
      after->lifecycle_ != MetaOperationLifecycle::kCompleted ||
      after->terminal_result_ != result) {
    co_return "ERR rejected";
  }
  co_return reply;
}

bycorf::Task<std::string> HandleAbortOp(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const MetaOperationId& id,
    const std::string& reason) {
  const auto lifecycle = state_machine->ClusterLifecycle();
  if (lifecycle.state_ != MetaClusterLifecycle::kUninitialized &&
      lifecycle.root_operation_id_ == id) {
    co_return "ERR workflow-owned";
  }
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value()) {
    co_return "ERR not-found";
  }
  if (record->kind_ == kMetaMembershipOperationKind ||
      record->kind_ == kMetaClusterCreateOperationKind ||
      record->kind_ == kMetaClusterCreateV1GroupOperationKind ||
      record->kind_ == kFailoverOperationKind)
    co_return "ERR workflow-owned";
  if (IsTerminal(record->lifecycle_)) {
    co_return "ERR terminal";
  }
  AbortOperation command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = id;
  command.expected_revision_ = record->revision_;
  command.reason_ = reason;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  const std::optional<MetaOperationRecord> after =
      state_machine->FindOperation(id);
  if (!after.has_value() ||
      after->lifecycle_ != MetaOperationLifecycle::kAborted ||
      after->terminal_result_ != reason) {
    co_return "ERR rejected";
  }
  co_return reply;
}

// Non-linearizable read of committed operator state (see the header).
std::string HandleGetOp(std::shared_ptr<MetaStateMachine> state_machine,
                        const MetaOperationId& id) {
  std::optional<MetaOperationRecord> record = state_machine->FindOperation(id);
  if (!record.has_value()) {
    return "ERR not-found";
  }
  if (IsTerminal(record->lifecycle_)) {
    return std::string("OK ") + LifecycleName(record->lifecycle_) + " " +
           record->terminal_result_;
  }
  if (record->kind_ == kFailoverOperationKind) {
    // The displayed Running state is derived rather than persisted. Re-read
    // the operation and transition from one aggregate cut so Admin cannot
    // combine a pre-Cutover operation with a post-Cutover topology (or the
    // reverse) across two individually valid reads.
    const MetaStores stores = state_machine->StoresSnapshot();
    record = stores.operation_.FindOperation(id);
    if (!record.has_value()) return "ERR not-found";
    if (IsTerminal(record->lifecycle_)) {
      return std::string("OK ") + LifecycleName(record->lifecycle_) + " " +
             record->terminal_result_;
    }
    const bool running = std::ranges::any_of(
        stores.topology_.Groups(), [&](const MetaTopologyGroupView& group) {
          return group.failover_transition_.has_value() &&
                 group.failover_transition_->mode_ ==
                     MetaFailoverMode::kControlled &&
                 group.failover_transition_->controlled_.has_value() &&
                 group.failover_transition_->controlled_->operation_id_ == id;
        });
    return running ? "OK running" : "OK submitted";
  }
  if (record->kind_ == kMetaClusterCreateOperationKind ||
      record->kind_ == kMetaClusterCreateV1GroupOperationKind ||
      record->kind_ == kMetaMembershipOperationKind) {
    return absl::StrCat("OK ", LifecycleName(record->lifecycle_), " phase=",
                        record->kind_phase_blob_.empty()
                            ? "submitted"
                            : record->kind_phase_blob_);
  }
  return std::string("OK ") + LifecycleName(record->lifecycle_);
}

std::string FailoverError(std::string_view stage, std::string_view code) {
  return absl::StrCat("ERR failover 1 ", stage, " ", code);
}

bycorf::Task<std::string> HandleFailover(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const FailoverAdminRequestV1& request) {
  const MetaStores before = state_machine->StoresSnapshot();
  if (before.topology_.ClusterLifecycle().state_ !=
      MetaClusterLifecycle::kCreated) {
    co_return FailoverError("preflight", "cluster-not-created");
  }
  if (!before.topology_.FindGroup(request.group_id_).has_value()) {
    co_return FailoverError("preflight", "group-not-found");
  }

  auto intent = EncodeFailoverOperationIntent(
      {.group_id_ = request.group_id_,
       .absolute_deadline_unix_ms_ = request.absolute_deadline_unix_ms_});
  if (!intent.ok()) co_return FailoverError("decode", "bad-request");

  SubmitOperation command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = request.operation_id_;
  command.kind_ = std::string(kFailoverOperationKind);
  command.intent_ = *intent;
  command.intent_hash_ = MetaSha256(command.intent_);
  const std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (!reply.starts_with("OK ")) {
    const std::string_view code = reply.starts_with("ERR ")
                                      ? std::string_view(reply).substr(4)
                                      : std::string_view("malformed-reply");
    co_return FailoverError("proposal", code);
  }

  const auto committed = state_machine->FindOperation(request.operation_id_);
  if (!committed.has_value() || committed->kind_ != kFailoverOperationKind ||
      committed->intent_ != command.intent_ ||
      committed->intent_hash_ != command.intent_hash_) {
    co_return FailoverError("proposal", "uncertain-outcome");
  }
  co_return absl::StrCat(
      "OK failover 1 ", std::string_view(reply).substr(3), " ",
      HexEncode(std::string_view(
          reinterpret_cast<const char*>(request.operation_id_.data()),
          request.operation_id_.size())));
}

// This command selects one current boot. It never invents a source cursor;
// the ordinary committed transition reconciler authorizes, prepares and cuts
// over with loss=unknown. Proposal validation repeats volatile checks at
// append.
bycorf::Task<std::string> HandlePromote(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    const std::shared_ptr<MetaObservationStore>& observations,
    AuthenticatedPrincipal principal, const std::string& group_id,
    const std::string& node_id) {
  const MetaStores before = state_machine->StoresSnapshot();
  if (before.topology_.ClusterLifecycle().state_ !=
      MetaClusterLifecycle::kCreated) {
    co_return "ERR promote cluster-not-created";
  }
  const auto group = before.topology_.FindGroup(group_id);
  if (!group.has_value()) co_return "ERR promote group-not-found";
  const MetaStoresFacts facts(before);
  const auto now = NowUnixMs();
  if (!observations->LiveCandidateProgressFor(group_id, facts, now).empty()) {
    co_return "ERR promote eligible-candidate-exists";
  }
  const auto populations =
      observations->LiveCandidateProgressFor(group_id, facts, now, true);
  const auto population =
      std::ranges::find_if(populations, [&](const auto& item) {
        return item.node_id_ == node_id && item.operator_recovery_;
      });
  if (population == populations.end())
    co_return "ERR promote no-readable-recovered-population";
  MetaFailoverCandidateAction action{
      .action_id_ = MakeRequiredId("operator recovery action"),
      .candidate_ = {.node_id_ = node_id,
                     .assignment_id_ = population->assignment_id_,
                     .boot_id_ = population->boot_incarnation_},
      .operator_recovery_ = true,
  };
  MetaCommand command;
  if (group->failover_transition_.has_value()) {
    const auto& transition = *group->failover_transition_;
    if (transition.mode_ != MetaFailoverMode::kUncontrolled ||
        transition.candidate_action_.has_value()) {
      co_return "ERR promote transition-already-has-action";
    }
    command = SetUncontrolledCandidate{
        .request_id_ = MakeRequestId(),
        .group_id_ = group_id,
        .expected_transition_ = {transition.transition_id_,
                                 transition.revision_},
        .candidate_action_ = action};
  } else {
    const auto owner =
        std::ranges::find_if(group->members_, [&](const auto& member) {
          return member.node_id_ == group->record_.owner_;
        });
    if (owner == group->members_.end() ||
        group->record_.group_term_ ==
            std::numeric_limits<std::uint64_t>::max()) {
      co_return "ERR promote invalid-group-authority";
    }
    command = BeginUncontrolledFailover{
        .request_id_ = MakeRequestId(),
        .group_id_ = group_id,
        .transition_id_ = MakeRequiredId("operator recovery transition"),
        .target_term_ = group->record_.group_term_ + 1,
        .candidate_action_ = action,
        .expected_owner_node_id_ = owner->node_id_,
        .expected_owner_assignment_id_ = owner->assignment_id_,
        .expected_membership_revision_ = group->revision_,
        .expected_group_term_ = group->record_.group_term_,
        .expected_population_manifest_revision_ =
            group->record_.population_manifest_revision_,
        .expected_population_manifest_digest_ =
            group->record_.population_manifest_digest_,
        .expected_partition_replication_epoch_ =
            group->record_.partition_replication_epoch_,
    };
  }
  const MetaCommittedView view(before, state_machine->last_commit_index());
  const absl::Status valid =
      ValidateFailoverProposal(command, view, *observations, now);
  if (!valid.ok()) co_return absl::StrCat("ERR promote ", valid.message());
  const std::string reply = co_await ProposeCommand(
      coordinator, std::move(principal), std::move(command));
  if (!reply.starts_with("OK ")) co_return reply;
  co_return absl::StrCat(
      reply, " promote loss=unknown action=",
      HexEncode(std::string_view(
          reinterpret_cast<const char*>(action.action_id_.data()),
          action.action_id_.size())));
}

bycorf::Task<std::string> HandleRegisterNode(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal authenticated, const std::string& node_id,
    const std::string& principal, MetaNodeRole role,
    std::vector<std::string> endpoints) {
  RegisterNode command;
  command.request_id_ = MakeRequestId();
  command.node_id_ = node_id;
  command.principal_ = principal;
  command.endpoints_ = std::move(endpoints);
  command.role_ = role;
  const std::vector<std::string> expected_endpoints = command.endpoints_;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(authenticated), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  // Same effect-verification as submitop: an idempotent re-register with
  // identical content verifies; a principal conflict does not.
  const std::optional<MetaNodeRecord> record = state_machine->FindNode(node_id);
  if (!record.has_value() || record->principal_ != principal ||
      record->endpoints_ != expected_endpoints || record->role_ != role ||
      record->retired_) {
    co_return "ERR rejected";
  }
  co_return reply;
}

std::string HandleGetNode(std::shared_ptr<MetaStateMachine> state_machine,
                          const std::string& node_id) {
  const std::optional<MetaNodeRecord> record = state_machine->FindNode(node_id);
  if (!record.has_value()) {
    return "ERR not-found";
  }
  return "OK principal=" + record->principal_ +
         " role=" + RoleName(record->role_) +
         " revision=" + std::to_string(record->revision_) +
         (record->retired_ ? " retired=1" : " retired=0");
}

// creategroup <group_id>: the topology epoch is absolute (current + 1), read
// from a committed snapshot. Effect-verified like submitop.
bycorf::Task<std::string> HandleCreateGroup(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& group_id) {
  CreateGroup command;
  command.request_id_ = MakeRequestId();
  command.group_id_ = group_id;
  command.new_topology_epoch_ =
      state_machine->StoresSnapshot().topology_.TopologyEpoch() + 1;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  if (!state_machine->StoresSnapshot().topology_.GroupExists(group_id)) {
    co_return "ERR rejected";
  }
  co_return reply;
}

// assignnode <group_id> <node_id> <primary|replica>. The operator names the
// desired membership, but never its incarnation: the trusted proposer creates
// a fresh nonzero 128-bit CSPRNG identity immediately before submission.
bycorf::Task<std::string> HandleAssignNode(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& group_id,
    const std::string& node_id, MetaNodeRole role) {
  const MetaStores before = state_machine->StoresSnapshot();
  const auto group = before.topology_.FindGroup(group_id);
  if (!group.has_value()) co_return "ERR not-found";
  const auto existing =
      std::find_if(group->members_.begin(), group->members_.end(),
                   [&](const MetaGroupMember& member) {
                     return member.node_id_ == node_id;
                   });
  if (existing != group->members_.end()) {
    co_return existing->role_ == role ? "OK already-assigned" : "ERR rejected";
  }

  AssignNodeToGroup command;
  command.request_id_ = MakeRequestId();
  command.group_id_ = group_id;
  command.node_id_ = node_id;
  command.assignment_id_ = MakeAssignmentId();
  command.role_ = role;
  command.expected_revision_ = group->revision_;
  command.new_topology_epoch_ = before.topology_.TopologyEpoch() + 1;
  const MetaAssignmentId expected_assignment = command.assignment_id_;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) co_return reply;

  const auto after =
      state_machine->StoresSnapshot().topology_.FindGroup(group_id);
  if (!after.has_value()) co_return "ERR rejected";
  const auto installed =
      std::find_if(after->members_.begin(), after->members_.end(),
                   [&](const MetaGroupMember& member) {
                     return member.node_id_ == node_id &&
                            member.assignment_id_ == expected_assignment &&
                            member.role_ == role;
                   });
  co_return installed == after->members_.end() ? "ERR rejected" : reply;
}

// begingroupterm <group_id> <expected> <new>: promotes the committed
// group_term (and fences the group), which is what term-bound observations
// anchor to.
bycorf::Task<std::string> HandleBeginGroupTerm(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& group_id,
    std::uint64_t expected, std::uint64_t next) {
  BeginGroupTerm command;
  command.request_id_ = MakeRequestId();
  command.group_id_ = group_id;
  command.expected_term_ = expected;
  command.new_term_ = next;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  const std::optional<std::uint64_t> term =
      state_machine->StoresSnapshot().topology_.CurrentGroupTerm(group_id);
  if (!term.has_value() || *term != next) {
    co_return "ERR rejected";
  }
  co_return reply;
}

// These topology/authority verbs intentionally expose the typed domain
// operations instead of a generic command-encoding escape hatch. Absolute
// CAS values remain operator input; only the cluster-wide topology epoch is
// derived from one committed snapshot because no external caller can safely
// guess commits in unrelated groups.
bycorf::Task<std::string> HandlePutPolicy(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, const std::string& policy_id,
    std::uint64_t version, const std::string& content) {
  PutPolicy command;
  command.request_id_ = MakeRequestId();
  command.policy_id_ = policy_id;
  command.version_ = version;
  command.content_ = content;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  // The committed apply verdict is authoritative. A later burst may install
  // enough consecutive versions to evict this immutable version before this
  // coroutine resumes; absence from the retained newest-32 window cannot turn
  // an accepted write into a rejection.
  co_return reply;
}

std::string HandleGetPolicy(std::shared_ptr<MetaStateMachine> state_machine,
                            const std::string& policy_id) {
  const MetaPolicyStore& policy = state_machine->StoresSnapshot().policy_;
  const auto version = policy.LatestVersion(policy_id);
  if (!version.has_value()) return "ERR not-found";
  const auto current = policy.FindVersion(policy_id, *version);
  if (!current.has_value()) return "ERR state_corrupt";
  return absl::StrCat("OK version=", current->version_,
                      " content=", current->content_);
}

bycorf::Task<std::string> HandleSetSlotMap(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, std::uint16_t first_slot,
    std::uint16_t last_slot, const std::string& group_id) {
  const MetaStores before = state_machine->StoresSnapshot();
  SetSlotMap command;
  command.request_id_ = MakeRequestId();
  command.ranges_.push_back({first_slot, last_slot, group_id});
  command.new_topology_epoch_ = before.topology_.TopologyEpoch() + 1;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) co_return reply;

  const MetaStores after = state_machine->StoresSnapshot();
  const auto group = after.topology_.FindGroup(group_id);
  if (!group.has_value() ||
      after.topology_.TopologyEpoch() != command.new_topology_epoch_) {
    co_return "ERR rejected";
  }
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    const std::optional<std::string> owner = after.topology_.SlotOwner(slot);
    const bool assigned = slot >= first_slot && slot <= last_slot;
    if ((assigned && owner != std::optional<std::string>(group_id)) ||
        (!assigned && owner.has_value())) {
      co_return "ERR rejected";
    }
  }
  co_return reply;
}

bycorf::Task<std::string> HandleActivateAuthority(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& group_id,
    std::uint64_t expected_term, const std::string& owner_node_id) {
  const MetaStores before = state_machine->StoresSnapshot();
  ActivateAuthority command;
  command.request_id_ = MakeRequestId();
  command.group_id_ = group_id;
  command.expected_term_ = expected_term;
  command.new_owner_ = owner_node_id;
  command.new_topology_epoch_ = before.topology_.TopologyEpoch() + 1;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) co_return reply;

  const MetaStores after = state_machine->StoresSnapshot();
  const auto topology = after.topology_.FindGroup(group_id);
  const auto grant = after.topology_.AuthorityFor(group_id);
  if (!topology.has_value() || !grant.has_value() ||
      !grant->grant_.has_value() || grant->grant_->owner_ != owner_node_id ||
      grant->group_term_ != expected_term ||
      topology->record_.owner_ != owner_node_id ||
      topology->record_.group_term_ != expected_term ||
      after.topology_.TopologyEpoch() != command.new_topology_epoch_) {
    co_return "ERR rejected";
  }
  co_return reply;
}

bycorf::Task<std::string> HandleFenceGroup(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& group_id,
    std::uint64_t expected_term) {
  FenceGroup command;
  command.request_id_ = MakeRequestId();
  command.group_id_ = group_id;
  command.expected_term_ = expected_term;
  command.new_term_ = expected_term + 1;
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) co_return reply;

  const MetaStores after = state_machine->StoresSnapshot();
  const auto state = after.topology_.AuthorityFor(group_id);
  const auto topology = after.topology_.FindGroup(group_id);
  if (!state.has_value() || !topology.has_value() ||
      state->group_term_ != command.new_term_ || state->grant_.has_value() ||
      topology->record_.group_term_ != command.new_term_) {
    co_return "ERR rejected";
  }
  co_return reply;
}

// transitionop <id32hex> <phase> <history>: moves the operation to Running.
// The history argument must match the anchor committed by submitop; it is a
// ctl-side consistency check and is not fabricated into evidence.
bycorf::Task<std::string> HandleTransitionOp(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const MetaOperationId& id,
    const std::string& phase, const MetaReplicationHistoryId& history) {
  const std::optional<MetaOperationRecord> record =
      state_machine->FindOperation(id);
  if (!record.has_value()) {
    co_return "ERR not-found";
  }
  if (IsTerminal(record->lifecycle_)) {
    co_return "ERR terminal";
  }
  TransitionOperationPhase command;
  command.request_id_ = MakeRequestId();
  command.operation_id_ = id;
  command.expected_revision_ = record->revision_;
  command.kind_phase_blob_ = phase;
  if (record->replication_history_id_ != history) {
    co_return "ERR history-mismatch";
  }
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) {
    co_return reply;
  }
  const std::optional<MetaOperationRecord> after =
      state_machine->FindOperation(id);
  if (!after.has_value() ||
      after->lifecycle_ != MetaOperationLifecycle::kRunning ||
      after->kind_phase_blob_ != phase) {
    co_return "ERR rejected";
  }
  co_return reply;
}

std::string ClusterCreateError(std::string_view stage, std::string_view code,
                               std::string detail) {
  std::replace(detail.begin(), detail.end(), '\n', ' ');
  std::replace(detail.begin(), detail.end(), '\r', ' ');
  return absl::StrCat("ERR clustercreate 1 ", stage, " ", code, " ", detail);
}

std::string ClusterAlreadyCreatedError(
    std::string_view stage, const MetaClusterLifecycleState& lifecycle) {
  return ClusterCreateError(
      stage, "already-created",
      absl::StrCat(
          "Meta already owns a Data cluster; operation=",
          HexEncode(std::string_view(reinterpret_cast<const char*>(
                                         lifecycle.root_operation_id_.data()),
                                     lifecycle.root_operation_id_.size())),
          " genesis=", lifecycle.genesis_commit_index_));
}

// Admission persists the whole plan BEFORE topology mutation. The leader
// reconciler, not this connection or its timeout, owns all subsequent work.
bycorf::Task<std::string> HandleClusterCreate(
    const std::shared_ptr<MetaRaft>& server,
    const std::shared_ptr<MetaStateMachine>& state_machine,
    const std::shared_ptr<MetaCoordinator>& coordinator,
    const std::shared_ptr<MetaMembershipGate>& membership_gate,
    AuthenticatedPrincipal principal, const ClusterCreateManifestV1& manifest,
    const MetaOperationId& root_operation_id, const bool* shutdown) {
  const std::string id = HexEncode(
      std::string_view(reinterpret_cast<const char*>(root_operation_id.data()),
                       root_operation_id.size()));
  if (*shutdown)
    co_return ClusterCreateError("preflight", "pre-commit-failed",
                                 "Meta is shutting down");
  const auto observed = state_machine->StoresSnapshot();
  if (observed.topology_.ClusterLifecycle().state_ !=
      MetaClusterLifecycle::kUninitialized) {
    co_return ClusterAlreadyCreatedError("preflight",
                                         observed.topology_.ClusterLifecycle());
  }
  auto create_lease = membership_gate->TryAcquire();
  if (create_lease == nullptr) {
    co_return ClusterCreateError(
        "preflight", "pre-commit-failed",
        "another cluster creation or Meta membership change is in progress");
  }
  if (!server->is_leader() || !server->is_leader_alive() ||
      !server->is_leader_sm_fully_caught_up()) {
    co_return ClusterCreateError("preflight", "pre-commit-failed",
                                 "responder is not an eligible leader");
  }
  const auto before = state_machine->StoresSnapshot();
  const auto& lifecycle = before.topology_.ClusterLifecycle();
  if (lifecycle.state_ != MetaClusterLifecycle::kUninitialized) {
    co_return ClusterAlreadyCreatedError("preflight", lifecycle);
  }
  if (before.operation_.HasActiveKind(kMetaMembershipOperationKind)) {
    co_return ClusterCreateError("preflight", "pre-commit-failed",
                                 "a Meta membership workflow is active");
  }
  MetaClusterCreateRaftView raft_view;
  raft_view.local_server_id_ = server->get_id();
  auto config = CaptureMembershipConfig(server->get_config());
  if (!config.ok()) {
    co_return ClusterCreateError("preflight", "pre-commit-failed",
                                 std::string(config.status().message()));
  }
  raft_view.members_ = std::move(*config);
  const MetaCommittedView committed(before, state_machine->last_commit_index());
  if (auto meta =
          detail::ValidateClusterCreateMetaSet(committed, manifest, raft_view);
      !meta.ok()) {
    co_return ClusterCreateError("preflight", "bad-request",
                                 std::string(meta.message()));
  }
  if (HasDataClusterArtifacts(before)) {
    co_return ClusterCreateError(
        "preflight", "non-pristine",
        "Uninitialized Meta contains Data-cluster artifacts");
  }
  auto intent = EncodeClusterCreateRequest(manifest, root_operation_id);
  if (!intent.ok())
    co_return ClusterCreateError("preflight", "bad-request",
                                 std::string(intent.status().message()));
  SubmitOperation submit;
  submit.request_id_ = MakeRequestId();
  submit.operation_id_ = root_operation_id;
  submit.kind_ = kMetaClusterCreateOperationKind;
  submit.intent_ = *intent;
  submit.intent_hash_ = MetaSha256(submit.intent_);
  auto applied =
      co_await coordinator->Propose(MetaCommand{submit}, std::move(principal));
  if (!applied.ok()) {
    const bool uncertain =
        applied.status().code() == absl::StatusCode::kDeadlineExceeded ||
        applied.status().code() == absl::StatusCode::kCancelled ||
        applied.status().code() == absl::StatusCode::kInternal;
    co_return ClusterCreateError(
        "proposal", uncertain ? "uncertain-outcome" : "pre-commit-failed",
        absl::StrCat(applied.status().message(), "; operation=", id));
  }
  if (applied->verdict_ != MetaAuditVerdict::kAccepted) {
    const auto after = state_machine->StoresSnapshot();
    if (after.topology_.ClusterLifecycle().state_ !=
        MetaClusterLifecycle::kUninitialized) {
      co_return ClusterAlreadyCreatedError("proposal",
                                           after.topology_.ClusterLifecycle());
    }
    if (HasDataClusterArtifacts(after)) {
      co_return ClusterCreateError(
          "proposal", "non-pristine",
          "Uninitialized Meta acquired Data-cluster artifacts before commit");
    }
    co_return ClusterCreateError("proposal", "pre-commit-failed",
                                 applied->detail_);
  }
  const auto committed_stores = state_machine->StoresSnapshot();
  const auto& accepted = committed_stores.topology_.ClusterLifecycle();
  // The background reconciler can terminalize a very small workflow before
  // this read. Any non-Uninitialized state with the exact Genesis identity
  // proves the atomic admission commit; readiness and terminal outcome are
  // reported separately through cluster-status.
  if (accepted.state_ == MetaClusterLifecycle::kUninitialized ||
      accepted.root_operation_id_ != root_operation_id ||
      accepted.genesis_commit_index_ != applied->log_index_) {
    co_return ClusterCreateError(
        "proposal", "uncertain-outcome",
        absl::StrCat("committed aggregate could not be verified; operation=",
                     id));
  }
  create_lease.reset();
  co_return absl::StrCat("OK clustercreate 1 ", applied->log_index_, " ", id);
}

bycorf::Task<std::string> HandlePruneAudit(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, std::uint64_t through) {
  PruneAudit command;
  command.request_id_ = MakeRequestId();
  command.through_log_index_ = through;
  co_return co_await ProposeCommand(coordinator, std::move(principal), command);
}

bycorf::Task<std::string> HandleSetAuditPolicy(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, MetaAuditPolicy policy,
    const std::string& attestation) {
  SetAuditPolicy command;
  command.request_id_ = MakeRequestId();
  command.policy_ = policy;
  command.attestation_ = attestation;
  co_return co_await ProposeCommand(coordinator, std::move(principal), command);
}

bycorf::Task<std::string> HandlePruneOperationArchive(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal, std::vector<std::uint64_t> seqs) {
  PruneOperationArchive command;
  command.request_id_ = MakeRequestId();
  command.operation_seqs_ = std::move(seqs);
  co_return co_await ProposeCommand(coordinator, std::move(principal), command);
}

bycorf::Task<std::string> HandleArchiveOperations(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, std::vector<std::uint64_t> seqs) {
  ArchiveOperations command;
  command.request_id_ = MakeRequestId();
  command.operation_seqs_ = std::move(seqs);
  std::string reply =
      co_await ProposeCommand(coordinator, std::move(principal), command);
  if (reply.rfind("OK ", 0) != 0) co_return reply;

  const MetaStores after = state_machine->StoresSnapshot();
  for (const std::uint64_t seq : command.operation_seqs_) {
    if (!after.operation_.FindArchivedBySeq(seq).has_value()) {
      co_return "ERR rejected";
    }
  }
  co_return reply;
}

// adoptsession injects a session identity supplied by the authorized transport
// adapter. No facts are needed here: adopting a session for an unregistered
// node is harmless because Ingest re-checks registration on every observation.
std::string HandleAdoptSession(
    const std::shared_ptr<MetaObservationStore>& obs_store,
    const MetaObservationIdentity& identity) {
  const absl::Status status = obs_store->AdoptSession(identity, NowUnixMs());
  if (!status.ok()) {
    return "ERR " + std::string(status.message());
  }
  return "OK";
}

// One obs ingest: sweep expired entries first (ctl-frequency TTL hygiene; the
// coordinator also sweeps on its own tick), then admit against a single
// committed snapshot.
std::string HandleObsIngest(
    const std::shared_ptr<MetaObservationStore>& obs_store,
    std::shared_ptr<MetaStateMachine> state_machine,
    MetaObservation observation) {
  const std::int64_t now = NowUnixMs();
  obs_store->SweepExpired(now);
  MetaStores stores = state_machine->StoresSnapshot();
  const auto bind_reporter_assignment = [&](std::string_view group_id,
                                            std::string* node_id,
                                            MetaAssignmentId* assignment_id) {
    *node_id = observation.identity_.node_id_;
    const auto group = stores.topology_.FindGroup(std::string(group_id));
    if (!group.has_value()) return;
    const auto member = std::find_if(
        group->members_.begin(), group->members_.end(),
        [&](const MetaGroupMember& candidate_member) {
          return candidate_member.node_id_ == observation.identity_.node_id_;
        });
    if (member != group->members_.end()) {
      *assignment_id = member->assignment_id_;
    }
  };
  if (auto* candidate =
          std::get_if<MetaCandidateProgressObs>(&observation.payload_)) {
    // The ctl surface stands in for an authenticated Data session in the
    // observation integration gate. Derive identity fields that production
    // receives from the session and heartbeat instead of asking an operator
    // to discover the CSPRNG-generated assignment id.
    bind_reporter_assignment(candidate->group_id_, &candidate->node_id_,
                             &candidate->assignment_id_);
    candidate->boot_incarnation_ = observation.identity_.boot_incarnation_;
  }

  const MetaStoresFacts facts(stores);
  const absl::Status status =
      obs_store->Ingest(std::move(observation), facts, now);
  if (!status.ok()) {
    return "ERR " + std::string(status.message());
  }
  return "OK";
}

std::string HandleObservations(
    const std::shared_ptr<MetaObservationStore>& obs_store,
    std::shared_ptr<MetaStateMachine> state_machine,
    const std::optional<std::string>& group_id) {
  obs_store->SweepExpired(NowUnixMs());
  if (!group_id.has_value()) {
    return "OK total=" + std::to_string(obs_store->size());
  }
  const MetaStores stores = state_machine->StoresSnapshot();
  const MetaStoresFacts facts(stores);
  const std::vector<MetaCandidateProgressObs> candidates =
      obs_store->CandidateProgressFor(*group_id, facts);
  std::string reply = "OK candidates=" + std::to_string(candidates.size());
  // Reporter history identifies its own session, not the population's source.
  // A Ready candidate can retain an older source domain during reparent.
  for (const MetaCandidateProgressObs& candidate : candidates) {
    reply +=
        " node=" + candidate.node_id_ +
        ",assignment=" + AssignmentIdText(candidate.assignment_id_) +
        ",term=" + std::to_string(candidate.group_term_) +
        ",manifest=" + std::to_string(candidate.population_manifest_revision_) +
        ",partition_epoch=" +
        std::to_string(candidate.partition_replication_epoch_) + ",history=" +
        ReplicationHistoryIdText(candidate.replication_history_id_) +
        ",storage_ready=" + (candidate.storage_ready_ ? "true" : "false") +
        ",population_ready=" +
        (candidate.population_ready_ ? "true" : "false") +
        ",source_term=" + std::to_string(candidate.source_group_term_) +
        ",source_node=" + candidate.source_node_id_ + ",source_history=" +
        ReplicationHistoryIdText(candidate.source_replication_history_id_);
  }
  return reply;
}

std::string HandleObsAudit(
    const std::shared_ptr<MetaObservationStore>& obs_store) {
  const std::vector<MetaObsAuditEvent> events = obs_store->AuditRing();
  std::string reply = "OK events=" + std::to_string(events.size());
  for (const MetaObsAuditEvent& event : events) {
    // Details are whitespace-free single tokens by construction
    // (observation_store.cpp), so the line protocol can dump them raw.
    reply += std::string(" kind=") + ObsAuditKindName(event.kind_) +
             ",node=" + event.node_id_ + ",detail=" + event.detail_ +
             ",ts=" + std::to_string(event.unix_ms_);
  }
  return reply;
}

bycorf::Task<std::string> HandleConfigChange(
    std::shared_ptr<MetaRaft> server,
    std::shared_ptr<MetaStateMachine> state_machine,
    const std::shared_ptr<MetaCoordinator>& coordinator,
    AuthenticatedPrincipal principal,
    const std::shared_ptr<MetaMembershipGate>& membership_gate, bool add,
    int server_id, std::string endpoint, std::string data_control_endpoint,
    std::string ctl_endpoint, const std::string& member_principal,
    const bool* shutdown) {
  if (*shutdown) co_return "ERR shutting-down";
  if (!server->is_leader() || !server->is_leader_alive() ||
      !server->is_leader_sm_fully_caught_up())
    co_return "ERR not-leader";
  if (add) {
    // Persist the same canonical addresses as the identity store. Otherwise
    // an equivalent IPv6/port spelling would appear to diverge after bind,
    // or a retry could fail to recognize its original operation.
    auto raft = lavik::ParseNumericEndpoint(endpoint);
    auto data = lavik::ParseNumericEndpoint(data_control_endpoint);
    auto ctl = lavik::ParseNumericEndpoint(ctl_endpoint);
    if (!raft) co_return "ERR bad-request";
    if (!data || !ctl) co_return "ERR rejected";
    endpoint = lavik::FormatNumericEndpoint(*raft);
    data_control_endpoint = lavik::FormatNumericEndpoint(*data);
    ctl_endpoint = lavik::FormatNumericEndpoint(*ctl);
  }
  const auto before = state_machine->StoresSnapshot();
  if (before.topology_.ClusterLifecycle().state_ ==
      MetaClusterLifecycle::kCreating)
    co_return "ERR config-changing";
  std::optional<MetaOperationId> operation_id;
  // Retrying an identical in-flight request attaches to its original task.
  // Different requests must not overtake an uncertain membership outcome.
  for (const auto& op : before.operation_.LiveOperations()) {
    if (op.kind_ != kMetaMembershipOperationKind || IsTerminal(op.lifecycle_))
      continue;
    auto intent = DecodeMembershipIntent(op.intent_);
    if (!intent.ok() || intent->add_ != add ||
        intent->target_.id_ != static_cast<std::uint32_t>(server_id) ||
        (add &&
         (intent->target_.endpoint_ != endpoint ||
          intent->target_.principal_ != member_principal ||
          intent->target_.data_control_endpoint_ != data_control_endpoint ||
          intent->target_.ctl_endpoint_ != ctl_endpoint ||
          intent->binding_.data_control_endpoint_ != data_control_endpoint ||
          intent->binding_.ctl_endpoint_ != std::optional(ctl_endpoint))))
      co_return "ERR config-changing";
    operation_id = op.operation_id_;
    break;
  }
  if (!operation_id) {
    auto lease = membership_gate->TryAcquire();
    if (!lease) co_return "ERR config-changing";
    auto config = CaptureMembershipConfig(server->get_config());
    if (!config.ok()) co_return "ERR bad-member-identity";
    auto peer =
        std::find_if(config->begin(), config->end(), [&](const auto& p) {
          return p.id_ == static_cast<std::uint32_t>(server_id);
        });
    if (add && peer != config->end()) co_return "ERR already-exists";
    if (!add && server_id == server->get_id())
      co_return "ERR cannot-remove-leader";
    if (!add && peer == config->end()) {
      auto binding = before.identity_.FindMetaMember(server_id);
      co_return binding && binding->retired_ ? "OK" : "ERR not-found";
    }
    MetaMembershipIntent intent;
    intent.add_ = add;
    intent.before_ = *config;
    for (const auto& p : *config) {
      auto binding = before.identity_.FindMetaMember(p.id_);
      // The membership reconciler owns initial binding; never persist an
      // intent reconstructed from this process's guesses about endpoints.
      if (!binding || binding->retired_ || binding->principal_ != p.principal_)
        co_return "ERR config-changing";
      if (add && !binding->ctl_endpoint_) co_return "ERR missing-ctl-endpoint";
      intent.bindings_.push_back(*binding);
    }
    if (add) {
      if (!lavik::ParseNumericEndpoint(endpoint).has_value())
        co_return "ERR bad-request";
      intent.target_ = {
          .id_ = static_cast<std::uint32_t>(server_id),
          .endpoint_ = endpoint,
          .principal_ = member_principal,
          .data_control_endpoint_ = data_control_endpoint,
          .ctl_endpoint_ = ctl_endpoint,
      };
      intent.binding_ = {static_cast<std::uint32_t>(server_id),
                         member_principal, data_control_endpoint, ctl_endpoint,
                         false};
      BindMetaMember bind;
      bind.server_id_ = server_id;
      bind.principal_ = member_principal;
      bind.data_control_endpoint_ = data_control_endpoint;
      bind.ctl_endpoint_ = ctl_endpoint;
      auto identity = before.identity_;
      if (!identity.Apply(bind).ok()) co_return "ERR rejected";
    } else {
      intent.target_ = *peer;
      intent.binding_ = *before.identity_.FindMetaMember(server_id);
    }
    auto encoded = EncodeMembershipIntent(intent);
    if (!encoded.ok()) co_return "ERR rejected";
    SubmitOperation submit;
    submit.request_id_ = MakeRequestId();
    submit.operation_id_ = MakeOperationId();
    submit.kind_ = kMetaMembershipOperationKind;
    submit.intent_ = *encoded;
    submit.intent_hash_ = MetaSha256(*encoded);
    operation_id = submit.operation_id_;
    const auto reply =
        co_await ProposeCommand(coordinator, std::move(principal), submit);
    if (!reply.starts_with("OK ")) {
      if (reply == "ERR rejected") co_return reply;
      // A timed-out submission can still commit after this coroutine leaves.
      // Preserve its id even before it is visible in the local operation view.
      co_return absl::StrCat(
          "ERR uncertain-outcome operation=",
          HexEncode(std::string_view(
              reinterpret_cast<const char*>(operation_id->data()),
              operation_id->size())));
    }
    if (!state_machine->FindOperation(*operation_id)) co_return "ERR rejected";
    // The committed active-kind reservation bridges release to the reconciler.
  }
  const auto id = HexEncode(
      std::string_view(reinterpret_cast<const char*>(operation_id->data()),
                       operation_id->size()));
  // A bounded wait is not cancellation of the accepted durable workflow.
  // Both the id in timeout replies and an identical request can locate it.
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(
                            server->get_current_params().client_req_timeout_);
  while (!*shutdown && server->is_leader() &&
         std::chrono::steady_clock::now() < deadline) {
    auto op = state_machine->FindOperation(*operation_id);
    if (!op) co_return absl::StrCat("ERR uncertain-outcome operation=", id);
    if (op->lifecycle_ == MetaOperationLifecycle::kCompleted) co_return "OK";
    if (op->lifecycle_ == MetaOperationLifecycle::kAborted ||
        op->kind_phase_blob_.starts_with("recovery-required:"))
      co_return absl::StrCat("ERR recovery-required operation=", id);
    auto slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                           std::chrono::milliseconds(10));
    if (!slept.ok()) break;
  }
  co_return absl::StrCat("ERR uncertain-outcome operation=", id);
}

std::vector<std::string> SplitTokens(std::string_view line) {
  std::vector<std::string> tokens;
  std::size_t pos = 0;
  while (pos < line.size()) {
    while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) {
      ++pos;
    }
    const std::size_t begin = pos;
    while (pos < line.size() && line[pos] != ' ' && line[pos] != '\t') {
      ++pos;
    }
    if (begin < pos) {
      tokens.emplace_back(line.substr(begin, pos - begin));
    }
  }
  return tokens;
}

bool ParseServerId(const std::string& text, int& out) {
  try {
    std::size_t used = 0;
    const int value = std::stoi(text, &used);
    if (used != text.size() || value <= 0) {
      return false;
    }
    out = value;
    return true;
  } catch (...) {
    return false;
  }
}

// Committed-mutation verbs: everything that proposes onto the raft log.
// Observation revalidation is scheduled by the coordinator's commit callback
// on its dispatch thread, independently of this command's response.
bycorf::Task<std::string> DispatchMutationVerb(
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaStateMachine> state_machine,
    AuthenticatedPrincipal principal, const std::string& command,
    const std::vector<std::string>& tokens) {
  if (command == "submitop") {
    // submitop <id> <kind> <payload> [replication_history_id];
    if (tokens.size() != 4u && tokens.size() != 5u) {
      co_return "ERR bad-request";
    }
    MetaOperationId id{};
    if (!ParseOperationId(tokens[1], id)) {
      co_return "ERR bad-request";
    }
    MetaReplicationHistoryId history{};
    if (tokens.size() == 5u && !ParseReplicationHistoryId(tokens[4], history)) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleSubmitOp(coordinator, std::move(state_machine),
                                      std::move(principal), id, tokens[2],
                                      tokens[3], history);
  }
  if (command == "completeop" || command == "abortop") {
    // The final token is optional so a durability-gated operator can express
    // zero-growth terminalization before archive/prune recovery.
    if (tokens.size() != 2u && tokens.size() != 3u) {
      co_return "ERR bad-request";
    }
    MetaOperationId id{};
    if (!ParseOperationId(tokens[1], id)) {
      co_return "ERR bad-request";
    }
    const std::string payload = tokens.size() == 3u ? tokens[2] : "";
    if (command == "completeop") {
      co_return co_await HandleCompleteOp(coordinator, std::move(state_machine),
                                          std::move(principal), id, payload);
    }
    co_return co_await HandleAbortOp(coordinator, std::move(state_machine),
                                     std::move(principal), id, payload);
  }
  if (command == "registernode") {
    if (tokens.size() < 5 || tokens.size() > 4 + kMaxMetaEndpointsPerNode ||
        !IsNodeId(tokens[1])) {
      co_return "ERR bad-request";
    }
    MetaNodeRole role;
    if (tokens[3] == "primary") {
      role = MetaNodeRole::kPrimary;
    } else if (tokens[3] == "replica") {
      role = MetaNodeRole::kReplica;
    } else {
      co_return "ERR bad-request";
    }
    std::vector<std::string> endpoints(tokens.begin() + 4, tokens.end());
    co_return co_await HandleRegisterNode(
        coordinator, std::move(state_machine), std::move(principal), tokens[1],
        tokens[2], role, std::move(endpoints));
  }
  if (command == "creategroup") {
    if (tokens.size() != 2 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleCreateGroup(coordinator, std::move(state_machine),
                                         std::move(principal), tokens[1]);
  }
  if (command == "assignnode") {
    if (tokens.size() != 4 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes || !IsNodeId(tokens[2])) {
      co_return "ERR bad-request";
    }
    MetaNodeRole role;
    if (tokens[3] == "primary") {
      role = MetaNodeRole::kPrimary;
    } else if (tokens[3] == "replica") {
      role = MetaNodeRole::kReplica;
    } else {
      co_return "ERR bad-request";
    }
    co_return co_await HandleAssignNode(coordinator, std::move(state_machine),
                                        std::move(principal), tokens[1],
                                        tokens[2], role);
  }
  if (command == "begingroupterm") {
    if (tokens.size() != 4 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes) {
      co_return "ERR bad-request";
    }
    std::uint64_t expected = 0;
    std::uint64_t next = 0;
    if (!ParseU64(tokens[2], expected) || !ParseU64(tokens[3], next)) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleBeginGroupTerm(
        coordinator, std::move(state_machine), std::move(principal), tokens[1],
        expected, next);
  }
  if (command == "putpolicy") {
    std::uint64_t version = 0;
    if (tokens.size() != 4 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaPolicyIdBytes ||
        !detail::ParseAdminPolicyVersion(tokens[2], &version) ||
        tokens[3].empty() || tokens[3].size() > kMaxMetaPayloadBytes) {
      co_return "ERR bad-request";
    }
    co_return co_await HandlePutPolicy(coordinator, std::move(principal),
                                       tokens[1], version, tokens[3]);
  }
  if (command == "setslotmap") {
    std::uint64_t first = 0;
    std::uint64_t last = 0;
    if (tokens.size() != 4 || !ParseU64(tokens[1], first) ||
        !ParseU64(tokens[2], last) || first > last || last >= kMetaSlotCount ||
        tokens[3].empty() || tokens[3].size() > kMaxMetaGroupIdBytes) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleSetSlotMap(
        coordinator, std::move(state_machine), std::move(principal),
        static_cast<std::uint16_t>(first), static_cast<std::uint16_t>(last),
        tokens[3]);
  }
  if (command == "activateauthority") {
    std::uint64_t expected_term = 0;
    if (tokens.size() != 4 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes ||
        !ParseU64(tokens[2], expected_term) || !IsNodeId(tokens[3])) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleActivateAuthority(
        coordinator, std::move(state_machine), std::move(principal), tokens[1],
        expected_term, tokens[3]);
  }
  if (command == "fencegroup") {
    std::uint64_t expected_term = 0;
    if (tokens.size() != 3 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes ||
        !ParseU64(tokens[2], expected_term) ||
        expected_term == std::numeric_limits<std::uint64_t>::max()) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleFenceGroup(coordinator, std::move(state_machine),
                                        std::move(principal), tokens[1],
                                        expected_term);
  }
  if (command == "transitionop") {
    if (tokens.size() != 4) {
      co_return "ERR bad-request";
    }
    MetaOperationId id{};
    MetaReplicationHistoryId history{};
    if (!ParseOperationId(tokens[1], id) ||
        !ParseReplicationHistoryId(tokens[3], history)) {
      co_return "ERR bad-request";
    }
    co_return co_await HandleTransitionOp(coordinator, std::move(state_machine),
                                          std::move(principal), id, tokens[2],
                                          history);
  }
  if (command == "pruneaudit") {
    std::uint64_t through = 0;
    if (tokens.size() != 2 || !ParseU64(tokens[1], through) || through == 0) {
      co_return "ERR bad-request";
    }
    co_return co_await HandlePruneAudit(coordinator, std::move(principal),
                                        through);
  }
  if (command == "setauditpolicy") {
    if (tokens.size() != 3) co_return "ERR bad-request";
    MetaAuditPolicy policy;
    if (tokens[1] == "disabled") {
      policy = MetaAuditPolicy::kDisabled;
    } else if (tokens[1] == "bounded-rotate") {
      policy = MetaAuditPolicy::kBoundedRotate;
    } else if (tokens[1] == "strict-export") {
      policy = MetaAuditPolicy::kStrictExport;
    } else {
      co_return "ERR bad-request";
    }
    co_return co_await HandleSetAuditPolicy(coordinator, std::move(principal),
                                            policy, tokens[2]);
  }
  if (command == "pruneoperations") {
    if (tokens.size() < 2 ||
        tokens.size() - 1 > kMaxMetaArchivedOperationSummaries) {
      co_return "ERR bad-request";
    }
    std::vector<std::uint64_t> seqs;
    seqs.reserve(tokens.size() - 1);
    for (std::size_t i = 1; i < tokens.size(); ++i) {
      std::uint64_t seq = 0;
      if (!ParseU64(tokens[i], seq) || seq == 0) {
        co_return "ERR bad-request";
      }
      seqs.push_back(seq);
    }
    co_return co_await HandlePruneOperationArchive(
        coordinator, std::move(principal), std::move(seqs));
  }
  if (command == "archiveoperations") {
    if (tokens.size() < 2 ||
        tokens.size() - 1 > kMaxMetaArchivedOperationSummaries) {
      co_return "ERR bad-request";
    }
    std::vector<std::uint64_t> seqs;
    seqs.reserve(tokens.size() - 1);
    for (std::size_t i = 1; i < tokens.size(); ++i) {
      std::uint64_t seq = 0;
      if (!ParseU64(tokens[i], seq) || seq == 0) {
        co_return "ERR bad-request";
      }
      seqs.push_back(seq);
    }
    co_return co_await HandleArchiveOperations(
        coordinator, std::move(state_machine), std::move(principal),
        std::move(seqs));
  }
  co_return "ERR unknown-command";
}

// Runs on the bycorf worker thread and suspends only on foreign-executor round
// trips. Shared references keep command dependencies alive if teardown
// releases the core's references mid-command. The proposal executor is a
// process-owned non-owning reference whose documented lifetime covers every
// worker coroutine. Transport admission has already resolved the actor; actor
// fields never come from command text. Observation access is internally
// serialized because commit-driven revalidation can run concurrently with
// this worker.
bycorf::Task<std::string> DispatchCommand(
    std::shared_ptr<MetaRaft> server,
    std::shared_ptr<MetaStateMachine> state_machine,
    const std::shared_ptr<MetaCoordinator>& coordinator,
    std::shared_ptr<MetaObservationStore> obs_store,
    bycorf::ForeignExecutor foreign_executor,
    MetaProposalExecutor& proposal_executor,
    std::shared_ptr<MetaMembershipGate> membership_gate,
    const MetaPrincipalIdentity& identity, AuthenticatedPrincipal principal,
    std::string_view local_data_control_endpoint,
    std::shared_ptr<MetaClusterStatusService> cluster_status_service,
    std::shared_ptr<MetaDataControlRuntimeStatus> data_control_runtime_status,
    std::shared_ptr<MetaAutomaticFailoverDiagnosticsRegistry>
        automatic_failover_diagnostics,
    std::uint32_t observation_ttl_ms, std::size_t* retained_status_bytes,
    std::string_view line, const bool* shutdown, bool creation_enabled,
    bool membership_enabled) {
  const std::vector<std::string> tokens = SplitTokens(line);
  if (tokens.empty()) {
    co_return "ERR bad-request";
  }
  const std::string& command = tokens[0];
  MetaAccess access = MetaAccess::kPrivileged;
  std::string_view target_node_id;
  if (command == "status") {
    access = MetaAccess::kStatus;
  } else if (command == "clusterhead" || command == "clusterstatus" ||
             command == "clustercreate" || command == "failover" ||
             command == "promote") {
    // Cluster-wide topology and readiness are operator-only even though the
    // legacy local status verb is also visible to a Data-node identity.
    if (identity.role_ != MetaPrincipalRole::kOperator) {
      co_return "ERR forbidden";
    }
  } else if (command == "adoptsession" && tokens.size() >= 2) {
    access = MetaAccess::kObservationWrite;
    target_node_id = tokens[1];
  } else if (command == "obs" && tokens.size() >= 3) {
    access = MetaAccess::kObservationWrite;
    target_node_id = tokens[2];
  }
  if (!AuthorizeMetaAccess(identity, access, target_node_id).ok()) {
    co_return "ERR forbidden";
  }
  if (command == "clusterhead") {
    if (tokens.size() != 2 || tokens[1] != "1") {
      co_return "ERR bad-request";
    }
    co_return BuildClusterHeadReply(server, state_machine);
  }
  if (command == "clusterstatus") {
    if (tokens.size() != 2 || tokens[1] != "1") {
      co_return "ERR bad-request";
    }
    if (!cluster_status_service->TryBeginCapture()) co_return "ERR busy";
    auto reply = std::make_shared<AsyncReply>();
    const absl::Status submitted = proposal_executor.Submit(
        [server, state_machine, data_control_runtime_status,
         automatic_failover_diagnostics, observation_ttl_ms,
         cluster_status_service, foreign_executor, reply]() mutable {
          std::string result;
          try {
            result = BuildClusterStatusReply(
                server, state_machine, data_control_runtime_status,
                automatic_failover_diagnostics, observation_ttl_ms);
          } catch (...) {
            result = "ERR state_corrupt";
          }
          std::size_t retained_bytes = 0;
          if (result.starts_with("OK clusterstatus 1 ")) {
            retained_bytes = result.size() + 1;
            if (!cluster_status_service->TryRetain(retained_bytes)) {
              retained_bytes = 0;
              result = "ERR busy";
            }
          }
          // The immutable, bracketed response no longer owns capture
          // admission. Sending is independently bounded by the retained-byte
          // budget and deadline on the Bycorf worker.
          cluster_status_service->EndCapture();
          CompleteAsyncReply(foreign_executor, std::move(reply),
                             std::move(result), cluster_status_service,
                             retained_bytes);
        });
    if (!submitted.ok()) {
      cluster_status_service->EndCapture();
      co_return "ERR busy";
    }
    co_return co_await AsyncReplyAwaiter(std::move(reply),
                                         retained_status_bytes);
  }
  if (command == "clustercreate") {
    if (!creation_enabled)
      co_return ClusterCreateError("preflight", "pre-commit-failed",
                                   "cluster-create reconciler is unavailable");
    if (tokens.size() != 3) {
      co_return ClusterCreateError("decode", "bad-request",
                                   "expected clustercreate 1 <hex-payload>");
    }
    if (tokens[1] != "1") {
      co_return ClusterCreateError("decode", "bad-request",
                                   "unsupported protocol version");
    }
    MetaOperationId root_operation_id{};
    auto manifest = DecodeClusterCreateRequest(line, &root_operation_id);
    if (!manifest.ok()) {
      co_return ClusterCreateError("decode", "bad-request",
                                   std::string(manifest.status().message()));
    }
    co_return co_await HandleClusterCreate(
        server, state_machine, coordinator, membership_gate,
        std::move(principal), *manifest, root_operation_id, shutdown);
  }
  if (command == "promote") {
    if (tokens.size() != 5 || tokens[2] != "--node" ||
        tokens[4] != "--accept-data-loss" || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaGroupIdBytes ||
        !cluster::control::IsCanonicalIdentity160(tokens[3])) {
      co_return "ERR promote expected: promote GROUP --node NODE --accept-data-loss";
    }
    co_return co_await HandlePromote(coordinator, state_machine, obs_store,
                                     std::move(principal), tokens[1],
                                     tokens[3]);
  }
  if (command == "failover") {
    if (tokens.size() != 3 || tokens[1] != "1") {
      co_return FailoverError("decode", "bad-request");
    }
    auto request = DecodeFailoverAdminRequest(line);
    if (!request.ok()) {
      co_return FailoverError("decode", "bad-request");
    }
    co_return co_await HandleFailover(coordinator, std::move(state_machine),
                                      std::move(principal), *request);
  }
  if (command == "submitop" || command == "completeop" ||
      command == "abortop" || command == "archiveoperations" ||
      command == "registernode" || command == "creategroup" ||
      command == "assignnode" || command == "begingroupterm" ||
      command == "putpolicy" || command == "setslotmap" ||
      command == "activateauthority" || command == "fencegroup" ||
      command == "transitionop" || command == "pruneaudit" ||
      command == "setauditpolicy" || command == "pruneoperations") {
    std::string reply = co_await DispatchMutationVerb(
        coordinator, state_machine, std::move(principal), command, tokens);
    co_return reply;
  }
  if (command == "getop") {
    if (tokens.size() != 2) {
      co_return "ERR bad-request";
    }
    MetaOperationId id{};
    if (!ParseOperationId(tokens[1], id)) {
      co_return "ERR bad-request";
    }
    co_return HandleGetOp(std::move(state_machine), id);
  }
  if (command == "getnode") {
    if (tokens.size() != 2 || !IsNodeId(tokens[1])) {
      co_return "ERR bad-request";
    }
    co_return HandleGetNode(std::move(state_machine), tokens[1]);
  }
  if (command == "getpolicy") {
    if (tokens.size() != 2 || tokens[1].empty() ||
        tokens[1].size() > kMaxMetaPolicyIdBytes) {
      co_return "ERR bad-request";
    }
    if (!server->is_leader() || !server->is_leader_alive() ||
        !server->is_leader_sm_fully_caught_up()) {
      co_return "ERR not-leader";
    }
    co_return HandleGetPolicy(std::move(state_machine), tokens[1]);
  }
  if (command == "adoptsession") {
    if (tokens.size() != 4) {
      co_return "ERR bad-request";
    }
    MetaObservationIdentity identity;
    if (!ParseObsIdentity(tokens, 1, identity)) {
      co_return "ERR bad-request";
    }
    co_return HandleAdoptSession(obs_store, identity);
  }
  if (command == "obs") {
    // obs <kind> <node> <boot> <gen> <kind fields...>; see the header.
    if (tokens.size() < 5) {
      co_return "ERR bad-request";
    }
    const std::string& kind = tokens[1];
    MetaObservationIdentity identity;
    if (!ParseObsIdentity(tokens, 2, identity)) {
      co_return "ERR bad-request";
    }
    MetaObservation observation;
    observation.identity_ = std::move(identity);
    if (kind == "boot" && tokens.size() == 5) {
      observation.payload_ = MetaNodeBootObs{};
    } else if (kind == "health" && tokens.size() == 6) {
      MetaNodeHealthObs payload;
      payload.health_ = tokens[5];
      observation.payload_ = std::move(payload);
    } else if (kind == "candidate" && tokens.size() == 10) {
      MetaCandidateProgressObs payload;
      payload.group_id_ = tokens[5];
      if (!ParseU64(tokens[6], payload.group_term_) ||
          !ParseU64(tokens[7], payload.population_manifest_revision_) ||
          !ParseU64(tokens[8], payload.partition_replication_epoch_) ||
          !ParseReplicationHistoryId(tokens[9],
                                     payload.replication_history_id_)) {
        co_return "ERR bad-request";
      }
      observation.payload_ = std::move(payload);
    } else {
      co_return "ERR bad-request";
    }
    co_return HandleObsIngest(obs_store, std::move(state_machine),
                              std::move(observation));
  }
  if (command == "observations") {
    if (tokens.size() == 1) {
      co_return HandleObservations(obs_store, std::move(state_machine),
                                   std::nullopt);
    }
    if (tokens.size() == 2 && !tokens[1].empty() &&
        tokens[1].size() <= kMaxMetaGroupIdBytes) {
      co_return HandleObservations(obs_store, std::move(state_machine),
                                   tokens[1]);
    }
    co_return "ERR bad-request";
  }
  if (command == "obsaudit") {
    if (tokens.size() != 1) {
      co_return "ERR bad-request";
    }
    co_return HandleObsAudit(obs_store);
  }
  if (command == "exportaudit") {
    std::uint64_t through = 0;
    if (tokens.size() != 2 || !ParseU64(tokens[1], through) || through == 0) {
      co_return "ERR bad-request";
    }
    auto exported =
        state_machine->StoresSnapshot().audit_.ExportThrough(through);
    if (!exported.ok()) co_return "ERR rejected";
    co_return "OK " + HexEncode(*exported);
  }
  if (command == "exportoperations") {
    if (tokens.size() != 1) co_return "ERR bad-request";
    auto exported = state_machine->StoresSnapshot().operation_.ExportArchive();
    if (!exported.ok()) co_return "ERR rejected";
    co_return "OK " + HexEncode(*exported);
  }
  if (command == "status") {
    const MetaAuditStore audit = state_machine->StoresSnapshot().audit_;
    co_return "OK leader=" + std::to_string(server->is_leader() ? 1 : 0) +
        " id=" + std::to_string(server->get_id()) +
        " committed=" + std::to_string(server->get_committed_log_idx()) +
        " snapshot_idx=" + std::to_string(server->get_last_snapshot_idx()) +
        " first_log_idx=" + std::to_string(server->FirstLogIndex()) +
        " rpc_failures=" + std::to_string(server->RpcFailures()) +
        " vote_rejections=" + std::to_string(server->VoteRejections()) +
        " vote_grants=" + std::to_string(server->VoteGrants()) +
        " gc_failures=" + std::to_string(server->GcFailures()) +
        " pending_raft_bytes=" + std::to_string(server->PendingBytes()) +
        " term=" + std::to_string(server->get_term()) +
        " initial_bindings_pending=" +
        std::to_string(server->initial_bindings_pending() ? 1 : 0) +
        " audit_policy=" + AuditPolicyName(audit.policy()) +
        " audit_size=" + std::to_string(audit.size()) +
        " audit_capacity=" + std::to_string(audit.capacity()) +
        " audit_dropped_total=" + std::to_string(audit.dropped_total()) +
        " audit_dropped_through=" + std::to_string(audit.dropped_through());
  }
  if (command == "addsrv" || command == "removesrv") {
    if (!membership_enabled) co_return "ERR membership-unavailable";
    const bool add = command == "addsrv";
    if ((!add && tokens.size() != 2u) ||
        (add && tokens.size() != 5u && tokens.size() != 6u)) {
      co_return "ERR bad-request";
    }
    int server_id = 0;
    if (!ParseServerId(tokens[1], server_id)) {
      co_return "ERR bad-request";
    }
    std::string member_principal = "lavik://meta/" + std::to_string(server_id);
    if (add && tokens.size() == 6u) {
      member_principal = tokens[5];
    }
    co_return co_await HandleConfigChange(
        std::move(server), std::move(state_machine), coordinator,
        std::move(principal), membership_gate, add, server_id,
        add ? tokens[2] : std::string(),
        add ? tokens[3] : std::string(local_data_control_endpoint),
        add ? tokens[4] : std::string(), member_principal, shutdown);
  }
  if (command == "snapshot") {
    // The Go application executor captures an exact cut. This wait runs on
    // the proposal executor; its result includes durable file/marker
    // publication and memory compaction, while whole-file reclamation remains
    // asynchronous.
    std::shared_ptr<AsyncReply> state = std::make_shared<AsyncReply>();
    const absl::Status submitted =
        proposal_executor.Submit([server, foreign_executor, state]() mutable {
          try {
            MetaRaft::create_snapshot_options options;
            options.serialize_commit_ = true;
            const std::uint64_t idx = server->create_snapshot(options);
            CompleteAsyncReply(
                foreign_executor, std::move(state),
                idx == 0 ? "ERR snapshot-failed" : "OK " + std::to_string(idx));
          } catch (...) {
            CompleteAsyncReply(foreign_executor, std::move(state),
                               "ERR exception");
          }
        });
    if (!submitted.ok()) co_return "ERR executor-unavailable";
    co_return co_await AsyncReplyAwaiter(std::move(state));
  }
  co_return "ERR unknown-command";
}

}  // namespace

class MetaCtlServer::SessionConnectionBorrow {
 public:
  SessionConnectionBorrow(CorePtr core, bycorf::Connection* connection)
      : core_(std::move(core)), connection_(connection) {
    core_->sessions_.push_back(connection_);
    bycorf::BorrowConnectionStorage(connection_);
  }

  SessionConnectionBorrow(SessionConnectionBorrow&& other) noexcept
      : core_(std::move(other.core_)), connection_(other.connection_) {
    other.connection_ = nullptr;
  }
  SessionConnectionBorrow(const SessionConnectionBorrow&) = delete;
  SessionConnectionBorrow& operator=(const SessionConnectionBorrow&) = delete;
  SessionConnectionBorrow& operator=(SessionConnectionBorrow&&) = delete;

  ~SessionConnectionBorrow() {
    if (connection_ == nullptr) return;
    const auto session = std::find(core_->sessions_.begin(),
                                   core_->sessions_.end(), connection_);
    if (session != core_->sessions_.end()) {
      *session = core_->sessions_.back();
      core_->sessions_.pop_back();
    }
    bycorf::ReleaseConnectionStorage(connection_);
    NotifyCtlShutdownDrained(*core_);
  }

 private:
  CorePtr core_;
  bycorf::Connection* connection_;
};

// static
absl::Status MetaCtlServer::ValidateOptions(
    const MetaCtlServerOptions& options) {
  if (options.transport_ == MetaCtlServerOptions::Transport::kUnix) {
    if (options.unix_socket_path_.empty()) {
      return absl::InvalidArgumentError(
          "Unix ctl socket path must not be empty");
    }
    if (options.allowed_uids_.empty()) {
      return absl::InvalidArgumentError(
          "Unix ctl socket requires at least one allowed uid");
    }
    if (options.port_ != 0 || !options.bind_host_.empty() ||
        !options.tls_ca_cert_file_.empty() || !options.tls_cert_file_.empty() ||
        !options.tls_key_file_.empty()) {
      return absl::InvalidArgumentError(
          "Unix ctl options cannot be mixed with TCP or TLS options");
    }
    return absl::OkStatus();
  }

  if (options.port_ == 0) {
    return absl::InvalidArgumentError("TCP ctl port must be non-zero");
  }
  in_addr address4{};
  in6_addr address6{};
  const bool numeric =
      ::inet_pton(AF_INET, options.bind_host_.c_str(), &address4) == 1 ||
      ::inet_pton(AF_INET6, options.bind_host_.c_str(), &address6) == 1;
  if (!numeric || options.bind_host_ == "0.0.0.0" ||
      options.bind_host_ == "::") {
    return absl::InvalidArgumentError(
        "TCP ctl host must be a concrete numeric address");
  }
  const auto local_endpoint =
      lavik::ParseNumericEndpoint(options.local_ctl_endpoint_);
  if (!local_endpoint.has_value() ||
      local_endpoint->host_ != options.bind_host_ ||
      local_endpoint->port_ != options.port_) {
    return absl::InvalidArgumentError(
        "TCP ctl bind must equal its process-local ctl endpoint");
  }
  if (options.transport_ != MetaCtlServerOptions::Transport::kTcpPlaintext &&
      options.transport_ != MetaCtlServerOptions::Transport::kTcpMtls) {
    return absl::InvalidArgumentError("unknown TCP ctl transport");
  }
  const bool tls_any = !options.tls_ca_cert_file_.empty() ||
                       !options.tls_cert_file_.empty() ||
                       !options.tls_key_file_.empty();
  const bool tls_all = !options.tls_ca_cert_file_.empty() &&
                       !options.tls_cert_file_.empty() &&
                       !options.tls_key_file_.empty();
  if (tls_any != tls_all) {
    return absl::InvalidArgumentError(
        "TCP ctl TLS options must be complete or omitted");
  }
  if (options.transport_ == MetaCtlServerOptions::Transport::kTcpPlaintext &&
      tls_any) {
    return absl::InvalidArgumentError(
        "plaintext TCP ctl cannot carry TLS options");
  }
  if (options.transport_ == MetaCtlServerOptions::Transport::kTcpMtls &&
      !tls_all) {
    return absl::InvalidArgumentError(
        "mTLS TCP ctl requires CA, certificate and private key");
  }
  if (!options.unix_socket_path_.empty() || !options.allowed_uids_.empty()) {
    return absl::InvalidArgumentError(
        "TCP ctl options cannot be mixed with Unix socket options");
  }
  return absl::OkStatus();
}

// static
absl::StatusOr<std::shared_ptr<MetaCtlServer>> MetaCtlServer::Create(
    bycorf::ForeignExecutor foreign_executor, std::shared_ptr<MetaRaft> server,
    std::shared_ptr<MetaStateMachine> state_machine,
    std::shared_ptr<MetaCoordinator> coordinator,
    std::shared_ptr<MetaObservationStore> obs_store,
    MetaProposalExecutor& proposal_executor,
    std::shared_ptr<MetaMembershipGate> membership_gate,
    MetaCtlServerOptions options) {
  if (!foreign_executor.valid()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "foreign executor must be valid");
  }
  if (server == nullptr || state_machine == nullptr || coordinator == nullptr) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "server, state machine, and coordinator must not be null");
  }
  if (obs_store == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "observation store must not be null");
  }
  if (membership_gate == nullptr) {
    return absl::InvalidArgumentError("membership gate must not be null");
  }
  const absl::Status valid = ValidateOptions(options);
  if (!valid.ok()) return valid;
  auto core = std::make_shared<Core>();
  core->foreign_executor_ = foreign_executor;
  core->server_ = std::move(server);
  core->state_machine_ = std::move(state_machine);
  core->coordinator_ = std::move(coordinator);
  core->obs_store_ = std::move(obs_store);
  core->proposal_executor_ = &proposal_executor;
  core->membership_gate_ = std::move(membership_gate);
  core->options_ = std::move(options);
  if (core->options_.cluster_status_service_ == nullptr) {
    core->options_.cluster_status_service_ =
        std::make_shared<MetaClusterStatusService>();
  }
  if (core->options_.data_control_runtime_status_ == nullptr) {
    core->options_.data_control_runtime_status_ =
        std::make_shared<MetaDataControlRuntimeStatus>();
  }
  if (core->options_.automatic_failover_diagnostics_ == nullptr) {
    core->options_.automatic_failover_diagnostics_ =
        std::make_shared<MetaAutomaticFailoverDiagnosticsRegistry>();
  }
  if (core->options_.transport_ == MetaCtlServerOptions::Transport::kTcpMtls) {
    bycorf::TlsServerOptions tls;
    tls.cert_file_ = core->options_.tls_cert_file_;
    tls.key_file_ = core->options_.tls_key_file_;
    tls.ca_cert_file_ = core->options_.tls_ca_cert_file_;
    tls.client_auth_ = bycorf::TlsClientAuth::kRequired;
    auto context = bycorf::TlsContext::CreateServer(tls);
    if (!context.ok()) return context.status();
    core->tls_context_ = std::move(*context);
  }
  return std::shared_ptr<MetaCtlServer>(new MetaCtlServer(std::move(core)));
}

MetaCtlServer::~MetaCtlServer() { Shutdown(); }

void MetaCtlServer::Start() {
  CorePtr core = core_;
  const bool accepted = core->foreign_executor_.Notify([core]() noexcept {
    bycorf::Worker& worker = *bycorf::ThisWorker().self_;
    if (core->listening_ || core->shutdown_) {
      return;
    }
    core->worker_ = &worker;
    absl::Status bound;
    if (core->options_.transport_ == MetaCtlServerOptions::Transport::kUnix) {
      bound = core->listener_.BindUnix(
          &worker, core->options_.unix_socket_path_, /*backlog=*/128,
          /*mode=*/0600);
    } else {
      bound = core->listener_.Bind(&worker, core->options_.bind_host_,
                                   core->options_.port_, /*backlog=*/128,
                                   /*reuse_port=*/false);
    }
    {
      std::lock_guard<std::mutex> lock(core->status_mu_);
      core->status_ = bound;
    }
    if (!bound.ok()) {
      return;
    }
    core->listening_ = true;
    core->accept_loop_running_ = true;
    worker.Spawn(AcceptLoop(core));
  });
  if (!accepted) {
    std::lock_guard<std::mutex> lock(core->status_mu_);
    core->status_ = absl::UnavailableError("Bycorf worker is stopping");
  }
}

void MetaCtlServer::Shutdown() {
  CorePtr core = core_;
  if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
  auto complete = std::make_shared<std::promise<void>>();
  std::future<void> done = complete->get_future();
  if (!core->foreign_executor_.Notify([core, complete]() noexcept {
        core->shutdown_drain_waiters_.push_back(complete);
        if (!core->shutdown_) {
          core->shutdown_ = true;
          core->listening_ = false;
          if (core->accept_loop_running_) {
            absl::Status wake_status =
                absl::UnavailableError("ctl shutdown wake was not attempted");
            for (int attempt = 0; attempt != 3; ++attempt) {
              auto wake = OpenCtlShutdownAcceptWakeSocket(core->options_);
              if (wake.ok()) {
                core->shutdown_accept_wake_fd_ = *wake;
                wake_status = absl::OkStatus();
                break;
              }
              wake_status = wake.status();
            }
            if (!wake_status.ok()) {
              spdlog::critical("cannot wake ctl accept loop for shutdown: {}",
                               wake_status.message());
              std::terminate();
            }
          } else {
            (void)core->listener_.Close();
          }
          if (core->worker_ != nullptr) {
            const std::vector<bycorf::Connection*> sessions = core->sessions_;
            for (bycorf::Connection* connection : sessions) {
              if (connection != nullptr && connection->file_.fd_ >= 0) {
                (void)::shutdown(connection->file_.fd_, SHUT_RDWR);
              }
              core->worker_->BeginClose(
                  connection, absl::CancelledError("ctl server shutdown"),
                  bycorf::CloseMode::kLocalClose);
            }
          }
        }
        NotifyCtlShutdownDrained(*core);
      })) {
    if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  core->shutdown_complete_.store(true, std::memory_order_release);
}

absl::Status MetaCtlServer::status() const {
  std::lock_guard<std::mutex> lock(core_->status_mu_);
  return core_->status_;
}

bycorf::Task<absl::Status> MetaCtlServer::AcceptLoop(CorePtr core) {
  bycorf::Worker& worker = *core->worker_;
  while (core->listening_) {
    auto accepted = co_await core->listener_.Accept();
    if (!accepted.ok()) {
      // A closed listener fails the pending accept; that is the
      // Shutdown() path.
      if (!core->listening_) {
        break;
      }
      const absl::Status slept =
          co_await bycorf::SleepFor(worker, std::chrono::milliseconds(10));
      if (!slept.ok()) {
        if (!core->listening_) break;
        co_return slept;
      }
      continue;
    }
    bycorf::Connection* connection = *accepted;
    if (!core->listening_) {
      if (connection != nullptr && connection->file_.fd_ >= 0) {
        (void)::shutdown(connection->file_.fd_, SHUT_RDWR);
      }
      worker.BeginClose(connection,
                        absl::CancelledError("ctl listener is shutting down"),
                        bycorf::CloseMode::kLocalClose);
      if (core->shutdown_accept_wake_fd_ >= 0) {
        (void)::shutdown(core->shutdown_accept_wake_fd_, SHUT_RDWR);
        (void)::close(core->shutdown_accept_wake_fd_);
        core->shutdown_accept_wake_fd_ = -1;
      }
      (void)core->listener_.Close();
      break;
    }
    // Frame ownership closes the accept/shutdown race: even if Spawn rejects
    // the task before its body runs, destruction unregisters the session and
    // releases its storage borrow.
    worker.Spawn(SessionLoop(core, bycorf::TcpStream(connection), connection,
                             SessionConnectionBorrow(core, connection)));
  }
  if (core->shutdown_accept_wake_fd_ >= 0) {
    (void)::shutdown(core->shutdown_accept_wake_fd_, SHUT_RDWR);
    (void)::close(core->shutdown_accept_wake_fd_);
    core->shutdown_accept_wake_fd_ = -1;
  }
  (void)core->listener_.Close();
  core->accept_loop_running_ = false;
  NotifyCtlShutdownDrained(*core);
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> MetaCtlServer::SessionLoop(
    CorePtr core, bycorf::TcpStream stream, bycorf::Connection* connection,
    SessionConnectionBorrow borrow) {
  // This frame-owned parameter unregisters the task and releases the
  // Connection during frame destruction, after body-local users have unwound.
  // It also covers shutdown before the coroutine body starts.
  (void)borrow;
  absl::StatusOr<MetaPrincipalIdentity> identity =
      absl::UnauthenticatedError("ctl session was not authenticated");
  if (core->options_.transport_ == MetaCtlServerOptions::Transport::kUnix) {
    ucred credentials{};
    socklen_t size = sizeof(credentials);
    if (::getsockopt(stream.NativeFd(), SOL_SOCKET, SO_PEERCRED, &credentials,
                     &size) != 0 ||
        size != sizeof(credentials)) {
      spdlog::warn("ctl rejected Unix peer: SO_PEERCRED failed: {}",
                   std::strerror(errno));
      (void)stream.Close();
      co_return absl::UnauthenticatedError("SO_PEERCRED failed");
    }
    identity = AuthenticateLocalOperator(credentials.uid,
                                         core->options_.allowed_uids_);
  } else if (core->options_.transport_ ==
             MetaCtlServerOptions::Transport::kTcpMtls) {
    const absl::Status tls =
        co_await stream.StartTls(core->tls_context_, /*server=*/true);
    if (!tls.ok()) {
      spdlog::warn("ctl rejected TCP peer during mTLS handshake: {}",
                   tls.message());
      (void)stream.Close();
      co_return tls;
    }
    auto sans = stream.PeerCertificateUriSans();
    if (!sans.ok()) {
      (void)stream.Close();
      co_return sans.status();
    }
    identity = AuthenticateMetaUriSans(*sans);
    if (identity.ok() && identity->role_ == MetaPrincipalRole::kMetaMember) {
      identity = absl::PermissionDeniedError(
          "Meta member certificate is not an admin/control identity");
    }
  } else {
    // Plain TCP has no trustworthy per-peer identity. Use one stable actor so
    // audit consumers cannot mistake a source address for authentication.
    // Reachability of this explicitly configured listener is the operator
    // authorization boundary.
    identity = MetaPrincipalIdentity{
        "lavik://operator/plaintext", MetaPrincipalRole::kOperator, {}};
  }
  if (!identity.ok()) {
    spdlog::warn("ctl rejected unauthenticated peer: {}",
                 identity.status().message());
    (void)stream.Close();
    co_return identity.status();
  }
  AuthenticatedPrincipal authenticated(identity->principal_,
                                       MetaPrincipalPasskey{});
  std::byte chunk[4096];
  std::string pending;
  bool drop = false;
  while (!drop) {
    auto read = co_await stream.ReadSome(chunk);
    if (!read.ok() || *read == 0) {
      break;  // peer EOF, Shutdown() close, or worker teardown
    }
    pending.append(reinterpret_cast<const char*>(chunk), *read);

    std::size_t newline = std::string::npos;
    while ((newline = pending.find('\n')) != std::string::npos) {
      std::string line = pending.substr(0, newline);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      pending.erase(0, newline + 1);
      // One in-flight command per connection: replies stay FIFO and the
      // gate driver is synchronous per connection anyway.
      std::size_t retained_status_bytes = 0;
      std::string reply = co_await DispatchCommand(
          core->server_, core->state_machine_, core->coordinator_,
          core->obs_store_, core->foreign_executor_, *core->proposal_executor_,
          core->membership_gate_, *identity, authenticated,
          core->options_.local_data_control_endpoint_,
          core->options_.cluster_status_service_,
          core->options_.data_control_runtime_status_,
          core->options_.automatic_failover_diagnostics_,
          core->options_.observation_ttl_ms_, &retained_status_bytes, line,
          &core->shutdown_,
          core->options_.cluster_create_reconciler_ != nullptr &&
              core->options_.cluster_create_reconciler_->accepting(),
          core->options_.membership_reconciler_ != nullptr &&
              core->options_.membership_reconciler_->accepting());
      reply.push_back('\n');
      std::unique_ptr<cluster::control::ControlDeadlineWatchdog>
          status_write_deadline;
      if (retained_status_bytes != 0) {
        status_write_deadline =
            std::make_unique<cluster::control::ControlDeadlineWatchdog>(
                *core->worker_, [connection] {
                  // Closing the native transport wakes either a plaintext or
                  // TLS WriteAll without coupling Admin backpressure to Data
                  // heartbeat progress on this shared worker.
                  if (connection != nullptr && connection->file_.fd_ >= 0) {
                    (void)::shutdown(connection->file_.fd_, SHUT_RDWR);
                  }
                });
        if (absl::Status armed =
                status_write_deadline->Arm(kClusterStatusSendDeadline);
            !armed.ok()) {
          core->options_.cluster_status_service_->Release(
              retained_status_bytes);
          drop = true;
          break;
        }
      }
      const absl::Status written =
          co_await stream.WriteAll(std::span<const std::byte>(
              reinterpret_cast<const std::byte*>(reply.data()), reply.size()));
      if (status_write_deadline != nullptr) {
        (void)status_write_deadline->Disarm();
      }
      if (retained_status_bytes != 0) {
        core->options_.cluster_status_service_->Release(retained_status_bytes);
      }
      if (!written.ok()) {
        drop = true;
        break;
      }
    }
    if (!drop && pending.size() > kMaxLineBytes) {
      const std::string reply = "ERR bad-request\n";
      (void)co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(reply.data()), reply.size()));
      break;  // framing abuse: close without reading further
    }
  }

  (void)stream.Close();
  co_return absl::OkStatus();
}

}  // namespace lavik::meta
