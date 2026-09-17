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

#include <chrono>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "lavik/meta/cluster_status.h"

namespace {

using lavik::meta::ClusterAutomaticFailoverState;
using lavik::meta::ClusterBlockerWireV1;
using lavik::meta::ClusterDataNodeRole;
using lavik::meta::ClusterHeadWireV1;
using lavik::meta::ClusterMetaMemberWireV1;
using lavik::meta::ClusterMetaRole;
using lavik::meta::ClusterOperator;
using lavik::meta::ClusterStateWireV1;
using lavik::meta::ClusterStatusOptions;
using lavik::meta::ClusterStatusResult;
using lavik::meta::ClusterStatusWireV1;
using lavik::meta::DecodeClusterHeadReply;
using lavik::meta::DecodeClusterStatusReply;
using lavik::meta::EncodeClusterHeadReply;
using lavik::meta::EncodeClusterStatusReply;
using lavik::meta::MetaAdminTarget;
using lavik::meta::RenderClusterStatusJson;
using lavik::meta::RenderClusterStatusText;

ClusterStatusWireV1 ReadyStatus(std::vector<ClusterMetaMemberWireV1> members,
                                std::uint32_t responder_id = 1) {
  ClusterStatusWireV1 status;
  status.capture_ = {.responder_id_ = responder_id,
                     .term_ = 9,
                     .config_index_ = 44,
                     .committed_index_ = 50,
                     .topology_epoch_ = 3};
  status.cluster_state_ = ClusterStateWireV1::kCreated;
  status.lifecycle_revision_ = 2;
  status.root_operation_id_ = "00112233445566778899aabbccddeeff";
  status.genesis_commit_index_ = 10;
  status.meta_available_ = true;
  status.meta_membership_stable_ = true;
  status.topology_converged_ = true;
  status.serving_ready_ = true;
  status.cluster_ready_ = true;
  status.meta_members_ = std::move(members);
  status.data_nodes_.push_back({
      .node_id_ = "data-1",
      .role_ = ClusterDataNodeRole::kPrimary,
      .group_id_ = "group-1",
      .current_session_ = true,
      .projection_current_ = true,
      .health_fresh_ = true,
      .population_current_ = true,
      .lease_status_ = lavik::meta::ClusterLeaseStatus::kRecentlyGranted,
  });
  status.groups_.push_back(
      {.group_id_ = "group-1",
       .term_ = 4,
       .owner_node_id_ = "data-1",
       .serving_ready_ = true,
       .topology_converged_ = true,
       .automatic_failover_state_ = ClusterAutomaticFailoverState::kHealthy,
       .effective_threshold_ms_ = 1'000,
       .blocked_reason_ = std::nullopt});
  status.slot_ranges_.push_back(
      {.first_ = 0, .last_ = 16'383, .group_id_ = "group-1"});
  return status;
}

TEST(MetaClusterStatusWireTest, RoundTripsStrictBoundedV1Messages) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kFollower;
  head.term_ = 9;
  head.leader_id_ = 2;
  head.config_index_ = 44;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101"},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102", .is_leader_ = true},
  };
  auto encoded_head = EncodeClusterHeadReply(head);
  ASSERT_TRUE(encoded_head.ok()) << encoded_head.status();
  EXPECT_EQ(encoded_head->substr(std::string("OK clusterhead 1 ").size(), 4),
            "0001");
  auto decoded_head = DecodeClusterHeadReply(*encoded_head);
  ASSERT_TRUE(decoded_head.ok()) << decoded_head.status();
  EXPECT_EQ(*decoded_head, head);
  EXPECT_FALSE(DecodeClusterHeadReply(*encoded_head + "00").ok());

  ClusterStatusWireV1 status;
  status.capture_ = {.responder_id_ = 2,
                     .term_ = 9,
                     .config_index_ = 44,
                     .committed_index_ = 50,
                     .topology_epoch_ = 3};
  status.meta_available_ = true;
  status.meta_membership_stable_ = true;
  status.meta_members_ = head.meta_members_;
  status.data_nodes_.push_back(
      {.node_id_ = "data-1", .role_ = ClusterDataNodeRole::kPrimary});
  status.blockers_.push_back(ClusterBlockerWireV1{
      .code_ = "slots_unassigned", .scope_ = "cluster", .detail_ = "0..16383"});
  auto encoded_status = EncodeClusterStatusReply(status);
  ASSERT_TRUE(encoded_status.ok()) << encoded_status.status();
  EXPECT_EQ(
      encoded_status->substr(std::string("OK clusterstatus 1 ").size(), 4),
      "0001");
  auto decoded_status = DecodeClusterStatusReply(*encoded_status);
  ASSERT_TRUE(decoded_status.ok()) << decoded_status.status();
  EXPECT_EQ(*decoded_status, status);
  std::string old_status_payload = *encoded_status;
  old_status_payload.replace(std::string("OK clusterstatus 1 ").size(), 4,
                             "0002");
  EXPECT_FALSE(DecodeClusterStatusReply(old_status_payload).ok());
  EXPECT_FALSE(DecodeClusterStatusReply("OK clusterstatus 2 00").ok());
  status.data_nodes_.front().role_ = static_cast<ClusterDataNodeRole>(2);
  EXPECT_FALSE(EncodeClusterStatusReply(status).ok());
}

TEST(MetaClusterStatusWireTest,
     RoundTripsRequiredAutomaticFailoverDiagnostics) {
  ClusterStatusWireV1 status = ReadyStatus({{.server_id_ = 1,
                                             .ctl_endpoint_ = "127.0.0.1:7101",
                                             .is_leader_ = true}});
  auto& group = status.groups_.front();
  group.automatic_failover_state_ = ClusterAutomaticFailoverState::kSuspect;
  group.current_reason_ = "heartbeat_expired";
  group.suspect_elapsed_ms_ = 750;
  group.effective_threshold_ms_ = 1'000;
  group.blocked_reason_.reset();

  auto encoded = EncodeClusterStatusReply(status);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = DecodeClusterStatusReply(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, status);

  group.automatic_failover_state_ =
      static_cast<ClusterAutomaticFailoverState>(255);
  EXPECT_FALSE(EncodeClusterStatusReply(status).ok());
}

TEST(MetaClusterStatusWireTest,
     RejectsContradictoryAutomaticFailoverDiagnostics) {
  const ClusterStatusWireV1 valid =
      ReadyStatus({{.server_id_ = 1,
                    .ctl_endpoint_ = "127.0.0.1:7101",
                    .is_leader_ = true}});

  const auto rejected = [&](auto mutate) {
    ClusterStatusWireV1 status = valid;
    mutate(status.groups_.front());
    EXPECT_FALSE(EncodeClusterStatusReply(status).ok());
  };
  rejected([](auto& group) {
    group.automatic_failover_state_ =
        static_cast<ClusterAutomaticFailoverState>(0);
    group.effective_threshold_ms_ = 0;
  });
  rejected([](auto& group) { group.effective_threshold_ms_ = 0; });
  rejected([](auto& group) { group.current_reason_ = "heartbeat_expired"; });
  rejected([](auto& group) { group.blocked_reason_ = "leadership_warmup"; });

  rejected([](auto& group) {
    group.automatic_failover_state_ = ClusterAutomaticFailoverState::kSuspect;
  });
  rejected([](auto& group) {
    group.automatic_failover_state_ = ClusterAutomaticFailoverState::kBlocked;
  });
  rejected([](auto& group) {
    group.automatic_failover_state_ =
        ClusterAutomaticFailoverState::kTriggering;
    group.current_reason_ = "heartbeat_expired";
    group.suspect_elapsed_ms_ = 999;
  });
  rejected([](auto& group) {
    group.automatic_failover_state_ = ClusterAutomaticFailoverState::kSuspect;
    group.current_reason_ = "free_form_reason";
  });
  rejected([](auto& group) {
    group.automatic_failover_state_ = ClusterAutomaticFailoverState::kBlocked;
    group.blocked_reason_ = "free_form_blocker";
  });
}

TEST(MetaClusterStatusWireTest,
     AllowsBlockedPrePolicyDiagnosticsForNonPristineGroup) {
  ClusterStatusWireV1 status;
  status.capture_ = {.responder_id_ = 1, .term_ = 2, .committed_index_ = 3};
  status.cluster_state_ = ClusterStateWireV1::kNonPristine;
  status.meta_available_ = true;
  status.meta_members_.push_back({.server_id_ = 1, .is_leader_ = true});
  status.groups_.push_back({.group_id_ = "group-before-genesis"});

  auto encoded = EncodeClusterStatusReply(status);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = DecodeClusterStatusReply(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, status);
}

TEST(MetaClusterStatusWireTest, EnforcesReadinessBasisAndReadyTopology) {
  const std::vector<ClusterMetaMemberWireV1> members = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101", .is_leader_ = true},
  };
  const ClusterStatusWireV1 ready = ReadyStatus(members);
  EXPECT_TRUE(EncodeClusterStatusReply(ready).ok());

  using ClearBasis = void (*)(ClusterStatusWireV1&);
  const ClearBasis clear_basis_cases[] = {
      [](ClusterStatusWireV1& value) { value.meta_available_ = false; },
      [](ClusterStatusWireV1& value) { value.meta_membership_stable_ = false; },
      [](ClusterStatusWireV1& value) { value.topology_converged_ = false; },
      [](ClusterStatusWireV1& value) { value.serving_ready_ = false; },
  };
  for (const ClearBasis clear_basis : clear_basis_cases) {
    ClusterStatusWireV1 not_ready = ready;
    clear_basis(not_ready);
    not_ready.cluster_ready_ = false;
    EXPECT_TRUE(EncodeClusterStatusReply(not_ready).ok());
    not_ready.cluster_ready_ = true;
    EXPECT_FALSE(EncodeClusterStatusReply(not_ready).ok());
  }

  ClusterStatusWireV1 no_slots = ready;
  no_slots.slot_ranges_.clear();
  EXPECT_FALSE(EncodeClusterStatusReply(no_slots).ok());
  ClusterStatusWireV1 stale_owner = ready;
  stale_owner.data_nodes_.front().health_fresh_ = false;
  EXPECT_FALSE(EncodeClusterStatusReply(stale_owner).ok());
}

TEST(MetaClusterStatusWireTest, ReportsCreatingAndFailedLifecycle) {
  ClusterStatusWireV1 status;
  status.capture_.responder_id_ = 1;
  status.meta_available_ = true;
  status.meta_members_ = {{.server_id_ = 1, .is_leader_ = true}};
  status.cluster_state_ = ClusterStateWireV1::kCreating;
  status.lifecycle_revision_ = 1;
  status.root_operation_id_ = "00112233445566778899aabbccddeeff";
  status.genesis_commit_index_ = 12;
  status.cluster_create_phase_ = "initialize-groups";

  auto encoded = EncodeClusterStatusReply(status);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = DecodeClusterStatusReply(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, status);

  lavik::meta::ClusterStatusOutcome outcome{
      .result_ = ClusterStatusResult::kNotReady, .status_ = status};
  auto json = RenderClusterStatusJson(outcome);
  ASSERT_TRUE(json.ok()) << json.status();
  EXPECT_NE(json->find("\"cluster_state\":\"creating\""), std::string::npos);
  EXPECT_NE(json->find("\"next_action\":"), std::string::npos);

  status.cluster_create_phase_.reset();
  EXPECT_FALSE(EncodeClusterStatusReply(status).ok());
  status.cluster_create_phase_ = "initialize-groups";
  status.cluster_state_ = ClusterStateWireV1::kProvisioningFailed;
  status.lifecycle_revision_ = 2;
  status.cluster_create_phase_.reset();
  status.provisioning_failure_summary_ = "cluster-create provisioning failed";
  EXPECT_TRUE(EncodeClusterStatusReply(status).ok());
  status.provisioning_failure_summary_ = "";
  EXPECT_FALSE(EncodeClusterStatusReply(status).ok());
  status.provisioning_failure_summary_ = "unsafe\nsummary";
  EXPECT_FALSE(EncodeClusterStatusReply(status).ok());
}

TEST(MetaClusterStatusOperatorTest, FollowerSeedRedirectsOnceToLeader) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kFollower;
  head.term_ = 9;
  head.leader_id_ = 2;
  head.config_index_ = 44;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101"},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102", .is_leader_ = true},
  };
  ClusterStatusWireV1 status = ReadyStatus(head.meta_members_, 2);
  const std::string head_reply = *EncodeClusterHeadReply(head);
  const std::string status_reply = *EncodeClusterStatusReply(status);

  std::vector<std::pair<std::string, std::string>> calls;
  ClusterOperator op([&](const MetaAdminTarget& target,
                         std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    calls.emplace_back(target.endpoint_, command);
    return command == "clusterhead 1" ? head_reply : status_reply;
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kTcpPlaintext,
                       .endpoint_ = "127.0.0.1:7101"};
  ClusterStatusOptions options;
  options.allow_plaintext_admin_ = true;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Status(seed, options);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->result_, ClusterStatusResult::kReady);
  ASSERT_EQ(calls.size(), 2U);
  EXPECT_EQ(calls[0], std::make_pair(std::string("127.0.0.1:7101"),
                                     std::string("clusterhead 1")));
  EXPECT_EQ(calls[1], std::make_pair(std::string("127.0.0.1:7102"),
                                     std::string("clusterstatus 1")));
}

TEST(MetaClusterStatusOperatorTest, LearnedTcpRequiresExplicitSecurityMode) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.leader_id_ = 2;
  head.meta_members_ = {
      {.server_id_ = 1},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102", .is_leader_ = true},
  };
  ClusterOperator op(
      [reply = *EncodeClusterHeadReply(head)](
          const MetaAdminTarget&, std::string_view,
          auto) -> absl::StatusOr<std::string> { return reply; });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/tmp/meta.sock"};
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Status(seed, options);
  ASSERT_FALSE(outcome.ok());
  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kFailedPrecondition);
}

TEST(MetaClusterStatusOperatorTest, RejectsPartialTlsEvenForUnixLeader) {
  ClusterOperator op([](const MetaAdminTarget&, std::string_view,
                        auto) -> absl::StatusOr<std::string> {
    return absl::UnknownError("transport must not be called");
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/tmp/meta.sock"};
  ClusterStatusOptions options;
  options.tls_.ca_file_ = "ca.pem";
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Status(seed, options);
  ASSERT_FALSE(outcome.ok());
  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST(MetaClusterStatusOperatorTest, TypedBusyExpiresAsRetryable) {
  ClusterOperator op(
      [](const MetaAdminTarget&, std::string_view,
         auto) -> absl::StatusOr<std::string> { return "ERR busy"; });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/tmp/meta.sock"};
  ClusterStatusOptions options;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(12);
  auto outcome = op.Status(seed, options);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->result_, ClusterStatusResult::kRetryable);
  EXPECT_EQ(outcome->retry_reason_, "ERR busy");
  auto json = RenderClusterStatusJson(*outcome);
  ASSERT_TRUE(json.ok()) << json.status();
  EXPECT_NE(json->find("\"result\":\"retryable\""), std::string::npos);
  EXPECT_NE(json->find("\"capture\":null"), std::string::npos);
}

TEST(MetaClusterStatusOperatorTest,
     UnixLeaderRetriesTransientCaptureWithoutTcpCredentials) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kLeader;
  head.leader_id_ = 1;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101", .is_leader_ = true},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102"},
  };
  const std::string head_reply = *EncodeClusterHeadReply(head);
  const std::string status_reply =
      *EncodeClusterStatusReply(ReadyStatus(head.meta_members_));
  const std::vector<std::string> transient_replies = {
      "ERR busy",
      "ERR cut_changed",
      "ERR not_leader",
      "ERR leader_unknown",
      "ERR leader_not_caught_up",
      "transport unavailable"};
  for (const auto& transient : transient_replies) {
    SCOPED_TRACE(transient);
    std::size_t calls = 0;
    ClusterOperator op([&](const MetaAdminTarget& target,
                           std::string_view command,
                           auto) -> absl::StatusOr<std::string> {
      ++calls;
      EXPECT_EQ(target.transport_, MetaAdminTarget::Transport::kUnix);
      EXPECT_EQ(target.endpoint_, "/tmp/meta.sock");
      if (command == "clusterhead 1") return head_reply;
      if (calls == 2) {
        if (transient == "transport unavailable") {
          return absl::UnavailableError("connection closed");
        }
        return transient;
      }
      return status_reply;
    });
    MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                         .endpoint_ = "/tmp/meta.sock"};
    ClusterStatusOptions options;
    options.deadline_ =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    auto outcome = op.Status(seed, options);
    ASSERT_TRUE(outcome.ok()) << outcome.status();
    EXPECT_EQ(outcome->result_, ClusterStatusResult::kReady);
    EXPECT_EQ(calls, 4u);
  }
}

TEST(MetaClusterStatusOperatorTest,
     UnixLeaderRediscoveryPreservesTcpAuthorization) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kLeader;
  head.leader_id_ = 1;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101", .is_leader_ = true},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102"},
  };
  const std::string first_head_reply = *EncodeClusterHeadReply(head);
  head.role_ = ClusterMetaRole::kFollower;
  head.leader_id_ = 2;
  head.meta_members_[0].is_leader_ = false;
  head.meta_members_[1].is_leader_ = true;
  const std::string next_head_reply = *EncodeClusterHeadReply(head);
  const std::string status_reply =
      *EncodeClusterStatusReply(ReadyStatus(head.meta_members_, 2));
  for (bool allow_plaintext : {false, true}) {
    SCOPED_TRACE(allow_plaintext);
    std::size_t calls = 0;
    ClusterOperator op([&](const MetaAdminTarget& target,
                           std::string_view command,
                           auto) -> absl::StatusOr<std::string> {
      ++calls;
      if (!allow_plaintext) {
        EXPECT_EQ(target.transport_, MetaAdminTarget::Transport::kUnix);
      }
      if (calls == 1) return first_head_reply;
      if (calls == 2) return "ERR cut_changed";
      if (calls == 3) {
        EXPECT_EQ(command, "clusterhead 1");
        return next_head_reply;
      }
      EXPECT_TRUE(allow_plaintext);
      EXPECT_EQ(target.transport_, MetaAdminTarget::Transport::kTcpPlaintext);
      EXPECT_EQ(target.endpoint_, "127.0.0.1:7102");
      EXPECT_EQ(command, "clusterstatus 1");
      return status_reply;
    });
    MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                         .endpoint_ = "/tmp/meta.sock"};
    ClusterStatusOptions options;
    options.allow_plaintext_admin_ = allow_plaintext;
    options.deadline_ =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    auto outcome = op.Status(seed, options);
    if (allow_plaintext) {
      ASSERT_TRUE(outcome.ok()) << outcome.status();
      EXPECT_EQ(outcome->result_, ClusterStatusResult::kReady);
      EXPECT_EQ(calls, 4u);
    } else {
      ASSERT_FALSE(outcome.ok());
      EXPECT_EQ(outcome.status().code(), absl::StatusCode::kFailedPrecondition);
      EXPECT_EQ(calls, 3u);
    }
  }
}

TEST(MetaClusterStatusOperatorTest,
     RediscoversAfterLeaderChangesDuringCapture) {
  ClusterHeadWireV1 first_head;
  first_head.responder_id_ = 1;
  first_head.role_ = ClusterMetaRole::kLeader;
  first_head.term_ = 9;
  first_head.leader_id_ = 1;
  first_head.config_index_ = 44;
  first_head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101", .is_leader_ = true},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102"},
  };
  ClusterHeadWireV1 second_head = first_head;
  second_head.role_ = ClusterMetaRole::kFollower;
  second_head.term_ = 10;
  second_head.leader_id_ = 2;
  second_head.meta_members_[0].is_leader_ = false;
  second_head.meta_members_[1].is_leader_ = true;
  ClusterStatusWireV1 status;
  status.capture_ = {.responder_id_ = 2, .term_ = 10, .config_index_ = 44};
  status.meta_available_ = true;
  status.meta_membership_stable_ = true;
  status.meta_members_ = second_head.meta_members_;
  const std::string first_head_reply = *EncodeClusterHeadReply(first_head);
  const std::string second_head_reply = *EncodeClusterHeadReply(second_head);
  const std::string status_reply = *EncodeClusterStatusReply(status);

  std::size_t call = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    ++call;
    if (call == 1) return first_head_reply;
    if (call == 2 && command == "clusterstatus 1") return "ERR cut_changed";
    if (call == 3) return second_head_reply;
    return status_reply;
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kTcpPlaintext,
                       .endpoint_ = "127.0.0.1:7101"};
  ClusterStatusOptions options;
  options.allow_plaintext_admin_ = true;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Status(seed, options);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->result_, ClusterStatusResult::kNotReady);
  EXPECT_EQ(call, 4u);
}

TEST(MetaClusterStatusOperatorTest,
     RediscoversAfterLeaderChangesDuringDiscovery) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kLeader;
  head.term_ = 9;
  head.leader_id_ = 1;
  head.config_index_ = 44;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101", .is_leader_ = true},
  };
  const std::string head_reply = *EncodeClusterHeadReply(head);
  const std::string status_reply =
      *EncodeClusterStatusReply(ReadyStatus(head.meta_members_));
  std::size_t call = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    ++call;
    if (call == 1) return "ERR cut_changed";
    return command == "clusterhead 1" ? head_reply : status_reply;
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kTcpPlaintext,
                       .endpoint_ = "127.0.0.1:7101"};
  ClusterStatusOptions options;
  options.allow_plaintext_admin_ = true;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Status(seed, options);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->result_, ClusterStatusResult::kReady);
  EXPECT_EQ(call, 3u);
}

TEST(MetaClusterStatusOperatorTest, RejectsResponderEndpointMismatch) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 2;
  head.role_ = ClusterMetaRole::kFollower;
  head.leader_id_ = 1;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101", .is_leader_ = true},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102"},
  };
  ClusterStatusWireV1 status;
  status.capture_.responder_id_ = 1;
  status.meta_members_ = head.meta_members_;
  const std::string head_reply = *EncodeClusterHeadReply(head);
  const std::string status_reply = *EncodeClusterStatusReply(status);
  std::size_t calls = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    ++calls;
    return command == "clusterhead 1" ? head_reply : status_reply;
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kTcpPlaintext,
                       .endpoint_ = "127.0.0.1:7101"};
  ClusterStatusOptions options;
  options.allow_plaintext_admin_ = true;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Status(seed, options);
  ASSERT_FALSE(outcome.ok());
  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kDataLoss);
  EXPECT_EQ(calls, 1u);
}

TEST(MetaClusterStatusOperatorTest, RejectsStatusResponderEndpointMismatch) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kFollower;
  head.leader_id_ = 2;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101"},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102", .is_leader_ = true},
  };
  ClusterStatusWireV1 status;
  status.capture_.responder_id_ = 2;
  status.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101"},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7103", .is_leader_ = true},
  };
  const std::string head_reply = *EncodeClusterHeadReply(head);
  const std::string status_reply = *EncodeClusterStatusReply(status);
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         auto) -> absl::StatusOr<std::string> {
    return command == "clusterhead 1" ? head_reply : status_reply;
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kTcpPlaintext,
                       .endpoint_ = "127.0.0.1:7101"};
  ClusterStatusOptions options;
  options.allow_plaintext_admin_ = true;
  options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Status(seed, options);
  ASSERT_FALSE(outcome.ok());
  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kDataLoss);
}

TEST(MetaClusterStatusWireTest, RejectsInconsistentLeaderIdentity) {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kLeader;
  head.leader_id_ = 2;
  head.meta_members_ = {
      {.server_id_ = 1, .ctl_endpoint_ = "127.0.0.1:7101"},
      {.server_id_ = 2, .ctl_endpoint_ = "127.0.0.1:7102", .is_leader_ = true},
  };
  auto encoded = EncodeClusterHeadReply(head);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_FALSE(DecodeClusterHeadReply(*encoded).ok());
}

TEST(MetaClusterStatusRenderTest, JsonUsesStableArraysAndStringU64) {
  ClusterStatusWireV1 status;
  status.capture_ = {.responder_id_ = 2,
                     .term_ = 9,
                     .config_index_ = 44,
                     .committed_index_ = 50,
                     .topology_epoch_ = 3};
  status.meta_available_ = true;
  status.meta_membership_stable_ = true;
  status.blockers_ = {
      {.code_ = "z", .scope_ = "node:b", .detail_ = "later"},
      {.code_ = "a", .scope_ = "cluster", .detail_ = "first"},
  };
  lavik::meta::ClusterStatusOutcome outcome{
      .result_ = ClusterStatusResult::kNotReady, .status_ = status};
  auto json = RenderClusterStatusJson(outcome);
  ASSERT_TRUE(json.ok()) << json.status();
  EXPECT_TRUE(
      json->starts_with("{\"schema_version\":1,\"result\":\"not_ready\""));
  EXPECT_NE(json->find("\"committed_index\":\"50\""), std::string::npos);
  EXPECT_LT(json->find("\"code\":\"a\""), json->find("\"code\":\"z\""));
}

TEST(MetaClusterStatusRenderTest,
     RendersAutomaticFailoverDiagnosticsWithoutPolicyData) {
  ClusterStatusWireV1 status = ReadyStatus({{.server_id_ = 1,
                                             .ctl_endpoint_ = "127.0.0.1:7101",
                                             .is_leader_ = true}});
  auto& group = status.groups_.front();
  group.automatic_failover_state_ = ClusterAutomaticFailoverState::kBlocked;
  group.current_reason_.reset();
  group.suspect_elapsed_ms_ = 375;
  group.effective_threshold_ms_ = 1'000;
  group.blocked_reason_ = "leadership_warmup";
  lavik::meta::ClusterStatusOutcome outcome{
      .result_ = ClusterStatusResult::kReady, .status_ = status};

  auto json = RenderClusterStatusJson(outcome);
  ASSERT_TRUE(json.ok()) << json.status();
  EXPECT_NE(json->find("\"automatic_failover_state\":\"blocked\""),
            std::string::npos);
  EXPECT_NE(json->find("\"current_reason\":null"), std::string::npos);
  EXPECT_NE(json->find("\"suspect_elapsed_ms\":\"375\""), std::string::npos);
  EXPECT_NE(json->find("\"effective_threshold_ms\":\"1000\""),
            std::string::npos);
  EXPECT_NE(json->find("\"blocked_reason\":\"leadership_warmup\""),
            std::string::npos);
  EXPECT_EQ(json->find("remaining_ms"), std::string::npos);
  EXPECT_EQ(json->find("\"triggering\""), std::string::npos);
  EXPECT_EQ(json->find("policy"), std::string::npos);

  auto text = RenderClusterStatusText(outcome);
  ASSERT_TRUE(text.ok()) << text.status();
  EXPECT_NE(text->find("automatic_failover group=group-1 state=blocked"),
            std::string::npos);
  EXPECT_NE(text->find("current_reason=- suspect_elapsed_ms=375 "
                       "effective_threshold_ms=1000 "
                       "blocked_reason=leadership_warmup"),
            std::string::npos);
  EXPECT_EQ(text->find("policy"), std::string::npos);
}

TEST(MetaClusterStatusRenderTest, RejectsInconsistentLifecycleInput) {
  ClusterStatusWireV1 status;
  status.cluster_state_ = ClusterStateWireV1::kCreated;
  status.lifecycle_revision_ = 2;
  status.root_operation_id_ = "00112233445566778899aabbccddeeff";
  lavik::meta::ClusterStatusOutcome outcome{
      .result_ = ClusterStatusResult::kNotReady, .status_ = status};

  EXPECT_FALSE(RenderClusterStatusJson(outcome).ok());
  EXPECT_FALSE(RenderClusterStatusText(outcome).ok());
}

}  // namespace
