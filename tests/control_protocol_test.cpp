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

#include "lavik/cluster/control_protocol.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"

namespace {

namespace control = lavik::cluster::control;

using control::FrameDecoder;
using control::FrameEncoder;
using control::LargeObjectReassembler;
using control::LargeObjectSink;
using control::MessageType;
using control::TransferChunk;
using control::TransferEnd;
using control::TransferKind;
using control::TransferStart;
using control::WireHash256;
using control::WireId128;

WireId128 Id(std::uint8_t last) {
  WireId128 id{};
  id.back() = last;
  return id;
}

void SetLeaseTiming(control::FullDesiredState* state) {
  state->control_revision = 1;
  state->authority_lease_duration_ms = 3'000;
}

control::FullDesiredState FailoverFullState() {
  control::FullDesiredState state;
  state.control_revision = 91;
  state.topology_epoch = 23;
  state.authority_lease_duration_ms = 3'000;
  state.nodes = {
      {.node_id = std::string(40, 'a'), .host = "127.0.0.1", .port = 6379},
      {.node_id = std::string(40, 'b'), .host = "127.0.0.2", .port = 6380},
  };

  control::WireDesiredGroup group;
  group.group_id = "group-a";
  group.members = {
      {.node_id = state.nodes[0].node_id, .assignment_id = Id(5)},
      {.node_id = state.nodes[1].node_id, .assignment_id = Id(6)},
  };
  group.owner_node_id = state.nodes[0].node_id;
  group.owner_assignment_id = Id(5);
  group.group_term = 7;
  group.grant_active = true;
  group.activation_action_id = Id(19);
  group.steady_replication_enabled = true;
  group.failover_transition = control::WireFailoverTransition{
      .transition_id = Id(20),
      .revision = 25,
      .mode = control::WireFailoverMode::kControlled,
      .target_term = 8,
      .candidate_action =
          control::WireFailoverCandidateAction{
              .action_id = Id(21),
              .candidate = {.node_id = state.nodes[1].node_id,
                            .assignment_id = Id(6),
                            .boot_id = std::string(40, 'c')},
              .domain = {.source_group_term = 7,
                         .source_node_id = state.nodes[0].node_id,
                         .source_assignment_id = Id(5),
                         .source_boot_id = std::string(40, 'd'),
                         .source_history_id = std::string(40, 'e'),
                         .flow_count = 3},
              .authorization =
                  control::WireFailoverAuthorization{
                      .authorized_revision = 24,
                      .loss_if_cutover = control::WireFailoverLoss::kNone}},
  };
  state.groups.push_back(std::move(group));
  return state;
}

void AppendBe32(std::string* bytes, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    bytes->push_back(static_cast<char>(value >> shift));
  }
}

class RecordingSink final : public LargeObjectSink {
 public:
  absl::Status Begin(const TransferStart& start) override {
    ++begin_count_;
    start_ = start;
    bytes_.clear();
    return absl::OkStatus();
  }

  absl::Status Write(std::uint64_t offset, std::string_view bytes) override {
    if (offset != bytes_.size()) {
      return absl::InvalidArgumentError("non-contiguous sink write");
    }
    bytes_.append(bytes);
    return absl::OkStatus();
  }

  absl::Status Commit() override {
    ++commit_count_;
    return absl::OkStatus();
  }

  void Abort() noexcept override {
    ++abort_count_;
    bytes_.clear();
  }

  int begin_count_ = 0;
  int commit_count_ = 0;
  int abort_count_ = 0;
  TransferStart start_;
  std::string bytes_;
};

TEST(ControlProtocolFrameTest, EncodesNetworkOrderAndChecksCrcAndSequence) {
  FrameEncoder encoder;
  FrameDecoder decoder;

  auto encoded = encoder.Encode(MessageType::kClientHello, "abc");
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  ASSERT_EQ(encoded->size(), control::kFrameHeaderBytes + 3U);

  // Header literals independently pin the v1 network-byte-order layout.
  const std::array<unsigned char, 24> expected_prefix = {
      0x4c, 0x56, 0x43, 0x50,                           // LVCP
      0x00, 0x01,                                       // protocol version
      0x00, 0x01,                                       // ClientHello
      0x00, 0x00,                                       // flags
      0x00, 0x00,                                       // reserved
      0x00, 0x00, 0x00, 0x03,                           // payload length
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};  // sequence
  EXPECT_EQ(std::memcmp(encoded->data(), expected_prefix.data(),
                        expected_prefix.size()),
            0);

  auto decoded = decoder.Decode(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->sequence, 1U);
  EXPECT_EQ(decoded->type, MessageType::kClientHello);
  EXPECT_EQ(decoded->payload, "abc");

  std::string corrupt = *encoded;
  corrupt.back() ^= 0x01;
  FrameDecoder fresh_decoder;
  EXPECT_EQ(fresh_decoder.Decode(corrupt).status().code(),
            absl::StatusCode::kDataLoss);
  // A corrupt frame must not consume the expected sequence number.
  EXPECT_TRUE(fresh_decoder.Decode(*encoded).ok());

  EXPECT_EQ(decoder.Decode(*encoded).status().code(),
            absl::StatusCode::kFailedPrecondition);
  auto third = encoder.Encode(MessageType::kClientHello, "next");
  ASSERT_TRUE(third.ok());
  EXPECT_TRUE(decoder.Decode(*third).ok());
}

TEST(ControlProtocolFrameTest, RejectsFramesAboveTheTotalFrameCap) {
  FrameEncoder encoder;
  std::string maximum(control::kMaxFrameBytes - control::kFrameHeaderBytes,
                      'x');
  EXPECT_TRUE(encoder.Encode(MessageType::kDirective, maximum).ok());
  maximum.push_back('x');
  EXPECT_EQ(encoder.Encode(MessageType::kDirective, maximum).status().code(),
            absl::StatusCode::kResourceExhausted);
  // Failed local encoding does not burn the per-direction sequence.
  EXPECT_EQ(encoder.next_sequence(), 2U);
}

TEST(ControlProtocolCodecTest, RoundTripsHeartbeatChallengeAndGrant) {
  control::Heartbeat heartbeat;
  heartbeat.session_id = Id(1);
  heartbeat.heartbeat_sequence = 9;
  heartbeat.health.storage_ready = true;
  heartbeat.health.population_ready = true;
  heartbeat.health.draining = false;
  heartbeat.health.active_groups = 3;
  const control::LeaseChallenge challenge{
      .nonce = Id(2),
      .control_revision = 42,
      .group_id = "group-a",
      .assignment_id = Id(3),
      .group_term = 7,
  };
  heartbeat.role_information =
      control::AuthorityLeaseRequest{.challenge = challenge};

  control::WireMessage message = heartbeat;
  auto encoded = control::EncodeMessage(message);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeMessage(MessageType::kHeartbeat, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_TRUE(std::holds_alternative<control::Heartbeat>(*decoded));
  EXPECT_EQ(std::get<control::Heartbeat>(*decoded), heartbeat);

  control::HeartbeatAck ack;
  ack.session_id = heartbeat.session_id;
  ack.heartbeat_sequence = heartbeat.heartbeat_sequence;
  ack.observation_status = control::ObservationStatus::kAccepted;
  ack.lease_decision = control::LeaseGranted{
      .nonce = challenge.nonce,
      .leader_id = 4,
      .raft_term = 22,
      .leadership_generation = 5,
      .data_boot_id = std::string(40, 'a'),
      .control_revision = challenge.control_revision,
      .group_id = challenge.group_id,
      .assignment_id = challenge.assignment_id,
      .group_term = challenge.group_term,
      .granted_duration_ms = 3'000,
  };
  encoded = control::EncodeMessage(control::WireMessage{ack});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  decoded = control::DecodeMessage(MessageType::kHeartbeatAck, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::HeartbeatAck>(*decoded), ack);

  ack.lease_decision = control::LeaseDenied{
      .nonce = challenge.nonce,
      .reason = control::LeaseDenialReason::kAuthorityHandoffPending,
      .current_control_revision = challenge.control_revision,
  };
  encoded = control::EncodeMessage(control::WireMessage{ack});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  decoded = control::DecodeMessage(MessageType::kHeartbeatAck, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::HeartbeatAck>(*decoded), ack);
}

TEST(ControlProtocolCodecTest,
     RoundTripsServerHelloWithoutIndependentHeartbeatCadence) {
  control::ServerHello hello{
      .disposition = control::ServerHelloDisposition::kAccepted,
      .negotiated_version = control::kProtocolVersion,
      .meta_server_id = 4,
      .raft_term = 22,
      .session_id = Id(1),
      .session_generation = 5,
      .leader_id = 4,
      .directory = {{.server_id = 4, .host = "127.0.0.1", .port = 7400}},
      .observation_ttl_ms = 600,
      .session_progress_timeout_ms = 1'000,
  };

  auto encoded = control::EncodeMessage(control::WireMessage{hello});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded =
      control::DecodeMessage(MessageType::kServerHello, std::move(*encoded));
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_TRUE(std::holds_alternative<control::ServerHello>(*decoded));
  EXPECT_EQ(std::get<control::ServerHello>(*decoded), hello);
}

TEST(ControlProtocolCodecTest, RoundTripsTypedReplicaCandidate) {
  control::Heartbeat heartbeat;
  heartbeat.session_id = Id(1);
  heartbeat.heartbeat_sequence = 10;
  heartbeat.health.storage_ready = true;
  heartbeat.health.population_ready = true;
  heartbeat.role_information = control::ReplicaCandidate{
      .progress =
          {
              .group_id = "group-a",
              .assignment_id = Id(4),
              .group_term = 7,
              .source_group_term = 6,
              .manifest_revision = 12,
              .manifest_digest = WireHash256{1},
              .partition_replication_epoch = 13,
              .source_node_id = std::string(40, 'a'),
              .source_assignment_id = Id(5),
              .source_boot_id = std::string(40, 'b'),
              .source_history_id = std::string(40, 'c'),
              .applied_next_lsns = {10, 20, 30},
          },
  };

  auto encoded = control::EncodeMessage(control::WireMessage{heartbeat});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeMessage(MessageType::kHeartbeat, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::Heartbeat>(*decoded), heartbeat);
}

TEST(ControlProtocolCodecTest, RejectsMalformedTypedCandidateAndRoleTag) {
  control::Heartbeat heartbeat;
  heartbeat.heartbeat_sequence = 1;
  heartbeat.role_information = control::ReplicaCandidate{
      .progress =
          {
              .group_id = "group-a",
              .assignment_id = Id(4),
              .group_term = 7,
              .source_group_term = 6,
              .manifest_revision = 12,
              .manifest_digest = WireHash256{1},
              .partition_replication_epoch = 13,
              .source_node_id = std::string(40, 'a'),
              .source_assignment_id = Id(5),
              .source_boot_id = std::string(40, 'b'),
              .source_history_id = std::string(40, 'c'),
              .applied_next_lsns = {10},
          },
  };
  auto encoded = control::EncodeMessage(control::WireMessage{heartbeat});
  ASSERT_TRUE(encoded.ok()) << encoded.status();

  std::string zero_cursor = *encoded;
  std::fill(zero_cursor.end() - 8, zero_cursor.end(), '\0');
  EXPECT_EQ(control::DecodeMessage(MessageType::kHeartbeat, zero_cursor)
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);

  std::string trailing = *encoded;
  trailing.push_back('\0');
  EXPECT_EQ(
      control::DecodeMessage(MessageType::kHeartbeat, trailing).status().code(),
      absl::StatusCode::kInvalidArgument);

  control::Heartbeat no_role;
  no_role.heartbeat_sequence = 2;
  encoded = control::EncodeMessage(control::WireMessage{no_role});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  ASSERT_GE(encoded->size(), 2u);
  (*encoded)[encoded->size() - 2] = static_cast<char>(99);
  EXPECT_EQ(
      control::DecodeMessage(MessageType::kHeartbeat, *encoded).status().code(),
      absl::StatusCode::kInvalidArgument);

  auto* candidate =
      std::get_if<control::ReplicaCandidate>(&heartbeat.role_information);
  ASSERT_NE(candidate, nullptr);
  candidate->progress.applied_next_lsns.clear();
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kResourceExhausted);
  candidate->progress.applied_next_lsns.assign(control::kMaxCandidateFlows + 1,
                                               1);
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kResourceExhausted);

  candidate->progress.applied_next_lsns = {1};
  candidate->progress.source_group_term = 0;
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kInvalidArgument);
  candidate->progress.source_group_term = 8;
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kInvalidArgument);
}

TEST(ControlProtocolCodecTest, MaximumCandidateProgressStillFitsOneFrame) {
  control::Heartbeat heartbeat;
  heartbeat.session_id = Id(1);
  heartbeat.heartbeat_sequence = 1;
  heartbeat.role_information = control::ReplicaCandidate{
      .progress =
          {
              .group_id = "group-a",
              .assignment_id = Id(2),
              .group_term = 7,
              .source_group_term = 6,
              .manifest_revision = 8,
              .manifest_digest = WireHash256{1},
              .partition_replication_epoch = 9,
              .source_node_id = std::string(40, 'a'),
              .source_assignment_id = Id(3),
              .source_boot_id = std::string(40, 'b'),
              .source_history_id = std::string(40, 'c'),
              .applied_next_lsns =
                  std::vector<std::uint64_t>(control::kMaxCandidateFlows, 1),
          },
  };

  const auto payload = control::EncodeMessage(control::WireMessage{heartbeat});
  ASSERT_TRUE(payload.ok()) << payload.status();
  EXPECT_LE(payload->size(), control::kMaxFramePayloadBytes);
  control::FrameEncoder encoder;
  EXPECT_TRUE(encoder.Encode(control::MessageType::kHeartbeat, *payload).ok());
}

TEST(ControlProtocolCodecTest,
     RoundTripsTypedFailoverObservationsBesideSteadyRole) {
  control::Heartbeat heartbeat;
  heartbeat.session_id = Id(1);
  heartbeat.heartbeat_sequence = 1;
  heartbeat.role_information =
      control::AuthorityLeaseRequest{.challenge = {.nonce = Id(2),
                                                   .control_revision = 42,
                                                   .group_id = "group-a",
                                                   .assignment_id = Id(3),
                                                   .group_term = 7}};
  heartbeat.failover_observation =
      control::SourcePaused{.transition_id = Id(4),
                            .source_node_id = std::string(40, 'a'),
                            .source_assignment_id = Id(3),
                            .source_boot_id = std::string(40, 'b'),
                            .source_history_id = std::string(40, 'c'),
                            .source_group_term = 7,
                            .stable_next_lsns = {11, 22, 33}};

  for (int kind = 0; kind < 3; ++kind) {
    if (kind == 1) {
      heartbeat.failover_observation =
          control::CandidatePrepared{.transition_id = Id(4),
                                     .action_id = Id(5),
                                     .candidate_node_id = std::string(40, 'd'),
                                     .candidate_assignment_id = Id(6),
                                     .candidate_boot_id = std::string(40, 'e'),
                                     .prepared_context_id = Id(7)};
    } else if (kind == 2) {
      heartbeat.failover_observation =
          control::ActionFailed{.transition_id = Id(4),
                                .action_id = Id(5),
                                .candidate_node_id = std::string(40, 'd'),
                                .candidate_assignment_id = Id(6),
                                .candidate_boot_id = std::string(40, 'e'),
                                .population_manifest_revision = 10,
                                .population_manifest_digest = WireHash256{1},
                                .partition_replication_epoch = 11,
                                .failure_class = "durability",
                                .failure_detail = "storage barrier failed"};
    }
    const auto encoded =
        control::EncodeMessage(control::WireMessage{heartbeat});
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    const auto decoded =
        control::DecodeMessage(control::MessageType::kHeartbeat, *encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(std::get<control::Heartbeat>(*decoded), heartbeat);
  }
}

TEST(ControlProtocolCodecTest, RecoveryCompletionRoundTripsActualFrontier) {
  control::Heartbeat heartbeat;
  heartbeat.session_id = Id(1);
  heartbeat.heartbeat_sequence = 1;
  control::CandidateRecoveryComplete complete{
      .transition_id = Id(4),
      .action_id = Id(5),
      .candidate_node_id = std::string(40, 'd'),
      .candidate_assignment_id = Id(6),
      .candidate_boot_id = std::string(40, 'e'),
      .recovery_deadline_unix_ms = 3000,
      .applied_next_lsns = {12, 9},
      .completion_reason = "coverage-unavailable",
  };
  heartbeat.failover_observation = complete;
  auto encoded = control::EncodeMessage(control::WireMessage{heartbeat});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded =
      control::DecodeMessage(control::MessageType::kHeartbeat, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::Heartbeat>(*decoded), heartbeat);
  complete.applied_next_lsns = {12, 0};
  heartbeat.failover_observation = complete;
  EXPECT_FALSE(control::EncodeMessage(control::WireMessage{heartbeat}).ok());
  complete.applied_next_lsns = {12, 9};
  complete.recovery_deadline_unix_ms = 0;
  heartbeat.failover_observation = complete;
  EXPECT_FALSE(control::EncodeMessage(control::WireMessage{heartbeat}).ok());
}

TEST(ControlProtocolCodecTest,
     FailoverObservationValidationAndSingleFrameBudgetAreStrict) {
  control::Heartbeat heartbeat;
  heartbeat.session_id = Id(1);
  heartbeat.heartbeat_sequence = 1;
  heartbeat.failover_observation =
      control::SourcePaused{.transition_id = Id(2),
                            .source_node_id = std::string(40, 'a'),
                            .source_assignment_id = Id(3),
                            .source_boot_id = std::string(40, 'b'),
                            .source_history_id = std::string(40, 'c'),
                            .source_group_term = 7,
                            .stable_next_lsns = std::vector<std::uint64_t>(
                                control::kMaxCandidateFlows, 1)};
  auto encoded = control::EncodeMessage(control::WireMessage{heartbeat});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_LE(encoded->size(), control::kMaxFramePayloadBytes);
  constexpr std::size_t kObservationTagOffset = 16 + 8 + 3 + 4 + 4 + 1 + 1;
  ASSERT_GT(encoded->size(), kObservationTagOffset);
  std::string unknown_kind = *encoded;
  unknown_kind[kObservationTagOffset] = static_cast<char>(99);
  EXPECT_EQ(
      control::DecodeMessage(control::MessageType::kHeartbeat, unknown_kind)
          .status()
          .code(),
      absl::StatusCode::kInvalidArgument);

  auto* paused =
      std::get_if<control::SourcePaused>(&*heartbeat.failover_observation);
  ASSERT_NE(paused, nullptr);
  paused->stable_next_lsns.back() = 0;
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kInvalidArgument);
  paused->stable_next_lsns.back() = 1;

  heartbeat.role_information = control::ReplicaCandidate{
      .progress = {.group_id = "group-a",
                   .assignment_id = Id(4),
                   .group_term = 8,
                   .source_group_term = 7,
                   .manifest_revision = 10,
                   .manifest_digest = WireHash256{1},
                   .partition_replication_epoch = 11,
                   .source_node_id = std::string(40, 'a'),
                   .source_assignment_id = Id(3),
                   .source_boot_id = std::string(40, 'b'),
                   .source_history_id = std::string(40, 'c'),
                   .applied_next_lsns = std::vector<std::uint64_t>(
                       control::kMaxCandidateFlows, 1)}};
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kResourceExhausted);

  heartbeat.failover_observation = control::ActionFailed{
      .transition_id = Id(2),
      .action_id = Id(5),
      .candidate_node_id = std::string(40, 'd'),
      .candidate_assignment_id = Id(4),
      .candidate_boot_id = std::string(40, 'e'),
      .population_manifest_revision = 10,
      .population_manifest_digest = WireHash256{1},
      .partition_replication_epoch = 11,
      .failure_class = std::string(control::kMaxFailoverFailureClassBytes, 'f'),
      .failure_detail =
          std::string(control::kMaxFailoverFailureDetailBytes, 'd')};
  encoded = control::EncodeMessage(control::WireMessage{heartbeat});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_LE(encoded->size(), control::kMaxFramePayloadBytes);

  auto* failed =
      std::get_if<control::ActionFailed>(&*heartbeat.failover_observation);
  ASSERT_NE(failed, nullptr);
  failed->action_id = {};
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kInvalidArgument);
  failed->action_id = Id(5);
  failed->failure_detail.push_back('x');
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{heartbeat}).status().code(),
      absl::StatusCode::kResourceExhausted);
}

TEST(ControlProtocolCodecTest,
     HeartbeatSequenceRejectsDuplicatesGapsAndRollback) {
  control::HeartbeatSequenceWindow window;
  EXPECT_EQ(window.Observe(0).code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(window.Observe(2).code(), absl::StatusCode::kFailedPrecondition);
  ASSERT_TRUE(window.Observe(1).ok());
  EXPECT_EQ(window.Observe(1).code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(window.Observe(3).code(), absl::StatusCode::kFailedPrecondition);
  ASSERT_TRUE(window.Observe(2).ok());
  EXPECT_EQ(window.Observe(1).code(), absl::StatusCode::kFailedPrecondition);
  window.Reset();
  ASSERT_TRUE(window.Observe(1).ok());
}

TEST(ControlProtocolCodecTest,
     DirectiveLifecycleKeepsRecipientDistinctFromRebuildTarget) {
  control::Directive directive{
      .session_id = Id(1),
      .basis = {.control_revision = 19},
      .authority = {.group_id = "group-a",
                    .assignment_id = Id(2),
                    .group_term = 3},
      .identity = {.operation_id = Id(6),
                   .directive_id = Id(7),
                   .attempt_id = Id(8),
                   .directive_revision = 20},
      .recipient_node_id = std::string(40, 'a'),
      .recipient_boot_id = std::string(40, 'b'),
      .target_node_id = std::string(40, 'c'),
      .target_boot_id = std::string(40, 'd'),
      .source_node_id = std::string(40, 'a'),
      .source_assignment_id = Id(9),
      .source_boot_id = std::string(40, 'b'),
      .source_replication_history_id = std::string(40, 'e'),
      .manifest_revision = 21,
      .manifest_digest = WireHash256{1},
      .partition_replication_epoch = 22,
      .kind = control::WireDirectiveKind::kAuthorizeSource,
      .payload = "authorize",

  };
  auto encoded = control::EncodeMessage(control::WireMessage{directive});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeMessage(MessageType::kDirective, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::Directive>(*decoded), directive);

  // Kind 5 is outside the active enum range and must not become accidentally
  // admissible.
  directive.kind = static_cast<control::WireDirectiveKind>(5);
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{directive}).status().code(),
      absl::StatusCode::kInvalidArgument);
  directive.kind = control::WireDirectiveKind::kAuthorizeSource;

  control::DirectiveResponse receipt{
      .session_id = directive.session_id,
      .recipient_boot_id = directive.recipient_boot_id,
      .identity = directive.identity,
      .started = true,
  };
  encoded = control::EncodeMessage(control::WireMessage{receipt});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  decoded = control::DecodeMessage(MessageType::kDirectiveResponse, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::DirectiveResponse>(*decoded), receipt);

  control::DirectiveResult result{
      .session_id = directive.session_id,
      .recipient_boot_id = directive.recipient_boot_id,
      .assignment_id = directive.authority.assignment_id,
      .identity = directive.identity,
      .status = control::DirectiveResultStatus::kSucceeded,
      .result = "ok",
  };
  encoded = control::EncodeMessage(control::WireMessage{result});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  decoded = control::DecodeMessage(MessageType::kDirectiveResult, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::DirectiveResult>(*decoded), result);

  control::ResultCommitted committed{
      .session_id = result.session_id,
      .recipient_boot_id = result.recipient_boot_id,
      .identity = result.identity,
      .committed_index = 22,
  };
  encoded = control::EncodeMessage(control::WireMessage{committed});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  decoded = control::DecodeMessage(MessageType::kResultCommitted, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::ResultCommitted>(*decoded), committed);

  control::ResultNoLongerTracked forgotten{
      .session_id = result.session_id,
      .recipient_boot_id = result.recipient_boot_id,
      .identity = result.identity,
  };
  encoded = control::EncodeMessage(control::WireMessage{forgotten});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  decoded =
      control::DecodeMessage(MessageType::kResultNoLongerTracked, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::ResultNoLongerTracked>(*decoded), forgotten);
}

TEST(ControlProtocolCodecTest, RoundTripsSourceLessPopulationInitialization) {
  control::Directive directive{
      .session_id = Id(1),
      .basis = {.control_revision = 19},
      .authority = {.group_id = "group-a",
                    .assignment_id = Id(2),
                    .group_term = 3},
      .identity = {.operation_id = Id(6),
                   .directive_id = Id(7),
                   .attempt_id = Id(8),
                   .directive_revision = 20},
      .recipient_node_id = std::string(40, 'a'),
      .recipient_boot_id = std::string(40, 'b'),
      .target_node_id = std::string(40, 'a'),
      .target_boot_id = std::string(40, 'b'),
      .source_node_id = std::string(40, '0'),
      .source_boot_id = std::string(40, '0'),
      .source_replication_history_id = std::string(40, '0'),
      .manifest_revision = 1,
      .manifest_digest = WireHash256{1},
      .partition_replication_epoch = 1,
      .kind = control::WireDirectiveKind::kInitializeEmptyPopulation,
      // The existing payload binds the history advertised by the target's
      // current Hello. Zero wire identities denote the absent source and can
      // never authorize a replication connection.
      .payload = std::string(40, 'c'),

  };

  auto encoded = control::EncodeMessage(control::WireMessage{directive});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeMessage(MessageType::kDirective, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::Directive>(*decoded), directive);
}

TEST(ControlProtocolCodecTest, HelloRequiresBoundedSourceFlowCount) {
  control::ClientHello hello{
      .node_id = std::string(40, 'a'),
      .boot_id = std::string(40, 'b'),
      .replication_history_id = std::string(40, 'c'),
      .replication_flow_count = 3,
  };
  auto encoded = control::EncodeMessage(control::WireMessage{hello});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeMessage(MessageType::kClientHello, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::ClientHello>(*decoded), hello);
  for (std::uint32_t count : {0U, 1025U}) {
    hello.replication_flow_count = count;
    EXPECT_FALSE(control::EncodeMessage(control::WireMessage{hello}).ok());
    std::string malformed = encoded->substr(0, encoded->size() - 4);
    AppendBe32(&malformed, count);
    EXPECT_FALSE(
        control::DecodeMessage(MessageType::kClientHello, malformed).ok());
  }
  EXPECT_FALSE(control::DecodeMessage(MessageType::kClientHello,
                                      encoded->substr(0, encoded->size() - 4))
                   .ok());
  EXPECT_FALSE(
      control::DecodeMessage(MessageType::kClientHello, *encoded + "x").ok());
}

TEST(ControlProtocolCodecTest,
     RebuildPayloadRequiresExactVersionedSourceLayout) {
  auto encoded = control::EncodeRebuildRequest({3});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeRebuildRequest(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->source_flow_count, 3);
  EXPECT_EQ(*encoded, std::string("LVRR\0\1\0\0\0\3", 10));
  for (std::uint32_t count : {0U, 1025U}) {
    EXPECT_FALSE(control::EncodeRebuildRequest({count}).ok());
    std::string malformed = encoded->substr(0, 6);
    AppendBe32(&malformed, count);
    EXPECT_FALSE(control::DecodeRebuildRequest(malformed).ok());
  }
  for (std::size_t size = 0; size < encoded->size(); ++size)
    EXPECT_FALSE(control::DecodeRebuildRequest(encoded->substr(0, size)).ok());
  EXPECT_FALSE(control::DecodeRebuildRequest(*encoded + "x").ok());
  (*encoded)[5] = 2;
  EXPECT_FALSE(control::DecodeRebuildRequest(*encoded).ok());
  (*encoded)[5] = 1;
  (*encoded)[0] = 'X';
  EXPECT_FALSE(control::DecodeRebuildRequest(*encoded).ok());
}

TEST(ControlProtocolLeaseTest, GrantMatchesOnceAndUsesOriginalSendTime) {
  control::LeaseChallengeTracker tracker;
  control::LeaseChallenge challenge{
      .nonce = Id(2),
      .control_revision = 42,
      .group_id = "group-a",
      .assignment_id = Id(3),
      .group_term = 7,
  };
  const WireId128 session_id = Id(1);
  constexpr std::int64_t kSentAtMs = 10'000;
  ASSERT_TRUE(tracker.Begin(session_id, std::string(40, 'a'), challenge).ok());
  ASSERT_TRUE(tracker.MarkWritten(challenge.nonce, kSentAtMs).ok());
  // A retransmission cannot move sent_at forward.
  EXPECT_EQ(tracker.MarkWritten(challenge.nonce, kSentAtMs + 500).code(),
            absl::StatusCode::kFailedPrecondition);

  control::LeaseGranted grant{
      .nonce = challenge.nonce,
      .leader_id = 4,
      .raft_term = 22,
      .leadership_generation = 5,
      .data_boot_id = std::string(40, 'a'),
      .control_revision = challenge.control_revision,
      .group_id = challenge.group_id,
      .assignment_id = challenge.assignment_id,
      .group_term = challenge.group_term,
      .granted_duration_ms = 3'000,
  };
  auto deadline = tracker.AcceptGrant(session_id, grant, kSentAtMs + 2'999);
  ASSERT_TRUE(deadline.ok()) << deadline.status();
  EXPECT_EQ(*deadline, 13'000);
  EXPECT_EQ(
      tracker.AcceptGrant(session_id, grant, kSentAtMs + 2'999).status().code(),
      absl::StatusCode::kFailedPrecondition);

  EXPECT_EQ(tracker.Begin(session_id, std::string(40, 'a'), challenge).code(),
            absl::StatusCode::kFailedPrecondition);
  challenge.nonce = Id(4);
  grant.nonce = challenge.nonce;
  ASSERT_TRUE(tracker.Begin(session_id, std::string(40, 'a'), challenge).ok());
  ASSERT_TRUE(tracker.MarkWritten(challenge.nonce, kSentAtMs).ok());
  EXPECT_EQ(tracker.AcceptGrant(session_id, grant, 13'000).status().code(),
            absl::StatusCode::kDeadlineExceeded);
}

TEST(ControlProtocolFullStateTest, RoundTripsTypedProjectionWithoutObjectHash) {
  control::FullDesiredState state;
  state.control_revision = 91;
  state.topology_epoch = 23;
  state.authority_lease_duration_ms = 3'000;
  state.meta_directory.push_back(
      {.server_id = 1, .host = "127.0.0.1", .port = 7400});
  state.nodes.push_back({.node_id = std::string(40, 'a'),
                         .host = "127.0.0.2",
                         .port = 6379,
                         .tls_port = 6380});
  control::WireDesiredGroup group;
  group.group_id = "group-a";
  group.owner_node_id = state.nodes[0].node_id;
  group.owner_assignment_id = Id(5);
  group.group_term = 7;
  group.grant_active = true;
  group.members.push_back(
      {.node_id = state.nodes[0].node_id, .assignment_id = Id(5)});
  group.slot_ranges.push_back({.first = 0, .last = 100});
  group.manifest_revision = 15;
  group.manifest_digest = WireHash256{1};
  group.partition_replication_epoch = 19;
  state.groups.push_back(std::move(group));
  state.manifests.push_back(
      {.revision = 15,
       .digest = WireHash256{1},
       .entries = {{.partition_id = 0, .logical_epoch = 17},
                   {.partition_id = 1, .logical_epoch = 18}}});
  state.current_directives.push_back(
      {.authority = {.group_id = "group-a",
                     .assignment_id = Id(5),
                     .group_term = 7},
       .identity = {.operation_id = Id(6),
                    .directive_id = Id(8),
                    .attempt_id = Id(7),
                    .directive_revision = 92},
       .recipient_node_id = std::string(40, 'a'),
       .recipient_boot_id = std::string(40, 'b'),
       .target_node_id = std::string(40, 'a'),
       .target_boot_id = std::string(40, 'b'),
       .source_node_id = std::string(40, 'c'),
       .source_assignment_id = Id(9),
       .source_boot_id = std::string(40, 'd'),
       .source_replication_history_id = std::string(40, 'e'),
       .manifest_revision = 15,
       .manifest_digest = WireHash256{1},
       .partition_replication_epoch = 19,
       .kind = control::WireDirectiveKind::kRebuild,
       .payload = "rebuild"});

  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeFullDesiredState(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, state);

  state.current_directives[0].payload = "different rebuild";
  EXPECT_FALSE(control::SameDesiredState(state, *decoded));
  EXPECT_TRUE(control::EncodeFullDesiredState(state).ok());
  state.current_directives[0].payload = "rebuild";
  ++state.control_revision;
  EXPECT_TRUE(control::SameDesiredState(state, *decoded));
  EXPECT_TRUE(control::EncodeFullDesiredState(state).ok());
}

TEST(ControlProtocolFullStateTest,
     RoundTripsTypedFailoverStateAndComparesItsSemantics) {
  control::FullDesiredState state = FailoverFullState();
  const auto original = state;

  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_LE(encoded->size(), control::kMaxFramePayloadBytes);
  auto decoded = control::DecodeFullDesiredState(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, state);

  state.groups[0].steady_replication_enabled = false;
  EXPECT_FALSE(control::SameDesiredState(state, original));
  state.groups[0].steady_replication_enabled = true;

  ASSERT_TRUE(state.groups[0].failover_transition.has_value());
  ++state.groups[0].failover_transition->revision;
  EXPECT_FALSE(control::SameDesiredState(state, original));

  state.groups[0].failover_transition.reset();
  state.groups[0].activation_action_id = Id(21);
  EXPECT_FALSE(control::SameDesiredState(state, original));
}

TEST(ControlProtocolFullStateTest, RejectsZeroLeaseAndDerivesHeartbeatCadence) {
  control::FullDesiredState state = FailoverFullState();

  state.authority_lease_duration_ms = 0;
  EXPECT_EQ(control::EncodeFullDesiredState(state).status().code(),
            absl::StatusCode::kInvalidArgument);

  state.authority_lease_duration_ms = 2;
  EXPECT_TRUE(control::EncodeFullDesiredState(state).ok());
  EXPECT_EQ(control::DataHeartbeatIntervalMs(2), 1u);
  EXPECT_EQ(control::DataHeartbeatIntervalMs(3'000), 1'000u);
}

TEST(ControlProtocolFullStateTest, RejectsMalformedFailoverState) {
  control::FullDesiredState state = FailoverFullState();
  ASSERT_TRUE(state.groups[0].failover_transition.has_value());
  auto& transition = *state.groups[0].failover_transition;

  transition.mode = static_cast<control::WireFailoverMode>(99);
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);
  transition.mode = control::WireFailoverMode::kControlled;

  ASSERT_TRUE(transition.candidate_action.has_value());
  transition.candidate_action->domain.source_group_term =
      transition.target_term;
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);
  transition.candidate_action->domain.source_group_term = 7;

  transition.candidate_action->authorization->loss_if_cutover =
      control::WireFailoverLoss::kUnknown;
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);
  transition.candidate_action->authorization->loss_if_cutover =
      control::WireFailoverLoss::kNone;

  transition.candidate_action.reset();
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);

  state = FailoverFullState();
  state.groups[0].failover_transition.reset();
  state.groups[0].activation_action_id = WireId128{};
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);

  state.groups[0].activation_action_id = Id(1);
  state.groups[0].grant_active = false;
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ControlProtocolFullStateTest, RoundTripsEmptyTopologyAtEpochZero) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.control_revision = 1;
  state.topology_epoch = 0;

  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeFullDesiredState(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->topology_epoch, 0U);
  EXPECT_TRUE(decoded->nodes.empty());
  EXPECT_TRUE(decoded->groups.empty());
}

TEST(ControlProtocolFullStateTest,
     ConsumingDecodeReleasesTransferredWireStorage) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.control_revision = 1;
  state.meta_directory.reserve(512);
  for (std::uint32_t id = 1; id <= 512; ++id) {
    state.meta_directory.push_back(
        {.server_id = id, .host = "127.0.0.1", .port = 7400});
  }

  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  std::string transfer = std::move(*encoded);
  const std::size_t empty_capacity = std::string{}.capacity();
  ASSERT_GT(transfer.capacity(), empty_capacity);
  auto decoded = control::DecodeFullDesiredState(std::move(transfer));
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_TRUE(transfer.empty());
  EXPECT_EQ(transfer.capacity(), empty_capacity);
  EXPECT_EQ(decoded->meta_directory.size(), 512U);

  auto invalid_encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(invalid_encoded.ok()) << invalid_encoded.status();
  invalid_encoded->front() = static_cast<char>(0xff);
  std::string invalid_transfer = std::move(*invalid_encoded);
  ASSERT_GT(invalid_transfer.capacity(), empty_capacity);
  EXPECT_FALSE(
      control::DecodeFullDesiredState(std::move(invalid_transfer)).ok());
  EXPECT_TRUE(invalid_transfer.empty());
  EXPECT_EQ(invalid_transfer.capacity(), empty_capacity);
}

TEST(ControlProtocolFullStateTest,
     FrameSizedProjectionRoundTripsAsTypedMessage) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.control_revision = 1;

  control::WireMessage message = state;
  EXPECT_EQ(control::MessageTypeOf(message),
            control::MessageType::kFullDesiredState);
  auto encoded = control::EncodeMessage(message);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  ASSERT_LE(encoded->size(), control::kMaxFramePayloadBytes);

  control::FrameEncoder frame_encoder;
  auto framed = frame_encoder.Encode(control::MessageTypeOf(message), *encoded);
  ASSERT_TRUE(framed.ok()) << framed.status();
  EXPECT_EQ(framed->size(), control::kFrameHeaderBytes + encoded->size());
  control::FrameDecoder frame_decoder;
  auto frame = frame_decoder.Decode(*framed);
  ASSERT_TRUE(frame.ok()) << frame.status();
  EXPECT_EQ(frame->type, control::MessageType::kFullDesiredState);

  auto decoded = control::DecodeMessage(control::MessageType::kFullDesiredState,
                                        frame->payload);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  const auto* full_state = std::get_if<control::FullDesiredState>(&*decoded);
  ASSERT_NE(full_state, nullptr);
  EXPECT_EQ(*full_state, state);
}

TEST(ControlProtocolFullStateTest,
     SemanticEqualityExcludesAppliedIndexButCoversDesiredState) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.control_revision = 10;
  state.topology_epoch = 1;
  state.nodes.push_back(
      {.node_id = std::string(40, 'a'), .host = "127.0.0.1", .port = 6379});
  state.groups.push_back(
      {.group_id = "group-a", .partition_replication_epoch = 4});
  const auto original = state;

  state.control_revision = 11;  // unrelated Meta commit
  EXPECT_TRUE(control::SameDesiredState(state, original));

  state.groups[0].partition_replication_epoch = 5;
  EXPECT_FALSE(control::SameDesiredState(state, original));
  state.groups[0].partition_replication_epoch = 4;

  state.nodes[0].port = 6380;  // node-specific semantic change
  EXPECT_FALSE(control::SameDesiredState(state, original));

  state.nodes[0].port = 6379;
  state.topology_epoch = 2;
  EXPECT_FALSE(control::SameDesiredState(state, original));

  state.topology_epoch = 1;
  state.authority_lease_duration_ms = 6'000;
  EXPECT_FALSE(control::SameDesiredState(state, original));
}

TEST(ControlProtocolFullStateTest, RoundTripsCommittedGrantlessGroup) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.topology_epoch = 1;
  state.groups.push_back(
      {.group_id = "grantless",
       .group_term = 3,
       .grant_active = false,
       .failover_transition = control::WireFailoverTransition{
           .transition_id = Id(1),
           .revision = 7,
           .mode = control::WireFailoverMode::kUncontrolled,
           .target_term = 3}});

  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeFullDesiredState(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_EQ(decoded->groups.size(), 1U);
  EXPECT_FALSE(decoded->groups[0].owner_node_id.has_value());
  EXPECT_FALSE(decoded->groups[0].owner_assignment_id.has_value());
  EXPECT_FALSE(decoded->groups[0].grant_active);
  ASSERT_TRUE(decoded->groups[0].failover_transition.has_value());
  EXPECT_EQ(decoded->groups[0].failover_transition->mode,
            control::WireFailoverMode::kUncontrolled);
  EXPECT_FALSE(
      decoded->groups[0].failover_transition->candidate_action.has_value());

  state.groups[0].grant_active = true;
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ControlProtocolFullStateTest, RejectsNonCanonicalManifestDocuments) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.topology_epoch = 1;
  state.manifests.push_back(
      {.revision = 1,
       .entries = {{.partition_id = 9, .logical_epoch = 1},
                   {.partition_id = 8, .logical_epoch = 2}}});
  EXPECT_EQ(control::ValidateFullDesiredState(state).code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ControlProtocolTransferTest, StreamsOneBoundedObject) {
  RecordingSink sink;
  LargeObjectReassembler reassembler(sink);
  const std::string contents = "large desired state";
  const TransferStart start{
      .kind = TransferKind::kFullDesiredState,
      .object_id = Id(7),
      .total_length = contents.size(),
  };

  ASSERT_TRUE(reassembler.Accept(start).ok());
  EXPECT_EQ(reassembler.Accept(start).code(),
            absl::StatusCode::kFailedPrecondition);
  ASSERT_TRUE(reassembler
                  .Accept(TransferChunk{.object_id = start.object_id,
                                        .offset = 0,
                                        .bytes = contents.substr(0, 5)})
                  .ok());
  ASSERT_TRUE(reassembler
                  .Accept(TransferChunk{.object_id = start.object_id,
                                        .offset = 5,
                                        .bytes = contents.substr(5)})
                  .ok());
  ASSERT_TRUE(reassembler.Accept(TransferEnd{start.object_id}).ok());
  EXPECT_EQ(sink.bytes_, contents);
  EXPECT_EQ(sink.commit_count_, 1);
  EXPECT_EQ(sink.abort_count_, 0);
  EXPECT_FALSE(reassembler.active());
}

TEST(ControlProtocolTransferTest,
     NamedSupersessionAbortResetsOnlyItsExactActiveObject) {
  RecordingSink sink;
  LargeObjectReassembler reassembler(sink);
  const TransferStart first{
      .kind = TransferKind::kFullDesiredState,
      .object_id = Id(7),
      .total_length = 3,
  };
  ASSERT_TRUE(reassembler.Accept(first).ok());
  ASSERT_TRUE(reassembler
                  .Accept(TransferChunk{
                      .object_id = first.object_id, .offset = 0, .bytes = "o"})
                  .ok());

  EXPECT_EQ(
      reassembler
          .Accept(control::TransferAbort{
              .object_id = Id(8),
              .reason =
                  control::TransferAbortReason::kFullDesiredStateSuperseded})
          .code(),
      absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(reassembler.active());
  ASSERT_TRUE(
      reassembler
          .Accept(control::TransferAbort{
              .object_id = first.object_id,
              .reason =
                  control::TransferAbortReason::kFullDesiredStateSuperseded})
          .ok());
  EXPECT_FALSE(reassembler.active());
  EXPECT_EQ(sink.abort_count_, 1);

  const control::TransferAbort named_abort{
      .object_id = first.object_id,
      .reason = control::TransferAbortReason::kFullDesiredStateSuperseded,
  };
  auto encoded = control::EncodeMessage(control::WireMessage(named_abort));
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded =
      control::DecodeMessage(control::MessageType::kTransferAbort, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::TransferAbort>(*decoded), named_abort);

  const TransferStart latest{
      .kind = TransferKind::kFullDesiredState,
      .object_id = Id(9),
      .total_length = 3,
  };
  ASSERT_TRUE(reassembler.Accept(latest).ok());
  ASSERT_TRUE(
      reassembler
          .Accept(TransferChunk{
              .object_id = latest.object_id, .offset = 0, .bytes = "new"})
          .ok());
  ASSERT_TRUE(reassembler.Accept(TransferEnd{latest.object_id}).ok());
  EXPECT_EQ(sink.bytes_, "new");
  EXPECT_EQ(sink.commit_count_, 1);
}

TEST(ControlProtocolTransferTest, RejectsOversizeAndIncompleteObjects) {
  RecordingSink sink;
  LargeObjectReassembler reassembler(sink);
  TransferStart start{
      .kind = TransferKind::kFullDesiredState,
      .object_id = Id(9),
      .total_length = control::kMaxFullDesiredStateBytes + 1,
  };
  EXPECT_EQ(reassembler.Accept(start).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(sink.begin_count_, 0);

  start.total_length = 3;
  ASSERT_TRUE(reassembler.Accept(start).ok());
  ASSERT_TRUE(reassembler
                  .Accept(TransferChunk{
                      .object_id = start.object_id, .offset = 0, .bytes = "ba"})
                  .ok());
  EXPECT_EQ(reassembler.Accept(TransferEnd{start.object_id}).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(sink.commit_count_, 0);
  EXPECT_TRUE(reassembler.active());
  ASSERT_TRUE(reassembler.Accept(control::TransferAbort{start.object_id}).ok());
  EXPECT_EQ(sink.abort_count_, 1);
  EXPECT_FALSE(reassembler.active());
}

TEST(ControlProtocolTransferTest, EnforcesPerEnvelopeTransferCaps) {
  const std::array<std::pair<TransferKind, std::uint64_t>, 3> cases{{
      {TransferKind::kFullDesiredState, control::kMaxFullDesiredStateBytes},
      {TransferKind::kDirectivePayload, control::kMaxDirectiveTransferBytes},
      {TransferKind::kDirectiveResult,
       control::kMaxDirectiveResultTransferBytes},
  }};
  for (const auto& [kind, cap] : cases) {
    TransferStart start{.kind = kind,
                        .object_id = Id(static_cast<std::uint8_t>(kind)),
                        .total_length = cap};
    EXPECT_TRUE(control::EncodeMessage(control::WireMessage(start)).ok());
    ++start.total_length;
    EXPECT_EQ(
        control::EncodeMessage(control::WireMessage(start)).status().code(),
        absl::StatusCode::kResourceExhausted);
  }
}

TEST(ControlProtocolIdentityTest, GeneratesCanonicalIndependentIdentities) {
  auto id160_a = control::GenerateIdentity160();
  auto id160_b = control::GenerateIdentity160();
  ASSERT_TRUE(id160_a.ok()) << id160_a.status();
  ASSERT_TRUE(id160_b.ok()) << id160_b.status();
  EXPECT_EQ(id160_a->size(), 40U);
  EXPECT_EQ(id160_b->size(), 40U);
  EXPECT_NE(*id160_a, *id160_b);
  EXPECT_TRUE(control::IsCanonicalIdentity160(*id160_a));

  auto id128_a = control::GenerateId128();
  auto id128_b = control::GenerateId128();
  ASSERT_TRUE(id128_a.ok()) << id128_a.status();
  ASSERT_TRUE(id128_b.ok()) << id128_b.status();
  EXPECT_NE(*id128_a, *id128_b);
}

TEST(ControlNodeStateTest, RemoteWorkDoesNotAdvanceLocalControl) {
  control::FullDesiredState bootstrap;
  bootstrap.control_revision = 42;
  bootstrap.groups = {{.group_id = "local", .members = {{"self", Id(1)}}},
                      {.group_id = "remote", .members = {{"other", Id(2)}}}};
  auto before = control::SelectNodeControlState(bootstrap, "self");
  ASSERT_EQ(before.local.groups.size(), 1);
  bootstrap.control_revision = 900;
  bootstrap.groups.back().failover_transition =
      control::WireFailoverTransition{};
  bootstrap.groups.back().manifest_revision = 5;
  auto after = control::SelectNodeControlState(bootstrap, "self");
  auto update = control::DiffNodeControlState(before, after);
  EXPECT_FALSE(update.local);
  EXPECT_FALSE(update.routing);
  EXPECT_FALSE(update.tasks);
  EXPECT_EQ(after.local.revision, 42);
  bootstrap.groups.back().group_term = 2;
  after = control::SelectNodeControlState(bootstrap, "self");
  update = control::DiffNodeControlState(before, after);
  EXPECT_TRUE(update.routing);
  EXPECT_FALSE(update.local);
}

TEST(ControlNodeStateTest, TaskDeltaIsAtomicIdempotentAndIndependent) {
  control::NodeControlState state;
  state.local.revision = 7;
  state.tasks_revision = 1;
  control::NodeControlUpdate update;
  control::WireProjectedDirective task;
  task.identity = {Id(1), Id(2), Id(3), 1};
  update.tasks = control::TaskChanges{1, 2, {task}, {}};
  ASSERT_TRUE(control::ApplyNodeControlUpdate(state, update).ok());
  ASSERT_TRUE(control::ApplyNodeControlUpdate(state, update).ok());
  EXPECT_EQ(state.tasks.size(), 1);
  EXPECT_EQ(state.local.revision, 7);
  auto invalid = update;
  invalid.local = control::LocalGroupState{.revision = 8};
  invalid.tasks->revision = 4;
  EXPECT_FALSE(control::ApplyNodeControlUpdate(state, invalid).ok());
  EXPECT_EQ(state.local.revision, 7);
  auto next = state;
  next.tasks.clear();
  auto removal = control::DiffNodeControlState(state, next);
  ASSERT_TRUE(removal.tasks);
  EXPECT_TRUE(removal.tasks->upserts.empty());
  ASSERT_TRUE(control::ApplyNodeControlUpdate(state, removal).ok());
  EXPECT_TRUE(state.tasks.empty());
  ASSERT_TRUE(control::ApplyNodeControlUpdate(state, update).ok());
  EXPECT_TRUE(state.tasks.empty());
}

TEST(ControlNodeStateTest, UpdateRoundTripAndConflictingRevision) {
  control::NodeControlUpdate update;
  update.request_id = Id(1);
  update.local =
      control::LocalGroupState{.revision = 3, .lease_duration_ms = 5000};
  update.directory = control::MetaDirectoryState{.revision = 2};
  auto bytes = control::EncodeNodeControlUpdate(update);
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  auto decoded = control::DecodeNodeControlUpdate(*bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, update);
  control::NodeControlState state;
  ASSERT_TRUE(control::ApplyNodeControlUpdate(state, update).ok());
  update.local->lease_duration_ms = 1000;
  EXPECT_FALSE(control::ApplyNodeControlUpdate(state, update).ok());
  update.local->revision = 2;
  ASSERT_TRUE(control::ApplyNodeControlUpdate(state, update).ok());
  EXPECT_EQ(state.local.lease_duration_ms, 5000);
}

}  // namespace
