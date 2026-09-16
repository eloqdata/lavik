#include "keylane/cluster/control_protocol.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"

namespace {

namespace control = keylane::cluster::control;

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

WireHash256 Sha256(std::string_view bytes) {
  return control::ComputeSha256(bytes);
}

void SetLeaseTiming(control::FullDesiredState* state) {
  state->authority_lease_duration_ms = 3'000;
}

control::FullDesiredState FailoverFullState() {
  control::FullDesiredState state;
  state.source_meta_applied_index = 91;
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
  state.directive_set_digest =
      *control::ComputeDirectiveSetDigest(state.current_directives);
  state.projection_hash = *control::ComputeProjectionHash(state);
  return state;
}

void AppendBe16(std::string* bytes, std::uint16_t value) {
  bytes->push_back(static_cast<char>(value >> 8));
  bytes->push_back(static_cast<char>(value));
}

void AppendBe32(std::string* bytes, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    bytes->push_back(static_cast<char>(value >> shift));
  }
}

absl::StatusOr<WireHash256> LegacyDirectiveSetDigest(
    const std::vector<control::WireProjectedDirective>& directives) {
  std::vector<std::string> entries;
  entries.reserve(directives.size());
  for (const control::WireProjectedDirective& source : directives) {
    control::Directive directive{
        .basis = source.basis,
        .authority = source.authority,
        .identity = source.identity,
        .recipient_node_id = source.recipient_node_id,
        .recipient_boot_id = source.recipient_boot_id,
        .target_node_id = source.target_node_id,
        .target_boot_id = source.target_boot_id,
        .source_node_id = source.source_node_id,
        .source_assignment_id = source.source_assignment_id,
        .source_boot_id = source.source_boot_id,
        .source_replication_history_id = source.source_replication_history_id,
        .manifest_revision = source.manifest_revision,
        .manifest_digest = source.manifest_digest,
        .partition_replication_epoch = source.partition_replication_epoch,
        .kind = source.kind,
        .payload = source.payload,
        .preconditions = source.preconditions,
        .storage_mutating = source.storage_mutating,
        .force = source.force,
    };
    directive.basis = {};
    auto encoded =
        control::EncodeMessage(control::WireMessage(std::move(directive)));
    if (!encoded.ok()) return encoded.status();
    constexpr std::size_t kSessionIdBytes = 16;
    entries.push_back(encoded->substr(kSessionIdBytes));
  }
  std::sort(entries.begin(), entries.end(),
            [](const std::string& left, const std::string& right) {
              return std::lexicographical_compare(
                  left.begin(), left.end(), right.begin(), right.end(),
                  [](char lhs, char rhs) {
                    return static_cast<unsigned char>(lhs) <
                           static_cast<unsigned char>(rhs);
                  });
            });
  std::string canonical = "KLDSET";
  AppendBe16(&canonical, 1);
  AppendBe32(&canonical, static_cast<std::uint32_t>(entries.size()));
  for (const std::string& entry : entries) {
    AppendBe32(&canonical, static_cast<std::uint32_t>(entry.size()));
    canonical.append(entry);
  }
  return Sha256(canonical);
}

std::string Hex(const WireHash256& bytes) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (std::uint8_t byte : bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0x0f]);
  }
  return result;
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
      0x4b, 0x4c, 0x43, 0x50,                           // KLCP
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
      .projection_hash = Sha256("projection"),
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
      .projection_hash = challenge.projection_hash,
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
      .current_projection_hash = challenge.projection_hash,
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
              .manifest_digest = Sha256("manifest"),
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
              .manifest_digest = Sha256("manifest"),
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
              .manifest_digest = Sha256("manifest"),
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
  heartbeat.role_information = control::AuthorityLeaseRequest{
      .challenge = {.nonce = Id(2),
                    .projection_hash = Sha256("projection"),
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
      heartbeat.failover_observation = control::CandidatePrepared{
          .transition_id = Id(4),
          .action_id = Id(5),
          .candidate_node_id = std::string(40, 'd'),
          .candidate_assignment_id = Id(6),
          .candidate_boot_id = std::string(40, 'e'),
          .prepared_context_id = Id(7),
          .prepared_context_hash = Sha256("prepared-context")};
    } else if (kind == 2) {
      heartbeat.failover_observation = control::ActionFailed{
          .transition_id = Id(4),
          .action_id = Id(5),
          .candidate_node_id = std::string(40, 'd'),
          .candidate_assignment_id = Id(6),
          .candidate_boot_id = std::string(40, 'e'),
          .population_manifest_revision = 10,
          .population_manifest_digest = Sha256("manifest"),
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
                   .manifest_digest = Sha256("manifest"),
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
      .population_manifest_digest = Sha256("manifest"),
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

TEST(ControlProtocolCodecTest, HeartbeatReplayIsExactAndGapFree) {
  control::HeartbeatSequenceWindow window;
  const WireHash256 first_hash = Sha256("heartbeat-1");
  auto observed = window.Observe(1, first_hash);
  ASSERT_TRUE(observed.ok());
  EXPECT_EQ(*observed, control::HeartbeatSequenceDisposition::kAcceptNew);
  observed = window.Observe(1, first_hash);
  ASSERT_TRUE(observed.ok());
  EXPECT_EQ(*observed, control::HeartbeatSequenceDisposition::kReplayCachedAck);
  EXPECT_EQ(window.Observe(1, Sha256("changed")).status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(window.Observe(3, Sha256("gap")).status().code(),
            absl::StatusCode::kFailedPrecondition);
  observed = window.Observe(2, Sha256("heartbeat-2"));
  ASSERT_TRUE(observed.ok());
  EXPECT_EQ(*observed, control::HeartbeatSequenceDisposition::kAcceptNew);
}

TEST(ControlProtocolCodecTest,
     DirectiveLifecycleKeepsRecipientDistinctFromRebuildTarget) {
  control::Directive directive{
      .session_id = Id(1),
      .basis = {.source_meta_applied_index = 19,
                .projection_hash = Sha256("projection")},
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
      .manifest_digest = Sha256("manifest"),
      .partition_replication_epoch = 22,
      .kind = control::WireDirectiveKind::kAuthorizeSource,
      .payload = "authorize",
      .preconditions = "ready",
      .storage_mutating = false,
      .force = false,
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

  control::DirectiveReceipt receipt{
      .session_id = directive.session_id,
      .recipient_boot_id = directive.recipient_boot_id,
      .identity = directive.identity,
      .stage = control::DirectiveReceiptStage::kAccepted,
  };
  encoded = control::EncodeMessage(control::WireMessage{receipt});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  decoded = control::DecodeMessage(MessageType::kDirectiveReceipt, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(std::get<control::DirectiveReceipt>(*decoded), receipt);

  control::DirectiveResult result{
      .session_id = directive.session_id,
      .recipient_boot_id = directive.recipient_boot_id,
      .assignment_id = directive.authority.assignment_id,
      .identity = directive.identity,
      .status = control::DirectiveResultStatus::kSucceeded,
      .result_hash = Sha256("ok"),
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
      .result_hash = result.result_hash,
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
      .basis = {.source_meta_applied_index = 19,
                .projection_hash = Sha256("projection")},
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
      .manifest_digest = Sha256("manifest"),
      .partition_replication_epoch = 1,
      .kind = control::WireDirectiveKind::kInitializeEmptyPopulation,
      // The existing payload binds the history advertised by the target's
      // current Hello. Zero wire identities denote the absent source and can
      // never authorize a replication connection.
      .payload = std::string(40, 'c'),
      .storage_mutating = true,
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
  EXPECT_EQ(*encoded, std::string("KLRR\0\1\0\0\0\3", 10));
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

TEST(ControlProtocolCodecTest,
     OperationEvidenceCarriesSessionBootAssignmentAndFreshnessAnchors) {
  const std::string evidence_body = "flow-0=41,flow-1=52";
  control::OperationEvidence evidence{
      .session_id = Id(1),
      .reporter_boot_id = std::string(40, 'b'),
      .assignment_id = Id(2),
      .operation_id = Id(3),
      .kind_phase = "promotion:durability-ready",
      .evidence_hash = Sha256(evidence_body),
      .evidence = evidence_body,
      .group_id = "group-a",
      .group_term = 7,
      .manifest_revision = 11,
      .partition_replication_epoch = 13,
      .replication_history_id = std::string(40, 'c'),
  };

  auto encoded = control::EncodeMessage(control::WireMessage{evidence});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded =
      control::DecodeMessage(MessageType::kOperationEvidence, *encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_TRUE(std::holds_alternative<control::OperationEvidence>(*decoded));
  EXPECT_EQ(std::get<control::OperationEvidence>(*decoded), evidence);

  evidence.evidence_hash = Sha256("different");
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{evidence}).status().code(),
      absl::StatusCode::kInvalidArgument);
  evidence.evidence_hash = Sha256(evidence.evidence);
  evidence.reporter_boot_id = "not-an-incarnation";
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{evidence}).status().code(),
      absl::StatusCode::kInvalidArgument);
}

TEST(ControlProtocolCodecTest, OperationEvidenceUsesObjectTransferWhenLarge) {
  control::OperationEvidence evidence{
      .session_id = Id(1),
      .reporter_boot_id = std::string(40, 'b'),
      .assignment_id = Id(2),
      .operation_id = Id(3),
      .kind_phase = "full-sync:progress",
      .evidence = std::string(control::kMaxFramePayloadBytes, 'x'),
      .group_id = "group-a",
      .group_term = 7,
      .manifest_revision = 11,
      .partition_replication_epoch = 13,
      .replication_history_id = std::string(40, 'c'),
  };
  evidence.evidence_hash = Sha256(evidence.evidence);

  auto encoded = control::EncodeMessage(control::WireMessage{evidence});
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_GT(encoded->size(), control::kMaxFramePayloadBytes);
  EXPECT_LE(encoded->size(), control::kMaxOperationEvidenceTransferBytes);

  evidence.evidence.push_back('x');
  evidence.evidence.resize(control::kMaxOpaqueFieldBytes + 1, 'x');
  evidence.evidence_hash = Sha256(evidence.evidence);
  EXPECT_EQ(
      control::EncodeMessage(control::WireMessage{evidence}).status().code(),
      absl::StatusCode::kResourceExhausted);
}

TEST(ControlProtocolLeaseTest, GrantMatchesOnceAndUsesOriginalSendTime) {
  control::LeaseChallengeTracker tracker;
  control::LeaseChallenge challenge{
      .nonce = Id(2),
      .projection_hash = Sha256("projection"),
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
      .projection_hash = challenge.projection_hash,
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

TEST(ControlProtocolFullStateTest,
     RoundTripsTypedProjectionAndDerivesObjectHash) {
  control::FullDesiredState state;
  state.source_meta_applied_index = 91;
  state.topology_epoch = 23;
  state.authority_lease_duration_ms = 3'000;
  state.projection_hash = Sha256("semantic projection");
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
  group.manifest_digest = Sha256("manifest");
  group.partition_replication_epoch = 19;
  state.groups.push_back(std::move(group));
  state.manifests.push_back(
      {.revision = 15,
       .digest = Sha256("manifest"),
       .entries = {{.partition_id = 0, .logical_epoch = 17},
                   {.partition_id = 1, .logical_epoch = 18}}});
  state.current_directives.push_back(
      {.basis = {.source_meta_applied_index = 91,
                 .projection_hash = state.projection_hash},
       .authority = {.group_id = "group-a",
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
       .manifest_digest = Sha256("manifest"),
       .partition_replication_epoch = 19,
       .kind = control::WireDirectiveKind::kRebuild,
       .payload = "rebuild",
       .preconditions = "empty target",
       .storage_mutating = true,
       .force = false});
  auto directive_digest =
      control::ComputeDirectiveSetDigest(state.current_directives);
  ASSERT_TRUE(directive_digest.ok()) << directive_digest.status();
  state.directive_set_digest = *directive_digest;

  auto projection_hash = control::ComputeProjectionHash(state);
  ASSERT_TRUE(projection_hash.ok()) << projection_hash.status();
  state.projection_hash = *projection_hash;
  state.current_directives[0].basis.projection_hash = *projection_hash;

  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = control::DecodeFullDesiredState(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  state.object_hash = Sha256(*encoded);
  EXPECT_EQ(*decoded, state);
}

TEST(ControlProtocolFullStateTest,
     RoundTripsTypedFailoverStateAndCoversItInProjectionHash) {
  control::FullDesiredState state = FailoverFullState();
  const auto original_hash = state.projection_hash;

  auto encoded = control::EncodeFullDesiredState(state);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_LE(encoded->size(), control::kMaxFramePayloadBytes);
  auto decoded = control::DecodeFullDesiredState(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  state.object_hash = Sha256(*encoded);
  EXPECT_EQ(*decoded, state);

  state.groups[0].steady_replication_enabled = false;
  const auto lifecycle_change = control::ComputeProjectionHash(state);
  ASSERT_TRUE(lifecycle_change.ok()) << lifecycle_change.status();
  EXPECT_NE(*lifecycle_change, original_hash);
  state.groups[0].steady_replication_enabled = true;

  ASSERT_TRUE(state.groups[0].failover_transition.has_value());
  ++state.groups[0].failover_transition->revision;
  const auto transition_change = control::ComputeProjectionHash(state);
  ASSERT_TRUE(transition_change.ok()) << transition_change.status();
  EXPECT_NE(*transition_change, original_hash);

  state.groups[0].failover_transition.reset();
  state.groups[0].activation_action_id = Id(21);
  const auto activation_change = control::ComputeProjectionHash(state);
  ASSERT_TRUE(activation_change.ok()) << activation_change.status();
  EXPECT_NE(*activation_change, original_hash);
}

TEST(ControlProtocolFullStateTest, RejectsZeroLeaseAndDerivesHeartbeatCadence) {
  control::FullDesiredState state = FailoverFullState();

  state.authority_lease_duration_ms = 0;
  EXPECT_EQ(control::EncodeFullDesiredState(state).status().code(),
            absl::StatusCode::kInvalidArgument);

  state.authority_lease_duration_ms = 2;
  auto projection_hash = control::ComputeProjectionHash(state);
  ASSERT_TRUE(projection_hash.ok()) << projection_hash.status();
  state.projection_hash = *projection_hash;
  EXPECT_TRUE(control::EncodeFullDesiredState(state).ok());
  EXPECT_EQ(control::DataHeartbeatIntervalMs(2), 1u);
  EXPECT_EQ(control::DataHeartbeatIntervalMs(3'000), 1'000u);
}

TEST(ControlProtocolFullStateTest, RejectsMalformedFailoverState) {
  control::FullDesiredState state = FailoverFullState();
  ASSERT_TRUE(state.groups[0].failover_transition.has_value());
  auto& transition = *state.groups[0].failover_transition;

  transition.mode = static_cast<control::WireFailoverMode>(99);
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
            absl::StatusCode::kInvalidArgument);
  transition.mode = control::WireFailoverMode::kControlled;

  ASSERT_TRUE(transition.candidate_action.has_value());
  transition.candidate_action->domain.source_group_term =
      transition.target_term;
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
            absl::StatusCode::kInvalidArgument);
  transition.candidate_action->domain.source_group_term = 7;

  transition.candidate_action->authorization->loss_if_cutover =
      control::WireFailoverLoss::kUnknown;
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
            absl::StatusCode::kInvalidArgument);
  transition.candidate_action->authorization->loss_if_cutover =
      control::WireFailoverLoss::kNone;

  transition.candidate_action.reset();
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
            absl::StatusCode::kInvalidArgument);

  state = FailoverFullState();
  state.groups[0].failover_transition.reset();
  state.groups[0].activation_action_id = WireId128{};
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
            absl::StatusCode::kInvalidArgument);

  state.groups[0].activation_action_id = Id(1);
  state.groups[0].grant_active = false;
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ControlProtocolFullStateTest, RoundTripsEmptyTopologyAtEpochZero) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.source_meta_applied_index = 1;
  state.topology_epoch = 0;
  auto directives =
      control::ComputeDirectiveSetDigest(state.current_directives);
  ASSERT_TRUE(directives.ok()) << directives.status();
  state.directive_set_digest = *directives;
  auto projection = control::ComputeProjectionHash(state);
  ASSERT_TRUE(projection.ok()) << projection.status();
  state.projection_hash = *projection;

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
  state.source_meta_applied_index = 1;
  state.meta_directory.reserve(512);
  for (std::uint32_t id = 1; id <= 512; ++id) {
    state.meta_directory.push_back(
        {.server_id = id, .host = "127.0.0.1", .port = 7400});
  }
  auto directives =
      control::ComputeDirectiveSetDigest(state.current_directives);
  ASSERT_TRUE(directives.ok()) << directives.status();
  state.directive_set_digest = *directives;
  auto projection = control::ComputeProjectionHash(state);
  ASSERT_TRUE(projection.ok()) << projection.status();
  state.projection_hash = *projection;

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
  state.source_meta_applied_index = 1;
  auto directives =
      control::ComputeDirectiveSetDigest(state.current_directives);
  ASSERT_TRUE(directives.ok()) << directives.status();
  state.directive_set_digest = *directives;
  auto projection = control::ComputeProjectionHash(state);
  ASSERT_TRUE(projection.ok()) << projection.status();
  state.projection_hash = *projection;

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
  state.object_hash = Sha256(*encoded);
  EXPECT_EQ(*full_state, state);
}

TEST(ControlProtocolFullStateTest,
     ProjectionHashExcludesAppliedIndexButCoversSemantics) {
  control::FullDesiredState state;
  SetLeaseTiming(&state);
  state.source_meta_applied_index = 10;
  state.topology_epoch = 1;
  state.nodes.push_back(
      {.node_id = std::string(40, 'a'), .host = "127.0.0.1", .port = 6379});
  state.groups.push_back(
      {.group_id = "group-a", .partition_replication_epoch = 4});
  auto original = control::ComputeProjectionHash(state);
  ASSERT_TRUE(original.ok()) << original.status();

  state.source_meta_applied_index = 11;  // unrelated Meta commit
  auto unrelated_commit = control::ComputeProjectionHash(state);
  ASSERT_TRUE(unrelated_commit.ok());
  EXPECT_EQ(*unrelated_commit, *original);

  state.groups[0].partition_replication_epoch = 5;
  auto population_epoch_change = control::ComputeProjectionHash(state);
  ASSERT_TRUE(population_epoch_change.ok());
  EXPECT_NE(*population_epoch_change, *original);
  state.groups[0].partition_replication_epoch = 4;

  state.nodes[0].port = 6380;  // node-specific semantic change
  auto semantic_change = control::ComputeProjectionHash(state);
  ASSERT_TRUE(semantic_change.ok());
  EXPECT_NE(*semantic_change, *original);

  state.nodes[0].port = 6379;
  state.topology_epoch = 2;
  auto topology_change = control::ComputeProjectionHash(state);
  ASSERT_TRUE(topology_change.ok());
  EXPECT_NE(*topology_change, *original);

  state.topology_epoch = 1;
  state.authority_lease_duration_ms = 6'000;
  auto lease_timing_change = control::ComputeProjectionHash(state);
  ASSERT_TRUE(lease_timing_change.ok()) << lease_timing_change.status();
  EXPECT_NE(*lease_timing_change, *original);
}

TEST(ControlProtocolFullStateTest,
     DirectiveSetDigestIsOrderIndependentAndIgnoresProjectionBasis) {
  control::WireProjectedDirective first{
      .basis = {.source_meta_applied_index = 1,
                .projection_hash = Sha256("old projection")},
      .authority = {.group_id = "group-a",
                    .assignment_id = Id(1),
                    .group_term = 2},
      .identity = {.operation_id = Id(5),
                   .directive_id = Id(6),
                   .attempt_id = Id(7),
                   .directive_revision = 8},
      .recipient_node_id = std::string(40, 'a'),
      .recipient_boot_id = std::string(40, 'b'),
      .target_node_id = std::string(40, 'a'),
      .target_boot_id = std::string(40, 'b'),
      .source_node_id = std::string(40, 'c'),
      .source_assignment_id = Id(11),
      .source_boot_id = std::string(40, 'd'),
      .source_replication_history_id = std::string(40, 'e'),
      .manifest_revision = 9,
      .manifest_digest = Sha256("manifest"),
      .partition_replication_epoch = 10,
      .kind = control::WireDirectiveKind::kRebuild,
      .payload = "payload",
      .preconditions = "preconditions",
      .storage_mutating = true,
      .force = false};
  control::WireProjectedDirective second = first;
  second.identity.directive_id = Id(10);
  second.kind = control::WireDirectiveKind::kRevokeSources;

  auto original =
      control::ComputeDirectiveSetDigest(std::vector{first, second});
  ASSERT_TRUE(original.ok()) << original.status();
  auto reversed =
      control::ComputeDirectiveSetDigest(std::vector{second, first});
  ASSERT_TRUE(reversed.ok()) << reversed.status();
  EXPECT_EQ(*reversed, *original);

  // Pin the streaming implementation to the original v1 definition, which
  // sorted complete canonical entries before hashing them.
  auto legacy = LegacyDirectiveSetDigest(std::vector{second, first});
  ASSERT_TRUE(legacy.ok()) << legacy.status();
  EXPECT_EQ(*legacy, *original);

  first.basis.source_meta_applied_index = 99;
  first.basis.projection_hash = Sha256("new projection");
  auto rebased = control::ComputeDirectiveSetDigest(std::vector{second, first});
  ASSERT_TRUE(rebased.ok()) << rebased.status();
  EXPECT_EQ(*rebased, *original);

  ++first.partition_replication_epoch;
  auto population_epoch_change =
      control::ComputeDirectiveSetDigest(std::vector{second, first});
  ASSERT_TRUE(population_epoch_change.ok()) << population_epoch_change.status();
  EXPECT_NE(*population_epoch_change, *original);
  --first.partition_replication_epoch;

  first.payload = "different";
  auto semantic_change =
      control::ComputeDirectiveSetDigest(std::vector{second, first});
  ASSERT_TRUE(semantic_change.ok()) << semantic_change.status();
  EXPECT_NE(*semantic_change, *original);

  first.payload = second.payload;
  first.source_assignment_id = Id(12);
  semantic_change =
      control::ComputeDirectiveSetDigest(std::vector{second, first});
  ASSERT_TRUE(semantic_change.ok()) << semantic_change.status();
  EXPECT_NE(*semantic_change, *original);
}

TEST(ControlProtocolFullStateTest,
     StreamingDirectiveDigestMatchesV1ByteSortAcrossFieldsAndPermutations) {
  const control::WireProjectedDirective base{
      .basis = {.source_meta_applied_index = 1,
                .projection_hash = Sha256("projection")},
      .authority = {.group_id = "group-a",
                    .assignment_id = Id(1),
                    .group_term = 2},
      .identity = {.operation_id = Id(5),
                   .directive_id = Id(6),
                   .attempt_id = Id(7),
                   .directive_revision = 8},
      .recipient_node_id = std::string(40, 'a'),
      .recipient_boot_id = std::string(40, 'b'),
      .target_node_id = std::string(40, 'c'),
      .target_boot_id = std::string(40, 'd'),
      .source_node_id = std::string(40, 'e'),
      .source_assignment_id = Id(9),
      .source_boot_id = std::string(40, 'f'),
      .source_replication_history_id = std::string(40, '1'),
      .manifest_revision = 10,
      .manifest_digest = Sha256("manifest"),
      .partition_replication_epoch = 11,
      .kind = control::WireDirectiveKind::kRebuild,
      .payload = "payload",
      .preconditions = "preconditions",
      .storage_mutating = true,
      .force = false};

  std::vector<control::WireProjectedDirective> directives{base};
  const auto add = [&](auto mutate) {
    control::WireProjectedDirective candidate = base;
    mutate(candidate);
    directives.push_back(std::move(candidate));
  };
  add([](auto& value) { value.basis.source_meta_applied_index = 99; });
  add([](auto& value) { value.authority.group_id = "z"; });
  add([](auto& value) { value.authority.assignment_id = Id(2); });
  add([](auto& value) { value.authority.group_term = 12; });
  add([](auto& value) { value.identity.operation_id = Id(15); });
  add([](auto& value) { value.identity.directive_id = Id(16); });
  add([](auto& value) { value.identity.attempt_id = Id(17); });
  add([](auto& value) { value.identity.directive_revision = 18; });
  add([](auto& value) { value.recipient_node_id = std::string(40, '2'); });
  add([](auto& value) { value.recipient_boot_id = std::string(40, '3'); });
  add([](auto& value) { value.target_node_id = std::string(40, '4'); });
  add([](auto& value) { value.target_boot_id = std::string(40, '5'); });
  add([](auto& value) { value.source_node_id = std::string(40, '6'); });
  add([](auto& value) { value.source_assignment_id = Id(19); });
  add([](auto& value) { value.source_boot_id = std::string(40, '7'); });
  add([](auto& value) {
    value.source_replication_history_id = std::string(40, '8');
  });
  add([](auto& value) { value.manifest_revision = 20; });
  add([](auto& value) { value.manifest_digest = Sha256("other manifest"); });
  add([](auto& value) { value.partition_replication_epoch = 21; });
  add([](auto& value) {
    value.kind = control::WireDirectiveKind::kRevokeSources;
  });
  add([](auto& value) { value.payload = "z"; });
  add([](auto& value) { value.payload = std::string(7, '\xff'); });
  add([](auto& value) { value.preconditions = "x"; });
  add([](auto& value) { value.storage_mutating = false; });
  add([](auto& value) { value.force = true; });

  std::mt19937_64 random(0x4b4c4350);
  for (int permutation = 0; permutation < 16; ++permutation) {
    auto streamed = control::ComputeDirectiveSetDigest(directives);
    auto legacy = LegacyDirectiveSetDigest(directives);
    ASSERT_TRUE(streamed.ok()) << streamed.status();
    ASSERT_TRUE(legacy.ok()) << legacy.status();
    EXPECT_EQ(*streamed, *legacy) << "permutation " << permutation;
    std::shuffle(directives.begin(), directives.end(), random);
  }
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
  auto directive_digest =
      control::ComputeDirectiveSetDigest(state.current_directives);
  ASSERT_TRUE(directive_digest.ok()) << directive_digest.status();
  state.directive_set_digest = *directive_digest;
  auto projection_hash = control::ComputeProjectionHash(state);
  ASSERT_TRUE(projection_hash.ok()) << projection_hash.status();
  state.projection_hash = *projection_hash;

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
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
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
  EXPECT_EQ(control::ComputeProjectionHash(state).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ControlProtocolTransferTest, StreamsOneBoundedObjectAndChecksDigest) {
  RecordingSink sink;
  LargeObjectReassembler reassembler(sink);
  const std::string contents = "large desired state";
  const TransferStart start{
      .kind = TransferKind::kFullDesiredState,
      .object_id = Id(7),
      .total_length = contents.size(),
      .sha256 = Sha256(contents),
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
      .sha256 = Sha256("old"),
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
      .sha256 = Sha256("new"),
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

TEST(ControlProtocolTransferTest, RejectsOversizeAndAbortsDigestMismatch) {
  RecordingSink sink;
  LargeObjectReassembler reassembler(sink);
  TransferStart start{
      .kind = TransferKind::kFullDesiredState,
      .object_id = Id(9),
      .total_length = control::kMaxFullDesiredStateBytes + 1,
      .sha256 = {},
  };
  EXPECT_EQ(reassembler.Accept(start).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(sink.begin_count_, 0);

  start.total_length = 3;
  start.sha256 = Sha256("good");
  ASSERT_TRUE(reassembler.Accept(start).ok());
  ASSERT_TRUE(
      reassembler
          .Accept(TransferChunk{
              .object_id = start.object_id, .offset = 0, .bytes = "bad"})
          .ok());
  EXPECT_EQ(reassembler.Accept(TransferEnd{start.object_id}).code(),
            absl::StatusCode::kDataLoss);
  EXPECT_EQ(sink.abort_count_, 1);
  EXPECT_FALSE(reassembler.active());
}

TEST(ControlProtocolTransferTest, EnforcesPerEnvelopeTransferCaps) {
  const std::array<std::pair<TransferKind, std::uint64_t>, 4> cases{{
      {TransferKind::kFullDesiredState, control::kMaxFullDesiredStateBytes},
      {TransferKind::kObservationEvidence,
       control::kMaxOperationEvidenceTransferBytes},
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

TEST(ControlProtocolIdentityTest, UsesStandardSha256) {
  EXPECT_EQ(Hex(Sha256("abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

}  // namespace
