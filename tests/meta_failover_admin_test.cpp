#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "keylane/meta/admin_client.h"
#include "keylane/meta/cluster_status.h"
#include "keylane/meta/failover_admin.h"

namespace keylane::meta {
namespace {

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

TEST(MetaFailoverAdminCodecTest, RoundTripsBoundedCanonicalRequestAndReply) {
  const FailoverAdminRequestV1 request{
      .operation_id_ = Bytes<16>(0x12),
      .group_id_ = "group-a",
      .absolute_deadline_unix_ms_ = 1'800'000'000'000,
  };
  auto encoded = EncodeFailoverAdminRequest(request);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_TRUE(encoded->starts_with("failover 1 "));
  auto decoded = DecodeFailoverAdminRequest(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, request);

  auto outcome = DecodeFailoverAdminReply(
      "OK failover 1 42 12121212121212121212121212121212");
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->submission_commit_index_, 42);
  EXPECT_EQ(outcome->operation_id_, "12121212121212121212121212121212");
}

TEST(MetaFailoverAdminCodecTest, RejectsNonCanonicalOrUnboundedInput) {
  FailoverAdminRequestV1 request{
      .operation_id_ = Bytes<16>(1),
      .group_id_ = "group-a",
      .absolute_deadline_unix_ms_ = 1'800'000'000'000,
  };
  request.operation_id_.fill(0);
  EXPECT_FALSE(EncodeFailoverAdminRequest(request).ok());
  request.operation_id_ = Bytes<16>(1);
  request.group_id_.clear();
  EXPECT_FALSE(EncodeFailoverAdminRequest(request).ok());

  EXPECT_FALSE(DecodeFailoverAdminRequest("failover 2 00").ok());
  EXPECT_FALSE(DecodeFailoverAdminRequest("failover 1 0g").ok());
  EXPECT_FALSE(DecodeFailoverAdminReply(
                   "OK failover 1 0 12121212121212121212121212121212")
                   .ok());
  EXPECT_FALSE(DecodeFailoverAdminReply(
                   "OK failover 1 42 12121212121212121212121212121212 trailing")
                   .ok());
}

ClusterHeadWireV1 LeaderHead() {
  ClusterHeadWireV1 head;
  head.responder_id_ = 1;
  head.role_ = ClusterMetaRole::kLeader;
  head.term_ = 9;
  head.leader_id_ = 1;
  head.config_index_ = 44;
  head.meta_members_ = {{.server_id_ = 1, .is_leader_ = true}};
  return head;
}

ClusterStatusWireV1 ReadyStatus() {
  ClusterStatusWireV1 status;
  status.capture_ = {.responder_id_ = 1,
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
  status.meta_members_ = LeaderHead().meta_members_;
  status.data_nodes_.push_back({
      .node_id_ = "data-1",
      .role_ = ClusterDataNodeRole::kPrimary,
      .group_id_ = "group-a",
      .current_session_ = true,
      .projection_current_ = true,
      .health_fresh_ = true,
      .population_current_ = true,
      .lease_status_ = ClusterLeaseStatus::kRecentlyGranted,
  });
  status.groups_.push_back({.group_id_ = "group-a",
                            .term_ = 4,
                            .owner_node_id_ = "data-1",
                            .config_epoch_ = 8,
                            .serving_ready_ = true,
                            .topology_converged_ = true,
                            .effective_threshold_ms_ = 1'000});
  status.slot_ranges_.push_back(
      {.first_ = 0, .last_ = 16'383, .group_id_ = "group-a"});
  return status;
}

TEST(MetaFailoverOperatorTest,
     SubmitsStableRequestToTheDiscoveredLeaderAndReportsCommit) {
  const std::string head = *EncodeClusterHeadReply(LeaderHead());
  const std::string status = *EncodeClusterStatusReply(ReadyStatus());
  const MetaOperationId operation_id = Bytes<16>(0x21);
  const std::uint64_t absolute_deadline = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() +
      5'000);
  std::vector<std::string> calls;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         MetaAdminDeadline) -> absl::StatusOr<std::string> {
    calls.emplace_back(command);
    if (command == "clusterhead 1") return head;
    if (command == "clusterstatus 1") return status;
    auto request = DecodeFailoverAdminRequest(command);
    EXPECT_TRUE(request.ok()) << request.status();
    if (!request.ok()) return request.status();
    EXPECT_EQ(request->operation_id_, operation_id);
    EXPECT_EQ(request->group_id_, "group-a");
    EXPECT_EQ(request->absolute_deadline_unix_ms_, absolute_deadline);
    return "OK failover 1 51 21212121212121212121212121212121";
  });

  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/meta.sock"};
  ClusterStatusOptions status_options;
  status_options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  FailoverRequestOptions request{
      .group_id_ = "group-a",
      .transition_timeout_ = std::chrono::seconds(5),
      .operation_id_ = operation_id,
      .absolute_deadline_unix_ms_ = absolute_deadline,
  };
  auto outcome = op.Failover(seed, request, status_options);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->submission_commit_index_, 51);
  EXPECT_EQ(outcome->operation_id_, "21212121212121212121212121212121");
  ASSERT_EQ(calls.size(), 3);
  EXPECT_TRUE(calls.back().starts_with("failover 1 "));
}

TEST(MetaFailoverOperatorTest, UnknownGroupNeverSendsMutation) {
  const std::string head = *EncodeClusterHeadReply(LeaderHead());
  const std::string status = *EncodeClusterStatusReply(ReadyStatus());
  int calls = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         MetaAdminDeadline) -> absl::StatusOr<std::string> {
    ++calls;
    return command == "clusterhead 1" ? head : status;
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/meta.sock"};
  ClusterStatusOptions status_options;
  status_options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Failover(seed,
                             {.group_id_ = "missing",
                              .operation_id_ = Bytes<16>(1),
                              .absolute_deadline_unix_ms_ = 1'800'000'000'000},
                             status_options);
  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(calls, 2);
}

TEST(MetaFailoverOperatorTest, DistinguishesDefinitelyNotSentFromUncertain) {
  const std::string head = *EncodeClusterHeadReply(LeaderHead());
  const std::string status = *EncodeClusterStatusReply(ReadyStatus());
  const MetaOperationId operation_id = Bytes<16>(0x31);
  const auto run = [&](bool definitely_not_sent) {
    ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                           MetaAdminDeadline) -> absl::StatusOr<std::string> {
      if (command == "clusterhead 1") return head;
      if (command == "clusterstatus 1") return status;
      absl::Status unavailable = absl::UnavailableError("connection lost");
      return definitely_not_sent
                 ? absl::StatusOr<std::string>(
                       MarkMetaAdminRequestNotSent(std::move(unavailable)))
                 : absl::StatusOr<std::string>(std::move(unavailable));
    });
    MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                         .endpoint_ = "/meta.sock"};
    ClusterStatusOptions status_options;
    status_options.deadline_ =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    return op.Failover(seed,
                       {.group_id_ = "group-a",
                        .operation_id_ = operation_id,
                        .absolute_deadline_unix_ms_ = 1'800'000'000'000},
                       status_options);
  };

  EXPECT_EQ(run(true).status().code(), absl::StatusCode::kUnavailable);
  auto uncertain = run(false);
  EXPECT_EQ(uncertain.status().code(), absl::StatusCode::kAborted);
  EXPECT_NE(uncertain.status().message().find(
                "operation=31313131313131313131313131313131"),
            std::string_view::npos);
}

TEST(MetaFailoverOperatorTest,
     UncertainAndUntrustworthyErrorsPreserveTheExpectedOperationId) {
  const std::string head = *EncodeClusterHeadReply(LeaderHead());
  const std::string status = *EncodeClusterStatusReply(ReadyStatus());
  const MetaOperationId operation_id = Bytes<16>(0x41);
  const std::array<std::string_view, 5> replies = {
      "ERR failover 1 proposal timeout",
      "ERR failover 1 proposal cancelled",
      "ERR failover 1 proposal propose-failed",
      "ERR failover 1 proposal uncertain-outcome",
      "ERR failover 1 unexpected-stage",
  };

  for (std::string_view reply : replies) {
    SCOPED_TRACE(reply);
    ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                           MetaAdminDeadline) -> absl::StatusOr<std::string> {
      if (command == "clusterhead 1") return head;
      if (command == "clusterstatus 1") return status;
      return std::string(reply);
    });
    MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                         .endpoint_ = "/meta.sock"};
    ClusterStatusOptions status_options;
    status_options.deadline_ =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    auto outcome =
        op.Failover(seed,
                    {.group_id_ = "group-a",
                     .operation_id_ = operation_id,
                     .absolute_deadline_unix_ms_ = 1'800'000'000'000},
                    status_options);

    EXPECT_EQ(outcome.status().code(), absl::StatusCode::kAborted);
    EXPECT_NE(outcome.status().message().find(
                  "operation=41414141414141414141414141414141"),
              std::string_view::npos);
  }
}

TEST(MetaFailoverOperatorTest, ProposalResourceExhaustionIsADefiniteRejection) {
  const std::string head = *EncodeClusterHeadReply(LeaderHead());
  const std::string status = *EncodeClusterStatusReply(ReadyStatus());
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view command,
                         MetaAdminDeadline) -> absl::StatusOr<std::string> {
    if (command == "clusterhead 1") return head;
    if (command == "clusterstatus 1") return status;
    return "ERR failover 1 proposal resource-exhausted";
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/meta.sock"};
  ClusterStatusOptions status_options;
  status_options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  auto outcome = op.Failover(seed,
                             {.group_id_ = "group-a",
                              .operation_id_ = Bytes<16>(0x51),
                              .absolute_deadline_unix_ms_ = 1'800'000'000'000},
                             status_options);

  EXPECT_EQ(outcome.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(outcome.status().message(), "proposal resource-exhausted");
}

TEST(MetaFailoverOperatorTest, ExactRetryIdentityRequiresDeadlinePair) {
  int calls = 0;
  ClusterOperator op([&](const MetaAdminTarget&, std::string_view,
                         MetaAdminDeadline) -> absl::StatusOr<std::string> {
    ++calls;
    return absl::InternalError("must not send");
  });
  MetaAdminTarget seed{.transport_ = MetaAdminTarget::Transport::kUnix,
                       .endpoint_ = "/meta.sock"};
  ClusterStatusOptions status_options;
  status_options.deadline_ =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);

  EXPECT_EQ(
      op.Failover(seed, {.group_id_ = "group-a", .operation_id_ = Bytes<16>(1)},
                  status_options)
          .status()
          .code(),
      absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(op.Failover(seed,
                        {.group_id_ = "group-a",
                         .absolute_deadline_unix_ms_ = 1'800'000'000'000},
                        status_options)
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(calls, 0);
}

}  // namespace
}  // namespace keylane::meta
