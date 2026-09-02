#include "tests/cluster/fault_harness.h"

#include <algorithm>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "gtest/gtest.h"
#include "tests/cluster/reference_model.h"

namespace keylane::test::cluster {
namespace {

class NoisyFailureWorld final : public ScenarioWorld {
 public:
  std::vector<Action> EnabledActions() const override {
    if (broken_) return {};
    return {Action{.name_ = "noop", .arguments_ = {}, .payload_ = {}},
            Action{.name_ = "trigger", .arguments_ = {}, .payload_ = {}}};
  }

  absl::Status Apply(const Action& action) override {
    if (action.name_ == "noop") return absl::OkStatus();
    if (action.name_ == "trigger") {
      broken_ = true;
      return absl::OkStatus();
    }
    return absl::FailedPreconditionError("unknown noisy action");
  }

  std::string Observe() const override { return broken_ ? "broken" : "ok"; }

  std::optional<Finding> CheckInvariants() const override {
    if (!broken_) return std::nullopt;
    return Finding{.invariant_id_ = "test.noisy-failure",
                   .witness_ = "triggered"};
  }

  bool HasPendingWork() const override { return !broken_; }

 private:
  bool broken_ = false;
};

class NoisyFailureScenario final : public Scenario {
 public:
  std::string_view name() const override { return "test-noisy-failure"; }
  std::uint32_t schema() const override { return 1; }
  std::unique_ptr<ScenarioWorld> NewWorld() const override {
    return std::make_unique<NoisyFailureWorld>();
  }
};

TEST(KftTraceTest, HasCanonicalRoundTripAndGoldenEncoding) {
  Trace trace{
      .mode_ = TraceMode::kExactModel,
      .scenario_ = "scenario one",
      .schema_ = 7,
      .seed_ = 42,
      .initial_state_hash_ = "ABCD",
      .records_ = {{.logical_time_ = 1,
                    .kind_ = TraceRecordKind::kChoice,
                    .name_ = "action",
                    .payload_ = "x y"}},
  };
  const std::string encoded = EncodeTrace(trace);
  EXPECT_EQ(encoded,
            "KFT1\n"
            "mode\texact-model\n"
            "scenario\tscenario%20one\n"
            "schema\t7\n"
            "seed\t42\n"
            "initial\tABCD\n"
            "record\t1\tchoice\taction\tx%20y\n");
  auto decoded = DecodeTrace(encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, trace);
  EXPECT_EQ(EncodeTrace(*decoded), encoded);
}

TEST(KftTraceTest, RejectsMalformedAndUnknownInputs) {
  EXPECT_FALSE(DecodeTrace("KFT2\n").ok());
  EXPECT_FALSE(
      DecodeTrace("KFT1\nmode\tother\nscenario\tx\nschema\t1\nseed\t1\n"
                  "initial\tx\n")
          .ok());
  EXPECT_FALSE(DecodeTrace("KFT1\nmode\texact-model\nscenario\t%GG\nschema\t1\n"
                           "seed\t1\ninitial\tx\n")
                   .ok());
  EXPECT_FALSE(DecodeTrace("KFT1\nmode\texact-model\nscenario\tx\nschema\t1\n"
                           "seed\t1\ninitial\tx\nrecord\t1\tunknown\tx\ty\n")
                   .ok());
}

TEST(KftTraceTest, ActionCodecPreservesTypedArgumentsAndPayload) {
  const Action action{.name_ = "advance timer",
                      .arguments_ = {-4, 0, 27},
                      .payload_ = "a;b%\n"};
  auto decoded = DecodeAction(EncodeAction(action));
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, action);
  EXPECT_FALSE(DecodeAction("missing-fields").ok());
  EXPECT_FALSE(DecodeAction("action;2;1;payload").ok());
}

TEST(ScenarioRunnerTest, SameSeedProducesByteIdenticalTrace) {
  std::unique_ptr<Scenario> first_scenario =
      MakeClusterScenario(Counterexample::kDualAuthority);
  std::unique_ptr<Scenario> second_scenario =
      MakeClusterScenario(Counterexample::kDualAuthority);
  ScenarioRunner runner;
  const RunResult first = runner.Execute(
      ExecuteRequest{.scenario_ = first_scenario.get(), .seed_ = 991});
  const RunResult second = runner.Execute(
      ExecuteRequest{.scenario_ = second_scenario.get(), .seed_ = 991});
  ASSERT_EQ(first.status_, RunStatus::kFinding);
  ASSERT_EQ(second.status_, RunStatus::kFinding);
  EXPECT_EQ(EncodeTrace(first.trace_), EncodeTrace(second.trace_));
}

TEST(ScenarioRunnerTest, MinimizerRemovesIrrelevantTransitions) {
  NoisyFailureScenario scenario;
  ScenarioRunner runner;
  std::optional<RunResult> noisy;
  for (std::uint64_t seed = 0; seed < 100; ++seed) {
    RunResult result = runner.Execute(ExecuteRequest{
        .scenario_ = &scenario,
        .seed_ = seed,
        .limits_ = RunLimits{.transition_budget_ = 32},
    });
    if (result.status_ == RunStatus::kFinding && result.transitions_ > 1) {
      noisy = std::move(result);
      break;
    }
  }
  ASSERT_TRUE(noisy.has_value());
  const RunResult minimized = runner.Minimize(MinimizeRequest{
      .scenario_ = &scenario, .failing_trace_ = &noisy->trace_});
  ASSERT_EQ(minimized.status_, RunStatus::kFinding) << minimized.diagnostic_;
  EXPECT_LT(minimized.transitions_, noisy->transitions_);
  EXPECT_EQ(minimized.transitions_, 1U);
}

class CounterexampleTest : public testing::TestWithParam<
                               std::tuple<Counterexample, std::string_view>> {};

std::vector<std::tuple<Counterexample, std::string_view>>
CounterexampleParameters() {
  std::vector<std::tuple<Counterexample, std::string_view>> result;
  for (const ScenarioDescriptor& descriptor : ClusterScenarioDescriptors()) {
    if (!descriptor.expected_invariant_.empty()) {
      result.emplace_back(descriptor.counterexample_,
                          descriptor.expected_invariant_);
    }
  }
  return result;
}

TEST_P(CounterexampleTest, GeneratesReplaysAndMinimizesStableFinding) {
  const auto [counterexample, invariant] = GetParam();
  std::unique_ptr<Scenario> scenario = MakeClusterScenario(counterexample);
  ScenarioRunner runner;
  const RunResult executed =
      runner.Execute(ExecuteRequest{.scenario_ = scenario.get(), .seed_ = 7});
  ASSERT_EQ(executed.status_, RunStatus::kFinding);
  ASSERT_TRUE(executed.finding_.has_value());
  EXPECT_EQ(executed.finding_->invariant_id_, invariant);

  const RunResult replayed = runner.Replay(
      ReplayRequest{.scenario_ = scenario.get(), .trace_ = &executed.trace_});
  ASSERT_EQ(replayed.status_, RunStatus::kFinding) << replayed.diagnostic_;
  EXPECT_EQ(replayed.finding_, executed.finding_);

  const RunResult minimized = runner.Minimize(MinimizeRequest{
      .scenario_ = scenario.get(), .failing_trace_ = &executed.trace_});
  ASSERT_EQ(minimized.status_, RunStatus::kFinding) << minimized.diagnostic_;
  ASSERT_TRUE(minimized.finding_.has_value());
  EXPECT_EQ(minimized.finding_->fingerprint(),
            executed.finding_->fingerprint());
  EXPECT_LE(minimized.transitions_, executed.transitions_);

  const RunResult minimized_replay = runner.Replay(
      ReplayRequest{.scenario_ = scenario.get(), .trace_ = &minimized.trace_});
  ASSERT_EQ(minimized_replay.status_, RunStatus::kFinding)
      << minimized_replay.diagnostic_;
  EXPECT_EQ(minimized_replay.finding_, minimized.finding_);
}

INSTANTIATE_TEST_SUITE_P(RequiredFaults, CounterexampleTest,
                         testing::ValuesIn(CounterexampleParameters()));

TEST(KftRegressionTest, CheckedInCounterexamplesRemainReplayable) {
  ScenarioRunner runner;
  for (const ScenarioDescriptor& descriptor : ClusterScenarioDescriptors()) {
    if (descriptor.expected_invariant_.empty()) continue;
    const std::string file = std::string(descriptor.cli_name_) + ".kft";
    SCOPED_TRACE(file);
    const std::filesystem::path path =
        std::filesystem::path(KEYLANE_SOURCE_DIR) / "tests" / "cluster" /
        "regressions" / file;
    auto trace = ReadTrace(path);
    ASSERT_TRUE(trace.ok()) << trace.status();
    std::unique_ptr<Scenario> scenario =
        MakeClusterScenario(descriptor.counterexample_);
    const RunResult result = runner.Replay(
        ReplayRequest{.scenario_ = scenario.get(), .trace_ = &*trace});
    ASSERT_EQ(result.status_, RunStatus::kFinding) << result.diagnostic_;
    ASSERT_TRUE(result.finding_.has_value());
    EXPECT_EQ(result.finding_->invariant_id_, descriptor.expected_invariant_);
  }
}

TEST(ScenarioRunnerTest, GoodScenarioExploresAndReplaysDistinctSchedules) {
  ScenarioRunner runner;
  std::set<std::string> action_sequences;
  for (std::uint64_t seed = 0; seed < 32; ++seed) {
    std::unique_ptr<Scenario> scenario =
        MakeClusterScenario(Counterexample::kNone);
    const RunResult executed = runner.Execute(
        ExecuteRequest{.scenario_ = scenario.get(), .seed_ = seed});
    ASSERT_EQ(executed.status_, RunStatus::kQuiescent) << executed.diagnostic_;

    std::string actions;
    bool saw_fault_acknowledgment = false;
    for (const TraceRecord& record : executed.trace_.records_) {
      if (record.kind_ == TraceRecordKind::kChoice) {
        actions.append(record.payload_).push_back('\n');
      }
      if (record.kind_ == TraceRecordKind::kAcknowledgment &&
          record.name_ == "fault") {
        saw_fault_acknowledgment = true;
      }
    }
    EXPECT_TRUE(saw_fault_acknowledgment);
    action_sequences.insert(std::move(actions));

    const RunResult replayed = runner.Replay(
        ReplayRequest{.scenario_ = scenario.get(), .trace_ = &executed.trace_});
    ASSERT_EQ(replayed.status_, RunStatus::kQuiescent) << replayed.diagnostic_;
    EXPECT_EQ(replayed.trace_, executed.trace_);
  }
  EXPECT_GT(action_sequences.size(), 1U);
}

TEST(ScenarioRunnerTest, RejectsScenarioAndObservationDrift) {
  std::unique_ptr<Scenario> scenario =
      MakeClusterScenario(Counterexample::kDualAuthority);
  ScenarioRunner runner;
  const RunResult executed =
      runner.Execute(ExecuteRequest{.scenario_ = scenario.get(), .seed_ = 1});
  ASSERT_EQ(executed.status_, RunStatus::kFinding);

  Trace wrong_schema = executed.trace_;
  ++wrong_schema.schema_;
  EXPECT_EQ(runner
                .Replay(ReplayRequest{.scenario_ = scenario.get(),
                                      .trace_ = &wrong_schema})
                .status_,
            RunStatus::kTraceDrift);

  Trace observation_drift = executed.trace_;
  ASSERT_GE(observation_drift.records_.size(), 4U);
  observation_drift.records_[3].payload_ += "-drift";
  EXPECT_EQ(runner
                .Replay(ReplayRequest{.scenario_ = scenario.get(),
                                      .trace_ = &observation_drift})
                .status_,
            RunStatus::kTraceDrift);

  Trace initial_hash_drift = executed.trace_;
  initial_hash_drift.initial_state_hash_ = "wrong";
  EXPECT_EQ(runner
                .Replay(ReplayRequest{.scenario_ = scenario.get(),
                                      .trace_ = &initial_hash_drift})
                .status_,
            RunStatus::kTraceDrift);

  Trace malformed_action = executed.trace_;
  const auto choice = std::find_if(
      malformed_action.records_.begin(), malformed_action.records_.end(),
      [](const TraceRecord& record) {
        return record.kind_ == TraceRecordKind::kChoice;
      });
  ASSERT_NE(choice, malformed_action.records_.end());
  choice->payload_ = "not-an-action";
  EXPECT_EQ(runner
                .Replay(ReplayRequest{.scenario_ = scenario.get(),
                                      .trace_ = &malformed_action})
                .status_,
            RunStatus::kTraceDrift);
}

TEST(ClusterInvariantTest, AcceptsGoodReferenceSnapshot) {
  ClusterSnapshot snapshot;
  snapshot.authorities_.push_back(AuthorityObservation{
      .group_ = GroupId{1},
      .node_ = NodeId{1},
      .boot_ = BootId{1},
      .can_admit_write_ = true,
      .can_decide_success_ = true,
  });
  snapshot.promotion_ = PromotionObservation{
      .candidate_selected_ = true,
      .durability_barrier_complete_ = true,
      .promotion_base_committed_ = true,
      .population_token_valid_ = true,
      .captured_catalog_generation_ = CatalogGeneration{10},
      .current_catalog_generation_ = CatalogGeneration{10},
      .child_history_ready_ = true,
      .candidate_activated_ = true,
      .write_gate_open_ = true,
  };
  snapshot.resume_ = ResumeObservation{
      .source_boot_ = BootId{2},
      .target_boot_ = BootId{3},
      .evidence_source_boot_ = BootId{2},
      .evidence_target_boot_ = BootId{3},
      .partial_resume_selected_ = true,
  };
  snapshot.population_ = PopulationObservation{};
  snapshot.meta_ = MetaObservation{
      .committed_topology_ = TopologyEpoch{4},
      .installed_topology_ = TopologyEpoch{4},
      .committed_term_ = GroupTerm{5},
      .installed_term_ = GroupTerm{5},
      .current_operation_ = OperationId{6},
      .directive_operation_ = OperationId{6},
      .current_boot_ = BootId{7},
      .evidence_boot_ = BootId{7},
      .serving_state_published_ = true,
      .directive_applied_ = true,
  };
  snapshot.migration_ = MigrationObservation{
      .source_serves_ = false,
      .target_serves_ = true,
      .ownership_committed_ = true,
      .target_population_complete_ = true,
  };
  snapshot.client_history_.push_back(ClientOperationObservation{
      .operation_ = OperationId{8},
      .group_ = GroupId{1},
      .authority_ = NodeId{1},
      .authority_boot_ = BootId{1},
      .authority_term_ = GroupTerm{1},
      .authority_grant_ = GrantId{1},
      .mutates_ = true,
      .admitted_ = true,
      .admitted_with_valid_authority_ = true,
      .mutation_durable_ = true,
      .authority_valid_at_success_decision_ = false,
      .outcome_ = ClientOutcome::kNotReturned,
  });
  snapshot.client_history_.push_back(ClientOperationObservation{
      .operation_ = OperationId{9},
      .group_ = GroupId{1},
      .authority_ = NodeId{2},
      .authority_boot_ = BootId{1},
      .authority_term_ = GroupTerm{2},
      .authority_grant_ = GrantId{2},
      .mutates_ = true,
      .outcome_ = ClientOutcome::kFailure,
  });
  snapshot.server_control_reply_ = "-CLUSTERDOWN no safe owner";
  EXPECT_FALSE(CheckClusterInvariants(snapshot).has_value());
  snapshot.server_control_reply_ = "-MOVED 42 127.0.0.1:6379";
  EXPECT_FALSE(CheckClusterInvariants(snapshot).has_value());
}

TEST(ClusterInvariantTest, CoversRemainingMatrixRows) {
  auto expect = [](ClusterSnapshot snapshot, std::string_view invariant) {
    const std::optional<Finding> finding = CheckClusterInvariants(snapshot);
    ASSERT_TRUE(finding.has_value());
    EXPECT_EQ(finding->invariant_id_, invariant);
  };

  ClusterSnapshot promotion;
  promotion.promotion_.write_gate_open_ = true;
  expect(promotion, "promotion.durable-before-write-authority");

  ClusterSnapshot child_history;
  child_history.promotion_.durability_barrier_complete_ = true;
  child_history.promotion_.write_gate_open_ = true;
  expect(child_history, "history.child-ready-before-write");

  ClusterSnapshot reset;
  reset.promotion_.another_replica_reset_ = true;
  expect(reset, "promotion.keep-replicas-until-activation");

  ClusterSnapshot incomplete_activation;
  incomplete_activation.promotion_.candidate_selected_ = true;
  incomplete_activation.promotion_.candidate_activated_ = true;
  incomplete_activation.promotion_.another_replica_reset_ = true;
  expect(incomplete_activation, "promotion.safe-activation");

  ClusterSnapshot catalog_ack;
  catalog_ack.function_catalog_.applied_cursor_advanced_ = true;
  catalog_ack.function_catalog_.replica_ack_sent_ = true;
  expect(catalog_ack, "function.catalog-durable-before-ack");

  ClusterSnapshot full_sync;
  full_sync.full_sync_.in_progress_ = true;
  expect(full_sync, "fullsync.destructive-invalidation-before-transfer");

  ClusterSnapshot stale_catalog;
  stale_catalog.promotion_.candidate_selected_ = true;
  stale_catalog.promotion_.durability_barrier_complete_ = true;
  stale_catalog.promotion_.promotion_base_committed_ = true;
  stale_catalog.promotion_.population_token_valid_ = true;
  stale_catalog.promotion_.captured_catalog_generation_ = CatalogGeneration{1};
  stale_catalog.promotion_.current_catalog_generation_ = CatalogGeneration{2};
  stale_catalog.promotion_.child_history_ready_ = true;
  stale_catalog.promotion_.candidate_activated_ = true;
  expect(stale_catalog, "promotion.catalog-token-current");

  ClusterSnapshot candidate;
  const CompatibilityDomain domain{.source_boot_ = BootId{1},
                                   .history_ = HistoryId{1},
                                   .manifest_ = ManifestId{1},
                                   .partition_epoch_ = PartitionEpoch{1}};
  candidate.candidate_ = CandidateObservation{
      .candidate_ = AppliedVector{.domain_ = domain,
                                  .cursors_ = {{FlowId{1}, 7}, {FlowId{2}, 3}}},
      .peer_ = AppliedVector{.domain_ = domain,
                             .cursors_ = {{FlowId{1}, 6}, {FlowId{2}, 4}}},
      .candidate_claimed_not_behind_ = true,
  };
  expect(candidate, "candidate.componentwise-applied-order");

  ClusterSnapshot meta;
  meta.meta_.committed_topology_ = TopologyEpoch{3};
  meta.meta_.installed_topology_ = TopologyEpoch{2};
  meta.meta_.serving_state_published_ = true;
  expect(meta, "meta.committed-state-monotonic");

  ClusterSnapshot directive;
  directive.meta_.current_operation_ = OperationId{2};
  directive.meta_.directive_operation_ = OperationId{1};
  directive.meta_.directive_applied_ = true;
  expect(directive, "meta.directive-evidence-scoped");

  ClusterSnapshot migration;
  migration.migration_.target_serves_ = true;
  expect(migration, "migration.single-owner");

  ClusterSnapshot wire;
  wire.server_control_reply_ = "-FAILED grant=9";
  expect(wire, "redis.compatible-control-error");

  for (const std::string_view private_field :
       {"term=7", "term 7", "grant:9", "failed=node-1", "retry unsafe"}) {
    ClusterSnapshot private_wire;
    private_wire.server_control_reply_ =
        std::string("-TRYAGAIN ") + std::string(private_field);
    expect(private_wire, "redis.private-control-state-hidden");
  }
  for (const std::string_view malformed_redirect :
       {"-MOVED", "-MOVED x 127.0.0.1:6379", "-ASK 1 missing-port",
        "-ASK 1 host:6379 extra", "-MOVED 16384 host:6379",
        "-MOVED 1 host:65536", "-MOVED 1 host:not-a-port",
        "-MOVED 1 host:grant=9", "-MOVED 1 grant=9:6379",
        "-MOVED 999999 host:not-a-port"}) {
    ClusterSnapshot wire;
    wire.server_control_reply_ = malformed_redirect;
    expect(wire, "redis.private-control-state-hidden");
  }

  ClusterSnapshot unsafe_success;
  unsafe_success.client_history_.push_back(ClientOperationObservation{
      .operation_ = OperationId{9},
      .group_ = GroupId{1},
      .authority_ = NodeId{1},
      .authority_boot_ = BootId{1},
      .authority_term_ = GroupTerm{1},
      .authority_grant_ = GrantId{1},
      .mutates_ = true,
      .admitted_ = true,
      .admitted_with_valid_authority_ = true,
      .mutation_durable_ = true,
      .authority_valid_at_success_decision_ = false,
      .outcome_ = ClientOutcome::kSuccess,
  });
  expect(unsafe_success, "client.safe-success-decision");
}

TEST(ClusterInvariantTest, DistinguishesAuthorityIncarnationsOnSameNode) {
  ClusterSnapshot snapshot;
  snapshot.authorities_ = {
      AuthorityObservation{.group_ = GroupId{1},
                           .node_ = NodeId{7},
                           .boot_ = BootId{1},
                           .term_ = GroupTerm{3},
                           .grant_ = GrantId{10},
                           .can_complete_inflight_ = true},
      AuthorityObservation{.group_ = GroupId{1},
                           .node_ = NodeId{7},
                           .boot_ = BootId{2},
                           .term_ = GroupTerm{4},
                           .grant_ = GrantId{11},
                           .can_admit_write_ = true},
  };
  const std::optional<Finding> finding = CheckClusterInvariants(snapshot);
  ASSERT_TRUE(finding.has_value());
  EXPECT_EQ(finding->invariant_id_, "authority.single-writer");

  snapshot.authorities_.erase(snapshot.authorities_.begin());
  snapshot.client_history_ = {
      ClientOperationObservation{.operation_ = OperationId{1},
                                 .group_ = GroupId{1},
                                 .authority_ = NodeId{7},
                                 .authority_boot_ = BootId{1},
                                 .authority_term_ = GroupTerm{3},
                                 .authority_grant_ = GrantId{10}},
      ClientOperationObservation{.operation_ = OperationId{1},
                                 .group_ = GroupId{1},
                                 .authority_ = NodeId{7},
                                 .authority_boot_ = BootId{2},
                                 .authority_term_ = GroupTerm{4},
                                 .authority_grant_ = GrantId{11}},
  };
  const std::optional<Finding> client_finding =
      CheckClusterInvariants(snapshot);
  ASSERT_TRUE(client_finding.has_value());
  EXPECT_EQ(client_finding->invariant_id_, "client.operation-single-authority");
}

TEST(AppliedVectorTest, UsesCompatibilityAndComponentwisePartialOrder) {
  const CompatibilityDomain first_domain{
      .source_boot_ = BootId{1},
      .history_ = HistoryId{2},
      .manifest_ = ManifestId{3},
      .partition_epoch_ = PartitionEpoch{4},
  };
  AppliedVector left{.domain_ = first_domain,
                     .cursors_ = {{FlowId{1}, 8}, {FlowId{2}, 3}}};
  AppliedVector right{.domain_ = first_domain,
                      .cursors_ = {{FlowId{1}, 7}, {FlowId{2}, 4}}};
  EXPECT_EQ(CompareAppliedVectors(left, right), VectorOrder::kIncomparable);
  right.cursors_[FlowId{2}] = 3;
  EXPECT_EQ(CompareAppliedVectors(left, right), VectorOrder::kGreater);
  right.domain_.history_ = HistoryId{99};
  EXPECT_EQ(CompareAppliedVectors(left, right), VectorOrder::kIncompatible);
}

}  // namespace
}  // namespace keylane::test::cluster
