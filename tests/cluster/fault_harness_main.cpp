#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tests/cluster/fault_harness.h"
#include "tests/cluster/reference_model.h"

namespace keylane::test::cluster {
namespace {

struct Options {
  std::string scenario_;
  std::uint64_t seed_ = 0;
  std::size_t steps_ = 256;
  std::optional<std::filesystem::path> replay_;
  std::optional<std::filesystem::path> minimize_;
  std::optional<std::filesystem::path> trace_out_;
  std::optional<std::string> expect_finding_;
  std::uint64_t soak_seconds_ = 0;
};

template <typename Integer>
bool ParseInteger(std::string_view text, Integer* output) {
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), *output);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

void Usage(std::ostream& output) {
  output << "usage: keylane_cluster_fault [options]\n"
            "  --scenario good|dual-authority|stale-evidence|history-gap|"
            "partial-activation|stale-directive|catalog-ack-before-durable|"
            "fullsync-retains-old-state|stale-catalog-promotion\n"
            "  --seed N --steps N [--trace-out PATH] [--expect-finding ID]\n"
            "  --replay PATH [--scenario NAME] [--expect-finding ID]\n"
            "  --minimize PATH [--scenario NAME] --trace-out PATH\n"
            "  --soak-seconds N [--trace-out DIRECTORY]\n";
}

std::optional<Options> ParseOptions(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view argument = argv[i];
    if (argument == "--help") {
      Usage(std::cout);
      return std::nullopt;
    }
    if (i + 1 >= argc) {
      std::cerr << "missing value for " << argument << '\n';
      return std::nullopt;
    }
    const std::string_view value = argv[++i];
    if (argument == "--scenario") {
      options.scenario_ = value;
    } else if (argument == "--seed") {
      if (!ParseInteger(value, &options.seed_)) return std::nullopt;
    } else if (argument == "--steps") {
      if (!ParseInteger(value, &options.steps_)) return std::nullopt;
    } else if (argument == "--replay") {
      options.replay_ = value;
    } else if (argument == "--minimize") {
      options.minimize_ = value;
    } else if (argument == "--trace-out") {
      options.trace_out_ = value;
    } else if (argument == "--expect-finding") {
      options.expect_finding_ = value;
    } else if (argument == "--soak-seconds") {
      if (!ParseInteger(value, &options.soak_seconds_)) return std::nullopt;
    } else {
      std::cerr << "unknown option: " << argument << '\n';
      return std::nullopt;
    }
  }
  if (options.replay_.has_value() && options.minimize_.has_value()) {
    std::cerr << "--replay and --minimize are mutually exclusive\n";
    return std::nullopt;
  }
  return options;
}

std::optional<Counterexample> ParseScenario(std::string_view name) {
  const ScenarioDescriptor* descriptor = FindClusterScenario(name);
  return descriptor == nullptr
             ? std::nullopt
             : std::optional<Counterexample>{descriptor->counterexample_};
}

std::string_view StatusName(RunStatus status) {
  switch (status) {
    case RunStatus::kQuiescent:
      return "quiescent";
    case RunStatus::kStepBudgetReached:
      return "step-budget-reached";
    case RunStatus::kFinding:
      return "finding";
    case RunStatus::kDeadlock:
      return "deadlock";
    case RunStatus::kTraceDrift:
      return "trace-drift";
    case RunStatus::kResourceExhausted:
      return "resource-exhausted";
  }
  return "unknown";
}

int Report(const RunResult& result,
           const std::optional<std::string>& expected_finding) {
  std::cout << "status=" << StatusName(result.status_)
            << " transitions=" << result.transitions_ << '\n';
  if (result.finding_.has_value()) {
    std::cout << "finding=" << result.finding_->invariant_id_ << '\n'
              << "witness=" << result.finding_->witness_ << '\n';
  }
  if (!result.diagnostic_.empty()) {
    std::cout << "diagnostic=" << result.diagnostic_ << '\n';
  }
  if (expected_finding.has_value()) {
    return result.status_ == RunStatus::kFinding &&
                   result.finding_.has_value() &&
                   result.finding_->invariant_id_ == *expected_finding
               ? 0
               : 1;
  }
  return result.status_ == RunStatus::kQuiescent ? 0 : 1;
}

int RunSoak(const Options& options) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(options.soak_seconds_);
  ScenarioRunner runner;
  std::uint64_t iteration = 0;
  const std::span<const ScenarioDescriptor> scenarios =
      ClusterScenarioDescriptors();
  do {
    const ScenarioDescriptor& descriptor =
        scenarios[iteration % scenarios.size()];
    std::unique_ptr<Scenario> scenario =
        MakeClusterScenario(descriptor.counterexample_);
    const RunResult result = runner.Execute(ExecuteRequest{
        .scenario_ = scenario.get(),
        .seed_ = options.seed_ + iteration,
        .limits_ = RunLimits{.transition_budget_ = options.steps_},
    });
    const bool valid = descriptor.expected_invariant_.empty()
                           ? result.status_ == RunStatus::kQuiescent
                           : result.status_ == RunStatus::kFinding &&
                                 result.finding_.has_value() &&
                                 result.finding_->invariant_id_ ==
                                     descriptor.expected_invariant_;
    if (!valid) {
      const std::filesystem::path directory =
          options.trace_out_.value_or("cluster-fault-artifacts");
      const std::filesystem::path trace_path =
          directory / ("soak-failure-" + std::to_string(iteration) + ".kft");
      const absl::Status written =
          WriteTraceAtomically(result.trace_, trace_path);
      std::cerr << "soak failed at iteration " << iteration
                << "; trace=" << trace_path << "; write=" << written << '\n';
      return 1;
    }
    ++iteration;
  } while (std::chrono::steady_clock::now() < deadline);
  std::cout << "validated " << iteration << " deterministic schedules\n";
  return 0;
}

}  // namespace
}  // namespace keylane::test::cluster

int main(int argc, char** argv) {
  using namespace keylane::test::cluster;
  const std::optional<Options> parsed = ParseOptions(argc, argv);
  if (!parsed.has_value()) {
    return argc > 1 && std::string_view(argv[1]) == "--help" ? 0 : 2;
  }
  const Options& options = *parsed;
  if (options.soak_seconds_ != 0) return RunSoak(options);

  ScenarioRunner runner;
  std::optional<Trace> input_trace;
  const std::optional<std::filesystem::path> input_path =
      options.replay_.has_value() ? options.replay_ : options.minimize_;
  if (input_path.has_value()) {
    auto read = ReadTrace(*input_path);
    if (!read.ok()) {
      std::cerr << read.status() << '\n';
      return 2;
    }
    input_trace = std::move(*read);
  }

  const std::string_view scenario_name = options.scenario_.empty()
                                             ? input_trace.has_value()
                                                   ? input_trace->scenario_
                                                   : std::string_view{}
                                             : options.scenario_;
  const std::optional<Counterexample> counterexample =
      ParseScenario(scenario_name);
  if (!counterexample.has_value()) {
    std::cerr << "a known --scenario is required\n";
    Usage(std::cerr);
    return 2;
  }
  std::unique_ptr<Scenario> scenario = MakeClusterScenario(*counterexample);
  const RunLimits limits{.transition_budget_ = options.steps_};
  RunResult result;
  if (options.minimize_.has_value()) {
    if (!options.trace_out_.has_value()) {
      std::cerr << "--minimize requires --trace-out\n";
      return 2;
    }
    result = runner.Minimize(MinimizeRequest{
        .scenario_ = scenario.get(),
        .failing_trace_ = &*input_trace,
        .limits_ = limits,
        .trace_path_ = options.trace_out_,
    });
  } else if (options.replay_.has_value()) {
    result = runner.Replay(ReplayRequest{.scenario_ = scenario.get(),
                                         .trace_ = &*input_trace,
                                         .limits_ = limits});
  } else {
    result = runner.Execute(ExecuteRequest{
        .scenario_ = scenario.get(),
        .seed_ = options.seed_,
        .limits_ = limits,
        .trace_path_ = options.trace_out_,
    });
  }
  return Report(result, options.expect_finding_);
}
