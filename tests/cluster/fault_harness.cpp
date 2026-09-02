#include "tests/cluster/fault_harness.h"

#include <algorithm>
#include <array>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

namespace keylane::test::cluster {
namespace {

char HashHexDigit(unsigned value) {
  return value < 10 ? static_cast<char>('0' + value)
                    : static_cast<char>('A' + value - 10);
}

std::string StableHash(std::string_view input) {
  std::uint64_t value = 14695981039346656037ULL;
  for (const unsigned char byte : input) {
    value ^= byte;
    value *= 1099511628211ULL;
  }
  std::array<char, 16> encoded{};
  for (std::size_t i = 0; i < encoded.size(); ++i) {
    const unsigned shift = static_cast<unsigned>((encoded.size() - i - 1) * 4);
    encoded[i] = HashHexDigit(static_cast<unsigned>((value >> shift) & 0xf));
  }
  return std::string(encoded.data(), encoded.size());
}

std::string CanonicalActionKey(const Action& action) {
  return EncodeAction(action);
}

bool ContainsAction(const std::vector<Action>& actions, const Action& target) {
  return std::find(actions.begin(), actions.end(), target) != actions.end();
}

std::optional<Finding> TraceFinding(const Trace& trace) {
  for (const TraceRecord& record : trace.records_) {
    if (record.kind_ == TraceRecordKind::kFinding) {
      return Finding{.invariant_id_ = record.name_,
                     .witness_ = record.payload_};
    }
  }
  return std::nullopt;
}

absl::StatusOr<std::vector<Action>> TraceActions(const Trace& trace) {
  std::vector<Action> actions;
  for (const TraceRecord& record : trace.records_) {
    if (record.kind_ != TraceRecordKind::kChoice) continue;
    auto decoded = DecodeAction(record.payload_);
    if (!decoded.ok()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "invalid choice action in KFT1 trace: ", decoded.status()));
    }
    actions.push_back(std::move(*decoded));
  }
  return actions;
}

RunResult RunActions(const Scenario& scenario, std::uint64_t seed,
                     const RunLimits& limits,
                     const std::vector<Action>& actions, bool verify_expected,
                     const Trace* expected_trace = nullptr) {
  RunResult result;
  result.trace_.mode_ = TraceMode::kExactModel;
  result.trace_.scenario_ = std::string(scenario.name());
  result.trace_.schema_ = scenario.schema();
  result.trace_.seed_ = seed;

  std::unique_ptr<ScenarioWorld> world = scenario.NewWorld();
  if (world == nullptr) {
    result.status_ = RunStatus::kTraceDrift;
    result.diagnostic_ = "scenario returned no world";
    return result;
  }
  const std::string initial = world->Observe();
  result.trace_.initial_state_hash_ = StableHash(initial);
  result.trace_.records_.push_back(TraceRecord{
      .logical_time_ = 0,
      .kind_ = TraceRecordKind::kObservation,
      .name_ = "world",
      .payload_ = initial,
  });

  std::size_t expected_index = 0;
  if (verify_expected) {
    if (expected_trace == nullptr || expected_trace->records_.empty()) {
      result.status_ = RunStatus::kTraceDrift;
      result.diagnostic_ = "exact replay has no initial observation";
      return result;
    }
    if (expected_trace->initial_state_hash_ !=
        result.trace_.initial_state_hash_) {
      result.status_ = RunStatus::kTraceDrift;
      result.diagnostic_ = "initial state hash drift";
      return result;
    }
    if (expected_trace->records_.front() != result.trace_.records_.front()) {
      result.status_ = RunStatus::kTraceDrift;
      result.diagnostic_ = "initial observation drift";
      return result;
    }
    expected_index = 1;
  }

  if (const auto initial_finding = world->CheckInvariants();
      initial_finding.has_value()) {
    result.finding_ = initial_finding;
    result.status_ = RunStatus::kFinding;
    result.trace_.records_.push_back(TraceRecord{
        .logical_time_ = 0,
        .kind_ = TraceRecordKind::kFinding,
        .name_ = initial_finding->invariant_id_,
        .payload_ = initial_finding->witness_,
    });
    if (verify_expected && result.trace_.records_ != expected_trace->records_) {
      result.status_ = RunStatus::kTraceDrift;
      result.finding_.reset();
      result.diagnostic_ = "initial finding drift";
    }
    return result;
  }

  for (const Action& action : actions) {
    if (result.transitions_ >= limits.transition_budget_) {
      result.status_ = RunStatus::kStepBudgetReached;
      return result;
    }
    std::vector<Action> enabled = world->EnabledActions();
    if (enabled.size() > limits.max_enabled_actions_) {
      result.status_ = RunStatus::kResourceExhausted;
      result.diagnostic_ = "enabled action limit exceeded";
      return result;
    }
    if (!ContainsAction(enabled, action)) {
      result.status_ = RunStatus::kTraceDrift;
      result.diagnostic_ =
          absl::StrCat("action is not enabled: ", action.name_);
      return result;
    }

    const std::uint64_t logical_time = result.transitions_ + 1;
    const std::size_t actual_begin = result.trace_.records_.size();
    const TraceRecord choice{.logical_time_ = logical_time,
                             .kind_ = TraceRecordKind::kChoice,
                             .name_ = "action",
                             .payload_ = EncodeAction(action)};
    result.trace_.records_.push_back(choice);
    absl::Status applied = world->Apply(action);
    if (!applied.ok()) {
      result.status_ = RunStatus::kTraceDrift;
      result.diagnostic_ = applied.ToString();
      return result;
    }
    const TraceRecord ack{.logical_time_ = logical_time,
                          .kind_ = TraceRecordKind::kAcknowledgment,
                          .name_ = action.name_,
                          .payload_ = "applied"};
    result.trace_.records_.push_back(ack);
    for (std::string& domain_ack : world->DrainAcknowledgments()) {
      result.trace_.records_.push_back(TraceRecord{
          .logical_time_ = logical_time,
          .kind_ = TraceRecordKind::kAcknowledgment,
          .name_ = "fault",
          .payload_ = std::move(domain_ack),
      });
    }
    const TraceRecord observation{.logical_time_ = logical_time,
                                  .kind_ = TraceRecordKind::kObservation,
                                  .name_ = "world",
                                  .payload_ = world->Observe()};
    result.trace_.records_.push_back(observation);
    ++result.transitions_;

    std::optional<Finding> finding = world->CheckInvariants();
    if (finding.has_value()) {
      result.finding_ = finding;
      result.trace_.records_.push_back(TraceRecord{
          .logical_time_ = logical_time,
          .kind_ = TraceRecordKind::kFinding,
          .name_ = finding->invariant_id_,
          .payload_ = finding->witness_,
      });
      result.status_ = RunStatus::kFinding;
    }

    if (verify_expected) {
      const std::size_t records_this_step =
          result.trace_.records_.size() - actual_begin;
      if (expected_index + records_this_step >
          expected_trace->records_.size()) {
        result.status_ = RunStatus::kTraceDrift;
        result.finding_.reset();
        result.diagnostic_ = "expected trace ended during a transition";
        return result;
      }
      for (std::size_t offset = 0; offset < records_this_step; ++offset) {
        if (result.trace_.records_[actual_begin + offset] !=
            expected_trace->records_[expected_index + offset]) {
          result.status_ = RunStatus::kTraceDrift;
          result.finding_.reset();
          result.diagnostic_ = absl::StrCat(
              "canonical observation drift at transition ", logical_time);
          return result;
        }
      }
      expected_index += records_this_step;
    }

    if (EncodeTrace(result.trace_).size() > limits.max_trace_bytes_) {
      result.status_ = RunStatus::kResourceExhausted;
      result.finding_.reset();
      result.diagnostic_ = "trace byte limit exceeded";
      return result;
    }
    if (finding.has_value()) break;
  }

  if (verify_expected && expected_index != expected_trace->records_.size()) {
    result.status_ = RunStatus::kTraceDrift;
    result.finding_.reset();
    result.diagnostic_ = "expected trace has trailing records";
    return result;
  }
  if (result.status_ == RunStatus::kFinding) return result;
  if (world->EnabledActions().empty()) {
    result.status_ =
        world->HasPendingWork() ? RunStatus::kDeadlock : RunStatus::kQuiescent;
  } else {
    result.status_ = RunStatus::kStepBudgetReached;
  }
  return result;
}

RunResult InvalidRequest(std::string diagnostic) {
  RunResult result;
  result.status_ = RunStatus::kTraceDrift;
  result.diagnostic_ = std::move(diagnostic);
  return result;
}

}  // namespace

std::string Finding::fingerprint() const {
  return invariant_id_ + ":" + witness_;
}

std::vector<std::string> ScenarioWorld::DrainAcknowledgments() { return {}; }

std::vector<Action> Scenario::Simplify(const Action&) const { return {}; }

std::uint64_t StableRandom::Bounded(std::uint64_t exclusive_upper_bound) {
  if (exclusive_upper_bound == 0) return 0;
  const std::uint64_t threshold =
      (std::uint64_t{0} - exclusive_upper_bound) % exclusive_upper_bound;
  for (;;) {
    const std::uint64_t value = engine_();
    if (value >= threshold) return value % exclusive_upper_bound;
  }
}

RunResult ScenarioRunner::Execute(const ExecuteRequest& request) const {
  if (request.scenario_ == nullptr) return InvalidRequest("missing scenario");
  StableRandom random(request.seed_);
  std::unique_ptr<ScenarioWorld> selection_world =
      request.scenario_->NewWorld();
  if (selection_world == nullptr) {
    return InvalidRequest("scenario returned no world");
  }
  std::vector<Action> actions;
  actions.reserve(request.limits_.transition_budget_);
  for (std::size_t step = 0; step < request.limits_.transition_budget_;
       ++step) {
    std::vector<Action> enabled = selection_world->EnabledActions();
    if (enabled.empty()) break;
    if (enabled.size() > request.limits_.max_enabled_actions_) {
      RunResult result;
      result.status_ = RunStatus::kResourceExhausted;
      result.diagnostic_ = "enabled action limit exceeded";
      return result;
    }
    std::sort(enabled.begin(), enabled.end(),
              [](const Action& left, const Action& right) {
                return CanonicalActionKey(left) < CanonicalActionKey(right);
              });
    const Action& selected = enabled[random.Bounded(enabled.size())];
    actions.push_back(selected);
    absl::Status applied = selection_world->Apply(selected);
    if (!applied.ok()) return InvalidRequest(applied.ToString());
    if (selection_world->CheckInvariants().has_value()) break;
  }

  RunResult result = RunActions(*request.scenario_, request.seed_,
                                request.limits_, actions, false);
  if (request.trace_path_.has_value()) {
    absl::Status written =
        WriteTraceAtomically(result.trace_, *request.trace_path_);
    if (!written.ok()) {
      result.status_ = RunStatus::kResourceExhausted;
      result.diagnostic_ = written.ToString();
    }
  }
  return result;
}

RunResult ScenarioRunner::Replay(const ReplayRequest& request) const {
  if (request.scenario_ == nullptr || request.trace_ == nullptr) {
    return InvalidRequest("missing replay scenario or trace");
  }
  if (request.trace_->mode_ != TraceMode::kExactModel) {
    return InvalidRequest("exact replay requires an exact-model trace");
  }
  if (request.trace_->scenario_ != request.scenario_->name() ||
      request.trace_->schema_ != request.scenario_->schema()) {
    return InvalidRequest("trace scenario or schema mismatch");
  }
  auto actions = TraceActions(*request.trace_);
  if (!actions.ok()) return InvalidRequest(actions.status().ToString());
  return RunActions(*request.scenario_, request.trace_->seed_, request.limits_,
                    *actions, true, request.trace_);
}

RunResult ScenarioRunner::Minimize(const MinimizeRequest& request) const {
  if (request.scenario_ == nullptr || request.failing_trace_ == nullptr) {
    return InvalidRequest("missing minimize scenario or trace");
  }
  if (request.failing_trace_->mode_ != TraceMode::kExactModel ||
      request.failing_trace_->scenario_ != request.scenario_->name() ||
      request.failing_trace_->schema_ != request.scenario_->schema()) {
    return InvalidRequest(
        "minimize requires a matching exact-model scenario and schema");
  }
  const std::optional<Finding> expected = TraceFinding(*request.failing_trace_);
  if (!expected.has_value()) {
    return InvalidRequest("minimize requires a failing trace");
  }
  auto trace_actions = TraceActions(*request.failing_trace_);
  if (!trace_actions.ok()) {
    return InvalidRequest(trace_actions.status().ToString());
  }
  std::vector<Action> current = std::move(*trace_actions);
  std::size_t attempts = 0;
  auto preserves_failure = [&](const std::vector<Action>& candidate,
                               RunResult* candidate_result) {
    if (attempts >= request.limits_.minimize_attempt_budget_) return false;
    ++attempts;
    *candidate_result =
        RunActions(*request.scenario_, request.failing_trace_->seed_,
                   request.limits_, candidate, false);
    return candidate_result->status_ == RunStatus::kFinding &&
           candidate_result->finding_.has_value() &&
           candidate_result->finding_->fingerprint() == expected->fingerprint();
  };

  std::size_t granularity = 2;
  RunResult best = RunActions(*request.scenario_, request.failing_trace_->seed_,
                              request.limits_, current, false);
  if (best.status_ != RunStatus::kFinding || !best.finding_.has_value() ||
      best.finding_->fingerprint() != expected->fingerprint()) {
    return InvalidRequest("failing trace does not reproduce its finding");
  }
  while (current.size() >= 2 &&
         attempts < request.limits_.minimize_attempt_budget_) {
    const std::size_t chunk = (current.size() + granularity - 1) / granularity;
    bool reduced = false;
    for (std::size_t begin = 0; begin < current.size(); begin += chunk) {
      std::vector<Action> candidate;
      candidate.reserve(current.size() -
                        std::min(chunk, current.size() - begin));
      candidate.insert(candidate.end(), current.begin(),
                       current.begin() + begin);
      candidate.insert(
          candidate.end(),
          current.begin() + std::min(current.size(), begin + chunk),
          current.end());
      RunResult candidate_result;
      if (preserves_failure(candidate, &candidate_result)) {
        current = std::move(candidate);
        best = std::move(candidate_result);
        granularity = std::max<std::size_t>(2, granularity - 1);
        reduced = true;
        break;
      }
    }
    if (reduced) continue;
    if (granularity >= current.size()) break;
    granularity = std::min(current.size(), granularity * 2);
  }

  for (std::size_t index = 0;
       index < current.size() &&
       attempts < request.limits_.minimize_attempt_budget_;
       ++index) {
    for (const Action& simplified :
         request.scenario_->Simplify(current[index])) {
      std::vector<Action> candidate = current;
      candidate[index] = simplified;
      RunResult candidate_result;
      if (preserves_failure(candidate, &candidate_result)) {
        current = std::move(candidate);
        best = std::move(candidate_result);
        break;
      }
    }
  }

  if (request.trace_path_.has_value()) {
    absl::Status written =
        WriteTraceAtomically(best.trace_, *request.trace_path_);
    if (!written.ok()) {
      best.status_ = RunStatus::kResourceExhausted;
      best.diagnostic_ = written.ToString();
    }
  }
  return best;
}

}  // namespace keylane::test::cluster
