#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace keylane::test::cluster {

// Exact-model traces are replayed byte-for-byte. Process action scripts retain
// intended external actions but permit observation drift from OS scheduling.
enum class TraceMode : std::uint8_t {
  kExactModel,
  kProcessActionScript,
};

// Every transition records its selected action, generic/domain acknowledgments,
// resulting observation, and optional invariant finding in this order.
enum class TraceRecordKind : std::uint8_t {
  kChoice,
  kAcknowledgment,
  kObservation,
  kFinding,
};

// Action is the durable scenario boundary: name and typed integer arguments
// identify a transition; payload holds opaque scenario-owned bytes.
struct Action {
  std::string name_;
  std::vector<std::int64_t> arguments_;
  std::string payload_;

  bool operator==(const Action&) const = default;
};

// Invariant IDs are stable API used by regression traces. fingerprint also
// includes the witness, so minimization cannot silently change the failure.
struct Finding {
  std::string invariant_id_;
  std::string witness_;

  bool operator==(const Finding&) const = default;
  std::string fingerprint() const;
};

// Logical time counts applied transitions rather than wall-clock time.
struct TraceRecord {
  std::uint64_t logical_time_ = 0;
  TraceRecordKind kind_ = TraceRecordKind::kChoice;
  std::string name_;
  std::string payload_;

  bool operator==(const TraceRecord&) const = default;
};

// A trace is self-identifying by scenario and schema. Exact replay rejects
// mismatches before applying any recorded action.
struct Trace {
  TraceMode mode_ = TraceMode::kExactModel;
  std::string scenario_;
  std::uint32_t schema_ = 1;
  std::uint64_t seed_ = 0;
  std::string initial_state_hash_;
  std::vector<TraceRecord> records_;

  bool operator==(const Trace&) const = default;
};

// KFT1 is a canonical, line-oriented format. Exact-model traces are durable
// regression inputs; process-action traces are diagnostic scripts and may
// record observation drift when the real scheduler takes a different path.
// Writes replace a trace through a sibling temporary file. A failed write
// leaves an existing target unchanged and removes the temporary file, though a
// newly created parent directory may remain. Callers must externally coordinate
// multiple writers targeting the same path.
std::string EncodeTrace(const Trace& trace);
absl::StatusOr<Trace> DecodeTrace(std::string_view encoded);
absl::Status WriteTraceAtomically(const Trace& trace,
                                  const std::filesystem::path& path);
absl::StatusOr<Trace> ReadTrace(const std::filesystem::path& path);

class ScenarioWorld {
 public:
  ScenarioWorld() = default;
  ScenarioWorld(const ScenarioWorld&) = delete;
  ScenarioWorld& operator=(const ScenarioWorld&) = delete;
  virtual ~ScenarioWorld() = default;

  virtual std::vector<Action> EnabledActions() const = 0;
  virtual absl::Status Apply(const Action& action) = 0;
  virtual std::string Observe() const = 0;
  virtual std::optional<Finding> CheckInvariants() const = 0;
  virtual bool HasPendingWork() const = 0;

  // Returns domain acknowledgments created since the preceding drain. Stable
  // fault-checkpoint acknowledgments belong here so exact replay verifies that
  // the requested fault was reached, not merely that an action was attempted.
  virtual std::vector<std::string> DrainAcknowledgments();
};

// A Scenario owns all domain knowledge. NewWorld must return byte-identical
// initial observations and equivalent transitions for the same action stream:
// Execute first selects an action stream in one world and then records it in a
// fresh world. EnabledActions, Apply, Observe, and CheckInvariants therefore
// must depend only on explicit world state, never wall time or ambient entropy.
// The generic runner owns scheduling and artifacts, while stable invariant
// identities and shrink candidates stay local to the scenario.
class Scenario {
 public:
  Scenario() = default;
  Scenario(const Scenario&) = delete;
  Scenario& operator=(const Scenario&) = delete;
  virtual ~Scenario() = default;

  virtual std::string_view name() const = 0;
  virtual std::uint32_t schema() const = 0;
  virtual std::unique_ptr<ScenarioWorld> NewWorld() const = 0;
  virtual std::vector<Action> Simplify(const Action& action) const;
};

struct RunLimits {
  // Bounds apply independently to transitions, one enabled-action set, encoded
  // trace size, and candidate executions attempted by minimization.
  std::size_t transition_budget_ = 256;
  std::size_t max_enabled_actions_ = 4096;
  std::size_t max_trace_bytes_ = 4 * 1024 * 1024;
  std::size_t minimize_attempt_budget_ = 4096;
};

// Deadlock means pending scenario work has no enabled transition. Step/resource
// limits are bounded inconclusive results; trace drift is a deterministic
// contract or replay mismatch rather than a discovered safety violation.
enum class RunStatus : std::uint8_t {
  kQuiescent,
  kStepBudgetReached,
  kFinding,
  kDeadlock,
  kTraceDrift,
  kResourceExhausted,
};

// RunResult always carries the trace produced up to termination. finding is
// populated when a domain violation was observed; it can coexist with
// kResourceExhausted if persisting that failing trace subsequently fails.
// diagnostic explains non-domain failures.
struct RunResult {
  RunStatus status_ = RunStatus::kQuiescent;
  Trace trace_;
  std::optional<Finding> finding_;
  std::string diagnostic_;
  std::size_t transitions_ = 0;
};

// Request pointers are borrowed for the duration of the synchronous call.
struct ExecuteRequest {
  const Scenario* scenario_ = nullptr;
  std::uint64_t seed_ = 0;
  RunLimits limits_;
  std::optional<std::filesystem::path> trace_path_;
};

struct ReplayRequest {
  const Scenario* scenario_ = nullptr;
  const Trace* trace_ = nullptr;
  RunLimits limits_;
};

struct MinimizeRequest {
  const Scenario* scenario_ = nullptr;
  const Trace* failing_trace_ = nullptr;
  RunLimits limits_;
  std::optional<std::filesystem::path> trace_path_;
};

class ScenarioRunner {
 public:
  // Execute chooses among canonically sorted enabled actions using only seed.
  // It returns a bounded partial trace rather than running indefinitely.
  RunResult Execute(const ExecuteRequest& request) const;

  // Replay requires an exact-model trace with matching scenario and schema;
  // any action, observation, acknowledgment, or finding drift is reported.
  RunResult Replay(const ReplayRequest& request) const;

  // Minimize preserves the original finding fingerprint and is bounded by the
  // configured attempt budget. Its output is itself exactly replayable.
  RunResult Minimize(const MinimizeRequest& request) const;
};

// std::mt19937_64 has a specified output sequence, but the standard library's
// integer distributions do not. This sampler keeps seed behavior stable across
// standard-library implementations.
class StableRandom {
 public:
  explicit StableRandom(std::uint64_t seed) : engine_(seed) {}

  std::uint64_t Bounded(std::uint64_t exclusive_upper_bound);

 private:
  std::mt19937_64 engine_;
};

std::string EncodeAction(const Action& action);
absl::StatusOr<Action> DecodeAction(std::string_view encoded);

}  // namespace keylane::test::cluster
