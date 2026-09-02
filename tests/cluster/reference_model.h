#pragma once

#include <compare>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "tests/cluster/fault_harness.h"

namespace keylane::test::cluster {

// StrongId prevents accidental comparisons across independently versioned HA
// identities while keeping the model cheap and trivially serializable.
template <typename Tag>
struct StrongId {
  std::uint64_t value_ = 0;

  auto operator<=>(const StrongId&) const = default;
};

using NodeId = StrongId<struct NodeIdTag>;
using BootId = StrongId<struct BootIdTag>;
using GroupId = StrongId<struct GroupIdTag>;
using GroupTerm = StrongId<struct GroupTermTag>;
using GrantId = StrongId<struct GrantIdTag>;
using TopologyEpoch = StrongId<struct TopologyEpochTag>;
using ConfigEpoch = StrongId<struct ConfigEpochTag>;
using ManifestId = StrongId<struct ManifestIdTag>;
using PartitionEpoch = StrongId<struct PartitionEpochTag>;
using HistoryId = StrongId<struct HistoryIdTag>;
using OperationId = StrongId<struct OperationIdTag>;
using EvidenceId = StrongId<struct EvidenceIdTag>;
using FlowId = StrongId<struct FlowIdTag>;

// Partial-sync and ordering evidence is comparable only inside one complete
// compatibility domain; missing flow cursors compare as zero.
struct CompatibilityDomain {
  BootId source_boot_;
  HistoryId history_;
  ManifestId manifest_;
  PartitionEpoch partition_epoch_;

  bool operator==(const CompatibilityDomain&) const = default;
};

struct AppliedVector {
  CompatibilityDomain domain_;
  std::map<FlowId, std::uint64_t> cursors_;
};

enum class VectorOrder : std::uint8_t {
  kLess,
  kEqual,
  kGreater,
  kIncomparable,
  kIncompatible,
};

VectorOrder CompareAppliedVectors(const AppliedVector& left,
                                  const AppliedVector& right);

// Authority identity includes node incarnation, term, and grant. Capabilities
// deliberately cover more than write admission: an expired identity must lose
// every path that can mutate or decide success.
struct AuthorityObservation {
  GroupId group_;
  NodeId node_;
  BootId boot_;
  GroupTerm term_;
  GrantId grant_;
  bool can_admit_write_ = false;
  bool can_complete_inflight_ = false;
  bool can_mutate_in_background_ = false;
  bool can_decide_success_ = false;
};

// Activation and write admission both require the durable promotion barrier
// and child-history readiness; peer reset is later than that full barrier.
struct PromotionObservation {
  bool candidate_selected_ = false;
  bool durability_barrier_complete_ = false;
  bool child_history_ready_ = false;
  bool candidate_activated_ = false;
  bool write_gate_open_ = false;
  bool another_replica_reset_ = false;
};

// Resume evidence is scoped to both process boots and requires exact,
// contiguous, transaction-complete history in a compatible population.
struct ResumeObservation {
  BootId source_boot_;
  BootId target_boot_;
  BootId evidence_source_boot_;
  BootId evidence_target_boot_;
  bool compatible_history_ = true;
  bool compatible_population_ = true;
  bool exact_cursor_ = true;
  bool retained_events_contiguous_ = true;
  bool transaction_boundary_complete_ = true;
  bool partial_resume_selected_ = false;
};

// Staging is never client-visible, and active population state must represent
// a complete, durably activated generation after recovery.
struct PopulationObservation {
  bool active_ = true;
  bool complete_ = true;
  bool activation_durable_ = true;
  bool staging_visible_ = false;
};

// Candidate vectors use the componentwise partial order above; scalar sums or
// lexicographic ordering are intentionally invalid election evidence.
struct CandidateObservation {
  AppliedVector candidate_;
  AppliedVector peer_;
  bool candidate_claimed_not_behind_ = false;
};

// Serving Meta state cannot trail committed state, and directive evidence is
// bound to both the current operation and process boot.
struct MetaObservation {
  TopologyEpoch committed_topology_;
  TopologyEpoch installed_topology_;
  GroupTerm committed_term_;
  GroupTerm installed_term_;
  OperationId current_operation_;
  OperationId directive_operation_;
  BootId current_boot_;
  BootId evidence_boot_;
  bool serving_state_published_ = false;
  bool directive_applied_ = false;
};

// Migration maintains one serving owner and activates the target only after
// both ownership commit and population completion.
struct MigrationObservation {
  bool source_serves_ = true;
  bool target_serves_ = false;
  bool ownership_committed_ = false;
  bool target_population_complete_ = false;
};

enum class ClientOutcome : std::uint8_t {
  kNotReturned,
  kSuccess,
  kFailure,
};

// Client history binds each operation to the full authority incarnation
// (node, boot, term, grant) and separates admission, durability, and response
// decision. A crash may leave an explicit not-returned/uncertain outcome
// without inventing either success or failure.
struct ClientOperationObservation {
  OperationId operation_;
  GroupId group_;
  NodeId authority_;
  BootId authority_boot_;
  GroupTerm authority_term_;
  GrantId authority_grant_;
  bool mutates_ = false;
  bool admitted_ = false;
  bool admitted_with_valid_authority_ = false;
  bool mutation_durable_ = false;
  bool authority_valid_at_success_decision_ = false;
  ClientOutcome outcome_ = ClientOutcome::kNotReturned;
};

// ClusterSnapshot is the protocol-independent observation boundary. Future
// real adapters project implementation state here without exposing internal
// lease, consensus, or replication wire fields to the harness.
struct ClusterSnapshot {
  std::vector<AuthorityObservation> authorities_;
  PromotionObservation promotion_;
  ResumeObservation resume_;
  PopulationObservation population_;
  std::optional<CandidateObservation> candidate_;
  MetaObservation meta_;
  MigrationObservation migration_;
  std::vector<ClientOperationObservation> client_history_;
  std::optional<std::string> server_control_reply_;
};

// Returns the first violated invariant in a stable priority order. Invariant
// identifiers are durable trace API; witnesses may grow more descriptive.
std::optional<Finding> CheckClusterInvariants(const ClusterSnapshot& snapshot);

enum class Counterexample : std::uint8_t {
  kNone,
  kDualAuthority,
  kStaleEvidence,
  kHistoryGap,
  kPartialActivation,
  kStaleDirective,
};

struct ScenarioDescriptor {
  Counterexample counterexample_;
  std::string_view cli_name_;
  std::string_view trace_name_;
  std::string_view expected_invariant_;
};

// The descriptor table is the single registry used by the CLI, soak runner,
// parameterized tests, and checked-in regression discovery.
std::span<const ScenarioDescriptor> ClusterScenarioDescriptors();
const ScenarioDescriptor* FindClusterScenario(std::string_view name);

// These scenarios deliberately keep the production protocol out of the test
// model. Later cluster issues map their observations onto ClusterSnapshot and
// reuse the same invariant identities.
std::unique_ptr<Scenario> MakeClusterScenario(Counterexample counterexample);

}  // namespace keylane::test::cluster
