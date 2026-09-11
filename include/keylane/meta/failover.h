#pragma once

// Typed state carried inside the generic MetaOperationRecord for one
// top-level failover. The journal remains operation-kind agnostic; this codec
// gives the failover reconciler a strict, replayable contract without adding
// nested promotion operations.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"

namespace keylane::meta {

class MetaCommittedView;
class MetaObservationStore;
struct MetaOperationRecord;
struct MetaTerminalReceipt;
struct MetaStores;

inline constexpr std::string_view kFailoverOperationKind = "failover";
// A controlled attempt owns the group's authority workflow, so its operator-
// supplied lifetime is bounded independently from the Admin connection wait.
inline constexpr std::uint32_t kMaxControlledFailoverAttemptTimeoutMs =
    3'600'000;

struct FailoverIntent {
  std::string group_id_;
  std::uint64_t recovery_generation_ = 0;
  // Duration, not a portable timestamp. Each Meta leader starts one monotonic
  // budget when it first observes this committed operation; phase changes and
  // reconnects within that tenure never refresh it.
  std::uint32_t attempt_timeout_ms_ = 0;
  std::string former_owner_node_id_;
  MetaAssignmentId former_owner_assignment_id_{};
  MetaBootIncarnation former_owner_boot_id_{};
  std::string candidate_node_id_;
  MetaAssignmentId candidate_assignment_id_{};
  MetaBootIncarnation candidate_boot_id_{};
  std::uint64_t group_term_ = 0;
  std::uint64_t authority_version_ = 0;
  std::uint64_t grant_revision_ = 0;
  // BeginGroupTerm removes the old finite grant. Retaining its exact spec in
  // the immutable intent lets a new Meta leader construct and validate the
  // later candidate activation without consulting expired observations.
  MetaGrantSpec old_grant_;
  std::uint64_t population_manifest_revision_ = 0;
  MetaHash256 population_manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  MetaReplicationHistoryId parent_history_id_{};
  // The selected candidate fixes the replication layout before observations
  // can expire; every later frozen/catch-up/prepare vector has this width.
  std::uint32_t flow_count_ = 0;

  bool operator==(const FailoverIntent&) const = default;
};

// Values are durable KLFP tags and their ordering is used by safety checks.
// Existing values must never be renumbered or reordered; add new stages only
// with an explicit codec/transition migration.
enum class FailoverPhaseStage : std::uint8_t {
  kSourceHolding = 1,
  kSourceHeld = 2,
  kOldAuthorityExcluding = 3,
  kOldAuthorityExcluded = 4,
  kCandidateCaughtUp = 5,
  kPromotionPreparing = 6,
  kPromotionPrepared = 7,
  kAuthorityActivated = 8,
  kServing = 9,
};

struct FailoverPhase {
  FailoverPhaseStage stage_ = FailoverPhaseStage::kSourceHolding;
  MetaHash256 old_authority_exclusion_hash_{};
  std::vector<std::uint64_t> required_applied_next_lsns_;
  MetaHash256 prepared_result_hash_{};

  bool operator==(const FailoverPhase&) const = default;
};

enum class FailoverLossClassification : std::uint8_t {
  kExact = 1,
  kBounded = 2,
  kUnknown = 3,
};

// Versioned terminal result stored in MetaOperationRecord. `recovery_required`
// is an instruction to the asynchronous cleanup pass, not an operation link:
// a later uncontrolled operation remains completely independent and reads
// only MetaFailoverRecoveryStore.
struct ControlledFailoverOutcome {
  bool succeeded_ = false;
  FailoverPhaseStage terminal_stage_ = FailoverPhaseStage::kSourceHolding;
  FailoverLossClassification loss_ = FailoverLossClassification::kUnknown;
  bool recovery_required_ = false;
  std::vector<std::uint64_t> proven_next_lsns_;
  std::string reason_;

  bool operator==(const ControlledFailoverOutcome&) const = default;
};

// Encoders reject invalid domain values before producing the canonical,
// versioned bytes stored in MetaOperationRecord. Decoders fail-stop on an
// unknown version, corrupt/trailing bytes, or decoded state that violates the
// same invariants: committed failover blobs are never repaired heuristically.
absl::StatusOr<std::string> EncodeFailoverIntent(const FailoverIntent& intent);
absl::StatusOr<FailoverIntent> DecodeFailoverIntent(std::string_view encoded);
absl::StatusOr<std::string> EncodeFailoverPhase(const FailoverPhase& phase);
absl::StatusOr<FailoverPhase> DecodeFailoverPhase(std::string_view encoded);
absl::StatusOr<std::string> EncodeControlledFailoverOutcome(
    const ControlledFailoverOutcome& outcome);
absl::StatusOr<ControlledFailoverOutcome> DecodeControlledFailoverOutcome(
    std::string_view encoded);
std::string_view FailoverLossClassificationName(
    FailoverLossClassification classification) noexcept;
std::string_view FailoverPhaseStageName(FailoverPhaseStage stage) noexcept;

// Produces the domain-separated proof summary used when fencing succeeded but
// the old source disappeared before it could return an exact frozen proof.
// The hash binds the immutable selection intent and candidate-backed frontier;
// it must never be substituted with a directive receipt hash.
absl::StatusOr<MetaHash256> ComputeFailoverUnavailableProofHash(
    const FailoverIntent& intent,
    const std::vector<std::uint64_t>& candidate_frontier);

// Resolves the two-part promotion proof: a committed successful receipt plus
// matching evidence from the candidate's current observation session. A
// missing observation is retryable and returns nullopt; malformed receipt
// bytes or a current exact-session report that contradicts the receipt are a
// definitive protocol conflict and return a non-OK status.
absl::StatusOr<std::optional<MetaEvidenceSummary>>
ResolveFailoverPreparedEvidence(const MetaCommittedView& view,
                                const MetaOperationRecord& operation,
                                const FailoverIntent& intent,
                                const FailoverPhase& phase,
                                const MetaObservationStore& observations,
                                const MetaTerminalReceipt& receipt,
                                std::int64_t now_unix_ms);

// Freezes one operator-triggered controlled attempt from a single committed
// and observation cut. Candidate selection is delegated to CandidatePlanFor;
// this seam only binds the selected incarnation to the current owner/grant
// and recovery generation, then returns the canonical generic-journal submit.
// The caller supplies both random ids and a fixed wall-clock instant so tests
// and retries never silently choose from different candidate cuts.
absl::StatusOr<SubmitOperation> BuildControlledFailoverSubmission(
    std::string_view group_id, const MetaOperationId& operation_id,
    const MetaRequestId& request_id, const MetaCommittedView& view,
    const MetaObservationStore& observations, std::int64_t now_unix_ms,
    std::uint32_t attempt_timeout_ms);

// Coordinator hook for the complete controlled-failover graph. It is a no-op
// for unrelated operations and rejects skipped phases, stale committed anchors,
// mismatched directives, or unsubstantiated exclusion/catch-up/preparation
// claims. Corrupt committed typed state is rejected conservatively; this hook
// validates proposals but never advances authority itself.
absl::Status ValidateFailoverProposal(const MetaCommand& command,
                                      const MetaCommittedView& view,
                                      const MetaObservationStore& observations);

// Deterministic apply-time guard for controlled-failover terminal commands.
// Unlike the leader-local proposal hook, this is evaluated by every replica
// against the current aggregate so a stale terminal command cannot cross a
// recovery, fencing, or activation mutation that does not advance the generic
// operation revision. It is a no-op for operations of other kinds and accepts
// an exact terminal replay.
absl::Status ValidateFailoverTerminalCommand(const CompleteOperation& command,
                                             const MetaStores& stores);
absl::Status ValidateFailoverTerminalCommand(const AbortOperation& command,
                                             const MetaStores& stores);

// A terminal failover may leave cross-store cleanup behind. Archival is
// therefore allowed only after a safe completion/cancellation has fully
// cleared its recovery record, or after a recovery-required outcome has been
// durably handed off. This deterministic guard is shared by proposal,
// durability fail-safe admission, and apply so a stale archive cannot erase
// the only workflow owner capable of completing cleanup.
absl::Status ValidateFailoverArchiveCommand(const ArchiveOperations& command,
                                            const MetaStores& stores);

// Apply-time admission for a new failover. Candidate freshness is necessarily
// leader-local, but the immutable authority, membership, population, and
// recovery-generation anchors are rechecked by every replica so a proposal
// cannot commit across an intervening fence or term change. Existing-id
// replay is resolved by the operation store before this guard is called.
absl::Status ValidateFailoverSubmissionCommand(const SubmitOperation& command,
                                               const MetaStores& stores);

// Recognizes only the two terminal recovery-store transitions needed to make
// a failover archivable: hold-only -> recovery handoff/release, then (for a
// release) physical clear. The durability fail-safe uses these checks to
// admit bounded workflow cleanup without opening normal recovery mutations.
absl::Status ValidateFailoverTerminalCleanupCommand(
    const SetFailoverRecovery& command, const MetaStores& stores);
absl::Status ValidateFailoverTerminalCleanupCommand(
    const ClearFailoverRecovery& command, const MetaStores& stores);

// The durability fail-safe normally accepts only terminalization and bounded
// cleanup. These two validators admit the smaller set of live checkpoints
// needed to reach such a terminal state: recovery-owner bootstrap, monotonic
// proof loss/persistence, frontier downgrade, and attribution of authority or
// serving facts that already occurred. They never admit directive dispatch or
// a new authority mutation.
absl::Status ValidateFailoverFailSafeCheckpoint(
    const SetFailoverRecovery& command, const MetaStores& stores);
absl::Status ValidateFailoverFailSafeCheckpoint(
    const TransitionOperationPhase& command, const MetaCommittedView& view,
    const MetaObservationStore& observations);

// Admits only a CAS-preserving pending/exact -> unavailable transition for an
// explicit group recovery handoff. Unlike operation-owned cleanup, this
// remains valid after the terminal operation has been archived so old-source
// availability cannot become stale indefinitely.
absl::Status ValidateFailoverRecoveryAvailabilityCommand(
    const SetFailoverRecovery& command, const MetaStores& stores);

// Deterministic apply-time guards for recovery mutations owned by a live or
// same-generation terminal controlled failover. Recovery CAS does not observe
// operation receipts, so these recheck the typed cross-store transition on
// every replica and prevent a queued proof-loss update from overtaking a
// successful frozen-source result or terminal outcome. Commands without such
// an owner, including future successor generations, remain the responsibility
// of their workflow validator and the recovery store.
absl::Status ValidateFailoverRecoveryApplyCommand(
    const SetFailoverRecovery& command, const MetaStores& stores,
    std::uint64_t log_index);
absl::Status ValidateFailoverRecoveryApplyCommand(
    const ClearFailoverRecovery& command, const MetaStores& stores,
    std::uint64_t log_index);

// Apply-time counterpart for authority mutations owned by controlled
// failover. `log_index` distinguishes an exact ActivateAuthority replay from
// a first application after the owning operation has already terminated.
// Unowned commands remain available to independent workflows, but may not
// mutate a group while a controlled failover is live.
absl::Status ValidateFailoverWorkflowCommand(const BeginGroupTerm& command,
                                             const MetaStores& stores,
                                             std::uint64_t log_index);
absl::Status ValidateFailoverWorkflowCommand(const ActivateAuthority& command,
                                             const MetaStores& stores,
                                             std::uint64_t log_index);

// Ordinary authority mutations have no workflow owner token. They remain
// available for groups without a live controlled failover, but cannot change
// the pinned former-authority cut while that workflow owns the group.
absl::Status ValidateFailoverWorkflowCommand(const GrantAuthority& command,
                                             const MetaStores& stores);
absl::Status ValidateFailoverWorkflowCommand(const RevokeGrant& command,
                                             const MetaStores& stores);
absl::Status ValidateFailoverWorkflowCommand(const FenceGroup& command,
                                             const MetaStores& stores);

}  // namespace keylane::meta
