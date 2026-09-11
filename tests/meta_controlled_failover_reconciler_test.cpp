#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/control_projector.h"
#include "keylane/meta/controlled_failover_reconciler.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/population_manifest_store.h"

namespace keylane::meta {
namespace {

namespace control = keylane::cluster::control;

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& bytes) {
  constexpr std::string_view kDigits = "0123456789abcdef";
  std::string result(bytes.size() * 2, '\0');
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    result[index * 2] = kDigits[bytes[index] >> 4];
    result[index * 2 + 1] = kDigits[bytes[index] & 0x0f];
  }
  return result;
}

std::int64_t NowUnixMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

TEST(ControlledFailoverOutcomeCodecTest, IsStrictBoundedAndCanonical) {
  const ControlledFailoverOutcome exact{
      .succeeded_ = true,
      .terminal_stage_ = FailoverPhaseStage::kServing,
      .loss_ = FailoverLossClassification::kExact,
      .proven_next_lsns_ = {21, 34},
      .reason_ = "candidate is serving the exact frozen frontier",
  };
  auto encoded = EncodeControlledFailoverOutcome(exact);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = DecodeControlledFailoverOutcome(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, exact);
  EXPECT_EQ(FailoverLossClassificationName(exact.loss_), "exact");
  EXPECT_EQ(FailoverPhaseStageName(exact.terminal_stage_), "serving");

  std::string trailing = *encoded + "x";
  EXPECT_EQ(
      MetaFailureClassOf(DecodeControlledFailoverOutcome(trailing).status()),
      MetaFailureClass::kFailStop);
  std::string unknown_version = *encoded;
  unknown_version[4] = '\x02';
  EXPECT_EQ(MetaFailureClassOf(
                DecodeControlledFailoverOutcome(unknown_version).status()),
            MetaFailureClass::kFailStop);
  EXPECT_EQ(MetaFailureClassOf(DecodeControlledFailoverOutcome(
                                   encoded->substr(0, encoded->size() - 1))
                                   .status()),
            MetaFailureClass::kFailStop);

  ControlledFailoverOutcome invalid = exact;
  invalid.terminal_stage_ = FailoverPhaseStage::kPromotionPrepared;
  EXPECT_EQ(
      MetaFailureClassOf(EncodeControlledFailoverOutcome(invalid).status()),
      MetaFailureClass::kDomainReject);
  invalid = exact;
  invalid.recovery_required_ = true;
  EXPECT_EQ(
      MetaFailureClassOf(EncodeControlledFailoverOutcome(invalid).status()),
      MetaFailureClass::kDomainReject);
  invalid = exact;
  invalid.succeeded_ = false;
  invalid.loss_ = FailoverLossClassification::kBounded;
  invalid.proven_next_lsns_.clear();
  EXPECT_EQ(
      MetaFailureClassOf(EncodeControlledFailoverOutcome(invalid).status()),
      MetaFailureClass::kDomainReject);
  invalid = exact;
  invalid.proven_next_lsns_ = {0};
  EXPECT_EQ(
      MetaFailureClassOf(EncodeControlledFailoverOutcome(invalid).status()),
      MetaFailureClass::kDomainReject);
  invalid = exact;
  invalid.reason_.clear();
  EXPECT_EQ(
      MetaFailureClassOf(EncodeControlledFailoverOutcome(invalid).status()),
      MetaFailureClass::kDomainReject);
}

TEST(ControlledFailoverOutcomeCodecTest,
     MaximumFlowFailureFitsTheDurableAbortCommand) {
  ControlledFailoverOutcome outcome{
      .succeeded_ = false,
      .terminal_stage_ = FailoverPhaseStage::kCandidateCaughtUp,
      .loss_ = FailoverLossClassification::kUnknown,
      .recovery_required_ = true,
      .proven_next_lsns_ =
          std::vector<std::uint64_t>(kMaxMetaFailoverRecoveryFlows, 1),
      .reason_ = "candidate failed after the authority cut",
  };
  auto encoded_outcome = EncodeControlledFailoverOutcome(outcome);
  ASSERT_TRUE(encoded_outcome.ok()) << encoded_outcome.status();
  ASSERT_GT(encoded_outcome->size(), 1024u);

  AbortOperation abort;
  abort.request_id_ = Bytes<16>(0x31);
  abort.operation_id_ = Bytes<16>(0x32);
  abort.expected_revision_ = 1;
  abort.reason_ = *encoded_outcome;
  abort.data_loss_possible_ = true;
  const auto encoded_command = EncodeMetaCommand(MetaCommand(abort));
  EXPECT_TRUE(encoded_command.ok()) << encoded_command.status();
}

class ControlledFailoverPlannerTest : public testing::Test {
 protected:
  static constexpr std::uint64_t kLeadershipGeneration = 7;

  void Apply(MetaCommand command) {
    const std::uint64_t committed_index = index_ + 1;
    std::visit(
        [committed_index](auto& value) {
          value.request_id_.fill(
              static_cast<std::uint8_t>((committed_index % 250) + 1));
        },
        command);
    const absl::Status proposal = ValidateFailoverProposal(
        command, MetaCommittedView(stores_, index_), observations_);
    ASSERT_TRUE(proposal.ok()) << proposal;

    const MetaApplyResult applied =
        ApplyCommitted(stores_, committed_index, command,
                       "keylane://operator/failover-test", "now");
    ASSERT_EQ(applied.verdict_, MetaAuditVerdict::kAccepted) << applied.detail_;
    index_ = committed_index;

    auto encoded = stores_.Serialize();
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    auto restored = MetaStores::Deserialize(*encoded);
    ASSERT_TRUE(restored.ok()) << restored.status();
    stores_ = std::move(*restored);

    // Every durable cut is exercised as an exact replay after snapshot
    // recovery. The replay must neither mint another directive/receipt nor
    // advance a CAS revision a second time.
    const std::string before_replay = *stores_.Serialize();
    const MetaApplyResult replay =
        ApplyCommitted(stores_, committed_index, command,
                       "keylane://operator/failover-test", "now");
    ASSERT_EQ(replay.verdict_, MetaAuditVerdict::kAccepted) << replay.detail_;
    auto after_replay = stores_.Serialize();
    ASSERT_TRUE(after_replay.ok()) << after_replay.status();
    EXPECT_EQ(*after_replay, before_replay);
  }

  auto Plan(detail::ControlledFailoverLiveness liveness = {}) {
    const auto operation = stores_.operation_.FindOperation(operation_id_);
    if (!operation.has_value()) {
      return absl::StatusOr<std::optional<MetaCommand>>(
          absl::NotFoundError("controlled failover operation is missing"));
    }
    return detail::PlanControlledFailoverStep(
        MetaCommittedView(stores_, index_), *operation, observations_, runtime_,
        liveness, now_unix_ms_);
  }

  detail::ControlledFailoverReadiness Readiness() {
    const auto operation = stores_.operation_.FindOperation(operation_id_);
    EXPECT_TRUE(operation.has_value());
    if (!operation.has_value()) return {};
    auto intent = DecodeFailoverIntent(operation->intent_);
    EXPECT_TRUE(intent.ok()) << intent.status();
    if (!intent.ok()) return {};
    std::optional<FailoverPhase> phase;
    if (!operation->kind_phase_blob_.empty()) {
      auto decoded = DecodeFailoverPhase(operation->kind_phase_blob_);
      EXPECT_TRUE(decoded.ok()) << decoded.status();
      if (!decoded.ok()) return {};
      phase = *decoded;
    }
    return detail::EvaluateControlledFailoverReadiness(
        MetaCommittedView(stores_, index_), observations_, runtime_, *operation,
        *intent, phase, now_unix_ms_);
  }

  template <typename Command>
  absl::StatusOr<Command> NextAs(
      detail::ControlledFailoverLiveness liveness = {}) {
    auto planned = Plan(liveness);
    if (!planned.ok()) return planned.status();
    if (!planned->has_value()) {
      return absl::NotFoundError("planner is waiting");
    }
    if (!std::holds_alternative<Command>(**planned)) {
      return absl::FailedPreconditionError(
          "planner returned a different command type");
    }
    return std::get<Command>(**planned);
  }

  template <typename Command>
  void ApplyNext(detail::ControlledFailoverLiveness liveness = {}) {
    auto command = NextAs<Command>(liveness);
    ASSERT_TRUE(command.ok()) << command.status();
    Apply(MetaCommand(*command));
  }

  MetaOperationRecord Operation() const {
    return *stores_.operation_.FindOperation(operation_id_);
  }

  FailoverPhase Phase() const {
    auto phase = DecodeFailoverPhase(Operation().kind_phase_blob_);
    EXPECT_TRUE(phase.ok()) << phase.status();
    return phase.ok() ? *phase : FailoverPhase{};
  }

  MetaDataControlRuntimeNode* RuntimeNode(std::string_view node_id) {
    const auto found =
        std::find_if(runtime_.nodes_.begin(), runtime_.nodes_.end(),
                     [&](const MetaDataControlRuntimeNode& node) {
                       return node.node_id_ == node_id;
                     });
    return found == runtime_.nodes_.end() ? nullptr : &*found;
  }

  void InstallSourceHold() {
    MetaDataControlRuntimeNode* source =
        RuntimeNode(intent_.former_owner_node_id_);
    ASSERT_NE(source, nullptr);
    ASSERT_EQ(source->groups_.size(), 1);
    source->groups_.front().source_history_hold_ =
        control::WireSourceHistoryHold{
            .generation = intent_.recovery_generation_,
            .source_assignment_id = intent_.former_owner_assignment_id_,
            .source_boot_id = Hex(intent_.former_owner_boot_id_),
            .source_replication_history_id = Hex(intent_.parent_history_id_),
            .manifest_revision = intent_.population_manifest_revision_,
            .manifest_digest = intent_.population_manifest_digest_,
            .partition_replication_epoch = intent_.partition_replication_epoch_,
    };
  }

  void RemoveSourceHold() {
    MetaDataControlRuntimeNode* source =
        RuntimeNode(intent_.former_owner_node_id_);
    ASSERT_NE(source, nullptr);
    ASSERT_EQ(source->groups_.size(), 1);
    source->groups_.front().source_history_hold_.reset();
  }

  void RemoveSourceRuntime() {
    std::erase_if(runtime_.nodes_, [&](const MetaDataControlRuntimeNode& node) {
      return node.node_id_ == intent_.former_owner_node_id_;
    });
  }

  void ReplaceSourceRuntimeBoot() {
    MetaDataControlRuntimeNode* source =
        RuntimeNode(intent_.former_owner_node_id_);
    ASSERT_NE(source, nullptr);
    source->boot_id_ = Hex(Bytes<20>(0x24));
  }

  void ReplaceSourceRuntimeHistory() {
    MetaDataControlRuntimeNode* source =
        RuntimeNode(intent_.former_owner_node_id_);
    ASSERT_NE(source, nullptr);
    source->replication_history_id_ = Bytes<20>(0x7c);
  }

  void RemoveCandidateRuntime() {
    std::erase_if(runtime_.nodes_, [&](const MetaDataControlRuntimeNode& node) {
      return node.node_id_ == intent_.candidate_node_id_;
    });
  }

  void SetRuntimeTerm(std::uint64_t term) {
    for (MetaDataControlRuntimeNode& node : runtime_.nodes_) {
      ASSERT_EQ(node.groups_.size(), 1);
      node.groups_.front().group_term_ = term;
    }
  }

  void ObserveCandidate(std::uint64_t term,
                        std::vector<std::uint64_t> frontier) {
    MetaCandidateProgressObs progress{
        .node_id_ = intent_.candidate_node_id_,
        .boot_incarnation_ = intent_.candidate_boot_id_,
        .session_generation_ = 1,
        .group_id_ = intent_.group_id_,
        .assignment_id_ = intent_.candidate_assignment_id_,
        .group_term_ = term,
        .population_manifest_revision_ = intent_.population_manifest_revision_,
        .population_manifest_digest_ = intent_.population_manifest_digest_,
        .partition_replication_epoch_ = intent_.partition_replication_epoch_,
        .replication_history_id_ = Bytes<20>(0x61),
        .source_node_id_ = intent_.former_owner_node_id_,
        .source_assignment_id_ = intent_.former_owner_assignment_id_,
        .source_boot_incarnation_ = intent_.former_owner_boot_id_,
        .source_replication_history_id_ = intent_.parent_history_id_,
        .applied_next_lsns_ = std::move(frontier),
        .storage_ready_ = true,
        .population_ready_ = true,
    };
    const MetaObservationIdentity identity{intent_.candidate_node_id_,
                                           intent_.candidate_boot_id_, 1};
    ASSERT_TRUE(observations_
                    .Ingest(MetaObservation{.identity_ = identity,
                                            .payload_ = std::move(progress)},
                            MetaStoresFacts(stores_), now_unix_ms_)
                    .ok());
  }

  void CommitCurrentResult(MetaDirectiveResultStatus status,
                           std::string result) {
    const MetaOperationRecord operation = Operation();
    ASSERT_EQ(operation.current_directives_.size(), 1);
    const MetaCurrentDirective& current = operation.current_directives_.front();
    CommitDirectiveResult command;
    command.operation_id_ = operation.operation_id_;
    command.directive_id_ = current.spec_.directive_id_;
    command.attempt_id_ = current.spec_.attempt_id_;
    command.directive_revision_ = current.directive_revision_;
    command.recipient_node_id_ = current.spec_.recipient_node_id_;
    command.recipient_boot_id_ =
        current.spec_.recipient_node_id_ == current.spec_.source_node_id_
            ? current.spec_.source_boot_id_
            : current.spec_.target_boot_id_;
    command.assignment_id_ = current.spec_.assignment_id_;
    command.status_ = status;
    command.result_ = std::move(result);
    command.result_hash_ = MetaSha256(command.result_);
    Apply(command);
  }

  void AdvanceToSourceHolding() {
    ApplyNext<SetFailoverRecovery>();
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHolding);
  }

  void AdvanceThroughHoldAndToExcluding() {
    AdvanceToSourceHolding();
    InstallSourceHold();
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHolding);
    ASSERT_EQ(Operation().current_directives_.size(), 1);
    CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded, "source-held");
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHeld);
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kOldAuthorityExcluding);
  }

  void AdvanceThroughBeginGroupTerm() {
    AdvanceThroughHoldAndToExcluding();
    ApplyNext<BeginGroupTerm>();
    SetRuntimeTerm(intent_.group_term_);
    ObserveCandidate(intent_.group_term_, {21, 34});
  }

  void CommitFrozenSourceSuccess(std::vector<std::uint64_t> frozen_frontier = {
                                     21, 34}) {
    control::FrozenSourceEvidence evidence{
        .recovery_generation = intent_.recovery_generation_,
        .source_history_id = Hex(intent_.parent_history_id_),
        .final_next_lsns = std::move(frozen_frontier),
    };
    auto proof = control::ComputeFrozenSourceProofHash(evidence);
    ASSERT_TRUE(proof.ok()) << proof.status();
    evidence.proof_hash = *proof;
    auto encoded = control::EncodeFrozenSourceEvidence(evidence);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded,
                        std::move(*encoded));
  }

  void AdvanceToExactExclusion(std::vector<std::uint64_t> frozen_frontier = {
                                   21, 34}) {
    AdvanceThroughBeginGroupTerm();
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Operation().current_directives_.size(), 1);
    ASSERT_FALSE(
        Operation().current_directives_.front().spec_.payload_.empty());
    CommitFrozenSourceSuccess(frozen_frontier);
    ApplyNext<SetFailoverRecovery>();
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kOldAuthorityExcluded);
    EXPECT_EQ(Phase().required_applied_next_lsns_, frozen_frontier);
  }

  void AdvanceToPromotionPrepared() {
    AdvanceToExactExclusion();
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kCandidateCaughtUp);
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPreparing);

    control::PromotionPreparedEvidence evidence{
        .parent_history_id = Hex(intent_.parent_history_id_),
        .frozen_applied_next_lsns = {21, 34},
        .population_generation = 11,
        .population_digest = 13,
        .catalog_generation = 17,
        .catalog_dump_crc64 = 19,
        .child_history_id = std::string(40, 'c'),
    };
    auto encoded_evidence = control::EncodePromotionPreparedEvidence(evidence);
    ASSERT_TRUE(encoded_evidence.ok()) << encoded_evidence.status();
    CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded,
                        *encoded_evidence);

    MetaOperationEvidenceObs observed{
        .node_id_ = intent_.candidate_node_id_,
        .boot_incarnation_ = intent_.candidate_boot_id_,
        .assignment_id_ = intent_.candidate_assignment_id_,
        .operation_id_ = operation_id_,
        .kind_phase_ = "promotion-prepare:prepared",
        .evidence_hash_ = MetaSha256(*encoded_evidence),
        .evidence_ = *encoded_evidence,
        .group_id_ = intent_.group_id_,
        .group_term_ = intent_.group_term_,
        .population_manifest_revision_ = intent_.population_manifest_revision_,
        .partition_replication_epoch_ = intent_.partition_replication_epoch_,
        .replication_history_id_ = intent_.parent_history_id_,
    };
    const MetaObservationIdentity identity{intent_.candidate_node_id_,
                                           intent_.candidate_boot_id_, 1};
    ASSERT_TRUE(observations_
                    .Ingest(MetaObservation{.identity_ = identity,
                                            .payload_ = std::move(observed)},
                            MetaStoresFacts(stores_), now_unix_ms_)
                    .ok());
    ApplyNext<TransitionOperationPhase>();
    ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPrepared);
  }

  void SetActivatedCandidateRuntime() {
    MetaDataControlRuntimeNode* candidate =
        RuntimeNode(intent_.candidate_node_id_);
    ASSERT_NE(candidate, nullptr);
    ASSERT_EQ(candidate->groups_.size(), 1);
    const auto grant = stores_.grant_.GroupState(intent_.group_id_);
    ASSERT_TRUE(grant.has_value() && grant->grant_.has_value());
    MetaDataControlRuntimeGroup& group = candidate->groups_.front();
    group.group_term_ = intent_.group_term_;
    group.authority_version_ = intent_.authority_version_ + 1;
    group.grant_revision_ = grant->grant_->grant_revision_;
    candidate->projection_hash_ = Bytes<32>(0x72);
    candidate->confirmed_serving_lease_ = control::LeaseGranted{
        .nonce = Bytes<16>(0x73),
        .leader_id = 1,
        .raft_term = 9,
        .leadership_generation = kLeadershipGeneration,
        .data_boot_id = Hex(intent_.candidate_boot_id_),
        .projection_hash = candidate->projection_hash_,
        .group_id = intent_.group_id_,
        .assignment_id = intent_.candidate_assignment_id_,
        .group_term = intent_.group_term_,
        .authority_version = intent_.authority_version_ + 1,
        .grant_revision = grant->grant_->grant_revision_,
        .granted_duration_ms = 1000,
    };
  }

  void SetUp() override {
    RegisterNode former;
    former.node_id_ = std::string(40, 'a');
    former.principal_ = "keylane://node/" + former.node_id_;
    former.endpoints_ = {"127.0.0.1:7001"};
    former.role_ = MetaNodeRole::kPrimary;
    Apply(former);

    RegisterNode candidate;
    candidate.node_id_ = std::string(40, 'b');
    candidate.principal_ = "keylane://node/" + candidate.node_id_;
    candidate.endpoints_ = {"127.0.0.1:7002"};
    candidate.role_ = MetaNodeRole::kReplica;
    Apply(candidate);

    CreateGroup create;
    create.group_id_ = "group-a";
    create.new_topology_epoch_ = 1;
    Apply(create);

    AssignNodeToGroup assign_former;
    assign_former.group_id_ = create.group_id_;
    assign_former.node_id_ = former.node_id_;
    assign_former.assignment_id_ = Bytes<16>(0x11);
    assign_former.role_ = MetaNodeRole::kPrimary;
    assign_former.expected_revision_ = 1;
    assign_former.new_topology_epoch_ = 2;
    Apply(assign_former);

    AssignNodeToGroup assign_candidate;
    assign_candidate.group_id_ = create.group_id_;
    assign_candidate.node_id_ = candidate.node_id_;
    assign_candidate.assignment_id_ = Bytes<16>(0x12);
    assign_candidate.role_ = MetaNodeRole::kReplica;
    assign_candidate.expected_revision_ = 2;
    assign_candidate.new_topology_epoch_ = 3;
    Apply(assign_candidate);

    PutPopulationManifest manifest;
    manifest.entries_ = {{1, 1}, {2, 1}};
    manifest.manifest_digest_ =
        MetaPopulationManifestStore::CanonicalDigest(manifest.entries_);
    Apply(manifest);

    SetGroupReplicationState population;
    population.group_id_ = create.group_id_;
    population.new_population_manifest_revision_ = 1;
    population.new_population_manifest_digest_ = manifest.manifest_digest_;
    population.new_partition_replication_epoch_ = 1;
    population.new_topology_epoch_ = 4;
    Apply(population);

    PutPolicy policy;
    policy.policy_id_ = "lease-policy";
    policy.version_ = 1;
    policy.content_ = "finite-lease";
    policy.content_hash_ = MetaSha256(policy.content_);
    Apply(policy);

    BeginGroupTerm first_term;
    first_term.group_id_ = create.group_id_;
    first_term.new_term_ = 1;
    Apply(first_term);

    const MetaGrantSpec old_grant{.lease_duration_ms_ = 5000,
                                  .policy_id_ = policy.policy_id_,
                                  .policy_version_ = policy.version_};
    const auto before_activation =
        stores_.topology_.FindGroup(create.group_id_);
    ASSERT_TRUE(before_activation.has_value());
    ActivateAuthority activate;
    activate.group_id_ = create.group_id_;
    activate.expected_term_ = 1;
    activate.new_owner_ = former.node_id_;
    activate.grant_ = old_grant;
    activate.new_authority_version_ = 1;
    activate.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
    activate.new_config_epoch_ = before_activation->config_epoch_ + 1;
    Apply(activate);

    const auto group = stores_.topology_.FindGroup(create.group_id_);
    const auto grant = stores_.grant_.GroupState(create.group_id_);
    ASSERT_TRUE(group.has_value());
    ASSERT_TRUE(grant.has_value() && grant->grant_.has_value());
    intent_ = FailoverIntent{
        .group_id_ = create.group_id_,
        .recovery_generation_ = 1,
        .attempt_timeout_ms_ = 120'000,
        .former_owner_node_id_ = former.node_id_,
        .former_owner_assignment_id_ = assign_former.assignment_id_,
        .former_owner_boot_id_ = Bytes<20>(0x21),
        .candidate_node_id_ = candidate.node_id_,
        .candidate_assignment_id_ = assign_candidate.assignment_id_,
        .candidate_boot_id_ = Bytes<20>(0x22),
        .group_term_ = 2,
        .authority_version_ = 1,
        .grant_revision_ = grant->grant_->grant_revision_,
        .old_grant_ = old_grant,
        .population_manifest_revision_ =
            group->record_.population_manifest_revision_,
        .population_manifest_digest_ =
            group->record_.population_manifest_digest_,
        .partition_replication_epoch_ =
            group->record_.partition_replication_epoch_,
        .parent_history_id_ = Bytes<20>(0x31),
        .flow_count_ = 2,
    };
    auto encoded_intent = EncodeFailoverIntent(intent_);
    ASSERT_TRUE(encoded_intent.ok()) << encoded_intent.status();
    operation_id_ = Bytes<16>(0x41);
    SubmitOperation submit;
    submit.operation_id_ = operation_id_;
    submit.kind_ = std::string(kFailoverOperationKind);
    submit.intent_ = *encoded_intent;
    submit.intent_hash_ = MetaSha256(submit.intent_);
    submit.replication_history_id_ = intent_.parent_history_id_;
    submit.policy_references_ = {
        {old_grant.policy_id_, old_grant.policy_version_}};

    // A fresh failover proposal is admitted against the same live candidate
    // cut used by deterministic candidate planning. Install that cut before
    // Apply() reaches the proposal hook; replays remain valid after the
    // observation expires because the exact intent is already committed.
    const MetaObservationIdentity identity{intent_.candidate_node_id_,
                                           intent_.candidate_boot_id_, 1};
    ASSERT_TRUE(observations_.AdoptSession(identity, now_unix_ms_).ok());
    ObserveCandidate(intent_.group_term_ - 1, {20, 33});
    Apply(submit);

    runtime_.leader_authority_eligible_ = true;
    runtime_.leadership_generation_ = kLeadershipGeneration;
    MetaDataControlRuntimeNode source{
        .node_id_ = intent_.former_owner_node_id_,
        .boot_id_ = Hex(intent_.former_owner_boot_id_),
        .session_id_ = Bytes<16>(0x51),
        .replication_history_id_ = intent_.parent_history_id_,
        .session_generation_ = 1,
        .leadership_generation_ = kLeadershipGeneration,
        .source_meta_applied_index_ = index_,
        .validated_committed_high_water_ = index_,
        .topology_epoch_ = stores_.topology_.TopologyEpoch(),
        .projection_hash_ = Bytes<32>(0x52),
    };
    source.groups_.push_back(MetaDataControlRuntimeGroup{
        .group_id_ = intent_.group_id_,
        .assignment_id_ = intent_.former_owner_assignment_id_,
        .group_term_ = intent_.group_term_ - 1,
        .authority_version_ = intent_.authority_version_,
        .grant_revision_ = intent_.grant_revision_,
        .manifest_revision_ = intent_.population_manifest_revision_,
        .manifest_digest_ = intent_.population_manifest_digest_,
        .partition_replication_epoch_ = intent_.partition_replication_epoch_,
    });
    source.health_ = control::HeartbeatHealth{
        .storage_ready = true,
        .population_ready = true,
        .draining = false,
        .active_groups = 1,
    };
    MetaDataControlRuntimeNode candidate_runtime{
        .node_id_ = intent_.candidate_node_id_,
        .boot_id_ = Hex(intent_.candidate_boot_id_),
        .session_id_ = Bytes<16>(0x53),
        .replication_history_id_ = Bytes<20>(0x61),
        .session_generation_ = 1,
        .leadership_generation_ = kLeadershipGeneration,
        .source_meta_applied_index_ = index_,
        .validated_committed_high_water_ = index_,
        .topology_epoch_ = stores_.topology_.TopologyEpoch(),
        .projection_hash_ = Bytes<32>(0x54),
    };
    candidate_runtime.groups_.push_back(MetaDataControlRuntimeGroup{
        .group_id_ = intent_.group_id_,
        .assignment_id_ = intent_.candidate_assignment_id_,
        .group_term_ = intent_.group_term_ - 1,
        .authority_version_ = intent_.authority_version_,
        .grant_revision_ = intent_.grant_revision_,
        .manifest_revision_ = intent_.population_manifest_revision_,
        .manifest_digest_ = intent_.population_manifest_digest_,
        .partition_replication_epoch_ = intent_.partition_replication_epoch_,
    });
    candidate_runtime.health_ = control::HeartbeatHealth{
        .storage_ready = true,
        .population_ready = true,
        .draining = false,
        .active_groups = 1,
    };
    runtime_.nodes_ = {std::move(source), std::move(candidate_runtime)};
  }

  MetaStores stores_;
  std::uint64_t index_ = 0;
  std::int64_t now_unix_ms_ = NowUnixMillis();
  FailoverIntent intent_;
  MetaOperationId operation_id_{};
  MetaObservationStore observations_;
  MetaDataControlRuntimeSnapshot runtime_;
};

TEST_F(ControlledFailoverPlannerTest,
       PersistsRecoveryBeforeEnteringSourceHolding) {
  auto recovery = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(recovery.ok()) << recovery.status();
  EXPECT_TRUE(recovery->hold_required_);
  EXPECT_FALSE(recovery->recovery_required_);
  EXPECT_EQ(recovery->proof_state_, MetaFailoverProofState::kPending);
  Apply(*recovery);

  auto phase = NextAs<TransitionOperationPhase>();
  ASSERT_TRUE(phase.ok()) << phase.status();
  Apply(*phase);
  EXPECT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHolding);
  EXPECT_TRUE(Operation().current_directives_.empty());
}

TEST_F(ControlledFailoverPlannerTest,
       AttemptDeadlineBeforeCutAbortsExactlyWhenOldPrimaryIsCurrent) {
  AdvanceToSourceHolding();

  auto abort = NextAs<AbortOperation>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kSourceHolding);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  EXPECT_FALSE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("attempt deadline expired"),
            std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       AttemptDeadlineBeforeCutKeepsRecoveryWhenOldPrimaryIsUnconfirmed) {
  AdvanceToSourceHolding();
  RemoveSourceRuntime();

  ApplyNext<SetFailoverRecovery>({.attempt_deadline_expired_ = true});
  ASSERT_EQ(stores_.failover_recovery_.Find(intent_.group_id_)->proof_state_,
            MetaFailoverProofState::kUnavailable);
  auto abort = NextAs<AbortOperation>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       AttemptDeadlineAfterBeginGroupTermLeavesRecoveryHandoff) {
  AdvanceThroughBeginGroupTerm();

  auto abort = NextAs<AbortOperation>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_,
            FailoverPhaseStage::kOldAuthorityExcluding);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       FrozenProofAtDeadlineIsPersistedBeforeAttemptFails) {
  AdvanceThroughBeginGroupTerm();
  ApplyNext<TransitionOperationPhase>();
  CommitFrozenSourceSuccess();

  ApplyNext<SetFailoverRecovery>({.attempt_deadline_expired_ = true});
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  EXPECT_EQ(recovery->proof_state_, MetaFailoverProofState::kExact);
  ASSERT_TRUE(recovery->frozen_proof_.has_value());

  auto abort = NextAs<AbortOperation>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_EQ(outcome->proven_next_lsns_, (std::vector<std::uint64_t>{21, 34}));
}

TEST_F(ControlledFailoverPlannerTest,
       FrozenSourceAuthorizationRemainsProjectedUntilPromotionPrepare) {
  AdvanceToExactExclusion({30, 50});

  const MetaOperationRecord excluded = Operation();
  ASSERT_EQ(excluded.current_directives_.size(), 1);
  const MetaCurrentDirective frozen = excluded.current_directives_.front();
  EXPECT_EQ(frozen.spec_.kind_, kMetaDirectiveAuthorizeSource);
  EXPECT_FALSE(frozen.spec_.payload_.empty());

  auto source = MetaControlProjector::ProjectNode(
      MetaCommittedView(stores_, index_), intent_.former_owner_node_id_);
  ASSERT_TRUE(source.ok()) << source.status();
  ASSERT_EQ(source->full_state.current_directives.size(), 1);
  EXPECT_EQ(
      source->full_state.current_directives.front().identity.directive_revision,
      frozen.directive_revision_);

  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  ObserveCandidate(intent_.group_term_, {30, 50});
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kCandidateCaughtUp);
  ASSERT_EQ(Operation().current_directives_.size(), 1);
  EXPECT_EQ(Operation().current_directives_.front(), frozen);

  source = MetaControlProjector::ProjectNode(MetaCommittedView(stores_, index_),
                                             intent_.former_owner_node_id_);
  ASSERT_TRUE(source.ok()) << source.status();
  ASSERT_EQ(source->full_state.current_directives.size(), 1);
  EXPECT_EQ(
      source->full_state.current_directives.front().identity.directive_revision,
      frozen.directive_revision_);

  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPreparing);
  ASSERT_EQ(Operation().current_directives_.size(), 1);
  EXPECT_EQ(Operation().current_directives_.front().spec_.kind_,
            kMetaDirectivePromotionPrepare);

  source = MetaControlProjector::ProjectNode(MetaCommittedView(stores_, index_),
                                             intent_.former_owner_node_id_);
  ASSERT_TRUE(source.ok()) << source.status();
  EXPECT_TRUE(source->full_state.current_directives.empty());
}

TEST_F(ControlledFailoverPlannerTest,
       SourceHoldRequiresFdsAckAndExactOrdinaryReceipt) {
  AdvanceToSourceHolding();
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  InstallSourceHold();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Operation().current_directives_.size(), 1);
  EXPECT_TRUE(Operation().current_directives_.front().spec_.payload_.empty());
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  RemoveSourceHold();
  CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded, "source-held");
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  InstallSourceHold();
  ApplyNext<TransitionOperationPhase>();
  EXPECT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHeld);
  EXPECT_TRUE(Operation().current_directives_.empty());
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateFailureBeforeCutoverAbortsExactThenReleasesAndClearsHold) {
  AdvanceToSourceHolding();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_FALSE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  EXPECT_FALSE(outcome->recovery_required_);
  Apply(*abort);

  ApplyNext<SetFailoverRecovery>();
  const auto released = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(released.has_value());
  EXPECT_FALSE(released->hold_required_);
  EXPECT_FALSE(released->recovery_required_);
  ApplyNext<ClearFailoverRecovery>();
  EXPECT_FALSE(stores_.failover_recovery_.Find(intent_.group_id_).has_value());
  auto done = Plan();
  ASSERT_TRUE(done.ok()) << done.status();
  EXPECT_FALSE(done->has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       SafeAbortCannotArchiveUntilRecoveryReleaseIsFullyCleared) {
  AdvanceToSourceHolding();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  const std::uint64_t operation_seq = Operation().operation_seq_;

  ArchiveOperations archive;
  archive.request_id_ = Bytes<16>(0x81);
  archive.operation_seqs_ = {operation_seq};
  MetaStores first_attempt = stores_;
  auto first = ApplyCommitted(first_attempt, index_ + 1, MetaCommand(archive),
                              "keylane://operator/failover-test", "now");
  EXPECT_EQ(first.verdict_, MetaAuditVerdict::kRejected);

  ApplyNext<SetFailoverRecovery>();
  const auto released = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(released.has_value());
  ASSERT_FALSE(released->hold_required_);
  ASSERT_FALSE(released->recovery_required_);
  archive.request_id_ = Bytes<16>(0x82);
  MetaStores second_attempt = stores_;
  auto second = ApplyCommitted(second_attempt, index_ + 1, MetaCommand(archive),
                               "keylane://operator/failover-test", "now");
  EXPECT_EQ(second.verdict_, MetaAuditVerdict::kRejected);

  ApplyNext<ClearFailoverRecovery>();
  Apply(archive);
  EXPECT_TRUE(stores_.operation_.FindArchivedBySeq(operation_seq).has_value());
}

TEST_F(ControlledFailoverPlannerTest, BlockedFailoverMakesAMixedArchiveAtomic) {
  AdvanceToSourceHolding();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  const std::uint64_t failover_seq = Operation().operation_seq_;

  SubmitOperation submit;
  submit.operation_id_ = Bytes<16>(0x85);
  submit.kind_ = "maintenance";
  submit.intent_ = "ready-to-archive";
  submit.intent_hash_ = MetaSha256(submit.intent_);
  Apply(submit);
  const std::uint64_t generic_seq =
      stores_.operation_.FindOperation(submit.operation_id_)->operation_seq_;
  AbortOperation generic_abort;
  generic_abort.operation_id_ = submit.operation_id_;
  Apply(generic_abort);

  ArchiveOperations archive;
  archive.operation_seqs_ = {generic_seq, failover_seq};
  MetaStores attempted = stores_;
  const auto rejected =
      ApplyCommitted(attempted, index_ + 1, MetaCommand(archive),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(rejected.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_TRUE(
      attempted.operation_.FindOperation(submit.operation_id_).has_value());
  EXPECT_FALSE(
      attempted.operation_.FindArchived(submit.operation_id_).has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       ClearedTerminalRecognizesAStartedSuccessorGeneration) {
  AdvanceToSourceHolding();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  const std::uint64_t operation_seq = Operation().operation_seq_;
  ApplyNext<SetFailoverRecovery>();
  ApplyNext<ClearFailoverRecovery>();

  SetFailoverRecovery successor;
  successor.group_id_ = intent_.group_id_;
  successor.expected_revision_ =
      *stores_.failover_recovery_.LastRevision(intent_.group_id_);
  successor.recovery_generation_ = intent_.recovery_generation_ + 1;
  successor.old_source_node_id_ = intent_.former_owner_node_id_;
  successor.old_source_assignment_id_ = intent_.former_owner_assignment_id_;
  successor.old_source_boot_incarnation_ = intent_.former_owner_boot_id_;
  successor.old_source_history_id_ = intent_.parent_history_id_;
  successor.excluded_authority_term_ = intent_.group_term_ - 1;
  successor.excluded_authority_version_ = intent_.authority_version_;
  successor.excluded_grant_revision_ = intent_.grant_revision_;
  successor.population_manifest_revision_ =
      intent_.population_manifest_revision_;
  successor.population_manifest_digest_ = intent_.population_manifest_digest_;
  successor.partition_replication_epoch_ = intent_.partition_replication_epoch_;
  successor.hold_required_ = true;
  Apply(successor);

  auto cleanup = Plan();
  ASSERT_TRUE(cleanup.ok()) << cleanup.status();
  EXPECT_FALSE(cleanup->has_value());

  ArchiveOperations archive;
  archive.request_id_ = Bytes<16>(0x84);
  archive.operation_seqs_ = {operation_seq};
  Apply(archive);
  EXPECT_TRUE(stores_.operation_.FindArchivedBySeq(operation_seq).has_value());
  EXPECT_EQ(
      stores_.failover_recovery_.Find(intent_.group_id_)->recovery_generation_,
      intent_.recovery_generation_ + 1);
}

TEST_F(ControlledFailoverPlannerTest,
       RejectsPreCutoverAbortAfterAuthorityWasFenced) {
  AdvanceThroughHoldAndToExcluding();
  auto stale_abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(stale_abort.ok()) << stale_abort.status();
  auto stale_outcome = DecodeControlledFailoverOutcome(stale_abort->reason_);
  ASSERT_TRUE(stale_outcome.ok()) << stale_outcome.status();
  ASSERT_FALSE(stale_outcome->recovery_required_);
  stale_abort->request_id_ = Bytes<16>(0x77);

  ApplyNext<BeginGroupTerm>();
  const MetaCommittedView fenced(stores_, index_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(*stale_abort), fenced, observations_)),
            MetaFailureClass::kDomainReject);

  const MetaApplyResult applied =
      ApplyCommitted(stores_, ++index_, MetaCommand(*stale_abort),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_EQ(Operation().lifecycle_, MetaOperationLifecycle::kRunning);
  const auto grant = stores_.grant_.GroupState(intent_.group_id_);
  ASSERT_TRUE(grant.has_value());
  EXPECT_TRUE(grant->fenced_);
}

TEST_F(ControlledFailoverPlannerTest,
       RejectsAuthorityCutAfterPreCutoverAbortCommittedFirst) {
  AdvanceThroughHoldAndToExcluding();
  auto stale_begin = NextAs<BeginGroupTerm>();
  ASSERT_TRUE(stale_begin.ok()) << stale_begin.status();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  stale_begin->request_id_ = Bytes<16>(0x79);

  const MetaCommittedView terminal(stores_, index_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(*stale_begin), terminal, observations_)),
            MetaFailureClass::kDomainReject);
  const MetaApplyResult applied =
      ApplyCommitted(stores_, ++index_, MetaCommand(*stale_begin),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
  const auto grant = stores_.grant_.GroupState(intent_.group_id_);
  ASSERT_TRUE(grant.has_value() && grant->grant_.has_value());
  EXPECT_FALSE(grant->fenced_);
  EXPECT_EQ(grant->group_term_, intent_.group_term_ - 1);
}

TEST_F(ControlledFailoverPlannerTest,
       RejectsQueuedAuthorityCutAfterPreCutSourceLossCheckpoint) {
  AdvanceThroughHoldAndToExcluding();
  auto stale_begin = NextAs<BeginGroupTerm>();
  ASSERT_TRUE(stale_begin.ok()) << stale_begin.status();

  ApplyNext<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  stale_begin->request_id_ = Bytes<16>(0x7a);
  const MetaCommittedView unavailable(stores_, index_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(*stale_begin), unavailable, observations_)),
            MetaFailureClass::kDomainReject);
  const MetaApplyResult applied =
      ApplyCommitted(stores_, ++index_, MetaCommand(*stale_begin),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
  const auto grant = stores_.grant_.GroupState(intent_.group_id_);
  ASSERT_TRUE(grant.has_value() && grant->grant_.has_value());
  EXPECT_FALSE(grant->fenced_);
  EXPECT_EQ(grant->group_term_, intent_.group_term_ - 1);
}

TEST_F(ControlledFailoverPlannerTest,
       RejectsQueuedLosslessAbortAfterPreCutSourceLossCheckpoint) {
  AdvanceToSourceHolding();
  auto stale_abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(stale_abort.ok()) << stale_abort.status();
  auto stale_outcome = DecodeControlledFailoverOutcome(stale_abort->reason_);
  ASSERT_TRUE(stale_outcome.ok()) << stale_outcome.status();
  ASSERT_EQ(stale_outcome->loss_, FailoverLossClassification::kExact);
  ASSERT_FALSE(stale_outcome->recovery_required_);

  ApplyNext<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  stale_abort->request_id_ = Bytes<16>(0x7b);
  const MetaCommittedView unavailable(stores_, index_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(*stale_abort), unavailable, observations_)),
            MetaFailureClass::kDomainReject);
  const MetaApplyResult applied =
      ApplyCommitted(stores_, ++index_, MetaCommand(*stale_abort),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_EQ(Operation().lifecycle_, MetaOperationLifecycle::kRunning);
  ASSERT_EQ(stores_.failover_recovery_.Find(intent_.group_id_)->proof_state_,
            MetaFailoverProofState::kUnavailable);
}

TEST_F(ControlledFailoverPlannerTest,
       OldPrimaryFailureBeforeCutoverAbortsUnknownAndRetainsRecoveryHandoff) {
  AdvanceToSourceHolding();
  ApplyNext<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  const auto unavailable = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(unavailable.has_value());
  EXPECT_EQ(unavailable->proof_state_, MetaFailoverProofState::kUnavailable);
  EXPECT_FALSE(unavailable->frozen_proof_.has_value());

  // The durable unavailable checkpoint is the degrade latch. A reconnect or
  // new Meta leader with empty liveness timers must not resume controlled
  // cutover after this point.
  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
  Apply(*abort);

  ApplyNext<SetFailoverRecovery>();
  const auto handoff = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(handoff.has_value());
  EXPECT_TRUE(handoff->hold_required_);
  EXPECT_TRUE(handoff->recovery_required_);
  EXPECT_EQ(handoff->proof_state_, MetaFailoverProofState::kUnavailable);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       SameBootReplacedSourceHistoryBeforeCutoverLatchesDegrade) {
  AdvanceToSourceHolding();
  ReplaceSourceRuntimeHistory();

  auto unavailable = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(unavailable.ok()) << unavailable.status();
  EXPECT_TRUE(unavailable->hold_required_);
  EXPECT_FALSE(unavailable->recovery_required_);
  EXPECT_EQ(unavailable->proof_state_, MetaFailoverProofState::kUnavailable);
  EXPECT_FALSE(unavailable->frozen_proof_.has_value());
  Apply(*unavailable);

  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       SimultaneousLossInSourceHoldingRetainsRecoveryHandoff) {
  AdvanceToSourceHolding();

  ApplyNext<SetFailoverRecovery>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kSourceHolding);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateFailureWaitsWhileSourceHoldingSourceIsInGrace) {
  AdvanceToSourceHolding();
  RemoveSourceRuntime();

  auto waiting = Plan({.candidate_unavailable_ = true});
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  ApplyNext<SetFailoverRecovery>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       SimultaneousLossInSourceHeldRetainsRecoveryHandoff) {
  AdvanceToSourceHolding();
  InstallSourceHold();
  ApplyNext<TransitionOperationPhase>();
  CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded, "source-held");
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHeld);

  ApplyNext<SetFailoverRecovery>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kSourceHeld);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateFailureWaitsWhileSourceHeldSourceIsInGrace) {
  AdvanceToSourceHolding();
  InstallSourceHold();
  ApplyNext<TransitionOperationPhase>();
  CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded, "source-held");
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHeld);
  RemoveSourceRuntime();

  auto waiting = Plan({.candidate_unavailable_ = true});
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  ApplyNext<SetFailoverRecovery>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       SimultaneousLossBeforeBeginGroupTermRetainsRecoveryHandoff) {
  AdvanceThroughHoldAndToExcluding();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kOldAuthorityExcluding);

  ApplyNext<SetFailoverRecovery>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_,
            FailoverPhaseStage::kOldAuthorityExcluding);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       UnhealthySourceCannotCrossTheAuthorityCut) {
  AdvanceThroughHoldAndToExcluding();
  MetaDataControlRuntimeNode* source =
      RuntimeNode(intent_.former_owner_node_id_);
  ASSERT_NE(source, nullptr);
  ASSERT_TRUE(source->health_.has_value());
  source->health_->storage_ready = false;

  EXPECT_FALSE(Readiness().former_owner_ready_);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  ApplyNext<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  auto abort = NextAs<AbortOperation>({.former_owner_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateFailureWaitsWhilePreCutoverSourceIsInGrace) {
  AdvanceThroughHoldAndToExcluding();
  RemoveSourceRuntime();

  auto waiting = Plan({.candidate_unavailable_ = true});
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  ApplyNext<SetFailoverRecovery>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       SameBootStaleCandidateFdsPausesCutoverUntilGraceExpires) {
  AdvanceThroughHoldAndToExcluding();
  MetaDataControlRuntimeNode* candidate =
      RuntimeNode(intent_.candidate_node_id_);
  ASSERT_NE(candidate, nullptr);
  candidate->groups_.front().group_term_ = 0;

  const auto readiness = Readiness();
  EXPECT_FALSE(readiness.candidate_ready_);
  EXPECT_FALSE(readiness.candidate_replaced_);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  EXPECT_FALSE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       SameBootCandidateAssignmentReplacementFailsBeforeCutoverImmediately) {
  AdvanceThroughHoldAndToExcluding();
  MetaDataControlRuntimeNode* candidate =
      RuntimeNode(intent_.candidate_node_id_);
  ASSERT_NE(candidate, nullptr);
  candidate->groups_.front().assignment_id_ = Bytes<16>(0x7a);

  const auto readiness = Readiness();
  EXPECT_FALSE(readiness.candidate_ready_);
  EXPECT_TRUE(readiness.candidate_replaced_);
  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_,
            FailoverPhaseStage::kOldAuthorityExcluding);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
}

TEST_F(ControlledFailoverPlannerTest,
       StaleCandidateObservationPausesCutoverUntilGraceExpires) {
  AdvanceThroughHoldAndToExcluding();
  now_unix_ms_ += 30'001;

  const auto readiness = Readiness();
  EXPECT_FALSE(readiness.candidate_ready_);
  EXPECT_FALSE(readiness.candidate_replaced_);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
}

TEST_F(ControlledFailoverPlannerTest,
       OldPrimaryDisconnectBeforeFrozenDispatchWaitsForGrace) {
  AdvanceThroughBeginGroupTerm();
  RemoveSourceRuntime();

  auto waiting = Plan(/*liveness=*/{});
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  EXPECT_EQ(recovery->proof_state_, MetaFailoverProofState::kPending);
}

TEST_F(ControlledFailoverPlannerTest,
       OldPrimaryDisconnectAfterFrozenDispatchWaitsForGrace) {
  AdvanceThroughBeginGroupTerm();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Operation().current_directives_.size(), 1U);
  ASSERT_FALSE(Operation().current_directives_.front().spec_.payload_.empty());
  RemoveSourceRuntime();

  auto waiting = Plan(/*liveness=*/{});
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  EXPECT_EQ(recovery->proof_state_, MetaFailoverProofState::kPending);
}

TEST_F(ControlledFailoverPlannerTest,
       MissingOldPrimaryAfterBeginPersistsUnavailableCandidateFrontier) {
  AdvanceThroughBeginGroupTerm();
  RemoveSourceRuntime();
  ApplyNext<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  EXPECT_EQ(recovery->proof_state_, MetaFailoverProofState::kUnavailable);
  EXPECT_FALSE(recovery->frozen_proof_.has_value());

  auto excluded =
      NextAs<TransitionOperationPhase>({.former_owner_unavailable_ = true});
  ASSERT_TRUE(excluded.ok()) << excluded.status();
  auto phase = DecodeFailoverPhase(excluded->kind_phase_blob_);
  ASSERT_TRUE(phase.ok()) << phase.status();
  EXPECT_EQ(phase->stage_, FailoverPhaseStage::kOldAuthorityExcluded);
  EXPECT_EQ(phase->required_applied_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
  auto expected_hash = ComputeFailoverUnavailableProofHash(
      intent_, phase->required_applied_next_lsns_);
  ASSERT_TRUE(expected_hash.ok()) << expected_hash.status();
  EXPECT_EQ(phase->old_authority_exclusion_hash_, *expected_hash);
  Apply(*excluded);
}

TEST_F(ControlledFailoverPlannerTest,
       SameBootReplacedSourceAssignmentAfterCutoverDoesNotBlockFallback) {
  AdvanceThroughBeginGroupTerm();
  MetaDataControlRuntimeNode* source =
      RuntimeNode(intent_.former_owner_node_id_);
  ASSERT_NE(source, nullptr);
  source->groups_.front().assignment_id_ = Bytes<16>(0x7b);

  const auto readiness = Readiness();
  EXPECT_FALSE(readiness.former_owner_ready_);
  EXPECT_TRUE(readiness.former_owner_replaced_);
  auto unavailable = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(unavailable.ok()) << unavailable.status();
  EXPECT_EQ(unavailable->proof_state_, MetaFailoverProofState::kUnavailable);
  EXPECT_FALSE(unavailable->frozen_proof_.has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       SameBootReplacedSourceHistoryAfterCutoverDoesNotBlockFallback) {
  AdvanceThroughBeginGroupTerm();
  ReplaceSourceRuntimeHistory();

  const auto readiness = Readiness();
  EXPECT_FALSE(readiness.former_owner_ready_);
  EXPECT_TRUE(readiness.former_owner_replaced_);
  auto unavailable = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(unavailable.ok()) << unavailable.status();
  EXPECT_EQ(unavailable->proof_state_, MetaFailoverProofState::kUnavailable);
  EXPECT_FALSE(unavailable->frozen_proof_.has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateFailureAfterCutoverAbortsAndLeavesRecoveryRequired) {
  AdvanceThroughBeginGroupTerm();
  RemoveSourceRuntime();
  ApplyNext<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  ApplyNext<TransitionOperationPhase>({.former_owner_unavailable_ = true});

  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  Apply(*abort);
  ApplyNext<SetFailoverRecovery>();
  const auto handoff = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(handoff.has_value());
  EXPECT_TRUE(handoff->hold_required_);
  EXPECT_TRUE(handoff->recovery_required_);
  EXPECT_EQ(handoff->proof_state_, MetaFailoverProofState::kUnavailable);
}

TEST_F(ControlledFailoverPlannerTest,
       RecoveryAbortCannotArchiveBeforeDurableHandoff) {
  AdvanceToExactExclusion();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  const std::uint64_t operation_seq = Operation().operation_seq_;

  ArchiveOperations archive;
  archive.request_id_ = Bytes<16>(0x83);
  archive.operation_seqs_ = {operation_seq};
  const MetaCommittedView before_handoff(stores_, index_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(archive), before_handoff, observations_)),
            MetaFailureClass::kDomainReject);
  MetaStores attempted = stores_;
  auto rejected = ApplyCommitted(attempted, index_ + 1, MetaCommand(archive),
                                 "keylane://operator/failover-test", "now");
  EXPECT_EQ(rejected.verdict_, MetaAuditVerdict::kRejected);

  auto handoff_command = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(handoff_command.ok()) << handoff_command.status();
  EXPECT_TRUE(
      ValidateFailoverTerminalCleanupCommand(*handoff_command, stores_).ok());
  Apply(*handoff_command);
  const auto handoff = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(handoff.has_value());
  ASSERT_TRUE(handoff->hold_required_);
  ASSERT_TRUE(handoff->recovery_required_);
  Apply(archive);
  EXPECT_TRUE(stores_.operation_.FindArchivedBySeq(operation_seq).has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       PersistsCommittedFrozenProofBeforeCandidateFailureHandoff) {
  AdvanceThroughBeginGroupTerm();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Operation().current_directives_.size(), 1);
  CommitFrozenSourceSuccess({21, 34});

  auto persist = NextAs<SetFailoverRecovery>({.candidate_unavailable_ = true});
  ASSERT_TRUE(persist.ok()) << persist.status();
  EXPECT_EQ(persist->proof_state_, MetaFailoverProofState::kExact);
  ASSERT_TRUE(persist->frozen_proof_.has_value());
  EXPECT_EQ(persist->frozen_proof_->final_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
  Apply(*persist);

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateFailureDoesNotPreserveExactWhenOldSourceIsDisconnected) {
  AdvanceToExactExclusion();
  RemoveSourceRuntime();

  // Candidate failure must hand recovery off immediately rather than wait for
  // either failed participant. With no current old-source session, its
  // historical frozen proof is no longer evidence that the exact bytes remain
  // recoverable, even though the ordinary source grace has not elapsed yet.
  auto downgrade =
      NextAs<SetFailoverRecovery>({.candidate_unavailable_ = true});
  ASSERT_TRUE(downgrade.ok()) << downgrade.status();
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  Apply(*downgrade);

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  const auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_EQ(outcome->proven_next_lsns_, (std::vector<std::uint64_t>{21, 34}));
}

TEST_F(ControlledFailoverPlannerTest,
       RejectsExactAbortAfterRecoveryAvailabilityDowngraded) {
  AdvanceToExactExclusion();
  auto stale_abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(stale_abort.ok()) << stale_abort.status();
  auto stale_outcome = DecodeControlledFailoverOutcome(stale_abort->reason_);
  ASSERT_TRUE(stale_outcome.ok()) << stale_outcome.status();
  ASSERT_EQ(stale_outcome->loss_, FailoverLossClassification::kExact);
  stale_abort->request_id_ = Bytes<16>(0x78);

  RemoveSourceRuntime();
  ApplyNext<SetFailoverRecovery>({.candidate_unavailable_ = true});
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  ASSERT_EQ(recovery->proof_state_, MetaFailoverProofState::kUnavailable);

  const MetaCommittedView downgraded(stores_, index_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(*stale_abort), downgraded, observations_)),
            MetaFailureClass::kDomainReject);

  const MetaApplyResult applied =
      ApplyCommitted(stores_, ++index_, MetaCommand(*stale_abort),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_EQ(Operation().lifecycle_, MetaOperationLifecycle::kRunning);
}

TEST_F(ControlledFailoverPlannerTest,
       RejectsRecoveryDowngradeOrderedAfterExactTerminalOutcome) {
  AdvanceToExactExclusion();
  const MetaDataControlRuntimeNode saved_source =
      *RuntimeNode(intent_.former_owner_node_id_);
  RemoveSourceRuntime();
  auto stale_downgrade =
      NextAs<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  ASSERT_TRUE(stale_downgrade.ok()) << stale_downgrade.status();
  ASSERT_EQ(stale_downgrade->proof_state_,
            MetaFailoverProofState::kUnavailable);

  runtime_.nodes_.push_back(saved_source);
  auto exact_abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(exact_abort.ok()) << exact_abort.status();
  auto outcome = DecodeControlledFailoverOutcome(exact_abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  ASSERT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  Apply(*exact_abort);

  stale_downgrade->request_id_ = Bytes<16>(0x79);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(*stale_downgrade),
                MetaCommittedView(stores_, index_), observations_)),
            MetaFailureClass::kDomainReject);
  const MetaApplyResult applied =
      ApplyCommitted(stores_, ++index_, MetaCommand(*stale_downgrade),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  EXPECT_EQ(recovery->proof_state_, MetaFailoverProofState::kExact);
}

TEST_F(ControlledFailoverPlannerTest,
       TerminalPendingHandoffDowngradesAfterOldSourceReplacement) {
  AdvanceThroughBeginGroupTerm();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  ApplyNext<SetFailoverRecovery>();

  const auto pending = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(pending.has_value());
  ASSERT_TRUE(pending->recovery_required_);
  ASSERT_EQ(pending->proof_state_, MetaFailoverProofState::kPending);

  ReplaceSourceRuntimeBoot();
  auto downgrade = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(downgrade.ok()) << downgrade.status();
  EXPECT_TRUE(downgrade->hold_required_);
  EXPECT_TRUE(downgrade->recovery_required_);
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  EXPECT_FALSE(downgrade->frozen_proof_.has_value());
  Apply(*downgrade);
}

TEST_F(ControlledFailoverPlannerTest,
       TerminalExactHandoffWaitsForGraceBeforeAvailabilityDowngrade) {
  AdvanceToExactExclusion();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  ApplyNext<SetFailoverRecovery>();
  RemoveSourceRuntime();

  auto reconnect_grace = Plan();
  ASSERT_TRUE(reconnect_grace.ok()) << reconnect_grace.status();
  EXPECT_FALSE(reconnect_grace->has_value());

  auto downgrade =
      NextAs<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  ASSERT_TRUE(downgrade.ok()) << downgrade.status();
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  EXPECT_EQ(downgrade->frozen_proof_->final_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
  Apply(*downgrade);
}

TEST_F(ControlledFailoverPlannerTest,
       ArchivedRecoveryHandoffStillTracksOldSourceAvailability) {
  AdvanceToExactExclusion();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  ApplyNext<SetFailoverRecovery>();
  const std::uint64_t operation_seq = Operation().operation_seq_;

  ArchiveOperations archive;
  archive.operation_seqs_ = {operation_seq};
  Apply(archive);
  ASSERT_TRUE(stores_.operation_.FindArchivedBySeq(operation_seq).has_value());

  ReplaceSourceRuntimeBoot();
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  auto downgrade = detail::PlanFailoverRecoveryAvailabilityStep(
      *recovery, runtime_, /*former_owner_unavailable=*/false);
  ASSERT_TRUE(downgrade.has_value());
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  EXPECT_TRUE(
      ValidateFailoverRecoveryAvailabilityCommand(*downgrade, stores_).ok());
  auto changed_flags = *downgrade;
  changed_flags.recovery_required_ = false;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverRecoveryAvailabilityCommand(
                changed_flags, stores_)),
            MetaFailureClass::kDomainReject);
  Apply(*downgrade);
}

TEST_F(ControlledFailoverPlannerTest,
       ArchivedRecoveryHandoffRejectsSameBootWithReplacedHistory) {
  AdvanceToExactExclusion();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  ApplyNext<SetFailoverRecovery>();
  const std::uint64_t operation_seq = Operation().operation_seq_;

  ArchiveOperations archive;
  archive.operation_seqs_ = {operation_seq};
  Apply(archive);
  ASSERT_TRUE(stores_.operation_.FindArchivedBySeq(operation_seq).has_value());

  ReplaceSourceRuntimeHistory();
  const auto recovery = stores_.failover_recovery_.Find(intent_.group_id_);
  ASSERT_TRUE(recovery.has_value());
  auto downgrade = detail::PlanFailoverRecoveryAvailabilityStep(
      *recovery, runtime_, /*former_owner_unavailable=*/false);
  ASSERT_TRUE(downgrade.has_value());
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  EXPECT_EQ(downgrade->frozen_proof_->final_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateRemovalStillAllowsRecoveryHandoffAbort) {
  AdvanceToExactExclusion();
  const auto group = stores_.topology_.FindGroup(intent_.group_id_);
  ASSERT_TRUE(group.has_value());
  RemoveNodeFromGroup remove;
  remove.group_id_ = intent_.group_id_;
  remove.node_id_ = intent_.candidate_node_id_;
  remove.expected_revision_ = group->revision_;
  remove.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
  Apply(remove);

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  ASSERT_TRUE(outcome->recovery_required_);
  Apply(*abort);
  EXPECT_EQ(Operation().lifecycle_, MetaOperationLifecycle::kAborted);
}

TEST_F(ControlledFailoverPlannerTest,
       OldPrimaryLossAfterExactFreezeDoesNotBlockCandidateCatchup) {
  AdvanceToExactExclusion();
  RemoveSourceRuntime();

  auto downgrade =
      NextAs<SetFailoverRecovery>({.former_owner_unavailable_ = true});
  ASSERT_TRUE(downgrade.ok()) << downgrade.status();
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  EXPECT_EQ(downgrade->frozen_proof_->final_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
  Apply(*downgrade);

  auto caught_up =
      NextAs<TransitionOperationPhase>({.former_owner_unavailable_ = true});
  ASSERT_TRUE(caught_up.ok()) << caught_up.status();
  auto phase = DecodeFailoverPhase(caught_up->kind_phase_blob_);
  ASSERT_TRUE(phase.ok()) << phase.status();
  EXPECT_EQ(phase->stage_, FailoverPhaseStage::kCandidateCaughtUp);
  EXPECT_EQ(phase->required_applied_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
}

TEST_F(ControlledFailoverPlannerTest,
       ReplacedOldPrimaryBootDowngradesUnreachedExactFrontierAndContinues) {
  AdvanceToExactExclusion({30, 50});
  ReplaceSourceRuntimeBoot();

  // The old boot held an exact historical frontier, but no surviving holder
  // has reached it. Preserve that proof for audit while switching the usable
  // recovery basis to the live candidate instead of waiting for the old boot.
  auto downgrade = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(downgrade.ok()) << downgrade.status();
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  EXPECT_EQ(downgrade->frozen_proof_->final_next_lsns_,
            (std::vector<std::uint64_t>{30, 50}));
  Apply(*downgrade);

  auto rebase = NextAs<TransitionOperationPhase>();
  ASSERT_TRUE(rebase.ok()) << rebase.status();
  auto rebased_phase = DecodeFailoverPhase(rebase->kind_phase_blob_);
  ASSERT_TRUE(rebased_phase.ok()) << rebased_phase.status();
  EXPECT_EQ(rebased_phase->stage_, FailoverPhaseStage::kOldAuthorityExcluded);
  EXPECT_EQ(rebased_phase->required_applied_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
  auto unavailable_hash = ComputeFailoverUnavailableProofHash(
      intent_, rebased_phase->required_applied_next_lsns_);
  ASSERT_TRUE(unavailable_hash.ok()) << unavailable_hash.status();
  EXPECT_EQ(rebased_phase->old_authority_exclusion_hash_, *unavailable_hash);
  EXPECT_TRUE(ValidateFailoverFailSafeCheckpoint(
                  *rebase, MetaCommittedView(stores_, index_), observations_)
                  .ok())
      << "fail-safe must retain, but cannot replace, the frozen source "
         "authorization while rebasing the frontier";
  auto changed_authorization = *rebase;
  changed_authorization.current_directives_.front().attempt_id_[0] ^= 0xff;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverFailSafeCheckpoint(
                changed_authorization, MetaCommittedView(stores_, index_),
                observations_)),
            MetaFailureClass::kDomainReject);
  Apply(*rebase);

  auto caught_up = NextAs<TransitionOperationPhase>();
  ASSERT_TRUE(caught_up.ok()) << caught_up.status();
  auto caught_up_phase = DecodeFailoverPhase(caught_up->kind_phase_blob_);
  ASSERT_TRUE(caught_up_phase.ok()) << caught_up_phase.status();
  EXPECT_EQ(caught_up_phase->stage_, FailoverPhaseStage::kCandidateCaughtUp);
  EXPECT_EQ(caught_up_phase->required_applied_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
}

TEST_F(ControlledFailoverPlannerTest,
       ExactSourceProofDoesNotClaimLosslessWhenNoHolderReachedIt) {
  AdvanceToExactExclusion({30, 50});
  RemoveSourceRuntime();

  auto downgrade = NextAs<SetFailoverRecovery>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(downgrade.ok()) << downgrade.status();
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  EXPECT_EQ(downgrade->frozen_proof_->final_next_lsns_,
            (std::vector<std::uint64_t>{30, 50}));
  Apply(*downgrade);

  auto abort = NextAs<AbortOperation>(
      {.former_owner_unavailable_ = true, .candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_,
            FailoverPhaseStage::kOldAuthorityExcluded);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       PromotionPrepareFailureAbortsWithoutAuthorityActivation) {
  AdvanceToExactExclusion();
  ApplyNext<TransitionOperationPhase>();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPreparing);
  CommitCurrentResult(MetaDirectiveResultStatus::kFailed,
                      "injected prepare failure");

  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kPromotionPreparing);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("injected prepare failure"),
            std::string::npos);
  const auto grant = stores_.grant_.GroupState(intent_.group_id_);
  ASSERT_TRUE(grant.has_value());
  EXPECT_TRUE(grant->fenced_);
  EXPECT_FALSE(grant->grant_.has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       SourceHoldFailureWinsConcurrentAttemptDeadline) {
  AdvanceToSourceHolding();
  InstallSourceHold();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kSourceHolding);
  CommitCurrentResult(MetaDirectiveResultStatus::kRejected,
                      "source refused history hold");

  auto abort = NextAs<AbortOperation>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kSourceHolding);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  EXPECT_FALSE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("source refused history hold"),
            std::string::npos);
  EXPECT_EQ(outcome->reason_.find("deadline"), std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       MalformedPromotionPreparedReceiptAbortsTheFencedAttempt) {
  AdvanceToExactExclusion();
  ApplyNext<TransitionOperationPhase>();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPreparing);
  CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded,
                      "not-promotion-prepared-evidence");

  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kPromotionPreparing);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("invalid promotion prepared evidence"),
            std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       ConflictingCurrentPromotionEvidenceAbortsTheFencedAttempt) {
  AdvanceToExactExclusion();
  ApplyNext<TransitionOperationPhase>();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPreparing);

  control::PromotionPreparedEvidence receipt_evidence{
      .parent_history_id = Hex(intent_.parent_history_id_),
      .frozen_applied_next_lsns = {21, 34},
      .population_generation = 11,
      .population_digest = 13,
      .catalog_generation = 17,
      .catalog_dump_crc64 = 19,
      .child_history_id = std::string(40, 'c'),
  };
  auto encoded_receipt =
      control::EncodePromotionPreparedEvidence(receipt_evidence);
  ASSERT_TRUE(encoded_receipt.ok()) << encoded_receipt.status();
  CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded, *encoded_receipt);

  control::PromotionPreparedEvidence conflicting_evidence = receipt_evidence;
  conflicting_evidence.child_history_id = std::string(40, 'd');
  auto encoded_conflict =
      control::EncodePromotionPreparedEvidence(conflicting_evidence);
  ASSERT_TRUE(encoded_conflict.ok()) << encoded_conflict.status();
  MetaOperationEvidenceObs observed{
      .node_id_ = intent_.candidate_node_id_,
      .boot_incarnation_ = intent_.candidate_boot_id_,
      .assignment_id_ = intent_.candidate_assignment_id_,
      .operation_id_ = operation_id_,
      .kind_phase_ = "promotion-prepare:prepared",
      .evidence_hash_ = MetaSha256(*encoded_conflict),
      .evidence_ = *encoded_conflict,
      .group_id_ = intent_.group_id_,
      .group_term_ = intent_.group_term_,
      .population_manifest_revision_ = intent_.population_manifest_revision_,
      .partition_replication_epoch_ = intent_.partition_replication_epoch_,
      .replication_history_id_ = intent_.parent_history_id_,
  };
  const MetaObservationIdentity identity{intent_.candidate_node_id_,
                                         intent_.candidate_boot_id_, 1};
  ASSERT_TRUE(observations_
                  .Ingest(MetaObservation{.identity_ = identity,
                                          .payload_ = std::move(observed)},
                          MetaStoresFacts(stores_), now_unix_ms_)
                  .ok());

  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kPromotionPreparing);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("conflicts with its terminal receipt"),
            std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       MalformedFrozenSourceEvidenceAbortsTheFencedAttempt) {
  AdvanceThroughBeginGroupTerm();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kOldAuthorityExcluding);
  ASSERT_EQ(Operation().current_directives_.size(), 1);
  CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded,
                      "not-frozen-source-evidence");

  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_,
            FailoverPhaseStage::kOldAuthorityExcluding);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("invalid frozen-source evidence"),
            std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       FrozenSourceTerminalFailureAbortsTheFencedAttempt) {
  AdvanceThroughBeginGroupTerm();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kOldAuthorityExcluding);
  CommitCurrentResult(MetaDirectiveResultStatus::kRejected,
                      "source refused frozen capture");

  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("source refused frozen capture"),
            std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       UnhealthyPreparedCandidateCannotActivateAuthority) {
  AdvanceToPromotionPrepared();
  MetaDataControlRuntimeNode* candidate =
      RuntimeNode(intent_.candidate_node_id_);
  ASSERT_NE(candidate, nullptr);
  ASSERT_TRUE(candidate->health_.has_value());
  candidate->health_->population_ready = false;

  EXPECT_FALSE(Readiness().candidate_ready_);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       RejectsAuthorityActivationAfterPreparedAbortCommittedFirst) {
  AdvanceToPromotionPrepared();
  auto stale_activate = NextAs<ActivateAuthority>();
  ASSERT_TRUE(stale_activate.ok()) << stale_activate.status();
  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  Apply(*abort);
  stale_activate->request_id_ = Bytes<16>(0x7a);

  const MetaCommittedView terminal(stores_, index_);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(*stale_activate), terminal, observations_)),
            MetaFailureClass::kDomainReject);
  const MetaApplyResult applied =
      ApplyCommitted(stores_, ++index_, MetaCommand(*stale_activate),
                     "keylane://operator/failover-test", "now");
  EXPECT_EQ(applied.verdict_, MetaAuditVerdict::kRejected);
  const auto grant = stores_.grant_.GroupState(intent_.group_id_);
  ASSERT_TRUE(grant.has_value());
  EXPECT_TRUE(grant->fenced_);
  EXPECT_FALSE(grant->grant_.has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       UnhealthyActivatedCandidateCannotClaimServing) {
  AdvanceToPromotionPrepared();
  ApplyNext<ActivateAuthority>();
  SetActivatedCandidateRuntime();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kAuthorityActivated);
  MetaDataControlRuntimeNode* candidate =
      RuntimeNode(intent_.candidate_node_id_);
  ASSERT_NE(candidate, nullptr);
  ASSERT_TRUE(candidate->health_.has_value());
  candidate->confirmed_serving_lease_.reset();
  candidate->health_->storage_ready = false;

  EXPECT_FALSE(Readiness().candidate_ready_);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_TRUE(outcome->recovery_required_);
}

TEST_F(ControlledFailoverPlannerTest,
       ConfirmedServingEventSurvivesLaterHealthRegression) {
  AdvanceToPromotionPrepared();
  ApplyNext<ActivateAuthority>();
  SetActivatedCandidateRuntime();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kAuthorityActivated);
  MetaDataControlRuntimeNode* candidate =
      RuntimeNode(intent_.candidate_node_id_);
  ASSERT_NE(candidate, nullptr);
  ASSERT_TRUE(candidate->health_.has_value());
  ASSERT_TRUE(candidate->confirmed_serving_lease_.has_value());
  candidate->health_->storage_ready = false;

  auto serving = NextAs<TransitionOperationPhase>();
  ASSERT_TRUE(serving.ok()) << serving.status();
  auto phase = DecodeFailoverPhase(serving->kind_phase_blob_);
  ASSERT_TRUE(phase.ok()) << phase.status();
  EXPECT_EQ(phase->stage_, FailoverPhaseStage::kServing);
}

TEST_F(ControlledFailoverPlannerTest,
       PrepareFailureCannotClaimExactAfterSourceHoldDisappears) {
  AdvanceToExactExclusion();
  ApplyNext<TransitionOperationPhase>();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPreparing);
  CommitCurrentResult(MetaDirectiveResultStatus::kFailed,
                      "injected prepare failure");
  RemoveSourceRuntime();

  auto downgrade = NextAs<SetFailoverRecovery>();
  ASSERT_TRUE(downgrade.ok()) << downgrade.status();
  EXPECT_EQ(downgrade->proof_state_, MetaFailoverProofState::kUnavailable);
  ASSERT_TRUE(downgrade->frozen_proof_.has_value());
  Apply(*downgrade);

  auto abort = NextAs<AbortOperation>();
  ASSERT_TRUE(abort.ok()) << abort.status();
  EXPECT_TRUE(abort->data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kUnknown);
}

TEST_F(ControlledFailoverPlannerTest,
       PreparedReceiptWaitsForCurrentSessionEvidenceBeforeAdvancing) {
  AdvanceToExactExclusion();
  ApplyNext<TransitionOperationPhase>();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kPromotionPreparing);

  control::PromotionPreparedEvidence prepared{
      .parent_history_id = Hex(intent_.parent_history_id_),
      .frozen_applied_next_lsns = {21, 34},
      .population_generation = 11,
      .population_digest = 13,
      .catalog_generation = 17,
      .catalog_dump_crc64 = 19,
      .child_history_id = std::string(40, 'c'),
  };
  auto encoded = control::EncodePromotionPreparedEvidence(prepared);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  CommitCurrentResult(MetaDirectiveResultStatus::kSucceeded, *encoded);

  EXPECT_FALSE(Readiness().candidate_ready_);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  MetaOperationEvidenceObs observed{
      .node_id_ = intent_.candidate_node_id_,
      .boot_incarnation_ = intent_.candidate_boot_id_,
      .assignment_id_ = intent_.candidate_assignment_id_,
      .operation_id_ = operation_id_,
      .kind_phase_ = "promotion-prepare:prepared",
      .evidence_hash_ = MetaSha256(*encoded),
      .evidence_ = *encoded,
      .group_id_ = intent_.group_id_,
      .group_term_ = intent_.group_term_,
      .population_manifest_revision_ = intent_.population_manifest_revision_,
      .partition_replication_epoch_ = intent_.partition_replication_epoch_,
      .replication_history_id_ = intent_.parent_history_id_,
  };
  const MetaObservationIdentity identity{intent_.candidate_node_id_,
                                         intent_.candidate_boot_id_, 1};
  ASSERT_TRUE(
      observations_
          .Ingest(MetaObservation{.identity_ = identity, .payload_ = observed},
                  MetaStoresFacts(stores_), now_unix_ms_)
          .ok());

  EXPECT_TRUE(Readiness().candidate_ready_);
  now_unix_ms_ += 30'001;
  EXPECT_FALSE(Readiness().candidate_ready_);
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  ASSERT_TRUE(observations_
                  .Ingest(MetaObservation{.identity_ = identity,
                                          .payload_ = std::move(observed)},
                          MetaStoresFacts(stores_), now_unix_ms_)
                  .ok());
  EXPECT_TRUE(Readiness().candidate_ready_);
  auto transition = NextAs<TransitionOperationPhase>();
  ASSERT_TRUE(transition.ok()) << transition.status();
  auto phase = DecodeFailoverPhase(transition->kind_phase_blob_);
  ASSERT_TRUE(phase.ok()) << phase.status();
  EXPECT_EQ(phase->stage_, FailoverPhaseStage::kPromotionPrepared);
}

TEST_F(ControlledFailoverPlannerTest,
       CandidateLossAfterActivationCommitIsAttributedToActivatedStage) {
  AdvanceToPromotionPrepared();
  ApplyNext<ActivateAuthority>();

  // Activation is already committed even though the phase checkpoint is
  // still promotion-prepared. Recovery must not describe this as a fenced
  // pre-activation failure; replacement now requires a successor term.
  auto activated =
      NextAs<TransitionOperationPhase>({.candidate_unavailable_ = true});
  ASSERT_TRUE(activated.ok()) << activated.status();
  auto phase = DecodeFailoverPhase(activated->kind_phase_blob_);
  ASSERT_TRUE(phase.ok()) << phase.status();
  EXPECT_EQ(phase->stage_, FailoverPhaseStage::kAuthorityActivated);
  Apply(*activated);

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kAuthorityActivated);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("new recovery term"), std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       ActivationCommitIsAttributedBeforeAttemptDeadlineFailure) {
  AdvanceToPromotionPrepared();
  ApplyNext<ActivateAuthority>();

  auto checkpoint =
      NextAs<TransitionOperationPhase>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(checkpoint.ok()) << checkpoint.status();
  Apply(*checkpoint);
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kAuthorityActivated);

  auto abort = NextAs<AbortOperation>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kAuthorityActivated);
  EXPECT_TRUE(outcome->recovery_required_);
  EXPECT_NE(outcome->reason_.find("new recovery term"), std::string::npos);
}

TEST_F(ControlledFailoverPlannerTest,
       ActivationCommitWinsConcurrentSourceProofDowngrade) {
  AdvanceToPromotionPrepared();
  ApplyNext<ActivateAuthority>();
  RemoveSourceRuntime();

  auto checkpoint = NextAs<TransitionOperationPhase>(
      {.former_owner_unavailable_ = true, .attempt_deadline_expired_ = true});
  ASSERT_TRUE(checkpoint.ok()) << checkpoint.status();
  auto phase = DecodeFailoverPhase(checkpoint->kind_phase_blob_);
  ASSERT_TRUE(phase.ok()) << phase.status();
  EXPECT_EQ(phase->stage_, FailoverPhaseStage::kAuthorityActivated);
}

TEST_F(ControlledFailoverPlannerTest,
       CurrentSessionServingEvidenceWinsAttemptDeadlineRace) {
  AdvanceToPromotionPrepared();
  ApplyNext<ActivateAuthority>();
  SetActivatedCandidateRuntime();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kAuthorityActivated);

  auto serving =
      NextAs<TransitionOperationPhase>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(serving.ok()) << serving.status();
  auto serving_phase = DecodeFailoverPhase(serving->kind_phase_blob_);
  ASSERT_TRUE(serving_phase.ok()) << serving_phase.status();
  EXPECT_EQ(serving_phase->stage_, FailoverPhaseStage::kServing);
  Apply(*serving);

  auto complete =
      NextAs<CompleteOperation>({.attempt_deadline_expired_ = true});
  ASSERT_TRUE(complete.ok()) << complete.status();
}

TEST_F(ControlledFailoverPlannerTest,
       CurrentSessionServingEvidenceWinsConcurrentSourceProofDowngrade) {
  AdvanceToPromotionPrepared();
  ApplyNext<ActivateAuthority>();
  SetActivatedCandidateRuntime();
  ApplyNext<TransitionOperationPhase>();
  ASSERT_EQ(Phase().stage_, FailoverPhaseStage::kAuthorityActivated);
  RemoveSourceRuntime();

  auto serving = NextAs<TransitionOperationPhase>(
      {.former_owner_unavailable_ = true, .attempt_deadline_expired_ = true});
  ASSERT_TRUE(serving.ok()) << serving.status();
  auto phase = DecodeFailoverPhase(serving->kind_phase_blob_);
  ASSERT_TRUE(phase.ok()) << phase.status();
  EXPECT_EQ(phase->stage_, FailoverPhaseStage::kServing);
}

TEST_F(ControlledFailoverPlannerTest,
       PreparedCandidateDisconnectWaitsForRevalidationGrace) {
  AdvanceToPromotionPrepared();
  RemoveCandidateRuntime();

  auto waiting = Plan(/*liveness=*/{});
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());
  const auto grant = stores_.grant_.GroupState(intent_.group_id_);
  ASSERT_TRUE(grant.has_value());
  EXPECT_TRUE(grant->fenced_);
  EXPECT_FALSE(grant->grant_.has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       PreparedCandidateMissingAfterGraceAbortsBeforeActivation) {
  AdvanceToPromotionPrepared();
  RemoveCandidateRuntime();

  auto abort = NextAs<AbortOperation>({.candidate_unavailable_ = true});
  ASSERT_TRUE(abort.ok()) << abort.status();
  auto outcome = DecodeControlledFailoverOutcome(abort->reason_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_EQ(outcome->terminal_stage_, FailoverPhaseStage::kPromotionPrepared);
  EXPECT_TRUE(outcome->recovery_required_);
  const auto grant = stores_.grant_.GroupState(intent_.group_id_);
  ASSERT_TRUE(grant.has_value());
  EXPECT_TRUE(grant->fenced_);
  EXPECT_FALSE(grant->grant_.has_value());
}

TEST_F(ControlledFailoverPlannerTest,
       ExactProofReachesServingAndCompletesOnlyAfterConfirmedLease) {
  AdvanceToPromotionPrepared();

  ApplyNext<ActivateAuthority>();
  ApplyNext<TransitionOperationPhase>();
  EXPECT_EQ(Phase().stage_, FailoverPhaseStage::kAuthorityActivated);
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  SetActivatedCandidateRuntime();
  ApplyNext<TransitionOperationPhase>();
  EXPECT_EQ(Phase().stage_, FailoverPhaseStage::kServing);
  ApplyNext<CompleteOperation>();
  EXPECT_EQ(Operation().lifecycle_, MetaOperationLifecycle::kCompleted);
  EXPECT_FALSE(Operation().data_loss_possible_);
  auto outcome = DecodeControlledFailoverOutcome(Operation().terminal_result_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_TRUE(outcome->succeeded_);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);

  ApplyNext<SetFailoverRecovery>();
  auto waiting_for_release_ack = Plan();
  ASSERT_TRUE(waiting_for_release_ack.ok()) << waiting_for_release_ack.status();
  EXPECT_FALSE(waiting_for_release_ack->has_value());
  RemoveSourceHold();
  ApplyNext<ClearFailoverRecovery>();
  auto done = Plan();
  ASSERT_TRUE(done.ok()) << done.status();
  EXPECT_FALSE(done->has_value());
}

}  // namespace
}  // namespace keylane::meta
