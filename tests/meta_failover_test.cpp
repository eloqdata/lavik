#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "gtest/gtest.h"
#include "keylane/meta/candidate_plan.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/failover.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/state_apply.h"

namespace keylane::meta {
namespace {

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

int64_t UnixMillisNow() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

FailoverIntent Intent() {
  return FailoverIntent{
      .group_id_ = "group-a",
      .recovery_generation_ = 5,
      .attempt_timeout_ms_ = 120'000,
      .former_owner_node_id_ = std::string(40, 'a'),
      .former_owner_assignment_id_ = Bytes<16>(1),
      .former_owner_boot_id_ = Bytes<20>(2),
      .candidate_node_id_ = std::string(40, 'b'),
      .candidate_assignment_id_ = Bytes<16>(3),
      .candidate_boot_id_ = Bytes<20>(4),
      .group_term_ = 2,
      .authority_version_ = 1,
      .grant_revision_ = 11,
      .old_grant_ = {.lease_duration_ms_ = 5000,
                     .policy_id_ = "lease-policy",
                     .policy_version_ = 1},
      .population_manifest_revision_ = 13,
      .population_manifest_digest_ = Bytes<32>(6),
      .partition_replication_epoch_ = 17,
      .parent_history_id_ = Bytes<20>(7),
      .flow_count_ = 2,
  };
}

TEST(MetaFailoverCodecTest, RoundTripsIntentAndPromotionPreparingPhase) {
  const FailoverIntent intent = Intent();
  auto encoded_intent = EncodeFailoverIntent(intent);
  ASSERT_TRUE(encoded_intent.ok()) << encoded_intent.status();
  auto decoded_intent = DecodeFailoverIntent(*encoded_intent);
  ASSERT_TRUE(decoded_intent.ok()) << decoded_intent.status();
  EXPECT_EQ(*decoded_intent, intent);

  const FailoverPhase phase{
      .stage_ = FailoverPhaseStage::kPromotionPreparing,
      .old_authority_exclusion_hash_ = Bytes<32>(8),
      .required_applied_next_lsns_ = {19, 23},
  };
  auto encoded_phase = EncodeFailoverPhase(phase);
  ASSERT_TRUE(encoded_phase.ok()) << encoded_phase.status();
  auto decoded_phase = DecodeFailoverPhase(*encoded_phase);
  ASSERT_TRUE(decoded_phase.ok()) << decoded_phase.status();
  EXPECT_EQ(*decoded_phase, phase);
}

TEST(MetaFailoverCodecTest, RejectsUnknownVersionTrailingBytesAndBadStageData) {
  auto encoded = EncodeFailoverIntent(Intent());
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  (*encoded)[4] = '\x03';
  EXPECT_EQ(MetaFailureClassOf(DecodeFailoverIntent(*encoded).status()),
            MetaFailureClass::kFailStop);

  encoded = EncodeFailoverIntent(Intent());
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  encoded->push_back('\0');
  EXPECT_EQ(MetaFailureClassOf(DecodeFailoverIntent(*encoded).status()),
            MetaFailureClass::kFailStop);

  FailoverPhase invalid{
      .stage_ = FailoverPhaseStage::kPromotionPrepared,
      .old_authority_exclusion_hash_ = Bytes<32>(8),
      .required_applied_next_lsns_ = {19},
  };
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverPhase(invalid).status()),
            MetaFailureClass::kDomainReject);

  FailoverIntent invalid_intent = Intent();
  invalid_intent.old_grant_.lease_duration_ms_ = 0;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverIntent(invalid_intent).status()),
            MetaFailureClass::kDomainReject);
  invalid_intent = Intent();
  invalid_intent.old_grant_.policy_id_.clear();
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverIntent(invalid_intent).status()),
            MetaFailureClass::kDomainReject);
  invalid_intent = Intent();
  invalid_intent.recovery_generation_ = 0;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverIntent(invalid_intent).status()),
            MetaFailureClass::kDomainReject);
  invalid_intent = Intent();
  invalid_intent.attempt_timeout_ms_ = 0;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverIntent(invalid_intent).status()),
            MetaFailureClass::kDomainReject);
  invalid_intent.attempt_timeout_ms_ =
      kMaxControlledFailoverAttemptTimeoutMs + 1;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverIntent(invalid_intent).status()),
            MetaFailureClass::kDomainReject);
  invalid_intent = Intent();
  invalid_intent.flow_count_ = 0;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverIntent(invalid_intent).status()),
            MetaFailureClass::kDomainReject);
  invalid_intent.flow_count_ = 1025;
  EXPECT_EQ(MetaFailureClassOf(EncodeFailoverIntent(invalid_intent).status()),
            MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverCodecTest,
     AttemptTimeoutIsCanonicalAndMissingDevelopmentLayoutFailsStop) {
  const FailoverIntent first = Intent();
  FailoverIntent second = first;
  ++second.attempt_timeout_ms_;
  auto first_encoded = EncodeFailoverIntent(first);
  auto second_encoded = EncodeFailoverIntent(second);
  ASSERT_TRUE(first_encoded.ok()) << first_encoded.status();
  ASSERT_TRUE(second_encoded.ok()) << second_encoded.status();
  EXPECT_NE(*first_encoded, *second_encoded);
  EXPECT_NE(MetaSha256(*first_encoded), MetaSha256(*second_encoded));

  auto first_proof = ComputeFailoverUnavailableProofHash(first, {19, 23});
  auto second_proof = ComputeFailoverUnavailableProofHash(second, {19, 23});
  ASSERT_TRUE(first_proof.ok()) << first_proof.status();
  ASSERT_TRUE(second_proof.ok()) << second_proof.status();
  EXPECT_NE(*first_proof, *second_proof);

  // KLFI is not released yet and the feature requires a homogeneous rollout.
  // Do not silently default the four-byte field when opening a development
  // snapshot written before attempt timeout became part of the intent hash.
  std::string missing_timeout = *first_encoded;
  constexpr std::size_t kTimeoutOffset =
      4 + 2 + 4 + std::string_view("group-a").size() + 8;
  missing_timeout.erase(kTimeoutOffset, sizeof(std::uint32_t));
  EXPECT_EQ(MetaFailureClassOf(DecodeFailoverIntent(missing_timeout).status()),
            MetaFailureClass::kFailStop);
}

TEST(MetaFailoverCodecTest,
     EnforcesEvidenceShapeAcrossTheCompleteControlledGraph) {
  const std::vector<FailoverPhaseStage> before_exclusion = {
      FailoverPhaseStage::kSourceHolding,
      FailoverPhaseStage::kSourceHeld,
      FailoverPhaseStage::kOldAuthorityExcluding,
  };
  for (FailoverPhaseStage stage : before_exclusion) {
    SCOPED_TRACE(static_cast<int>(stage));
    EXPECT_TRUE(EncodeFailoverPhase(FailoverPhase{.stage_ = stage}).ok());
    EXPECT_FALSE(
        EncodeFailoverPhase(
            FailoverPhase{.stage_ = stage,
                          .old_authority_exclusion_hash_ = Bytes<32>(8)})
            .ok());
  }

  const std::vector<FailoverPhaseStage> after_exclusion = {
      FailoverPhaseStage::kOldAuthorityExcluded,
      FailoverPhaseStage::kCandidateCaughtUp,
      FailoverPhaseStage::kPromotionPreparing,
  };
  for (FailoverPhaseStage stage : after_exclusion) {
    SCOPED_TRACE(static_cast<int>(stage));
    EXPECT_TRUE(EncodeFailoverPhase(
                    FailoverPhase{.stage_ = stage,
                                  .old_authority_exclusion_hash_ = Bytes<32>(8),
                                  .required_applied_next_lsns_ = {19, 23}})
                    .ok());
  }

  const std::vector<FailoverPhaseStage> after_prepare = {
      FailoverPhaseStage::kPromotionPrepared,
      FailoverPhaseStage::kAuthorityActivated,
      FailoverPhaseStage::kServing,
  };
  for (FailoverPhaseStage stage : after_prepare) {
    SCOPED_TRACE(static_cast<int>(stage));
    EXPECT_TRUE(EncodeFailoverPhase(
                    FailoverPhase{.stage_ = stage,
                                  .old_authority_exclusion_hash_ = Bytes<32>(8),
                                  .required_applied_next_lsns_ = {19, 23},
                                  .prepared_result_hash_ = Bytes<32>(9)})
                    .ok());
  }

  EXPECT_FALSE(
      EncodeFailoverPhase(
          FailoverPhase{.stage_ = FailoverPhaseStage::kOldAuthorityExcluded})
          .ok());
  EXPECT_FALSE(
      EncodeFailoverPhase(
          FailoverPhase{.stage_ = FailoverPhaseStage::kPromotionPreparing,
                        .old_authority_exclusion_hash_ = Bytes<32>(8),
                        .required_applied_next_lsns_ = {19},
                        .prepared_result_hash_ = Bytes<32>(9)})
          .ok());
}

TEST(MetaFailoverValidationTest,
     ValidatesTypedSubmitAndRejectsSkippingAuthorityExclusion) {
  MetaObservationStore observations;
  MetaStores stores;
  SubmitOperation submit;
  submit.operation_id_ = Bytes<16>(9);
  submit.kind_ = std::string(kFailoverOperationKind);
  submit.intent_ = "not-a-failover-intent";
  submit.intent_hash_ = MetaSha256(submit.intent_);
  submit.replication_history_id_ = Intent().parent_history_id_;
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          MetaCommand(submit), MetaCommittedView(stores, 0), observations)),
      MetaFailureClass::kDomainReject);

  auto intent = EncodeFailoverIntent(Intent());
  ASSERT_TRUE(intent.ok()) << intent.status();
  submit.intent_ = *intent;
  submit.intent_hash_ = MetaSha256(submit.intent_);
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          MetaCommand(submit), MetaCommittedView(stores, 0), observations)),
      MetaFailureClass::kDomainReject);
  ASSERT_TRUE(stores.operation_.SubmitOperation(submit, 1).ok());

  FailoverPhase skipped{
      .stage_ = FailoverPhaseStage::kPromotionPreparing,
      .old_authority_exclusion_hash_ = Bytes<32>(8),
      .required_applied_next_lsns_ = {19, 23},
  };
  auto encoded_skipped = EncodeFailoverPhase(skipped);
  ASSERT_TRUE(encoded_skipped.ok()) << encoded_skipped.status();
  TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  transition.kind_phase_blob_ = *encoded_skipped;
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          MetaCommand(transition), MetaCommittedView(stores, 1), observations)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     RejectsPromotionPrepareOwnedByAnotherOperationKind) {
  MetaObservationStore observations;
  MetaStores stores;
  SubmitOperation submit;
  submit.operation_id_ = Bytes<16>(9);
  submit.kind_ = "maintenance";
  submit.intent_ = "opaque";
  submit.intent_hash_ = MetaSha256(submit.intent_);
  submit.replication_history_id_ = Bytes<20>(7);
  ASSERT_TRUE(stores.operation_.SubmitOperation(submit, 1).ok());

  TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  transition.current_directives_.push_back(
      MetaDirectiveSpec{.kind_ = "promotion-prepare"});
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          MetaCommand(transition), MetaCommittedView(stores, 1), observations)),
      MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     AcceptsOrderedPrepareAndExactPreparedReceiptEvidence) {
  FailoverIntent failover = Intent();
  failover.recovery_generation_ = 1;
  MetaObservationStore observations;
  MetaStores stores;

  PutPopulationManifest manifest;
  manifest.entries_ = {{1, 10}, {2, 20}};
  manifest.manifest_digest_ =
      MetaPopulationManifestStore::CanonicalDigest(manifest.entries_);
  failover.population_manifest_digest_ = manifest.manifest_digest_;
  ASSERT_TRUE(stores.population_manifest_.Put(manifest).ok());

  RegisterNode register_former;
  register_former.node_id_ = failover.former_owner_node_id_;
  register_former.principal_ =
      "keylane://node/" + failover.former_owner_node_id_;
  register_former.endpoints_ = {"127.0.0.1:6380"};
  register_former.role_ = MetaNodeRole::kPrimary;
  ASSERT_TRUE(stores.identity_.Apply(register_former).ok());

  RegisterNode register_candidate;
  register_candidate.node_id_ = failover.candidate_node_id_;
  register_candidate.principal_ =
      "keylane://node/" + failover.candidate_node_id_;
  register_candidate.endpoints_ = {"127.0.0.1:6379"};
  register_candidate.role_ = MetaNodeRole::kReplica;
  ASSERT_TRUE(stores.identity_.Apply(register_candidate).ok());

  CreateGroup create;
  create.group_id_ = failover.group_id_;
  create.new_topology_epoch_ = 1;
  ASSERT_TRUE(stores.topology_.Apply(create).ok());
  ASSERT_TRUE(stores.grant_.AddGroup(failover.group_id_).ok());

  AssignNodeToGroup former;
  former.group_id_ = failover.group_id_;
  former.node_id_ = failover.former_owner_node_id_;
  former.assignment_id_ = failover.former_owner_assignment_id_;
  former.expected_revision_ = 1;
  former.new_topology_epoch_ = 2;
  ASSERT_TRUE(stores.topology_.Apply(former).ok());
  AssignNodeToGroup candidate;
  candidate.group_id_ = failover.group_id_;
  candidate.node_id_ = failover.candidate_node_id_;
  candidate.assignment_id_ = failover.candidate_assignment_id_;
  candidate.role_ = MetaNodeRole::kReplica;
  candidate.expected_revision_ = 2;
  candidate.new_topology_epoch_ = 3;
  ASSERT_TRUE(stores.topology_.Apply(candidate).ok());

  BeginGroupTerm first_term;
  first_term.group_id_ = failover.group_id_;
  first_term.new_term_ = 1;
  ASSERT_TRUE(stores.grant_.BeginGroupTerm(first_term).ok());
  ActivateAuthority old_authority;
  old_authority.group_id_ = failover.group_id_;
  old_authority.expected_term_ = 1;
  old_authority.new_owner_ = failover.former_owner_node_id_;
  old_authority.grant_ = failover.old_grant_;
  old_authority.new_authority_version_ = failover.authority_version_;
  ASSERT_TRUE(
      stores.grant_.ValidateActivate(old_authority, failover.grant_revision_)
          .ok());
  ASSERT_TRUE(
      stores.grant_.ApplyGrantPart(old_authority, failover.grant_revision_)
          .ok());
  ASSERT_TRUE(stores.topology_
                  .SetOwner(failover.group_id_, failover.former_owner_node_id_)
                  .ok());
  ASSERT_TRUE(stores.topology_.SetGroupTerm(failover.group_id_, 1).ok());
  ASSERT_TRUE(
      stores.topology_
          .SetAuthorityVersion(failover.group_id_, failover.authority_version_)
          .ok());
  ASSERT_TRUE(stores.topology_
                  .SetPopulationManifest(failover.group_id_,
                                         failover.population_manifest_revision_,
                                         failover.population_manifest_digest_)
                  .ok());
  ASSERT_TRUE(stores.topology_
                  .SetPartitionReplicationEpoch(
                      failover.group_id_, failover.partition_replication_epoch_)
                  .ok());

  const std::int64_t observed_at = UnixMillisNow();
  const MetaObservationIdentity candidate_identity{
      failover.candidate_node_id_, failover.candidate_boot_id_, 1};
  ASSERT_TRUE(observations.AdoptSession(candidate_identity, observed_at).ok());
  MetaCandidateProgressObs admission_progress{
      .node_id_ = failover.candidate_node_id_,
      .boot_incarnation_ = failover.candidate_boot_id_,
      .session_generation_ = 1,
      .group_id_ = failover.group_id_,
      .assignment_id_ = failover.candidate_assignment_id_,
      .group_term_ = failover.group_term_ - 1,
      .population_manifest_revision_ = failover.population_manifest_revision_,
      .population_manifest_digest_ = failover.population_manifest_digest_,
      .partition_replication_epoch_ = failover.partition_replication_epoch_,
      .replication_history_id_ = Bytes<20>(8),
      .source_node_id_ = failover.former_owner_node_id_,
      .source_assignment_id_ = failover.former_owner_assignment_id_,
      .source_boot_incarnation_ = failover.former_owner_boot_id_,
      .source_replication_history_id_ = failover.parent_history_id_,
      .applied_next_lsns_ = {19, 23},
      .storage_ready_ = true,
      .population_ready_ = true,
  };
  ASSERT_TRUE(observations
                  .Ingest(MetaObservation{.identity_ = candidate_identity,
                                          .payload_ = admission_progress},
                          MetaStoresFacts(stores), observed_at)
                  .ok());

  auto encoded_intent = EncodeFailoverIntent(failover);
  ASSERT_TRUE(encoded_intent.ok()) << encoded_intent.status();
  SubmitOperation submit;
  submit.operation_id_ = Bytes<16>(9);
  submit.request_id_ = Bytes<16>(10);
  submit.kind_ = std::string(kFailoverOperationKind);
  submit.intent_ = *encoded_intent;
  submit.intent_hash_ = MetaSha256(submit.intent_);
  submit.replication_history_id_ = failover.parent_history_id_;
  submit.policy_references_ = {
      {failover.old_grant_.policy_id_, failover.old_grant_.policy_version_}};
  SubmitOperation missing_grant_policy = submit;
  missing_grant_policy.policy_references_.clear();
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(missing_grant_policy), MetaCommittedView(stores, 1),
                observations)),
            MetaFailureClass::kDomainReject);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(submit),
                                       MetaCommittedView(stores, 1),
                                       observations)
                  .ok());
  ASSERT_TRUE(stores.operation_.SubmitOperation(submit, 1).ok());

  // The recovery handoff belongs to the accepted workflow. A pre-existing
  // handoff would correctly make a fresh controlled-failover submission
  // ineligible, so install it only after the operation is durable.
  SetFailoverRecovery recovery;
  recovery.group_id_ = failover.group_id_;
  recovery.recovery_generation_ = failover.recovery_generation_;
  recovery.old_source_node_id_ = failover.former_owner_node_id_;
  recovery.old_source_assignment_id_ = failover.former_owner_assignment_id_;
  recovery.old_source_boot_incarnation_ = failover.former_owner_boot_id_;
  recovery.old_source_history_id_ = failover.parent_history_id_;
  recovery.excluded_authority_term_ = failover.group_term_ - 1;
  recovery.excluded_authority_version_ = failover.authority_version_;
  recovery.excluded_grant_revision_ = failover.grant_revision_;
  recovery.population_manifest_revision_ =
      failover.population_manifest_revision_;
  recovery.population_manifest_digest_ = failover.population_manifest_digest_;
  recovery.partition_replication_epoch_ = failover.partition_replication_epoch_;
  recovery.hold_required_ = true;
  ASSERT_TRUE(stores.failover_recovery_.Set(recovery, 1).ok());

  auto transition_phase = [&](FailoverPhase phase,
                              std::uint64_t expected_revision,
                              std::vector<MetaDirectiveSpec> directives = {},
                              std::vector<MetaEvidenceSummary> evidence = {}) {
    auto encoded = EncodeFailoverPhase(phase);
    EXPECT_TRUE(encoded.ok()) << encoded.status();
    TransitionOperationPhase transition;
    transition.operation_id_ = submit.operation_id_;
    transition.expected_revision_ = expected_revision;
    if (encoded.ok()) transition.kind_phase_blob_ = *encoded;
    transition.current_directives_ = std::move(directives);
    transition.evidence_ = std::move(evidence);
    return transition;
  };

  const std::vector<std::uint64_t> required = {19, 23};
  TransitionOperationPhase source_holding = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kSourceHolding}, 0);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(source_holding),
                                       MetaCommittedView(stores, 1),
                                       observations)
                  .ok());
  ASSERT_TRUE(
      stores.operation_.TransitionOperationPhase(source_holding, 2).ok());

  TransitionOperationPhase premature_source_held = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kSourceHeld}, 1);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(premature_source_held),
                MetaCommittedView(stores, 2), observations)),
            MetaFailureClass::kDomainReject);

  MetaDirectiveSpec hold_source;
  hold_source.directive_id_ = Bytes<16>(0x21);
  hold_source.attempt_id_ = Bytes<16>(0x22);
  hold_source.recipient_node_id_ = failover.former_owner_node_id_;
  hold_source.target_node_id_ = failover.candidate_node_id_;
  hold_source.target_boot_id_ = failover.candidate_boot_id_;
  hold_source.assignment_id_ = failover.candidate_assignment_id_;
  hold_source.source_node_id_ = failover.former_owner_node_id_;
  hold_source.source_assignment_id_ = failover.former_owner_assignment_id_;
  hold_source.source_boot_id_ = failover.former_owner_boot_id_;
  hold_source.source_replication_history_id_ = failover.parent_history_id_;
  hold_source.group_id_ = failover.group_id_;
  hold_source.group_term_ = failover.group_term_ - 1;
  hold_source.authority_version_ = failover.authority_version_;
  hold_source.grant_revision_ = failover.grant_revision_;
  hold_source.population_manifest_revision_ =
      failover.population_manifest_revision_;
  hold_source.population_manifest_digest_ =
      failover.population_manifest_digest_;
  hold_source.partition_replication_epoch_ =
      failover.partition_replication_epoch_;
  hold_source.kind_ = std::string(kMetaDirectiveAuthorizeSource);
  TransitionOperationPhase dispatch_hold = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kSourceHolding}, 1,
      {hold_source});
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(dispatch_hold),
                                       MetaCommittedView(stores, 2),
                                       observations)
                  .ok());
  ASSERT_TRUE(
      stores.operation_.TransitionOperationPhase(dispatch_hold, 3).ok());

  CommitDirectiveResult hold_receipt;
  hold_receipt.operation_id_ = submit.operation_id_;
  hold_receipt.directive_id_ = hold_source.directive_id_;
  hold_receipt.attempt_id_ = hold_source.attempt_id_;
  hold_receipt.directive_revision_ = 3;
  hold_receipt.recipient_node_id_ = failover.former_owner_node_id_;
  hold_receipt.recipient_boot_id_ = failover.former_owner_boot_id_;
  hold_receipt.assignment_id_ = failover.candidate_assignment_id_;
  hold_receipt.result_ = "source-held";
  hold_receipt.result_hash_ = MetaSha256(hold_receipt.result_);
  ASSERT_TRUE(stores.operation_.CommitDirectiveResult(hold_receipt, 4).ok());

  TransitionOperationPhase source_held = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kSourceHeld}, 3);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(source_held),
                                       MetaCommittedView(stores, 4),
                                       observations)
                  .ok());
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(source_held, 5).ok());

  TransitionOperationPhase excluding = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kOldAuthorityExcluding}, 4);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(excluding),
                                       MetaCommittedView(stores, 5),
                                       observations)
                  .ok());
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(excluding, 6).ok());

  BeginGroupTerm exclude_authority;
  exclude_authority.group_id_ = failover.group_id_;
  exclude_authority.expected_term_ = failover.group_term_ - 1;
  exclude_authority.new_term_ = failover.group_term_;
  exclude_authority.workflow_operation_id_ = submit.operation_id_;
  exclude_authority.expected_operation_revision_ =
      stores.operation_.FindOperation(submit.operation_id_)->revision_;
  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(exclude_authority),
                                       MetaCommittedView(stores, 6),
                                       observations)
                  .ok());
  BeginGroupTerm wrong_term = exclude_authority;
  --wrong_term.expected_term_;
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          MetaCommand(wrong_term), MetaCommittedView(stores, 6), observations)),
      MetaFailureClass::kDomainReject);
  ASSERT_TRUE(stores.grant_.BeginGroupTerm(exclude_authority).ok());
  ASSERT_TRUE(
      stores.topology_.SetGroupTerm(failover.group_id_, failover.group_term_)
          .ok());

  MetaDirectiveSpec frozen_source;
  frozen_source.directive_id_ = Bytes<16>(0x31);
  frozen_source.attempt_id_ = Bytes<16>(0x32);
  frozen_source.recipient_node_id_ = failover.former_owner_node_id_;
  frozen_source.target_node_id_ = failover.candidate_node_id_;
  frozen_source.target_boot_id_ = failover.candidate_boot_id_;
  frozen_source.assignment_id_ = failover.candidate_assignment_id_;
  frozen_source.source_node_id_ = failover.former_owner_node_id_;
  frozen_source.source_assignment_id_ = failover.former_owner_assignment_id_;
  frozen_source.source_boot_id_ = failover.former_owner_boot_id_;
  frozen_source.source_replication_history_id_ = failover.parent_history_id_;
  frozen_source.group_id_ = failover.group_id_;
  frozen_source.group_term_ = failover.group_term_;
  frozen_source.authority_version_ = failover.authority_version_;
  frozen_source.grant_revision_ = failover.grant_revision_;
  frozen_source.population_manifest_revision_ =
      failover.population_manifest_revision_;
  frozen_source.population_manifest_digest_ =
      failover.population_manifest_digest_;
  frozen_source.partition_replication_epoch_ =
      failover.partition_replication_epoch_;
  frozen_source.kind_ = std::string(kMetaDirectiveAuthorizeSource);
  auto frozen_request = cluster::control::EncodeFrozenSourceRequest(
      {.recovery_generation = failover.recovery_generation_});
  auto frozen_preconditions = cluster::control::EncodeFrozenSourcePreconditions(
      {.excluded_group_term = failover.group_term_ - 1,
       .excluded_authority_version = failover.authority_version_,
       .excluded_grant_revision = failover.grant_revision_});
  ASSERT_TRUE(frozen_request.ok()) << frozen_request.status();
  ASSERT_TRUE(frozen_preconditions.ok()) << frozen_preconditions.status();
  frozen_source.payload_ = *frozen_request;
  frozen_source.preconditions_ = *frozen_preconditions;
  TransitionOperationPhase dispatch_frozen = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kOldAuthorityExcluding}, 5,
      {frozen_source});

  auto malformed_frozen = dispatch_frozen;
  malformed_frozen.current_directives_[0].payload_.append("trailing");
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(malformed_frozen), MetaCommittedView(stores, 6),
                observations)),
            MetaFailureClass::kDomainReject);
  auto wrong_generation_request = cluster::control::EncodeFrozenSourceRequest(
      {.recovery_generation = failover.recovery_generation_ + 1});
  ASSERT_TRUE(wrong_generation_request.ok())
      << wrong_generation_request.status();
  auto wrong_frozen = dispatch_frozen;
  wrong_frozen.current_directives_[0].payload_ = *wrong_generation_request;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(wrong_frozen), MetaCommittedView(stores, 6),
                observations)),
            MetaFailureClass::kDomainReject);
  auto wrong_old_term = cluster::control::EncodeFrozenSourcePreconditions(
      {.excluded_group_term = failover.group_term_,
       .excluded_authority_version = failover.authority_version_,
       .excluded_grant_revision = failover.grant_revision_});
  ASSERT_TRUE(wrong_old_term.ok()) << wrong_old_term.status();
  wrong_frozen = dispatch_frozen;
  wrong_frozen.current_directives_[0].preconditions_ = *wrong_old_term;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(wrong_frozen), MetaCommittedView(stores, 6),
                observations)),
            MetaFailureClass::kDomainReject);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(dispatch_frozen),
                                       MetaCommittedView(stores, 6),
                                       observations)
                  .ok());
  ASSERT_TRUE(
      stores.operation_.TransitionOperationPhase(dispatch_frozen, 7).ok());
  MetaStores unavailable_stores = stores;

  cluster::control::FrozenSourceEvidence frozen_evidence{
      .recovery_generation = failover.recovery_generation_,
      .source_history_id = Hex(failover.parent_history_id_),
      .final_next_lsns = required,
  };
  auto frozen_proof_hash =
      cluster::control::ComputeFrozenSourceProofHash(frozen_evidence);
  ASSERT_TRUE(frozen_proof_hash.ok()) << frozen_proof_hash.status();
  frozen_evidence.proof_hash = *frozen_proof_hash;
  auto frozen_result =
      cluster::control::EncodeFrozenSourceEvidence(frozen_evidence);
  ASSERT_TRUE(frozen_result.ok()) << frozen_result.status();
  const MetaHash256 exclusion_hash = *frozen_proof_hash;
  CommitDirectiveResult frozen_receipt;
  frozen_receipt.operation_id_ = submit.operation_id_;
  frozen_receipt.directive_id_ = frozen_source.directive_id_;
  frozen_receipt.attempt_id_ = frozen_source.attempt_id_;
  frozen_receipt.directive_revision_ = 7;
  frozen_receipt.recipient_node_id_ = failover.former_owner_node_id_;
  frozen_receipt.recipient_boot_id_ = failover.former_owner_boot_id_;
  frozen_receipt.assignment_id_ = failover.candidate_assignment_id_;
  frozen_receipt.result_ = *frozen_result;
  frozen_receipt.result_hash_ = MetaSha256(frozen_receipt.result_);
  EXPECT_NE(frozen_receipt.result_hash_, exclusion_hash)
      << "receipt hash authenticates encoded bytes; proof hash authenticates "
         "the frozen-source proof fields";

  // A source-disconnect observation can queue an unavailable update while the
  // exact frozen result is still in Raft. If the receipt commits first, apply
  // must reject the stale downgrade even though it still matches the recovery
  // record CAS: the receipt advances only the operation revision.
  MetaStores raced_stores = unavailable_stores;
  SetFailoverRecovery stale_unavailable = recovery;
  stale_unavailable.expected_revision_ = 1;
  stale_unavailable.proof_state_ = MetaFailoverProofState::kUnavailable;
  const MetaApplyResult raced_receipt = ApplyCommitted(
      raced_stores, 8, MetaCommand(frozen_receipt), "reconciler", "time");
  ASSERT_EQ(raced_receipt.verdict_, MetaAuditVerdict::kAccepted)
      << raced_receipt.detail_;
  const MetaApplyResult raced_downgrade = ApplyCommitted(
      raced_stores, 9, MetaCommand(stale_unavailable), "reconciler", "time");
  EXPECT_EQ(raced_downgrade.verdict_, MetaAuditVerdict::kRejected)
      << raced_downgrade.detail_;
  EXPECT_NE(raced_downgrade.detail_.find("successful source result"),
            std::string::npos)
      << raced_downgrade.detail_;
  const auto pending_after_race =
      raced_stores.failover_recovery_.Find(failover.group_id_);
  ASSERT_TRUE(pending_after_race.has_value());
  EXPECT_EQ(pending_after_race->proof_state_, MetaFailoverProofState::kPending);

  ASSERT_TRUE(stores.operation_.CommitDirectiveResult(frozen_receipt, 8).ok());

  recovery.expected_revision_ = 1;
  recovery.proof_state_ = MetaFailoverProofState::kExact;
  recovery.frozen_proof_ = MetaFailoverFrozenProof{
      .final_next_lsns_ = required,
      .proof_hash_ = exclusion_hash,
  };
  ASSERT_TRUE(ValidateFailoverFailSafeCheckpoint(recovery, stores).ok())
      << "a committed frozen result must remain persistable during fail-safe";
  ASSERT_TRUE(stores.failover_recovery_.Set(recovery, 9).ok());

  SetFailoverRecovery fail_safe_downgrade = recovery;
  fail_safe_downgrade.expected_revision_ = 9;
  fail_safe_downgrade.proof_state_ = MetaFailoverProofState::kUnavailable;
  ASSERT_TRUE(
      ValidateFailoverFailSafeCheckpoint(fail_safe_downgrade, stores).ok());
  auto discarded_historical_proof = fail_safe_downgrade;
  discarded_historical_proof.frozen_proof_.reset();
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverFailSafeCheckpoint(
                discarded_historical_proof, stores)),
            MetaFailureClass::kDomainReject);
  auto changed_recovery_flags = fail_safe_downgrade;
  changed_recovery_flags.recovery_required_ = true;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverFailSafeCheckpoint(
                changed_recovery_flags, stores)),
            MetaFailureClass::kDomainReject);
  auto changed_recovery_source = fail_safe_downgrade;
  changed_recovery_source.old_source_boot_incarnation_ = Bytes<20>(0x7f);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverFailSafeCheckpoint(
                changed_recovery_source, stores)),
            MetaFailureClass::kDomainReject);

  TransitionOperationPhase authority_excluded = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kOldAuthorityExcluded,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required},
      7, {frozen_source});
  auto receipt_hash_is_not_proof = authority_excluded;
  auto receipt_hash_phase = EncodeFailoverPhase(FailoverPhase{
      .stage_ = FailoverPhaseStage::kOldAuthorityExcluded,
      .old_authority_exclusion_hash_ = frozen_receipt.result_hash_,
      .required_applied_next_lsns_ = required});
  ASSERT_TRUE(receipt_hash_phase.ok()) << receipt_hash_phase.status();
  receipt_hash_is_not_proof.kind_phase_blob_ = *receipt_hash_phase;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(receipt_hash_is_not_proof),
                MetaCommittedView(stores, 9), observations)),
            MetaFailureClass::kDomainReject);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(authority_excluded),
                                       MetaCommittedView(stores, 9),
                                       observations)
                  .ok());
  ASSERT_TRUE(
      stores.operation_.TransitionOperationPhase(authority_excluded, 10).ok());

  TransitionOperationPhase candidate_caught_up = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kCandidateCaughtUp,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required},
      8, {frozen_source});
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(candidate_caught_up), MetaCommittedView(stores, 10),
                observations)),
            MetaFailureClass::kDomainReject);

  const int64_t now_unix_ms = UnixMillisNow();
  MetaCandidateProgressObs candidate_progress{
      .node_id_ = failover.candidate_node_id_,
      .boot_incarnation_ = failover.candidate_boot_id_,
      .session_generation_ = 1,
      .group_id_ = failover.group_id_,
      .assignment_id_ = failover.candidate_assignment_id_,
      .group_term_ = failover.group_term_,
      .population_manifest_revision_ = failover.population_manifest_revision_,
      .population_manifest_digest_ = failover.population_manifest_digest_,
      .partition_replication_epoch_ = failover.partition_replication_epoch_,
      .replication_history_id_ = Bytes<20>(0x51),
      .source_node_id_ = failover.former_owner_node_id_,
      .source_assignment_id_ = failover.former_owner_assignment_id_,
      .source_boot_incarnation_ = failover.former_owner_boot_id_,
      .source_replication_history_id_ = failover.parent_history_id_,
      .applied_next_lsns_ = {19, 24},
      .storage_ready_ = true,
      .population_ready_ = true,
  };
  const auto admit_progress = [&](MetaObservationStore& destination,
                                  MetaCandidateProgressObs progress,
                                  int64_t received_unix_ms) {
    const MetaObservationIdentity identity{failover.candidate_node_id_,
                                           failover.candidate_boot_id_, 1};
    ASSERT_TRUE(destination.AdoptSession(identity, received_unix_ms).ok());
    ASSERT_TRUE(destination
                    .Ingest(MetaObservation{.identity_ = identity,
                                            .payload_ = std::move(progress)},
                            MetaStoresFacts(stores), received_unix_ms)
                    .ok());
  };

  SetFailoverRecovery unavailable_recovery = recovery;
  unavailable_recovery.proof_state_ = MetaFailoverProofState::kUnavailable;
  unavailable_recovery.frozen_proof_.reset();
  ASSERT_TRUE(
      unavailable_stores.failover_recovery_.Set(unavailable_recovery, 9).ok());
  MetaObservationStore unavailable_observations;
  admit_progress(unavailable_observations, candidate_progress, now_unix_ms);
  auto unavailable_hash =
      ComputeFailoverUnavailableProofHash(failover, required);
  ASSERT_TRUE(unavailable_hash.ok()) << unavailable_hash.status();
  TransitionOperationPhase unavailable_exclusion = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kOldAuthorityExcluded,
                    .old_authority_exclusion_hash_ = *unavailable_hash,
                    .required_applied_next_lsns_ = required},
      6, {frozen_source});
  const absl::Status unavailable_status = ValidateFailoverProposal(
      MetaCommand(unavailable_exclusion),
      MetaCommittedView(unavailable_stores, 9), unavailable_observations);
  ASSERT_TRUE(unavailable_status.ok())
      << "a missing old source degrades to candidate-backed unavailable proof: "
      << unavailable_status;
  auto forged_unavailable = unavailable_exclusion;
  FailoverPhase forged_phase{
      .stage_ = FailoverPhaseStage::kOldAuthorityExcluded,
      .old_authority_exclusion_hash_ = frozen_receipt.result_hash_,
      .required_applied_next_lsns_ = required};
  auto forged_blob = EncodeFailoverPhase(forged_phase);
  ASSERT_TRUE(forged_blob.ok()) << forged_blob.status();
  forged_unavailable.kind_phase_blob_ = *forged_blob;
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          MetaCommand(forged_unavailable),
          MetaCommittedView(unavailable_stores, 9), unavailable_observations)),
      MetaFailureClass::kDomainReject);

  MetaObservationStore incompatible_observations;
  MetaCandidateProgressObs incompatible = candidate_progress;
  incompatible.source_boot_incarnation_ = Bytes<20>(0x52);
  admit_progress(incompatible_observations, std::move(incompatible),
                 now_unix_ms);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(candidate_caught_up), MetaCommittedView(stores, 10),
                incompatible_observations)),
            MetaFailureClass::kDomainReject);

  MetaObservationStore behind_observations;
  MetaCandidateProgressObs behind = candidate_progress;
  behind.applied_next_lsns_[1] = 22;
  admit_progress(behind_observations, std::move(behind), now_unix_ms);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(candidate_caught_up), MetaCommittedView(stores, 10),
                behind_observations)),
            MetaFailureClass::kDomainReject);

  MetaObservationStore::Limits candidate_stale_limits;
  candidate_stale_limits.ttl_ms_ = 1;
  MetaObservationStore stale_candidate_observations(candidate_stale_limits);
  admit_progress(stale_candidate_observations, candidate_progress,
                 now_unix_ms - 1000);
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(candidate_caught_up), MetaCommittedView(stores, 10),
                stale_candidate_observations)),
            MetaFailureClass::kDomainReject);

  ASSERT_TRUE(observations
                  .Ingest(MetaObservation{.identity_ = candidate_identity,
                                          .payload_ = candidate_progress},
                          MetaStoresFacts(stores), now_unix_ms)
                  .ok());
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(candidate_caught_up),
                                       MetaCommittedView(stores, 10),
                                       observations)
                  .ok());
  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverFailSafeCheckpoint(
          candidate_caught_up, MetaCommittedView(stores, 10), observations)),
      MetaFailureClass::kDomainReject)
      << "fail-safe checkpoints must not admit ordinary catch-up progression";
  ASSERT_TRUE(
      stores.operation_.TransitionOperationPhase(candidate_caught_up, 11).ok());

  auto payload = cluster::control::EncodePromotionPrepareRequest(
      {.parent_history_id = Hex(failover.parent_history_id_),
       .required_applied_next_lsns = required});
  auto preconditions = cluster::control::EncodePromotionPreparePreconditions(
      {.excluded_group_term = failover.group_term_,
       .old_authority_exclusion_hash = exclusion_hash});
  ASSERT_TRUE(payload.ok()) << payload.status();
  ASSERT_TRUE(preconditions.ok()) << preconditions.status();
  MetaDirectiveSpec directive;
  directive.directive_id_ = Bytes<16>(10);
  directive.attempt_id_ = Bytes<16>(11);
  directive.recipient_node_id_ = failover.candidate_node_id_;
  directive.target_node_id_ = failover.candidate_node_id_;
  directive.target_boot_id_ = failover.candidate_boot_id_;
  directive.assignment_id_ = failover.candidate_assignment_id_;
  directive.source_node_id_ = failover.former_owner_node_id_;
  directive.source_assignment_id_ = failover.former_owner_assignment_id_;
  directive.source_boot_id_ = failover.former_owner_boot_id_;
  directive.source_replication_history_id_ = failover.parent_history_id_;
  directive.group_id_ = failover.group_id_;
  directive.group_term_ = failover.group_term_;
  directive.authority_version_ = failover.authority_version_;
  directive.grant_revision_ = failover.grant_revision_;
  directive.population_manifest_revision_ =
      failover.population_manifest_revision_;
  directive.population_manifest_digest_ = failover.population_manifest_digest_;
  directive.partition_replication_epoch_ =
      failover.partition_replication_epoch_;
  directive.kind_ = "promotion-prepare";
  directive.payload_ = *payload;
  directive.preconditions_ = *preconditions;
  directive.storage_mutating_ = true;
  TransitionOperationPhase phase3 = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kPromotionPreparing,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required},
      9, {directive});

  const auto expect_directive_rejected = [&](MetaDirectiveSpec changed) {
    TransitionOperationPhase stale = phase3;
    stale.current_directives_ = {std::move(changed)};
    EXPECT_EQ(
        MetaFailureClassOf(ValidateFailoverProposal(
            MetaCommand(stale), MetaCommittedView(stores, 11), observations)),
        MetaFailureClass::kDomainReject);
  };
  MetaDirectiveSpec changed = directive;
  changed.assignment_id_ = Bytes<16>(0x31);
  expect_directive_rejected(std::move(changed));
  changed = directive;
  changed.target_boot_id_ = Bytes<20>(0x41);
  expect_directive_rejected(std::move(changed));
  changed = directive;
  ++changed.group_term_;
  expect_directive_rejected(std::move(changed));
  changed = directive;
  ++changed.population_manifest_revision_;
  expect_directive_rejected(std::move(changed));
  changed = directive;
  ++changed.partition_replication_epoch_;
  expect_directive_rejected(std::move(changed));
  changed = directive;
  changed.source_replication_history_id_ = Bytes<20>(0x71);
  expect_directive_rejected(std::move(changed));
  changed = directive;
  auto changed_frontier = cluster::control::EncodePromotionPrepareRequest(
      {.parent_history_id = Hex(failover.parent_history_id_),
       .required_applied_next_lsns = {19, 24}});
  ASSERT_TRUE(changed_frontier.ok()) << changed_frontier.status();
  changed.payload_ = *changed_frontier;
  expect_directive_rejected(std::move(changed));

  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(phase3),
                                       MetaCommittedView(stores, 11),
                                       observations)
                  .ok());
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(phase3, 12).ok());

  auto result = cluster::control::EncodePromotionPreparedEvidence(
      {.parent_history_id = Hex(failover.parent_history_id_),
       .frozen_applied_next_lsns = {20, 24},
       .population_generation = 31,
       .population_digest = 37,
       .catalog_generation = 41,
       .catalog_dump_crc64 = 43,
       .child_history_id = std::string(40, 'c')});
  ASSERT_TRUE(result.ok()) << result.status();
  CommitDirectiveResult receipt;
  receipt.operation_id_ = submit.operation_id_;
  receipt.directive_id_ = directive.directive_id_;
  receipt.attempt_id_ = directive.attempt_id_;
  receipt.directive_revision_ = 12;
  receipt.recipient_node_id_ = failover.candidate_node_id_;
  receipt.recipient_boot_id_ = failover.candidate_boot_id_;
  receipt.assignment_id_ = failover.candidate_assignment_id_;
  receipt.result_hash_ = MetaSha256(*result);
  receipt.result_ = *result;
  ASSERT_TRUE(stores.operation_.CommitDirectiveResult(receipt, 13).ok());

  MetaOperationEvidenceObs observed_evidence{
      .node_id_ = failover.candidate_node_id_,
      .boot_incarnation_ = failover.candidate_boot_id_,
      .assignment_id_ = failover.candidate_assignment_id_,
      .operation_id_ = submit.operation_id_,
      .kind_phase_ = "promotion-prepare:prepared",
      .evidence_hash_ = receipt.result_hash_,
      .evidence_ = *result,
      .group_id_ = failover.group_id_,
      .group_term_ = failover.group_term_,
      .population_manifest_revision_ = failover.population_manifest_revision_,
      .partition_replication_epoch_ = failover.partition_replication_epoch_,
      .replication_history_id_ = failover.parent_history_id_,
  };
  const MetaEvidenceSummary evidence = SummarizeOperationEvidence(
      observed_evidence, failover.population_manifest_digest_);
  TransitionOperationPhase phase4 = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kPromotionPrepared,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required,
                    .prepared_result_hash_ = receipt.result_hash_},
      11, {}, {evidence});

  EXPECT_EQ(
      MetaFailureClassOf(ValidateFailoverProposal(
          MetaCommand(phase4), MetaCommittedView(stores, 13), observations)),
      MetaFailureClass::kDomainReject);

  MetaObservation observation{
      .identity_ = {failover.candidate_node_id_, failover.candidate_boot_id_,
                    1},
      .payload_ = observed_evidence,
      .received_unix_ms_ = 1000,
  };
  ASSERT_TRUE(
      observations
          .Ingest(std::move(observation), MetaStoresFacts(stores), now_unix_ms)
          .ok());
  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(phase4),
                                       MetaCommittedView(stores, 13),
                                       observations)
                  .ok());

  MetaObservationStore::Limits stale_limits;
  stale_limits.ttl_ms_ = 1;
  MetaObservationStore stale_observations(stale_limits);
  ASSERT_TRUE(stale_observations
                  .AdoptSession({failover.candidate_node_id_,
                                 failover.candidate_boot_id_, 1},
                                now_unix_ms - 1000)
                  .ok());
  MetaObservation stale_observation{
      .identity_ = {failover.candidate_node_id_, failover.candidate_boot_id_,
                    1},
      .payload_ = observed_evidence,
  };
  ASSERT_TRUE(stale_observations
                  .Ingest(std::move(stale_observation), MetaStoresFacts(stores),
                          now_unix_ms - 1000)
                  .ok());
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(phase4), MetaCommittedView(stores, 13),
                stale_observations)),
            MetaFailureClass::kDomainReject);

  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(phase4, 14).ok());
  const auto committed = stores.operation_.FindOperation(submit.operation_id_);
  ASSERT_TRUE(committed.has_value());
  observations.ResetForLeadershipChange();
  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(phase4),
                                       MetaCommittedView(stores, 14),
                                       observations)
                  .ok())
      << "an already committed phase replay needs no former leader evidence";
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(phase4, 14).ok());
  const auto replayed = stores.operation_.FindOperation(submit.operation_id_);
  ASSERT_TRUE(replayed.has_value());
  EXPECT_EQ(replayed->operation_id_, committed->operation_id_);
  EXPECT_EQ(replayed->revision_, committed->revision_);
  EXPECT_EQ(replayed->kind_phase_blob_, committed->kind_phase_blob_);
  EXPECT_EQ(replayed->evidence_, committed->evidence_);
  EXPECT_EQ(replayed->terminal_receipts_, committed->terminal_receipts_);

  ActivateAuthority activate;
  activate.group_id_ = failover.group_id_;
  activate.expected_term_ = failover.group_term_;
  activate.new_owner_ = failover.candidate_node_id_;
  activate.grant_ = failover.old_grant_;
  activate.new_authority_version_ = failover.authority_version_ + 1;
  activate.new_topology_epoch_ = 4;
  activate.new_config_epoch_ = 1;
  activate.workflow_operation_id_ = submit.operation_id_;
  activate.expected_operation_revision_ =
      stores.operation_.FindOperation(submit.operation_id_)->revision_;
  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(activate),
                                       MetaCommittedView(stores, 14),
                                       observations)
                  .ok());
  ActivateAuthority wrong_activation = activate;
  wrong_activation.grant_.lease_duration_ms_++;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(wrong_activation), MetaCommittedView(stores, 14),
                observations)),
            MetaFailureClass::kDomainReject);

  constexpr std::uint64_t kActivationGrantRevision = 20;
  ASSERT_TRUE(
      stores.grant_.ValidateActivate(activate, kActivationGrantRevision).ok());
  ASSERT_TRUE(
      stores.grant_.ApplyGrantPart(activate, kActivationGrantRevision).ok());
  ASSERT_TRUE(
      stores.topology_.SetOwner(failover.group_id_, failover.candidate_node_id_)
          .ok());
  ASSERT_TRUE(stores.topology_
                  .SetAuthorityVersion(failover.group_id_,
                                       failover.authority_version_ + 1)
                  .ok());
  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(activate),
                                       MetaCommittedView(stores, 20),
                                       observations)
                  .ok())
      << "an exact activation replay remains valid after its commit";

  TransitionOperationPhase activated = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kAuthorityActivated,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required,
                    .prepared_result_hash_ = receipt.result_hash_},
      12);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(activated),
                                       MetaCommittedView(stores, 20),
                                       observations)
                  .ok());
  ASSERT_TRUE(ValidateFailoverFailSafeCheckpoint(
                  activated, MetaCommittedView(stores, 20), observations)
                  .ok())
      << "already-committed authority is a factual fail-safe checkpoint";

  TransitionOperationPhase changed_prepared_proof = activated;
  auto changed_activated =
      FailoverPhase{.stage_ = FailoverPhaseStage::kAuthorityActivated,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required,
                    .prepared_result_hash_ = Bytes<32>(0x61)};
  auto changed_activated_blob = EncodeFailoverPhase(changed_activated);
  ASSERT_TRUE(changed_activated_blob.ok()) << changed_activated_blob.status();
  changed_prepared_proof.kind_phase_blob_ = *changed_activated_blob;
  EXPECT_EQ(MetaFailureClassOf(ValidateFailoverProposal(
                MetaCommand(changed_prepared_proof),
                MetaCommittedView(stores, 20), observations)),
            MetaFailureClass::kDomainReject);

  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(activated, 21).ok());
  TransitionOperationPhase serving = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kServing,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required,
                    .prepared_result_hash_ = receipt.result_hash_},
      13);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(serving),
                                       MetaCommittedView(stores, 21),
                                       observations)
                  .ok());
  ASSERT_TRUE(ValidateFailoverFailSafeCheckpoint(
                  serving, MetaCommittedView(stores, 21), observations)
                  .ok())
      << "serving attribution contains no new directive or authority effect";
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(serving, 22).ok());
}

class ControlledFailoverSubmissionTest : public ::testing::Test {
 protected:
  static constexpr std::int64_t kNowUnixMs = 10'000;
  static constexpr std::uint64_t kCurrentTerm = 1;
  static constexpr std::uint64_t kAuthorityVersion = 9;
  static constexpr std::uint64_t kGrantRevision = 41;
  static constexpr std::uint64_t kManifestRevision = 13;
  static constexpr std::uint64_t kPartitionEpoch = 17;

  static std::string Node(char digit) { return std::string(40, digit); }

  void Register(char digit, MetaNodeRole role) {
    RegisterNode command;
    command.node_id_ = Node(digit);
    command.principal_ = "keylane://node/" + command.node_id_;
    command.endpoints_ = {"127.0.0.1:700" + std::to_string(digit - 'a' + 1)};
    command.role_ = role;
    ASSERT_TRUE(stores_.identity_.Apply(command).ok());
  }

  void Assign(char digit, const MetaAssignmentId& assignment, MetaNodeRole role,
              std::uint64_t expected_revision, std::uint64_t topology_epoch) {
    AssignNodeToGroup command;
    command.group_id_ = group_id_;
    command.node_id_ = Node(digit);
    command.assignment_id_ = assignment;
    command.role_ = role;
    command.expected_revision_ = expected_revision;
    command.new_topology_epoch_ = topology_epoch;
    ASSERT_TRUE(stores_.topology_.Apply(command).ok());
  }

  void SetUp() override {
    Register('a', MetaNodeRole::kPrimary);
    Register('b', MetaNodeRole::kReplica);
    Register('c', MetaNodeRole::kReplica);

    CreateGroup create;
    create.group_id_ = group_id_;
    create.new_topology_epoch_ = 1;
    ASSERT_TRUE(stores_.topology_.Apply(create).ok());
    ASSERT_TRUE(stores_.grant_.AddGroup(group_id_).ok());
    Assign('a', former_assignment_, MetaNodeRole::kPrimary, 1, 2);
    Assign('b', candidate_b_assignment_, MetaNodeRole::kReplica, 2, 3);
    Assign('c', candidate_c_assignment_, MetaNodeRole::kReplica, 3, 4);

    PutPolicy policy;
    policy.policy_id_ = old_grant_.policy_id_;
    policy.version_ = old_grant_.policy_version_;
    policy.content_ = "finite-lease";
    policy.content_hash_ = MetaSha256(policy.content_);
    ASSERT_TRUE(stores_.policy_.Apply(policy).ok());

    BeginGroupTerm first_term;
    first_term.group_id_ = group_id_;
    first_term.new_term_ = kCurrentTerm;
    ASSERT_TRUE(stores_.grant_.BeginGroupTerm(first_term).ok());

    ActivateAuthority activate;
    activate.group_id_ = group_id_;
    activate.expected_term_ = kCurrentTerm;
    activate.new_owner_ = Node('a');
    activate.grant_ = old_grant_;
    activate.new_authority_version_ = kAuthorityVersion;
    ASSERT_TRUE(stores_.grant_.ValidateActivate(activate, kGrantRevision).ok());
    ASSERT_TRUE(stores_.grant_.ApplyGrantPart(activate, kGrantRevision).ok());
    ASSERT_TRUE(stores_.topology_.SetOwner(group_id_, Node('a')).ok());
    ASSERT_TRUE(stores_.topology_.SetGroupTerm(group_id_, kCurrentTerm).ok());
    ASSERT_TRUE(
        stores_.topology_.SetAuthorityVersion(group_id_, kAuthorityVersion)
            .ok());
    ASSERT_TRUE(stores_.topology_
                    .SetPopulationManifest(group_id_, kManifestRevision,
                                           manifest_digest_)
                    .ok());
    ASSERT_TRUE(stores_.topology_
                    .SetPartitionReplicationEpoch(group_id_, kPartitionEpoch)
                    .ok());
  }

  MetaCandidateProgressObs Candidate(
      char digit, const MetaAssignmentId& assignment,
      const MetaBootIncarnation& boot,
      std::vector<std::uint64_t> frontier) const {
    return MetaCandidateProgressObs{
        .node_id_ = Node(digit),
        .boot_incarnation_ = boot,
        .session_generation_ = 1,
        .group_id_ = group_id_,
        .assignment_id_ = assignment,
        .group_term_ = kCurrentTerm,
        .population_manifest_revision_ = kManifestRevision,
        .population_manifest_digest_ = manifest_digest_,
        .partition_replication_epoch_ = kPartitionEpoch,
        .replication_history_id_ = Bytes<20>(digit == 'b' ? 0x42 : 0x43),
        .source_node_id_ = Node('a'),
        .source_assignment_id_ = former_assignment_,
        .source_boot_incarnation_ = former_boot_,
        .source_replication_history_id_ = source_history_,
        .applied_next_lsns_ = std::move(frontier),
        .storage_ready_ = true,
        .population_ready_ = true,
    };
  }

  void Admit(MetaCandidateProgressObs candidate) {
    const MetaObservationIdentity identity{candidate.node_id_,
                                           candidate.boot_incarnation_,
                                           candidate.session_generation_};
    ASSERT_TRUE(observations_.AdoptSession(identity, kNowUnixMs - 1).ok());
    ASSERT_TRUE(observations_
                    .Ingest(MetaObservation{.identity_ = identity,
                                            .payload_ = std::move(candidate)},
                            MetaStoresFacts(stores_), kNowUnixMs)
                    .ok());
  }

  SetFailoverRecovery Recovery(std::uint64_t generation,
                               std::uint64_t expected_revision = 0) const {
    SetFailoverRecovery recovery;
    recovery.group_id_ = group_id_;
    recovery.expected_revision_ = expected_revision;
    recovery.recovery_generation_ = generation;
    recovery.old_source_node_id_ = Node('a');
    recovery.old_source_assignment_id_ = former_assignment_;
    recovery.old_source_boot_incarnation_ = former_boot_;
    recovery.old_source_history_id_ = source_history_;
    recovery.excluded_authority_term_ = kCurrentTerm;
    recovery.excluded_authority_version_ = kAuthorityVersion;
    recovery.excluded_grant_revision_ = kGrantRevision;
    recovery.population_manifest_revision_ = kManifestRevision;
    recovery.population_manifest_digest_ = manifest_digest_;
    recovery.partition_replication_epoch_ = kPartitionEpoch;
    recovery.hold_required_ = true;
    return recovery;
  }

  void AdvanceRecoveryGenerationFloor(std::uint64_t generation) {
    ASSERT_TRUE(stores_.failover_recovery_.Set(Recovery(generation), 50).ok());
    auto release = Recovery(generation, /*expected_revision=*/50);
    release.hold_required_ = false;
    ASSERT_TRUE(stores_.failover_recovery_.Set(release, 51).ok());
    ClearFailoverRecovery clear;
    clear.group_id_ = group_id_;
    clear.expected_revision_ = 51;
    clear.recovery_generation_ = generation;
    ASSERT_TRUE(stores_.failover_recovery_.Clear(clear, 52).ok());
  }

  absl::StatusOr<SubmitOperation> Build(
      const MetaOperationId& operation_id = Bytes<16>(0x91),
      const MetaRequestId& request_id = Bytes<16>(0x92)) const {
    return BuildControlledFailoverSubmission(
        group_id_, operation_id, request_id, MetaCommittedView(stores_, 80),
        observations_, kNowUnixMs + 1, 120'000);
  }

  const std::string group_id_ = "group-a";
  const MetaAssignmentId former_assignment_ = Bytes<16>(0x11);
  const MetaAssignmentId candidate_b_assignment_ = Bytes<16>(0x12);
  const MetaAssignmentId candidate_c_assignment_ = Bytes<16>(0x13);
  const MetaBootIncarnation former_boot_ = Bytes<20>(0x21);
  const MetaBootIncarnation candidate_b_boot_ = Bytes<20>(0x22);
  const MetaBootIncarnation candidate_c_boot_ = Bytes<20>(0x23);
  const MetaReplicationHistoryId source_history_ = Bytes<20>(0x31);
  const MetaHash256 manifest_digest_ = Bytes<32>(0x32);
  const MetaGrantSpec old_grant_{.lease_duration_ms_ = 4321,
                                 .policy_id_ = "lease-policy",
                                 .policy_version_ = 5};
  MetaStores stores_;
  MetaObservationStore observations_;
};

TEST_F(ControlledFailoverSubmissionTest,
       ReusesCandidatePlanAndFreezesEveryCurrentAnchor) {
  AdvanceRecoveryGenerationFloor(3);
  Admit(Candidate('c', candidate_c_assignment_, candidate_c_boot_, {31, 37}));
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));

  const CandidatePlan plan = CandidatePlanFor(
      group_id_, MetaStoresFacts(stores_), observations_, kNowUnixMs + 1);
  ASSERT_EQ(plan.disposition_, CandidatePlanDisposition::kSelected);
  ASSERT_TRUE(plan.selected_.has_value());
  ASSERT_EQ(plan.selected_->node_id_, Node('b'));
  EXPECT_EQ(plan.selection_basis_,
            CandidateSelectionBasis::kEqualGreatestNodeTieBreak);

  const MetaOperationId operation_id = Bytes<16>(0xa1);
  const MetaRequestId request_id = Bytes<16>(0xa2);
  auto submission = Build(operation_id, request_id);
  ASSERT_TRUE(submission.ok()) << submission.status();
  EXPECT_EQ(submission->operation_id_, operation_id);
  EXPECT_EQ(submission->request_id_, request_id);
  EXPECT_EQ(submission->kind_, kFailoverOperationKind);
  EXPECT_EQ(submission->intent_hash_, MetaSha256(submission->intent_));
  EXPECT_EQ(submission->replication_history_id_, source_history_);
  EXPECT_EQ(submission->policy_references_,
            (std::vector<MetaPolicyReference>{
                {old_grant_.policy_id_, old_grant_.policy_version_}}));

  auto intent = DecodeFailoverIntent(submission->intent_);
  ASSERT_TRUE(intent.ok()) << intent.status();
  EXPECT_EQ(*intent, (FailoverIntent{
                         .group_id_ = group_id_,
                         .recovery_generation_ = 4,
                         .attempt_timeout_ms_ = 120'000,
                         .former_owner_node_id_ = Node('a'),
                         .former_owner_assignment_id_ = former_assignment_,
                         .former_owner_boot_id_ = former_boot_,
                         .candidate_node_id_ = plan.selected_->node_id_,
                         .candidate_assignment_id_ = candidate_b_assignment_,
                         .candidate_boot_id_ = candidate_b_boot_,
                         .group_term_ = kCurrentTerm + 1,
                         .authority_version_ = kAuthorityVersion,
                         .grant_revision_ = kGrantRevision,
                         .old_grant_ = old_grant_,
                         .population_manifest_revision_ = kManifestRevision,
                         .population_manifest_digest_ = manifest_digest_,
                         .partition_replication_epoch_ = kPartitionEpoch,
                         .parent_history_id_ = source_history_,
                         .flow_count_ = 2,
                     }));
}

TEST_F(ControlledFailoverSubmissionTest,
       ProposalRejectsAWellFormedButNonSelectedCandidate) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {41, 43}));
  Admit(Candidate('c', candidate_c_assignment_, candidate_c_boot_, {31, 37}));

  auto submission = Build();
  ASSERT_TRUE(submission.ok()) << submission.status();
  auto forged_intent = DecodeFailoverIntent(submission->intent_);
  ASSERT_TRUE(forged_intent.ok()) << forged_intent.status();
  ASSERT_EQ(forged_intent->candidate_node_id_, Node('b'));
  forged_intent->candidate_node_id_ = Node('c');
  forged_intent->candidate_assignment_id_ = candidate_c_assignment_;
  forged_intent->candidate_boot_id_ = candidate_c_boot_;
  auto forged_blob = EncodeFailoverIntent(*forged_intent);
  ASSERT_TRUE(forged_blob.ok()) << forged_blob.status();
  submission->intent_ = *forged_blob;
  submission->intent_hash_ = MetaSha256(submission->intent_);

  const absl::Status status = ValidateFailoverProposal(
      MetaCommand(*submission), MetaCommittedView(stores_, 80), observations_);
  EXPECT_EQ(MetaFailureClassOf(status), MetaFailureClass::kDomainReject);
}

TEST_F(ControlledFailoverSubmissionTest, RejectsWhenNoCandidateIsEligible) {
  const auto submission = Build();
  ASSERT_FALSE(submission.ok());
  EXPECT_NE(
      std::string(submission.status().message()).find("no eligible candidate"),
      std::string::npos);
}

TEST_F(ControlledFailoverSubmissionTest,
       RejectsCandidatesFromMultipleCompatibilityDomains) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));
  MetaCandidateProgressObs other =
      Candidate('c', candidate_c_assignment_, candidate_c_boot_, {31, 38});
  other.source_boot_incarnation_ = Bytes<20>(0x55);
  Admit(std::move(other));

  const auto submission = Build();
  ASSERT_FALSE(submission.ok());
  EXPECT_NE(
      std::string(submission.status().message()).find("compatibility domains"),
      std::string::npos);
}

TEST_F(ControlledFailoverSubmissionTest,
       RejectsSelectedCandidateWhoseSourceAssignmentIsStale) {
  MetaCandidateProgressObs candidate =
      Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37});
  candidate.source_assignment_id_ = Bytes<16>(0x7f);
  Admit(std::move(candidate));

  const auto submission = Build();
  ASSERT_FALSE(submission.ok());
  EXPECT_NE(std::string(submission.status().message())
                .find("not compatible with the current primary"),
            std::string::npos);
}

TEST_F(ControlledFailoverSubmissionTest, RejectsAnActiveRecoveryHandoff) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));
  ASSERT_TRUE(stores_.failover_recovery_.Set(Recovery(1), 50).ok());

  const auto submission = Build();
  ASSERT_FALSE(submission.ok());
  EXPECT_NE(std::string(submission.status().message()).find("recovery handoff"),
            std::string::npos);
}

TEST_F(ControlledFailoverSubmissionTest, RejectsAnotherLiveSameGroupWorkflow) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));
  auto first = Build(Bytes<16>(0xb1), Bytes<16>(0xb2));
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(stores_.operation_.SubmitOperation(*first, 70).ok());

  const auto second = Build(Bytes<16>(0xb3), Bytes<16>(0xb4));
  ASSERT_FALSE(second.ok());
  EXPECT_NE(
      std::string(second.status().message()).find("another failover workflow"),
      std::string::npos);
}

TEST_F(ControlledFailoverSubmissionTest,
       ApplyAcceptsExactReplayButRejectsASecondIdForTheSameGroup) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));
  auto first = Build(Bytes<16>(0xc1), Bytes<16>(0xc2));
  auto second = Build(Bytes<16>(0xc3), Bytes<16>(0xc4));
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(second.ok()) << second.status();

  const MetaApplyResult accepted = ApplyCommitted(
      stores_, 100, MetaCommand(*first), "keylane://operator/test", "now");
  ASSERT_EQ(accepted.verdict_, MetaAuditVerdict::kAccepted) << accepted.detail_;
  const MetaApplyResult rejected = ApplyCommitted(
      stores_, 101, MetaCommand(*second), "keylane://operator/test", "now");
  EXPECT_EQ(rejected.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_NE(rejected.detail_.find("another failover workflow"),
            std::string::npos);
  EXPECT_EQ(stores_.operation_.LiveCount(), 1u);

  const MetaApplyResult replay = ApplyCommitted(
      stores_, 102, MetaCommand(*first), "keylane://operator/test", "later");
  EXPECT_EQ(replay.verdict_, MetaAuditVerdict::kAccepted) << replay.detail_;
  EXPECT_EQ(stores_.operation_.LiveCount(), 1u);
}

TEST_F(ControlledFailoverSubmissionTest,
       ApplyRejectsSubmitWhoseFormerAuthorityWasFencedAfterProposal) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));
  auto submission = Build();
  ASSERT_TRUE(submission.ok()) << submission.status();

  FenceGroup fence;
  fence.group_id_ = group_id_;
  fence.expected_term_ = kCurrentTerm;
  const MetaApplyResult fenced = ApplyCommitted(
      stores_, 100, MetaCommand(fence), "keylane://operator/test", "now");
  ASSERT_EQ(fenced.verdict_, MetaAuditVerdict::kAccepted) << fenced.detail_;

  const MetaApplyResult stale_submit = ApplyCommitted(
      stores_, 101, MetaCommand(*submission), "keylane://operator/test", "now");
  EXPECT_EQ(stale_submit.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_FALSE(
      stores_.operation_.FindOperation(submission->operation_id_).has_value());
}

TEST_F(ControlledFailoverSubmissionTest,
       ApplyRejectsSubmitWhoseGroupTermAdvancedAfterProposal) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));
  auto submission = Build();
  ASSERT_TRUE(submission.ok()) << submission.status();

  BeginGroupTerm begin;
  begin.group_id_ = group_id_;
  begin.expected_term_ = kCurrentTerm;
  begin.new_term_ = kCurrentTerm + 1;
  const MetaApplyResult advanced = ApplyCommitted(
      stores_, 100, MetaCommand(begin), "keylane://operator/test", "now");
  ASSERT_EQ(advanced.verdict_, MetaAuditVerdict::kAccepted) << advanced.detail_;

  const MetaApplyResult stale_submit = ApplyCommitted(
      stores_, 101, MetaCommand(*submission), "keylane://operator/test", "now");
  EXPECT_EQ(stale_submit.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_FALSE(
      stores_.operation_.FindOperation(submission->operation_id_).has_value());
}

TEST_F(ControlledFailoverSubmissionTest,
       ApplyRejectsOrdinaryAuthorityMutationsWhileFailoverIsLive) {
  Admit(Candidate('b', candidate_b_assignment_, candidate_b_boot_, {31, 37}));
  auto submission = Build();
  ASSERT_TRUE(submission.ok()) << submission.status();
  const MetaApplyResult submitted = ApplyCommitted(
      stores_, 100, MetaCommand(*submission), "keylane://operator/test", "now");
  ASSERT_EQ(submitted.verdict_, MetaAuditVerdict::kAccepted)
      << submitted.detail_;

  auto intent = DecodeFailoverIntent(submission->intent_);
  ASSERT_TRUE(intent.ok()) << intent.status();
  ASSERT_TRUE(stores_.failover_recovery_
                  .Set(Recovery(intent->recovery_generation_), 101)
                  .ok());

  auto source_holding = EncodeFailoverPhase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kSourceHolding});
  ASSERT_TRUE(source_holding.ok()) << source_holding.status();
  TransitionOperationPhase transition;
  transition.operation_id_ = submission->operation_id_;
  transition.kind_phase_blob_ = *source_holding;
  const MetaApplyResult running = ApplyCommitted(
      stores_, 102, MetaCommand(transition), "keylane://operator/test", "now");
  ASSERT_EQ(running.verdict_, MetaAuditVerdict::kAccepted) << running.detail_;

  const MetaStores protected_state = stores_;
  const auto grant_before = protected_state.grant_.GroupState(group_id_);
  ASSERT_TRUE(grant_before.has_value());

  {
    MetaStores candidate = protected_state;
    FenceGroup fence;
    fence.group_id_ = group_id_;
    fence.expected_term_ = kCurrentTerm;
    EXPECT_FALSE(ValidateFailoverProposal(MetaCommand(fence),
                                          MetaCommittedView(candidate, 102),
                                          observations_)
                     .ok());
    EXPECT_EQ(ApplyCommitted(candidate, 103, MetaCommand(fence),
                             "keylane://operator/test", "now")
                  .verdict_,
              MetaAuditVerdict::kRejected);
    EXPECT_EQ(candidate.grant_.GroupState(group_id_), grant_before);
  }
  {
    MetaStores candidate = protected_state;
    RevokeGrant revoke;
    revoke.group_id_ = group_id_;
    revoke.expected_term_ = kCurrentTerm;
    EXPECT_FALSE(ValidateFailoverProposal(MetaCommand(revoke),
                                          MetaCommittedView(candidate, 102),
                                          observations_)
                     .ok());
    EXPECT_EQ(ApplyCommitted(candidate, 103, MetaCommand(revoke),
                             "keylane://operator/test", "now")
                  .verdict_,
              MetaAuditVerdict::kRejected);
    EXPECT_EQ(candidate.grant_.GroupState(group_id_), grant_before);
  }
  {
    MetaStores candidate = protected_state;
    GrantAuthority grant;
    grant.group_id_ = group_id_;
    grant.node_id_ = Node('a');
    grant.term_ = kCurrentTerm;
    grant.authority_version_ = kAuthorityVersion;
    grant.grant_ = old_grant_;
    ++grant.grant_.lease_duration_ms_;
    EXPECT_FALSE(ValidateFailoverProposal(MetaCommand(grant),
                                          MetaCommittedView(candidate, 102),
                                          observations_)
                     .ok());
    EXPECT_EQ(ApplyCommitted(candidate, 103, MetaCommand(grant),
                             "keylane://operator/test", "now")
                  .verdict_,
              MetaAuditVerdict::kRejected);
    EXPECT_EQ(candidate.grant_.GroupState(group_id_), grant_before);
  }
}

}  // namespace
}  // namespace keylane::meta
