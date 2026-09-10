#include "keylane/meta/failover.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "gtest/gtest.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/hash.h"

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

FailoverIntent Intent() {
  return FailoverIntent{
      .group_id_ = "group-a",
      .former_owner_node_id_ = std::string(40, 'a'),
      .former_owner_assignment_id_ = Bytes<16>(1),
      .former_owner_boot_id_ = Bytes<20>(2),
      .candidate_node_id_ = std::string(40, 'b'),
      .candidate_assignment_id_ = Bytes<16>(3),
      .candidate_boot_id_ = Bytes<20>(4),
      .group_term_ = 2,
      .authority_version_ = 1,
      .grant_revision_ = 11,
      .population_manifest_revision_ = 13,
      .population_manifest_digest_ = Bytes<32>(6),
      .partition_replication_epoch_ = 17,
      .parent_history_id_ = Bytes<20>(7),
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
  (*encoded)[4] = '\x02';
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
  EXPECT_EQ(MetaFailureClassOf(
                ValidateFailoverProposal(MetaCommand(submit),
                                         MetaCommittedView(stores, 0),
                                         observations)),
            MetaFailureClass::kDomainReject);

  auto intent = EncodeFailoverIntent(Intent());
  ASSERT_TRUE(intent.ok()) << intent.status();
  submit.intent_ = *intent;
  submit.intent_hash_ = MetaSha256(submit.intent_);
  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(submit),
                                       MetaCommittedView(stores, 0),
                                       observations)
                  .ok());
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
  EXPECT_EQ(MetaFailureClassOf(
                ValidateFailoverProposal(MetaCommand(transition),
                                         MetaCommittedView(stores, 1),
                                         observations)),
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
  EXPECT_EQ(MetaFailureClassOf(
                ValidateFailoverProposal(MetaCommand(transition),
                                         MetaCommittedView(stores, 1),
                                         observations)),
            MetaFailureClass::kDomainReject);
}

TEST(MetaFailoverValidationTest,
     AcceptsOrderedPrepareAndExactPreparedReceiptEvidence) {
  const FailoverIntent failover = Intent();
  MetaObservationStore observations;
  MetaStores stores;

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
  old_authority.grant_.lease_duration_ms_ = 5000;
  old_authority.grant_.policy_id_ = "lease-policy";
  old_authority.grant_.policy_version_ = 1;
  old_authority.new_authority_version_ = failover.authority_version_;
  ASSERT_TRUE(stores.grant_.ValidateActivate(old_authority,
                                             failover.grant_revision_)
                  .ok());
  ASSERT_TRUE(stores.grant_.ApplyGrantPart(old_authority,
                                           failover.grant_revision_)
                  .ok());
  ASSERT_TRUE(stores.topology_
                  .SetOwner(failover.group_id_,
                            failover.former_owner_node_id_)
                  .ok());
  ASSERT_TRUE(stores.topology_.SetGroupTerm(failover.group_id_, 1).ok());
  ASSERT_TRUE(stores.topology_
                  .SetAuthorityVersion(failover.group_id_,
                                       failover.authority_version_)
                  .ok());
  BeginGroupTerm excluded;
  excluded.group_id_ = failover.group_id_;
  excluded.expected_term_ = 1;
  excluded.new_term_ = failover.group_term_;
  ASSERT_TRUE(stores.grant_.BeginGroupTerm(excluded).ok());
  ASSERT_TRUE(stores.topology_
                  .SetGroupTerm(failover.group_id_, failover.group_term_)
                  .ok());
  ASSERT_TRUE(stores.topology_
                  .SetPopulationManifest(
                      failover.group_id_,
                      failover.population_manifest_revision_,
                      failover.population_manifest_digest_)
                  .ok());
  ASSERT_TRUE(stores.topology_
                  .SetPartitionReplicationEpoch(
                      failover.group_id_,
                      failover.partition_replication_epoch_)
                  .ok());

  auto encoded_intent = EncodeFailoverIntent(failover);
  ASSERT_TRUE(encoded_intent.ok()) << encoded_intent.status();
  SubmitOperation submit;
  submit.operation_id_ = Bytes<16>(9);
  submit.kind_ = std::string(kFailoverOperationKind);
  submit.intent_ = *encoded_intent;
  submit.intent_hash_ = MetaSha256(submit.intent_);
  submit.replication_history_id_ = failover.parent_history_id_;
  ASSERT_TRUE(stores.operation_.SubmitOperation(submit, 1).ok());

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

  const MetaHash256 exclusion_hash = Bytes<32>(8);
  TransitionOperationPhase phase1 = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kOldAuthorityExcluded,
                    .old_authority_exclusion_hash_ = exclusion_hash},
      0);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(phase1),
                                       MetaCommittedView(stores, 1),
                                       observations)
                  .ok());
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(phase1, 2).ok());

  const std::vector<std::uint64_t> required = {19, 23};
  TransitionOperationPhase phase2 = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kCandidateCaughtUp,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required},
      1);
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(phase2),
                                       MetaCommittedView(stores, 2),
                                       observations)
                  .ok());
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(phase2, 3).ok());

  auto payload = cluster::control::EncodePromotionPrepareRequest(
      {.parent_history_id = Hex(failover.parent_history_id_),
       .required_applied_next_lsns = required});
  auto preconditions =
      cluster::control::EncodePromotionPreparePreconditions(
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
  directive.population_manifest_digest_ =
      failover.population_manifest_digest_;
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
      2, {directive});
  ASSERT_TRUE(ValidateFailoverProposal(MetaCommand(phase3),
                                       MetaCommittedView(stores, 3),
                                       observations)
                  .ok());
  ASSERT_TRUE(stores.operation_.TransitionOperationPhase(phase3, 4).ok());

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
  receipt.directive_revision_ = 4;
  receipt.recipient_node_id_ = failover.candidate_node_id_;
  receipt.recipient_boot_id_ = failover.candidate_boot_id_;
  receipt.assignment_id_ = failover.candidate_assignment_id_;
  receipt.result_hash_ = MetaSha256(*result);
  receipt.result_ = *result;
  ASSERT_TRUE(stores.operation_.CommitDirectiveResult(receipt, 5).ok());

  MetaEvidenceSummary evidence{
      .node_id_ = failover.candidate_node_id_,
      .group_id_ = failover.group_id_,
      .assignment_id_ = failover.candidate_assignment_id_,
      .boot_incarnation_ = failover.candidate_boot_id_,
      .group_term_ = failover.group_term_,
      .population_manifest_revision_ =
          failover.population_manifest_revision_,
      .population_manifest_digest_ =
          failover.population_manifest_digest_,
      .partition_replication_epoch_ =
          failover.partition_replication_epoch_,
      .replication_history_id_ = failover.parent_history_id_,
      .operation_id_ = submit.operation_id_,
      .kind_hash_ = receipt.result_hash_,
  };
  TransitionOperationPhase phase4 = transition_phase(
      FailoverPhase{.stage_ = FailoverPhaseStage::kPromotionPrepared,
                    .old_authority_exclusion_hash_ = exclusion_hash,
                    .required_applied_next_lsns_ = required,
                    .prepared_result_hash_ = receipt.result_hash_},
      4, {}, {evidence});
  EXPECT_TRUE(ValidateFailoverProposal(MetaCommand(phase4),
                                       MetaCommittedView(stores, 5),
                                       observations)
                  .ok());
}

}  // namespace
}  // namespace keylane::meta
