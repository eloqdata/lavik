// Journal-side tests for the audit
// store (src/meta/audit_store.cpp), the term/grant store
// (src/meta/grant_store.cpp), and the operation journal store
// (src/meta/operation_store.cpp).
//
// The tests exercise only the public surface: state queryable after applying
// commands, rejection behavior, idempotent replay acceptance vs conflict
// rejection, and serialization round-trips. Apply must be a deterministic pure
// function of (command, committed state): no wall-clock reads, no observation
// access, no IO.

#include <cstdint>
#include <limits>
#include <string>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/audit_store.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/grant_store.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/operation_store.h"

namespace {

using keylane::meta::MetaAuditPolicy;
using keylane::meta::MetaAuditRecord;
using keylane::meta::MetaAuditStore;
using keylane::meta::MetaAuditVerdict;
using keylane::meta::MetaFailureClass;
using keylane::meta::MetaFailureClassOf;
using keylane::meta::MetaHash256;

MetaAuditRecord MakeAuditRecord(
    std::uint64_t index, std::string summary = "CreateGroup(g1)",
    MetaAuditVerdict verdict = MetaAuditVerdict::kAccepted) {
  MetaAuditRecord record;
  record.log_index_ = index;
  record.actor_principal_ = "keylane://operator/alice";
  record.command_summary_ = std::move(summary);
  record.verdict_ = verdict;
  record.verdict_detail_ =
      verdict == MetaAuditVerdict::kAccepted ? "" : "expected_term mismatch";
  // Readable time is carried by the command's ActorContext; the store never
  // reads a clock. Fixed text keeps applies byte-identical across nodes.
  record.readable_time_ = "2026-09-04T17:00:00Z";
  return record;
}

// ---------------------------------------------------------------------------
// MetaAuditStore: append, keyed idempotency, rolling hash chain.
// ---------------------------------------------------------------------------

TEST(MetaAuditStore, AppendAndFindByLogIndex) {
  MetaAuditStore store;
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(2, "FenceGroup(g1)")).ok());

  auto first = store.Find(1);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->record_, MakeAuditRecord(1));
  auto second = store.Find(2);
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->record_.command_summary_, "FenceGroup(g1)");
  EXPECT_FALSE(store.Find(3).has_value());
  EXPECT_EQ(store.size(), 2);
}

TEST(MetaAuditStore, ChainHashIsDeterministicAndLinksAcrossRecords) {
  MetaAuditStore a;
  MetaAuditStore b;
  for (std::uint64_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(a.Append(MakeAuditRecord(i)).ok());
    ASSERT_TRUE(b.Append(MakeAuditRecord(i)).ok());
  }
  // Same input sequence on two independent stores yields the identical chain.
  EXPECT_EQ(a.chain_head(), b.chain_head());
  // Every record's hash differs from its predecessor's (chaining, not a
  // constant), and the all-zero hash never occurs naturally.
  MetaHash256 previous{};
  for (std::uint64_t i = 1; i <= 3; ++i) {
    auto entry = a.Find(i);
    ASSERT_TRUE(entry.has_value());
    EXPECT_NE(entry->chain_hash_, previous);
    previous = entry->chain_hash_;
  }
  // Content anywhere in the chain changes the head.
  MetaAuditStore c;
  for (std::uint64_t i = 1; i <= 2; ++i) {
    ASSERT_TRUE(c.Append(MakeAuditRecord(i)).ok());
  }
  ASSERT_TRUE(c.Append(MakeAuditRecord(3, "SetSlotMap(...)")).ok());
  EXPECT_NE(a.chain_head(), c.chain_head());
}

TEST(MetaAuditStore, ReplaySameIndexSameContentIsNoOp) {
  MetaAuditStore store;
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(2)).ok());
  const MetaHash256 head = store.chain_head();
  // Replay rewrites the identical record at the same index: no-op.
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(2)).ok());
  EXPECT_EQ(store.size(), 2);
  EXPECT_EQ(store.chain_head(), head);
}

TEST(MetaAuditStore, SameIndexDifferentContentFailsStop) {
  MetaAuditStore store;
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  // The same raft log index carrying different content means the apply layer
  // lost the index/record correspondence: an implementation bug, fail-stop.
  EXPECT_DEATH(store.Append(MakeAuditRecord(1, "SetSlotMap(...)")), "");
}

TEST(MetaAuditStore, OutOfOrderNewIndexFailsStop) {
  MetaAuditStore store;
  ASSERT_TRUE(store.Append(MakeAuditRecord(5)).ok());
  // A NEW record below the window's newest index: apply is ordered by log
  // index, so this is an implementation bug, fail-stop.
  EXPECT_DEATH(store.Append(MakeAuditRecord(3)), "");
}

TEST(MetaAuditStore, RejectsOverCapFields) {
  MetaAuditStore store;
  MetaAuditRecord record = MakeAuditRecord(1);
  record.command_summary_ =
      std::string(keylane::meta::kMaxMetaAuditSummaryBytes + 1, 'x');
  const absl::Status status = store.Append(record);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(MetaFailureClassOf(status), MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.size(), 0);
}

// ---------------------------------------------------------------------------
// MetaAuditStore: bounded window, export (drain to bytes), prune, snapshots.
// ---------------------------------------------------------------------------

TEST(MetaAuditStore, DefaultBoundedWindowRotatesAndReportsLoss) {
  MetaAuditStore store(/*window_capacity=*/2);
  EXPECT_FALSE(store.NeedsExport());
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(2)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(3)).ok());
  EXPECT_FALSE(store.Find(1).has_value());
  EXPECT_TRUE(store.Find(2).has_value());
  EXPECT_EQ(store.dropped_total(), 1u);
  EXPECT_EQ(store.dropped_through(), 1u);
  EXPECT_TRUE(store.VerifyChain());
}

TEST(MetaAuditStore, StrictExportFullWindowFailsStopIfGateIsBypassed) {
  MetaAuditStore store(/*window_capacity=*/2);
  ASSERT_TRUE(store.SetPolicy(MetaAuditPolicy::kStrictExport).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(2)).ok());
  EXPECT_TRUE(store.NeedsExport());
  EXPECT_DEATH(store.Append(MakeAuditRecord(3)), "");
}

TEST(MetaAuditStore, DisabledSuppressesOrdinaryRecordsButForcedRecordRemains) {
  MetaAuditStore store(/*window_capacity=*/2);
  ASSERT_TRUE(store.SetPolicy(MetaAuditPolicy::kDisabled).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  EXPECT_EQ(store.size(), 0u);
  ASSERT_TRUE(store
                  .Append(MakeAuditRecord(2, "SetAuditPolicy"),
                          /*force_record=*/true)
                  .ok());
  EXPECT_TRUE(store.Find(2).has_value());
}

TEST(MetaAuditStore, ExportDrainsRecordsWithTheirChainContext) {
  MetaAuditStore store;
  for (std::uint64_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(store.Append(MakeAuditRecord(i)).ok());
  }
  const auto bytes = store.ExportThrough(2);
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const auto decoded = keylane::meta::DecodeMetaAuditExport(*bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_EQ(decoded->records_.size(), 2);
  // The export chains from the genesis anchor and carries per-record hashes,
  // so an external archive can verify continuity and, within its own
  // deployment namespace, deduplicate by (raft_log_index, record_hash).
  EXPECT_EQ(decoded->anchor_before_, MetaHash256{});
  EXPECT_EQ(decoded->records_[0], *store.Find(1));
  EXPECT_EQ(decoded->records_[1], *store.Find(2));
}

TEST(MetaAuditStore, PruneKeepsRemainingChainVerifiable) {
  MetaAuditStore pruned;
  MetaAuditStore full;
  for (std::uint64_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(pruned.Append(MakeAuditRecord(i)).ok());
    ASSERT_TRUE(full.Append(MakeAuditRecord(i)).ok());
  }
  ASSERT_TRUE(pruned.PruneThrough(2).ok());
  EXPECT_FALSE(pruned.Find(1).has_value());
  EXPECT_FALSE(pruned.Find(2).has_value());
  EXPECT_EQ(pruned.pruned_floor(), 2);
  EXPECT_TRUE(pruned.VerifyChain());
  // Window truncation does not perturb the chain: the retained record keeps
  // the hash it had in the untruncated window, and both stores extend it
  // identically.
  EXPECT_EQ(pruned.Find(3)->chain_hash_, full.Find(3)->chain_hash_);
  ASSERT_TRUE(pruned.Append(MakeAuditRecord(4)).ok());
  ASSERT_TRUE(full.Append(MakeAuditRecord(4)).ok());
  EXPECT_EQ(pruned.chain_head(), full.chain_head());
}

TEST(MetaOperationStore, PruneArchiveFreesCapacityAfterExternalExport) {
  keylane::meta::MetaOperationStore store(/*max_active=*/4,
                                          /*max_archived=*/1);
  keylane::meta::SubmitOperation submit;
  submit.operation_id_[0] = 1;
  submit.kind_ = "test";
  submit.intent_hash_[0] = 9;
  ASSERT_TRUE(store.SubmitOperation(submit, /*operation_seq=*/10).ok());
  keylane::meta::CompleteOperation complete;
  complete.operation_id_ = submit.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "done";
  ASSERT_TRUE(store.CompleteOperation(complete).ok());
  keylane::meta::ArchiveOperations archive;
  archive.operation_seqs_ = {10};
  ASSERT_TRUE(store.ArchiveOperations(archive).ok());
  ASSERT_EQ(store.ArchivedCount(), 1u);
  ASSERT_TRUE(store.ExportArchive().ok());

  keylane::meta::PruneOperationArchive prune;
  prune.operation_seqs_ = {10};
  EXPECT_TRUE(store.PruneArchive(prune).ok());
  EXPECT_EQ(store.ArchivedCount(), 0u);
  EXPECT_FALSE(store.FindArchivedBySeq(10).has_value());
  EXPECT_TRUE(store.PruneArchive(prune).ok());
}

TEST(MetaAuditStore, PruneWatermarkMustNameAWindowRecord) {
  MetaAuditStore store;
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(3)).ok());  // gap at 2 is normal
  const absl::Status status = store.PruneThrough(2);   // not a record index
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(MetaFailureClassOf(status), MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.size(), 2);
  // Re-pruning at/below the floor is an idempotent no-op.
  ASSERT_TRUE(store.PruneThrough(1).ok());
  EXPECT_EQ(store.pruned_floor(), 1);
  ASSERT_TRUE(store.PruneThrough(1).ok());
  EXPECT_EQ(store.size(), 1);
}

TEST(MetaAuditStore, AppendAtOrBelowPrunedFloorFailsStop) {
  MetaAuditStore store;
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(2)).ok());
  ASSERT_TRUE(store.PruneThrough(2).ok());
  // Re-applying a pruned index means replay below the snapshot boundary: an
  // implementation bug, fail-stop (the prune is committed state, so a correct
  // recovery never revisits these indexes).
  EXPECT_DEATH(store.Append(MakeAuditRecord(1)), "");
}

TEST(MetaAuditStore, SerializationRoundTripPreservesWindowAndChain) {
  MetaAuditStore store;
  for (std::uint64_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(store.Append(MakeAuditRecord(i)).ok());
  }
  ASSERT_TRUE(store.PruneThrough(1).ok());
  const auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  auto restored = MetaAuditStore::Deserialize(*bytes);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->pruned_floor(), store.pruned_floor());
  EXPECT_EQ(restored->policy(), store.policy());
  EXPECT_EQ(restored->dropped_total(), store.dropped_total());
  EXPECT_EQ(restored->chain_head(), store.chain_head());
  EXPECT_EQ(restored->size(), store.size());
  EXPECT_EQ(restored->Find(2), store.Find(2));
  EXPECT_TRUE(restored->VerifyChain());
  // Appends continue the restored chain identically.
  ASSERT_TRUE(restored->Append(MakeAuditRecord(4)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(4)).ok());
  EXPECT_EQ(restored->chain_head(), store.chain_head());
}

TEST(MetaAuditStore, DeserializeRejectsCorruptionAndChainBreaks) {
  MetaAuditStore store;
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok());
  EXPECT_EQ(MetaFailureClassOf(MetaAuditStore::Deserialize("").status()),
            MetaFailureClass::kFailStop);
  std::string truncated = bytes->substr(0, bytes->size() - 1);
  EXPECT_EQ(MetaFailureClassOf(MetaAuditStore::Deserialize(truncated).status()),
            MetaFailureClass::kFailStop);
  std::string trailing = *bytes + '\x00';
  EXPECT_EQ(MetaFailureClassOf(MetaAuditStore::Deserialize(trailing).status()),
            MetaFailureClass::kFailStop);
  // A tampered stored hash breaks the recomputed chain: fail-stop class.
  std::string tampered = *bytes;
  tampered[tampered.size() - 5] ^= '\x01';
  EXPECT_EQ(MetaFailureClassOf(MetaAuditStore::Deserialize(tampered).status()),
            MetaFailureClass::kFailStop);
}

// ---------------------------------------------------------------------------
// MetaGrantStore: per-group term, grant, and fencing.
// ---------------------------------------------------------------------------

using keylane::meta::ActivateAuthority;
using keylane::meta::BeginGroupTerm;
using keylane::meta::GrantAuthority;
using keylane::meta::MetaFailoverActionId;
using keylane::meta::MetaGrantSpec;
using keylane::meta::MetaGrantStore;
using keylane::meta::RevokeGrant;
using keylane::meta::ValidateMetaGrantSpec;

MetaGrantSpec MakeSpec(std::uint64_t lease_ms = 30000,
                       std::string policy_id = "policy/leader-lease",
                       std::uint64_t policy_version = 7) {
  MetaGrantSpec spec;
  spec.lease_duration_ms_ = lease_ms;
  spec.policy_id_ = std::move(policy_id);
  spec.policy_version_ = policy_version;
  return spec;
}

TEST(MetaGrantStore, SharedGrantSpecValidationMatchesControlWireDomain) {
  EXPECT_TRUE(ValidateMetaGrantSpec(MakeSpec()).ok());
  EXPECT_TRUE(
      ValidateMetaGrantSpec(MakeSpec(std::numeric_limits<std::uint32_t>::max()))
          .ok());

  EXPECT_EQ(MetaFailureClassOf(ValidateMetaGrantSpec(MakeSpec(/*lease_ms=*/0))),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(MetaFailureClassOf(ValidateMetaGrantSpec(
                MakeSpec(static_cast<std::uint64_t>(
                             std::numeric_limits<std::uint32_t>::max()) +
                         1))),
            MetaFailureClass::kDomainReject);
}

BeginGroupTerm MakeBeginTerm(std::string group_id, std::uint64_t expected,
                             std::uint64_t new_term) {
  BeginGroupTerm cmd;
  cmd.group_id_ = std::move(group_id);
  cmd.expected_term_ = expected;
  cmd.new_term_ = new_term;
  return cmd;
}

// An activation carries no new term; only BeginGroupTerm advances it.
ActivateAuthority MakeActivate(std::string group_id,
                               std::uint64_t expected_term,
                               std::string new_owner,
                               std::uint64_t new_authority_version) {
  ActivateAuthority cmd;
  cmd.group_id_ = std::move(group_id);
  cmd.expected_term_ = expected_term;
  cmd.new_owner_ = std::move(new_owner);
  cmd.grant_ = MakeSpec();
  cmd.new_authority_version_ = new_authority_version;
  cmd.new_topology_epoch_ = 100;
  cmd.new_config_epoch_ = 200;
  return cmd;
}

TEST(MetaGrantStore, CommandsOnUnknownGroupReject) {
  MetaGrantStore store;
  EXPECT_EQ(MetaFailureClassOf(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1))),
            MetaFailureClass::kDomainReject);
  ActivateAuthority activate = MakeActivate("g1", 0, "node-a", 1);
  EXPECT_EQ(MetaFailureClassOf(store.ValidateActivate(activate, 1)),
            MetaFailureClass::kDomainReject);
  GrantAuthority grant;
  grant.group_id_ = "g1";
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(grant, 1)),
            MetaFailureClass::kDomainReject);
  RevokeGrant revoke;
  revoke.group_id_ = "g1";
  EXPECT_EQ(MetaFailureClassOf(store.RevokeGrant(revoke)),
            MetaFailureClass::kDomainReject);
  EXPECT_FALSE(store.GroupState("g1").has_value());
  EXPECT_FALSE(store.CurrentGroupTerm("g1").has_value());
}

TEST(MetaGrantStore, AddGroupCreatesFencedGrantlessState) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  const auto state = store.GroupState("g1");
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state->group_term_, 0);
  EXPECT_FALSE(state->grant_.has_value());
  EXPECT_TRUE(state->fenced_);
  EXPECT_EQ(store.CurrentGroupTerm("g1"), 0);
  // Idempotent: re-adding an existing group is a no-op.
  ASSERT_TRUE(store.AddGroup("g1").ok());
  EXPECT_EQ(store.GroupCount(), 1);
  // Empty or over-cap ids are domain rejections.
  EXPECT_EQ(MetaFailureClassOf(store.AddGroup("")),
            MetaFailureClass::kDomainReject);
}

TEST(MetaGrantStore, BeginGroupTermPromotesOnceAndFences) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  auto state = store.GroupState("g1");
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state->group_term_, 1);
  EXPECT_TRUE(state->fenced_);  // promotion enters the no-grant/fenced state
  EXPECT_FALSE(state->grant_.has_value());
  // Replay of the same command: the effect exists and the content is
  // consistent — idempotent no-op accept.
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  EXPECT_EQ(store.GroupState("g1")->group_term_, 1);
  // CAS conflict: expected term does not match the current term.
  EXPECT_EQ(MetaFailureClassOf(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 2))),
            MetaFailureClass::kDomainReject);
  // Terms advance one step per command; skipping is malformed.
  EXPECT_EQ(MetaFailureClassOf(store.BeginGroupTerm(MakeBeginTerm("g1", 1, 3))),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.GroupState("g1")->group_term_, 1);
  // The next legitimate step succeeds.
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 1, 2)).ok());
  EXPECT_EQ(store.GroupState("g1")->group_term_, 2);
}

TEST(MetaGrantStore, ActivateInstallsGrantWithoutMovingTerm) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  // The split primitives: the dispatcher validates, writes the topology part,
  // then applies the grant part atomically.
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());
  const auto state = store.GroupState("g1");
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state->group_term_, 1);  // unchanged by activation
  EXPECT_FALSE(state->fenced_);
  ASSERT_TRUE(state->grant_.has_value());
  EXPECT_EQ(state->grant_->owner_, "node-a");
  EXPECT_EQ(state->grant_->term_, 1);
  EXPECT_EQ(state->grant_->authority_version_, 1);
  EXPECT_EQ(state->grant_->grant_revision_, 10);
  EXPECT_EQ(state->grant_->spec_, MakeSpec());
  EXPECT_EQ(state->last_authority_version_, 1);
  EXPECT_EQ(state->last_grant_revision_, 10);
}

TEST(MetaGrantStore, ActivationActionIdentityIsInstalledPreservedAndCleared) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());

  MetaFailoverActionId action_id{};
  action_id.fill(0x5a);
  ActivateAuthority failover = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(failover, 10, action_id).ok());
  ASSERT_TRUE(store.ApplyGrantPart(failover, 10, action_id).ok());
  ASSERT_EQ(store.GroupState("g1")->grant_->activation_action_id_, action_id);

  GrantAuthority renew;
  renew.group_id_ = "g1";
  renew.node_id_ = "node-a";
  renew.term_ = 1;
  renew.authority_version_ = 1;
  renew.grant_ = MakeSpec(/*lease_ms=*/31000);
  ASSERT_TRUE(store.GrantAuthority(renew, 11).ok());
  ASSERT_EQ(store.GroupState("g1")->grant_->activation_action_id_, action_id);

  const auto encoded = store.Serialize();
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  const auto restored = MetaGrantStore::Deserialize(*encoded);
  ASSERT_TRUE(restored.ok()) << restored.status();
  ASSERT_EQ(restored->GroupState("g1")->grant_->activation_action_id_,
            action_id);

  ActivateAuthority ordinary = MakeActivate("g1", 1, "node-a", 2);
  ASSERT_TRUE(store.ValidateActivate(ordinary, 12).ok());
  ASSERT_TRUE(store.ApplyGrantPart(ordinary, 12).ok());
  EXPECT_FALSE(
      store.GroupState("g1")->grant_->activation_action_id_.has_value());
}

TEST(MetaGrantStore, RejectsZeroPresentActivationActionIdentity) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  const MetaFailoverActionId zero_action{};

  EXPECT_EQ(
      MetaFailureClassOf(store.ValidateActivate(activate, 10, zero_action)),
      MetaFailureClass::kDomainReject);
  EXPECT_DEATH(store.ApplyGrantPart(activate, 10, zero_action), "");
}

TEST(MetaGrantStore, DeserializeRejectsZeroPresentActivationActionIdentity) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  MetaFailoverActionId action_id{};
  action_id.fill(0x5a);
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate, 10, action_id).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10, action_id).ok());

  auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const std::string encoded_action(16, static_cast<char>(0x5a));
  const std::size_t offset = bytes->find(encoded_action);
  ASSERT_NE(offset, std::string::npos);
  bytes->replace(offset, encoded_action.size(), encoded_action.size(), '\0');

  const auto restored = MetaGrantStore::Deserialize(*bytes);
  ASSERT_FALSE(restored.ok());
  EXPECT_EQ(MetaFailureClassOf(restored.status()), MetaFailureClass::kFailStop);
}

TEST(MetaGrantStore, RejectsLeaseDurationOutsideControlWireDomain) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());

  ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  activate.grant_.lease_duration_ms_ = 0;
  EXPECT_EQ(MetaFailureClassOf(store.ValidateActivate(activate, 10)),
            MetaFailureClass::kDomainReject);
  activate.grant_.lease_duration_ms_ =
      static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 1;
  EXPECT_EQ(MetaFailureClassOf(store.ValidateActivate(activate, 10)),
            MetaFailureClass::kDomainReject);

  activate.grant_.lease_duration_ms_ =
      std::numeric_limits<std::uint32_t>::max();
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());

  GrantAuthority renew;
  renew.group_id_ = "g1";
  renew.node_id_ = "node-a";
  renew.term_ = 1;
  renew.authority_version_ = 1;
  renew.grant_ = MakeSpec(/*lease_ms=*/0);
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(renew, 11)),
            MetaFailureClass::kDomainReject);
  renew.grant_.lease_duration_ms_ =
      static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 1;
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(renew, 11)),
            MetaFailureClass::kDomainReject);
}

TEST(MetaGrantStore, ActivateRejectsZeroServingTerm) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());

  const ActivateAuthority activate = MakeActivate("g1", 0, "node-a", 1);
  EXPECT_EQ(MetaFailureClassOf(store.ValidateActivate(activate, 10)),
            MetaFailureClass::kDomainReject);
  EXPECT_TRUE(store.GroupState("g1")->fenced_);
  EXPECT_FALSE(store.GroupState("g1")->grant_.has_value());
}

TEST(MetaGrantStore, ActivateWithStaleTermRejects) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  // A candidate from the previous term can never activate.
  EXPECT_EQ(MetaFailureClassOf(
                store.ValidateActivate(MakeActivate("g1", 0, "n", 1), 10)),
            MetaFailureClass::kDomainReject);
  // A future term equally cannot.
  EXPECT_EQ(MetaFailureClassOf(
                store.ValidateActivate(MakeActivate("g1", 2, "n", 1), 10)),
            MetaFailureClass::kDomainReject);
  EXPECT_TRUE(store.GroupState("g1")->fenced_);
}

TEST(MetaGrantStore, ActivateReplayIdempotentAndVersionConflictRejected) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());
  // Replay of the same activation: identical content already installed —
  // idempotent no-op accept through both primitives.
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());
  EXPECT_EQ(store.GroupState("g1")->grant_->owner_, "node-a");
  // Same term, same authority version, different content: conflict reject.
  ActivateAuthority conflict = MakeActivate("g1", 1, "node-b", 1);
  EXPECT_EQ(MetaFailureClassOf(store.ValidateActivate(conflict, 11)),
            MetaFailureClass::kDomainReject);
  // An older authority version never installs.
  EXPECT_EQ(MetaFailureClassOf(
                store.ValidateActivate(MakeActivate("g1", 1, "node-b", 0), 11)),
            MetaFailureClass::kDomainReject);
  // A newer authority version in the same term replaces the grant (planned
  // migration commits through the same atomic point).
  const ActivateAuthority migration = MakeActivate("g1", 1, "node-b", 2);
  ASSERT_TRUE(store.ValidateActivate(migration, 11).ok());
  ASSERT_TRUE(store.ApplyGrantPart(migration, 11).ok());
  const auto state = store.GroupState("g1");
  EXPECT_EQ(state->grant_->owner_, "node-b");
  EXPECT_EQ(state->grant_->authority_version_, 2);
  EXPECT_EQ(state->group_term_, 1);
}

TEST(MetaGrantStore, GrantAuthorityRenewsLeaseForSameOwnerOnly) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  // No grant exists while fenced: renewal rejects.
  GrantAuthority renew;
  renew.group_id_ = "g1";
  renew.node_id_ = "node-a";
  renew.term_ = 1;
  renew.authority_version_ = 1;
  renew.grant_ = MakeSpec(/*lease_ms=*/60000);
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(renew, 11)),
            MetaFailureClass::kDomainReject);

  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());
  ASSERT_TRUE(store.GrantAuthority(renew, 11).ok());
  auto state = store.GroupState("g1");
  EXPECT_EQ(state->grant_->spec_.lease_duration_ms_, 60000);
  EXPECT_EQ(state->grant_->term_, 1);               // unchanged
  EXPECT_EQ(state->grant_->authority_version_, 1);  // unchanged
  EXPECT_EQ(state->grant_->grant_revision_, 11);
  // Replay installs the same spec again: idempotent.
  ASSERT_TRUE(store.GrantAuthority(renew, 11).ok());
  EXPECT_EQ(store.GroupState("g1")->grant_->grant_revision_, 11);
  // Owner/term/authority_version are CAS tokens; each mismatch rejects.
  GrantAuthority wrong_owner = renew;
  wrong_owner.node_id_ = "node-b";
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(wrong_owner, 12)),
            MetaFailureClass::kDomainReject);
  GrantAuthority wrong_term = renew;
  wrong_term.term_ = 2;
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(wrong_term, 12)),
            MetaFailureClass::kDomainReject);
  GrantAuthority wrong_version = renew;
  wrong_version.authority_version_ = 2;
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(wrong_version, 12)),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.GroupState("g1")->grant_->spec_.lease_duration_ms_, 60000);
}

TEST(MetaGrantStore, RevokeAndFenceDropTheGrant) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());

  RevokeGrant revoke;
  revoke.group_id_ = "g1";
  revoke.expected_term_ = 2;  // stale term rejects
  EXPECT_EQ(MetaFailureClassOf(store.RevokeGrant(revoke)),
            MetaFailureClass::kDomainReject);
  revoke.expected_term_ = 1;
  ASSERT_TRUE(store.RevokeGrant(revoke).ok());
  EXPECT_TRUE(store.GroupState("g1")->fenced_);
  EXPECT_FALSE(store.GroupState("g1")->grant_.has_value());
  // The authority version survives revocation so a stale activation still
  // cannot install (checked against last_authority_version_).
  EXPECT_EQ(store.GroupState("g1")->last_authority_version_, 1);
  EXPECT_EQ(store.GroupState("g1")->last_grant_revision_, 10);
  // Replay: already revoked — idempotent no-op accept.
  ASSERT_TRUE(store.RevokeGrant(revoke).ok());

  keylane::meta::FenceGroup fence;
  fence.group_id_ = "g1";
  fence.expected_term_ = 1;
  ASSERT_TRUE(store.FenceGroup(fence).ok());  // already fenced: no-op accept
  fence.expected_term_ = 9;
  EXPECT_EQ(MetaFailureClassOf(store.FenceGroup(fence)),
            MetaFailureClass::kDomainReject);
}

TEST(MetaGrantStore, FactQueriesTrackGrantState) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  EXPECT_EQ(store.CurrentGroupTerm("g1"), 1);
  EXPECT_FALSE(store.PolicyInUse("policy/leader-lease", 7));
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());
  // PolicyInUse feeds the RetirePolicy guard.
  EXPECT_TRUE(store.PolicyInUse("policy/leader-lease", 7));
  EXPECT_FALSE(store.PolicyInUse("policy/leader-lease", 8));
  EXPECT_FALSE(store.PolicyInUse("policy/other", 7));
  RevokeGrant revoke;
  revoke.group_id_ = "g1";
  revoke.expected_term_ = 1;
  ASSERT_TRUE(store.RevokeGrant(revoke).ok());
  EXPECT_FALSE(store.PolicyInUse("policy/leader-lease", 7));
}

TEST(MetaGrantStore, RemoveGroupLifecycle) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());
  // A live grant must be revoked before the group record can go.
  EXPECT_EQ(MetaFailureClassOf(store.RemoveGroup("g1")),
            MetaFailureClass::kDomainReject);
  RevokeGrant revoke;
  revoke.group_id_ = "g1";
  revoke.expected_term_ = 1;
  ASSERT_TRUE(store.RevokeGrant(revoke).ok());
  ASSERT_TRUE(store.RemoveGroup("g1").ok());
  EXPECT_FALSE(store.GroupState("g1").has_value());
  ASSERT_TRUE(store.RemoveGroup("g1").ok());  // idempotent no-op
}

TEST(MetaGrantStore, GroupCountCapEnforced) {
  MetaGrantStore store(/*max_groups=*/2);
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.AddGroup("g2").ok());
  EXPECT_EQ(MetaFailureClassOf(store.AddGroup("g3")),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.GroupCount(), 2);
}

TEST(MetaGrantStore, ApplyGrantPartWithoutValidateFailsStop) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  // Applying the grant part against a term the validation could not have
  // accepted is an apply-layer contract violation: fail-stop.
  EXPECT_DEATH(store.ApplyGrantPart(MakeActivate("g1", 2, "node-a", 1), 10),
               "");
  EXPECT_DEATH(store.ApplyGrantPart(MakeActivate("g9", 1, "node-a", 1), 10),
               "");
}

TEST(MetaGrantStore, SerializationRoundTripPreservesState) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.AddGroup("g2").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate, 10).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate, 10).ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g2", 0, 1)).ok());

  const auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  auto restored = MetaGrantStore::Deserialize(*bytes);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->GroupState("g1"), store.GroupState("g1"));
  EXPECT_EQ(restored->GroupState("g2"), store.GroupState("g2"));
  EXPECT_EQ(restored->GroupCount(), 2);
  EXPECT_TRUE(restored->PolicyInUse("policy/leader-lease", 7));
  // Behavior continues identically after restore: replay idempotency and CAS
  // checks are unaffected by a snapshot round-trip.
  ASSERT_TRUE(restored->ValidateActivate(activate, 10).ok());  // replay no-op
  ASSERT_TRUE(restored->BeginGroupTerm(MakeBeginTerm("g2", 1, 2)).ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g2", 1, 2)).ok());
  // Replay of that same command against the restored state: idempotent.
  ASSERT_TRUE(restored->BeginGroupTerm(MakeBeginTerm("g2", 1, 2)).ok());
  EXPECT_EQ(restored->GroupState("g2"), store.GroupState("g2"));
}

TEST(MetaGrantStore, DeserializeRejectsCorruption) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  const auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok());
  EXPECT_EQ(MetaFailureClassOf(MetaGrantStore::Deserialize("").status()),
            MetaFailureClass::kFailStop);
  const std::string truncated = bytes->substr(0, bytes->size() - 1);
  EXPECT_EQ(MetaFailureClassOf(MetaGrantStore::Deserialize(truncated).status()),
            MetaFailureClass::kFailStop);
  const std::string trailing = *bytes + '\x00';
  EXPECT_EQ(MetaFailureClassOf(MetaGrantStore::Deserialize(trailing).status()),
            MetaFailureClass::kFailStop);
}

TEST(MetaGrantStore, DeserializeRejectsUnprojectableLeaseDuration) {
  keylane::meta::MetaWriter writer;
  writer.WriteU16(keylane::meta::kMetaFormatVersion);
  writer.WriteCount(1);
  writer.WriteString("g1");
  writer.WriteU64(1);   // group term
  writer.WriteU64(1);   // last authority version
  writer.WriteU64(10);  // last grant revision
  writer.WriteBool(false);
  writer.WriteU8(1);  // active grant present
  writer.WriteString("node-a");
  writer.WriteU64(1);       // grant term
  writer.WriteU64(1);       // authority version
  writer.WriteU64(10);      // grant revision
  writer.WriteBool(false);  // no failover activation action
  writer.WriteU64(0);       // lease duration cannot be projected to the wire
  writer.WriteString("policy/leader-lease");
  writer.WriteU64(7);

  const std::string bytes = writer.TakeBuffer();
  const auto restored = MetaGrantStore::Deserialize(bytes);
  ASSERT_FALSE(restored.ok());
  EXPECT_EQ(MetaFailureClassOf(restored.status()), MetaFailureClass::kFailStop);
}

// ---------------------------------------------------------------------------
// MetaOperationStore: the client-id-keyed operation journal with terminal
// tombstone archival.
// ---------------------------------------------------------------------------

using keylane::meta::AbortOperation;
using keylane::meta::ArchiveOperations;
using keylane::meta::CompleteOperation;
using keylane::meta::MetaEvidenceSummary;
using keylane::meta::MetaOperationId;
using keylane::meta::MetaOperationLifecycle;
using keylane::meta::MetaOperationStore;
using keylane::meta::SubmitOperation;
using keylane::meta::TransitionOperationPhase;

MetaOperationId MakeOperationId(std::uint8_t tag) {
  MetaOperationId id{};
  id.fill(tag);
  return id;
}

MetaHash256 MakeIntentHash(std::uint8_t tag) {
  MetaHash256 hash{};
  hash.fill(tag);
  return hash;
}

SubmitOperation MakeSubmit(const MetaOperationId& id, std::uint8_t intent_tag,
                           std::string kind = "failover") {
  SubmitOperation cmd;
  cmd.operation_id_ = id;
  cmd.kind_ = std::move(kind);
  cmd.intent_ = "intent-bytes";
  cmd.intent_hash_ = MakeIntentHash(intent_tag);
  cmd.replication_history_id_.fill(22);
  cmd.actor_.principal_ = "keylane://operator/alice";
  cmd.actor_.readable_time_ = "2026-09-04T17:00:00Z";
  return cmd;
}

MetaEvidenceSummary MakeEvidence(std::string node_id, std::uint64_t term,
                                 MetaOperationId operation_id = {}) {
  MetaEvidenceSummary evidence;
  evidence.node_id_ = std::move(node_id);
  evidence.group_id_ = "group-a";
  evidence.assignment_id_.fill(21);
  evidence.boot_incarnation_.fill(20);
  evidence.group_term_ = term;
  evidence.population_manifest_revision_ = 11;
  evidence.population_manifest_digest_.fill(19);
  evidence.partition_replication_epoch_ = 12;
  evidence.replication_history_id_.fill(22);
  evidence.operation_id_ = operation_id;
  return evidence;
}

TEST(MetaOperationStore, SubmitCreatesSubmittedRecordKeyedByClientId) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  const auto result = store.SubmitOperation(MakeSubmit(id, 42),
                                            /*operation_seq=*/100);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result->created_);
  EXPECT_FALSE(result->archived_);

  const auto record = store.FindOperation(id);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->operation_id_, id);
  EXPECT_EQ(record->operation_seq_, 100);  // the submit command's log index
  EXPECT_EQ(record->kind_, "failover");
  EXPECT_EQ(record->intent_, "intent-bytes");
  EXPECT_EQ(record->intent_hash_, MakeIntentHash(42));
  EXPECT_EQ(record->lifecycle_, MetaOperationLifecycle::kSubmitted);
  EXPECT_EQ(record->revision_, 0);
  EXPECT_EQ(record->actor_.principal_, "keylane://operator/alice");
  // The seq index resolves the same record.
  ASSERT_TRUE(store.FindOperationBySeq(100).has_value());
  EXPECT_EQ(store.FindOperationBySeq(100)->operation_id_, id);
  EXPECT_EQ(store.ActiveCount(), 1);
  EXPECT_TRUE(store.HasActiveKind("failover"));
  EXPECT_FALSE(
      store.HasActiveKind(keylane::meta::kMetaClusterCreateOperationKind));
  EXPECT_TRUE(store.OperationKnown(id));
  EXPECT_FALSE(store.OperationKnown(MakeOperationId(9)));
}

TEST(MetaOperationStore, DuplicateSubmitIsIdempotentOnlyForSameIntent) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  // Same id + same intent_hash: idempotent accept returning the existing
  // record, even with a different log index (a retried client request that
  // got logged twice converges.
  const auto dup = store.SubmitOperation(MakeSubmit(id, 42), 150);
  ASSERT_TRUE(dup.ok()) << dup.status();
  EXPECT_FALSE(dup->created_);
  EXPECT_EQ(store.LiveCount(), 1);
  EXPECT_EQ(store.FindOperation(id)->operation_seq_, 100);
  // Same id + different intent_hash: payload reuse, rejected.
  const auto conflict = store.SubmitOperation(MakeSubmit(id, 43), 151);
  ASSERT_FALSE(conflict.ok());
  EXPECT_EQ(MetaFailureClassOf(conflict.status()),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.LiveCount(), 1);
}

TEST(MetaOperationStore, DirectiveRevisionTracksOnlySemanticChanges) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());

  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_.fill(1);
  directive.attempt_id_.fill(2);
  directive.assignment_id_.fill(3);
  directive.recipient_node_id_ = std::string(40, 'a');
  directive.target_node_id_ = std::string(40, 'a');
  directive.target_boot_id_.fill(4);
  directive.source_node_id_ = std::string(40, 'b');
  directive.source_assignment_id_.fill(7);
  directive.source_boot_id_.fill(5);
  directive.source_replication_history_id_.fill(6);
  directive.group_id_ = "g1";
  directive.group_term_ = 7;
  directive.authority_version_ = 8;
  directive.grant_revision_ = 9;
  directive.partition_replication_epoch_ = 10;
  directive.kind_ = "rebuild";
  directive.payload_ = *keylane::cluster::control::EncodeRebuildRequest({3});
  directive.storage_mutating_ = true;

  TransitionOperationPhase first;
  first.operation_id_ = id;
  keylane::meta::MetaDirectiveSpec ambiguous = directive;
  ambiguous.directive_id_.fill(10);
  first.current_directives_ = {directive, ambiguous};
  EXPECT_EQ(MetaFailureClassOf(store.TransitionOperationPhase(first, 101)),
            MetaFailureClass::kDomainReject);
  keylane::meta::MetaDirectiveSpec misrouted = directive;
  misrouted.recipient_node_id_ = misrouted.source_node_id_;
  first.current_directives_ = {misrouted};
  EXPECT_EQ(MetaFailureClassOf(store.TransitionOperationPhase(first, 101)),
            MetaFailureClass::kDomainReject);
  first.current_directives_ = {directive};
  ASSERT_TRUE(store.TransitionOperationPhase(first, 101).ok());
  ASSERT_EQ(store.FindOperation(id)->current_directives_.size(), 1u);
  EXPECT_EQ(store.FindOperation(id)->current_directives_[0].directive_revision_,
            101u);

  TransitionOperationPhase unchanged = first;
  unchanged.expected_revision_ = 1;
  ASSERT_TRUE(store.TransitionOperationPhase(unchanged, 102).ok());
  EXPECT_EQ(store.FindOperation(id)->current_directives_[0].directive_revision_,
            101u);

  TransitionOperationPhase unsupported = unchanged;
  unsupported.expected_revision_ = 2;
  unsupported.current_directives_[0].payload_ = "not-interpreted-in-v1";
  EXPECT_EQ(
      MetaFailureClassOf(store.TransitionOperationPhase(unsupported, 103)),
      MetaFailureClass::kDomainReject);

  TransitionOperationPhase changed = unchanged;
  changed.expected_revision_ = 2;
  changed.current_directives_[0].target_boot_id_.fill(9);
  ASSERT_TRUE(store.TransitionOperationPhase(changed, 104).ok());
  EXPECT_EQ(store.FindOperation(id)->current_directives_[0].directive_revision_,
            104u);

  const auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const auto restored = MetaOperationStore::Deserialize(*bytes);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->FindOperation(id)->current_directives_,
            store.FindOperation(id)->current_directives_);
}

TEST(MetaOperationStore,
     TerminalReceiptSurvivesLostAckReplayArchiveAndSnapshot) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());

  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_.fill(1);
  directive.attempt_id_.fill(2);
  directive.recipient_node_id_ = std::string(40, 'b');
  // A stable node can reappear under another boot. Kind, not node-id equality,
  // selects the source incarnation that must sign an authorize result.
  directive.target_node_id_ = directive.recipient_node_id_;
  directive.target_boot_id_.fill(3);
  directive.assignment_id_.fill(4);
  directive.source_node_id_ = std::string(40, 'b');
  directive.source_assignment_id_.fill(7);
  directive.source_boot_id_.fill(5);
  directive.source_replication_history_id_.fill(6);
  directive.group_id_ = "g1";
  directive.group_term_ = 7;
  directive.authority_version_ = 8;
  directive.grant_revision_ = 9;
  directive.partition_replication_epoch_ = 10;
  directive.kind_ = "authorize-source";
  directive.payload_ = *keylane::cluster::control::EncodeRebuildRequest({3});
  TransitionOperationPhase transition;
  transition.operation_id_ = id;
  transition.current_directives_ = {directive};
  ASSERT_TRUE(store.TransitionOperationPhase(transition, 101).ok());

  keylane::meta::CommitDirectiveResult commit;
  commit.operation_id_ = id;
  commit.directive_id_ = directive.directive_id_;
  commit.attempt_id_ = directive.attempt_id_;
  commit.directive_revision_ = 101;
  commit.recipient_node_id_ = directive.recipient_node_id_;
  commit.recipient_boot_id_ = directive.source_boot_id_;
  commit.assignment_id_ = directive.assignment_id_;
  commit.status_ = keylane::meta::MetaDirectiveResultStatus::kSucceeded;
  commit.result_ = "installed";
  commit.result_hash_ = keylane::meta::MetaSha256(commit.result_);
  auto wrong_role_boot = commit;
  wrong_role_boot.recipient_boot_id_ = directive.target_boot_id_;
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(
                store.CommitDirectiveResult(wrong_role_boot, 102)),
            keylane::meta::MetaFailureClass::kDomainReject);
  ASSERT_TRUE(store.CommitDirectiveResult(commit, 102).ok());
  ASSERT_EQ(store.FindOperation(id)->revision_, 2u);

  // A reconciler that built its next phase from revision 1 before the result
  // committed must reload the authoritative receipt instead of overwriting
  // it with a stale transition.
  TransitionOperationPhase stale_transition = transition;
  stale_transition.expected_revision_ = 1;
  stale_transition.kind_phase_blob_ = "stale-after-result";
  EXPECT_EQ(
      MetaFailureClassOf(store.TransitionOperationPhase(stale_transition, 103)),
      MetaFailureClass::kDomainReject);

  const keylane::meta::MetaTerminalReceiptKey key{id, directive.directive_id_,
                                                  directive.attempt_id_, 101};
  auto receipt = store.FindTerminalReceipt(key);
  ASSERT_TRUE(receipt.has_value());
  EXPECT_EQ(receipt->committed_index_, 102u);
  EXPECT_EQ(receipt->result_, "installed");

  // The data node did not receive ResultCommitted and sends the same result
  // again through a later proposal. It resolves to the first receipt.
  ASSERT_TRUE(store.CommitDirectiveResult(commit, 110).ok());
  EXPECT_EQ(store.FindTerminalReceipt(key)->committed_index_, 102u);
  EXPECT_EQ(store.FindOperation(id)->revision_, 2u);

  CompleteOperation complete;
  complete.operation_id_ = id;
  complete.expected_revision_ = 2;
  ASSERT_TRUE(store.CompleteOperation(complete).ok());
  ArchiveOperations archive;
  archive.operation_seqs_ = {100};
  ASSERT_TRUE(store.ArchiveOperations(archive).ok());
  EXPECT_EQ(store.FindTerminalReceipt(key)->committed_index_, 102u);

  const auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const auto restored = MetaOperationStore::Deserialize(*bytes);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->FindTerminalReceipt(key), store.FindTerminalReceipt(key));
}

TEST(MetaOperationStore,
     TerminalReceiptRejectsConflictAndPruneMeansNoLongerTracked) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());

  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_.fill(1);
  directive.attempt_id_.fill(2);
  directive.recipient_node_id_ = std::string(40, 'a');
  directive.target_node_id_ = std::string(40, 'a');
  directive.target_boot_id_.fill(3);
  directive.assignment_id_.fill(4);
  directive.source_node_id_ = std::string(40, 'b');
  directive.source_assignment_id_.fill(7);
  directive.source_boot_id_.fill(5);
  directive.source_replication_history_id_.fill(6);
  directive.group_id_ = "g1";
  directive.group_term_ = 7;
  directive.authority_version_ = 8;
  directive.grant_revision_ = 9;
  directive.partition_replication_epoch_ = 10;
  directive.kind_ = "rebuild";
  directive.payload_ = *keylane::cluster::control::EncodeRebuildRequest({3});
  TransitionOperationPhase transition;
  transition.operation_id_ = id;
  transition.current_directives_ = {directive};
  ASSERT_TRUE(store.TransitionOperationPhase(transition, 101).ok());

  keylane::meta::CommitDirectiveResult commit;
  commit.operation_id_ = id;
  commit.directive_id_ = directive.directive_id_;
  commit.attempt_id_ = directive.attempt_id_;
  commit.directive_revision_ = 101;
  commit.recipient_node_id_ = directive.recipient_node_id_;
  commit.recipient_boot_id_ = directive.target_boot_id_;
  commit.assignment_id_ = directive.assignment_id_;
  commit.status_ = keylane::meta::MetaDirectiveResultStatus::kSucceeded;
  commit.result_ = "installed";
  commit.result_hash_ = keylane::meta::MetaSha256(commit.result_);
  ASSERT_TRUE(store.CommitDirectiveResult(commit, 102).ok());
  EXPECT_EQ(store.FindOperation(id)->revision_, 2u);

  keylane::meta::CommitDirectiveResult conflict = commit;
  conflict.status_ = keylane::meta::MetaDirectiveResultStatus::kFailed;
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(
                store.CommitDirectiveResult(conflict, 103)),
            keylane::meta::MetaFailureClass::kDomainReject);

  const keylane::meta::MetaTerminalReceiptKey key{id, directive.directive_id_,
                                                  directive.attempt_id_, 101};
  keylane::meta::PruneTerminalReceipts prune;
  prune.receipts_ = {key};
  keylane::meta::PruneTerminalReceipts missing_directive = prune;
  missing_directive.receipts_[0].directive_id_.fill(0);
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(
                store.PruneTerminalReceipts(missing_directive)),
            keylane::meta::MetaFailureClass::kDomainReject);
  EXPECT_EQ(
      keylane::meta::MetaFailureClassOf(store.PruneTerminalReceipts(prune)),
      keylane::meta::MetaFailureClass::kDomainReject);
  EXPECT_TRUE(store.FindTerminalReceipt(key).has_value());

  CompleteOperation complete;
  complete.operation_id_ = id;
  complete.expected_revision_ = 2;
  ASSERT_TRUE(store.CompleteOperation(complete).ok());
  ArchiveOperations archive;
  archive.operation_seqs_ = {100};
  ASSERT_TRUE(store.ArchiveOperations(archive).ok());
  ASSERT_TRUE(store.PruneTerminalReceipts(prune).ok());
  EXPECT_FALSE(store.FindTerminalReceipt(key).has_value());
  const absl::Status replay = store.CommitDirectiveResult(commit, 104);
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(replay),
            keylane::meta::MetaFailureClass::kDomainReject);
  EXPECT_NE(replay.message().find("no longer tracked"), std::string_view::npos);
  ASSERT_TRUE(store.PruneTerminalReceipts(prune).ok());
}

TEST(MetaOperationStore, TerminalReceiptRetentionIsBoundedPerOperation) {
  MetaOperationStore store(/*max_active=*/4, /*max_archived=*/4,
                           /*max_terminal_receipts_per_operation=*/1);
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());

  keylane::meta::MetaDirectiveSpec first;
  first.directive_id_.fill(1);
  first.attempt_id_.fill(2);
  first.recipient_node_id_ = std::string(40, 'a');
  first.target_node_id_ = std::string(40, 'a');
  first.target_boot_id_.fill(3);
  first.assignment_id_.fill(4);
  first.source_node_id_ = std::string(40, 'b');
  first.source_assignment_id_.fill(7);
  first.source_boot_id_.fill(5);
  first.source_replication_history_id_.fill(6);
  first.group_id_ = "g1";
  first.group_term_ = 7;
  first.authority_version_ = 8;
  first.grant_revision_ = 9;
  first.partition_replication_epoch_ = 10;
  first.kind_ = "rebuild";
  first.payload_ = *keylane::cluster::control::EncodeRebuildRequest({3});
  keylane::meta::MetaDirectiveSpec second = first;
  second.directive_id_.fill(10);
  second.attempt_id_.fill(11);

  TransitionOperationPhase transition;
  transition.operation_id_ = id;
  transition.current_directives_ = {first, second};
  ASSERT_TRUE(store.TransitionOperationPhase(transition, 101).ok());

  const auto make_result = [&](const keylane::meta::MetaDirectiveSpec& spec) {
    keylane::meta::CommitDirectiveResult result;
    result.operation_id_ = id;
    result.directive_id_ = spec.directive_id_;
    result.attempt_id_ = spec.attempt_id_;
    result.directive_revision_ = 101;
    result.recipient_node_id_ = spec.recipient_node_id_;
    result.recipient_boot_id_ = spec.target_boot_id_;
    result.assignment_id_ = spec.assignment_id_;
    result.result_hash_ = keylane::meta::MetaSha256(result.result_);
    return result;
  };
  ASSERT_TRUE(store.CommitDirectiveResult(make_result(first), 102).ok());
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(
                store.CommitDirectiveResult(make_result(second), 103)),
            keylane::meta::MetaFailureClass::kDomainReject);
  EXPECT_TRUE(store.CommitDirectiveResult(make_result(first), 104)
                  .ok());  // replay bypasses capacity
}

TransitionOperationPhase MakeTransition(const MetaOperationId& id,
                                        std::uint64_t expected_revision,
                                        std::string blob = "phase-1") {
  TransitionOperationPhase cmd;
  cmd.operation_id_ = id;
  cmd.expected_revision_ = expected_revision;
  cmd.kind_phase_blob_ = std::move(blob);
  cmd.evidence_.push_back(MakeEvidence(std::string(40, 'a'), 3, id));
  return cmd;
}

CompleteOperation MakeComplete(const MetaOperationId& id,
                               std::uint64_t expected_revision,
                               std::string result = "done",
                               bool data_loss = false) {
  CompleteOperation cmd;
  cmd.operation_id_ = id;
  cmd.expected_revision_ = expected_revision;
  cmd.result_ = std::move(result);
  cmd.data_loss_possible_ = data_loss;
  return cmd;
}

TEST(MetaOperationStore, TransitionRunsWithRevisionCasAndEvidence) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());

  // CAS conflict: the record is at revision 0.
  EXPECT_EQ(MetaFailureClassOf(store.TransitionOperationPhase(
                MakeTransition(id, /*expected_revision=*/7))),
            MetaFailureClass::kDomainReject);

  const TransitionOperationPhase transition = MakeTransition(id, 0);
  ASSERT_TRUE(store.TransitionOperationPhase(transition).ok());
  auto record = store.FindOperation(id);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->lifecycle_, MetaOperationLifecycle::kRunning);
  EXPECT_EQ(record->revision_, 1);  // expected + 1
  EXPECT_EQ(record->kind_phase_blob_, "phase-1");
  ASSERT_EQ(record->evidence_.size(), 1);
  EXPECT_EQ(record->evidence_[0], MakeEvidence(std::string(40, 'a'), 3, id));
  EXPECT_EQ(record->kind_, "failover");  // immutable across transitions

  // Replay of the same command: post-effect already present with identical
  // content — idempotent no-op accept (revision CAS would otherwise reject).
  ASSERT_TRUE(store.TransitionOperationPhase(transition).ok());
  EXPECT_EQ(store.FindOperation(id)->revision_, 1);
  EXPECT_EQ(store.FindOperation(id)->evidence_.size(), 1);

  // A DIFFERENT phase at the same expected revision is a conflict.
  EXPECT_EQ(MetaFailureClassOf(store.TransitionOperationPhase(
                MakeTransition(id, 0, "phase-other"))),
            MetaFailureClass::kDomainReject);
  // The next phase advances with the new revision.
  TransitionOperationPhase next = MakeTransition(id, 1, "phase-2");
  ASSERT_TRUE(store.TransitionOperationPhase(next).ok());
  record = store.FindOperation(id);
  EXPECT_EQ(record->revision_, 2);
  EXPECT_EQ(record->kind_phase_blob_, "phase-2");
  EXPECT_EQ(record->evidence_.size(), 2);  // evidence accumulates
}

TEST(MetaOperationStore, TransitionsOnUnknownOrTerminalOperationsReject) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  EXPECT_EQ(
      MetaFailureClassOf(store.TransitionOperationPhase(MakeTransition(id, 0))),
      MetaFailureClass::kDomainReject);
  EXPECT_EQ(MetaFailureClassOf(store.CompleteOperation(MakeComplete(id, 0))),
            MetaFailureClass::kDomainReject);
  AbortOperation abort;
  abort.operation_id_ = id;
  EXPECT_EQ(MetaFailureClassOf(store.AbortOperation(abort)),
            MetaFailureClass::kDomainReject);

  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  ASSERT_TRUE(store.CompleteOperation(MakeComplete(id, 0)).ok());
  // Terminal states are irreversible: every further mutation rejects.
  EXPECT_EQ(MetaFailureClassOf(
                store.TransitionOperationPhase(MakeTransition(id, 1, "late"))),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(MetaFailureClassOf(store.CompleteOperation(MakeComplete(id, 1))),
            MetaFailureClass::kDomainReject);
}

TEST(MetaOperationStore, CompleteIsTerminalAndReplayIdempotent) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  ASSERT_TRUE(store.TransitionOperationPhase(MakeTransition(id, 0)).ok());
  // CAS conflict.
  EXPECT_EQ(MetaFailureClassOf(store.CompleteOperation(MakeComplete(id, 0))),
            MetaFailureClass::kDomainReject);
  const CompleteOperation complete = MakeComplete(id, 1, "done",
                                                  /*data_loss=*/true);
  ASSERT_TRUE(store.CompleteOperation(complete).ok());
  const auto record = store.FindOperation(id);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->lifecycle_, MetaOperationLifecycle::kCompleted);
  EXPECT_EQ(record->revision_, 2);
  EXPECT_EQ(record->terminal_result_, "done");
  EXPECT_TRUE(record->data_loss_possible_);
  EXPECT_EQ(store.ActiveCount(), 0);  // terminal ops are not active
  EXPECT_FALSE(store.HasActiveKind("failover"));
  // Replay: identical content already installed — no-op accept.
  ASSERT_TRUE(store.CompleteOperation(complete).ok());
  // Same revision, different content: conflict reject.
  EXPECT_EQ(
      MetaFailureClassOf(store.CompleteOperation(MakeComplete(id, 1, "other"))),
      MetaFailureClass::kDomainReject);
}

TEST(MetaOperationStore, AbortIsTerminalAndReplayIdempotent) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  // Abort directly from Submitted is a legal step of the generic machine.
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  AbortOperation abort;
  abort.operation_id_ = id;
  abort.expected_revision_ = 0;
  abort.reason_ = "operator canceled";
  ASSERT_TRUE(store.AbortOperation(abort).ok());
  const auto record = store.FindOperation(id);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->lifecycle_, MetaOperationLifecycle::kAborted);
  EXPECT_EQ(record->terminal_result_, "operator canceled");
  EXPECT_FALSE(record->data_loss_possible_);
  ASSERT_TRUE(store.AbortOperation(abort).ok());  // replay no-op
  // Terminal irreversibility also holds after an abort.
  EXPECT_EQ(MetaFailureClassOf(store.CompleteOperation(MakeComplete(id, 1))),
            MetaFailureClass::kDomainReject);
}

ArchiveOperations MakeArchive(std::vector<std::uint64_t> seqs) {
  ArchiveOperations cmd;
  cmd.operation_seqs_ = std::move(seqs);
  return cmd;
}

TEST(MetaOperationStore, ArchiveMovesTerminalOpsToTombstonesNonContiguously) {
  MetaOperationStore store;
  const MetaOperationId id_a = MakeOperationId(1);
  const MetaOperationId id_b = MakeOperationId(2);
  const MetaOperationId id_c = MakeOperationId(3);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_a, 1), 100).ok());
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_b, 2), 101).ok());
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_c, 3), 102).ok());
  ASSERT_TRUE(store
                  .CompleteOperation(MakeComplete(id_a, 0, "done-a",
                                                  /*data_loss=*/true))
                  .ok());
  ASSERT_TRUE(store.TransitionOperationPhase(MakeTransition(id_b, 0)).ok());
  ASSERT_TRUE(store
                  .AbortOperation([&] {
                    AbortOperation abort;
                    abort.operation_id_ = id_c;
                    abort.expected_revision_ = 0;
                    abort.reason_ = "canceled";
                    return abort;
                  }())
                  .ok());

  // Non-contiguous archival: the Running operation at seq 101 is skipped, so
  // a long-Running operation never blocks archival.
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({100, 102})).ok());
  EXPECT_EQ(store.LiveCount(), 1);
  EXPECT_EQ(store.ArchivedCount(), 2);
  EXPECT_TRUE(store.FindOperation(id_b).has_value());  // still live

  // The tombstone keeps the identity and outcome fields.
  EXPECT_FALSE(store.FindOperation(id_a).has_value());
  const auto tombstone = store.FindArchived(id_a);
  ASSERT_TRUE(tombstone.has_value());
  EXPECT_EQ(tombstone->operation_seq_, 100);
  EXPECT_EQ(tombstone->intent_hash_, MakeIntentHash(1));
  EXPECT_EQ(tombstone->actor_.principal_, "keylane://operator/alice");
  EXPECT_EQ(tombstone->terminal_lifecycle_, MetaOperationLifecycle::kCompleted);
  EXPECT_EQ(tombstone->terminal_result_, "done-a");
  EXPECT_TRUE(tombstone->data_loss_possible_);
  // Seq references resolve to "already done" through the tombstone index.
  ASSERT_TRUE(store.FindArchivedBySeq(102).has_value());
  EXPECT_EQ(store.FindArchivedBySeq(102)->terminal_lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_TRUE(store.OperationKnown(id_a));  // known via the archive
}

TEST(MetaOperationStore, ArchiveValidationIsAtomicAndReplayIdempotent) {
  MetaOperationStore store;
  const MetaOperationId id_a = MakeOperationId(1);
  const MetaOperationId id_b = MakeOperationId(2);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_a, 1), 100).ok());
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_b, 2), 101).ok());
  ASSERT_TRUE(store.CompleteOperation(MakeComplete(id_a, 0)).ok());

  // Non-terminal operations are never archivable; the whole command rejects
  // atomically (id_a stays live despite being terminal).
  EXPECT_EQ(
      MetaFailureClassOf(store.ArchiveOperations(MakeArchive({100, 101}))),
      MetaFailureClass::kDomainReject);
  EXPECT_TRUE(store.FindOperation(id_a).has_value());
  EXPECT_EQ(store.ArchivedCount(), 0);
  // References to never-submitted seqs reject.
  EXPECT_EQ(MetaFailureClassOf(store.ArchiveOperations(MakeArchive({999}))),
            MetaFailureClass::kDomainReject);

  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({100})).ok());
  // Replay: the seq already resolves to a tombstone — idempotent no-op.
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({100})).ok());
  EXPECT_EQ(store.ArchivedCount(), 1);
}

TEST(MetaOperationStore, LateDuplicateSubmitResolvesViaTombstone) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  ASSERT_TRUE(store.CompleteOperation(MakeComplete(id, 0)).ok());
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({100})).ok());

  // A retried client submission deterministically resolves as already done,
  // even at a fresh log index because the idempotency tombstone is retained.
  const auto dup = store.SubmitOperation(MakeSubmit(id, 42), 200);
  ASSERT_TRUE(dup.ok()) << dup.status();
  EXPECT_FALSE(dup->created_);
  EXPECT_TRUE(dup->archived_);
  EXPECT_EQ(store.LiveCount(), 0);
  EXPECT_EQ(store.ArchivedCount(), 1);
  // Payload reuse with the same id still rejects against the tombstone.
  EXPECT_EQ(MetaFailureClassOf(
                store.SubmitOperation(MakeSubmit(id, 43), 201).status()),
            MetaFailureClass::kDomainReject);
  // Mutations against an archived operation reject (terminal, irreversible).
  EXPECT_EQ(
      MetaFailureClassOf(store.TransitionOperationPhase(MakeTransition(id, 1))),
      MetaFailureClass::kDomainReject);
}

TEST(MetaOperationStore, MaxActiveOperationsEnforced) {
  MetaOperationStore store(/*max_active=*/2);
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(1), 1), 100).ok());
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(2), 2), 101).ok());
  // The third concurrent operation exceeds the bound: fail-safe rejection.
  EXPECT_EQ(MetaFailureClassOf(
                store.SubmitOperation(MakeSubmit(MakeOperationId(3), 3), 102)
                    .status()),
            MetaFailureClass::kDomainReject);
  // An idempotent duplicate is not a new operation and stays accepted.
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(1), 1), 200).ok());
  // Completing one frees its active slot.
  ASSERT_TRUE(
      store.CompleteOperation(MakeComplete(MakeOperationId(1), 0)).ok());
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(3), 3), 102).ok());
  EXPECT_EQ(store.ActiveCount(), 2);
}

TEST(MetaOperationStore, ArchiveSummaryCapEnforced) {
  MetaOperationStore store(/*max_active=*/4, /*max_archived=*/1);
  for (std::uint8_t i = 1; i <= 2; ++i) {
    ASSERT_TRUE(
        store.SubmitOperation(MakeSubmit(MakeOperationId(i), i), 100 + i).ok());
    ASSERT_TRUE(
        store.CompleteOperation(MakeComplete(MakeOperationId(i), 0)).ok());
  }
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({101})).ok());
  // At the cap ArchiveOperations rejects until the operator exports and
  // prunes a retained summary.
  EXPECT_EQ(MetaFailureClassOf(store.ArchiveOperations(MakeArchive({102}))),
            MetaFailureClass::kDomainReject);
  // Re-archiving the already-archived seq is still an idempotent no-op.
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({101})).ok());
}

TEST(MetaOperationStore, LiveRecordBoundFailsSafe) {
  // The live set (including terminal records awaiting archival) is bounded by
  // max_active + max_archived, keeping all store state bounded; the escape
  // valve is ArchiveOperations.
  MetaOperationStore store(/*max_active=*/2, /*max_archived=*/1);
  for (std::uint8_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(
        store.SubmitOperation(MakeSubmit(MakeOperationId(i), i), 100 + i).ok());
    ASSERT_TRUE(
        store.CompleteOperation(MakeComplete(MakeOperationId(i), 0)).ok());
  }
  EXPECT_EQ(store.LiveCount(), 3);
  // Active slots are free, yet the live-set bound rejects a new submit until
  // terminal records are archived.
  EXPECT_EQ(MetaFailureClassOf(
                store.SubmitOperation(MakeSubmit(MakeOperationId(4), 4), 104)
                    .status()),
            MetaFailureClass::kDomainReject);
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({101})).ok());
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(4), 4), 104).ok());
}

TEST(MetaOperationStore, PerRecordEvidenceCapEnforced) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  TransitionOperationPhase transition;
  transition.operation_id_ = id;
  transition.expected_revision_ = 0;
  transition.evidence_.assign(
      keylane::meta::kMaxMetaOperationEvidencePerRecord + 1,
      MakeEvidence(std::string(40, 'a'), 3));
  EXPECT_EQ(MetaFailureClassOf(store.TransitionOperationPhase(transition)),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.FindOperation(id)->lifecycle_,
            MetaOperationLifecycle::kSubmitted);
}

TEST(MetaOperationStore, ReusedOrZeroSeqFailsStop) {
  MetaOperationStore store;
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(1), 1), 100).ok());
  // seq is the submit command's own raft log index: zero or a collision with
  // another operation's seq means the apply wiring broke — fail-stop.
  EXPECT_DEATH(
      store.SubmitOperation(MakeSubmit(MakeOperationId(2), 2), 0).IgnoreError(),
      "");
  EXPECT_DEATH(store.SubmitOperation(MakeSubmit(MakeOperationId(2), 2), 100)
                   .IgnoreError(),
               "");
}

TEST(MetaOperationStore, ExportArchiveDrainsSummaries) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  ASSERT_TRUE(store.CompleteOperation(MakeComplete(id, 0, "done")).ok());
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({100})).ok());
  const auto bytes = store.ExportArchive();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const auto decoded = keylane::meta::DecodeMetaOperationArchiveExport(*bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_EQ(decoded->summaries_.size(), 1);
  EXPECT_EQ(decoded->summaries_[0], *store.FindArchived(id));
}

TEST(MetaOperationStore, SerializationRoundTripPreservesJournal) {
  MetaOperationStore store;
  const MetaOperationId id_live = MakeOperationId(1);
  const MetaOperationId id_done = MakeOperationId(2);
  const MetaOperationId id_archived = MakeOperationId(3);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_live, 1), 100).ok());
  ASSERT_TRUE(store.TransitionOperationPhase(MakeTransition(id_live, 0)).ok());
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_done, 2), 101).ok());
  ASSERT_TRUE(store.CompleteOperation(MakeComplete(id_done, 0, "done")).ok());
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id_archived, 3), 102).ok());
  ASSERT_TRUE(
      store.CompleteOperation(MakeComplete(id_archived, 0, "arch")).ok());
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({102})).ok());

  const auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  auto restored = MetaOperationStore::Deserialize(*bytes);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->FindOperation(id_live), store.FindOperation(id_live));
  ASSERT_EQ(restored->FindOperation(id_live)->evidence_.size(), 1u);
  EXPECT_EQ(restored->FindOperation(id_live)->evidence_.front().group_id_,
            "group-a");
  EXPECT_EQ(restored->FindOperation(id_live)->evidence_.front().assignment_id_,
            MakeEvidence(std::string(40, 'a'), 3, id_live).assignment_id_);
  EXPECT_EQ(restored->FindOperation(id_done), store.FindOperation(id_done));
  EXPECT_EQ(restored->FindArchived(id_archived),
            store.FindArchived(id_archived));
  EXPECT_EQ(restored->ActiveCount(), store.ActiveCount());
  // Behavior continues identically: replay no-ops and new work both line up.
  ASSERT_TRUE(
      restored->TransitionOperationPhase(MakeTransition(id_live, 0)).ok());
  ASSERT_TRUE(
      store.TransitionOperationPhase(MakeTransition(id_live, 1, "phase-2"))
          .ok());
  ASSERT_TRUE(
      restored->TransitionOperationPhase(MakeTransition(id_live, 1, "phase-2"))
          .ok());
  EXPECT_EQ(restored->FindOperation(id_live), store.FindOperation(id_live));
  ASSERT_TRUE(
      restored->SubmitOperation(MakeSubmit(MakeOperationId(4), 4), 200).ok());
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(4), 4), 200).ok());
  EXPECT_EQ(restored->FindOperationBySeq(200), store.FindOperationBySeq(200));
}

TEST(MetaOperationStore, DeserializeRejectsCorruption) {
  MetaOperationStore store;
  ASSERT_TRUE(
      store.SubmitOperation(MakeSubmit(MakeOperationId(1), 1), 100).ok());
  const auto bytes = store.Serialize();
  ASSERT_TRUE(bytes.ok());
  EXPECT_EQ(MetaFailureClassOf(MetaOperationStore::Deserialize("").status()),
            MetaFailureClass::kFailStop);
  const std::string truncated = bytes->substr(0, bytes->size() - 1);
  EXPECT_EQ(
      MetaFailureClassOf(MetaOperationStore::Deserialize(truncated).status()),
      MetaFailureClass::kFailStop);
  const std::string trailing = *bytes + '\x00';
  EXPECT_EQ(
      MetaFailureClassOf(MetaOperationStore::Deserialize(trailing).status()),
      MetaFailureClass::kFailStop);
}

}  // namespace
