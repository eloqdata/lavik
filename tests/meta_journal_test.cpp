// Journal-side store tests for the issue #19 formal implementation: the audit
// store (src/meta/meta_audit_store), the term/grant store
// (src/meta/meta_grant_store), and the operation journal store
// (src/meta/meta_operation_store). See
// docs/plans/issue-19-metadata-raft-implementation.md §2.
//
// The tests exercise only the public surface: state queryable after applying
// commands, rejection behavior, idempotent replay acceptance vs conflict
// rejection, and serialization round-trips. Apply must be a deterministic pure
// function of (command, committed state): no wall-clock reads, no observation
// access, no IO.

#include <cstdint>
#include <string>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "meta/meta_audit_store.h"
#include "meta/meta_commands.h"
#include "meta/meta_encoding.h"
#include "meta/meta_grant_store.h"
#include "meta/meta_operation_store.h"

namespace {

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

TEST(MetaAuditStore, FullWindowReportsNeedsExportAndAppendFailsStop) {
  MetaAuditStore store(/*window_capacity=*/2);
  EXPECT_FALSE(store.NeedsExport());
  ASSERT_TRUE(store.Append(MakeAuditRecord(1)).ok());
  ASSERT_TRUE(store.Append(MakeAuditRecord(2)).ok());
  // The coordinator's propose layer gates privileged proposals on this signal
  // (RESOURCE_EXHAUSTED until export); the store only exposes the state.
  EXPECT_TRUE(store.NeedsExport());
  // A committed audit write cannot be refused; overflowing the cap means the
  // propose gate was bypassed: fail-stop, never a silent drop.
  EXPECT_DEATH(store.Append(MakeAuditRecord(3)), "");
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
  // so an external archive can verify continuity (plan §2 dedup key:
  // (cluster_id, raft_log_index, record_hash)).
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
// MetaGrantStore: per-group term, grant, fencing (plan §2 term/grant).
// ---------------------------------------------------------------------------

using keylane::meta::ActivateAuthority;
using keylane::meta::BeginGroupTerm;
using keylane::meta::GrantAuthority;
using keylane::meta::MetaGrantSpec;
using keylane::meta::MetaGrantStore;
using keylane::meta::RevokeGrant;

MetaGrantSpec MakeSpec(std::uint64_t lease_ms = 30000,
                       std::string policy_id = "policy/leader-lease",
                       std::uint64_t policy_version = 7) {
  MetaGrantSpec spec;
  spec.lease_duration_ms_ = lease_ms;
  spec.policy_id_ = std::move(policy_id);
  spec.policy_version_ = policy_version;
  return spec;
}

BeginGroupTerm MakeBeginTerm(std::string group_id, std::uint64_t expected,
                             std::uint64_t new_term) {
  BeginGroupTerm cmd;
  cmd.group_id_ = std::move(group_id);
  cmd.expected_term_ = expected;
  cmd.new_term_ = new_term;
  return cmd;
}

// An activate carries no new term (plan §2: term 只升一次,激活不再动 term).
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
  EXPECT_EQ(MetaFailureClassOf(store.ValidateActivate(activate)),
            MetaFailureClass::kDomainReject);
  GrantAuthority grant;
  grant.group_id_ = "g1";
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(grant)),
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
  // consistent — idempotent no-op accept (plan §2 replay rule).
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
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());
  const auto state = store.GroupState("g1");
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state->group_term_, 1);  // unchanged by activation
  EXPECT_FALSE(state->fenced_);
  ASSERT_TRUE(state->grant_.has_value());
  EXPECT_EQ(state->grant_->owner_, "node-a");
  EXPECT_EQ(state->grant_->term_, 1);
  EXPECT_EQ(state->grant_->authority_version_, 1);
  EXPECT_EQ(state->grant_->spec_, MakeSpec());
  EXPECT_EQ(state->last_authority_version_, 1);
}

TEST(MetaGrantStore, ActivateWithStaleTermRejects) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  // A candidate from the previous term can never activate.
  EXPECT_EQ(
      MetaFailureClassOf(store.ValidateActivate(MakeActivate("g1", 0, "n", 1))),
      MetaFailureClass::kDomainReject);
  // A future term equally cannot.
  EXPECT_EQ(
      MetaFailureClassOf(store.ValidateActivate(MakeActivate("g1", 2, "n", 1))),
      MetaFailureClass::kDomainReject);
  EXPECT_TRUE(store.GroupState("g1")->fenced_);
}

TEST(MetaGrantStore, ActivateReplayIdempotentAndVersionConflictRejected) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());
  // Replay of the same activation: identical content already installed —
  // idempotent no-op accept through both primitives.
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());
  EXPECT_EQ(store.GroupState("g1")->grant_->owner_, "node-a");
  // Same term, same authority version, different content: conflict reject.
  ActivateAuthority conflict = MakeActivate("g1", 1, "node-b", 1);
  EXPECT_EQ(MetaFailureClassOf(store.ValidateActivate(conflict)),
            MetaFailureClass::kDomainReject);
  // An older authority version never installs.
  EXPECT_EQ(MetaFailureClassOf(
                store.ValidateActivate(MakeActivate("g1", 1, "node-b", 0))),
            MetaFailureClass::kDomainReject);
  // A newer authority version in the same term replaces the grant (planned
  // migration commits through the same atomic point).
  const ActivateAuthority migration = MakeActivate("g1", 1, "node-b", 2);
  ASSERT_TRUE(store.ValidateActivate(migration).ok());
  ASSERT_TRUE(store.ApplyGrantPart(migration).ok());
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
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(renew)),
            MetaFailureClass::kDomainReject);

  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());
  ASSERT_TRUE(store.GrantAuthority(renew).ok());
  auto state = store.GroupState("g1");
  EXPECT_EQ(state->grant_->spec_.lease_duration_ms_, 60000);
  EXPECT_EQ(state->grant_->term_, 1);               // unchanged
  EXPECT_EQ(state->grant_->authority_version_, 1);  // unchanged
  // Replay installs the same spec again: idempotent.
  ASSERT_TRUE(store.GrantAuthority(renew).ok());
  // Owner/term/authority_version are CAS tokens; each mismatch rejects.
  GrantAuthority wrong_owner = renew;
  wrong_owner.node_id_ = "node-b";
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(wrong_owner)),
            MetaFailureClass::kDomainReject);
  GrantAuthority wrong_term = renew;
  wrong_term.term_ = 2;
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(wrong_term)),
            MetaFailureClass::kDomainReject);
  GrantAuthority wrong_version = renew;
  wrong_version.authority_version_ = 2;
  EXPECT_EQ(MetaFailureClassOf(store.GrantAuthority(wrong_version)),
            MetaFailureClass::kDomainReject);
  EXPECT_EQ(store.GroupState("g1")->grant_->spec_.lease_duration_ms_, 60000);
}

TEST(MetaGrantStore, RevokeAndFenceDropTheGrant) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());

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
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());
  // PolicyInUse feeds the RetirePolicy guard (plan §2 引用检查).
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
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());
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
  EXPECT_DEATH(store.ApplyGrantPart(MakeActivate("g1", 2, "node-a", 1)), "");
  EXPECT_DEATH(store.ApplyGrantPart(MakeActivate("g9", 1, "node-a", 1)), "");
}

TEST(MetaGrantStore, SerializationRoundTripPreservesState) {
  MetaGrantStore store;
  ASSERT_TRUE(store.AddGroup("g1").ok());
  ASSERT_TRUE(store.AddGroup("g2").ok());
  ASSERT_TRUE(store.BeginGroupTerm(MakeBeginTerm("g1", 0, 1)).ok());
  const ActivateAuthority activate = MakeActivate("g1", 1, "node-a", 1);
  ASSERT_TRUE(store.ValidateActivate(activate).ok());
  ASSERT_TRUE(store.ApplyGrantPart(activate).ok());
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
  ASSERT_TRUE(restored->ValidateActivate(activate).ok());  // replay no-op
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

// ---------------------------------------------------------------------------
// MetaOperationStore: the client-id-keyed operation journal with terminal
// tombstone archival (plan §2 Operation 标识与归档).
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
  cmd.replication_history_id_ = 22;
  cmd.actor_.principal_ = "keylane://operator/alice";
  cmd.actor_.readable_time_ = "2026-09-04T17:00:00Z";
  return cmd;
}

MetaEvidenceSummary MakeEvidence(std::string node_id, std::uint64_t term,
                                 MetaOperationId operation_id = {}) {
  MetaEvidenceSummary evidence;
  evidence.node_id_ = std::move(node_id);
  evidence.group_term_ = term;
  evidence.population_manifest_id_ = 11;
  evidence.replication_history_id_ = 22;
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
  EXPECT_EQ(record->intent_hash_, MakeIntentHash(42));
  EXPECT_EQ(record->lifecycle_, MetaOperationLifecycle::kSubmitted);
  EXPECT_EQ(record->revision_, 0);
  EXPECT_EQ(record->actor_.principal_, "keylane://operator/alice");
  // The seq index resolves the same record.
  ASSERT_TRUE(store.FindOperationBySeq(100).has_value());
  EXPECT_EQ(store.FindOperationBySeq(100)->operation_id_, id);
  EXPECT_EQ(store.ActiveCount(), 1);
  EXPECT_TRUE(store.OperationKnown(id));
  EXPECT_FALSE(store.OperationKnown(MakeOperationId(9)));
}

TEST(MetaOperationStore, DuplicateSubmitIsIdempotentOnlyForSameIntent) {
  MetaOperationStore store;
  const MetaOperationId id = MakeOperationId(1);
  ASSERT_TRUE(store.SubmitOperation(MakeSubmit(id, 42), 100).ok());
  // Same id + same intent_hash: idempotent accept returning the existing
  // record, even with a different log index (a retried client request that
  // got logged twice converges — plan §2 永久幂等键).
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

TransitionOperationPhase MakeTransition(const MetaOperationId& id,
                                        std::uint64_t expected_revision,
                                        std::string blob = "phase-1") {
  TransitionOperationPhase cmd;
  cmd.operation_id_ = id;
  cmd.expected_revision_ = expected_revision;
  cmd.kind_phase_blob_ = std::move(blob);
  cmd.evidence_.push_back(MakeEvidence("node-a", 3, id));
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
  EXPECT_EQ(record->evidence_[0], MakeEvidence("node-a", 3, id));
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
  // a long-Running operation never blocks archival (plan §2 归档去卡死).
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
  // even at a fresh log index (plan §2 永久幂等键 + 墓碑索引).
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
  // At the cap ArchiveOperations rejects until the operator exports.
  EXPECT_EQ(MetaFailureClassOf(store.ArchiveOperations(MakeArchive({102}))),
            MetaFailureClass::kDomainReject);
  // Re-archiving the already-archived seq is still an idempotent no-op.
  ASSERT_TRUE(store.ArchiveOperations(MakeArchive({101})).ok());
}

TEST(MetaOperationStore, LiveRecordBoundFailsSafe) {
  // The live set (including terminal records awaiting archival) is bounded by
  // max_active + max_archived, keeping all store state bounded (plan §2
  // 硬上限); the escape valve is ArchiveOperations.
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
      MakeEvidence("node-a", 3));
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
