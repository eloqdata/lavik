// MetaObservationStore tests.
//
// The store is volatile and leader-local: observations never enter the Raft
// log. These tests drive only the public surface against a fake
// MetaCommittedFacts and cover the freshness matrix: unregistered
// nodes, stale/future session generations, boot mismatch, old/future/exact
// group terms, manifest mismatch, unbound history, unknown/terminal
// operations, generation-adoption purge, commit-driven revalidation with
// read-path re-filtering, TTL expiry, capacity bounds, and the audit ring.

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "meta/meta_encoding.h"
#include "meta/meta_observation_store.h"

namespace {

using keylane::meta::MetaBootIncarnation;
using keylane::meta::MetaCandidateProgressObs;
using keylane::meta::MetaCommittedFacts;
using keylane::meta::MetaFailureClass;
using keylane::meta::MetaFailureClassOf;
using keylane::meta::MetaNodeBootObs;
using keylane::meta::MetaNodeHealthObs;
using keylane::meta::MetaObsAuditEvent;
using keylane::meta::MetaObsAuditKind;
using keylane::meta::MetaObservation;
using keylane::meta::MetaObservationIdentity;
using keylane::meta::MetaObservationStore;
using keylane::meta::MetaOperationEvidenceObs;
using keylane::meta::MetaOperationId;

// Scripted committed state: the conservative-answer contract (unknown -> 0 /
// false) is honored by the fake the same way the real projection does, so
// tests exercise the store's rejection side of every rule.
class FakeCommittedFacts : public MetaCommittedFacts {
 public:
  bool IsActiveNode(std::string_view node_id) const override {
    return active_nodes_.contains(std::string(node_id));
  }
  std::uint64_t CurrentGroupTerm(std::string_view group_id) const override {
    const auto it = group_terms_.find(std::string(group_id));
    return it != group_terms_.end() ? it->second : 0;
  }
  std::uint64_t CurrentPopulationManifestId(
      std::string_view group_id) const override {
    const auto it = group_manifests_.find(std::string(group_id));
    return it != group_manifests_.end() ? it->second : 0;
  }
  bool OperationNonTerminal(const MetaOperationId& id) const override {
    return nonterminal_ops_.contains(id);
  }
  bool HistoryBoundToOperation(const MetaOperationId& id,
                               std::uint64_t history_id) const override {
    const auto it = bound_histories_.find(id);
    return it != bound_histories_.end() && it->second.contains(history_id);
  }

  std::set<std::string> active_nodes_;
  std::map<std::string, std::uint64_t> group_terms_;
  std::map<std::string, std::uint64_t> group_manifests_;
  std::set<MetaOperationId> nonterminal_ops_;
  std::map<MetaOperationId, std::set<std::uint64_t>> bound_histories_;
};

MetaBootIncarnation Boot(std::uint8_t tag) {
  MetaBootIncarnation boot{};
  boot.fill(tag);
  return boot;
}

MetaOperationId OpId(std::uint8_t tag) {
  MetaOperationId id{};
  id.fill(tag);
  return id;
}

MetaObservationIdentity Ident(std::string node_id, std::uint8_t boot_tag,
                              std::uint64_t generation) {
  MetaObservationIdentity identity;
  identity.node_id_ = std::move(node_id);
  identity.boot_incarnation_ = Boot(boot_tag);
  identity.session_generation_ = generation;
  return identity;
}

MetaObservation BootObs(MetaObservationIdentity identity) {
  MetaObservation observation;
  observation.identity_ = std::move(identity);
  observation.payload_ = MetaNodeBootObs{};
  return observation;
}

MetaObservation HealthObs(MetaObservationIdentity identity,
                          std::string health = "ok") {
  MetaObservation observation;
  observation.identity_ = std::move(identity);
  MetaNodeHealthObs payload;
  payload.health_ = std::move(health);
  observation.payload_ = std::move(payload);
  return observation;
}

MetaObservation CandidateObs(MetaObservationIdentity identity,
                             std::string group_id, std::uint64_t term,
                             std::uint64_t manifest, std::uint64_t history) {
  MetaObservation observation;
  observation.identity_ = std::move(identity);
  MetaCandidateProgressObs payload;
  payload.group_id_ = std::move(group_id);
  payload.group_term_ = term;
  payload.population_manifest_id_ = manifest;
  payload.replication_history_id_ = history;
  payload.applied_flow_vector_ = "flow";
  payload.backlog_coverage_ = "backlog";
  payload.readiness_ = "ready";
  observation.payload_ = std::move(payload);
  return observation;
}

MetaObservation EvidenceObs(MetaObservationIdentity identity,
                            MetaOperationId operation_id, std::string phase,
                            std::string group_id, std::uint64_t term,
                            std::uint64_t manifest, std::uint64_t history) {
  MetaObservation observation;
  observation.identity_ = std::move(identity);
  MetaOperationEvidenceObs payload;
  payload.operation_id_ = operation_id;
  payload.kind_phase_ = std::move(phase);
  payload.evidence_ = "evidence-bytes";
  payload.group_id_ = std::move(group_id);
  payload.group_term_ = term;
  payload.population_manifest_id_ = manifest;
  payload.replication_history_id_ = history;
  observation.payload_ = std::move(payload);
  return observation;
}

// Registers the node, a group at term/manifest, and one non-terminal
// operation with a bound history: the standard "everything fresh" backdrop.
FakeCommittedFacts MakeFreshFacts() {
  FakeCommittedFacts facts;
  facts.active_nodes_.insert("n1");
  facts.active_nodes_.insert("n2");
  facts.active_nodes_.insert("n3");
  facts.group_terms_["g1"] = 3;
  facts.group_manifests_["g1"] = 7;
  facts.nonterminal_ops_.insert(OpId(0x51));
  facts.bound_histories_[OpId(0x51)].insert(42);
  return facts;
}

void ExpectDomainReject(const absl::Status& status) {
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(status),
            MetaFailureClass::kDomainReject);
}

bool RingHas(const MetaObservationStore& store, MetaObsAuditKind kind,
             std::string_view detail_substr) {
  for (const MetaObsAuditEvent& event : store.AuditRing()) {
    if (event.kind_ == kind &&
        event.detail_.find(detail_substr) != std::string::npos) {
      return true;
    }
  }
  return false;
}

}  // namespace

// ---------------------------------------------------------------------------
// Slice A: session lifecycle, identity checks, generation adoption purge.
// ---------------------------------------------------------------------------

TEST(MetaObservationStore, AdoptThenIngestBootAndHealth) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();

  ASSERT_TRUE(
      store.AdoptSession(Ident("n1", 0x0a, 1), /*now_unix_ms=*/1000).ok());
  EXPECT_EQ(store.CurrentGeneration("n1"), std::optional<std::uint64_t>(1));
  EXPECT_FALSE(store.CurrentGeneration("n2").has_value());

  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1000).ok());
  ASSERT_TRUE(
      store.Ingest(HealthObs(Ident("n1", 0x0a, 1), "degraded"), facts, 1001)
          .ok());
  EXPECT_EQ(store.size(), 2);

  const auto latest = store.LatestForNode("n1", facts);
  ASSERT_TRUE(latest.has_value());
  // The health report is the newest (received at 1001 > 1000).
  EXPECT_EQ(latest->received_unix_ms_, 1001);
  ASSERT_TRUE(std::holds_alternative<MetaNodeHealthObs>(latest->payload_));
  EXPECT_EQ(std::get<MetaNodeHealthObs>(latest->payload_).health_, "degraded");
}

TEST(MetaObservationStore, AdoptSessionRejectsEqualOrOlderGeneration) {
  MetaObservationStore store;
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 2), 1000).ok());

  ExpectDomainReject(store.AdoptSession(Ident("n1", 0x0a, 2), 1001));
  ExpectDomainReject(store.AdoptSession(Ident("n1", 0x0b, 1), 1002));
  EXPECT_EQ(store.CurrentGeneration("n1"), std::optional<std::uint64_t>(2));
  // A rejected adoption never purges: nothing was dropped (nothing stored),
  // but the rejections are audited.
  EXPECT_TRUE(
      RingHas(store, MetaObsAuditKind::kRejected, "stale-session-generation"));
}

TEST(MetaObservationStore, IngestRejectsUnregisteredNode) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();  // "ghost" is not registered

  ASSERT_TRUE(store.AdoptSession(Ident("ghost", 0x0a, 1), 1000).ok());
  ExpectDomainReject(
      store.Ingest(BootObs(Ident("ghost", 0x0a, 1)), facts, 1000));
  EXPECT_EQ(store.size(), 0);
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "node"));
}

TEST(MetaObservationStore, IngestRejectsStaleAndFutureGeneration) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 2), 1000).ok());

  // Older than the adopted generation.
  ExpectDomainReject(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1000));
  // Newer than any adopted generation: forged, never pre-admitted.
  ExpectDomainReject(store.Ingest(BootObs(Ident("n1", 0x0a, 3)), facts, 1000));
  EXPECT_EQ(store.size(), 0);
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "generation"));
}

TEST(MetaObservationStore, IngestRejectsBootMismatchAndMissingSession) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();

  // No session adopted at all.
  ExpectDomainReject(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1000));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "no-session"));

  // boot_incarnation is bound to the session at adoption time; a different
  // boot under the current generation is an identity violation.
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());
  ExpectDomainReject(store.Ingest(BootObs(Ident("n1", 0x0b, 1)), facts, 1000));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "boot-mismatch"));
  EXPECT_EQ(store.size(), 0);
}

TEST(MetaObservationStore, NewGenerationAtomicallyPurgesOldObservations) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  // One observation of every kind, all fresh at ingest.
  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1000).ok());
  ASSERT_TRUE(store.Ingest(HealthObs(Ident("n1", 0x0a, 1)), facts, 1001).ok());
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1002)
                  .ok());
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1003)
                  .ok());
  ASSERT_EQ(store.size(), 4);

  // Adopting generation 2 atomically drops every generation-1 observation.
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0b, 2), 2000).ok());
  EXPECT_EQ(store.size(), 0);
  EXPECT_FALSE(store.LatestForNode("n1", facts).has_value());
  EXPECT_TRUE(store.CandidateProgressFor("g1", facts).empty());
  EXPECT_TRUE(store.EvidenceForOperation(OpId(0x51), facts).empty());
  // Every purged entry is audited.
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kStalePurged,
                      "superseded-by-generation"));

  // The old generation stays rejected; the new one ingests.
  ExpectDomainReject(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 2001));
  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0b, 2)), facts, 2001).ok());
  EXPECT_EQ(store.size(), 1);
}

TEST(MetaObservationStore, LeadershipChangeDropsSessionsAndAllSoftState) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 7), 1000).ok());
  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0a, 7)), facts, 1000).ok());
  ASSERT_TRUE(store.Ingest(HealthObs(Ident("n1", 0x0a, 7)), facts, 1001).ok());
  ASSERT_EQ(store.size(), 2);

  store.ResetForLeadershipChange();

  EXPECT_EQ(store.size(), 0);
  EXPECT_FALSE(store.CurrentGeneration("n1").has_value());
  EXPECT_TRUE(store.AuditRing().empty());
  // A session from the previous leader epoch cannot continue writing even if
  // its generation number was high; the newly elected leader must
  // authenticate and adopt a fresh session first.
  ExpectDomainReject(store.Ingest(BootObs(Ident("n1", 0x0a, 7)), facts, 2000));
}

// ---------------------------------------------------------------------------
// Slice B: candidate/evidence freshness matrix and the read paths.
// ---------------------------------------------------------------------------

TEST(MetaObservationStore, CandidateRequiresCurrentTerm) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();  // g1 committed at term 3
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  // Older than committed: stale.
  ExpectDomainReject(store.Ingest(
      CandidateObs(Ident("n1", 0x0a, 1), "g1", 2, 7, 42), facts, 1000));
  // Newer than committed: forged.
  ExpectDomainReject(store.Ingest(
      CandidateObs(Ident("n1", 0x0a, 1), "g1", 4, 7, 42), facts, 1000));
  // Term 0 never anchors a term-bound observation: a group's term begins at
  // 1 via BeginGroupTerm, and an unknown group also reports 0 — admitting
  // term 0 would let observations reference nonexistent groups.
  ExpectDomainReject(store.Ingest(
      CandidateObs(Ident("n1", 0x0a, 1), "g1", 0, 7, 42), facts, 1000));
  ExpectDomainReject(store.Ingest(
      CandidateObs(Ident("n1", 0x0a, 1), "ghost-group", 0, 0, 0), facts, 1000));
  // Exactly the committed term: accepted.
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  EXPECT_EQ(store.size(), 1);
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "term-mismatch"));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "term-not-begun"));
}

TEST(MetaObservationStore, CandidateRequiresManifestMatch) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();  // g1 committed manifest 7
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  ExpectDomainReject(store.Ingest(
      CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 6, 42), facts, 1000));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "manifest-mismatch"));
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
}

TEST(MetaObservationStore, CandidateLatestWinsPerNodeAndGroup) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());
  ASSERT_TRUE(store.AdoptSession(Ident("n2", 0x0a, 1), 1000).ok());

  MetaObservation first = CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42);
  std::get<MetaCandidateProgressObs>(first.payload_).readiness_ = "warm";
  ASSERT_TRUE(store.Ingest(first, facts, 1000).ok());
  MetaObservation second = CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42);
  std::get<MetaCandidateProgressObs>(second.payload_).readiness_ = "hot";
  ASSERT_TRUE(store.Ingest(second, facts, 1001).ok());
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n2", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 999)
                  .ok());

  // Latest-wins per (node, group): n1's newer report replaced the older one.
  EXPECT_EQ(store.size(), 2);
  const auto for_group = store.CandidateProgressFor("g1", facts);
  ASSERT_EQ(for_group.size(), 2);
  // Node-sorted order is deterministic.
  EXPECT_EQ(for_group[0].readiness_, "hot");
  const auto latest = store.LatestCandidateProgress("g1", facts);
  ASSERT_TRUE(latest.has_value());
  EXPECT_EQ(latest->readiness_, "hot");  // received at 1001 > n2's 999
  EXPECT_EQ(for_group[1].readiness_, "ready");
  // Unknown group reads empty.
  EXPECT_TRUE(store.CandidateProgressFor("ghost", facts).empty());
  EXPECT_FALSE(store.LatestCandidateProgress("ghost", facts).has_value());
}

TEST(MetaObservationStore, CandidateSetIsBoundedPerGroup) {
  MetaObservationStore::Limits limits;
  limits.max_candidates_per_group_ = 2;
  MetaObservationStore store(limits);
  FakeCommittedFacts facts = MakeFreshFacts();
  for (const char* node : {"n1", "n2", "n3"}) {
    ASSERT_TRUE(store.AdoptSession(Ident(node, 0x0a, 1), 1000).ok());
  }
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n2", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  // A third distinct node overflows the bounded set: rejected, not squeezed
  // in, rejected without truncation, and audited.
  ExpectDomainReject(store.Ingest(
      CandidateObs(Ident("n3", 0x0a, 1), "g1", 3, 7, 42), facts, 1000));
  EXPECT_TRUE(
      RingHas(store, MetaObsAuditKind::kRejected, "candidate-set-full"));
  EXPECT_EQ(store.CandidateProgressFor("g1", facts).size(), 2);
  // Refreshing an existing member never counts against the bound.
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1001)
                  .ok());
  EXPECT_EQ(store.size(), 2);
}

TEST(MetaObservationStore, EvidenceRequiresKnownNonTerminalOperation) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();  // OpId(0x51) non-terminal
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  // Unknown operation id.
  ExpectDomainReject(store.Ingest(
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x77), "p1", "g1", 3, 7, 42),
      facts, 1000));
  // Known but terminal (the fake only reports non-terminal ids as live).
  facts.nonterminal_ops_.insert(OpId(0x52));
  facts.bound_histories_[OpId(0x52)].insert(42);
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x52), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  facts.nonterminal_ops_.erase(OpId(0x52));  // committed terminal transition
  ExpectDomainReject(store.Ingest(
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x52), "p1", "g1", 3, 7, 42),
      facts, 1001));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected,
                      "operation-unknown-or-terminal"));
}

TEST(MetaObservationStore, EvidenceRequiresBoundHistory) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();  // OpId(0x51) binds history 42
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  // A history the operation journal never committed a binding for.
  ExpectDomainReject(store.Ingest(
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1", "g1", 3, 7, 43),
      facts, 1000));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "history-not-bound"));
  // The committed binding admits the evidence.
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
}

TEST(MetaObservationStore, EvidenceRequiresCurrentTermAndManifest) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  ExpectDomainReject(store.Ingest(
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1", "g1", 2, 7, 42),
      facts, 1000));
  ExpectDomainReject(store.Ingest(
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1", "g1", 4, 7, 42),
      facts, 1000));
  ExpectDomainReject(store.Ingest(
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1", "g1", 3, 8, 42),
      facts, 1000));
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
}

TEST(MetaObservationStore, EvidenceLatestWinsPerNodeAndPhase) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());
  ASSERT_TRUE(store.AdoptSession(Ident("n2", 0x0a, 1), 1000).ok());

  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p2",
                                      "g1", 3, 7, 42),
                          facts, 1001)
                  .ok());
  MetaObservation replaced =
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1", "g1", 3, 7, 42);
  std::get<MetaOperationEvidenceObs>(replaced.payload_).evidence_ = "v2";
  ASSERT_TRUE(store.Ingest(replaced, facts, 1002).ok());
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n2", 0x0a, 1), OpId(0x51), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1003)
                  .ok());

  // (n1,p1) was replaced, (n1,p2) and (n2,p1) coexist: 3 entries.
  const auto evidence = store.EvidenceForOperation(OpId(0x51), facts);
  ASSERT_EQ(evidence.size(), 3);
  // (node, phase)-sorted: (n1,p1) first, carrying the replacement payload.
  EXPECT_EQ(evidence[0].kind_phase_, "p1");
  EXPECT_EQ(evidence[0].evidence_, "v2");
  EXPECT_EQ(store.EvidenceForOperation(OpId(0x77), facts).size(), 0);
  EXPECT_EQ(store.size(), 3);
}

TEST(MetaObservationStore, LatestForNodePicksNewestAcrossKinds) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1005).ok());
  ASSERT_TRUE(store.Ingest(HealthObs(Ident("n1", 0x0a, 1)), facts, 1003).ok());

  const auto latest = store.LatestForNode("n1", facts);
  ASSERT_TRUE(latest.has_value());
  EXPECT_EQ(latest->received_unix_ms_, 1005);
  EXPECT_TRUE(std::holds_alternative<MetaNodeBootObs>(latest->payload_));
  EXPECT_FALSE(store.LatestForNode("ghost", facts).has_value());
}

// ---------------------------------------------------------------------------
// Slice C: commit-driven revalidation, read re-filtering, TTL, total
// capacity, and the audit ring.
// ---------------------------------------------------------------------------

TEST(MetaObservationStore, RevalidateAllPurgesCommitStaleObservations) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());
  ASSERT_TRUE(store.AdoptSession(Ident("n2", 0x0a, 1), 1000).ok());

  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1000).ok());
  ASSERT_TRUE(store.Ingest(HealthObs(Ident("n2", 0x0a, 1)), facts, 1000).ok());
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  ASSERT_EQ(store.size(), 4);

  // Committed state moves: the term is promoted, the operation completes,
  // and n2 is retired. Boot/health are not term-bound; n1's boot survives.
  facts.group_terms_["g1"] = 4;
  facts.nonterminal_ops_.erase(OpId(0x51));
  facts.active_nodes_.erase("n2");

  store.RevalidateAll(facts, 2000);
  EXPECT_EQ(store.size(), 1);  // only n1's boot survives
  const auto latest = store.LatestForNode("n1", facts);
  ASSERT_TRUE(latest.has_value());
  EXPECT_TRUE(std::holds_alternative<MetaNodeBootObs>(latest->payload_));
  EXPECT_FALSE(store.LatestForNode("n2", facts).has_value());
  EXPECT_TRUE(store.CandidateProgressFor("g1", facts).empty());
  EXPECT_TRUE(store.EvidenceForOperation(OpId(0x51), facts).empty());
  // Every drop is audited with the failed rule in the detail.
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kStalePurged,
                      "commit-stale:term-mismatch"));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kStalePurged,
                      "commit-stale:operation-unknown-or-terminal"));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kStalePurged,
                      "commit-stale:node-not-active"));
}

TEST(MetaObservationStore, ReadPathsRefilterEvenWithoutRevalidate) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  ASSERT_TRUE(store
                  .Ingest(EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1",
                                      "g1", 3, 7, 42),
                          facts, 1001)
                  .ok());
  ASSERT_EQ(store.size(), 2);

  // A commit lands between ingest and query and NO RevalidateAll ran: the
  // entries are still stored, but every read path must re-filter them out.
  facts.group_terms_["g1"] = 4;
  facts.nonterminal_ops_.erase(OpId(0x51));
  EXPECT_EQ(store.size(), 2);  // not actively purged yet
  EXPECT_TRUE(store.CandidateProgressFor("g1", facts).empty());
  EXPECT_FALSE(store.LatestCandidateProgress("g1", facts).has_value());
  EXPECT_FALSE(store.LatestForNode("n1", facts).has_value());
  EXPECT_TRUE(store.EvidenceForOperation(OpId(0x51), facts).empty());

  // The subsequent commit-driven purge reclaims them and audits the drops.
  store.RevalidateAll(facts, 2000);
  EXPECT_EQ(store.size(), 0);
}

TEST(MetaObservationStore, SweepExpiredDropsEntriesOlderThanTtl) {
  MetaObservationStore::Limits limits;
  limits.ttl_ms_ = 1000;
  MetaObservationStore store(limits);
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 0).ok());
  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1000).ok());
  ASSERT_TRUE(store.Ingest(HealthObs(Ident("n1", 0x0a, 1)), facts, 1500).ok());

  store.SweepExpired(2000);
  EXPECT_EQ(store.size(), 2);  // ages 1000 and 500: boundary survives
  store.SweepExpired(2001);
  EXPECT_EQ(store.size(), 1);  // the boot (age 1001 > ttl) is gone
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kTtlExpired, "ttl-expired"));
  const auto latest = store.LatestForNode("n1", facts);
  ASSERT_TRUE(latest.has_value());
  EXPECT_TRUE(std::holds_alternative<MetaNodeHealthObs>(latest->payload_));
}

TEST(MetaObservationStore, TotalCapacityRejectsNewKeys) {
  MetaObservationStore::Limits limits;
  limits.max_observations_total_ = 3;
  limits.max_candidates_per_group_ = 16;  // isolate the total cap
  MetaObservationStore store(limits);
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());
  ASSERT_TRUE(store.AdoptSession(Ident("n2", 0x0a, 1), 1000).ok());

  ASSERT_TRUE(store.Ingest(BootObs(Ident("n1", 0x0a, 1)), facts, 1000).ok());
  ASSERT_TRUE(store.Ingest(HealthObs(Ident("n1", 0x0a, 1)), facts, 1000).ok());
  ASSERT_TRUE(store
                  .Ingest(CandidateObs(Ident("n1", 0x0a, 1), "g1", 3, 7, 42),
                          facts, 1000)
                  .ok());
  ASSERT_EQ(store.size(), 3);

  // New keys beyond the total cap reject (fail-safe, audited) — across kinds.
  ExpectDomainReject(store.Ingest(BootObs(Ident("n2", 0x0a, 1)), facts, 1000));
  ExpectDomainReject(store.Ingest(
      EvidenceObs(Ident("n1", 0x0a, 1), OpId(0x51), "p1", "g1", 3, 7, 42),
      facts, 1000));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "store-full"));
  // Refreshing an existing key stays legal at the cap.
  ASSERT_TRUE(
      store.Ingest(HealthObs(Ident("n1", 0x0a, 1), "ok2"), facts, 1001).ok());
  EXPECT_EQ(store.size(), 3);
}

TEST(MetaObservationStore, OversizedPayloadFieldRejects) {
  MetaObservationStore store;
  FakeCommittedFacts facts = MakeFreshFacts();
  ASSERT_TRUE(store.AdoptSession(Ident("n1", 0x0a, 1), 1000).ok());

  MetaObservation observation = HealthObs(Ident("n1", 0x0a, 1));
  std::get<MetaNodeHealthObs>(observation.payload_)
      .health_.assign(keylane::meta::kMaxMetaPayloadBytes + 1, 'x');
  ExpectDomainReject(store.Ingest(observation, facts, 1000));
  EXPECT_TRUE(RingHas(store, MetaObsAuditKind::kRejected, "field-too-large"));
  EXPECT_EQ(store.size(), 0);
}

TEST(MetaObservationStore, AuditRingIsBoundedFifoOverwriteOldest) {
  MetaObservationStore::Limits limits;
  limits.audit_ring_capacity_ = 3;
  MetaObservationStore store(limits);
  FakeCommittedFacts facts = MakeFreshFacts();
  // No session adopted: every ingest rejects and audits.
  for (std::uint64_t ii = 1; ii <= 5; ++ii) {
    ExpectDomainReject(store.Ingest(BootObs(Ident("n1", 0x0a, ii)), facts,
                                    static_cast<std::int64_t>(ii)));
  }
  const std::vector<MetaObsAuditEvent> ring = store.AuditRing();
  ASSERT_EQ(ring.size(), 3);  // the oldest two events were overwritten
  // FIFO order preserved: events 3, 4, 5 survive, in arrival order.
  for (std::size_t ii = 0; ii < 3; ++ii) {
    EXPECT_EQ(ring[ii].kind_, MetaObsAuditKind::kRejected);
    EXPECT_EQ(ring[ii].node_id_, "n1");
    EXPECT_EQ(ring[ii].unix_ms_, static_cast<std::int64_t>(ii + 3));
    EXPECT_EQ(ring[ii].detail_, "no-session");
  }
}
