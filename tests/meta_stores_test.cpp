// Store-level tests for the committed stores:
// meta_identity_store (identity/enrollment), meta_topology_store (topology),
// meta_policy_store (policy).
//
// The tests exercise only the public surface: state queryable after applying
// commands, domain rejection behavior, replay idempotency (same content
// re-applied = idempotent accept no-op; conflicting content = rejection), and
// serialization round-trips. Domain rejections must classify as
// MetaFailureClass::kDomainReject; corrupt snapshot bytes must classify as
// kFailStop.

#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "meta/meta_encoding.h"
#include "meta/meta_identity_store.h"
#include "meta/meta_policy_store.h"
#include "meta/meta_topology_store.h"

namespace {

using keylane::meta::MetaFailureClass;
using keylane::meta::MetaFailureClassOf;
using keylane::meta::MetaIdentityStore;
using keylane::meta::MetaNodeRecord;
using keylane::meta::MetaNodeRole;
using keylane::meta::MetaRequestId;
using keylane::meta::RegisterNode;

MetaRequestId MakeRequestId(std::uint8_t seed) {
  MetaRequestId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = static_cast<std::uint8_t>(seed + i);
  }
  return id;
}

// 40 lowercase hex chars, matching the data-plane node_id convention.
std::string MakeNodeId(std::uint8_t seed) {
  std::string id(40, '0');
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = "0123456789abcdef"[(seed + i) & 0xF];
  }
  return id;
}

std::string MakePrincipal(std::uint8_t seed) {
  return "keylane://node/" + MakeNodeId(seed);
}

RegisterNode MakeRegister(std::uint8_t seed) {
  RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.principal_ = MakePrincipal(seed);
  cmd.endpoints_ = {"10.0.0.1:7000", "10.0.0.1:17000"};
  cmd.capability_mask_ = 0x5;
  cmd.role_ = MetaNodeRole::kReplica;
  return cmd;
}

void ExpectDomainReject(const absl::Status& status) {
  ASSERT_FALSE(status.ok()) << "expected a domain rejection";
  EXPECT_EQ(MetaFailureClassOf(status), MetaFailureClass::kDomainReject)
      << status;
}

// ---------------------------------------------------------------------------
// RegisterNode: creates the queryable record; replay is an idempotent
// accept; conflicting content and principal rebinds are domain rejections.
// ---------------------------------------------------------------------------

TEST(MetaIdentityStore, RegisterNodeCreatesQueryableRecord) {
  MetaIdentityStore store;
  const RegisterNode cmd = MakeRegister(0x10);
  ASSERT_TRUE(store.Apply(cmd).ok());

  const auto record = store.FindNode(cmd.node_id_);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->node_id_, cmd.node_id_);
  EXPECT_EQ(record->principal_, cmd.principal_);
  EXPECT_EQ(record->endpoints_, cmd.endpoints_);
  EXPECT_EQ(record->capability_mask_, cmd.capability_mask_);
  EXPECT_EQ(record->role_, cmd.role_);
  EXPECT_EQ(record->revision_, 1u);
  EXPECT_FALSE(record->retired_);
  EXPECT_TRUE(store.IsActiveNode(cmd.node_id_));
  EXPECT_EQ(store.NodeCount(), 1u);

  const auto by_principal = store.FindNodeByPrincipal(cmd.principal_);
  ASSERT_TRUE(by_principal.has_value());
  EXPECT_EQ(by_principal->node_id_, cmd.node_id_);

  EXPECT_FALSE(store.FindNode(MakeNodeId(0x99)).has_value());
  EXPECT_FALSE(store.IsActiveNode(MakeNodeId(0x99)));
}

TEST(MetaIdentityStore, RegisterNodeReplayIsIdempotentAccept) {
  MetaIdentityStore store;
  const RegisterNode cmd = MakeRegister(0x11);
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Same content re-applied (replay of the same log index): accepted as a
  // no-op, with state and revision unchanged.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.NodeCount(), 1u);
  EXPECT_EQ(store.FindNode(cmd.node_id_)->revision_, 1u);
}

TEST(MetaIdentityStore, RegisterNodeConflictingContentRejected) {
  MetaIdentityStore store;
  const RegisterNode cmd = MakeRegister(0x12);
  ASSERT_TRUE(store.Apply(cmd).ok());
  const auto before = store.FindNode(cmd.node_id_);

  RegisterNode conflict = cmd;  // same node_id, different endpoints
  conflict.endpoints_ = {"10.0.0.2:7000"};
  ExpectDomainReject(store.Apply(conflict));

  RegisterNode conflict_role = cmd;
  conflict_role.role_ = MetaNodeRole::kPrimary;
  ExpectDomainReject(store.Apply(conflict_role));

  RegisterNode conflict_principal = cmd;
  conflict_principal.principal_ = MakePrincipal(0x77);
  ExpectDomainReject(store.Apply(conflict_principal));

  EXPECT_EQ(store.FindNode(cmd.node_id_), before);  // state unchanged
  EXPECT_EQ(store.FindNode(cmd.node_id_)->revision_, 1u);
}

TEST(MetaIdentityStore, RegisterNodePrincipalGloballyOneToOne) {
  MetaIdentityStore store;
  const RegisterNode first = MakeRegister(0x13);
  ASSERT_TRUE(store.Apply(first).ok());

  // Binding the same principal to a second node_id is rejected.
  RegisterNode second = MakeRegister(0x14);
  second.principal_ = first.principal_;
  ExpectDomainReject(store.Apply(second));

  // The binding still belongs to the first node.
  EXPECT_EQ(store.FindNodeByPrincipal(first.principal_)->node_id_,
            first.node_id_);
  EXPECT_FALSE(store.FindNode(second.node_id_).has_value());
}

TEST(MetaIdentityStore, RegisterNodeRejectsInvalidFields) {
  MetaIdentityStore store;
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.node_id_.clear();
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.principal_.clear();
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.node_id_ = std::string(keylane::meta::kMetaNodeIdBytes + 1, 'a');
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.endpoints_.resize(keylane::meta::kMaxMetaEndpointsPerNode + 1, "e");
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.endpoints_ = {
        std::string(keylane::meta::kMaxMetaEndpointBytes + 1, 'e')};
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.principal_ =
        std::string(keylane::meta::kMaxMetaPrincipalBytes + 1, 'p');
    ExpectDomainReject(store.Apply(cmd));
  }
  EXPECT_EQ(store.NodeCount(), 0u);
}

TEST(MetaIdentityStore, RegisterNodeEnforcesNodeCap) {
  MetaIdentityStore store;
  char hex4[5];
  for (std::uint32_t i = 0; i < keylane::meta::kMaxMetaNodes; ++i) {
    RegisterNode cmd = MakeRegister(0x20);
    // Distinct node_id (within the 40-char cap) and principal per
    // registration: replace the tail with the hex counter.
    std::snprintf(hex4, sizeof(hex4), "%04x", i);
    cmd.node_id_ = MakeNodeId(0x20).substr(0, 36) + hex4;
    cmd.principal_ = "keylane://node/" + cmd.node_id_;
    ASSERT_TRUE(store.Apply(cmd).ok()) << i;
  }
  EXPECT_EQ(store.NodeCount(), keylane::meta::kMaxMetaNodes);
  // The cap counts records, not active ones: one more is rejected.
  ExpectDomainReject(store.Apply(MakeRegister(0x21)));
}

// ---------------------------------------------------------------------------
// UpdateNode: expected_revision CAS; cannot modify the principal binding
// (the schema carries no principal field because rotation is unimplemented);
// replay is an idempotent accept; retired nodes reject updates.
// ---------------------------------------------------------------------------

using keylane::meta::RetireNode;
using keylane::meta::UpdateNode;

UpdateNode MakeUpdate(std::uint8_t seed, std::uint64_t expected_revision) {
  UpdateNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.expected_revision_ = expected_revision;
  cmd.endpoints_ = {"10.0.9.9:7000"};
  cmd.capability_mask_ = 0x77;
  return cmd;
}

TEST(MetaIdentityStore, UpdateNodeAppliesWithCas) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x30)).ok());
  const UpdateNode cmd = MakeUpdate(0x30, /*expected_revision=*/1);
  ASSERT_TRUE(store.Apply(cmd).ok());

  const auto record = store.FindNode(cmd.node_id_);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->revision_, 2u);
  EXPECT_EQ(record->endpoints_, cmd.endpoints_);
  EXPECT_EQ(record->capability_mask_, cmd.capability_mask_);
  // Principal binding untouched because UpdateNode has no principal field.
  EXPECT_EQ(record->principal_, MakePrincipal(0x30));
  EXPECT_FALSE(record->retired_);
}

TEST(MetaIdentityStore, UpdateNodeCasConflictRejected) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x31)).ok());
  ExpectDomainReject(store.Apply(MakeUpdate(0x31, /*expected_revision=*/7)));
  const auto record = store.FindNode(MakeNodeId(0x31));
  EXPECT_EQ(record->revision_, 1u);  // unchanged
  EXPECT_EQ(record->capability_mask_, 0x5u);
}

TEST(MetaIdentityStore, UpdateNodeUnknownNodeRejected) {
  MetaIdentityStore store;
  ExpectDomainReject(store.Apply(MakeUpdate(0x32, /*expected_revision=*/1)));
}

TEST(MetaIdentityStore, UpdateNodeReplayIsIdempotentAccept) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x33)).ok());
  const UpdateNode cmd = MakeUpdate(0x33, /*expected_revision=*/1);
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay of the same log index: the record already sits at the revision
  // this command produces with identical content -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.FindNode(cmd.node_id_)->revision_, 2u);

  // A DIFFERENT update colliding with the consumed revision is a conflict.
  UpdateNode conflict = MakeUpdate(0x33, /*expected_revision=*/1);
  conflict.capability_mask_ = 0x11;
  ExpectDomainReject(store.Apply(conflict));
  EXPECT_EQ(store.FindNode(cmd.node_id_)->capability_mask_, 0x77u);
}

TEST(MetaIdentityStore, UpdateNodeRetiredNodeRejected) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x34)).ok());
  RetireNode retire;
  retire.request_id_ = MakeRequestId(0x34);
  retire.node_id_ = MakeNodeId(0x34);
  retire.expected_revision_ = 1;
  ASSERT_TRUE(store.Apply(retire).ok());
  ExpectDomainReject(store.Apply(MakeUpdate(0x34, /*expected_revision=*/2)));
}

// ---------------------------------------------------------------------------
// RetireNode: terminal tombstone; the principal binding is kept (never
// rebound); replay is an idempotent accept.
// ---------------------------------------------------------------------------

RetireNode MakeRetire(std::uint8_t seed, std::uint64_t expected_revision) {
  RetireNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.expected_revision_ = expected_revision;
  return cmd;
}

TEST(MetaIdentityStore, RetireNodeRetiresAndKeepsPrincipalBinding) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x40)).ok());
  ASSERT_TRUE(store.Apply(MakeRetire(0x40, /*expected_revision=*/1)).ok());

  const auto record = store.FindNode(MakeNodeId(0x40));
  ASSERT_TRUE(record.has_value());
  EXPECT_TRUE(record->retired_);
  EXPECT_EQ(record->revision_, 2u);
  EXPECT_FALSE(store.IsActiveNode(MakeNodeId(0x40)));
  EXPECT_EQ(store.NodeCount(), 1u);  // tombstone retained

  // The tombstone still holds the principal: rebinding is rejected.
  EXPECT_EQ(store.FindNodeByPrincipal(MakePrincipal(0x40))->node_id_,
            MakeNodeId(0x40));
  RegisterNode rebind = MakeRegister(0x41);
  rebind.principal_ = MakePrincipal(0x40);
  ExpectDomainReject(store.Apply(rebind));
}

TEST(MetaIdentityStore, RetireNodeReplayIsIdempotentAccept) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x42)).ok());
  const RetireNode cmd = MakeRetire(0x42, /*expected_revision=*/1);
  ASSERT_TRUE(store.Apply(cmd).ok());
  ASSERT_TRUE(store.Apply(cmd).ok());  // replay: no-op accept
  EXPECT_EQ(store.FindNode(MakeNodeId(0x42))->revision_, 2u);
}

TEST(MetaIdentityStore, RetireNodeCasConflictAndUnknownRejected) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x43)).ok());
  ExpectDomainReject(store.Apply(MakeRetire(0x43, /*expected_revision=*/9)));
  ExpectDomainReject(store.Apply(MakeRetire(0x44, /*expected_revision=*/1)));
  EXPECT_FALSE(store.FindNode(MakeNodeId(0x43))->retired_);
}

TEST(MetaIdentityStore, RetiredNodeCannotReregister) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x45)).ok());
  ASSERT_TRUE(store.Apply(MakeRetire(0x45, /*expected_revision=*/1)).ok());
  // The tombstone conflicts with any re-registration, even identical content.
  ExpectDomainReject(store.Apply(MakeRegister(0x45)));
}

// ---------------------------------------------------------------------------
// Serialization: u16 schema_version envelope + sorted records; deterministic
// bytes; strict fail-stop decode.
// ---------------------------------------------------------------------------

void ExpectStoreFailStop(const absl::Status& status) {
  ASSERT_FALSE(status.ok()) << "expected a fail-stop decode failure";
  EXPECT_EQ(MetaFailureClassOf(status), MetaFailureClass::kFailStop) << status;
}

TEST(MetaIdentityStore, SerializationRoundTrip) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x50)).ok());
  ASSERT_TRUE(store.Apply(MakeRegister(0x51)).ok());
  ASSERT_TRUE(store.Apply(MakeUpdate(0x51, /*expected_revision=*/1)).ok());
  ASSERT_TRUE(store.Apply(MakeRegister(0x52)).ok());
  ASSERT_TRUE(store.Apply(MakeRetire(0x52, /*expected_revision=*/1)).ok());

  const std::string bytes = store.Serialize();
  // Same u16 schema_version envelope convention as the command codec.
  ASSERT_GE(bytes.size(), 2u);
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
  EXPECT_EQ(static_cast<std::uint16_t>(p[0] | (p[1] << 8)),
            keylane::meta::kMetaFormatVersion);

  const auto loaded = MetaIdentityStore::Deserialize(bytes);
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  EXPECT_EQ(loaded->NodeCount(), 3u);
  EXPECT_EQ(loaded->FindNode(MakeNodeId(0x50)),
            store.FindNode(MakeNodeId(0x50)));
  EXPECT_EQ(loaded->FindNode(MakeNodeId(0x51)),
            store.FindNode(MakeNodeId(0x51)));
  EXPECT_EQ(loaded->FindNode(MakeNodeId(0x52)),
            store.FindNode(MakeNodeId(0x52)));
  EXPECT_TRUE(loaded->FindNode(MakeNodeId(0x52))->retired_);
  EXPECT_EQ(loaded->FindNodeByPrincipal(MakePrincipal(0x52))->node_id_,
            MakeNodeId(0x52));
  EXPECT_FALSE(loaded->IsActiveNode(MakeNodeId(0x52)));
  // A re-loaded store serializes to the same bytes (fixed point).
  EXPECT_EQ(loaded->Serialize(), bytes);
}

TEST(MetaIdentityStore, EmptyStoreRoundTrip) {
  const MetaIdentityStore store;
  const auto loaded = MetaIdentityStore::Deserialize(store.Serialize());
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  EXPECT_EQ(loaded->NodeCount(), 0u);
}

TEST(MetaIdentityStore, SerializationIsDeterministic) {
  // Equal states serialize to equal bytes regardless of apply order
  // (sorted registry), which is what snapshot/install convergence needs.
  MetaIdentityStore a;
  MetaIdentityStore b;
  ASSERT_TRUE(a.Apply(MakeRegister(0x60)).ok());
  ASSERT_TRUE(a.Apply(MakeRegister(0x61)).ok());
  ASSERT_TRUE(b.Apply(MakeRegister(0x61)).ok());
  ASSERT_TRUE(b.Apply(MakeRegister(0x60)).ok());
  EXPECT_EQ(a.Serialize(), b.Serialize());
}

TEST(MetaIdentityStore, DeserializeRejectsCorruption) {
  MetaIdentityStore store;
  ASSERT_TRUE(store.Apply(MakeRegister(0x70)).ok());
  const std::string bytes = store.Serialize();

  for (std::size_t len = 0; len < bytes.size(); ++len) {
    SCOPED_TRACE("len=" + std::to_string(len));
    ExpectStoreFailStop(
        MetaIdentityStore::Deserialize(std::string_view(bytes).substr(0, len))
            .status());
  }
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(bytes + '\0').status());  // trailing

  std::string bad_version = bytes;
  bad_version[0] = '\x7F';  // unknown schema_version
  ExpectStoreFailStop(MetaIdentityStore::Deserialize(bad_version).status());
}

TEST(MetaIdentityStore, DeserializeRejectsInvariantViolations) {
  // Hand-built byte sequences that decode cleanly but violate store
  // invariants must fail-stop identically on every node.
  using keylane::meta::MetaWriter;
  const std::string node_a = MakeNodeId(0x71);
  const std::string node_b = MakeNodeId(0x72);
  const std::string principal = MakePrincipal(0x71);

  auto write_record = [&](MetaWriter& w, const std::string& node_id,
                          const std::string& prin, std::uint64_t revision) {
    w.WriteString(node_id);
    w.WriteString(prin);
    w.WriteCount(0);  // endpoints
    w.WriteU64(0x5);  // capability_mask
    w.WriteU8(static_cast<std::uint8_t>(MetaNodeRole::kReplica));
    w.WriteU64(revision);
    w.WriteU8(0);  // active
  };
  auto make_blob = [&](auto write_body) {
    MetaWriter w;
    w.WriteU16(keylane::meta::kMetaFormatVersion);
    write_body(w);
    return w.buffer();
  };

  // Duplicate node_id.
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(make_blob([&](MetaWriter& w) {
        w.WriteCount(2);
        write_record(w, node_a, principal, 1);
        write_record(w, node_a, MakePrincipal(0x73), 1);
      })).status());
  // Duplicate principal across node_ids (breaks the global 1:1 binding).
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(make_blob([&](MetaWriter& w) {
        w.WriteCount(2);
        write_record(w, node_a, principal, 1);
        write_record(w, node_b, principal, 1);
      })).status());
  // Revision 0 (revisions start at 1).
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(make_blob([&](MetaWriter& w) {
        w.WriteCount(1);
        write_record(w, node_a, principal, 0);
      })).status());
  // Empty node_id / principal.
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(make_blob([&](MetaWriter& w) {
        w.WriteCount(1);
        write_record(w, "", principal, 1);
      })).status());
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(make_blob([&](MetaWriter& w) {
        w.WriteCount(1);
        write_record(w, node_a, "", 1);
      })).status());
  // Over-cap count prefix.
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(make_blob([&](MetaWriter& w) {
        w.WriteU32(keylane::meta::kMaxMetaNodes + 1);
      })).status());
  // Unknown role tag.
  ExpectStoreFailStop(
      MetaIdentityStore::Deserialize(make_blob([&](MetaWriter& w) {
        w.WriteCount(1);
        w.WriteString(node_a);
        w.WriteString(principal);
        w.WriteCount(0);
        w.WriteU64(0x5);
        w.WriteU8(9);  // not a MetaNodeRole
        w.WriteU64(1);
        w.WriteU8(0);
      })).status());
}

// ===========================================================================
// Topology store.
// ===========================================================================

using keylane::meta::CreateGroup;
using keylane::meta::MetaTopologyStore;

std::string MakeGroupId(std::uint8_t seed) {
  return "group-" + std::string(1, static_cast<char>('a' + (seed & 0xF))) +
         std::to_string(seed);
}

CreateGroup MakeCreateGroup(const std::string& group_id,
                            std::uint64_t new_topology_epoch) {
  CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x80);
  cmd.group_id_ = group_id;
  cmd.new_topology_epoch_ = new_topology_epoch;
  return cmd;
}

// ---------------------------------------------------------------------------
// CreateGroup: absolute topology_epoch (exactly current+1), pristine replay
// idempotency, group cap.
// ---------------------------------------------------------------------------

TEST(MetaTopologyStore, CreateGroupCreatesQueryableGroup) {
  MetaTopologyStore store;
  const CreateGroup cmd = MakeCreateGroup("group-a", /*new_topology_epoch=*/1);
  ASSERT_TRUE(store.Apply(cmd).ok());

  EXPECT_EQ(store.TopologyEpoch(), 1u);
  EXPECT_TRUE(store.GroupExists("group-a"));
  EXPECT_EQ(store.GroupCount(), 1u);
  const auto view = store.FindGroup("group-a");
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->group_id_, "group-a");
  EXPECT_EQ(view->revision_, 1u);
  EXPECT_EQ(view->config_epoch_, 0u);
  EXPECT_TRUE(view->members_.empty());
  // Freshly created GroupRecord: no owner, all counters zero.
  EXPECT_EQ(view->record_.owner_, "");
  EXPECT_EQ(view->record_.group_term_, 0u);
  EXPECT_EQ(view->record_.authority_version_, 0u);
  EXPECT_EQ(view->record_.population_manifest_id_, 0u);
  EXPECT_EQ(view->record_.partition_replication_epoch_, 0u);

  EXPECT_FALSE(store.FindGroup("group-b").has_value());
  EXPECT_FALSE(store.GroupExists("group-b"));
}

TEST(MetaTopologyStore, CreateGroupRequiresExactNextEpoch) {
  MetaTopologyStore store;
  // Initial epoch is 0: only exactly current+1 is accepted.
  ExpectDomainReject(store.Apply(MakeCreateGroup("group-a", 0)));
  ExpectDomainReject(store.Apply(MakeCreateGroup("group-a", 2)));
  EXPECT_EQ(store.TopologyEpoch(), 0u);
  EXPECT_EQ(store.GroupCount(), 0u);
}

TEST(MetaTopologyStore, CreateGroupReplayIsIdempotentAccept) {
  MetaTopologyStore store;
  const CreateGroup cmd = MakeCreateGroup("group-a", 1);
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay of the same log index: the pristine group exists and the epoch
  // already carries this command's value -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.GroupCount(), 1u);
  EXPECT_EQ(store.TopologyEpoch(), 1u);
}

TEST(MetaTopologyStore, CreateGroupDuplicateRejected) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  // A fresh create for the same group_id (different epoch) conflicts.
  ExpectDomainReject(store.Apply(MakeCreateGroup("group-a", 2)));
  EXPECT_EQ(store.TopologyEpoch(), 1u);  // unchanged
}

TEST(MetaTopologyStore, CreateGroupRejectsInvalidGroupId) {
  MetaTopologyStore store;
  ExpectDomainReject(store.Apply(MakeCreateGroup("", 1)));
  ExpectDomainReject(store.Apply(MakeCreateGroup(
      std::string(keylane::meta::kMaxMetaGroupIdBytes + 1, 'g'), 1)));
  EXPECT_EQ(store.GroupCount(), 0u);
  EXPECT_EQ(store.TopologyEpoch(), 0u);
}

TEST(MetaTopologyStore, CreateGroupEnforcesGroupCap) {
  MetaTopologyStore store;
  for (std::uint32_t i = 0; i < keylane::meta::kMaxMetaGroups; ++i) {
    char gid[32];
    std::snprintf(gid, sizeof(gid), "group-%04x", i);
    ASSERT_TRUE(store.Apply(MakeCreateGroup(gid, i + 1)).ok()) << i;
  }
  EXPECT_EQ(store.GroupCount(), keylane::meta::kMaxMetaGroups);
  ExpectDomainReject(store.Apply(
      MakeCreateGroup("group-over", keylane::meta::kMaxMetaGroups + 1)));
  EXPECT_EQ(store.TopologyEpoch(), keylane::meta::kMaxMetaGroups);
}

// ---------------------------------------------------------------------------
// AssignNodeToGroup / RemoveNodeFromGroup: one-node-one-group enforcement,
// group-record revision CAS, replay idempotency.
// ---------------------------------------------------------------------------

using keylane::meta::AssignNodeToGroup;
using keylane::meta::MetaGroupMember;
using keylane::meta::RemoveNodeFromGroup;

AssignNodeToGroup MakeAssign(const std::string& group_id, std::uint8_t seed,
                             MetaNodeRole role, std::uint64_t expected_revision,
                             std::uint64_t topology_epoch = 0) {
  AssignNodeToGroup cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.group_id_ = group_id;
  cmd.node_id_ = MakeNodeId(seed);
  cmd.role_ = role;
  cmd.expected_revision_ = expected_revision;
  cmd.new_topology_epoch_ =
      topology_epoch == 0 ? expected_revision + 1 : topology_epoch;
  return cmd;
}

RemoveNodeFromGroup MakeRemove(const std::string& group_id, std::uint8_t seed,
                               std::uint64_t expected_revision,
                               std::uint64_t topology_epoch = 0) {
  RemoveNodeFromGroup cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.group_id_ = group_id;
  cmd.node_id_ = MakeNodeId(seed);
  cmd.expected_revision_ = expected_revision;
  cmd.new_topology_epoch_ =
      topology_epoch == 0 ? expected_revision + 1 : topology_epoch;
  return cmd;
}

TEST(MetaTopologyStore, AssignNodeToGroupAddsMember) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary,
                                    /*expected_revision=*/1))
                  .ok());

  const auto view = store.FindGroup("group-a");
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->revision_, 2u);
  ASSERT_EQ(view->members_.size(), 1u);
  EXPECT_EQ(view->members_[0],
            (MetaGroupMember{MakeNodeId(0x10), MetaNodeRole::kPrimary}));
  EXPECT_EQ(store.FindGroupOfNode(MakeNodeId(0x10)),
            std::optional<std::string>("group-a"));
}

TEST(MetaTopologyStore, AssignNodeToGroupUnknownGroupRejected) {
  MetaTopologyStore store;
  ExpectDomainReject(
      store.Apply(MakeAssign("group-ghost", 0x10, MetaNodeRole::kPrimary, 1)));
  EXPECT_FALSE(store.FindGroupOfNode(MakeNodeId(0x10)).has_value());
}

TEST(MetaTopologyStore, AssignNodeToGroupCasConflictRejected) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ExpectDomainReject(store.Apply(
      MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary, /*expected=*/9)));
  EXPECT_TRUE(store.FindGroup("group-a")->members_.empty());
  EXPECT_EQ(store.FindGroup("group-a")->revision_, 1u);
}

TEST(MetaTopologyStore, AssignNodeToGroupReplayIsIdempotentAccept) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  const AssignNodeToGroup cmd =
      MakeAssign("group-a", 0x10, MetaNodeRole::kReplica, /*expected=*/1);
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay: member already present with the same role and the group record
  // at the revision this command produces -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.FindGroup("group-a")->revision_, 2u);
  EXPECT_EQ(store.FindGroup("group-a")->members_.size(), 1u);

  // Same member slot, different content (role) -> conflict rejection.
  ExpectDomainReject(store.Apply(
      MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary, /*expected=*/1)));
  EXPECT_EQ(store.FindGroup("group-a")->members_[0].role_,
            MetaNodeRole::kReplica);
}

TEST(MetaTopologyStore, AssignNodeToOtherGroupRejectedOneNodeOneGroup) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-b", 2)).ok());
  ASSERT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary,
                                    /*expected=*/1, /*topology_epoch=*/3))
                  .ok());
  // One-node-one-group: assigning the same node elsewhere is rejected;
  // the old membership fact stays queryable for the apply dispatcher.
  ExpectDomainReject(store.Apply(
      MakeAssign("group-b", 0x10, MetaNodeRole::kReplica, /*expected=*/1)));
  EXPECT_EQ(store.FindGroupOfNode(MakeNodeId(0x10)),
            std::optional<std::string>("group-a"));
  EXPECT_TRUE(store.FindGroup("group-b")->members_.empty());

  // A different node can join group-b just fine.
  ASSERT_TRUE(store
                  .Apply(MakeAssign("group-b", 0x11, MetaNodeRole::kReplica,
                                    /*expected=*/1, /*topology_epoch=*/4))
                  .ok());
  EXPECT_EQ(store.FindGroup("group-b")->members_.size(), 1u);
}

TEST(MetaTopologyStore, AssignNodeEnforcesMemberCap) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  char hex4[5];
  std::uint64_t revision = 1;
  for (std::uint32_t i = 0; i < keylane::meta::kMaxMetaNodes; ++i) {
    AssignNodeToGroup cmd;
    cmd.request_id_ = MakeRequestId(0x20);
    cmd.group_id_ = "group-a";
    std::snprintf(hex4, sizeof(hex4), "%04x", i);
    cmd.node_id_ = MakeNodeId(0x20).substr(0, 36) + hex4;
    cmd.role_ = MetaNodeRole::kReplica;
    cmd.expected_revision_ = revision;
    cmd.new_topology_epoch_ = revision + 1;
    ASSERT_TRUE(store.Apply(cmd).ok()) << i;
    ++revision;
  }
  AssignNodeToGroup over;
  over.request_id_ = MakeRequestId(0x21);
  over.group_id_ = "group-a";
  over.node_id_ = MakeNodeId(0x21);
  over.expected_revision_ = revision;
  over.new_topology_epoch_ = revision + 1;
  ExpectDomainReject(store.Apply(over));
}

TEST(MetaTopologyStore, RemoveNodeFromGroupRemovesMember) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary,
                                    /*expected=*/1))
                  .ok());
  ASSERT_TRUE(
      store.Apply(MakeRemove("group-a", 0x10, /*expected_revision=*/2)).ok());
  const auto view = store.FindGroup("group-a");
  EXPECT_TRUE(view->members_.empty());
  EXPECT_EQ(view->revision_, 3u);
  EXPECT_FALSE(store.FindGroupOfNode(MakeNodeId(0x10)).has_value());
}

TEST(MetaTopologyStore, RemoveNodeFromGroupReplayIsIdempotentAccept) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary,
                                    /*expected=*/1))
                  .ok());
  const RemoveNodeFromGroup cmd = MakeRemove("group-a", 0x10, /*expected=*/2);
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay: the member is already gone and the record sits at the revision
  // this command produces -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.FindGroup("group-a")->revision_, 3u);
}

TEST(MetaTopologyStore, RemoveNodeFromGroupRejects) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary,
                                    /*expected=*/1))
                  .ok());
  // Unknown group.
  ExpectDomainReject(store.Apply(MakeRemove("group-ghost", 0x10, 1)));
  // CAS conflict.
  ExpectDomainReject(store.Apply(MakeRemove("group-a", 0x10, /*expected=*/9)));
  // Not a member of this group.
  ExpectDomainReject(store.Apply(MakeRemove("group-a", 0x11, /*expected=*/2)));
  // Unchanged.
  EXPECT_EQ(store.FindGroup("group-a")->members_.size(), 1u);
  EXPECT_EQ(store.FindGroup("group-a")->revision_, 2u);
}

// ---------------------------------------------------------------------------
// SetSlotMap: absolute slot map replacement, exact-next topology_epoch,
// overlap/bounds/unknown-group rejection, replay idempotency.
// ---------------------------------------------------------------------------

using keylane::meta::SetSlotMap;

SetSlotMap MakeSlotMap(
    std::vector<keylane::meta::MetaSlotAssignment> ranges,
    std::uint64_t new_topology_epoch,
    std::vector<keylane::meta::MetaGroupConfigEpoch> config_epochs = {}) {
  SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x90);
  cmd.ranges_ = std::move(ranges);
  cmd.new_topology_epoch_ = new_topology_epoch;
  cmd.config_epochs_ = std::move(config_epochs);
  return cmd;
}

// Two groups at epochs 1 and 2.
void MakeTwoGroups(MetaTopologyStore& store) {
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-b", 2)).ok());
}

TEST(MetaTopologyStore, SetSlotMapAssignsSlotsAndEpochs) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  const SetSlotMap cmd =
      MakeSlotMap({{0, 100, "group-a"}, {200, 300, "group-b"}},
                  /*new_topology_epoch=*/3, {{"group-a", 11}});
  ASSERT_TRUE(store.Apply(cmd).ok());

  EXPECT_EQ(store.TopologyEpoch(), 3u);
  EXPECT_EQ(store.SlotOwner(0), std::optional<std::string>("group-a"));
  EXPECT_EQ(store.SlotOwner(100), std::optional<std::string>("group-a"));
  EXPECT_FALSE(store.SlotOwner(101).has_value());  // gap: unassigned
  EXPECT_EQ(store.SlotOwner(299), std::optional<std::string>("group-b"));
  EXPECT_EQ(store.SlotOwner(300), std::optional<std::string>("group-b"));
  EXPECT_FALSE(store.SlotOwner(301).has_value());
  EXPECT_FALSE(store.SlotOwner(keylane::meta::kMetaSlotCount)
                   .has_value());  // out-of-range query
  EXPECT_EQ(store.FindGroup("group-a")->config_epoch_, 11u);
  EXPECT_EQ(store.FindGroup("group-b")->config_epoch_, 0u);
}

TEST(MetaTopologyStore, SetSlotMapRequiresExactNextEpoch) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  ExpectDomainReject(store.Apply(MakeSlotMap({{0, 100, "group-a"}}, 2)));
  ExpectDomainReject(store.Apply(MakeSlotMap({{0, 100, "group-a"}}, 4)));
  EXPECT_EQ(store.TopologyEpoch(), 2u);
  EXPECT_FALSE(store.SlotOwner(0).has_value());
}

TEST(MetaTopologyStore, SetSlotMapRejectsOverlap) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  // Overlapping ranges, even for the same group, are rejected (both ends
  // inclusive).
  ExpectDomainReject(
      store.Apply(MakeSlotMap({{0, 100, "group-a"}, {50, 150, "group-a"}}, 3)));
  ExpectDomainReject(store.Apply(
      MakeSlotMap({{0, 100, "group-a"}, {100, 200, "group-b"}}, 3)));
  // Unsorted input that overlaps is still rejected.
  ExpectDomainReject(store.Apply(
      MakeSlotMap({{100, 200, "group-b"}, {0, 100, "group-a"}}, 3)));
  EXPECT_EQ(store.TopologyEpoch(), 2u);
}

TEST(MetaTopologyStore, SetSlotMapRejectsOutOfBounds) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  // Constructed in memory, bypassing the codec: the store re-validates.
  ExpectDomainReject(store.Apply(MakeSlotMap({{100, 99, "group-a"}}, 3)));
  ExpectDomainReject(store.Apply(
      MakeSlotMap({{0, keylane::meta::kMetaSlotCount, "group-a"}}, 3)));
}

TEST(MetaTopologyStore, SetSlotMapRejectsUnknownGroups) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  ExpectDomainReject(store.Apply(MakeSlotMap({{0, 100, "group-ghost"}}, 3)));
  ExpectDomainReject(
      store.Apply(MakeSlotMap({{0, 100, "group-a"}}, 3, {{"group-ghost", 7}})));
  EXPECT_EQ(store.TopologyEpoch(), 2u);
  EXPECT_FALSE(store.SlotOwner(0).has_value());
}

TEST(MetaTopologyStore, SetSlotMapRejectsDuplicateConfigEpochEntries) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  ExpectDomainReject(store.Apply(
      MakeSlotMap({{0, 100, "group-a"}}, 3, {{"group-a", 7}, {"group-a", 8}})));
}

TEST(MetaTopologyStore, SetSlotMapIsAbsolute) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  ASSERT_TRUE(store.Apply(MakeSlotMap({{0, 100, "group-a"}}, 3)).ok());
  // A new map replaces the whole old one because commands carry absolute
  // values.
  ASSERT_TRUE(store.Apply(MakeSlotMap({{500, 600, "group-b"}}, 4)).ok());
  EXPECT_FALSE(store.SlotOwner(0).has_value());
  EXPECT_EQ(store.SlotOwner(550), std::optional<std::string>("group-b"));
}

TEST(MetaTopologyStore, SetSlotMapEmptyRangesClearMap) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  ASSERT_TRUE(store.Apply(MakeSlotMap({{0, 100, "group-a"}}, 3)).ok());
  ASSERT_TRUE(store.Apply(MakeSlotMap(/*ranges=*/{}, 4)).ok());
  EXPECT_FALSE(store.SlotOwner(0).has_value());
  EXPECT_EQ(store.TopologyEpoch(), 4u);
}

TEST(MetaTopologyStore, SetSlotMapReplayIdempotentAndConflictRejected) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  const SetSlotMap cmd =
      MakeSlotMap({{0, 100, "group-a"}}, 3, {{"group-a", 11}});
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay: slot map, epoch, and config epochs already carry this command's
  // effect -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.TopologyEpoch(), 3u);

  // Same epoch, different content -> conflict rejection.
  ExpectDomainReject(store.Apply(MakeSlotMap({{0, 99, "group-a"}}, 3)));
  ExpectDomainReject(
      store.Apply(MakeSlotMap({{0, 100, "group-a"}}, 3, {{"group-a", 12}})));
  EXPECT_EQ(store.SlotOwner(100), std::optional<std::string>("group-a"));
  EXPECT_EQ(store.FindGroup("group-a")->config_epoch_, 11u);
}

// ---------------------------------------------------------------------------
// Granular primitives for the apply dispatcher (owner switch atomicity across
// the grant store is orchestrated there, not here).
// ---------------------------------------------------------------------------

TEST(MetaTopologyStore, GranularPrimitivesSetRecordFields) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());

  ASSERT_TRUE(store.SetOwner("group-a", MakeNodeId(0x30)).ok());
  ASSERT_TRUE(store.SetGroupTerm("group-a", 7).ok());
  ASSERT_TRUE(store.SetAuthorityVersion("group-a", 3).ok());
  ASSERT_TRUE(store.SetPopulationManifestId("group-a", 555).ok());
  ASSERT_TRUE(store.SetPartitionReplicationEpoch("group-a", 2).ok());
  ASSERT_TRUE(store.SetGroupConfigEpoch("group-a", 9).ok());

  const auto view = store.FindGroup("group-a");
  EXPECT_EQ(view->record_.owner_, MakeNodeId(0x30));
  EXPECT_EQ(view->record_.group_term_, 7u);
  EXPECT_EQ(view->record_.authority_version_, 3u);
  EXPECT_EQ(view->record_.population_manifest_id_, 555u);
  EXPECT_EQ(view->record_.partition_replication_epoch_, 2u);
  EXPECT_EQ(view->config_epoch_, 9u);
  // Membership CAS revision untouched by record-field changes.
  EXPECT_EQ(view->revision_, 1u);

  // Setting the value already held is an idempotent no-op accept.
  ASSERT_TRUE(store.SetOwner("group-a", MakeNodeId(0x30)).ok());
  ASSERT_TRUE(store.SetGroupTerm("group-a", 7).ok());
  ASSERT_TRUE(store.SetAuthorityVersion("group-a", 3).ok());
  ASSERT_TRUE(store.SetPopulationManifestId("group-a", 555).ok());
  ASSERT_TRUE(store.SetPartitionReplicationEpoch("group-a", 2).ok());
  ASSERT_TRUE(store.SetGroupConfigEpoch("group-a", 9).ok());

  // Unknown groups are rejected by every primitive.
  ExpectDomainReject(store.SetOwner("group-ghost", MakeNodeId(0x30)));
  ExpectDomainReject(store.SetGroupTerm("group-ghost", 7));
  ExpectDomainReject(store.SetAuthorityVersion("group-ghost", 3));
  ExpectDomainReject(store.SetPopulationManifestId("group-ghost", 555));
  ExpectDomainReject(store.SetPartitionReplicationEpoch("group-ghost", 2));
  ExpectDomainReject(store.SetGroupConfigEpoch("group-ghost", 9));
}

TEST(MetaTopologyStore, SetTopologyEpochRules) {
  MetaTopologyStore store;
  // Exactly current+1 is accepted; the current value is an idempotent
  // no-op; anything else is rejected.
  ExpectDomainReject(store.SetTopologyEpoch(2));  // gap from 0
  ASSERT_TRUE(store.SetTopologyEpoch(0).ok());    // 0 already held: no-op
  ASSERT_TRUE(store.SetTopologyEpoch(1).ok());
  EXPECT_EQ(store.TopologyEpoch(), 1u);
  ASSERT_TRUE(store.SetTopologyEpoch(1).ok());  // idempotent no-op
  EXPECT_EQ(store.TopologyEpoch(), 1u);
  ExpectDomainReject(store.SetTopologyEpoch(3));  // gap
  ExpectDomainReject(store.SetTopologyEpoch(0));  // regression
  EXPECT_EQ(store.TopologyEpoch(), 1u);
  ASSERT_TRUE(store.SetTopologyEpoch(2).ok());
  EXPECT_EQ(store.TopologyEpoch(), 2u);
}

TEST(MetaTopologyStore, RemoveOwnerDoesNotCascade) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x30, MetaNodeRole::kPrimary,
                                    /*expected=*/1))
                  .ok());
  ASSERT_TRUE(store.SetOwner("group-a", MakeNodeId(0x30)).ok());
  ASSERT_TRUE(store.Apply(MakeRemove("group-a", 0x30, /*expected=*/2)).ok());
  // Membership is independent of the owner fact: the store exposes it, the
  // apply dispatcher reacts.
  const auto view = store.FindGroup("group-a");
  EXPECT_TRUE(view->members_.empty());
  EXPECT_EQ(view->record_.owner_, MakeNodeId(0x30));
}

// ---------------------------------------------------------------------------
// Topology serialization: u16 schema_version envelope, deterministic sorted
// output, strict fail-stop decode including in-byte invariant violations.
// ---------------------------------------------------------------------------

MetaTopologyStore MakePopulatedTopology() {
  MetaTopologyStore store;
  EXPECT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  EXPECT_TRUE(store.Apply(MakeCreateGroup("group-b", 2)).ok());
  EXPECT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary,
                                    /*expected=*/1, /*topology_epoch=*/3))
                  .ok());
  EXPECT_TRUE(store
                  .Apply(MakeAssign("group-a", 0x11, MetaNodeRole::kReplica,
                                    /*expected=*/2, /*topology_epoch=*/4))
                  .ok());
  EXPECT_TRUE(store
                  .Apply(MakeAssign("group-b", 0x12, MetaNodeRole::kPrimary,
                                    /*expected=*/1, /*topology_epoch=*/5))
                  .ok());
  EXPECT_TRUE(store.SetOwner("group-a", MakeNodeId(0x10)).ok());
  EXPECT_TRUE(store.SetGroupTerm("group-a", 7).ok());
  EXPECT_TRUE(store.SetAuthorityVersion("group-a", 3).ok());
  EXPECT_TRUE(store.SetPopulationManifestId("group-a", 555).ok());
  EXPECT_TRUE(store.SetPartitionReplicationEpoch("group-a", 2).ok());
  EXPECT_TRUE(
      store
          .Apply(MakeSlotMap({{0, 100, "group-a"}, {200, 300, "group-b"}},
                             /*new_topology_epoch=*/6,
                             {{"group-a", 11}, {"group-b", 12}}))
          .ok());
  return store;
}

void ExpectEqualTopology(const MetaTopologyStore& a,
                         const MetaTopologyStore& b) {
  EXPECT_EQ(a.TopologyEpoch(), b.TopologyEpoch());
  EXPECT_EQ(a.GroupCount(), b.GroupCount());
  EXPECT_EQ(a.FindGroup("group-a"), b.FindGroup("group-a"));
  EXPECT_EQ(a.FindGroup("group-b"), b.FindGroup("group-b"));
  for (const std::uint32_t slot : {0u, 50u, 100u, 101u, 200u, 300u, 301u}) {
    EXPECT_EQ(a.SlotOwner(slot), b.SlotOwner(slot)) << "slot=" << slot;
  }
  EXPECT_EQ(a.FindGroupOfNode(MakeNodeId(0x10)),
            b.FindGroupOfNode(MakeNodeId(0x10)));
  EXPECT_EQ(a.FindGroupOfNode(MakeNodeId(0x12)),
            b.FindGroupOfNode(MakeNodeId(0x12)));
}

TEST(MetaTopologyStore, SerializationRoundTrip) {
  const MetaTopologyStore store = MakePopulatedTopology();
  const std::string bytes = store.Serialize();
  ASSERT_GE(bytes.size(), 2u);
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
  EXPECT_EQ(static_cast<std::uint16_t>(p[0] | (p[1] << 8)),
            keylane::meta::kMetaFormatVersion);

  const auto loaded = MetaTopologyStore::Deserialize(bytes);
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  ExpectEqualTopology(store, *loaded);
  // Fixed point: re-serializing the loaded store reproduces the bytes.
  EXPECT_EQ(loaded->Serialize(), bytes);
}

TEST(MetaTopologyStore, EmptyTopologyRoundTrip) {
  const MetaTopologyStore store;
  const auto loaded = MetaTopologyStore::Deserialize(store.Serialize());
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  EXPECT_EQ(loaded->TopologyEpoch(), 0u);
  EXPECT_EQ(loaded->GroupCount(), 0u);
  EXPECT_FALSE(loaded->SlotOwner(0).has_value());
}

TEST(MetaTopologyStore, SerializationIsDeterministic) {
  // Same final membership reached in different apply orders serializes to
  // identical bytes (sorted tables), which snapshot convergence relies on.
  MetaTopologyStore a;
  MetaTopologyStore b;
  for (MetaTopologyStore* store : {&a, &b}) {
    ASSERT_TRUE(store->Apply(MakeCreateGroup("group-a", 1)).ok());
  }
  ASSERT_TRUE(
      a.Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary, 1)).ok());
  ASSERT_TRUE(
      a.Apply(MakeAssign("group-a", 0x11, MetaNodeRole::kReplica, 2)).ok());
  ASSERT_TRUE(
      b.Apply(MakeAssign("group-a", 0x11, MetaNodeRole::kReplica, 1)).ok());
  ASSERT_TRUE(
      b.Apply(MakeAssign("group-a", 0x10, MetaNodeRole::kPrimary, 2)).ok());
  EXPECT_EQ(a.Serialize(), b.Serialize());
}

TEST(MetaTopologyStore, DeserializeRejectsCorruption) {
  const MetaTopologyStore store = MakePopulatedTopology();
  const std::string bytes = store.Serialize();
  for (std::size_t len = 0; len < bytes.size(); ++len) {
    SCOPED_TRACE("len=" + std::to_string(len));
    ExpectStoreFailStop(
        MetaTopologyStore::Deserialize(std::string_view(bytes).substr(0, len))
            .status());
  }
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(bytes + '\0').status());  // trailing
  std::string bad_version = bytes;
  bad_version[0] = '\x7F';
  ExpectStoreFailStop(MetaTopologyStore::Deserialize(bad_version).status());
}

// Hand-builds a topology blob from parts, so in-byte invariant violations
// can be expressed. Layout mirrors the store's serialization contract.
struct TopologyBlobGroup {
  std::string group_id;
  std::string owner;
  std::uint64_t revision = 1;
  std::vector<MetaGroupMember> members;
};

std::string MakeTopologyBlob(
    std::uint64_t topology_epoch, const std::vector<TopologyBlobGroup>& groups,
    const std::vector<keylane::meta::MetaSlotAssignment>& runs) {
  keylane::meta::MetaWriter w;
  w.WriteU16(keylane::meta::kMetaFormatVersion);
  w.WriteU64(topology_epoch);
  w.WriteCount(static_cast<std::uint32_t>(groups.size()));
  for (const TopologyBlobGroup& group : groups) {
    w.WriteString(group.group_id);
    w.WriteString(group.owner);
    w.WriteU64(0);  // group_term
    w.WriteU64(0);  // authority_version
    w.WriteU64(0);  // population_manifest_id
    w.WriteU64(0);  // partition_replication_epoch
    w.WriteU64(0);  // config_epoch
    w.WriteU64(group.revision);
    w.WriteList(group.members, [](keylane::meta::MetaWriter& ww,
                                  const MetaGroupMember& member) {
      ww.WriteString(member.node_id_);
      ww.WriteU8(static_cast<std::uint8_t>(member.role_));
    });
  }
  w.WriteList(runs, [](keylane::meta::MetaWriter& ww,
                       const keylane::meta::MetaSlotAssignment& run) {
    ww.WriteU16(run.first_slot_);
    ww.WriteU16(run.last_slot_);
    ww.WriteString(run.group_id_);
  });
  return w.buffer();
}

TEST(MetaTopologyStore, DeserializeRejectsInvariantViolations) {
  const std::string node_a = MakeNodeId(0x40);

  // A node appearing in two groups breaks one-node-one-group.
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(
              2,
              {TopologyBlobGroup{
                   "group-a",
                   "",
                   1,
                   {MetaGroupMember{node_a, MetaNodeRole::kPrimary}}},
               TopologyBlobGroup{
                   "group-b",
                   "",
                   1,
                   {MetaGroupMember{node_a, MetaNodeRole::kReplica}}}},
              {}))
          .status());
  // Duplicate group_id.
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(
              2, {TopologyBlobGroup{"group-a"}, TopologyBlobGroup{"group-a"}},
              {}))
          .status());
  // Duplicate member within one group.
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(
              1,
              {TopologyBlobGroup{
                  "group-a",
                  "",
                  1,
                  {MetaGroupMember{node_a, MetaNodeRole::kPrimary},
                   MetaGroupMember{node_a, MetaNodeRole::kReplica}}}},
              {}))
          .status());
  // Revision 0 (revisions start at 1).
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(
              1, {TopologyBlobGroup{"group-a", "", /*revision=*/0, {}}}, {}))
          .status());
  // Slot run out of bounds (last >= kMetaSlotCount).
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(1, {TopologyBlobGroup{"group-a"}},
                           {{0, keylane::meta::kMetaSlotCount, "group-a"}}))
          .status());
  // Overlapping slot runs.
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(1, {TopologyBlobGroup{"group-a"}},
                           {{0, 100, "group-a"}, {50, 200, "group-a"}}))
          .status());
  // Unsorted slot runs.
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(1, {TopologyBlobGroup{"group-a"}},
                           {{200, 300, "group-a"}, {0, 100, "group-a"}}))
          .status());
  // Slot run referencing an unknown group.
  ExpectStoreFailStop(MetaTopologyStore::Deserialize(
                          MakeTopologyBlob(1, {TopologyBlobGroup{"group-a"}},
                                           {{0, 100, "group-ghost"}}))
                          .status());
  // Empty group_id.
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(1, {TopologyBlobGroup{"", "", 1, {}}}, {}))
          .status());
}

// ===========================================================================
// Policy store.
// ===========================================================================

using keylane::meta::MetaHash256;
using keylane::meta::MetaPolicyStore;
using keylane::meta::PutPolicy;

// SHA-256 of `hex`, as bytes, for known-answer tests.
MetaHash256 HashFromHex(const std::string& hex) {
  MetaHash256 out{};
  for (std::size_t i = 0; i < out.size(); ++i) {
    out[i] = static_cast<std::uint8_t>(
        std::stoul(hex.substr(i * 2, 2), nullptr, 16));
  }
  return out;
}

PutPolicy MakePut(const std::string& policy_id, std::uint64_t version,
                  std::string content) {
  PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(0xA0);
  cmd.policy_id_ = policy_id;
  cmd.version_ = version;
  cmd.content_ = std::move(content);
  cmd.content_hash_ = MetaPolicyStore::ContentHash(cmd.content_);
  return cmd;
}

TEST(MetaPolicyStore, ContentHashMatchesSha256KnownAnswer) {
  // FIPS 180-4 / RFC 6234 known-answer vectors: the store's hash check is
  // SHA-256, not an opaque proposer token.
  EXPECT_EQ(MetaPolicyStore::ContentHash(""),
            HashFromHex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934c"
                        "a495991b7852b855"));
  EXPECT_EQ(MetaPolicyStore::ContentHash("abc"),
            HashFromHex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9c"
                        "b410ff61f20015ad"));
  EXPECT_EQ(MetaPolicyStore::ContentHash(std::string(1000000, 'a')),
            HashFromHex("cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e"
                        "046d39ccc7112cd0"));
}

TEST(MetaPolicyStore, PutPolicyStoresQueryableVersion) {
  MetaPolicyStore store;
  const PutPolicy cmd = MakePut("migration-policy", /*version=*/1,
                                "{\"phases\":[\"prepare\",\"cutover\"]}");
  ASSERT_TRUE(store.Apply(cmd).ok());

  EXPECT_TRUE(store.IsVersionPresent("migration-policy", 1));
  EXPECT_TRUE(store.IsVersionActive("migration-policy", 1));
  EXPECT_FALSE(store.IsVersionPresent("migration-policy", 2));
  EXPECT_EQ(store.LatestVersion("migration-policy"),
            std::optional<std::uint64_t>(1));
  EXPECT_EQ(store.PolicyCount(), 1u);
  EXPECT_EQ(store.TotalContentBytes(), cmd.content_.size());

  const auto view = store.FindVersion("migration-policy", 1);
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->policy_id_, "migration-policy");
  EXPECT_EQ(view->version_, 1u);
  EXPECT_EQ(view->content_, cmd.content_);
  EXPECT_EQ(view->content_hash_, cmd.content_hash_);
  EXPECT_FALSE(view->retired_);
}

TEST(MetaPolicyStore, PutPolicyRejectsHashMismatch) {
  MetaPolicyStore store;
  PutPolicy cmd = MakePut("migration-policy", 1, "content-v1");
  cmd.content_hash_ = MetaPolicyStore::ContentHash("different-content");
  ExpectDomainReject(store.Apply(cmd));
  EXPECT_FALSE(store.IsVersionPresent("migration-policy", 1));
}

TEST(MetaPolicyStore, PutPolicyRejectsInvalidFields) {
  MetaPolicyStore store;
  {
    PutPolicy cmd = MakePut("", 1, "content");  // empty policy_id
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    // Empty content: a zero-byte policy document has no meaning, and the
    // byte cap could not bound the version count (see header).
    PutPolicy cmd = MakePut("p", 1, "");
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    PutPolicy cmd =
        MakePut(std::string(keylane::meta::kMaxMetaPolicyIdBytes + 1, 'p'), 1,
                "content");
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    PutPolicy cmd;
    cmd.request_id_ = MakeRequestId(0xA1);
    cmd.policy_id_ = "p";
    cmd.version_ = 1;
    cmd.content_ = std::string(keylane::meta::kMaxMetaPayloadBytes + 1, 'x');
    cmd.content_hash_ = MetaPolicyStore::ContentHash(cmd.content_);
    ExpectDomainReject(store.Apply(cmd));
  }
  EXPECT_EQ(store.PolicyCount(), 0u);
}

TEST(MetaPolicyStore, PutPolicyReplayIsIdempotentAccept) {
  MetaPolicyStore store;
  const PutPolicy cmd = MakePut("migration-policy", 1, "content-v1");
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay of the same log index: same version slot, same content, still
  // active -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.PolicyCount(), 1u);
  EXPECT_EQ(store.TotalContentBytes(), cmd.content_.size());
}

TEST(MetaPolicyStore, PutPolicySameVersionDifferentContentRejected) {
  MetaPolicyStore store;
  ASSERT_TRUE(store.Apply(MakePut("migration-policy", 1, "content-v1")).ok());
  ExpectDomainReject(
      store.Apply(MakePut("migration-policy", 1, "content-OTHER")));
  EXPECT_EQ(store.FindVersion("migration-policy", 1)->content_, "content-v1");
}

TEST(MetaPolicyStore, PutPolicyRequiresMonotonicVersion) {
  MetaPolicyStore store;
  ASSERT_TRUE(store.Apply(MakePut("p", 5, "v5")).ok());
  // Strictly greater than the latest existing version (gaps are legal).
  ASSERT_TRUE(store.Apply(MakePut("p", 9, "v9")).ok());
  ExpectDomainReject(store.Apply(MakePut("p", 4, "v4")));
  ExpectDomainReject(store.Apply(MakePut("p", 5, "v5-different")));
  ExpectDomainReject(store.Apply(MakePut("p", 9, "v9-different")));
  ASSERT_TRUE(store.Apply(MakePut("p", 10, "v10")).ok());
  EXPECT_EQ(store.LatestVersion("p"), std::optional<std::uint64_t>(10));
  // Independent version sequences per policy_id.
  ASSERT_TRUE(store.Apply(MakePut("q", 1, "v1")).ok());
  EXPECT_EQ(store.LatestVersion("q"), std::optional<std::uint64_t>(1));
}

// ---------------------------------------------------------------------------
// Caps: kMaxMetaPolicyVersionsPerPolicy per policy,
// kMaxMetaPolicyTotalBytes across all policies. Over-cap = rejection, never
// silent truncation.
// ---------------------------------------------------------------------------

TEST(MetaPolicyStore, PutPolicyEnforcesVersionsPerPolicyCap) {
  MetaPolicyStore store;
  for (std::uint32_t v = 1; v <= keylane::meta::kMaxMetaPolicyVersionsPerPolicy;
       ++v) {
    ASSERT_TRUE(
        store.Apply(MakePut("p", v, "content-" + std::to_string(v))).ok())
        << v;
  }
  ExpectDomainReject(store.Apply(MakePut(
      "p", keylane::meta::kMaxMetaPolicyVersionsPerPolicy + 1, "over")));
  EXPECT_EQ(store.LatestVersion("p"),
            std::optional<std::uint64_t>(
                keylane::meta::kMaxMetaPolicyVersionsPerPolicy));
  // The cap is per policy: another policy still accepts versions.
  ASSERT_TRUE(store.Apply(MakePut("q", 1, "fine")).ok());
}

TEST(MetaPolicyStore, PutPolicyEnforcesTotalBytesCap) {
  MetaPolicyStore store;
  // Fill the budget exactly: 64 policies x 256 KiB = 16 MiB.
  const std::string chunk(keylane::meta::kMaxMetaPayloadBytes, 'x');
  constexpr std::uint32_t kChunks = keylane::meta::kMaxMetaPolicyTotalBytes /
                                    keylane::meta::kMaxMetaPayloadBytes;
  for (std::uint32_t i = 0; i < kChunks; ++i) {
    ASSERT_TRUE(
        store.Apply(MakePut("policy-" + std::to_string(i), 1, chunk)).ok())
        << i;
  }
  EXPECT_EQ(store.TotalContentBytes(), keylane::meta::kMaxMetaPolicyTotalBytes);
  // One more byte is rejected; state unchanged.
  ExpectDomainReject(store.Apply(MakePut("policy-over", 1, "y")));
  EXPECT_FALSE(store.IsVersionPresent("policy-over", 1));
  EXPECT_EQ(store.TotalContentBytes(), keylane::meta::kMaxMetaPolicyTotalBytes);
}

// ---------------------------------------------------------------------------
// RetirePolicy: terminal tombstone, content retained, replay idempotent.
// The guard against retiring a version still referenced by an active
// grant or non-terminal operation is cross-store (grant/operation stores)
// and enforced by the apply dispatcher; the store exposes the facts.
// ---------------------------------------------------------------------------

using keylane::meta::RetirePolicy;

RetirePolicy MakeRetirePolicy(const std::string& policy_id,
                              std::uint64_t version) {
  RetirePolicy cmd;
  cmd.request_id_ = MakeRequestId(0xB0);
  cmd.policy_id_ = policy_id;
  cmd.version_ = version;
  return cmd;
}

TEST(MetaPolicyStore, RetirePolicyRetires) {
  MetaPolicyStore store;
  const PutPolicy put = MakePut("p", 1, "content-v1");
  ASSERT_TRUE(store.Apply(put).ok());
  ASSERT_TRUE(store.Apply(MakeRetirePolicy("p", 1)).ok());

  EXPECT_TRUE(store.IsVersionPresent("p", 1));  // tombstone stays
  EXPECT_FALSE(store.IsVersionActive("p", 1));
  const auto view = store.FindVersion("p", 1);
  ASSERT_TRUE(view.has_value());
  EXPECT_TRUE(view->retired_);
  EXPECT_EQ(view->content_, put.content_);  // content retained
  EXPECT_EQ(store.LatestVersion("p"), std::optional<std::uint64_t>(1));
  EXPECT_EQ(store.TotalContentBytes(), put.content_.size());
}

TEST(MetaPolicyStore, RetirePolicyReplayIsIdempotentAccept) {
  MetaPolicyStore store;
  ASSERT_TRUE(store.Apply(MakePut("p", 1, "content-v1")).ok());
  const RetirePolicy cmd = MakeRetirePolicy("p", 1);
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay: the version is already retired -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_FALSE(store.IsVersionActive("p", 1));
}

TEST(MetaPolicyStore, RetirePolicyUnknownRejected) {
  MetaPolicyStore store;
  ASSERT_TRUE(store.Apply(MakePut("p", 1, "content-v1")).ok());
  ExpectDomainReject(store.Apply(MakeRetirePolicy("p", 2)));
  ExpectDomainReject(store.Apply(MakeRetirePolicy("ghost", 1)));
  EXPECT_TRUE(store.IsVersionActive("p", 1));
}

TEST(MetaPolicyStore, RetiredVersionIsTerminal) {
  MetaPolicyStore store;
  ASSERT_TRUE(store.Apply(MakePut("p", 1, "content-v1")).ok());
  ASSERT_TRUE(store.Apply(MakeRetirePolicy("p", 1)).ok());
  // Re-putting the retired version slot is rejected even with identical
  // content: retired is terminal (the replay path is RetirePolicy itself).
  ExpectDomainReject(store.Apply(MakePut("p", 1, "content-v1")));
  // A newer version of the same policy is fine.
  ASSERT_TRUE(store.Apply(MakePut("p", 2, "content-v2")).ok());
  EXPECT_TRUE(store.IsVersionActive("p", 2));
  EXPECT_FALSE(store.IsVersionActive("p", 1));
}

// ---------------------------------------------------------------------------
// Policy serialization: u16 schema_version envelope, deterministic sorted
// output, strict fail-stop decode including in-byte invariant violations
// (hash mismatch, cap overflow, ...).
// ---------------------------------------------------------------------------

MetaPolicyStore MakePopulatedPolicies() {
  MetaPolicyStore store;
  EXPECT_TRUE(store.Apply(MakePut("migration-policy", 1, "v1-content")).ok());
  EXPECT_TRUE(store.Apply(MakePut("migration-policy", 3, "v3-content")).ok());
  EXPECT_TRUE(store.Apply(MakePut("failover-policy", 2, "v2-content")).ok());
  EXPECT_TRUE(store.Apply(MakeRetirePolicy("migration-policy", 1)).ok());
  return store;
}

TEST(MetaPolicyStore, SerializationRoundTrip) {
  const MetaPolicyStore store = MakePopulatedPolicies();
  const std::string bytes = store.Serialize();
  ASSERT_GE(bytes.size(), 2u);
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
  EXPECT_EQ(static_cast<std::uint16_t>(p[0] | (p[1] << 8)),
            keylane::meta::kMetaFormatVersion);

  const auto loaded = MetaPolicyStore::Deserialize(bytes);
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  EXPECT_EQ(loaded->PolicyCount(), 2u);
  EXPECT_EQ(loaded->TotalContentBytes(), store.TotalContentBytes());
  EXPECT_EQ(loaded->FindVersion("migration-policy", 1),
            store.FindVersion("migration-policy", 1));
  EXPECT_EQ(loaded->FindVersion("migration-policy", 3),
            store.FindVersion("migration-policy", 3));
  EXPECT_EQ(loaded->FindVersion("failover-policy", 2),
            store.FindVersion("failover-policy", 2));
  EXPECT_TRUE(loaded->IsVersionPresent("migration-policy", 1));
  EXPECT_FALSE(loaded->IsVersionActive("migration-policy", 1));
  EXPECT_EQ(loaded->LatestVersion("migration-policy"),
            std::optional<std::uint64_t>(3));
  EXPECT_EQ(loaded->Serialize(), bytes);  // fixed point
}

TEST(MetaPolicyStore, EmptyPolicyStoreRoundTrip) {
  const MetaPolicyStore store;
  const auto loaded = MetaPolicyStore::Deserialize(store.Serialize());
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  EXPECT_EQ(loaded->PolicyCount(), 0u);
  EXPECT_EQ(loaded->TotalContentBytes(), 0u);
}

TEST(MetaPolicyStore, SerializationIsDeterministic) {
  // Equal states serialize to equal bytes regardless of apply order.
  MetaPolicyStore a;
  MetaPolicyStore b;
  ASSERT_TRUE(a.Apply(MakePut("policy-a", 1, "a1")).ok());
  ASSERT_TRUE(a.Apply(MakePut("policy-b", 1, "b1")).ok());
  ASSERT_TRUE(b.Apply(MakePut("policy-b", 1, "b1")).ok());
  ASSERT_TRUE(b.Apply(MakePut("policy-a", 1, "a1")).ok());
  EXPECT_EQ(a.Serialize(), b.Serialize());
}

TEST(MetaPolicyStore, DeserializeRejectsCorruption) {
  const MetaPolicyStore store = MakePopulatedPolicies();
  const std::string bytes = store.Serialize();
  for (std::size_t len = 0; len < bytes.size(); ++len) {
    SCOPED_TRACE("len=" + std::to_string(len));
    ExpectStoreFailStop(
        MetaPolicyStore::Deserialize(std::string_view(bytes).substr(0, len))
            .status());
  }
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(bytes + '\0').status());  // trailing
  std::string bad_version = bytes;
  bad_version[0] = '\x7F';
  ExpectStoreFailStop(MetaPolicyStore::Deserialize(bad_version).status());
}

// Hand-builds a policy blob. Layout mirrors the store's serialization
// contract; hash override exists so a mismatch can be expressed.
struct PolicyBlobVersion {
  std::uint64_t version;
  std::string content;
  bool retired = false;
  std::optional<MetaHash256> hash_override;
};

std::string MakePolicyBlob(
    const std::vector<std::pair<std::string, std::vector<PolicyBlobVersion>>>&
        policies) {
  keylane::meta::MetaWriter w;
  w.WriteU16(keylane::meta::kMetaFormatVersion);
  w.WriteCount(static_cast<std::uint32_t>(policies.size()));
  for (const auto& [policy_id, versions] : policies) {
    w.WriteString(policy_id);
    w.WriteCount(static_cast<std::uint32_t>(versions.size()));
    for (const PolicyBlobVersion& version : versions) {
      w.WriteU64(version.version);
      w.WriteU8(version.retired ? 1 : 0);
      w.WriteString(version.content);
      const MetaHash256 hash = version.hash_override.value_or(
          MetaPolicyStore::ContentHash(version.content));
      keylane::meta::WriteFixedArray(w, hash);
    }
  }
  return w.buffer();
}

TEST(MetaPolicyStore, DeserializeRejectsInvariantViolations) {
  // Hash mismatch: content does not match content_hash.
  {
    MetaHash256 wrong{};
    ExpectStoreFailStop(
        MetaPolicyStore::Deserialize(
            MakePolicyBlob(
                {{"p", {PolicyBlobVersion{1, "content", false, wrong}}}}))
            .status());
  }
  // Version count over the per-policy cap.
  {
    std::vector<PolicyBlobVersion> versions;
    for (std::uint32_t v = 1;
         v <= keylane::meta::kMaxMetaPolicyVersionsPerPolicy + 1; ++v) {
      versions.push_back(PolicyBlobVersion{v, "c" + std::to_string(v)});
    }
    ExpectStoreFailStop(
        MetaPolicyStore::Deserialize(MakePolicyBlob({{"p", versions}}))
            .status());
  }
  // Duplicate version within one policy.
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob(
              {{"p", {PolicyBlobVersion{1, "a"}, PolicyBlobVersion{1, "b"}}}}))
          .status());
  // Retired tag other than 0/1: hand-encode (the blob helper writes 0/1).
  {
    keylane::meta::MetaWriter w;
    w.WriteU16(keylane::meta::kMetaFormatVersion);
    w.WriteCount(1);
    w.WriteString("p");
    w.WriteCount(1);
    w.WriteU64(1);
    w.WriteU8(2);  // invalid retired tag
    w.WriteString("content");
    keylane::meta::WriteFixedArray(w, MetaPolicyStore::ContentHash("content"));
    ExpectStoreFailStop(MetaPolicyStore::Deserialize(w.buffer()).status());
  }
  // Empty content / empty policy_id / zero-version policy entry.
  ExpectStoreFailStop(MetaPolicyStore::Deserialize(
                          MakePolicyBlob({{"p", {PolicyBlobVersion{1, ""}}}}))
                          .status());
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob({{"", {PolicyBlobVersion{1, "content"}}}}))
          .status());
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(MakePolicyBlob({{"p", {}}})).status());
  // Total content bytes over the global cap.
  {
    const std::string chunk(keylane::meta::kMaxMetaPayloadBytes, 'x');
    constexpr std::uint32_t kChunks = keylane::meta::kMaxMetaPolicyTotalBytes /
                                          keylane::meta::kMaxMetaPayloadBytes +
                                      1;
    std::vector<std::pair<std::string, std::vector<PolicyBlobVersion>>>
        policies;
    for (std::uint32_t i = 0; i < kChunks; ++i) {
      policies.push_back(
          {"policy-" + std::to_string(i), {PolicyBlobVersion{1, chunk}}});
    }
    ExpectStoreFailStop(
        MetaPolicyStore::Deserialize(MakePolicyBlob(policies)).status());
  }
}

}  // namespace
