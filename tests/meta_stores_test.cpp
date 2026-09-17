/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "meta_topology_test_access.h"

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
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "lavik/meta/encoding.h"
#include "lavik/meta/identity_store.h"
#include "lavik/meta/policy_store.h"
#include "lavik/meta/population_manifest_store.h"
#include "lavik/meta/topology_store.h"

namespace {

using lavik::meta::MetaFailureClass;
using lavik::meta::MetaFailureClassOf;
using lavik::meta::MetaIdentityStore;
using lavik::meta::MetaNodeRecord;
using lavik::meta::MetaNodeRole;
using lavik::meta::MetaRequestId;
using lavik::meta::RegisterNode;

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
  return "lavik://node/" + MakeNodeId(seed);
}

RegisterNode MakeRegister(std::uint8_t seed) {
  RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.principal_ = MakePrincipal(seed);
  cmd.endpoints_ = {"10.0.0.1:7000", "10.0.0.1:17000"};

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
    cmd.node_id_ = std::string(lavik::meta::kMetaNodeIdBytes + 1, 'a');
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.endpoints_.resize(lavik::meta::kMaxMetaEndpointsPerNode + 1, "e");
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.endpoints_ = {std::string(lavik::meta::kMaxMetaEndpointBytes + 1, 'e')};
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    RegisterNode cmd = MakeRegister(0x15);
    cmd.principal_ = std::string(lavik::meta::kMaxMetaPrincipalBytes + 1, 'p');
    ExpectDomainReject(store.Apply(cmd));
  }
  EXPECT_EQ(store.NodeCount(), 0u);
}

TEST(MetaIdentityStore, RegisterNodeEnforcesNodeCap) {
  MetaIdentityStore store;
  char hex4[5];
  for (std::uint32_t i = 0; i < lavik::meta::kMaxMetaNodes; ++i) {
    RegisterNode cmd = MakeRegister(0x20);
    // Distinct node_id (within the 40-char cap) and principal per
    // registration: replace the tail with the hex counter.
    std::snprintf(hex4, sizeof(hex4), "%04x", i);
    cmd.node_id_ = MakeNodeId(0x20).substr(0, 36) + hex4;
    cmd.principal_ = "lavik://node/" + cmd.node_id_;
    ASSERT_TRUE(store.Apply(cmd).ok()) << i;
  }
  EXPECT_EQ(store.NodeCount(), lavik::meta::kMaxMetaNodes);
  // The cap counts records, not active ones: one more is rejected.
  ExpectDomainReject(store.Apply(MakeRegister(0x21)));
}

// ---------------------------------------------------------------------------
// UpdateNode: expected_revision CAS; cannot modify the principal binding
// (the schema carries no principal field because rotation is unimplemented);
// replay is an idempotent accept; retired nodes reject updates.
// ---------------------------------------------------------------------------

using lavik::meta::RetireNode;
using lavik::meta::UpdateNode;

UpdateNode MakeUpdate(std::uint8_t seed, std::uint64_t expected_revision) {
  UpdateNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.expected_revision_ = expected_revision;
  cmd.endpoints_ = {"10.0.9.9:7000"};

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
  conflict.endpoints_ = {"tcp://127.0.0.1:7999"};
  ExpectDomainReject(store.Apply(conflict));
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
            lavik::meta::kMetaIdentityStoreFormatVersion);

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
  using lavik::meta::MetaWriter;
  const std::string node_a = MakeNodeId(0x71);
  const std::string node_b = MakeNodeId(0x72);
  const std::string principal = MakePrincipal(0x71);

  auto write_record = [&](MetaWriter& w, const std::string& node_id,
                          const std::string& prin, std::uint64_t revision) {
    w.WriteString(node_id);
    w.WriteString(prin);
    w.WriteCount(0);  // endpoints
    w.WriteU8(static_cast<std::uint8_t>(MetaNodeRole::kReplica));
    w.WriteU64(revision);
    w.WriteU8(0);  // active
  };
  auto make_blob = [&](auto write_body) {
    MetaWriter w;
    w.WriteU16(lavik::meta::kMetaIdentityStoreFormatVersion);
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
        w.WriteU32(lavik::meta::kMaxMetaNodes + 1);
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

using lavik::meta::CreateGroup;
using lavik::meta::MetaClusterLifecycle;

using lavik::meta::MetaFailoverMode;
using lavik::meta::MetaFailoverTransition;
using lavik::meta::MetaFailoverTransitionRef;
using lavik::meta::MetaOperationId;
using lavik::meta::MetaTopologyStore;

MetaOperationId MakeOperationId(std::uint8_t seed) {
  MetaOperationId id{};
  id.fill(seed);
  return id;
}

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

MetaFailoverTransition MakeUncontrolledTransition(std::uint8_t seed) {
  MetaFailoverTransition transition;
  transition.transition_id_.fill(seed);
  transition.revision_ = 999;  // The store replaces this with the apply index.
  transition.mode_ = MetaFailoverMode::kUncontrolled;
  transition.target_term_ = 8;
  return transition;
}

// ---------------------------------------------------------------------------
// Cluster lifecycle: one durable creation binding independent of topology
// epochs and the operation journal's retention policy.
// ---------------------------------------------------------------------------

TEST(MetaTopologyStore, ClusterLifecycleBeginsAndCompletesIndependently) {
  MetaTopologyStore store;
  const MetaOperationId root = MakeOperationId(0x91);

  EXPECT_EQ(store.ClusterLifecycle().state_,
            MetaClusterLifecycle::kUninitialized);
  EXPECT_EQ(store.ClusterLifecycle().Revision(), 0u);
  EXPECT_EQ(store.TopologyEpoch(), 0u);

  ASSERT_TRUE(store.BeginClusterCreate(root, 42).ok());
  EXPECT_EQ(store.ClusterLifecycle().state_, MetaClusterLifecycle::kCreating);
  EXPECT_EQ(store.ClusterLifecycle().Revision(), 1u);
  EXPECT_EQ(store.ClusterLifecycle().root_operation_id_, root);
  EXPECT_EQ(store.ClusterLifecycle().genesis_commit_index_, 42u);

  EXPECT_EQ(store.TopologyEpoch(), 0u);

  ASSERT_TRUE(store.BeginClusterCreate(root, 42).ok());
  EXPECT_EQ(store.ClusterLifecycle().Revision(), 1u);
  ExpectDomainReject(store.BeginClusterCreate(MakeOperationId(0x92), 43));

  ASSERT_TRUE(store.CompleteClusterCreate(root).ok());
  EXPECT_EQ(store.ClusterLifecycle().state_, MetaClusterLifecycle::kCreated);
  EXPECT_EQ(store.ClusterLifecycle().Revision(), 2u);

  EXPECT_TRUE(store.ClusterLifecycle().failure_summary_.empty());
  EXPECT_EQ(store.TopologyEpoch(), 0u);

  ASSERT_TRUE(store.CompleteClusterCreate(root).ok());
  EXPECT_EQ(store.ClusterLifecycle().Revision(), 2u);
  ExpectDomainReject(store.FailClusterCreate(root, "too late"));
}

TEST(MetaTopologyStore, ClusterLifecycleFailureIsBoundedAndSerialized) {
  MetaTopologyStore store;
  const MetaOperationId root = MakeOperationId(0x93);
  ASSERT_TRUE(store.BeginClusterCreate(root, 77).ok());
  ASSERT_TRUE(store.FailClusterCreate(root, "initial population failed").ok());

  const auto loaded = MetaTopologyStore::Deserialize(store.Serialize());
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  EXPECT_EQ(loaded->ClusterLifecycle(), store.ClusterLifecycle());
  EXPECT_EQ(loaded->ClusterLifecycle().state_,
            MetaClusterLifecycle::kProvisioningFailed);
  EXPECT_EQ(loaded->ClusterLifecycle().failure_summary_,
            "initial population failed");
  ExpectDomainReject(store.FailClusterCreate(
      root,
      std::string(lavik::meta::kMaxMetaClusterFailureSummaryBytes + 1, 'x')));
  ExpectDomainReject(store.FailClusterCreate(root, ""));
  ExpectDomainReject(store.FailClusterCreate(root, "unsafe\nsummary"));
}

// ---------------------------------------------------------------------------
// CreateGroup: absolute topology_epoch (exactly current+1), pristine replay
// idempotency, group cap.
// ---------------------------------------------------------------------------

TEST(MetaTopologyStore, AuthorityAndOwnerShareTheGroupLifetime) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("g1", 1)).ok());
  lavik::meta::BeginGroupTerm begin;
  begin.group_id_ = "g1";
  begin.new_term_ = 1;
  ASSERT_TRUE(store.BeginGroupTerm(begin).ok());
  lavik::meta::ActivateAuthority activate;
  activate.group_id_ = "g1";
  activate.expected_term_ = 1;
  activate.new_owner_ = std::string(40, 'a');
  ASSERT_TRUE(store.ActivateAuthority(activate).ok());
  EXPECT_EQ(store.FindGroup("g1")->record_.owner_, activate.new_owner_);
  EXPECT_TRUE(store.AuthorityFor("g1")->grant_.has_value());
  const auto bytes = store.Serialize();
  auto restored = MetaTopologyStore::Deserialize(bytes);
  ASSERT_TRUE(restored.ok());
  EXPECT_EQ(restored->AuthorityFor("g1"), store.AuthorityFor("g1"));
  begin.expected_term_ = 1;
  begin.new_term_ = 2;
  ASSERT_TRUE(restored->BeginGroupTerm(begin).ok());
  EXPECT_EQ(restored->FindGroup("g1")->record_.group_term_, 2u);
  EXPECT_EQ(restored->FindGroup("g1")->record_.owner_, activate.new_owner_);
  EXPECT_FALSE(restored->AuthorityFor("g1")->grant_.has_value());
  EXPECT_FALSE(restored->ActivateAuthority(activate).ok());
}

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
  EXPECT_TRUE(view->members_.empty());
  // Freshly created GroupRecord: no owner, all counters zero.
  EXPECT_EQ(view->record_.owner_, "");
  EXPECT_EQ(view->record_.group_term_, 0u);
  EXPECT_EQ(view->record_.population_manifest_revision_, 0u);
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
      std::string(lavik::meta::kMaxMetaGroupIdBytes + 1, 'g'), 1)));
  EXPECT_EQ(store.GroupCount(), 0u);
  EXPECT_EQ(store.TopologyEpoch(), 0u);
}

TEST(MetaTopologyStore, CreateGroupEnforcesGroupCap) {
  MetaTopologyStore store;
  for (std::uint32_t i = 0; i < lavik::meta::kMaxMetaGroups; ++i) {
    char gid[32];
    std::snprintf(gid, sizeof(gid), "group-%04x", i);
    ASSERT_TRUE(store.Apply(MakeCreateGroup(gid, i + 1)).ok()) << i;
  }
  EXPECT_EQ(store.GroupCount(), lavik::meta::kMaxMetaGroups);
  ExpectDomainReject(store.Apply(
      MakeCreateGroup("group-over", lavik::meta::kMaxMetaGroups + 1)));
  EXPECT_EQ(store.TopologyEpoch(), lavik::meta::kMaxMetaGroups);
}

TEST(MetaTopologyStore, InstallFailoverTransitionCreatesQueryableState) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());

  MetaFailoverTransition expected = MakeUncontrolledTransition(0x51);
  ExpectDomainReject(store.InstallFailoverTransition("group-a", expected, 0));
  MetaFailoverTransition invalid = expected;
  invalid.transition_id_.fill(0);
  ExpectDomainReject(store.InstallFailoverTransition("group-a", invalid, 42));
  EXPECT_FALSE(store.FindGroup("group-a")->failover_transition_.has_value());

  ASSERT_TRUE(store
                  .InstallFailoverTransition("group-a", expected,
                                             /*committed_index=*/42)
                  .ok());
  expected.revision_ = 42;
  ASSERT_TRUE(store.InstallFailoverTransition("group-a", expected, 42).ok());

  MetaFailoverTransition conflict = MakeUncontrolledTransition(0x52);
  ExpectDomainReject(store.InstallFailoverTransition("group-a", conflict, 43));

  const auto view = store.FindGroup("group-a");
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->failover_transition_, expected);
  EXPECT_EQ(view->revision_, 1u);  // Membership CAS is independent.
}

TEST(MetaTopologyStore, ReplaceFailoverTransitionUsesExactRevisionCas) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  const MetaFailoverTransition initial = MakeUncontrolledTransition(0x51);
  ASSERT_TRUE(store.InstallFailoverTransition("group-a", initial, 42).ok());

  const MetaFailoverTransitionRef initial_ref{initial.transition_id_, 42};
  MetaFailoverTransition replacement = initial;
  replacement.target_term_ = 9;
  ASSERT_TRUE(
      store.ReplaceFailoverTransition("group-a", initial_ref, replacement, 50)
          .ok());
  replacement.revision_ = 50;
  EXPECT_EQ(store.FindGroup("group-a")->failover_transition_, replacement);

  // The caller's old precondition is stale after the first apply, but the
  // complete post-state proves this is an exact replay.
  ASSERT_TRUE(
      store.ReplaceFailoverTransition("group-a", initial_ref, replacement, 50)
          .ok());

  MetaFailoverTransition conflict = replacement;
  conflict.target_term_ = 10;
  ExpectDomainReject(store.ReplaceFailoverTransition(
      "group-a", MetaFailoverTransitionRef{initial.transition_id_, 49},
      conflict, 51));
  EXPECT_EQ(store.FindGroup("group-a")->failover_transition_, replacement);

  ExpectDomainReject(store.ReplaceFailoverTransition(
      "group-a", MetaFailoverTransitionRef{initial.transition_id_, 50},
      conflict, 50));
  EXPECT_EQ(store.FindGroup("group-a")->failover_transition_, replacement);

  MetaFailoverTransition different_identity = conflict;
  different_identity.transition_id_.fill(0x52);
  ExpectDomainReject(store.ReplaceFailoverTransition(
      "group-a", MetaFailoverTransitionRef{initial.transition_id_, 50},
      different_identity, 51));
  EXPECT_EQ(store.FindGroup("group-a")->failover_transition_, replacement);
}

TEST(MetaTopologyStore, ClearFailoverTransitionIsCasBoundAndIdempotent) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  const MetaFailoverTransition first = MakeUncontrolledTransition(0x51);
  ASSERT_TRUE(store.InstallFailoverTransition("group-a", first, 42).ok());

  ExpectDomainReject(store.ClearFailoverTransition(
      "group-a", MetaFailoverTransitionRef{first.transition_id_, 41}));
  ASSERT_TRUE(store.FindGroup("group-a")->failover_transition_.has_value());

  const MetaFailoverTransitionRef first_ref{first.transition_id_, 42};
  ASSERT_TRUE(store.ClearFailoverTransition("group-a", first_ref).ok());
  EXPECT_FALSE(store.FindGroup("group-a")->failover_transition_.has_value());
  ASSERT_TRUE(store.ClearFailoverTransition("group-a", first_ref).ok());

  const MetaFailoverTransition second = MakeUncontrolledTransition(0x52);
  ASSERT_TRUE(store.InstallFailoverTransition("group-a", second, 50).ok());
  ExpectDomainReject(store.ClearFailoverTransition("group-a", first_ref));
  EXPECT_EQ(store.FindGroup("group-a")->failover_transition_->transition_id_,
            second.transition_id_);
}

// ---------------------------------------------------------------------------
// AssignNodeToGroup / RemoveNodeFromGroup: one-node-one-group enforcement,
// group-record revision CAS, replay idempotency.
// ---------------------------------------------------------------------------

using lavik::meta::AssignNodeToGroup;
using lavik::meta::MetaGroupMember;
using lavik::meta::RemoveNodeFromGroup;

AssignNodeToGroup MakeAssign(const std::string& group_id, std::uint8_t seed,
                             MetaNodeRole role, std::uint64_t expected_revision,
                             std::uint64_t topology_epoch = 0) {
  AssignNodeToGroup cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.group_id_ = group_id;
  cmd.node_id_ = MakeNodeId(seed);
  cmd.assignment_id_.fill(seed);
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
            (MetaGroupMember{MakeNodeId(0x10),
                             [] {
                               lavik::meta::MetaAssignmentId id{};
                               id.fill(0x10);
                               return id;
                             }(),
                             MetaNodeRole::kPrimary}));
  EXPECT_EQ(store.FindGroupOfNode(MakeNodeId(0x10)),
            std::optional<std::string>("group-a"));
}

TEST(MetaTopologyStore, RemoveAndReaddRequiresANewAssignmentIdentity) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  const AssignNodeToGroup first =
      MakeAssign("group-a", 0x10, MetaNodeRole::kReplica, 1);
  ASSERT_TRUE(store.Apply(first).ok());
  ASSERT_TRUE(store.Apply(MakeRemove("group-a", 0x10, 2)).ok());

  AssignNodeToGroup stale_readd =
      MakeAssign("group-a", 0x10, MetaNodeRole::kReplica, 3);
  stale_readd.assignment_id_ = first.assignment_id_;
  ExpectDomainReject(store.Apply(stale_readd));

  AssignNodeToGroup fresh_readd = stale_readd;
  fresh_readd.assignment_id_.fill(0x11);
  ASSERT_TRUE(store.Apply(fresh_readd).ok());
  ASSERT_EQ(store.FindGroup("group-a")->members_.size(), 1u);
  EXPECT_EQ(store.FindGroup("group-a")->members_[0].assignment_id_,
            fresh_readd.assignment_id_);
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
  for (std::uint32_t i = 0; i < lavik::meta::kMaxMetaNodes; ++i) {
    AssignNodeToGroup cmd;
    cmd.request_id_ = MakeRequestId(0x20);
    cmd.group_id_ = "group-a";
    std::snprintf(hex4, sizeof(hex4), "%04x", i);
    cmd.node_id_ = MakeNodeId(0x20).substr(0, 36) + hex4;
    cmd.assignment_id_[0] = static_cast<std::uint8_t>(i);
    cmd.assignment_id_[1] = static_cast<std::uint8_t>(i >> 8);
    cmd.assignment_id_[15] = 1;
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
  over.assignment_id_.fill(0x21);
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

using lavik::meta::SetSlotMap;

SetSlotMap MakeSlotMap(std::vector<lavik::meta::MetaSlotAssignment> ranges,
                       std::uint64_t new_topology_epoch) {
  SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x90);
  cmd.ranges_ = std::move(ranges);
  cmd.new_topology_epoch_ = new_topology_epoch;
  return cmd;
}

// Two groups at epochs 1 and 2.
void MakeTwoGroups(MetaTopologyStore& store) {
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-b", 2)).ok());
}

TEST(MetaTopologyStore, SetSlotMapAssignsSlotsAndTopologyEpoch) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  const SetSlotMap cmd =
      MakeSlotMap({{0, 100, "group-a"}, {200, 300, "group-b"}},
                  /*new_topology_epoch=*/3);
  ASSERT_TRUE(store.Apply(cmd).ok());

  EXPECT_EQ(store.TopologyEpoch(), 3u);
  EXPECT_EQ(store.SlotOwner(0), std::optional<std::string>("group-a"));
  EXPECT_EQ(store.SlotOwner(100), std::optional<std::string>("group-a"));
  EXPECT_FALSE(store.SlotOwner(101).has_value());  // gap: unassigned
  EXPECT_EQ(store.SlotOwner(299), std::optional<std::string>("group-b"));
  EXPECT_EQ(store.SlotOwner(300), std::optional<std::string>("group-b"));
  EXPECT_FALSE(store.SlotOwner(301).has_value());
  EXPECT_FALSE(store.SlotOwner(lavik::meta::kMetaSlotCount)
                   .has_value());  // out-of-range query
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
      MakeSlotMap({{0, lavik::meta::kMetaSlotCount, "group-a"}}, 3)));
}

TEST(MetaTopologyStore, SetSlotMapRejectsUnknownGroups) {
  MetaTopologyStore store;
  MakeTwoGroups(store);
  ExpectDomainReject(store.Apply(MakeSlotMap({{0, 100, "group-ghost"}}, 3)));
  EXPECT_EQ(store.TopologyEpoch(), 2u);
  EXPECT_FALSE(store.SlotOwner(0).has_value());
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
  const SetSlotMap cmd = MakeSlotMap({{0, 100, "group-a"}}, 3);
  ASSERT_TRUE(store.Apply(cmd).ok());
  // Replay: slot map and topology epoch already carry this command's
  // effect -> idempotent accept.
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.TopologyEpoch(), 3u);

  // Same epoch, different content -> conflict rejection.
  ExpectDomainReject(store.Apply(MakeSlotMap({{0, 99, "group-a"}}, 3)));
  EXPECT_EQ(store.SlotOwner(100), std::optional<std::string>("group-a"));
}

// ---------------------------------------------------------------------------
// Granular primitives for the apply dispatcher (owner switch atomicity across
// the grant store is orchestrated there, not here).
// ---------------------------------------------------------------------------

TEST(MetaTopologyStore, GranularPrimitivesSetRecordFields) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());

  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetOwner(store, "group-a",
                                                            MakeNodeId(0x30))
                  .ok());
  ASSERT_TRUE(
      lavik::meta::MetaTopologyTestAccess::SetGroupTerm(store, "group-a", 7)
          .ok());
  lavik::meta::MetaHash256 manifest_digest{};
  manifest_digest.fill(0x55);
  ASSERT_TRUE(
      store.SetPopulationManifest("group-a", 555, manifest_digest).ok());
  ASSERT_TRUE(store.SetPartitionReplicationEpoch("group-a", 2).ok());

  const auto view = store.FindGroup("group-a");
  EXPECT_EQ(view->record_.owner_, MakeNodeId(0x30));
  EXPECT_EQ(view->record_.group_term_, 7u);
  EXPECT_EQ(view->record_.population_manifest_revision_, 555u);
  EXPECT_EQ(view->record_.population_manifest_digest_, manifest_digest);
  EXPECT_EQ(view->record_.partition_replication_epoch_, 2u);
  // Membership CAS revision untouched by record-field changes.
  EXPECT_EQ(view->revision_, 1u);

  // Setting the value already held is an idempotent no-op accept.
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetOwner(store, "group-a",
                                                            MakeNodeId(0x30))
                  .ok());
  ASSERT_TRUE(
      lavik::meta::MetaTopologyTestAccess::SetGroupTerm(store, "group-a", 7)
          .ok());
  ASSERT_TRUE(
      store.SetPopulationManifest("group-a", 555, manifest_digest).ok());
  ASSERT_TRUE(store.SetPartitionReplicationEpoch("group-a", 2).ok());

  // Unknown groups are rejected by every primitive.
  ExpectDomainReject(lavik::meta::MetaTopologyTestAccess::SetOwner(
      store, "group-ghost", MakeNodeId(0x30)));
  ExpectDomainReject(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
      store, "group-ghost", 7));
  ExpectDomainReject(
      store.SetPopulationManifest("group-ghost", 555, manifest_digest));
  ExpectDomainReject(store.SetPartitionReplicationEpoch("group-ghost", 2));
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
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetOwner(store, "group-a",
                                                            MakeNodeId(0x30))
                  .ok());
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
  EXPECT_TRUE(lavik::meta::MetaTopologyTestAccess::SetOwner(store, "group-a",
                                                            MakeNodeId(0x10))
                  .ok());
  EXPECT_TRUE(
      lavik::meta::MetaTopologyTestAccess::SetGroupTerm(store, "group-a", 7)
          .ok());
  lavik::meta::MetaHash256 manifest_digest{};
  manifest_digest.fill(0x55);
  EXPECT_TRUE(
      store.SetPopulationManifest("group-a", 555, manifest_digest).ok());
  EXPECT_TRUE(store.SetPartitionReplicationEpoch("group-a", 2).ok());
  EXPECT_TRUE(
      store
          .Apply(MakeSlotMap({{0, 100, "group-a"}, {200, 300, "group-b"}},
                             /*new_topology_epoch=*/6))
          .ok());
  return store;
}

void ExpectEqualTopology(const MetaTopologyStore& a,
                         const MetaTopologyStore& b) {
  EXPECT_EQ(a.ClusterLifecycle(), b.ClusterLifecycle());
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
            lavik::meta::kMetaTopologyStoreFormatVersion);

  const auto loaded = MetaTopologyStore::Deserialize(bytes);
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  ExpectEqualTopology(store, *loaded);
  // Fixed point: re-serializing the loaded store reproduces the bytes.
  EXPECT_EQ(loaded->Serialize(), bytes);
}

TEST(MetaTopologyStore, FailoverTransitionSerializationRoundTrip) {
  MetaTopologyStore store;
  ASSERT_TRUE(store.Apply(MakeCreateGroup("group-a", 1)).ok());
  MetaFailoverTransition transition = MakeUncontrolledTransition(0x51);
  ASSERT_TRUE(store.InstallFailoverTransition("group-a", transition, 42).ok());
  transition.revision_ = 42;

  const auto loaded = MetaTopologyStore::Deserialize(store.Serialize());
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  ASSERT_TRUE(loaded->FindGroup("group-a").has_value());
  EXPECT_EQ(loaded->FindGroup("group-a")->failover_transition_, transition);
  EXPECT_EQ(loaded->Serialize(), store.Serialize());
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
  EXPECT_EQ(bytes[0], '\x01');
  EXPECT_EQ(bytes[1], '\x00');
  std::string old_development_version = bytes;
  old_development_version[0] = '\x02';
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(old_development_version).status());
}

// Hand-builds a topology blob from parts, so in-byte invariant violations
// can be expressed. Layout mirrors the store's serialization contract.
struct TopologyBlobGroup {
  std::string group_id;
  std::string owner;
  std::uint64_t revision = 1;
  std::vector<MetaGroupMember> members;
  std::optional<std::string> encoded_failover_transition;
};

std::string MakeTopologyBlob(
    std::uint64_t topology_epoch, const std::vector<TopologyBlobGroup>& groups,
    const std::vector<lavik::meta::MetaSlotAssignment>& runs) {
  lavik::meta::MetaWriter w;
  w.WriteU16(lavik::meta::kMetaTopologyStoreFormatVersion);
  w.WriteU8(static_cast<std::uint8_t>(MetaClusterLifecycle::kUninitialized));
  lavik::meta::WriteFixedArray(w, MetaOperationId{});
  w.WriteU64(0);
  w.WriteString("");
  w.WriteU64(topology_epoch);
  w.WriteCount(static_cast<std::uint32_t>(groups.size()));
  for (const TopologyBlobGroup& group : groups) {
    w.WriteString(group.group_id);
    w.WriteString(group.owner);
    w.WriteU64(0);       // group_term
    w.WriteBool(false);  // fenced
    w.WriteBool(false);  // no activation action
    w.WriteU64(0);       // population_manifest_revision
    lavik::meta::WriteFixedArray(w, lavik::meta::MetaHash256{});
    w.WriteU64(0);  // partition_replication_epoch
    w.WriteU64(group.revision);
    w.WriteOptional(
        group.encoded_failover_transition,
        [](lavik::meta::MetaWriter& ww, const std::string& encoded) {
          // The bytes are deliberately supplied by the caller rather than
          // re-encoded here so corrupt transition invariants can be tested.
          ww.WriteRaw(encoded);
        });
    w.WriteList(group.members,
                [](lavik::meta::MetaWriter& ww, const MetaGroupMember& member) {
                  ww.WriteString(member.node_id_);
                  lavik::meta::WriteFixedArray(ww, member.assignment_id_);
                  ww.WriteU8(static_cast<std::uint8_t>(member.role_));
                });
  }
  std::map<std::string, lavik::meta::MetaAssignmentId> assignment_history;
  for (const TopologyBlobGroup& group : groups) {
    for (const MetaGroupMember& member : group.members) {
      assignment_history.emplace(member.node_id_, member.assignment_id_);
    }
  }
  w.WriteCount(static_cast<std::uint32_t>(assignment_history.size()));
  for (const auto& [node_id, assignment_id] : assignment_history) {
    w.WriteString(node_id);
    lavik::meta::WriteFixedArray(w, assignment_id);
  }
  w.WriteList(runs, [](lavik::meta::MetaWriter& ww,
                       const lavik::meta::MetaSlotAssignment& run) {
    ww.WriteU16(run.first_slot_);
    ww.WriteU16(run.last_slot_);
    ww.WriteString(run.group_id_);
  });
  return w.buffer();
}

TEST(MetaTopologyStore, CurrentRawLayoutFixtureDecodes) {
  const auto loaded = MetaTopologyStore::Deserialize(
      MakeTopologyBlob(1, {TopologyBlobGroup{"group-a"}}, {}));
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  ASSERT_TRUE(loaded->FindGroup("group-a").has_value());
  EXPECT_FALSE(loaded->FindGroup("group-a")->failover_transition_.has_value());
}

TEST(MetaTopologyStore, DeserializeRejectsInvalidFailoverTransition) {
  MetaFailoverTransition transition = MakeUncontrolledTransition(0x51);
  transition.revision_ = 42;
  lavik::meta::MetaWriter transition_writer;
  ASSERT_TRUE(
      lavik::meta::WriteMetaFailoverTransition(transition_writer, transition)
          .ok());
  std::string encoded = transition_writer.TakeBuffer();
  ASSERT_GT(encoded.size(), 24u);
  encoded[24] = '\x7f';  // unknown MetaFailoverMode tag

  TopologyBlobGroup group{"group-a"};
  group.encoded_failover_transition = std::move(encoded);
  ExpectStoreFailStop(MetaTopologyStore::Deserialize(
                          MakeTopologyBlob(1, {std::move(group)}, {}))
                          .status());
}

TEST(MetaTopologyStore, DeserializeRejectsInvariantViolations) {
  const std::string node_a = MakeNodeId(0x40);
  lavik::meta::MetaAssignmentId assignment_a{};
  assignment_a.fill(0x40);
  lavik::meta::MetaAssignmentId assignment_b{};
  assignment_b.fill(0x41);

  // A node appearing in two groups breaks one-node-one-group.
  ExpectStoreFailStop(
      MetaTopologyStore::Deserialize(
          MakeTopologyBlob(
              2,
              {TopologyBlobGroup{"group-a",
                                 "",
                                 1,
                                 {MetaGroupMember{node_a, assignment_a,
                                                  MetaNodeRole::kPrimary}}},
               TopologyBlobGroup{"group-b",
                                 "",
                                 1,
                                 {MetaGroupMember{node_a, assignment_a,
                                                  MetaNodeRole::kReplica}}}},
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
              {TopologyBlobGroup{"group-a",
                                 "",
                                 1,
                                 {MetaGroupMember{node_a, assignment_a,
                                                  MetaNodeRole::kPrimary},
                                  MetaGroupMember{node_a, assignment_b,
                                                  MetaNodeRole::kReplica}}}},
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
                           {{0, lavik::meta::kMetaSlotCount, "group-a"}}))
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

using lavik::meta::kAuthorityLeasePolicyId;
using lavik::meta::kAutomaticUncontrolledFailoverPolicyId;
using lavik::meta::MetaPolicyStore;
using lavik::meta::PutPolicy;

PutPolicy MakePut(const std::string& policy_id, std::uint64_t version,
                  std::string content) {
  PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(0xA0);
  cmd.policy_id_ = policy_id;
  cmd.version_ = version;
  cmd.content_ = std::move(content);
  return cmd;
}

std::string AutomaticPolicy(std::uint64_t suspect_after_ms) {
  return absl::StrCat(
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":)",
      suspect_after_ms, "}");
}

std::string LeasePolicy(std::uint64_t duration_ms) {
  return absl::StrCat(R"({"kind":"authority-lease-v1","duration_ms":)",
                      duration_ms, "}");
}

TEST(MetaPolicyStore, AutomaticFailoverNeedsOnlySuspectThreshold) {
  MetaPolicyStore store;
  ASSERT_TRUE(
      store
          .Apply(MakePut(
              std::string(kAutomaticUncontrolledFailoverPolicyId), 1,
              R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})"))
          .ok());
  ASSERT_TRUE(store.CurrentAutomaticUncontrolledFailover().has_value());
  EXPECT_EQ(store.CurrentAutomaticUncontrolledFailover()->suspect_after_ms_,
            5000u);
}

TEST(MetaPolicyStore,
     RecoveryBudgetAcceptsZeroAndSurvivesAlongsideOtherPolicies) {
  MetaPolicyStore store;
  const std::string id = "lavik.candidate-recovery-v1";
  ASSERT_TRUE(store
                  .Apply(MakePut(std::string(kAuthorityLeasePolicyId), 1,
                                 LeasePolicy(3000)))
                  .ok());
  ASSERT_TRUE(
      store
          .Apply(MakePut(std::string(kAutomaticUncontrolledFailoverPolicyId), 1,
                         AutomaticPolicy(5000)))
          .ok());
  ASSERT_TRUE(
      store
          .Apply(MakePut(
              id, 1, R"({"kind":"candidate-recovery-v1","budget_ms":2000})"))
          .ok());
  ASSERT_TRUE(
      store
          .Apply(MakePut(id, 2,
                         R"({"kind":"candidate-recovery-v1","budget_ms":0})"))
          .ok());
  auto restored = MetaPolicyStore::Deserialize(store.Serialize());
  ASSERT_TRUE(restored.ok()) << restored.status();
  ASSERT_EQ(restored->PolicyCount(), 3u);
  EXPECT_EQ(restored->FindVersion(id, 2)->content_,
            R"({"kind":"candidate-recovery-v1","budget_ms":0})");
  EXPECT_EQ(restored->CurrentAutomaticUncontrolledFailover()->suspect_after_ms_,
            5000u);
  EXPECT_EQ(restored->CurrentAuthorityLease()->duration_ms_, 3000u);
}

TEST(MetaPolicyStore, StoresTypedPoliciesAndReturnsOriginalRawBytes) {
  MetaPolicyStore store;
  const std::string reordered =
      R"({"suspect_after_ms":7000,"kind":"automatic-uncontrolled-failover-v1"})";
  ASSERT_TRUE(
      store
          .Apply(MakePut(std::string(kAutomaticUncontrolledFailoverPolicyId), 1,
                         reordered))
          .ok());
  ASSERT_TRUE(store
                  .Apply(MakePut(std::string(kAuthorityLeasePolicyId), 1,
                                 LeasePolicy(3000)))
                  .ok());

  EXPECT_TRUE(
      store.FindVersion(std::string(kAutomaticUncontrolledFailoverPolicyId), 1)
          .has_value());
  EXPECT_FALSE(
      store.FindVersion(std::string(kAutomaticUncontrolledFailoverPolicyId), 2)
          .has_value());
  EXPECT_EQ(
      store.LatestVersion(std::string(kAutomaticUncontrolledFailoverPolicyId)),
      std::optional<std::uint64_t>(1));
  EXPECT_EQ(store.PolicyCount(), 2u);

  const auto view =
      store.FindVersion(std::string(kAutomaticUncontrolledFailoverPolicyId), 1);
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->policy_id_, kAutomaticUncontrolledFailoverPolicyId);
  EXPECT_EQ(view->version_, 1u);
  EXPECT_EQ(view->content_, reordered);

  const auto automatic = store.CurrentAutomaticUncontrolledFailover();
  ASSERT_TRUE(automatic.has_value());
  EXPECT_EQ(automatic->version_, 1u);
  EXPECT_EQ(automatic->suspect_after_ms_, 7000u);

  const auto lease = store.CurrentAuthorityLease();
  ASSERT_TRUE(lease.has_value());
  EXPECT_EQ(lease->version_, 1u);
  EXPECT_EQ(lease->duration_ms_, 3000u);
}

TEST(MetaPolicyStore, RejectsUnregisteredPolicyAndInvalidCommandFields) {
  MetaPolicyStore store;
  {
    PutPolicy cmd = MakePut("", 1, AutomaticPolicy(5000));
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    PutPolicy cmd = MakePut("unknown-policy", 1, AutomaticPolicy(5000));
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    PutPolicy cmd =
        MakePut(std::string(lavik::meta::kMaxMetaPolicyIdBytes + 1, 'p'), 1,
                AutomaticPolicy(5000));
    ExpectDomainReject(store.Apply(cmd));
  }
  {
    PutPolicy cmd;
    cmd.request_id_ = MakeRequestId(0xA1);
    cmd.policy_id_ = "p";
    cmd.version_ = 1;
    cmd.content_ = std::string(lavik::meta::kMaxMetaPayloadBytes + 1, 'x');
    ExpectDomainReject(store.Apply(cmd));
  }
  EXPECT_EQ(store.PolicyCount(), 0u);
}

TEST(MetaPolicyStore, AutomaticFailoverSchemaIsStrict) {
  const std::vector<std::string> invalid = {
      "",
      R"({})",
      R"({"kind":"automatic-uncontrolled-failover-v1"})",
      R"({"kind":"wrong","suspect_after_ms":5000})",
      R"({"kind":"automatic-uncontrolled-failover-v1","enabled":true,"suspect_after_ms":5000})",
      R"({"kind":"automatic-uncontrolled-failover-v1","enabled":false,"suspect_after_ms":5000})",
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":"5000"})",
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":999})",
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":86400001})",
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":18446744073709551616})",
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000,"suspect_after_ms":6000})",
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000,"extra":1})",
      R"({ "kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})",
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000} trailing)",
  };
  for (const std::string& content : invalid) {
    SCOPED_TRACE(content);
    MetaPolicyStore store;
    ExpectDomainReject(store.Apply(MakePut(
        std::string(kAutomaticUncontrolledFailoverPolicyId), 1, content)));
  }
}

TEST(MetaPolicyStore, AuthorityLeaseSchemaIsStrict) {
  const std::vector<std::string> invalid = {
      R"({})",
      R"({"kind":"wrong","duration_ms":5000})",
      R"({"kind":"authority-lease-v1"})",
      R"({"kind":"authority-lease-v1","duration_ms":99})",
      R"({"kind":"authority-lease-v1","duration_ms":86400001})",
      R"({"kind":"authority-lease-v1","duration_ms":-1})",
      R"({"kind":"authority-lease-v1","duration_ms":1.5})",
      R"({"kind":"authority-lease-v1","duration_ms":05000})",
      R"({"kind":"authority-lease-v1","duration_ms":5000,"duration_ms":6000})",
      R"({"kind":"authority-lease-v1","duration_ms":5000,"extra":true})",
  };
  for (const std::string& content : invalid) {
    SCOPED_TRACE(content);
    MetaPolicyStore store;
    ExpectDomainReject(
        store.Apply(MakePut(std::string(kAuthorityLeasePolicyId), 1, content)));
  }
}

TEST(MetaPolicyStore, PutPolicyReplayIsIdempotentAccept) {
  MetaPolicyStore store;
  const PutPolicy cmd =
      MakePut(std::string(kAutomaticUncontrolledFailoverPolicyId), 1,
              AutomaticPolicy(5000));
  ASSERT_TRUE(store.Apply(cmd).ok());
  ASSERT_TRUE(store.Apply(cmd).ok());
  EXPECT_EQ(store.PolicyCount(), 1u);
  EXPECT_EQ(store.TotalContentBytes(), cmd.content_.size());
}

TEST(MetaPolicyStore, PutPolicySameVersionDifferentContentRejected) {
  MetaPolicyStore store;
  const std::string policy_id(kAutomaticUncontrolledFailoverPolicyId);
  ASSERT_TRUE(store.Apply(MakePut(policy_id, 1, AutomaticPolicy(5000))).ok());
  ExpectDomainReject(store.Apply(MakePut(policy_id, 1, AutomaticPolicy(6000))));
  EXPECT_EQ(store.FindVersion(policy_id, 1)->content_, AutomaticPolicy(5000));
}

TEST(MetaPolicyStore, PutPolicyRequiresFirstAndConsecutiveVersions) {
  MetaPolicyStore store;
  const std::string policy_id(kAutomaticUncontrolledFailoverPolicyId);
  ExpectDomainReject(store.Apply(MakePut(policy_id, 2, AutomaticPolicy(5000))));
  ASSERT_TRUE(store.Apply(MakePut(policy_id, 1, AutomaticPolicy(5000))).ok());
  ExpectDomainReject(store.Apply(MakePut(policy_id, 3, AutomaticPolicy(7000))));
  ASSERT_TRUE(store.Apply(MakePut(policy_id, 2, AutomaticPolicy(6000))).ok());
  ExpectDomainReject(store.Apply(MakePut(policy_id, 1, AutomaticPolicy(4000))));
  EXPECT_EQ(store.LatestVersion(policy_id), std::optional<std::uint64_t>(2));
}

TEST(MetaPolicyStore, PutPolicyEvictsOldestVersionAfterHistoryCap) {
  MetaPolicyStore store;
  const std::string policy_id(kAutomaticUncontrolledFailoverPolicyId);
  for (std::uint32_t v = 1; v <= lavik::meta::kMaxMetaPolicyVersionsPerPolicy;
       ++v) {
    ASSERT_TRUE(
        store.Apply(MakePut(policy_id, v, AutomaticPolicy(1000 + v))).ok())
        << v;
  }
  const std::uint64_t next = lavik::meta::kMaxMetaPolicyVersionsPerPolicy + 1;
  ASSERT_TRUE(
      store.Apply(MakePut(policy_id, next, AutomaticPolicy(1000 + next))).ok());
  EXPECT_FALSE(store.FindVersion(policy_id, 1).has_value());
  EXPECT_TRUE(store.FindVersion(policy_id, 2).has_value());
  EXPECT_TRUE(store.FindVersion(policy_id, next).has_value());
  EXPECT_EQ(store.Versions().size(),
            lavik::meta::kMaxMetaPolicyVersionsPerPolicy);
}

// ---------------------------------------------------------------------------
// Policy serialization: u16 schema_version envelope, deterministic sorted
// output and strict fail-stop decode of the new hash-free current format.
// ---------------------------------------------------------------------------

MetaPolicyStore MakePopulatedPolicies() {
  MetaPolicyStore store;
  EXPECT_TRUE(
      store
          .Apply(MakePut(std::string(kAutomaticUncontrolledFailoverPolicyId), 1,
                         AutomaticPolicy(5000)))
          .ok());
  EXPECT_TRUE(store
                  .Apply(MakePut(std::string(kAuthorityLeasePolicyId), 1,
                                 LeasePolicy(5000)))
                  .ok());
  return store;
}

TEST(MetaPolicyStore, SerializationRoundTrip) {
  const MetaPolicyStore store = MakePopulatedPolicies();
  const std::string bytes = store.Serialize();
  ASSERT_GE(bytes.size(), 2u);
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
  EXPECT_EQ(static_cast<std::uint16_t>(p[0] | (p[1] << 8)),
            lavik::meta::kMetaFormatVersion);

  const auto loaded = MetaPolicyStore::Deserialize(bytes);
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  EXPECT_EQ(loaded->PolicyCount(), 2u);
  EXPECT_EQ(loaded->TotalContentBytes(), store.TotalContentBytes());
  EXPECT_EQ(loaded->Versions(), store.Versions());
  EXPECT_EQ(loaded->CurrentAutomaticUncontrolledFailover(),
            store.CurrentAutomaticUncontrolledFailover());
  EXPECT_EQ(loaded->CurrentAuthorityLease(), store.CurrentAuthorityLease());
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
  const PutPolicy automatic =
      MakePut(std::string(kAutomaticUncontrolledFailoverPolicyId), 1,
              AutomaticPolicy(5000));
  const PutPolicy lease =
      MakePut(std::string(kAuthorityLeasePolicyId), 1, LeasePolicy(5000));
  ASSERT_TRUE(a.Apply(automatic).ok());
  ASSERT_TRUE(a.Apply(lease).ok());
  ASSERT_TRUE(b.Apply(lease).ok());
  ASSERT_TRUE(b.Apply(automatic).ok());
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

// Hand-builds a policy blob using the current hash-free snapshot layout.
struct PolicyBlobVersion {
  std::uint64_t version;
  std::string content;
};

std::string MakePolicyBlob(
    const std::vector<std::pair<std::string, std::vector<PolicyBlobVersion>>>&
        policies) {
  lavik::meta::MetaWriter w;
  w.WriteU16(lavik::meta::kMetaFormatVersion);
  w.WriteCount(static_cast<std::uint32_t>(policies.size()));
  for (const auto& [policy_id, versions] : policies) {
    w.WriteString(policy_id);
    w.WriteCount(static_cast<std::uint32_t>(versions.size()));
    for (const PolicyBlobVersion& version : versions) {
      w.WriteU64(version.version);
      w.WriteString(version.content);
    }
  }
  return w.buffer();
}

TEST(MetaPolicyStore, DeserializeRejectsInvariantViolations) {
  // Version count over the per-policy cap.
  {
    std::vector<PolicyBlobVersion> versions;
    for (std::uint32_t v = 1;
         v <= lavik::meta::kMaxMetaPolicyVersionsPerPolicy + 1; ++v) {
      versions.push_back(PolicyBlobVersion{v, AutomaticPolicy(1000 + v)});
    }
    ExpectStoreFailStop(
        MetaPolicyStore::Deserialize(
            MakePolicyBlob(
                {{std::string(kAutomaticUncontrolledFailoverPolicyId),
                  versions}}))
            .status());
  }
  // Duplicate version within one policy.
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob({{std::string(kAuthorityLeasePolicyId),
                           {PolicyBlobVersion{1, LeasePolicy(5000)},
                            PolicyBlobVersion{1, LeasePolicy(6000)}}}}))
          .status());
  // A gap, malformed content, unknown id, empty id, or zero-version family.
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob({{std::string(kAuthorityLeasePolicyId),
                           {PolicyBlobVersion{1, LeasePolicy(5000)},
                            PolicyBlobVersion{3, LeasePolicy(6000)}}}}))
          .status());
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob({{std::string(kAuthorityLeasePolicyId),
                           {PolicyBlobVersion{1, "not-json"}}}}))
          .status());
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob(
              {{"unknown", {PolicyBlobVersion{1, LeasePolicy(5000)}}}}))
          .status());
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob({{"", {PolicyBlobVersion{1, LeasePolicy(5000)}}}}))
          .status());
  ExpectStoreFailStop(
      MetaPolicyStore::Deserialize(
          MakePolicyBlob({{std::string(kAuthorityLeasePolicyId), {}}}))
          .status());
}

TEST(MetaPopulationManifestStore,
     StoresCanonicalImmutableDocumentsAndRoundTrips) {
  using lavik::meta::MetaPopulationManifestEntry;
  using lavik::meta::MetaPopulationManifestStore;
  using lavik::meta::PutPopulationManifest;

  PutPopulationManifest put;
  put.entries_ = {MetaPopulationManifestEntry{1, 10},
                  MetaPopulationManifestEntry{7, 22}};
  put.manifest_digest_ =
      MetaPopulationManifestStore::CanonicalDigest(put.entries_);

  MetaPopulationManifestStore store;
  ASSERT_TRUE(store.Put(put).ok());
  ASSERT_TRUE(store.Put(put).ok());
  ASSERT_EQ(store.Size(), 1u);
  EXPECT_EQ(store.Find(put.manifest_digest_)->entries_, put.entries_);

  auto restored = MetaPopulationManifestStore::Deserialize(store.Serialize());
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->Find(put.manifest_digest_),
            store.Find(put.manifest_digest_));
}

TEST(MetaPopulationManifestStore, RejectsNonCanonicalOrMismatchedContent) {
  using lavik::meta::MetaPopulationManifestEntry;
  using lavik::meta::MetaPopulationManifestStore;
  using lavik::meta::PutPopulationManifest;

  MetaPopulationManifestStore store;
  PutPopulationManifest unsorted;
  unsorted.entries_ = {MetaPopulationManifestEntry{7, 22},
                       MetaPopulationManifestEntry{1, 10}};
  unsorted.manifest_digest_ =
      MetaPopulationManifestStore::CanonicalDigest(unsorted.entries_);
  ExpectDomainReject(store.Put(unsorted));

  PutPopulationManifest duplicate;
  duplicate.entries_ = {MetaPopulationManifestEntry{1, 10},
                        MetaPopulationManifestEntry{1, 11}};
  duplicate.manifest_digest_ =
      MetaPopulationManifestStore::CanonicalDigest(duplicate.entries_);
  ExpectDomainReject(store.Put(duplicate));

  PutPopulationManifest mismatched;
  mismatched.entries_ = {MetaPopulationManifestEntry{1, 10}};
  ExpectDomainReject(store.Put(mismatched));

  PutPopulationManifest out_of_range;
  out_of_range.entries_ = {
      MetaPopulationManifestEntry{lavik::meta::kMetaSlotCount, 1}};
  out_of_range.manifest_digest_ =
      MetaPopulationManifestStore::CanonicalDigest(out_of_range.entries_);
  ExpectDomainReject(store.Put(out_of_range));

  PutPopulationManifest zero_epoch;
  zero_epoch.entries_ = {MetaPopulationManifestEntry{1, 0}};
  zero_epoch.manifest_digest_ =
      MetaPopulationManifestStore::CanonicalDigest(zero_epoch.entries_);
  ExpectDomainReject(store.Put(zero_epoch));
}

}  // namespace
