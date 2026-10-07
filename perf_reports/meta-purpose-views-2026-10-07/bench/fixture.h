/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "lavik/cluster/control_protocol.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_machine.h"
namespace bench {
namespace m = lavik::meta;
template <std::size_t N>
std::array<std::uint8_t, N> Id(std::uint64_t value) {
  std::array<std::uint8_t, N> id{};
  for (int i = 0; i < 8; ++i)
    id[N - 1 - i] = static_cast<std::uint8_t>(value >> (i * 8));
  return id;
}
inline std::string Node(std::uint64_t value) {
  std::ostringstream s;
  s << std::hex << std::setfill('0') << std::setw(40) << value;
  return s.str();
}
inline void Check(const absl::Status& s) {
  if (!s.ok()) throw std::runtime_error(std::string(s.message()));
}
struct Shape {
  std::string name;
  int nodes = 128, groups = 32, transitions = 0, operations = 1, op_bytes = 0;
  int selected_bytes = 1024;
  int meta_members = 0, slot_ranges = 0;
  int archives = 0, archive_bytes = 0, policy_versions = 1, manifests = 0,
      entries = 0, audit = 0, directives = 0;
};
inline m::MetaStores Fixture(const Shape& shape) {
  m::MetaStores s;
  Check(s.topology_.BeginClusterCreate(Id<16>(99999), 1,
                                       lavik::ClientMode::kCluster));
  Check(s.topology_.CompleteClusterCreate(Id<16>(99999)));
  for (int v = 1; v <= shape.policy_versions; ++v) {
    m::PutPolicy p;
    p.version_ = v;
    p.policy_id_ = m::kAutomaticUncontrolledFailoverPolicyId;
    p.content_ =
        "{\"kind\":\"automatic-uncontrolled-failover-v1\",\"suspect_after_"
        "ms\":" +
        std::to_string(5000) + "}";
    Check(s.policy_.Apply(p));
    p.policy_id_ = m::kAuthorityLeasePolicyId;
    p.content_ = "{\"kind\":\"authority-lease-v1\",\"duration_ms\":" +
                 std::to_string(5000) + "}";
    Check(s.policy_.Apply(p));
    p.policy_id_ = m::kCandidateRecoveryPolicyId;
    p.content_ = "{\"kind\":\"candidate-recovery-v1\",\"budget_ms\":" +
                 std::to_string(2000) + "}";
    Check(s.policy_.Apply(p));
  }
  for (int i = 1; i <= shape.meta_members; ++i) {
    m::BindMetaMember b;
    b.server_id_ = i;
    b.principal_ = "lavik://meta/" + std::to_string(i);
    b.data_control_endpoint_ = "127.0.0.1:" + std::to_string(20000 + i);
    b.ctl_endpoint_ = "127.0.0.1:" + std::to_string(30000 + i);
    Check(s.identity_.Apply(b));
  }
  for (int n = 0; n < shape.nodes; ++n) {
    m::RegisterNode cmd;
    cmd.node_id_ = Node(n + 1);
    cmd.principal_ = "lavik://node/" + cmd.node_id_;
    cmd.endpoints_ = {"tcp://127.0.0.1:" + std::to_string(10000 + n)};
    cmd.role_ = n % (shape.nodes / shape.groups) == 0
                    ? m::MetaNodeRole::kPrimary
                    : m::MetaNodeRole::kReplica;
    Check(s.identity_.Apply(cmd));
  }
  for (int g = 0; g < shape.groups; ++g) {
    std::string group = "g" + std::to_string(g);
    m::CreateGroup cmd;
    cmd.group_id_ = group;
    cmd.new_topology_epoch_ = s.topology_.TopologyEpoch() + 1;
    Check(s.topology_.Apply(cmd));
    int first = g * (shape.nodes / shape.groups),
        end = (g + 1) * (shape.nodes / shape.groups);
    for (int n = first; n < end; ++n) {
      m::AssignNodeToGroup a;
      a.group_id_ = group;
      a.node_id_ = Node(n + 1);
      a.assignment_id_ = Id<16>(n + 1);
      a.role_ =
          n == first ? m::MetaNodeRole::kPrimary : m::MetaNodeRole::kReplica;
      a.expected_revision_ = s.topology_.FindGroup(group)->revision_;
      a.new_topology_epoch_ = s.topology_.TopologyEpoch() + 1;
      Check(s.topology_.Apply(a));
    }
    m::BeginGroupTerm term;
    term.group_id_ = group;
    term.new_term_ = 1;
    Check(s.topology_.BeginGroupTerm(term));
    m::ActivateAuthority grant;
    grant.group_id_ = group;
    grant.expected_term_ = 1;
    grant.new_owner_ = Node(first + 1);
    Check(s.topology_.ActivateAuthority(grant));
    if (g < shape.transitions) {
      term.expected_term_ = 1;
      term.new_term_ = 2;
      Check(s.topology_.BeginGroupTerm(term));
      m::MetaFailoverTransition t;
      t.transition_id_ = Id<16>(g + 10000);
      t.target_term_ = 2;
      m::MetaFailoverCandidateAction action;
      action.action_id_ = Id<16>(g + 20000);
      action.candidate_ = {Node(first + 2), Id<16>(first + 2),
                           Id<20>(first + 2)};
      action.domain_ = {1,
                        Node(first + 1),
                        Id<16>(first + 1),
                        Id<20>(first + 1),
                        Id<20>(30000 + g),
                        2};
      t.candidate_action_ = action;
      Check(s.topology_.InstallFailoverTransition(group, t, 100 + g));
    }
  }
  if (shape.slot_ranges > 0) {
    m::SetSlotMap slots;
    slots.new_topology_epoch_ = s.topology_.TopologyEpoch() + 1;
    for (int i = 0; i < shape.slot_ranges; ++i)
      slots.ranges_.push_back({static_cast<std::uint16_t>(i),
                               static_cast<std::uint16_t>(i),
                               "g" + std::to_string(i % shape.groups)});
    Check(s.topology_.Apply(slots));
  }
  for (int i = 0; i < shape.operations + shape.archives; ++i) {
    bool archive = i >= shape.operations;
    m::SubmitOperation op;
    op.operation_id_ = Id<16>(1000 + i);
    op.kind_ = "maintenance";
    op.intent_ = std::string(archive  ? 0
                             : i == 0 ? shape.selected_bytes
                                      : shape.op_bytes,
                             'x');
    op.intent_hash_ = m::MetaSha256(op.intent_);
    auto result = s.operation_.SubmitOperation(op, 1000 + i);
    if (!result.ok()) Check(result.status());
    if (!archive && i == 0 && shape.directives > 0) {
      m::TransitionOperationPhase t;
      t.operation_id_ = op.operation_id_;
      t.kind_phase_blob_ = "running";
      for (int n = 0; n < std::max(1, shape.directives); ++n) {
        int target = 4 * (n / 3) + 2 + n % 3, source = 4 * (n / 3) + 1;
        m::MetaDirectiveSpec d;
        d.directive_id_ = Id<16>(n + 1);
        d.attempt_id_ = Id<16>(n + 100);
        d.assignment_id_ = Id<16>(target);
        d.recipient_node_id_ = Node(target);
        d.target_node_id_ = Node(target);
        d.target_boot_id_ = Id<20>(4);
        d.source_node_id_ = Node(source);
        d.source_assignment_id_ = Id<16>(source);
        d.source_boot_id_ = Id<20>(5);
        d.source_replication_history_id_ = Id<20>(6);
        d.group_id_ = "g" + std::to_string(n / 3);
        d.group_term_ = 1;
        d.partition_replication_epoch_ =
            s.topology_.FindGroup(d.group_id_)
                ->record_.partition_replication_epoch_;
        d.kind_ = "rebuild";
        d.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});
        t.current_directives_.push_back(d);
      }
      Check(s.operation_.TransitionOperationPhase(t, 2000));
      for (const auto& d : t.current_directives_) {
        m::CommitDirectiveResult c;
        c.operation_id_ = op.operation_id_;
        c.directive_id_ = d.directive_id_;
        c.attempt_id_ = d.attempt_id_;
        c.directive_revision_ = 2000;
        c.recipient_node_id_ = d.recipient_node_id_;
        c.recipient_boot_id_ = d.target_boot_id_;
        c.assignment_id_ = d.assignment_id_;
        c.status_ = m::MetaDirectiveResultStatus::kSucceeded;
        c.result_ = "done";
        Check(s.operation_.CommitDirectiveResult(c, 2001));
      }
    }
    if (archive) {
      m::CompleteOperation done;
      done.operation_id_ = op.operation_id_;
      done.result_ = std::string(shape.archive_bytes, 'y');
      Check(s.operation_.CompleteOperation(done));
      m::ArchiveOperations a;
      a.operation_seqs_ = {static_cast<std::uint64_t>(1000 + i)};
      Check(s.operation_.ArchiveOperations(a));
    }
  }
  for (int i = 0; i < shape.manifests; ++i) {
    m::PutPopulationManifest p;
    for (int e = 0; e < shape.entries; ++e)
      p.entries_.push_back(
          {static_cast<std::uint32_t>(e), static_cast<std::uint64_t>(i + 1)});
    p.manifest_digest_ =
        m::MetaPopulationManifestStore::CanonicalDigest(p.entries_);
    Check(s.population_manifest_.Put(p));
  }
  for (int i = 1; i <= shape.audit; ++i) {
    m::MetaAuditRecord a;
    a.log_index_ = i;
    a.actor_principal_ = "lavik://bench";
    a.command_summary_ = std::string(128, 'a');
    a.readable_time_ = "2026-10-05T00:00:00.000Z";
    a.verdict_ = m::MetaAuditVerdict::kAccepted;
    Check(s.audit_.Append(a));
  }
  return s;
}
}  // namespace bench
