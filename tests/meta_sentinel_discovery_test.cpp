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

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/meta/sentinel_discovery.h"

namespace lavik::meta {
namespace {

constexpr std::int64_t kNowUnixMs = 1'000'000;
constexpr std::uint32_t kObservationTtlMs = 500;
constexpr std::uint64_t kManifestRevision = 5;
constexpr std::uint64_t kPartitionEpoch = 9;

const std::string kOwnerId(kMetaNodeIdBytes, 'a');
const std::string kReplicaOneId(kMetaNodeIdBytes, 'b');
const std::string kReplicaTwoId(kMetaNodeIdBytes, 'c');

// Each fixture uniquely owns a view originally allocated as mutable. Tests
// edit it only between synchronous discovery calls; production cuts never
// mutate a published view.
MetaCommittedStatusView& MutableCommitted(MetaDiscoveryCut& cut) {
  EXPECT_EQ(cut.committed_.use_count(), 1);
  return *std::const_pointer_cast<MetaCommittedStatusView>(cut.committed_);
}

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t fill) {
  std::array<std::uint8_t, N> out{};
  out.fill(fill);
  return out;
}

MetaNodeRecord NodeRecord(const std::string& node_id,
                          std::vector<std::string> endpoints) {
  return MetaNodeRecord{.node_id_ = node_id,
                        .principal_ = "lavik://data/" + node_id,
                        .endpoints_ = std::move(endpoints),
                        .role_ = MetaNodeRole::kPrimary,
                        .revision_ = 1,
                        .retired_ = false};
}

// One live session with green in-TTL health and a projection anchor matching
// the committed term 7, the member's own assignment, and the committed
// manifest revision/digest plus partition replication epoch.
MetaDataControlRuntimeNode LiveRuntimeNode(const std::string& node_id,
                                           const MetaAssignmentId& assignment) {
  MetaDataControlRuntimeNode node;
  node.node_id_ = node_id;
  node.groups_ = {MetaDataControlRuntimeGroup{
      .group_id_ = "g1",
      .assignment_id_ = assignment,
      .group_term_ = 7,
      .manifest_revision_ = kManifestRevision,
      .manifest_digest_ = Bytes<32>(0x11),
      .partition_replication_epoch_ = kPartitionEpoch,
  }};
  node.health_ = cluster::control::HeartbeatHealth{.storage_ready = true,
                                                   .population_ready = true,
                                                   .draining = false,
                                                   .active_groups = 1,
                                                   .summary = {}};
  node.health_received_unix_ms_ = kNowUnixMs;
  return node;
}

// The canonical healthy cut: a Created Single-mode cluster with one Group "g1"
// owned by kOwnerId at term 7, two committed replicas, live sessions for all
// three nodes, and a Healthy diagnostics cut whose identity matches the
// runtime registry.
MetaDiscoveryCut MakeCut() {
  MetaDiscoveryCut cut;
  cut.committed_ = std::make_shared<MetaCommittedStatusView>();
  cut.now_unix_ms_ = kNowUnixMs;
  cut.observation_ttl_ms_ = kObservationTtlMs;
  MutableCommitted(cut).applied_index_ = 100;
  MutableCommitted(cut).cluster_lifecycle_.state_ =
      MetaClusterLifecycle::kCreated;
  MutableCommitted(cut).cluster_lifecycle_.client_mode_ = ClientMode::kSingle;

  MetaCommittedStatusGroup group;
  group.topology_.group_id_ = "g1";
  group.topology_.record_.owner_ = kOwnerId;
  group.topology_.record_.group_term_ = 7;
  group.topology_.record_.population_manifest_revision_ = kManifestRevision;
  group.topology_.record_.population_manifest_digest_ = Bytes<32>(0x11);
  group.topology_.record_.partition_replication_epoch_ = kPartitionEpoch;
  group.grant_.group_term_ = 7;
  group.grant_.grant_ = MetaActiveAuthorityView{.owner_ = kOwnerId};
  group.topology_.members_ = {
      MetaGroupMember{kOwnerId, Bytes<16>(0x01), MetaNodeRole::kPrimary},
      MetaGroupMember{kReplicaOneId, Bytes<16>(0x02), MetaNodeRole::kReplica},
      MetaGroupMember{kReplicaTwoId, Bytes<16>(0x03), MetaNodeRole::kReplica},
  };
  MutableCommitted(cut).groups_.push_back(std::move(group));
  MutableCommitted(cut).slot_ranges_ = {
      MetaCommittedStatusSlotRange{
          .first_ = 0, .last_ = kMetaSlotCount - 1, .group_id_ = "g1"},
  };
  MutableCommitted(cut).data_nodes_ = {
      NodeRecord(kOwnerId, {"tcp://10.0.0.1:7001"}),
      NodeRecord(kReplicaOneId, {"tcp://10.0.0.2:7002"}),
      NodeRecord(kReplicaTwoId, {"tcp://10.0.0.3:7003"}),
  };

  cut.runtime_.leader_term_ = 3;
  cut.runtime_.leader_authority_eligibility_revision_ = 2;
  cut.runtime_.nodes_ = {
      LiveRuntimeNode(kOwnerId, Bytes<16>(0x01)),
      LiveRuntimeNode(kReplicaOneId, Bytes<16>(0x02)),
      LiveRuntimeNode(kReplicaTwoId, Bytes<16>(0x03)),
  };
  cut.runtime_.observed_nodes_ = {kOwnerId, kReplicaOneId, kReplicaTwoId};

  cut.diagnostics_.leader_term_ = 3;
  cut.diagnostics_.leader_authority_eligibility_revision_ = 2;
  cut.diagnostics_.evaluated_applied_index_ = 100;
  MetaAutomaticFailoverStatus status;
  status.anchor_.group_id_ = "g1";
  status.anchor_.leader_term_ = 3;
  status.anchor_.leader_authority_eligibility_revision_ = 2;
  status.anchor_.owner_node_id_ = kOwnerId;
  status.anchor_.owner_assignment_id_ = Bytes<16>(0x01);
  status.anchor_.group_term_ = 7;
  status.state_ = MetaAutomaticFailoverState::kHealthy;
  cut.diagnostics_.statuses_ = {status};
  return cut;
}

MetaAutomaticFailoverStatus& CutStatus(MetaDiscoveryCut& cut) {
  return cut.diagnostics_.statuses_.front();
}

const MetaCommittedStatusGroup& SoleGroup(const MetaDiscoveryCut& cut) {
  const MetaCommittedStatusGroup* group = DiscoveryServiceGroup(cut);
  EXPECT_NE(group, nullptr);
  return *group;
}

// ---------------------------------------------------------------------------
// Minimal RESP reply decoding for structural assertions.
// ---------------------------------------------------------------------------

struct RespValue {
  char tag_ = 0;  // '+', '-', ':', '$', '*', '%', or '_' (null)
  std::string scalar_;
  long long integer_ = 0;
  std::vector<RespValue> items_;
};

std::optional<RespValue> DecodeRespAt(std::string_view wire, std::size_t* at) {
  if (*at >= wire.size()) return std::nullopt;
  const char tag = wire[(*at)++];
  const auto line_end = wire.find("\r\n", *at);
  if (line_end == std::string_view::npos) return std::nullopt;
  const std::string_view header = wire.substr(*at, line_end - *at);
  *at = line_end + 2;
  RespValue value;
  value.tag_ = tag;
  switch (tag) {
    case '+':
    case '-':
      value.scalar_ = std::string(header);
      return value;
    case '_':
      return value;
    case ':': {
      const auto parsed = std::from_chars(
          header.data(), header.data() + header.size(), value.integer_);
      if (parsed.ec != std::errc{} ||
          parsed.ptr != header.data() + header.size()) {
        return std::nullopt;
      }
      return value;
    }
    case '$': {
      long long length = 0;
      const auto parsed =
          std::from_chars(header.data(), header.data() + header.size(), length);
      if (parsed.ec != std::errc{} || length < 0 ||
          *at + static_cast<std::size_t>(length) + 2 > wire.size()) {
        return std::nullopt;
      }
      value.scalar_ = std::string(wire.substr(*at, length));
      *at += length;
      if (wire.substr(*at, 2) != "\r\n") return std::nullopt;
      *at += 2;
      return value;
    }
    case '*':
    case '%': {
      long long count = 0;
      const auto parsed =
          std::from_chars(header.data(), header.data() + header.size(), count);
      if (parsed.ec != std::errc{}) return std::nullopt;
      if (count < 0) return value;  // null array
      // A RESP3 map header counts key/value pairs; its elements follow as
      // 2N items, like the RESP2 flat-array degradation.
      const long long items = tag == '%' ? count * 2 : count;
      for (long long i = 0; i < items; ++i) {
        auto item = DecodeRespAt(wire, at);
        if (!item.has_value()) return std::nullopt;
        value.items_.push_back(std::move(*item));
      }
      return value;
    }
    default:
      return std::nullopt;
  }
}

RespValue DecodeResp(std::string_view wire) {
  std::size_t at = 0;
  auto value = DecodeRespAt(wire, &at);
  if (!value.has_value() || at != wire.size()) {
    ADD_FAILURE() << "malformed RESP reply";
    return RespValue{};
  }
  return *value;
}

// Both the RESP2 flat kv array and the RESP3 map decode to one field map.
std::map<std::string, RespValue> EntryFields(const RespValue& entry) {
  std::map<std::string, RespValue> fields;
  EXPECT_TRUE(entry.tag_ == '*' || entry.tag_ == '%') << entry.tag_;
  EXPECT_EQ(entry.items_.size() % 2, 0u);
  for (std::size_t i = 0; i + 1 < entry.items_.size(); i += 2) {
    fields[entry.items_[i].scalar_] = entry.items_[i + 1];
  }
  return fields;
}

std::string FieldText(const std::map<std::string, RespValue>& fields,
                      const std::string& key) {
  const auto it = fields.find(key);
  EXPECT_TRUE(it != fields.end()) << "missing field " << key;
  return it == fields.end() ? std::string() : it->second.scalar_;
}

// Real Redis 7.2 Sentinel entries carry numeric values as bulk strings;
// accept both encodings so the assertions focus on the numeric content.
long long FieldInt(const std::map<std::string, RespValue>& fields,
                   const std::string& key) {
  const auto it = fields.find(key);
  EXPECT_TRUE(it != fields.end()) << "missing field " << key;
  if (it == fields.end()) return 0;
  if (it->second.tag_ == ':') return it->second.integer_;
  EXPECT_EQ(it->second.tag_, '$') << key;
  long long value = 0;
  const auto parsed = std::from_chars(
      it->second.scalar_.data(),
      it->second.scalar_.data() + it->second.scalar_.size(), value);
  EXPECT_TRUE(parsed.ec == std::errc{} &&
              parsed.ptr ==
                  it->second.scalar_.data() + it->second.scalar_.size())
      << key << " is not numeric: " << it->second.scalar_;
  return value;
}

// ---------------------------------------------------------------------------
// Publication gate matrix: every failure retracts the Primary
// (null address, omitted from MASTERS, "No such master" for MASTER) instead
// of announcing a down-marked master.
// ---------------------------------------------------------------------------

void ExpectRetracted(const MetaDiscoveryCut& cut) {
  if (const MetaCommittedStatusGroup* group = DiscoveryServiceGroup(cut);
      group != nullptr) {
    EXPECT_FALSE(PublishablePrimary(cut, *group).has_value());
  }
  ReplyBuilder resp2;
  EncodeDiscoveryAddressReply(resp2, cut, "g1");
  EXPECT_EQ(resp2.View(), "*-1\r\n");
  ReplyBuilder resp3(RespVersion::k3);
  EncodeDiscoveryAddressReply(resp3, cut, "g1");
  EXPECT_EQ(resp3.View(), "_\r\n");
  ReplyBuilder masters;
  EncodeDiscoveryMastersReply(masters, cut);
  EXPECT_EQ(masters.View(), "*0\r\n");
  ReplyBuilder master;
  EncodeDiscoveryMasterReply(master, cut, "g1");
  EXPECT_EQ(master.View(), "-ERR No such master with that name\r\n");
}

TEST(MetaSentinelDiscoveryTest, PublicationGatesRetractPrimary) {
  {  // Fenced: no active authority grant in the current term.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).groups_.front().grant_.grant_.reset();
    ExpectRetracted(cut);
  }
  {  // Cluster mode exposes no discovery service at all: discovery serves
     // Meta-managed Single deployments only.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).cluster_lifecycle_.client_mode_ =
        ClientMode::kCluster;
    EXPECT_EQ(DiscoveryServiceGroup(cut), nullptr);
    ExpectRetracted(cut);
  }
  {  // Not yet Created.
    for (const MetaClusterLifecycle state :
         {MetaClusterLifecycle::kUninitialized, MetaClusterLifecycle::kCreating,
          MetaClusterLifecycle::kProvisioningFailed}) {
      MetaDiscoveryCut cut = MakeCut();
      MutableCommitted(cut).cluster_lifecycle_.state_ = state;
      ExpectRetracted(cut);
    }
  }
  {  // A Single service is exactly one Group; two Groups expose none.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).groups_.push_back(
        MutableCommitted(cut).groups_.front());
    MutableCommitted(cut).groups_.back().topology_.group_id_ = "g2";
    EXPECT_EQ(DiscoveryServiceGroup(cut), nullptr);
    ExpectRetracted(cut);
  }
  {  // Retired owner.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).data_nodes_.front().retired_ = true;
    ExpectRetracted(cut);
  }
  {  // Owner absent from the identity registry entirely.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).data_nodes_.erase(
        MutableCommitted(cut).data_nodes_.begin());
    ExpectRetracted(cut);
  }
  {  // Owner is not a member of its own Group: the Group is not complete.
    MetaDiscoveryCut cut = MakeCut();
    auto& members = MutableCommitted(cut).groups_.front().topology_.members_;
    members.erase(members.begin());
    ExpectRetracted(cut);
  }
  {  // TLS-only client endpoints publish nothing until TLS discovery exists.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).data_nodes_.front().endpoints_ = {
        "tls://10.0.0.1:7443"};
    ExpectRetracted(cut);
  }
  {  // Wildcard and unusable addresses are listen forms, not client routes.
    for (const std::string endpoint :
         {"tcp://0.0.0.0:7001", "tcp://[::]:7001",
          "tcp://[::ffff:0.0.0.0]:7001", "tcp://127.0.0.1:0",
          "tcp://localhost:7001", "10.0.0.1:notaport"}) {
      SCOPED_TRACE(endpoint);
      MetaDiscoveryCut cut = MakeCut();
      MutableCommitted(cut).data_nodes_.front().endpoints_ = {endpoint};
      ExpectRetracted(cut);
    }
  }
  {  // Legacy untagged plaintext endpoints remain publishable.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).data_nodes_.front().endpoints_ = {"10.0.0.1:7001"};
    const auto primary = PublishablePrimary(cut, SoleGroup(cut));
    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(primary->endpoint_.host_, "10.0.0.1");
    EXPECT_EQ(primary->endpoint_.port_, 7001);
  }
  {  // The plaintext address is selected when both transports are registered.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).data_nodes_.front().endpoints_ = {
        "tcp://10.0.0.1:7001", "tls://10.0.0.1:7443"};
    const auto primary = PublishablePrimary(cut, SoleGroup(cut));
    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(primary->endpoint_.port_, 7001);
  }
}

TEST(MetaSentinelDiscoveryTest, SwitchNotificationCrossesMasterlessWindow) {
  auto cut = MakeCut();
  MetaDiscoveryEvents events;
  EXPECT_TRUE(events.Observe(cut).empty());
  auto& group = MutableCommitted(cut).groups_.front();
  group.grant_.grant_.reset();
  group.grant_.group_term_ = 8;
  EXPECT_TRUE(events.Observe(cut).empty());
  group.grant_.grant_ = MetaActiveAuthorityView{.owner_ = kReplicaOneId};
  group.topology_.record_.owner_ = kReplicaOneId;
  group.topology_.record_.group_term_ = 8;
  const auto messages = events.Observe(cut);
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].channel_, "+switch-master");
  EXPECT_EQ(messages[0].payload_, "g1 10.0.0.1 7001 10.0.0.2 7002");
  EXPECT_TRUE(events.Observe(cut).empty());
  ++group.grant_.group_term_;
  EXPECT_TRUE(events.Observe(cut).empty());
  events.Reset();
  EXPECT_TRUE(events.Observe(cut).empty());
}

TEST(MetaSentinelDiscoveryTest, ReadableReplicaDoesNotProveSourceAdoption) {
  auto cut = MakeCut();
  const auto& group = SoleGroup(cut);
  EXPECT_FALSE(
      ReplicaReconfigurationComplete(cut, group, group.topology_.members_[1]));
}

// A current ReadyToken lineage, received on this live reporter incarnation,
// is stronger evidence than FDS acknowledgement or a readable population.
MetaDiscoveryCut SourceProofCut() {
  auto cut = MakeCut();
  for (auto& node : cut.runtime_.nodes_) node.leader_term_ = 3;
  cut.runtime_.nodes_[0].boot_id_ = "owner-boot";
  cut.runtime_.nodes_[1].boot_id_ = "replica-boot";
  cluster::control::CandidateProgress proof;
  proof.group_id = "g1";
  proof.assignment_id = Bytes<16>(0x02);
  proof.group_term = proof.source_group_term = 7;
  proof.manifest_revision = kManifestRevision;
  proof.manifest_digest = Bytes<32>(0x11);
  proof.partition_replication_epoch = kPartitionEpoch;
  proof.source_node_id = kOwnerId;
  proof.source_assignment_id = Bytes<16>(0x01);
  proof.source_boot_id = "owner-boot";
  proof.source_history_id =
      std::string(2 * kMetaReplicationHistoryIdBytes, '0');
  cut.runtime_.nodes_[1].replica_progress_ = proof;
  return cut;
}

TEST(MetaSentinelDiscoveryTest, CompletionRequiresExactFreshSourceLineage) {
  const auto complete = [](const MetaDiscoveryCut& cut) {
    const auto& group = SoleGroup(cut);
    return ReplicaReconfigurationComplete(cut, group,
                                          group.topology_.members_[1]);
  };
  ASSERT_TRUE(complete(SourceProofCut()));
  for (int dimension = 0; dimension < 11; ++dimension) {
    auto cut = SourceProofCut();
    auto& proof = *cut.runtime_.nodes_[1].replica_progress_;
    switch (dimension) {
      case 0:
        --proof.source_group_term;
        break;
      case 1:
        proof.source_boot_id = "old-boot";
        break;
      case 2:
        proof.source_history_id = std::string(32, '1');
        break;
      case 3:
        proof.source_assignment_id = Bytes<16>(0x7f);
        break;
      case 4:
        proof.assignment_id = Bytes<16>(0x7f);
        break;
      case 5:
        --proof.manifest_revision;
        break;
      case 6:
        proof.recovered = true;
        break;
      case 7:
        proof.operator_recovery = true;
        break;
      case 8:
        --cut.runtime_.nodes_[1].leader_term_;
        break;
      case 9:
        cut.runtime_.nodes_[1].health_received_unix_ms_ -= 501;
        break;
      case 10:
        cut.runtime_.nodes_[0].health_received_unix_ms_ -= 501;
        break;
    }
    EXPECT_FALSE(complete(cut)) << dimension;
  }
}

TEST(MetaSentinelDiscoveryTest,
     CompletionIsDeduplicatedAndReacquisitionIsBaseline) {
  auto cut = SourceProofCut();
  auto& proof = *cut.runtime_.nodes_[1].replica_progress_;
  --proof.source_group_term;
  MetaDiscoveryEvents events;
  EXPECT_TRUE(events.Observe(cut).empty());
  ++proof.source_group_term;
  const auto messages = events.Observe(cut);
  ASSERT_EQ(messages.size(), 1);
  EXPECT_EQ(messages[0].channel_, "+replica-reconf-done");
  EXPECT_EQ(messages[0].payload_,
            "slave 10.0.0.2:7002 10.0.0.2 7002 @ g1 10.0.0.1 7001");
  EXPECT_TRUE(events.Observe(cut).empty());
  events.Reset();
  EXPECT_TRUE(events.Observe(cut).empty());
  cut.runtime_.nodes_[1].boot_id_ = "replacement-boot";
  EXPECT_TRUE(events.Observe(cut).empty());
}

TEST(MetaSentinelDiscoveryTest,
     PeerDirectoryExcludesSelfRetiredAndStagedMembers) {
  auto cut = MakeCut();
  cut.local_meta_id_ = 1;
  cut.effective_meta_ids_ = {1, 2, 3};
  for (unsigned id = 1; id <= 4; ++id) {
    MetaMemberRecord peer;
    peer.server_id_ = id;
    peer.sentinel_endpoint_ = "10.0.0." + std::to_string(id) + ":26379";
    peer.retired_ = id == 3;
    MutableCommitted(cut).meta_members_.push_back(peer);
  }
  const auto peers = DiscoverySentinels(cut);
  ASSERT_EQ(peers.size(), 1);
  EXPECT_EQ(peers[0].server_id_, 2);
  ReplyBuilder master;
  EncodeDiscoveryMasterReply(master, cut, "g1");
  EXPECT_EQ(
      FieldInt(EntryFields(DecodeResp(master.View())), "num-other-sentinels"),
      1);
  ReplyBuilder directory;
  EncodeDiscoverySentinelsReply(directory, cut, "g1");
  EXPECT_TRUE(directory.View().starts_with("*1\r\n"));
  EXPECT_NE(directory.View().find("10.0.0.2"), std::string_view::npos);
}

// The complete-Single-Group condition is explicit: the one Group must own a
// contiguous 0..16383 slot assignment. Any hole or foreign range exposes no
// service at all — null address, empty MASTERS, and the unknown-service error
// for the named verbs.
TEST(MetaSentinelDiscoveryTest, IncompleteSlotCoverageExposesNoService) {
  {
    MetaDiscoveryCut cut = MakeCut();
    EXPECT_NE(DiscoveryServiceGroup(cut), nullptr);
  }
  const std::vector<std::vector<MetaCommittedStatusSlotRange>> broken = {
      {},  // nothing assigned yet
      {{.first_ = 1, .last_ = kMetaSlotCount - 1, .group_id_ = "g1"}},
      {{.first_ = 0, .last_ = kMetaSlotCount - 2, .group_id_ = "g1"}},
      {{.first_ = 0, .last_ = 100, .group_id_ = "g1"},
       {.first_ = 102, .last_ = kMetaSlotCount - 1, .group_id_ = "g1"}},
      {{.first_ = 0, .last_ = kMetaSlotCount - 1, .group_id_ = "g2"}},
      {{.first_ = 0, .last_ = 100, .group_id_ = "g1"},
       {.first_ = 101, .last_ = kMetaSlotCount - 1, .group_id_ = "g2"}},
      {{.first_ = 0, .last_ = 100, .group_id_ = "g1"},
       {.first_ = 50, .last_ = kMetaSlotCount - 1, .group_id_ = "g1"}},
  };
  for (const auto& ranges : broken) {
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).slot_ranges_ = ranges;
    EXPECT_EQ(DiscoveryServiceGroup(cut), nullptr);
    ExpectRetracted(cut);
    ReplyBuilder replicas;
    EncodeDiscoveryReplicasReply(replicas, cut, "g1");
    EXPECT_EQ(replicas.View(), "-ERR No such master with that name\r\n");
  }
}

TEST(MetaSentinelDiscoveryTest, AddressReplyShapeAndUnknownName) {
  MetaDiscoveryCut cut = MakeCut();
  ReplyBuilder resp2;
  EncodeDiscoveryAddressReply(resp2, cut, "g1");
  EXPECT_EQ(resp2.View(), "*2\r\n$8\r\n10.0.0.1\r\n$4\r\n7001\r\n");
  ReplyBuilder resp3(RespVersion::k3);
  EncodeDiscoveryAddressReply(resp3, cut, "g1");
  EXPECT_EQ(resp3.View(), "*2\r\n$8\r\n10.0.0.1\r\n$4\r\n7001\r\n");
  // Unknown service names get the same null contract as unavailable ones.
  ReplyBuilder unknown;
  EncodeDiscoveryAddressReply(unknown, cut, "nosuch");
  EXPECT_EQ(unknown.View(), "*-1\r\n");
}

// ---------------------------------------------------------------------------
// MASTER/MASTERS entries.
// ---------------------------------------------------------------------------

TEST(MetaSentinelDiscoveryTest, MasterReplyFieldsResp2AndResp3) {
  MetaDiscoveryCut cut = MakeCut();
  ReplyBuilder resp2;
  EncodeDiscoveryMasterReply(resp2, cut, "g1");
  EXPECT_TRUE(resp2.View().starts_with("*40\r\n")) << resp2.View();
  const RespValue entry = DecodeResp(resp2.View());
  EXPECT_EQ(entry.tag_, '*');
  const auto fields = EntryFields(entry);
  EXPECT_EQ(fields.size(), 20u);
  EXPECT_EQ(FieldText(fields, "name"), "g1");
  EXPECT_EQ(FieldText(fields, "ip"), "10.0.0.1");
  EXPECT_EQ(FieldInt(fields, "port"), 7001);
  EXPECT_EQ(FieldText(fields, "runid"), kOwnerId);
  EXPECT_EQ(FieldText(fields, "flags"), "master");
  EXPECT_EQ(FieldInt(fields, "config-epoch"), 7);
  EXPECT_EQ(FieldInt(fields, "num-slaves"), 2);
  EXPECT_EQ(FieldInt(fields, "num-other-sentinels"), 0);
  EXPECT_EQ(FieldText(fields, "role-reported"), "master");
  EXPECT_EQ(FieldInt(fields, "down-after-milliseconds"), 30000);
  EXPECT_EQ(FieldInt(fields, "quorum"), 1);
  EXPECT_EQ(FieldInt(fields, "failover-timeout"), 180000);
  EXPECT_EQ(FieldInt(fields, "parallel-syncs"), 1);
  // Byte fidelity: numeric values travel as bulk strings, never RESP integers.
  EXPECT_EQ(fields.at("port").tag_, '$');
  EXPECT_TRUE(resp2.View().contains("$4\r\nport\r\n$4\r\n7001\r\n"))
      << resp2.View();
  EXPECT_TRUE(resp2.View().contains("$12\r\nconfig-epoch\r\n$1\r\n7\r\n"))
      << resp2.View();
  EXPECT_TRUE(resp2.View().contains("$10\r\nnum-slaves\r\n$1\r\n2\r\n"))
      << resp2.View();

  ReplyBuilder resp3(RespVersion::k3);
  EncodeDiscoveryMasterReply(resp3, cut, "g1");
  EXPECT_TRUE(resp3.View().starts_with("%20\r\n")) << resp3.View();
  EXPECT_EQ(EntryFields(DecodeResp(resp3.View())).size(), 20u);

  ReplyBuilder masters;
  EncodeDiscoveryMastersReply(masters, cut);
  const RespValue list = DecodeResp(masters.View());
  ASSERT_EQ(list.tag_, '*');
  ASSERT_EQ(list.items_.size(), 1u);
  EXPECT_EQ(EntryFields(list.items_.front()).at("runid").scalar_, kOwnerId);
}

TEST(MetaSentinelDiscoveryTest, MasterAndReplicasRejectUnknownName) {
  MetaDiscoveryCut cut = MakeCut();
  for (RespVersion version : {RespVersion::k2, RespVersion::k3}) {
    ReplyBuilder master(version);
    EncodeDiscoveryMasterReply(master, cut, "nosuch");
    EXPECT_EQ(master.View(), "-ERR No such master with that name\r\n");
    ReplyBuilder replicas(version);
    EncodeDiscoveryReplicasReply(replicas, cut, "nosuch");
    EXPECT_EQ(replicas.View(), "-ERR No such master with that name\r\n");
  }
}

// ---------------------------------------------------------------------------
// Master flags: s_down comes only from an identity-validated detector
// verdict; disconnected only from session absence; o_down never exists.
// ---------------------------------------------------------------------------

TEST(MetaSentinelDiscoveryTest, MasterFlagsFollowDetectorVerdict) {
  const auto primary_at = [](const MetaDiscoveryCut& cut) {
    const auto primary = PublishablePrimary(cut, SoleGroup(cut));
    EXPECT_TRUE(primary.has_value());
    return *primary;
  };
  {
    MetaDiscoveryCut cut = MakeCut();
    EXPECT_FALSE(MasterFlags(cut, primary_at(cut)).s_down_);
    EXPECT_FALSE(MasterFlags(cut, primary_at(cut)).disconnected_);
  }
  for (const MetaAutomaticFailoverState state :
       {MetaAutomaticFailoverState::kSuspect,
        MetaAutomaticFailoverState::kTriggering}) {
    MetaDiscoveryCut cut = MakeCut();
    CutStatus(cut).state_ = state;
    const auto flags = MasterFlags(cut, primary_at(cut));
    EXPECT_TRUE(flags.s_down_);
    EXPECT_FALSE(flags.disconnected_);
  }
  {  // A session-less owner is disconnected even while the detector is calm.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_.erase(cut.runtime_.nodes_.begin());
    const auto flags = MasterFlags(cut, primary_at(cut));
    EXPECT_FALSE(flags.s_down_);
    EXPECT_TRUE(flags.disconnected_);
  }
}

TEST(MetaSentinelDiscoveryTest, DiagnosticsRequireSnapshotIdentityAndAnchor) {
  const auto s_down = [](MetaDiscoveryCut cut) {
    CutStatus(cut).state_ = MetaAutomaticFailoverState::kSuspect;
    const auto primary = PublishablePrimary(cut, SoleGroup(cut));
    EXPECT_TRUE(primary.has_value());
    return MasterFlags(cut, *primary).s_down_;
  };
  EXPECT_TRUE(s_down(MakeCut()));
  {  // A detector decision for an older committed view is not current.
    MetaDiscoveryCut cut = MakeCut();
    cut.diagnostics_.evaluated_applied_index_ = 99;
    EXPECT_FALSE(s_down(std::move(cut)));
  }
  {  // A detector cut from a prior leadership generation never applies.
    MetaDiscoveryCut cut = MakeCut();
    cut.diagnostics_.leader_term_ = 2;
    EXPECT_FALSE(s_down(std::move(cut)));
  }
  {  // Zero marks "no active diagnostics bracket" and must never match.
    MetaDiscoveryCut cut = MakeCut();
    cut.diagnostics_.leader_term_ = 0;
    EXPECT_FALSE(s_down(std::move(cut)));
  }
  {  // Eligibility interruption inside one generation invalidates the cut.
    MetaDiscoveryCut cut = MakeCut();
    cut.diagnostics_.leader_authority_eligibility_revision_ = 1;
    EXPECT_FALSE(s_down(std::move(cut)));
  }
  {  // The verdict must name the committed owner and term.
    MetaDiscoveryCut cut = MakeCut();
    CutStatus(cut).anchor_.owner_node_id_ = kReplicaOneId;
    EXPECT_FALSE(s_down(std::move(cut)));
  }
  {
    MetaDiscoveryCut cut = MakeCut();
    CutStatus(cut).anchor_.group_term_ = 6;
    EXPECT_FALSE(s_down(std::move(cut)));
  }
  {  // No status for the Group at all.
    MetaDiscoveryCut cut = MakeCut();
    cut.diagnostics_.statuses_.clear();
    EXPECT_FALSE(s_down(std::move(cut)));
  }
}

TEST(MetaSentinelDiscoveryTest, ObservationGraceMasksOnlyNeverObserved) {
  const auto flags_for = [](MetaDiscoveryCut cut) {
    const auto primary = PublishablePrimary(cut, SoleGroup(cut));
    EXPECT_TRUE(primary.has_value());
    return MasterFlags(cut, *primary);
  };
  {  // Inside the grace window a node this leader never saw is unknown, not
     // down.
    MetaDiscoveryCut cut = MakeCut();
    cut.observation_grace_active_ = true;
    cut.runtime_.nodes_.clear();
    cut.runtime_.observed_nodes_.clear();
    const auto flags = flags_for(std::move(cut));
    EXPECT_FALSE(flags.s_down_);
    EXPECT_FALSE(flags.disconnected_);
  }
  {  // The same absence outside the window is a disconnect.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_.clear();
    cut.runtime_.observed_nodes_.clear();
    EXPECT_TRUE(flags_for(std::move(cut)).disconnected_);
  }
  {  // Observed-then-gone counts as down even inside the grace window.
    MetaDiscoveryCut cut = MakeCut();
    cut.observation_grace_active_ = true;
    cut.runtime_.nodes_.clear();
    EXPECT_TRUE(flags_for(std::move(cut)).disconnected_);
  }
}

// ---------------------------------------------------------------------------
// Replica listing: listing follows committed membership; flags follow
// observation truth.
// ---------------------------------------------------------------------------

std::map<std::string, MetaDiscoveryReplica> ReplicasById(
    const MetaDiscoveryCut& cut, bool primary_publishable = true) {
  std::map<std::string, MetaDiscoveryReplica> by_id;
  for (auto& replica : ListReplicas(cut, SoleGroup(cut), primary_publishable)) {
    by_id.emplace(replica.node_id_, std::move(replica));
  }
  return by_id;
}

TEST(MetaSentinelDiscoveryTest, ReplicaReadabilityRequiresSessionHealthAnchor) {
  {  // Healthy steady state: no down marks.
    const auto replicas = ReplicasById(MakeCut());
    ASSERT_EQ(replicas.size(), 2u);
    EXPECT_FALSE(replicas.at(kReplicaOneId).s_down_);
    EXPECT_FALSE(replicas.at(kReplicaOneId).disconnected_);
    EXPECT_FALSE(replicas.at(kReplicaOneId).master_down_);
  }
  const auto replica_one_flags = [](MetaDiscoveryCut cut) {
    const auto replicas = ReplicasById(cut);
    EXPECT_EQ(replicas.size(), 2u);
    return replicas.at(kReplicaOneId);
  };
  {  // Heartbeat older than the observation TTL is not green.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].health_received_unix_ms_ =
        kNowUnixMs - kObservationTtlMs - 1;
    const auto replica = replica_one_flags(std::move(cut));
    EXPECT_TRUE(replica.s_down_);
    EXPECT_FALSE(replica.disconnected_);
  }
  for (const auto unhealthy :
       {cluster::control::HeartbeatHealth{.storage_ready = false},
        cluster::control::HeartbeatHealth{.storage_ready = true,
                                          .population_ready = false},
        cluster::control::HeartbeatHealth{.storage_ready = true,
                                          .population_ready = true,
                                          .draining = true}}) {
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].health_ = unhealthy;
    EXPECT_TRUE(replica_one_flags(std::move(cut)).s_down_);
  }
  {  // A projection from an older term is not the current committed anchor.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].groups_.front().group_term_ = 6;
    EXPECT_TRUE(replica_one_flags(std::move(cut)).s_down_);
  }
  {  // Nor is a stale membership incarnation.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].groups_.front().assignment_id_ = Bytes<16>(0x7f);
    EXPECT_TRUE(replica_one_flags(std::move(cut)).s_down_);
  }
  {  // No projected anchor for the Group at all.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].groups_.clear();
    EXPECT_TRUE(replica_one_flags(std::move(cut)).s_down_);
  }
  {  // No live session: both s_down and disconnected.
    MetaDiscoveryCut cut = MakeCut();
    auto& nodes = cut.runtime_.nodes_;
    nodes.erase(nodes.begin() + 1);
    const auto replica = replica_one_flags(std::move(cut));
    EXPECT_TRUE(replica.s_down_);
    EXPECT_TRUE(replica.disconnected_);
  }
  {  // Within the grace window a never-observed member is omitted entirely
     // rather than listed on unverified state.
    MetaDiscoveryCut cut = MakeCut();
    cut.observation_grace_active_ = true;
    auto& nodes = cut.runtime_.nodes_;
    nodes.erase(nodes.begin() + 1);
    auto& observed = cut.runtime_.observed_nodes_;
    observed.erase(std::find(observed.begin(), observed.end(), kReplicaOneId));
    const auto replicas = ReplicasById(std::move(cut));
    ASSERT_EQ(replicas.size(), 1u);
    EXPECT_FALSE(replicas.contains(kReplicaOneId));
    EXPECT_TRUE(replicas.contains(kReplicaTwoId));
  }
  {  // A fenced service marks replicas master_down without inventing a
     // Primary.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).groups_.front().grant_.grant_.reset();
    const auto replicas = ReplicasById(cut, /*primary_publishable=*/false);
    ASSERT_EQ(replicas.size(), 2u);
    EXPECT_TRUE(replicas.at(kReplicaOneId).master_down_);
    EXPECT_FALSE(replicas.at(kReplicaOneId).s_down_);
  }
}

// The readability anchor is the complete replication identity, not just the
// authority term and membership incarnation: SetGroupReplicationState advances
// the manifest revision/digest and partition replication epoch while those two
// stay unchanged, and a node still holding the superseded projection must read
// as s_down.
TEST(MetaSentinelDiscoveryTest,
     ReplicaAnchorRequiresCurrentReplicationIdentity) {
  const auto replica_one = [](MetaDiscoveryCut cut) {
    const auto replicas = ReplicasById(cut);
    EXPECT_TRUE(replicas.contains(kReplicaOneId));
    return replicas.at(kReplicaOneId);
  };
  {  // Baseline: every anchor dimension matches, so the replica is readable.
    EXPECT_FALSE(replica_one(MakeCut()).s_down_);
    EXPECT_FALSE(replica_one(MakeCut()).disconnected_);
  }
  {  // Authority term moved on.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].groups_.front().group_term_ = 6;
    EXPECT_TRUE(replica_one(std::move(cut)).s_down_);
  }
  {  // Stale membership incarnation.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].groups_.front().assignment_id_ = Bytes<16>(0x7f);
    EXPECT_TRUE(replica_one(std::move(cut)).s_down_);
  }
  {  // The committed manifest revision advanced while term and assignment
     // stayed unchanged.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut)
        .groups_.front()
        .topology_.record_.population_manifest_revision_ =
        kManifestRevision + 1;
    EXPECT_TRUE(replica_one(std::move(cut)).s_down_);
  }
  {  // Same revision counter but rotated manifest content.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut)
        .groups_.front()
        .topology_.record_.population_manifest_digest_ = Bytes<32>(0x22);
    EXPECT_TRUE(replica_one(std::move(cut)).s_down_);
  }
  {  // The committed partition replication epoch advanced.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut)
        .groups_.front()
        .topology_.record_.partition_replication_epoch_ = kPartitionEpoch + 1;
    EXPECT_TRUE(replica_one(std::move(cut)).s_down_);
  }
  {  // A runtime anchor from a superseded projection fails against the
     // unchanged committed state.
    MetaDiscoveryCut cut = MakeCut();
    cut.runtime_.nodes_[1].groups_.front().manifest_revision_ =
        kManifestRevision - 1;
    EXPECT_TRUE(replica_one(std::move(cut)).s_down_);
  }
}

// A never-observed member is omitted while the leader's observation grace
// window is open, listed with down evidence once it closes, and reads clean
// once green evidence arrives.
TEST(MetaSentinelDiscoveryTest, GraceOmitsNeverObservedReplicas) {
  const auto without_replica_one_evidence = [] {
    MetaDiscoveryCut cut = MakeCut();
    auto& nodes = cut.runtime_.nodes_;
    nodes.erase(nodes.begin() + 1);
    auto& observed = cut.runtime_.observed_nodes_;
    observed.erase(std::find(observed.begin(), observed.end(), kReplicaOneId));
    return cut;
  };
  {  // Window open: omission, not a flagless listing.
    MetaDiscoveryCut cut = without_replica_one_evidence();
    cut.observation_grace_active_ = true;
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 1u);
    EXPECT_FALSE(replicas.contains(kReplicaOneId));
    EXPECT_TRUE(replicas.contains(kReplicaTwoId));
  }
  {  // Window closed: the same absence is down evidence.
    MetaDiscoveryCut cut = without_replica_one_evidence();
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 2u);
    EXPECT_TRUE(replicas.at(kReplicaOneId).s_down_);
    EXPECT_TRUE(replicas.at(kReplicaOneId).disconnected_);
  }
  {  // Observed-then-gone is evidence even inside the window.
    MetaDiscoveryCut cut = MakeCut();
    cut.observation_grace_active_ = true;
    auto& nodes = cut.runtime_.nodes_;
    nodes.erase(nodes.begin() + 1);
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 2u);
    EXPECT_TRUE(replicas.at(kReplicaOneId).s_down_);
    EXPECT_TRUE(replicas.at(kReplicaOneId).disconnected_);
  }
  {  // A live session with green in-window health and the current anchor
     // lists clean again.
    MetaDiscoveryCut cut = without_replica_one_evidence();
    cut.runtime_.nodes_.insert(cut.runtime_.nodes_.begin() + 1,
                               LiveRuntimeNode(kReplicaOneId, Bytes<16>(0x02)));
    auto& observed = cut.runtime_.observed_nodes_;
    observed.insert(std::find(observed.begin(), observed.end(), kReplicaTwoId),
                    kReplicaOneId);
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 2u);
    EXPECT_FALSE(replicas.at(kReplicaOneId).s_down_);
    EXPECT_FALSE(replicas.at(kReplicaOneId).disconnected_);
  }
}

TEST(MetaSentinelDiscoveryTest, ReplicaListingReflectsCommittedMembership) {
  {  // A member committed away simply disappears.
    MetaDiscoveryCut cut = MakeCut();
    auto& members = MutableCommitted(cut).groups_.front().topology_.members_;
    members.erase(std::find_if(members.begin(), members.end(),
                               [&](const MetaGroupMember& member) {
                                 return member.node_id_ == kReplicaTwoId;
                               }));
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 1u);
    EXPECT_TRUE(replicas.contains(kReplicaOneId));
  }
  {  // A retired member is not a replica even while it remains committed.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).data_nodes_[2].retired_ = true;
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 1u);
    EXPECT_FALSE(replicas.contains(kReplicaTwoId));
  }
  {  // A member whose endpoint cannot be published is omitted rather than
     // announced with a fabricated address.
    MetaDiscoveryCut cut = MakeCut();
    MutableCommitted(cut).data_nodes_[2].endpoints_ = {"tcp://0.0.0.0:7003"};
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 1u);
    EXPECT_FALSE(replicas.contains(kReplicaTwoId));
  }
  {  // Zero committed replicas is a truthful empty list, not an error.
    MetaDiscoveryCut cut = MakeCut();
    auto& members = MutableCommitted(cut).groups_.front().topology_.members_;
    members.erase(members.begin() + 1, members.end());
    ReplyBuilder reply;
    EncodeDiscoveryReplicasReply(reply, cut, "g1");
    EXPECT_EQ(reply.View(), "*0\r\n");
    const auto primary = PublishablePrimary(cut, SoleGroup(cut));
    ASSERT_TRUE(primary.has_value());
    EXPECT_EQ(primary->replica_count_, 0u);
  }
  {  // After a committed cutover the new owner is not its own replica and the
     // replaced owner becomes an ordinary member: membership roles are
     // creation-time hints only.
    MetaDiscoveryCut cut = MakeCut();
    auto& group = MutableCommitted(cut).groups_.front();
    group.topology_.record_.owner_ = kReplicaOneId;
    group.grant_.grant_->owner_ = kReplicaOneId;
    const auto replicas = ReplicasById(cut);
    ASSERT_EQ(replicas.size(), 2u);
    EXPECT_FALSE(replicas.contains(kReplicaOneId));
    EXPECT_TRUE(replicas.contains(kOwnerId));
    EXPECT_TRUE(replicas.contains(kReplicaTwoId));
  }
}

TEST(MetaSentinelDiscoveryTest, ReplicaEntryFieldsResp2AndResp3) {
  MetaDiscoveryCut cut = MakeCut();
  ReplyBuilder resp2;
  EncodeDiscoveryReplicasReply(resp2, cut, "g1");
  EXPECT_TRUE(resp2.View().starts_with("*2\r\n*38\r\n")) << resp2.View();
  const RespValue list = DecodeResp(resp2.View());
  ASSERT_EQ(list.items_.size(), 2u);
  const auto fields = EntryFields(list.items_.front());
  EXPECT_EQ(fields.size(), 19u);
  EXPECT_EQ(FieldText(fields, "name"), "10.0.0.2:7002");
  EXPECT_EQ(FieldText(fields, "ip"), "10.0.0.2");
  EXPECT_EQ(FieldInt(fields, "port"), 7002);
  EXPECT_EQ(FieldText(fields, "runid"), kReplicaOneId);
  EXPECT_EQ(FieldText(fields, "flags"), "slave");
  EXPECT_EQ(FieldText(fields, "role-reported"), "slave");
  EXPECT_EQ(FieldText(fields, "master-host"), "10.0.0.1");
  EXPECT_EQ(FieldInt(fields, "master-port"), 7001);
  EXPECT_EQ(FieldInt(fields, "slave-priority"), 100);
  EXPECT_EQ(FieldInt(fields, "slave-repl-offset"), 0);
  EXPECT_EQ(FieldInt(fields, "replica-announced"), 1);
  // Byte fidelity: numeric values travel as bulk strings, never RESP integers.
  EXPECT_TRUE(resp2.View().contains("$4\r\nport\r\n$4\r\n7002\r\n"))
      << resp2.View();
  EXPECT_TRUE(resp2.View().contains("$14\r\nslave-priority\r\n$3\r\n100\r\n"))
      << resp2.View();
  // The replication link-state pair is deliberately not published: Meta has
  // no truthful steady-state observation of a replica's upstream link, so the
  // fields are omitted rather than carried as fabricated constants.
  EXPECT_EQ(fields.count("master-link-status"), 0u);
  EXPECT_EQ(fields.count("master-link-down-time"), 0u);

  ReplyBuilder resp3(RespVersion::k3);
  EncodeDiscoveryReplicasReply(resp3, cut, "g1");
  EXPECT_TRUE(resp3.View().starts_with("*2\r\n%19\r\n")) << resp3.View();
  EXPECT_EQ(EntryFields(DecodeResp(resp3.View()).items_.front()).size(), 19u);

  {  // An unresolvable committed owner endpoint omits the pair instead of
     // fabricating one.
    MutableCommitted(cut).data_nodes_.front().endpoints_ = {
        "tls://10.0.0.1:7443"};
    ReplyBuilder reply;
    EncodeDiscoveryReplicasReply(reply, cut, "g1");
    const auto degraded = EntryFields(DecodeResp(reply.View()).items_.front());
    EXPECT_EQ(degraded.size(), 17u);
    EXPECT_EQ(degraded.count("master-host"), 0u);
    EXPECT_EQ(degraded.count("master-port"), 0u);
  }
}

TEST(MetaSentinelDiscoveryTest, ReplicaFlagsSpelling) {
  MetaDiscoveryCut cut = MakeCut();
  auto& nodes = cut.runtime_.nodes_;
  nodes.erase(nodes.begin() + 1);
  ReplyBuilder reply;
  EncodeDiscoveryReplicasReply(reply, cut, "g1");
  const RespValue list = DecodeResp(reply.View());
  ASSERT_EQ(list.items_.size(), 2u);
  EXPECT_EQ(EntryFields(list.items_[0]).at("flags").scalar_,
            "s_down,slave,disconnected");
  EXPECT_EQ(EntryFields(list.items_[1]).at("flags").scalar_, "slave");
}

// go-redis parses these replies into generic maps: every master entry must be
// an even kv sequence with non-null values, and the address reply must be a
// two-element bulk pair.
TEST(MetaSentinelDiscoveryTest, GenericClientParseInvariants) {
  MetaDiscoveryCut cut = MakeCut();
  ReplyBuilder address;
  EncodeDiscoveryAddressReply(address, cut, "g1");
  const RespValue pair = DecodeResp(address.View());
  ASSERT_EQ(pair.items_.size(), 2u);
  EXPECT_EQ(pair.items_[0].tag_, '$');
  EXPECT_EQ(pair.items_[1].tag_, '$');

  for (RespVersion version : {RespVersion::k2, RespVersion::k3}) {
    ReplyBuilder master(version);
    EncodeDiscoveryMasterReply(master, cut, "g1");
    const RespValue master_reply = DecodeResp(master.View());
    EXPECT_EQ(master_reply.tag_, version == RespVersion::k2 ? '*' : '%');
    EXPECT_EQ(master_reply.items_.size() % 2, 0u);
    // Real Redis 7.2 Sentinel encodes every entry key and value — including
    // the numeric fields — as a bulk string.
    for (const RespValue& item : master_reply.items_) {
      EXPECT_EQ(item.tag_, '$');
    }
    ReplyBuilder replicas(version);
    EncodeDiscoveryReplicasReply(replicas, cut, "g1");
    for (const RespValue& entry : DecodeResp(replicas.View()).items_) {
      const auto fields = EntryFields(entry);
      EXPECT_FALSE(fields.at("flags").scalar_.empty());
    }
  }
}

}  // namespace
}  // namespace lavik::meta
