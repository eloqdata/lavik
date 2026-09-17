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

#include "lavik/meta/state_machine.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "lavik/fault_injection.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/nuraft_state_mgr.h"
#include "libnuraft/buffer.hxx"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/snapshot.hxx"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {

constexpr uint32_t kSnapshotMagic = 0x4D534E31;  // "MSN1"

template <std::size_t N>
std::string HexId(const std::array<std::uint8_t, N>& id) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(N * 2);
  for (const std::uint8_t byte : id) {
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

bool IsSafeLogTokenByte(std::uint8_t byte) {
  return (byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
         (byte >= '0' && byte <= '9') || byte == '-' || byte == '_' ||
         byte == '.' || byte == ':';
}

// Failover logs use whitespace-delimited key=value tokens. Percent-encode
// every byte outside a deliberately small ASCII alphabet so opaque committed
// identifiers cannot inject fields or record boundaries. '%' is encoded too,
// making the representation canonical and reversible.
std::string LogToken(std::string_view value) {
  static constexpr char kHex[] = "0123456789ABCDEF";
  std::string result;
  result.reserve(value.size());
  for (const char character : value) {
    const auto byte = static_cast<std::uint8_t>(character);
    if (IsSafeLogTokenByte(byte)) {
      result.push_back(character);
      continue;
    }
    result.push_back('%');
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

struct FailoverCommitLog {
  std::string message_;
  bool data_loss_possible_ = false;
};

// Build the event from the pre-apply aggregate so candidate replacement and
// domain fallback remain distinguishable after the command overwrites the
// transition. The line is emitted only if deterministic apply accepts the
// entry; its fields deliberately mirror the durable audit identifiers.
std::optional<FailoverCommitLog> DescribeFailoverCommit(
    const MetaCommand& command, const MetaStores& before,
    std::uint64_t commit_index) {
  return std::visit(
      [&]<typename Command>(
          const Command& cmd) -> std::optional<FailoverCommitLog> {
        std::string event;
        std::string mode;
        std::string group;
        std::string transition = "none";
        std::string action = "none";
        std::string loss = "pending";
        std::string detail;
        bool data_loss_possible = false;

        if constexpr (std::is_same_v<Command, BeginControlledFailover>) {
          event = "begin";
          mode = "controlled";
          group = cmd.group_id_;
          transition = HexId(cmd.transition_id_);
          action = HexId(cmd.candidate_action_.action_id_);
          loss = "none";
          detail = " candidate=" +
                   LogToken(cmd.candidate_action_.candidate_.node_id_);
        } else if constexpr (std::is_same_v<Command,
                                            BeginUncontrolledFailover>) {
          event = "begin";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.transition_id_);
          if (cmd.candidate_action_.has_value()) {
            action = HexId(cmd.candidate_action_->action_id_);
          }
          loss = "unknown";
          data_loss_possible = true;
          if (cmd.trigger_reason_ != MetaAutomaticFailoverReason::kManual) {
            detail = absl::StrCat(
                " suspect_ms=", cmd.suspect_duration_ms_, " reason=",
                LogToken(MetaAutomaticFailoverReasonName(cmd.trigger_reason_)));
          }
        } else if constexpr (std::is_same_v<Command, StartCandidateRecovery>) {
          event = "recovery-start";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          detail = absl::StrCat(" recovery_deadline_ms=",
                                cmd.recovery_deadline_unix_ms_);
        } else if constexpr (std::is_same_v<Command,
                                            SetUncontrolledCandidate>) {
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          const auto group_before = before.topology_.FindGroup(cmd.group_id_);
          const MetaFailoverCandidateAction* previous = nullptr;
          if (group_before.has_value() &&
              group_before->failover_transition_.has_value() &&
              group_before->failover_transition_->candidate_action_
                  .has_value()) {
            previous = &*group_before->failover_transition_->candidate_action_;
          }
          const bool exact_post_effect =
              cmd.candidate_action_.has_value() && previous != nullptr &&
              group_before->failover_transition_->transition_id_ ==
                  cmd.expected_transition_.transition_id_ &&
              group_before->failover_transition_->revision_ == commit_index &&
              *previous == *cmd.candidate_action_;
          if (exact_post_effect) {
            // Apply accepts the same index against its exact post-state. The
            // original event depended on the overwritten previous action, so
            // replay cannot reconstruct it and must not emit a conflicting
            // selected/fallback/replaced classification at the same index.
            return std::nullopt;
          }
          if (!cmd.candidate_action_.has_value()) {
            // A fresh clear requires an installed candidate. If none is
            // visible, this can only become an accepted command through the
            // exact post-effect replay path; suppress that duplicate event
            // because the cleared action identity is no longer reconstructible.
            if (previous == nullptr) return std::nullopt;
            event = "candidate-cleared";
            action = HexId(previous->action_id_);
          } else {
            action = HexId(cmd.candidate_action_->action_id_);
            if (previous == nullptr) {
              event = "candidate-selected";
            } else if (previous->domain_ != cmd.candidate_action_->domain_) {
              event = "domain-fallback";
            } else {
              event = "candidate-replaced";
            }
            detail = absl::StrCat(
                " source_group_term=",
                cmd.candidate_action_->domain_.source_group_term_,
                " candidate=",
                LogToken(cmd.candidate_action_->candidate_.node_id_));
          }
          loss = "unknown";
          data_loss_possible = true;
        } else if constexpr (std::is_same_v<Command,
                                            AuthorizeFailoverPrepare>) {
          event = "authorize";
          mode = cmd.loss_if_cutover_ == MetaFailoverLoss::kNone
                     ? "controlled"
                     : "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          loss = cmd.loss_if_cutover_ == MetaFailoverLoss::kNone ? "none"
                                                                 : "unknown";
          data_loss_possible = cmd.loss_if_cutover_ != MetaFailoverLoss::kNone;
        } else if constexpr (std::is_same_v<Command, AbortControlledFailover>) {
          event = "abort";
          mode = "controlled";
          group = cmd.group_id_;
          if (cmd.expected_transition_.has_value()) {
            transition = HexId(cmd.expected_transition_->transition_id_);
            const auto group_before = before.topology_.FindGroup(cmd.group_id_);
            if (!group_before.has_value() ||
                !group_before->failover_transition_.has_value() ||
                group_before->failover_transition_->transition_id_ !=
                    cmd.expected_transition_->transition_id_ ||
                group_before->failover_transition_->revision_ !=
                    cmd.expected_transition_->revision_ ||
                group_before->failover_transition_->mode_ !=
                    MetaFailoverMode::kControlled ||
                !group_before->failover_transition_->candidate_action_
                     .has_value()) {
              // A post-Begin Abort clears the transition that supplied its
              // action id. Exact replay is accepted against that post-state,
              // but emitting action=none would conflict with the original
              // event at the same commit index.
              return std::nullopt;
            }
            action = HexId(group_before->failover_transition_->candidate_action_
                               ->action_id_);
          }
          loss = "none";
          detail = " reason=" + LogToken(cmd.reason_);
        } else if constexpr (std::is_same_v<Command,
                                            DegradeControlledFailover>) {
          event = "degrade";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          if (cmd.expected_candidate_action_.has_value()) {
            action = HexId(cmd.expected_candidate_action_->action_id_);
          }
          loss = cmd.retain_candidate_action_ ? "none" : "unknown";
          data_loss_possible = !cmd.retain_candidate_action_;
          detail = " reason=" + LogToken(cmd.reason_);
        } else if constexpr (std::is_same_v<Command,
                                            CommitControlledFailover>) {
          event = "cutover";
          mode = "controlled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          loss = "none";
          detail = " candidate=" + LogToken(cmd.expected_candidate_.node_id_);
        } else if constexpr (std::is_same_v<Command,
                                            CommitUncontrolledFailover>) {
          event = "cutover";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          loss = cmd.loss_if_cutover_ == MetaFailoverLoss::kNone ? "none"
                                                                 : "unknown";
          data_loss_possible = cmd.loss_if_cutover_ != MetaFailoverLoss::kNone;
          detail = " candidate=" + LogToken(cmd.expected_candidate_.node_id_);
        } else {
          return std::nullopt;
        }

        return FailoverCommitLog{
            .message_ = absl::StrCat("failover event=", event, " mode=", mode,
                                     " group=", LogToken(group),
                                     " transition=", transition,
                                     " action=", action, " loss=", loss,
                                     " commit_index=", commit_index, detail),
            .data_loss_possible_ = data_loss_possible,
        };
      },
      command);
}

void PutLe32(std::vector<uint8_t>& out, uint32_t value) {
  out.push_back(static_cast<uint8_t>(value));
  out.push_back(static_cast<uint8_t>(value >> 8));
  out.push_back(static_cast<uint8_t>(value >> 16));
  out.push_back(static_cast<uint8_t>(value >> 24));
}

// Bounds-checked little-endian cursor over a loaded byte range.
class LeReader {
 public:
  LeReader(const uint8_t* data, size_t size) : data_(data), size_(size) {}

  bool U32(uint32_t& out) {
    if (pos_ + 4 > size_) return false;
    out = static_cast<uint32_t>(data_[pos_]) |
          (static_cast<uint32_t>(data_[pos_ + 1]) << 8) |
          (static_cast<uint32_t>(data_[pos_ + 2]) << 16) |
          (static_cast<uint32_t>(data_[pos_ + 3]) << 24);
    pos_ += 4;
    return true;
  }

  bool Bytes(size_t len, std::string& out) {
    if (pos_ + len > size_) return false;
    out.assign(reinterpret_cast<const char*>(data_ + pos_), len);
    pos_ += len;
    return true;
  }

  size_t pos() const { return pos_; }

 private:
  const uint8_t* data_;
  size_t size_;
  size_t pos_ = 0;
};

uint32_t Fnv1a32(const uint8_t* data, size_t len) {
  uint32_t hash = 2166136261u;
  for (size_t ii = 0; ii < len; ++ii) {
    hash ^= data[ii];
    hash *= 16777619u;
  }
  return hash;
}

absl::Status ErrnoStatus(const char* op, const std::string& path) {
  return absl::ErrnoToStatus(errno, std::string(op) + " failed on " + path);
}

absl::Status PwriteAll(int fd, const uint8_t* data, size_t len) {
  size_t done = 0;
  while (done < len) {
    ssize_t written = ::pwrite(fd, data + done, len - done, done);
    if (written < 0) {
      if (errno == EINTR) continue;
      return absl::ErrnoToStatus(errno, "pwrite");
    }
    done += static_cast<size_t>(written);
  }
  return absl::OkStatus();
}

absl::Status FsyncDirectory(const std::string& dir) {
  int dir_fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dir_fd < 0) return ErrnoStatus("open(dir)", dir);
  int rc = ::fsync(dir_fd);
  int saved_errno = errno;
  ::close(dir_fd);
  if (rc < 0) {
    errno = saved_errno;
    return ErrnoStatus("fsync(dir)", dir);
  }
  return absl::OkStatus();
}

std::string SnapshotFileName(uint64_t log_idx) {
  return "snapshot_" + std::to_string(log_idx) + ".dat";
}

// Frames one snapshot file image (see the header for the layout): magic |
// meta_len | meta | payload_len | payload | checksum over the framed body.
std::vector<uint8_t> FrameSnapshotImage(const nuraft::buffer& meta_blob,
                                        const std::string& envelope) {
  std::vector<uint8_t> image;
  image.reserve(4 + 4 + meta_blob.size() + 4 + envelope.size() + 4);
  PutLe32(image, kSnapshotMagic);
  PutLe32(image, static_cast<uint32_t>(meta_blob.size()));
  image.insert(image.end(), meta_blob.data_begin(),
               meta_blob.data_begin() + meta_blob.size());
  PutLe32(image, static_cast<uint32_t>(envelope.size()));
  image.insert(image.end(), envelope.begin(), envelope.end());
  PutLe32(image, Fnv1a32(image.data() + 4, image.size() - 4));
  return image;
}

}  // namespace

MetaStateMachine::MetaStateMachine(std::string data_dir)
    : data_dir_(std::move(data_dir)) {}

MetaStateMachine::~MetaStateMachine() {
  {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    writer_stopping_ = true;
  }
  writer_cv_.notify_one();
  if (writer_thread_.joinable()) writer_thread_.join();
}

absl::StatusOr<std::unique_ptr<MetaStateMachine>> MetaStateMachine::Open(
    const std::string& data_dir) {
  if (::mkdir(data_dir.c_str(), 0755) < 0 && errno != EEXIST) {
    return ErrnoStatus("mkdir", data_dir);
  }

  std::unique_ptr<MetaStateMachine> machine(new MetaStateMachine(data_dir));
  if (auto status = machine->LoadLatestSnapshot(); !status.ok()) return status;

  try {
    machine->writer_thread_ =
        std::thread(&MetaStateMachine::WriterMain, machine.get());
  } catch (const std::system_error& e) {
    return absl::Status(
        absl::StatusCode::kInternal,
        std::string("snapshot writer thread start failed: ") + e.what());
  }
  return machine;
}

absl::Status MetaStateMachine::LoadLatestSnapshot() {
  // Find the newest intact snapshot file; only the latest (plus in-flight
  // pins) is ever retained, so at most one candidate is expected.
  uint64_t latest_idx = 0;
  bool found = false;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
    const std::string name = entry.path().filename().string();
    if (name.rfind("snapshot_", 0) != 0) continue;
    if (name.size() < 5 || name.compare(name.size() - 4, 4, ".dat") != 0) {
      continue;  // includes leftover snapshot_*.dat.tmp files
    }
    const std::string digits = name.substr(9, name.size() - 9 - 4);
    uint64_t idx = 0;
    try {
      idx = std::stoull(digits);
    } catch (...) {
      continue;  // not one of ours
    }
    if (!found || idx > latest_idx) {
      latest_idx = idx;
      found = true;
    }
  }

  if (found) {
    SnapshotData data;
    absl::Status status = ReadSnapshotFileLocked(latest_idx, data);
    if (!status.ok()) return status;
    // A snapshot whose envelope does not decode is fail-stop-class corruption
    // (MetaStores::Deserialize is strict; the same bytes fail identically on
    // every node) — boot refuses the directory loudly.
    absl::StatusOr<MetaStores> stores = MetaStores::Deserialize(data.envelope_);
    if (!stores.ok()) return stores.status();
    stores_ = std::move(*stores);
    last_snapshot_ = data.snapshot_;
    snapshots_[latest_idx] = std::move(data);
    last_committed_idx_ = latest_idx;
    last_state_change_idx_ = latest_idx;
  }

  return absl::OkStatus();
}

absl::StatusOr<std::optional<MetaSnapshotMembership>>
MetaStateMachine::ReadSnapshotMembership(const std::string& data_dir) {
  // The startup-only second read avoids accepting config evidence from a
  // corrupt payload, without starting background work or retaining two stores.
  auto reader =
      std::unique_ptr<MetaStateMachine>(new MetaStateMachine(data_dir));
  if (auto status = reader->LoadLatestSnapshot(); !status.ok()) return status;
  if (reader->last_snapshot_ == nullptr) return std::nullopt;
  return MetaSnapshotMembership{reader->last_snapshot_->get_last_log_idx(),
                                reader->last_snapshot_->get_last_config(),
                                reader->stores_.identity_.MetaMembers()};
}

void MetaStateMachine::AttachStateMgr(std::weak_ptr<NuraftStateMgr> state_mgr) {
  std::lock_guard<std::mutex> lock(mutex_);
  state_mgr_ = std::move(state_mgr);
}

MetaStores MetaStateMachine::StoresSnapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stores_;
}

MetaCommittedStatusView MetaStateMachine::StatusSnapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  MetaCommittedStatusView view;
  view.applied_index_ = last_committed_idx_.load(std::memory_order_relaxed);
  view.topology_epoch_ = stores_.topology_.TopologyEpoch();
  view.cluster_lifecycle_ = stores_.topology_.ClusterLifecycle();
  view.cluster_non_pristine_ =
      view.cluster_lifecycle_.state_ == MetaClusterLifecycle::kUninitialized &&
      HasDataClusterArtifacts(stores_);
  view.active_cluster_create_operation_ =
      view.cluster_lifecycle_.state_ == MetaClusterLifecycle::kCreating;
  if (view.active_cluster_create_operation_) {
    const auto operation = stores_.operation_.FindOperation(
        view.cluster_lifecycle_.root_operation_id_);
    if (operation.has_value()) {
      view.active_cluster_create_phase_ = operation->kind_phase_blob_;
      MetaOperationId intent_root{};
      auto manifest =
          DecodeClusterCreateRequest(operation->intent_, &intent_root);
      if (manifest.ok()) {
        view.active_cluster_create_data_nodes_.reserve(
            manifest->data_nodes_.size());
        for (const auto& node : manifest->data_nodes_) {
          view.active_cluster_create_data_nodes_.push_back(node.node_id_);
        }
      }
    }
  }
  view.meta_members_ = stores_.identity_.MetaMembers();
  view.data_nodes_ = stores_.identity_.Nodes();
  const auto automatic_policy =
      stores_.policy_.CurrentAutomaticUncontrolledFailover();
  const auto authority_lease_policy = stores_.policy_.CurrentAuthorityLease();
  if (automatic_policy.has_value()) {
    view.automatic_failover_threshold_ms_ = automatic_policy->suspect_after_ms_;
  }
  for (MetaTopologyGroupView topology : stores_.topology_.Groups()) {
    auto grant = stores_.topology_.AuthorityFor(topology.group_id_);
    // Cross-store validation guarantees the grant half exists for every
    // topology group; retain a defensive fenced value if corrupted in memory
    // so status reports NOT READY instead of inventing authority.
    MetaCommittedStatusGroup group;
    group.topology_ = std::move(topology);
    if (grant.has_value()) group.grant_ = std::move(*grant);
    const auto& record = group.topology_.record_;
    group.manifest_present_ = record.population_manifest_revision_ != 0 &&
                              stores_.population_manifest_.Contains(
                                  record.population_manifest_digest_);
    group.policy_active_ =
        automatic_policy.has_value() && authority_lease_policy.has_value();
    view.groups_.push_back(std::move(group));
  }
  for (std::uint32_t slot = 0; slot < kMetaSlotCount;) {
    auto owner = stores_.topology_.SlotOwner(slot);
    if (!owner.has_value()) {
      ++slot;
      continue;
    }
    std::uint32_t last = slot;
    while (last + 1 < kMetaSlotCount &&
           stores_.topology_.SlotOwner(last + 1) == owner) {
      ++last;
    }
    view.slot_ranges_.push_back(
        {.first_ = slot, .last_ = last, .group_id_ = std::move(*owner)});
    slot = last + 1;
  }
  return view;
}

void MetaStateMachine::SetCommitEventSink(MetaCommitEventSink sink) {
  std::lock_guard<std::mutex> lock(sink_mutex_);
  commit_event_sink_ = std::move(sink);
}

absl::StatusOr<nuraft::ptr<nuraft::buffer>> MetaStateMachine::EncodeCommand(
    const MetaCommand& command) {
  absl::StatusOr<std::string> encoded = EncodeMetaCommand(command);
  if (!encoded.ok()) return encoded.status();
  nuraft::ptr<nuraft::buffer> out = nuraft::buffer::alloc(encoded->size());
  std::memcpy(out->data_begin(), encoded->data(), encoded->size());
  return out;
}

nuraft::ptr<nuraft::buffer> MetaStateMachine::commit(nuraft::ulong log_idx,
                                                     nuraft::buffer& data) {
  const std::string_view bytes(
      data.size() > 0 ? reinterpret_cast<const char*>(data.data_begin()) : "",
      data.size());
  absl::StatusOr<MetaCommand> decoded = DecodeMetaCommand(bytes);
  if (!decoded.ok()) {
    // A decode failure is fail-stop, strictly
    // separated from a domain rejection (cleanly decoded, refused by
    // ApplyCommitted, index consumed). The same byte sequence fails
    // identically on every node, so aborting here cannot fork the group —
    // this is also how an old binary loudly refuses a newer encoding after an
    // upgrade.
    spdlog::critical(
        "meta state machine: undecodable committed command at {}: {}", log_idx,
        decoded.status().message());
    std::abort();
  }

  // The trusted entry's injected ActorContext rides the command struct and
  // the raft-log encoding (commands.h), so the decoded command carries
  // the same actor on every node; apply only copies it into audit/journal.
  // Unforgeability is enforced at the ctl/coordinator entry layer, not here.
  const ActorContext actor =
      std::visit([](const auto& cmd) { return cmd.actor_; }, *decoded);
  MetaApplyResult applied;
  std::optional<FailoverCommitLog> failover_log;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    failover_log = DescribeFailoverCommit(*decoded, stores_, log_idx);
    applied = ApplyCommitted(stores_, log_idx, *decoded, actor.principal_,
                             actor.readable_time_);
    // Publish the cursor while the same state lock still protects the effects
    // it names. Readers cannot copy stores_ until the cursor and sink event
    // for this commit are both visible.
    last_committed_idx_ = log_idx;
    last_state_change_idx_ = log_idx;
    if (const auto manager = state_mgr_.lock()) {
      if (auto status =
              manager->ReconcileCommittedBindings(stores_.identity_, log_idx);
          !status.ok()) {
        spdlog::critical("committed Meta binding reconciliation failed: {}",
                         status.message());
        std::abort();
      }
    }
    // Coordinator commit-event sink: still under the state mutex, so consumers
    // observe the event atomically with the apply. The sink contract
    // (state_machine.h) keeps this O(1) and non-blocking.
    std::lock_guard<std::mutex> sink_lock(sink_mutex_);
    if (commit_event_sink_) {
      commit_event_sink_(log_idx, applied);
    }
  }
  if (failover_log.has_value() &&
      applied.verdict_ == MetaAuditVerdict::kAccepted) {
    if (failover_log->data_loss_possible_) {
      spdlog::warn("{}", failover_log->message_);
    } else {
      spdlog::info("{}", failover_log->message_);
    }
  }
  const std::string completion = EncodeMetaApplyResult(applied);
  nuraft::ptr<nuraft::buffer> result = nuraft::buffer::alloc(completion.size());
  if (!completion.empty()) {
    std::memcpy(result->data_begin(), completion.data(), completion.size());
  }
  result->pos(0);
  return result;
}

void MetaStateMachine::commit_config(
    nuraft::ulong log_idx, nuraft::ptr<nuraft::cluster_config>& /*new_conf*/) {
  // Membership bindings use ordinary committed commands in a two-phase
  // transition; the configuration entry itself touches no store. NuRaft may
  // deliver a configuration callback after a newer ordinary commit has
  // already advanced the visible cursor, so this hook must never regress it.
  std::lock_guard<std::mutex> lock(mutex_);
  nuraft::ulong current = last_committed_idx_.load(std::memory_order_relaxed);
  while (current < log_idx && !last_committed_idx_.compare_exchange_weak(
                                  current, log_idx, std::memory_order_release,
                                  std::memory_order_relaxed)) {
  }
  if (const auto manager = state_mgr_.lock()) {
    if (auto status = manager->ReconcileCommittedBindings(
            stores_.identity_, last_committed_idx_.load());
        !status.ok()) {
      spdlog::critical("committed config binding reconciliation failed: {}",
                       status.message());
      std::abort();
    }
  }
}

nuraft::ptr<nuraft::snapshot> MetaStateMachine::last_snapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_snapshot_;
}

nuraft::ulong MetaStateMachine::last_commit_index() {
  return last_committed_idx_.load();
}

void MetaStateMachine::create_snapshot(
    nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& when_done) {
  const uint64_t idx = s.get_last_log_idx();

  SnapshotJob job;
  job.idx_ = idx;
  job.when_done_ = when_done;
  {
    // EXACT CUT POINT: serialize the stores under the state mutex,
    // synchronously. Automatic snapshots arrive on the commit thread at a
    // commit boundary; the manual path (ctl `snapshot` in meta_main) must use
    // create_snapshot({serialize_commit_=true}) or
    // schedule_snapshot_creation() so the same exclusion holds there.
    std::lock_guard<std::mutex> lock(mutex_);
    absl::StatusOr<std::string> envelope = stores_.Serialize();
    if (!envelope.ok()) {
      // Size fail-safe: reject this snapshot round (log compaction is
      // skipped with it) and alert; never silently truncate.
      ++consecutive_snapshot_failures_;
      spdlog::error(
          "meta state machine: snapshot {} capture failed: {} (consecutive "
          "failures: {})",
          idx, envelope.status().message(),
          consecutive_snapshot_failures_.load());
      nuraft::ptr<std::exception> err(nullptr);
      bool ok = false;
      when_done(ok, err);
      return;
    }
    job.envelope_ = std::move(*envelope);
    nuraft::ptr<nuraft::buffer> meta_blob = s.serialize();
    job.snapshot_ = nuraft::snapshot::deserialize(*meta_blob);
    job.image_ = FrameSnapshotImage(*meta_blob, job.envelope_);
  }

  {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    writer_queue_.push(std::move(job));
  }
  writer_cv_.notify_one();
}

void MetaStateMachine::WriterMain() {
  while (true) {
    SnapshotJob job;
    {
      std::unique_lock<std::mutex> lock(writer_mutex_);
      writer_cv_.wait(
          lock, [this] { return writer_stopping_ || !writer_queue_.empty(); });
      // The destructor owns teardown; queued jobs drop with their handlers
      // (the raft core that would consume when_done is gone by then — the
      // state machine is destroyed after the core's threads have stopped).
      if (writer_stopping_) return;
      job = std::move(writer_queue_.front());
      writer_queue_.pop();
      writer_job_running_ = true;
    }

    absl::Status status = WriteSnapshotImage(job.idx_, job.image_);
    if (status.ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      SnapshotData data;
      data.snapshot_ = job.snapshot_;
      data.envelope_ = std::move(job.envelope_);
      snapshots_[job.idx_] = std::move(data);
      last_snapshot_ = snapshots_[job.idx_].snapshot_;
      consecutive_snapshot_failures_ = 0;
      PruneSnapshotsLocked(job.idx_);
    } else {
      // Rejecting only skips this snapshot round (and with it log
      // compaction); no acked data depends on it.
      ++consecutive_snapshot_failures_;
      spdlog::error(
          "meta state machine: snapshot {} write failed: {} (consecutive "
          "failures: {})",
          job.idx_, status.message(), consecutive_snapshot_failures_.load());
    }

    bool ok = status.ok();
    nuraft::ptr<std::exception> err(nullptr);
    job.when_done_(ok, err);

    {
      std::lock_guard<std::mutex> lock(writer_mutex_);
      writer_job_running_ = false;
      if (writer_queue_.empty()) writer_idle_cv_.notify_all();
    }
  }
}

void MetaStateMachine::WaitForSnapshotWriterIdle() {
  std::unique_lock<std::mutex> lock(writer_mutex_);
  writer_idle_cv_.wait(
      lock, [this] { return writer_queue_.empty() && !writer_job_running_; });
}

// Read cursor for one in-flight snapshot sync stream; user_snp_ctx points at
// this between calls. Object ids are chunk offsets by construction, so the
// cursor only carries the pin bookkeeping.
struct SnapshotReadCursor {
  uint64_t snapshot_idx_;
};

int MetaStateMachine::read_logical_snp_obj(
    nuraft::snapshot& s, void*& user_snp_ctx, nuraft::ulong obj_id,
    nuraft::ptr<nuraft::buffer>& data_out, bool& is_last_obj) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t idx = s.get_last_log_idx();
  auto found = snapshots_.find(idx);
  if (found == snapshots_.end()) {
    // The core treats a negative return as a send failure and retries later.
    data_out = nullptr;
    is_last_obj = true;
    return -1;
  }
  const std::string& envelope = found->second.envelope_;

  // Opening the cursor pins its snapshot: create_snapshot() would otherwise
  // prune it mid-stream, the core would reset the sync context on the failed
  // read, and the follower would restart from object zero with the newest
  // snapshot on every snapshot round — never completing one while the
  // snapshot interval is shorter than the stream time.
  auto* cursor = static_cast<SnapshotReadCursor*>(user_snp_ctx);
  if (cursor == nullptr) {
    cursor = new SnapshotReadCursor{idx};
    ++pinned_snapshots_[idx];
    user_snp_ctx = cursor;
  }

  // Object N is the Nth chunk of the envelope; overflow-safe past-end guard.
  if (obj_id > UINT64_MAX / kSnapshotObjectBytes ||
      obj_id * kSnapshotObjectBytes >= envelope.size()) {
    data_out = nuraft::buffer::alloc(0);
    is_last_obj = true;
    return 0;
  }
  const uint64_t offset = obj_id * kSnapshotObjectBytes;
  const size_t chunk = static_cast<size_t>(
      std::min<uint64_t>(kSnapshotObjectBytes, envelope.size() - offset));
  data_out = nuraft::buffer::alloc(chunk);
  std::memcpy(data_out->data_begin(), envelope.data() + offset, chunk);
  is_last_obj = (offset + chunk == envelope.size());
  return 0;
}

void MetaStateMachine::save_logical_snp_obj(nuraft::snapshot& s,
                                            nuraft::ulong& obj_id,
                                            nuraft::buffer& data,
                                            bool is_first_obj,
                                            bool is_last_obj) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_first_obj) {
      // Stream (re)start — also how the leader's sync-ctx timeout/retry and
      // snapshot rollover begin a fresh assembly.
      receiving_bytes_.clear();
      receiving_idx_ = s.get_last_log_idx();
      receiving_next_obj_ = 0;
    }
    // Append only the next expected object of the stream being assembled:
    // duplicates (leader resends, timeout retries) and objects of a
    // superseded snapshot are dropped. See the header for why NuRaft can
    // deliver the same object repeatedly.
    const bool consumed =
        s.get_last_log_idx() == receiving_idx_ && obj_id == receiving_next_obj_;
    if (consumed && data.size() > 0) {
      receiving_bytes_.append(reinterpret_cast<const char*>(data.data_begin()),
                              data.size());
      ++receiving_next_obj_;
    }

    if (is_last_obj && consumed) {
      // The stream completed in sequence — the assembly is the whole
      // envelope. Receive-side durability precedes apply: the file
      // must exist before apply_snapshot() loads it. An out-of-sequence
      // is_last (a duplicate, or a superseded stream's tail) writes nothing;
      // apply_snapshot() then still finds the completed stream's file.
      nuraft::ptr<nuraft::buffer> meta_blob = s.serialize();
      std::vector<uint8_t> image =
          FrameSnapshotImage(*meta_blob, receiving_bytes_);
      absl::Status status = WriteSnapshotImage(receiving_idx_, image);
      if (!status.ok()) {
        // The file never materialized, so the later apply_snapshot() returns
        // false and the core escalates via state_mgr::system_exit.
        spdlog::error(
            "meta state machine: received snapshot {} write failed: {}",
            receiving_idx_, status.message());
      }
      receiving_bytes_.clear();
    }
  }
  // Acknowledge with the assembly cursor — the object the leader should send
  // next. Pull semantics: a stale or duplicate response can only move the
  // leader's sync-ctx offset BACK to the true position, never past a hole.
  obj_id = receiving_next_obj_;
}

void MetaStateMachine::free_user_snp_ctx(void*& user_snp_ctx) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto* cursor = static_cast<SnapshotReadCursor*>(user_snp_ctx);
  if (cursor != nullptr) {
    auto pin = pinned_snapshots_.find(cursor->snapshot_idx_);
    if (pin != pinned_snapshots_.end() && --pin->second == 0) {
      pinned_snapshots_.erase(pin);
    }
    delete cursor;
  }
  user_snp_ctx = nullptr;
}

bool MetaStateMachine::apply_snapshot(nuraft::snapshot& s) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t idx = s.get_last_log_idx();
  SnapshotData data;
  if (!ReadSnapshotFileLocked(idx, data).ok()) {
    spdlog::error("meta state machine: no durable snapshot file for {}", idx);
    return false;
  }
  absl::StatusOr<MetaStores> stores = MetaStores::Deserialize(data.envelope_);
  if (!stores.ok()) {
    // Fail-stop-class corruption (strict decode, identical on every node);
    // returning false is the core's designed escalation into system_exit.
    spdlog::error("meta state machine: snapshot {} decode failed: {}", idx,
                  stores.status().message());
    return false;
  }
  // The received snapshot file is already durable, so it is also the redo
  // authority if a crash interrupts config/baseline/marker publication here.
  LAVIK_MAYBE_CRASH_AT("meta-snapshot-before-membership");
  if (const auto manager = state_mgr_.lock()) {
    const MetaSnapshotMembership membership{idx,
                                            data.snapshot_->get_last_config(),
                                            stores->identity_.MetaMembers()};
    if (auto status = manager->InstallSnapshotMembership(membership);
        !status.ok()) {
      spdlog::error("snapshot membership installation failed: {}",
                    status.message());
      return false;
    }
  }
  stores_ = std::move(*stores);
  last_snapshot_ = data.snapshot_;
  snapshots_[idx] = std::move(data);
  PruneSnapshotsLocked(idx);
  if (last_committed_idx_ < idx) last_committed_idx_ = idx;
  if (last_state_change_idx_ < idx) last_state_change_idx_ = idx;
  return true;
}

absl::Status MetaStateMachine::WriteSnapshotImage(
    uint64_t log_idx, const std::vector<uint8_t>& image) {
  std::lock_guard<std::mutex> lock(io_mutex_);
  const std::string path = data_dir_ + "/" + SnapshotFileName(log_idx);
  const std::string tmp_path = path + ".tmp";
  int fd =
      ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) return ErrnoStatus("open", tmp_path);
  absl::Status status = PwriteAll(fd, image.data(), image.size());
  if (status.ok() && ::fdatasync(fd) < 0) {
    status = ErrnoStatus("fdatasync", tmp_path);
  }
  int close_rc = ::close(fd);
  if (status.ok() && close_rc < 0) status = ErrnoStatus("close", tmp_path);
  if (!status.ok()) return status;

  if (::rename(tmp_path.c_str(), path.c_str()) < 0) {
    return ErrnoStatus("rename", path);
  }
  return FsyncDirectory(data_dir_);
}

absl::Status MetaStateMachine::ReadSnapshotFileLocked(uint64_t log_idx,
                                                      SnapshotData& data) {
  std::lock_guard<std::mutex> io_lock(io_mutex_);
  const std::string path = data_dir_ + "/" + SnapshotFileName(log_idx);
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return ErrnoStatus("open", path);
  std::vector<uint8_t> file;
  uint8_t chunk[4096];
  while (true) {
    ssize_t got = ::read(fd, chunk, sizeof(chunk));
    if (got < 0) {
      if (errno == EINTR) continue;
      absl::Status status = ErrnoStatus("read", path);
      ::close(fd);
      return status;
    }
    if (got == 0) break;
    file.insert(file.end(), chunk, chunk + got);
  }
  ::close(fd);

  if (file.size() < 12) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "snapshot file too short: " + path);
  }
  LeReader reader(file.data(), file.size());
  uint32_t magic = 0;
  if (!reader.U32(magic) || magic != kSnapshotMagic) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "bad snapshot magic: " + path);
  }
  const size_t body_len = file.size() - 8;
  uint32_t stored_checksum = 0;
  {
    // Checksum covers everything between magic and itself.
    LeReader tail(file.data() + 4 + body_len, 4);
    if (!tail.U32(stored_checksum)) {
      return absl::Status(absl::StatusCode::kDataLoss,
                          "truncated snapshot checksum: " + path);
    }
  }
  if (Fnv1a32(file.data() + 4, body_len) != stored_checksum) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "snapshot checksum mismatch: " + path);
  }

  LeReader body(file.data() + 4, body_len);
  uint32_t meta_len = 0;
  std::string meta_bytes;
  if (!body.U32(meta_len) || !body.Bytes(meta_len, meta_bytes)) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "truncated snapshot meta: " + path);
  }
  nuraft::ptr<nuraft::buffer> meta_buf = nuraft::buffer::alloc(meta_len);
  std::memcpy(meta_buf->data_begin(), meta_bytes.data(), meta_len);
  data.snapshot_ = nuraft::snapshot::deserialize(*meta_buf);
  if (!data.snapshot_ || data.snapshot_->get_last_log_idx() != log_idx) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "snapshot meta index mismatch: " + path);
  }

  uint32_t payload_len = 0;
  if (!body.U32(payload_len) || !body.Bytes(payload_len, data.envelope_)) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "truncated snapshot payload: " + path);
  }
  if (body.pos() != body_len) {
    return absl::Status(absl::StatusCode::kDataLoss,
                        "trailing snapshot bytes: " + path);
  }
  return absl::OkStatus();
}

void MetaStateMachine::PruneSnapshotsLocked(uint64_t keep_idx) {
  for (auto it = snapshots_.begin(); it != snapshots_.end();) {
    if (it->first == keep_idx ||
        pinned_snapshots_.find(it->first) != pinned_snapshots_.end()) {
      ++it;
      continue;
    }
    const std::string path = data_dir_ + "/" + SnapshotFileName(it->first);
    if (::unlink(path.c_str()) < 0 && errno != ENOENT) {
      // A leftover old snapshot file only wastes space; Open() always picks
      // the newest intact one, so this is not a correctness problem.
      spdlog::warn("meta state machine: failed to remove {}: {}", path,
                   std::strerror(errno));
    }
    it = snapshots_.erase(it);
  }
}

}  // namespace lavik::meta
