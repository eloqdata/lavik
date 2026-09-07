#include "tests/cluster/reference_model.h"

#include <algorithm>
#include <array>
#include <set>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "tests/cluster/fault_controls.h"

namespace keylane::test::cluster {
namespace {

constexpr std::array<ScenarioDescriptor, 9> kScenarioDescriptors{
    ScenarioDescriptor{Counterexample::kNone, "good", "cluster-reference-good",
                       ""},
    ScenarioDescriptor{Counterexample::kDualAuthority, "dual-authority",
                       "cluster-mutant-dual-authority",
                       "authority.single-writer"},
    ScenarioDescriptor{Counterexample::kStaleEvidence, "stale-evidence",
                       "cluster-mutant-stale-evidence",
                       "replication.restart-invalidates-evidence"},
    ScenarioDescriptor{Counterexample::kHistoryGap, "history-gap",
                       "cluster-mutant-history-gap",
                       "replication.reparent-requires-complete-history"},
    ScenarioDescriptor{Counterexample::kPartialActivation, "partial-activation",
                       "cluster-mutant-partial-activation",
                       "population.atomic-activation"},
    ScenarioDescriptor{Counterexample::kStaleDirective, "stale-directive",
                       "cluster-mutant-stale-directive",
                       "meta.directive-evidence-scoped"},
    ScenarioDescriptor{Counterexample::kCatalogAckBeforeDurable,
                       "catalog-ack-before-durable",
                       "cluster-mutant-catalog-ack-before-durable",
                       "function.catalog-durable-before-ack"},
    ScenarioDescriptor{Counterexample::kFullSyncRetainsOldState,
                       "fullsync-retains-old-state",
                       "cluster-mutant-fullsync-retains-old-state",
                       "fullsync.destructive-invalidation-before-transfer"},
    ScenarioDescriptor{Counterexample::kStaleCatalogPromotion,
                       "stale-catalog-promotion",
                       "cluster-mutant-stale-catalog-promotion",
                       "promotion.catalog-token-current"},
};

const ScenarioDescriptor& DescriptorFor(Counterexample counterexample) {
  const auto found =
      std::find_if(kScenarioDescriptors.begin(), kScenarioDescriptors.end(),
                   [&](const ScenarioDescriptor& descriptor) {
                     return descriptor.counterexample_ == counterexample;
                   });
  return found == kScenarioDescriptors.end() ? kScenarioDescriptors.front()
                                             : *found;
}

bool CanExerciseAuthority(const AuthorityObservation& observation) {
  return observation.can_admit_write_ || observation.can_complete_inflight_ ||
         observation.can_mutate_in_background_ ||
         observation.can_decide_success_;
}

// The good scenario composes every deterministic control behind the same
// ScenarioWorld seam used for replay. Dependencies encode the safe promotion
// barrier, while the seeded scheduler is free to permute unrelated faults and
// control-plane work.
enum class ExplorationAction : std::uint8_t {
  kAdvanceClock,
  kPartitionLink,
  kHealLink,
  kDuplicateHeartbeat,
  kDeliverMessage,
  kDropDuplicate,
  kDisconnectLink,
  kReconnectLink,
  kInjectWriteFailure,
  kInjectReadFailure,
  kInjectFlushFailure,
  kPersistCandidate,
  kPersistPrefix,
  kCrashStorage,
  kPrepareChildHistory,
  kChangeMetaLeader,
  kReplayMeta,
  kRestoreMetaSnapshot,
  kApplyDirective,
  kReplayDirective,
  kRestartDirectiveTarget,
  kRejectStaleDirective,
  kExpireOldGrant,
  kActivateCandidate,
  kResetOldReplica,
  kRecordClientSuccess,
};

struct ExplorationActionDescriptor {
  ExplorationAction kind_;
  std::string_view name_;
};

constexpr std::array<ExplorationActionDescriptor, 26>
    kExplorationActionDescriptors{
        ExplorationActionDescriptor{ExplorationAction::kAdvanceClock,
                                    "advance-clock"},
        ExplorationActionDescriptor{ExplorationAction::kPartitionLink,
                                    "partition-link"},
        ExplorationActionDescriptor{ExplorationAction::kHealLink, "heal-link"},
        ExplorationActionDescriptor{ExplorationAction::kDuplicateHeartbeat,
                                    "duplicate-heartbeat"},
        ExplorationActionDescriptor{ExplorationAction::kDeliverMessage,
                                    "deliver-message"},
        ExplorationActionDescriptor{ExplorationAction::kDropDuplicate,
                                    "drop-duplicate"},
        ExplorationActionDescriptor{ExplorationAction::kDisconnectLink,
                                    "disconnect-link"},
        ExplorationActionDescriptor{ExplorationAction::kReconnectLink,
                                    "reconnect-link"},
        ExplorationActionDescriptor{ExplorationAction::kInjectWriteFailure,
                                    "inject-write-failure"},
        ExplorationActionDescriptor{ExplorationAction::kInjectReadFailure,
                                    "inject-read-failure"},
        ExplorationActionDescriptor{ExplorationAction::kInjectFlushFailure,
                                    "inject-flush-failure"},
        ExplorationActionDescriptor{ExplorationAction::kPersistCandidate,
                                    "persist-candidate"},
        ExplorationActionDescriptor{ExplorationAction::kPersistPrefix,
                                    "persist-prefix"},
        ExplorationActionDescriptor{ExplorationAction::kCrashStorage,
                                    "crash-storage"},
        ExplorationActionDescriptor{ExplorationAction::kPrepareChildHistory,
                                    "prepare-child-history"},
        ExplorationActionDescriptor{ExplorationAction::kChangeMetaLeader,
                                    "change-meta-leader"},
        ExplorationActionDescriptor{ExplorationAction::kReplayMeta,
                                    "replay-meta"},
        ExplorationActionDescriptor{ExplorationAction::kRestoreMetaSnapshot,
                                    "restore-meta-snapshot"},
        ExplorationActionDescriptor{ExplorationAction::kApplyDirective,
                                    "apply-directive"},
        ExplorationActionDescriptor{ExplorationAction::kReplayDirective,
                                    "replay-directive"},
        ExplorationActionDescriptor{ExplorationAction::kRestartDirectiveTarget,
                                    "restart-directive-target"},
        ExplorationActionDescriptor{ExplorationAction::kRejectStaleDirective,
                                    "reject-stale-directive"},
        ExplorationActionDescriptor{ExplorationAction::kExpireOldGrant,
                                    "expire-old-grant"},
        ExplorationActionDescriptor{ExplorationAction::kActivateCandidate,
                                    "activate-candidate"},
        ExplorationActionDescriptor{ExplorationAction::kResetOldReplica,
                                    "reset-old-replica"},
        ExplorationActionDescriptor{ExplorationAction::kRecordClientSuccess,
                                    "record-client-success"},
};

std::string_view ExplorationActionName(ExplorationAction kind) {
  const auto found =
      std::find_if(kExplorationActionDescriptors.begin(),
                   kExplorationActionDescriptors.end(),
                   [&](const ExplorationActionDescriptor& descriptor) {
                     return descriptor.kind_ == kind;
                   });
  return found == kExplorationActionDescriptors.end() ? "unknown"
                                                      : found->name_;
}

std::optional<ExplorationAction> ParseExplorationAction(std::string_view name) {
  const auto found =
      std::find_if(kExplorationActionDescriptors.begin(),
                   kExplorationActionDescriptors.end(),
                   [&](const ExplorationActionDescriptor& descriptor) {
                     return descriptor.name_ == name;
                   });
  return found == kExplorationActionDescriptors.end()
             ? std::nullopt
             : std::optional<ExplorationAction>{found->kind_};
}

std::string ExplorationCheckpoint(ExplorationAction kind) {
  return absl::StrCat("cluster.", ExplorationActionName(kind));
}

class ExplorationWorld final : public ScenarioWorld {
 public:
  ExplorationWorld()
      : faults_(std::vector<FaultRule>{
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kPartitionLink),
                      .effect_ = FaultEffect::kDelay},
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kInjectWriteFailure),
                      .effect_ = FaultEffect::kFailBefore},
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kInjectReadFailure),
                      .effect_ = FaultEffect::kFailBefore},
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kInjectFlushFailure),
                      .effect_ = FaultEffect::kFailBefore},
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kPersistPrefix),
                      .effect_ = FaultEffect::kPersistPrefix,
                      .argument_ = 3},
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kCrashStorage),
                      .effect_ = FaultEffect::kCrash},
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kDropDuplicate),
                      .effect_ = FaultEffect::kFailAfter},
            FaultRule{.checkpoint_ = ExplorationCheckpoint(
                          ExplorationAction::kDisconnectLink),
                      .effect_ = FaultEffect::kFailAfter},
        }) {
    snapshot_.authorities_ = {
        AuthorityObservation{
            .group_ = GroupId{1},
            .node_ = NodeId{1},
            .boot_ = BootId{1},
            .term_ = GroupTerm{1},
            .grant_ = GrantId{1},
            .can_admit_write_ = true,
            .can_complete_inflight_ = true,
            .can_mutate_in_background_ = true,
            .can_decide_success_ = true,
        },
        AuthorityObservation{
            .group_ = GroupId{1},
            .node_ = NodeId{2},
            .boot_ = BootId{1},
            .term_ = GroupTerm{2},
            .grant_ = GrantId{2},
        },
    };
    snapshot_.promotion_.candidate_selected_ = true;
    snapshot_.promotion_.promotion_base_committed_ = false;
    snapshot_.promotion_.population_token_valid_ = true;
    snapshot_.promotion_.captured_catalog_generation_ = CatalogGeneration{1};
    snapshot_.promotion_.current_catalog_generation_ = CatalogGeneration{1};
    snapshot_.resume_ = ResumeObservation{
        .source_boot_ = BootId{1},
        .target_boot_ = BootId{1},
        .evidence_source_boot_ = BootId{1},
        .evidence_target_boot_ = BootId{1},
    };
    snapshot_.meta_ = MetaObservation{
        .committed_topology_ = TopologyEpoch{1},
        .installed_topology_ = TopologyEpoch{1},
        .committed_term_ = GroupTerm{1},
        .installed_term_ = GroupTerm{1},
        .current_operation_ = OperationId{1},
        .directive_operation_ = OperationId{1},
        .current_boot_ = BootId{1},
        .evidence_boot_ = BootId{1},
        .serving_state_published_ = true,
        .directive_applied_ = false,
    };
    snapshot_.server_control_reply_ = "-CLUSTERDOWN no safe owner";

    heartbeat_id_ = network_.Send(NodeId{1}, NodeId{2}, "grant-heartbeat");
    metadata_id_ = network_.Send(NodeId{2}, NodeId{1}, "meta-commit");
    pending_message_ids_.insert(heartbeat_id_);
    pending_message_ids_.insert(metadata_id_);
    (void)network_.Delay(heartbeat_id_, kGrantDeadline);
    (void)candidate_storage_.Write("candidate", "ready");
    (void)meta_.AppendCommitted(
        MetaEntry{.index_ = 1, .term_ = GroupTerm{1}, .payload_ = "topology"});
  }

  std::vector<Action> EnabledActions() const override {
    std::vector<Action> actions;
    auto add = [&](bool enabled, ExplorationAction kind) {
      if (enabled) {
        actions.push_back(
            Action{.name_ = std::string(ExplorationActionName(kind)),
                   .arguments_ = {},
                   .payload_ = {}});
      }
    };
    add(!clock_advanced_, ExplorationAction::kAdvanceClock);
    add(!partition_exercised_, ExplorationAction::kPartitionLink);
    add(link_partitioned_, ExplorationAction::kHealLink);
    add(!heartbeat_duplicated_, ExplorationAction::kDuplicateHeartbeat);
    add(!write_failure_exercised_, ExplorationAction::kInjectWriteFailure);
    add(!read_failure_exercised_, ExplorationAction::kInjectReadFailure);
    add(!flush_failure_exercised_, ExplorationAction::kInjectFlushFailure);
    add(!candidate_durable_, ExplorationAction::kPersistCandidate);
    add(!prefix_persisted_, ExplorationAction::kPersistPrefix);
    add(prefix_persisted_ && !prefix_recovery_exercised_,
        ExplorationAction::kCrashStorage);
    add(!child_history_ready_, ExplorationAction::kPrepareChildHistory);
    add(!meta_leader_changed_, ExplorationAction::kChangeMetaLeader);
    add(!meta_replayed_, ExplorationAction::kReplayMeta);
    add(meta_replayed_ && !meta_restored_,
        ExplorationAction::kRestoreMetaSnapshot);
    add(!directive_applied_, ExplorationAction::kApplyDirective);
    add(directive_applied_ && !directive_replayed_,
        ExplorationAction::kReplayDirective);
    add(directive_replayed_ && !directive_target_restarted_,
        ExplorationAction::kRestartDirectiveTarget);
    add(directive_target_restarted_ && !stale_directive_rejected_,
        ExplorationAction::kRejectStaleDirective);

    for (const NetworkMessage& message : network_.Deliverable(clock_.now())) {
      const bool heartbeat_ready =
          message.id_ != heartbeat_id_ || heartbeat_duplicated_;
      if (heartbeat_ready) {
        actions.push_back(Action{
            .name_ = std::string(
                ExplorationActionName(ExplorationAction::kDeliverMessage)),
            .arguments_ = {static_cast<std::int64_t>(message.id_)},
            .payload_ = {}});
      }
    }
    add(duplicate_id_.has_value() &&
            pending_message_ids_.contains(*duplicate_id_),
        ExplorationAction::kDropDuplicate);
    add(partition_exercised_ && !link_partitioned_ && heartbeat_duplicated_ &&
            pending_message_ids_.empty() && !disconnect_exercised_,
        ExplorationAction::kDisconnectLink);
    add(link_disconnected_, ExplorationAction::kReconnectLink);

    const bool failover_barrier =
        clock_advanced_ && partition_exercised_ && !link_partitioned_ &&
        heartbeat_duplicated_ && disconnect_exercised_ && reconnected_ &&
        !link_disconnected_ && pending_message_ids_.empty() &&
        write_failure_exercised_ && read_failure_exercised_ &&
        flush_failure_exercised_ && candidate_durable_ &&
        prefix_recovery_exercised_ && child_history_ready_ &&
        meta_leader_changed_ && meta_replayed_ && meta_restored_ &&
        stale_directive_rejected_;
    add(failover_barrier && !old_authority_fenced_,
        ExplorationAction::kExpireOldGrant);
    add(old_authority_fenced_ && !candidate_activated_,
        ExplorationAction::kActivateCandidate);
    add(candidate_activated_ && !replica_reset_,
        ExplorationAction::kResetOldReplica);
    add(candidate_activated_ && !client_result_recorded_,
        ExplorationAction::kRecordClientSuccess);
    return actions;
  }

  absl::Status Apply(const Action& action) override {
    const std::optional<ExplorationAction> kind =
        ParseExplorationAction(action.name_);
    if (!kind.has_value()) {
      return absl::InvalidArgumentError("unknown exploration action");
    }
    const FaultDecision decision = faults_.Reach(ExplorationCheckpoint(*kind));
    if (*kind == ExplorationAction::kAdvanceClock) {
      absl::Status advanced = clock_.AdvanceTo(kGrantDeadline);
      if (!advanced.ok()) return advanced;
      clock_advanced_ = true;
    } else if (*kind == ExplorationAction::kPartitionLink) {
      if (decision.effect_ != FaultEffect::kDelay) {
        return absl::InternalError("partition fault rule did not fire");
      }
      network_.Partition(NodeId{1}, NodeId{2});
      partition_exercised_ = true;
      link_partitioned_ = true;
    } else if (*kind == ExplorationAction::kHealLink) {
      network_.Heal(NodeId{1}, NodeId{2});
      link_partitioned_ = false;
    } else if (*kind == ExplorationAction::kDuplicateHeartbeat) {
      auto duplicate = network_.Duplicate(heartbeat_id_);
      if (!duplicate.ok()) return duplicate.status();
      duplicate_id_ = *duplicate;
      pending_message_ids_.insert(*duplicate);
      heartbeat_duplicated_ = true;
    } else if (*kind == ExplorationAction::kDeliverMessage) {
      if (action.arguments_.size() != 1 || action.arguments_.front() < 0) {
        return absl::InvalidArgumentError("invalid delivery action");
      }
      const auto id = static_cast<std::uint64_t>(action.arguments_.front());
      auto delivered = network_.Deliver(id, clock_.now());
      if (!delivered.ok()) return delivered.status();
      pending_message_ids_.erase(id);
    } else if (*kind == ExplorationAction::kDropDuplicate) {
      if (decision.effect_ != FaultEffect::kFailAfter ||
          !duplicate_id_.has_value()) {
        return absl::InternalError("drop fault rule did not fire");
      }
      absl::Status dropped = network_.Drop(*duplicate_id_);
      if (!dropped.ok()) return dropped;
      pending_message_ids_.erase(*duplicate_id_);
    } else if (*kind == ExplorationAction::kDisconnectLink) {
      if (decision.effect_ != FaultEffect::kFailAfter) {
        return absl::InternalError("disconnect fault rule did not fire");
      }
      network_.Disconnect(NodeId{1}, NodeId{2});
      disconnect_exercised_ = true;
      link_disconnected_ = true;
    } else if (*kind == ExplorationAction::kReconnectLink) {
      network_.Reconnect(NodeId{1}, NodeId{2});
      link_disconnected_ = false;
      reconnected_ = true;
      pending_message_ids_.insert(
          network_.Send(NodeId{2}, NodeId{1}, "post-reconnect"));
    } else if (*kind == ExplorationAction::kInjectWriteFailure) {
      if (decision.effect_ != FaultEffect::kFailBefore) {
        return absl::InternalError("write fault rule did not fire");
      }
      candidate_storage_.FailNextWrite();
      if (candidate_storage_.Write("discarded", "value").ok()) {
        return absl::InternalError("injected write unexpectedly succeeded");
      }
      write_failure_exercised_ = true;
    } else if (*kind == ExplorationAction::kInjectReadFailure) {
      if (decision.effect_ != FaultEffect::kFailBefore) {
        return absl::InternalError("read fault rule did not fire");
      }
      candidate_storage_.FailNextRead();
      if (candidate_storage_.Read("candidate").ok()) {
        return absl::InternalError("injected read unexpectedly succeeded");
      }
      read_failure_exercised_ = true;
    } else if (*kind == ExplorationAction::kInjectFlushFailure) {
      if (decision.effect_ != FaultEffect::kFailBefore) {
        return absl::InternalError("flush fault rule did not fire");
      }
      candidate_storage_.FailNextFlush();
      if (candidate_storage_.Flush("candidate").ok()) {
        return absl::InternalError("injected flush unexpectedly succeeded");
      }
      flush_failure_exercised_ = true;
    } else if (*kind == ExplorationAction::kPersistCandidate) {
      absl::Status flushed = candidate_storage_.Flush("candidate");
      if (!flushed.ok()) return flushed;
      candidate_durable_ = true;
      snapshot_.promotion_.durability_barrier_complete_ = true;
      snapshot_.promotion_.promotion_base_committed_ = true;
    } else if (*kind == ExplorationAction::kPersistPrefix) {
      if (decision.effect_ != FaultEffect::kPersistPrefix) {
        return absl::InternalError("prefix fault rule did not fire");
      }
      absl::Status written = scratch_storage_.Write("staging", "staging");
      if (!written.ok()) return written;
      absl::Status persisted = scratch_storage_.PersistPrefix(
          "staging", static_cast<std::size_t>(decision.argument_));
      if (!persisted.ok()) return persisted;
      prefix_persisted_ = true;
    } else if (*kind == ExplorationAction::kCrashStorage) {
      if (decision.effect_ != FaultEffect::kCrash) {
        return absl::InternalError("crash fault rule did not fire");
      }
      scratch_storage_.CrashAndRecover();
      auto recovered = scratch_storage_.Read("staging");
      if (!recovered.ok() || !recovered->has_value() || **recovered != "sta") {
        return absl::InternalError("partial persistence was not recovered");
      }
      prefix_recovery_exercised_ = true;
    } else if (*kind == ExplorationAction::kPrepareChildHistory) {
      child_history_ready_ = true;
      snapshot_.promotion_.child_history_ready_ = true;
    } else if (*kind == ExplorationAction::kChangeMetaLeader) {
      meta_.ChangeLeader(NodeId{2});
      meta_leader_changed_ = true;
    } else if (*kind == ExplorationAction::kReplayMeta) {
      absl::Status replayed = meta_.Replay(
          {MetaEntry{
               .index_ = 1, .term_ = GroupTerm{1}, .payload_ = "topology"},
           MetaEntry{
               .index_ = 2, .term_ = GroupTerm{2}, .payload_ = "promotion"}});
      if (!replayed.ok()) return replayed;
      meta_replayed_ = true;
      snapshot_.meta_.committed_topology_ = TopologyEpoch{2};
      snapshot_.meta_.installed_topology_ = TopologyEpoch{2};
      snapshot_.meta_.committed_term_ = GroupTerm{2};
      snapshot_.meta_.installed_term_ = GroupTerm{2};
    } else if (*kind == ExplorationAction::kRestoreMetaSnapshot) {
      absl::Status restored = meta_.RestoreSnapshot(meta_.Snapshot());
      if (!restored.ok()) return restored;
      meta_restored_ = true;
    } else if (*kind == ExplorationAction::kApplyDirective) {
      snapshot_.meta_.directive_applied_ = true;
      directive_applied_ = true;
    } else if (*kind == ExplorationAction::kReplayDirective) {
      // Reapplying the same operation/boot evidence is idempotent.
      snapshot_.meta_.directive_applied_ = true;
      directive_replayed_ = true;
    } else if (*kind == ExplorationAction::kRestartDirectiveTarget) {
      snapshot_.meta_.current_boot_ = BootId{2};
      snapshot_.meta_.directive_applied_ = false;
      directive_target_restarted_ = true;
    } else if (*kind == ExplorationAction::kRejectStaleDirective) {
      // Evidence remains scoped to boot 1; the restarted target must not mark
      // that stale directive applied under boot 2.
      snapshot_.meta_.directive_applied_ = false;
      stale_directive_rejected_ = true;
    } else if (*kind == ExplorationAction::kExpireOldGrant) {
      AuthorityObservation& old = snapshot_.authorities_.front();
      old.can_admit_write_ = false;
      old.can_complete_inflight_ = false;
      old.can_mutate_in_background_ = false;
      old.can_decide_success_ = false;
      old_authority_fenced_ = true;
    } else if (*kind == ExplorationAction::kActivateCandidate) {
      snapshot_.promotion_.candidate_activated_ = true;
      snapshot_.promotion_.write_gate_open_ = true;
      snapshot_.authorities_.back().can_admit_write_ = true;
      snapshot_.authorities_.back().can_decide_success_ = true;
      snapshot_.server_control_reply_.reset();
      candidate_activated_ = true;
    } else if (*kind == ExplorationAction::kResetOldReplica) {
      snapshot_.promotion_.another_replica_reset_ = true;
      replica_reset_ = true;
    } else if (*kind == ExplorationAction::kRecordClientSuccess) {
      snapshot_.client_history_.push_back(ClientOperationObservation{
          .operation_ = OperationId{1},
          .group_ = GroupId{1},
          .authority_ = NodeId{2},
          .authority_boot_ = BootId{1},
          .authority_term_ = GroupTerm{2},
          .authority_grant_ = GrantId{2},
          .mutates_ = true,
          .admitted_ = true,
          .admitted_with_valid_authority_ = true,
          .mutation_durable_ = true,
          .authority_valid_at_success_decision_ = true,
          .outcome_ = ClientOutcome::kSuccess,
      });
      client_result_recorded_ = true;
    }
    return absl::OkStatus();
  }

  std::string Observe() const override {
    return absl::StrCat(
        "clock=", clock_.now(), ";partitioned=", link_partitioned_,
        ";disconnected=", link_disconnected_, ";reconnected=", reconnected_,
        ";partition-seen=", partition_exercised_,
        ";pending=", pending_message_ids_.size(),
        ";deliverable=", network_.Deliverable(clock_.now()).size(),
        ";write-fault=", write_failure_exercised_,
        ";read-fault=", read_failure_exercised_,
        ";flush-fault=", flush_failure_exercised_,
        ";candidate-durable=", candidate_durable_,
        ";prefix-recovered=", prefix_recovery_exercised_,
        ";child-history=", child_history_ready_,
        ";meta-leader=", meta_.leader().value_,
        ";meta-index=", meta_.applied_index(),
        ";meta-restored=", meta_restored_,
        ";directive-replayed=", directive_replayed_,
        ";stale-directive-rejected=", stale_directive_rejected_,
        ";old-fenced=", old_authority_fenced_,
        ";candidate-active=", candidate_activated_,
        ";replica-reset=", replica_reset_,
        ";client-results=", snapshot_.client_history_.size(),
        ";fault-acks=", faults_.acknowledgments().size());
  }

  std::optional<Finding> CheckInvariants() const override {
    return CheckClusterInvariants(snapshot_);
  }

  bool HasPendingWork() const override { return !EnabledActions().empty(); }

  std::vector<std::string> DrainAcknowledgments() override {
    const std::vector<std::string>& all = faults_.acknowledgments();
    std::vector<std::string> result(all.begin() + fault_ack_cursor_, all.end());
    fault_ack_cursor_ = all.size();
    return result;
  }

 private:
  static constexpr std::uint64_t kGrantDeadline = 10;

  ManualClock clock_;
  SimNetwork network_;
  SimStorage candidate_storage_;
  SimStorage scratch_storage_;
  ControlPlaneReferenceMachine meta_;
  FaultController faults_;
  ClusterSnapshot snapshot_;
  std::set<std::uint64_t> pending_message_ids_;
  std::uint64_t heartbeat_id_ = 0;
  std::uint64_t metadata_id_ = 0;
  std::optional<std::uint64_t> duplicate_id_;
  std::size_t fault_ack_cursor_ = 0;
  bool clock_advanced_ = false;
  bool partition_exercised_ = false;
  bool link_partitioned_ = false;
  bool disconnect_exercised_ = false;
  bool link_disconnected_ = false;
  bool reconnected_ = false;
  bool heartbeat_duplicated_ = false;
  bool write_failure_exercised_ = false;
  bool read_failure_exercised_ = false;
  bool flush_failure_exercised_ = false;
  bool candidate_durable_ = false;
  bool prefix_persisted_ = false;
  bool prefix_recovery_exercised_ = false;
  bool child_history_ready_ = false;
  bool meta_leader_changed_ = false;
  bool meta_replayed_ = false;
  bool meta_restored_ = false;
  bool directive_applied_ = false;
  bool directive_replayed_ = false;
  bool directive_target_restarted_ = false;
  bool stale_directive_rejected_ = false;
  bool old_authority_fenced_ = false;
  bool candidate_activated_ = false;
  bool replica_reset_ = false;
  bool client_result_recorded_ = false;
};

enum class ScriptStep : std::uint8_t {
  kAdmitOldWrite,
  kExpireOldGrant,
  kActivateCandidate,
  kResumeOldWork,
  kCaptureResumeEvidence,
  kRestartTarget,
  kChooseResumeMode,
  kCaptureExactCursor,
  kEvictRetainedEvent,
  kBeginStaging,
  kPersistPartialStaging,
  kCrashAndRecover,
  kCaptureDirective,
  kRestartDirectiveTarget,
  kReplayStaleDirective,
  kAcknowledgeCatalog,
  kBeginFullSync,
  kCaptureCatalogToken,
  kAdvanceCatalogGeneration,
  kActivateWithCatalogToken,
};

struct ScriptStepDescriptor {
  ScriptStep step_;
  std::string_view name_;
};

constexpr std::array<ScriptStepDescriptor, 20> kScriptStepDescriptors{
    ScriptStepDescriptor{ScriptStep::kAdmitOldWrite, "admit-old-write"},
    ScriptStepDescriptor{ScriptStep::kExpireOldGrant, "expire-old-grant"},
    ScriptStepDescriptor{ScriptStep::kActivateCandidate, "activate-candidate"},
    ScriptStepDescriptor{ScriptStep::kResumeOldWork, "resume-old-work"},
    ScriptStepDescriptor{ScriptStep::kCaptureResumeEvidence,
                         "capture-resume-evidence"},
    ScriptStepDescriptor{ScriptStep::kRestartTarget, "restart-target"},
    ScriptStepDescriptor{ScriptStep::kChooseResumeMode, "choose-resume-mode"},
    ScriptStepDescriptor{ScriptStep::kCaptureExactCursor,
                         "capture-exact-cursor"},
    ScriptStepDescriptor{ScriptStep::kEvictRetainedEvent,
                         "evict-retained-event"},
    ScriptStepDescriptor{ScriptStep::kBeginStaging, "begin-staging"},
    ScriptStepDescriptor{ScriptStep::kPersistPartialStaging,
                         "persist-partial-staging"},
    ScriptStepDescriptor{ScriptStep::kCrashAndRecover, "crash-and-recover"},
    ScriptStepDescriptor{ScriptStep::kCaptureDirective, "capture-directive"},
    ScriptStepDescriptor{ScriptStep::kRestartDirectiveTarget,
                         "restart-directive-target"},
    ScriptStepDescriptor{ScriptStep::kReplayStaleDirective,
                         "replay-stale-directive"},
    ScriptStepDescriptor{ScriptStep::kAcknowledgeCatalog,
                         "acknowledge-catalog"},
    ScriptStepDescriptor{ScriptStep::kBeginFullSync, "begin-full-sync"},
    ScriptStepDescriptor{ScriptStep::kCaptureCatalogToken,
                         "capture-catalog-token"},
    ScriptStepDescriptor{ScriptStep::kAdvanceCatalogGeneration,
                         "advance-catalog-generation"},
    ScriptStepDescriptor{ScriptStep::kActivateWithCatalogToken,
                         "activate-with-catalog-token"},
};

std::string_view ScriptStepName(ScriptStep step) {
  const auto found =
      std::find_if(kScriptStepDescriptors.begin(), kScriptStepDescriptors.end(),
                   [&](const ScriptStepDescriptor& descriptor) {
                     return descriptor.step_ == step;
                   });
  return found == kScriptStepDescriptors.end() ? "unknown" : found->name_;
}

std::optional<ScriptStep> ParseScriptStep(std::string_view name) {
  const auto found =
      std::find_if(kScriptStepDescriptors.begin(), kScriptStepDescriptors.end(),
                   [&](const ScriptStepDescriptor& descriptor) {
                     return descriptor.name_ == name;
                   });
  return found == kScriptStepDescriptors.end()
             ? std::nullopt
             : std::optional<ScriptStep>{found->step_};
}

class ClusterScenarioWorld final : public ScenarioWorld {
 public:
  explicit ClusterScenarioWorld(Counterexample counterexample)
      : counterexample_(counterexample) {
    snapshot_.authorities_.push_back(AuthorityObservation{
        .group_ = GroupId{1},
        .node_ = NodeId{1},
        .boot_ = BootId{1},
        .term_ = GroupTerm{1},
        .grant_ = GrantId{1},
        .can_admit_write_ = true,
        .can_complete_inflight_ = true,
        .can_mutate_in_background_ = true,
        .can_decide_success_ = true,
    });
    snapshot_.authorities_.push_back(AuthorityObservation{
        .group_ = GroupId{1},
        .node_ = NodeId{2},
        .boot_ = BootId{1},
        .term_ = GroupTerm{2},
        .grant_ = GrantId{2},
    });
    snapshot_.resume_ = ResumeObservation{
        .source_boot_ = BootId{1},
        .target_boot_ = BootId{1},
        .evidence_source_boot_ = BootId{1},
        .evidence_target_boot_ = BootId{1},
    };
    snapshot_.meta_.current_operation_ = OperationId{1};
    snapshot_.meta_.directive_operation_ = OperationId{1};
    snapshot_.meta_.current_boot_ = BootId{1};
    snapshot_.meta_.evidence_boot_ = BootId{1};
    snapshot_.promotion_.captured_catalog_generation_ = CatalogGeneration{1};
    snapshot_.promotion_.current_catalog_generation_ = CatalogGeneration{1};
  }

  std::vector<Action> EnabledActions() const override {
    if (phase_ >= Actions().size()) return {};
    return {Action{.name_ = std::string(ScriptStepName(Actions()[phase_])),
                   .arguments_ = {},
                   .payload_ = {}}};
  }

  absl::Status Apply(const Action& action) override {
    const std::optional<ScriptStep> step = ParseScriptStep(action.name_);
    if (phase_ >= Actions().size() || !step.has_value() ||
        *step != Actions()[phase_]) {
      return absl::FailedPreconditionError("scenario action out of order");
    }
    ApplyPhase(*step);
    ++phase_;
    return absl::OkStatus();
  }

  std::string Observe() const override {
    const AuthorityObservation& old = snapshot_.authorities_[0];
    const AuthorityObservation& candidate = snapshot_.authorities_[1];
    return absl::StrCat(
        "phase=", phase_, ";old=", CanExerciseAuthority(old),
        ";new=", CanExerciseAuthority(candidate),
        ";source-boot=", snapshot_.resume_.source_boot_.value_,
        ";target-boot=", snapshot_.resume_.target_boot_.value_,
        ";partial=", snapshot_.resume_.partial_resume_selected_,
        ";coverage=", snapshot_.resume_.retained_events_contiguous_,
        ";population-complete=", snapshot_.population_.complete_);
  }

  std::optional<Finding> CheckInvariants() const override {
    return CheckClusterInvariants(snapshot_);
  }

  bool HasPendingWork() const override { return phase_ < Actions().size(); }

 private:
  std::span<const ScriptStep> Actions() const {
    static constexpr std::array authority{
        ScriptStep::kAdmitOldWrite, ScriptStep::kExpireOldGrant,
        ScriptStep::kActivateCandidate, ScriptStep::kResumeOldWork};
    static constexpr std::array stale{ScriptStep::kCaptureResumeEvidence,
                                      ScriptStep::kRestartTarget,
                                      ScriptStep::kChooseResumeMode};
    static constexpr std::array gap{ScriptStep::kCaptureExactCursor,
                                    ScriptStep::kEvictRetainedEvent,
                                    ScriptStep::kChooseResumeMode};
    static constexpr std::array activation{ScriptStep::kBeginStaging,
                                           ScriptStep::kPersistPartialStaging,
                                           ScriptStep::kCrashAndRecover};
    static constexpr std::array directive{ScriptStep::kCaptureDirective,
                                          ScriptStep::kRestartDirectiveTarget,
                                          ScriptStep::kReplayStaleDirective};
    static constexpr std::array catalog{ScriptStep::kAcknowledgeCatalog};
    static constexpr std::array full_sync{ScriptStep::kBeginFullSync};
    static constexpr std::array stale_catalog{
        ScriptStep::kCaptureCatalogToken, ScriptStep::kAdvanceCatalogGeneration,
        ScriptStep::kActivateWithCatalogToken};
    switch (counterexample_) {
      case Counterexample::kNone:
      case Counterexample::kDualAuthority:
        return authority;
      case Counterexample::kStaleEvidence:
        return stale;
      case Counterexample::kHistoryGap:
        return gap;
      case Counterexample::kPartialActivation:
        return activation;
      case Counterexample::kStaleDirective:
        return directive;
      case Counterexample::kCatalogAckBeforeDurable:
        return catalog;
      case Counterexample::kFullSyncRetainsOldState:
        return full_sync;
      case Counterexample::kStaleCatalogPromotion:
        return stale_catalog;
    }
    return authority;
  }

  void FenceOldAuthority() {
    AuthorityObservation& old = snapshot_.authorities_[0];
    old.can_admit_write_ = false;
    old.can_complete_inflight_ = false;
    old.can_mutate_in_background_ = false;
    old.can_decide_success_ = false;
  }

  void ApplyPhase(ScriptStep step) {
    if (step == ScriptStep::kExpireOldGrant) {
      FenceOldAuthority();
      if (counterexample_ == Counterexample::kDualAuthority) {
        snapshot_.authorities_[0].can_complete_inflight_ = true;
      }
    } else if (step == ScriptStep::kActivateCandidate) {
      snapshot_.promotion_.candidate_selected_ = true;
      snapshot_.promotion_.durability_barrier_complete_ = true;
      snapshot_.promotion_.child_history_ready_ = true;
      snapshot_.promotion_.candidate_activated_ = true;
      snapshot_.promotion_.write_gate_open_ = true;
      snapshot_.authorities_[1].can_admit_write_ = true;
      snapshot_.authorities_[1].can_decide_success_ = true;
    } else if (step == ScriptStep::kRestartTarget) {
      snapshot_.resume_.target_boot_ = BootId{2};
    } else if (step == ScriptStep::kEvictRetainedEvent) {
      snapshot_.resume_.retained_events_contiguous_ = false;
    } else if (step == ScriptStep::kChooseResumeMode) {
      snapshot_.resume_.partial_resume_selected_ =
          counterexample_ == Counterexample::kStaleEvidence ||
          counterexample_ == Counterexample::kHistoryGap;
    } else if (step == ScriptStep::kBeginStaging) {
      snapshot_.population_.active_ = false;
      snapshot_.population_.complete_ = false;
      snapshot_.population_.activation_durable_ = false;
    } else if (step == ScriptStep::kCrashAndRecover) {
      if (counterexample_ == Counterexample::kPartialActivation) {
        snapshot_.population_.active_ = true;
      } else {
        snapshot_.population_.active_ = true;
        snapshot_.population_.complete_ = true;
        snapshot_.population_.activation_durable_ = true;
      }
    } else if (step == ScriptStep::kRestartDirectiveTarget) {
      snapshot_.meta_.current_boot_ = BootId{2};
    } else if (step == ScriptStep::kReplayStaleDirective) {
      snapshot_.meta_.directive_applied_ =
          counterexample_ == Counterexample::kStaleDirective;
    } else if (step == ScriptStep::kAcknowledgeCatalog) {
      snapshot_.function_catalog_.applied_cursor_advanced_ = true;
      snapshot_.function_catalog_.replica_ack_sent_ = true;
    } else if (step == ScriptStep::kBeginFullSync) {
      snapshot_.full_sync_.in_progress_ = true;
      // The mutant begins transfer without first committing the destructive
      // invalidation of the old population, catalog readiness, and base.
    } else if (step == ScriptStep::kCaptureCatalogToken) {
      snapshot_.promotion_.candidate_selected_ = true;
      snapshot_.promotion_.durability_barrier_complete_ = true;
      snapshot_.promotion_.promotion_base_committed_ = true;
      snapshot_.promotion_.population_token_valid_ = true;
      snapshot_.promotion_.child_history_ready_ = true;
    } else if (step == ScriptStep::kAdvanceCatalogGeneration) {
      snapshot_.promotion_.current_catalog_generation_ = CatalogGeneration{2};
    } else if (step == ScriptStep::kActivateWithCatalogToken) {
      snapshot_.promotion_.candidate_activated_ = true;
      snapshot_.promotion_.write_gate_open_ = true;
    }
  }

  Counterexample counterexample_;
  std::size_t phase_ = 0;
  ClusterSnapshot snapshot_;
};

class ClusterScenario final : public Scenario {
 public:
  explicit ClusterScenario(Counterexample counterexample)
      : counterexample_(counterexample) {}

  std::string_view name() const override {
    return DescriptorFor(counterexample_).trace_name_;
  }

  std::uint32_t schema() const override { return 1; }

  std::unique_ptr<ScenarioWorld> NewWorld() const override {
    if (counterexample_ == Counterexample::kNone) {
      return std::make_unique<ExplorationWorld>();
    }
    return std::make_unique<ClusterScenarioWorld>(counterexample_);
  }

 private:
  Counterexample counterexample_;
};

}  // namespace

std::span<const ScenarioDescriptor> ClusterScenarioDescriptors() {
  return kScenarioDescriptors;
}

const ScenarioDescriptor* FindClusterScenario(std::string_view name) {
  const auto found = std::find_if(
      kScenarioDescriptors.begin(), kScenarioDescriptors.end(),
      [&](const ScenarioDescriptor& descriptor) {
        return descriptor.cli_name_ == name || descriptor.trace_name_ == name;
      });
  return found == kScenarioDescriptors.end() ? nullptr : &*found;
}

std::unique_ptr<Scenario> MakeClusterScenario(Counterexample counterexample) {
  return std::make_unique<ClusterScenario>(counterexample);
}

}  // namespace keylane::test::cluster
