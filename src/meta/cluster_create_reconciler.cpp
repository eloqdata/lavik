#include "keylane/meta/cluster_create_reconciler.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <string_view>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "celer/io/storage.h"
#include "celer/runtime/worker.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/fault_injection.h"
#include "keylane/meta/cluster_create.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/population_manifest_store.h"
#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {
constexpr std::string_view kPopulationKind =
    kMetaClusterCreatePopulationOperationKind;
constexpr std::string_view kPolicy = "keylane.cluster-create-v1";
constexpr std::array<std::string_view, 11> kPhases = {
    "register-data",        "create-group",   "assign-primary",
    "begin-term",           "slot-map",       "population-manifest",
    "population-anchor",    "policy",         "activate-authority",
    "wait-data-projection", "initialize-data"};

bool IsTerminal(MetaOperationLifecycle state) {
  return state == MetaOperationLifecycle::kCompleted ||
         state == MetaOperationLifecycle::kAborted;
}

const PutPopulationManifest& EmptyPopulationManifest() {
  static const PutPopulationManifest manifest = [] {
    PutPopulationManifest value;
    value.entries_.reserve(kMetaSlotCount);
    for (std::uint32_t partition = 0; partition < kMetaSlotCount; ++partition)
      value.entries_.push_back({partition, 1});
    value.manifest_digest_ =
        MetaPopulationManifestStore::CanonicalDigest(value.entries_);
    return value;
  }();
  return manifest;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& value) {
  return absl::BytesToHexString(std::string_view(
      reinterpret_cast<const char*>(value.data()), value.size()));
}

// The randomly generated root id supplies the entropy. Domain separation
// gives recovery the SAME child, assignment and directive identities without
// another durable format or a random in-memory value lost between commits.
MetaOperationId DerivedId(const MetaOperationId& root,
                          std::string_view purpose) {
  const auto hash =
      MetaSha256(absl::StrCat("cluster-create-v1/", Hex(root), "/", purpose));
  MetaOperationId id;
  std::copy_n(hash.begin(), id.size(), id.begin());
  return id;
}

using Plan = absl::StatusOr<std::optional<MetaCommand>>;
template <typename Command>
Plan Emit(Command command) {
  return std::optional<MetaCommand>(std::move(command));
}
Plan Conflict(std::string_view reason) {
  return absl::FailedPreconditionError(std::string(reason));
}
Plan Advance(const MetaOperationRecord& operation, std::string_view phase) {
  TransitionOperationPhase command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.kind_phase_blob_ = phase;
  return Emit(std::move(command));
}
Plan Complete(const MetaOperationRecord& operation) {
  CompleteOperation command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.result_ = "cluster-created";
  return Emit(std::move(command));
}
Plan Abort(const MetaOperationRecord& operation, std::string reason) {
  AbortOperation command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.reason_ = std::move(reason);
  return Emit(std::move(command));
}

bool ProjectionMatches(const MetaDataControlRuntimeNode& runtime,
                       const MetaTopologyGroupView& group,
                       const MetaGroupGrantState& grant,
                       std::uint64_t required_applied_index) {
  // Runtime is published only after Data acknowledges the installed FDS.
  // Unrelated commits advance validated high-water without resending an
  // unchanged projection. Requiring source_meta_applied_index to catch up
  // would therefore wait forever for an already-current projection.
  if (runtime.validated_committed_high_water_ < required_applied_index ||
      runtime.groups_.size() != 1 || !grant.grant_.has_value())
    return false;
  const auto member = std::find_if(
      group.members_.begin(), group.members_.end(),
      [&](const auto& item) { return item.node_id_ == runtime.node_id_; });
  if (member == group.members_.end()) return false;
  const auto& projected = runtime.groups_.front();
  return projected.group_id_ == group.group_id_ &&
         projected.assignment_id_ == member->assignment_id_ &&
         projected.group_term_ == group.record_.group_term_ &&
         projected.authority_version_ == group.record_.authority_version_ &&
         projected.grant_revision_ == grant.grant_->grant_revision_ &&
         projected.manifest_revision_ ==
             group.record_.population_manifest_revision_ &&
         projected.manifest_digest_ ==
             group.record_.population_manifest_digest_ &&
         projected.partition_replication_epoch_ ==
             group.record_.partition_replication_epoch_;
}
}  // namespace

Plan detail::PlanClusterCreateStep(
    const MetaCommittedView& view, const MetaOperationRecord& operation,
    const MetaDataControlRuntimeSnapshot& runtime) {
  if (operation.kind_ != kMetaClusterCreateOperationKind ||
      IsTerminal(operation.lifecycle_))
    return std::nullopt;
  std::uint32_t unused_timeout = 0;
  const auto manifest =
      DecodeClusterCreateRequest(operation.intent_, &unused_timeout);
  if (!manifest.ok())
    return Conflict("creation intent is not a recoverable v1 plan");
  const auto phase = operation.kind_phase_blob_.empty()
                         ? kPhases.begin()
                         : std::find(kPhases.begin(), kPhases.end(),
                                     operation.kind_phase_blob_);
  if (phase == kPhases.end()) return Conflict("unknown creation phase");
  const std::size_t step = phase - kPhases.begin();
  const auto& stores = view.stores();
  const auto& m = *manifest;
  const auto assignment = DerivedId(operation.operation_id_, "assignment");
  const auto child_id = DerivedId(operation.operation_id_, "population");
  const auto node = stores.identity_.FindNode(m.data_node_id_);
  const auto group = stores.topology_.FindGroup(m.group_id_);
  const auto grant = stores.grant_.GroupState(m.group_id_);
  const MetaGrantSpec expected_grant{5'000, std::string(kPolicy), 1};
  const auto policy = stores.policy_.FindVersion(std::string(kPolicy), 1);
  const auto& population = EmptyPopulationManifest();

  // Check the already-committed prefix before filling a missing next effect.
  // Recovery must never undo an operator's later membership, epoch, owner or
  // endpoint change merely because the old creation intent still exists.
  const auto meta_members = stores.identity_.MetaMembers();
  if (std::count_if(meta_members.begin(), meta_members.end(),
                    [](const auto& member) { return !member.retired_; }) != 1 ||
      std::none_of(meta_members.begin(), meta_members.end(),
                   [&](const auto& member) {
                     return !member.retired_ &&
                            member.server_id_ == m.meta_member_id_;
                   }) ||
      stores.identity_.NodeCount() > 1 || stores.topology_.GroupCount() > 1)
    return Conflict("creation membership no longer matches its intent");
  const bool node_matches =
      node.has_value() && !node->retired_ &&
      node->principal_ == absl::StrCat("keylane://node/", m.data_node_id_) &&
      node->role_ == MetaNodeRole::kPrimary &&
      node->endpoints_ == std::vector<std::string>{m.client_endpoint_};
  if ((node.has_value() && !node_matches) || (step > 0 && !node_matches) ||
      (stores.identity_.NodeCount() != 0 && !node.has_value()) ||
      (step > 1 && !group.has_value()) ||
      (stores.topology_.GroupCount() != 0 && !group.has_value()))
    return Conflict("creation identity or group was replaced");
  const bool assigned =
      group.has_value() && group->members_.size() == 1 &&
      group->members_.front() ==
          MetaGroupMember{m.data_node_id_, assignment, MetaNodeRole::kPrimary};
  if (group.has_value() &&
      ((!group->members_.empty() && !assigned) || (step > 2 && !assigned) ||
       group->record_.group_term_ > 1 ||
       (step > 3 && group->record_.group_term_ != 1) ||
       group->record_.authority_version_ > 1 || group->config_epoch_ > 1 ||
       (!group->record_.owner_.empty() &&
        group->record_.owner_ != m.data_node_id_)))
    return Conflict("creation group incarnation or authority changed");
  bool slots_complete = true;
  bool slots_empty = true;
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    const auto owner = stores.topology_.SlotOwner(slot);
    slots_complete &= owner == std::optional<std::string>(m.group_id_);
    slots_empty &= !owner.has_value();
  }
  if ((!slots_empty && !slots_complete) || (step > 4 && !slots_complete))
    return Conflict("creation slot map changed");
  const bool population_matches =
      group.has_value() && group->record_.population_manifest_revision_ == 1 &&
      group->record_.population_manifest_digest_ ==
          population.manifest_digest_ &&
      group->record_.partition_replication_epoch_ == 1;
  if (group.has_value() &&
      ((group->record_.population_manifest_revision_ != 0 &&
        !population_matches) ||
       (step > 6 && !population_matches)))
    return Conflict("creation population epoch changed");
  const bool policy_matches =
      policy.has_value() && !policy->retired_ &&
      policy->content_ == "first-empty-population" &&
      policy->content_hash_ == MetaSha256("first-empty-population");
  if ((policy.has_value() && !policy_matches) || (step > 7 && !policy_matches))
    return Conflict("creation policy changed");
  const bool authority_matches =
      grant.has_value() && grant->grant_.has_value() && !grant->fenced_ &&
      grant->grant_->owner_ == m.data_node_id_ && grant->grant_->term_ == 1 &&
      grant->grant_->authority_version_ == 1 &&
      grant->grant_->spec_ == expected_grant && group->config_epoch_ == 1;
  if ((grant.has_value() && grant->grant_.has_value() && !authority_matches) ||
      (step == 9 && !authority_matches))
    return Conflict("creation authority changed");

  switch (step) {
    case 0:
      if (!node_matches) {
        RegisterNode c;
        c.node_id_ = m.data_node_id_;
        c.principal_ = absl::StrCat("keylane://node/", m.data_node_id_);
        c.role_ = MetaNodeRole::kPrimary;
        c.endpoints_ = {m.client_endpoint_};
        return Emit(std::move(c));
      }
      break;
    case 1:
      if (!group.has_value()) {
        CreateGroup c;
        c.group_id_ = m.group_id_;
        c.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        return Emit(std::move(c));
      }
      break;
    case 2:
      if (!assigned) {
        AssignNodeToGroup c;
        c.group_id_ = m.group_id_;
        c.node_id_ = m.data_node_id_;
        c.assignment_id_ = assignment;
        c.role_ = MetaNodeRole::kPrimary;
        c.expected_revision_ = group->revision_;
        c.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        return Emit(std::move(c));
      }
      break;
    case 3:
      if (group->record_.group_term_ == 0) {
        BeginGroupTerm c;
        c.group_id_ = m.group_id_;
        c.new_term_ = 1;
        return Emit(std::move(c));
      }
      break;
    case 4:
      if (!slots_complete || group->config_epoch_ != 1) {
        SetSlotMap c;
        c.ranges_ = {{m.first_slot_, m.last_slot_, m.group_id_}};
        c.config_epochs_ = {{m.group_id_, 1}};
        c.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        return Emit(std::move(c));
      }
      break;
    case 5:
      if (!stores.population_manifest_.Contains(population.manifest_digest_))
        return Emit(population);
      break;
    case 6:
      if (!population_matches) {
        SetGroupReplicationState c;
        c.group_id_ = m.group_id_;
        c.new_population_manifest_revision_ = 1;
        c.new_population_manifest_digest_ = population.manifest_digest_;
        c.new_partition_replication_epoch_ = 1;
        c.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        return Emit(std::move(c));
      }
      break;
    case 7:
      if (!policy_matches) {
        PutPolicy c;
        c.policy_id_ = kPolicy;
        c.version_ = 1;
        c.content_ = "first-empty-population";
        c.content_hash_ = MetaSha256(c.content_);
        return Emit(std::move(c));
      }
      break;
    case 8:
      if (!authority_matches) {
        // A fenced authority version 1 is a later revocation, not the
        // never-activated version 0. Do not undo it during recovery.
        if (group->record_.authority_version_ != 0)
          return Conflict("creation authority was revoked");
        ActivateAuthority c;
        c.group_id_ = m.group_id_;
        c.expected_term_ = 1;
        c.new_owner_ = m.data_node_id_;
        c.grant_ = expected_grant;
        c.new_authority_version_ = 1;
        c.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        c.new_config_epoch_ = 1;
        return Emit(std::move(c));
      }
      break;
    default: {
      const auto child = stores.operation_.FindOperation(child_id);
      if (!child.has_value()) {
        if (step != 9 || stores.operation_.OperationKnown(child_id))
          return Conflict(
              "initialization operation disappeared or was archived");
        const auto target = std::find_if(
            runtime.nodes_.begin(), runtime.nodes_.end(),
            [&](const auto& item) { return item.node_id_ == m.data_node_id_; });
        if (!runtime.leader_authority_eligible_ ||
            target == runtime.nodes_.end() ||
            !ProjectionMatches(*target, *group, *grant, view.applied_index()))
          return std::nullopt;
        if (!cluster::control::IsCanonicalIdentity160(target->boot_id_))
          return Conflict("Data boot identity is malformed");
        SubmitOperation c;
        c.operation_id_ = child_id;
        c.kind_ = kPopulationKind;
        c.intent_ =
            absl::StrCat("empty-population-v1 ", Hex(operation.operation_id_),
                         " ", target->boot_id_);
        c.intent_hash_ = MetaSha256(c.intent_);
        c.replication_history_id_ = target->replication_history_id_;
        c.policy_references_ = {{std::string(kPolicy), 1}};
        return Emit(std::move(c));
      }
      const std::string prefix = absl::StrCat(
          "empty-population-v1 ", Hex(operation.operation_id_), " ");
      if (child->kind_ != kPopulationKind ||
          !child->intent_.starts_with(prefix) ||
          !cluster::control::IsCanonicalIdentity160(
              child->intent_.substr(prefix.size())))
        return Conflict(
            "initialization operation is not owned by this creation");
      if (step == 9) break;
      if (child->lifecycle_ == MetaOperationLifecycle::kCompleted)
        return Complete(operation);
      if (child->lifecycle_ == MetaOperationLifecycle::kAborted)
        return Abort(operation, child->terminal_result_);
      if (!child->terminal_receipts_.empty()) {
        const auto& receipt = child->terminal_receipts_.back();
        if (receipt.key_.directive_id_ != DerivedId(child_id, "directive") ||
            receipt.key_.attempt_id_ != DerivedId(child_id, "attempt"))
          return Conflict("initialization receipt belongs to another attempt");
        if (receipt.status_ != MetaDirectiveResultStatus::kSucceeded) {
          if (authority_matches) {
            FenceGroup c;
            c.group_id_ = m.group_id_;
            c.expected_term_ = 1;
            return Emit(std::move(c));
          }
          return Abort(*child, absl::StrCat("empty population initialization: ",
                                            receipt.result_));
        }
        if (!child->current_directives_.empty())
          return Advance(*child, "population-ready");
        return Complete(*child);
      }
      if (!authority_matches)
        return Conflict("initialization authority was revoked");
      if (!child->current_directives_.empty()) {
        const auto target = std::find_if(
            runtime.nodes_.begin(), runtime.nodes_.end(),
            [&](const auto& item) { return item.node_id_ == m.data_node_id_; });
        if (target != runtime.nodes_.end() &&
            (target->boot_id_ != child->intent_.substr(prefix.size()) ||
             target->replication_history_id_ != child->replication_history_id_))
          return Conflict(
              "Data restarted during initialization; recovery requires a new "
              "authorized attempt");
        // The publisher owns resend/receipt tracking. A missing receipt is
        // NOT permission to mint another destructive directive identity.
        return std::nullopt;
      }
      if (child->lifecycle_ != MetaOperationLifecycle::kSubmitted)
        return Conflict("initialization directive was invalidated");
      MetaDirectiveSpec initialize;
      initialize.directive_id_ = DerivedId(child_id, "directive");
      initialize.attempt_id_ = DerivedId(child_id, "attempt");
      initialize.recipient_node_id_ = m.data_node_id_;
      initialize.target_node_id_ = m.data_node_id_;
      std::string boot;
      if (!absl::HexStringToBytes(child->intent_.substr(prefix.size()), &boot))
        return Conflict("invalid initialization boot identity");
      std::copy(boot.begin(), boot.end(), initialize.target_boot_id_.begin());
      initialize.assignment_id_ = assignment;
      initialize.source_node_id_ = std::string(kMetaNodeIdBytes, '0');
      initialize.group_id_ = m.group_id_;
      initialize.group_term_ = 1;
      initialize.authority_version_ = 1;
      initialize.grant_revision_ = grant->grant_->grant_revision_;
      initialize.population_manifest_revision_ = 1;
      initialize.population_manifest_digest_ = population.manifest_digest_;
      initialize.partition_replication_epoch_ = 1;
      initialize.kind_ = kMetaDirectiveInitializeEmptyPopulation;
      initialize.payload_ = Hex(child->replication_history_id_);
      initialize.storage_mutating_ = true;
      TransitionOperationPhase c;
      c.operation_id_ = child_id;
      c.expected_revision_ = child->revision_;
      c.kind_phase_blob_ = "initializing-empty-population";
      c.current_directives_.push_back(std::move(initialize));
      return Emit(std::move(c));
    }
  }
  return Advance(operation, kPhases[step + 1]);
}

struct MetaClusterCreateReconciler::Core {
  celer::ForeignExecutor executor_;
  std::shared_ptr<MetaMembershipGate> membership_gate_;
  std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status_;
  // Worker-owned except the atomic ingress/stop-completion flags below.
  bool running_ = false;
  bool cancelled_ = true;
  bool shutdown_ = false;
  std::vector<std::shared_ptr<std::promise<void>>> waiters_;
  std::atomic<bool> shutdown_complete_{false};
  std::atomic<bool> stopping_{false};
};

MetaClusterCreateReconciler::MetaClusterCreateReconciler(
    celer::ForeignExecutor executor, std::shared_ptr<MetaMembershipGate> gate,
    std::shared_ptr<MetaDataControlRuntimeStatus> runtime)
    : core_(std::make_shared<Core>()) {
  core_->executor_ = std::move(executor);
  core_->membership_gate_ = std::move(gate);
  core_->runtime_status_ = std::move(runtime);
}
MetaClusterCreateReconciler::~MetaClusterCreateReconciler() { Shutdown(); }

void MetaClusterCreateReconciler::Start(MetaLeaderContext& context) {
  const auto core = core_;
  if (!core->executor_.Notify([core, context = &context]() noexcept {
        if (core->shutdown_) return;
        if (core->running_) std::terminate();
        core->cancelled_ = false;
        core->running_ = true;
        celer::ThisWorker().self_->Spawn(Run(core, context));
      }))
    std::terminate();
}

void MetaClusterCreateReconciler::Stop(bool permanent) {
  const auto core = core_;
  if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
  if (permanent) core->stopping_.store(true, std::memory_order_release);
  auto complete = std::make_shared<std::promise<void>>();
  auto done = complete->get_future();
  if (!core->executor_.Notify([core, complete, permanent]() noexcept {
        core->shutdown_ |= permanent;
        core->cancelled_ = true;
        if (core->running_)
          core->waiters_.push_back(complete);
        else
          complete->set_value();
      })) {
    if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  if (permanent)
    core->shutdown_complete_.store(true, std::memory_order_release);
}
void MetaClusterCreateReconciler::CancelAndWait() { Stop(false); }
void MetaClusterCreateReconciler::Shutdown() { Stop(true); }
bool MetaClusterCreateReconciler::accepting() const {
  return !core_->stopping_.load(std::memory_order_acquire);
}

celer::Task<absl::Status> MetaClusterCreateReconciler::Run(
    std::shared_ptr<Core> core, MetaLeaderContext* context) {
  std::unique_ptr<MetaMembershipGate::Lease> lease;
  std::string last_cut;
  auto changed = std::make_shared<std::atomic<bool>>(false);
  auto subscribe = [&] {
    return context->SubscribeCommitted([changed](const MetaCommitEvent&) {
      changed->store(true, std::memory_order_release);
    });
  };
  auto subscribed = subscribe();
  while (!core->cancelled_) {
    // CommittedView includes snapshot restoration and WAL replay. There is
    // deliberately no saved process-local task list to reconstruct on boot.
    // Idle polling only reads a notification bit, not the entire metadata
    // aggregate. Overflow resubscribes from an atomic view instead of losing
    // a committed task. Active Data waits still observe volatile runtime.
    if (subscribed.subscription_->needs_resync()) subscribed = subscribe();
    if (changed->exchange(false, std::memory_order_acq_rel))
      subscribed.view_ = context->CommittedView();
    const auto& view = subscribed.view_;
    const auto operations =
        view.operation().HasActiveKind(kMetaClusterCreateOperationKind)
            ? view.operation().LiveOperations()
            : std::vector<MetaOperationRecord>{};
    const auto operation = std::find_if(
        operations.begin(), operations.end(), [](const auto& item) {
          return item.kind_ == kMetaClusterCreateOperationKind &&
                 !IsTerminal(item.lifecycle_);
        });
    if (operation == operations.end())
      lease.reset();
    else {
      if (lease == nullptr) lease = core->membership_gate_->TryAcquire();
      if (lease != nullptr &&
          !operation->kind_phase_blob_.starts_with("recovery-required:")) {
        std::string cut = operation->kind_phase_blob_.empty()
                              ? "submitted"
                              : operation->kind_phase_blob_;
        if (cut == "initialize-data") {
          const auto child = view.operation().FindOperation(
              DerivedId(operation->operation_id_, "population"));
          if (child.has_value()) {
            if (child->lifecycle_ == MetaOperationLifecycle::kCompleted)
              cut = "child-completed";
            else if (child->kind_phase_blob_ == "population-ready")
              cut = "directive-removed";
            else if (!child->terminal_receipts_.empty())
              cut = "result-committed";
            else if (!child->current_directives_.empty())
              cut = "initialization-issued";
          }
        }
        if (cut != last_cut) {
          spdlog::info("cluster-create {} phase={}",
                       Hex(operation->operation_id_), cut);
          last_cut = cut;
        }
        // Debug pauses occur on the owner coroutine and still obey shutdown;
        // tests can stop at durable cuts without killing unrelated processes.
        bool paused = false;
        KEYLANE_FAULT_INJECT(
            paused = KEYLANE_FAULT_MATCHES(
                "KEYLANE_TEST_PAUSE_CLUSTER_CREATE_PHASE", cut););
        if (!paused) {
          auto planned = detail::PlanClusterCreateStep(
              view, *operation, core->runtime_status_->Snapshot());
          if (!planned.ok()) {
            spdlog::warn("cluster-create {} requires recovery: {}",
                         Hex(operation->operation_id_),
                         planned.status().message());
            planned = Advance(
                *operation,
                absl::StrCat("recovery-required:", planned.status().message()));
          }
          if (planned.ok() && planned->has_value() && !core->cancelled_) {
            MetaCommand command = std::move(**planned);
            auto request = cluster::control::GenerateId128();
            if (!request.ok()) std::terminate();
            std::visit([&](auto& c) { c.request_id_ = *request; }, command);
            // Cancellation never rolls back or submits a compensating fence.
            // An accepted proposal may still commit; the next owner re-reads
            // its effect before deciding whether anything remains to do.
            const auto applied = co_await context->Propose(std::move(command));
            if (core->cancelled_) break;
            // Own completion may precede subscription delivery. Force a new
            // committed view before planning, including domain rejections.
            changed->store(true, std::memory_order_release);
            if (applied.ok() &&
                applied->verdict_ == MetaAuditVerdict::kAccepted)
              continue;
            spdlog::warn("cluster-create {} proposal deferred: {}",
                         Hex(operation->operation_id_),
                         applied.ok()
                             ? applied->detail_
                             : std::string(applied.status().message()));
          }
        }
      }
    }
    const auto slept = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                                std::chrono::milliseconds(25));
    if (!slept.ok()) break;
  }
  lease.reset();
  core->running_ = false;
  for (const auto& waiter : core->waiters_) waiter->set_value();
  core->waiters_.clear();
  co_return absl::OkStatus();
}
}  // namespace keylane::meta
