// Component-integration gate for the real controlled-failover owner loop.
//
// This component gate drives FDS acknowledgements, directive receipts,
// observations, and finite-lease seams directly so every workflow cut can be
// made deterministic. The separate real-process controlled-failover gate
// covers production Meta/Data transport and Redis routing. Meta itself remains
// real here: a single-node NuRaft server, MetaCoordinator, state machine, WAL,
// and MetaControlledFailoverReconciler::Run execute the complete workflow.

#include <unistd.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "celer/runtime/runtime.h"
#include "celer/runtime/worker.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/control_projector.h"
#include "keylane/meta/controlled_failover_reconciler.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/failover.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/nuraft_log_store.h"
#include "keylane/meta/nuraft_state_mgr.h"
#include "keylane/meta/observation_store.h"
#include "keylane/meta/population_manifest_store.h"
#include "keylane/meta/state_machine.h"
#include "libnuraft/nuraft.hxx"

namespace keylane::meta {

// The passkey keeps test-authored commands on the same trusted Propose entry
// as Admin and reconciler traffic.
class MetaCoordinatorTestPeer {
 public:
  static AuthenticatedPrincipal Make(std::string principal) {
    return AuthenticatedPrincipal(std::move(principal), MetaPrincipalPasskey{});
  }
};

namespace {

namespace control = keylane::cluster::control;
using namespace std::chrono_literals;

constexpr std::string_view kActor = "keylane://operator/failover-lifecycle";
constexpr std::string_view kGroup = "group-a";
constexpr std::string_view kFormerNode =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kCandidateNode =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::uint32_t kGraceMs = 400;

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& bytes) {
  constexpr std::string_view kDigits = "0123456789abcdef";
  std::string result(bytes.size() * 2, '\0');
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    result[index * 2] = kDigits[bytes[index] >> 4];
    result[index * 2 + 1] = kDigits[bytes[index] & 0x0f];
  }
  return result;
}

std::int64_t NowUnixMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

bool WaitFor(const std::function<bool()>& predicate,
             std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(5ms);
  }
  return predicate();
}

absl::StatusOr<MetaApplyResult> RunProposal(
    celer::Task<absl::StatusOr<MetaApplyResult>> task) {
  std::promise<void> done;
  std::future<void> signal = done.get_future();
  task.SetCompletionCallback(
      &done, [](void* context, std::coroutine_handle<>) noexcept {
        static_cast<std::promise<void>*>(context)->set_value();
      });
  auto handle = std::move(task).ReleaseHandle();
  if (!handle) return absl::InternalError("proposal task has no frame");
  handle.resume();
  if (signal.wait_for(15s) != std::future_status::ready) {
    handle.destroy();
    return absl::DeadlineExceededError("proposal did not complete");
  }
  auto result = std::move(handle.promise().value_);
  handle.destroy();
  return result;
}

class ThreadScheduler final : public nuraft::delayed_task_scheduler {
 public:
  ~ThreadScheduler() override { Shutdown(); }

  void schedule(nuraft::ptr<nuraft::delayed_task>& task,
                nuraft::int32 milliseconds) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (stopped_) return;
    threads_.emplace_back([this, task, milliseconds] {
      {
        std::unique_lock<std::mutex> guard(mu_);
        stopped_cv_.wait_for(guard, std::chrono::milliseconds(milliseconds),
                             [this] { return stopped_; });
        if (stopped_) return;
      }
      task->execute();
    });
  }

  void Shutdown() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stopped_) return;
      stopped_ = true;
    }
    stopped_cv_.notify_all();
    for (std::thread& thread : threads_) {
      if (thread.joinable()) thread.join();
    }
    threads_.clear();
  }

 private:
  void cancel_impl(nuraft::ptr<nuraft::delayed_task>&) override {}

  std::mutex mu_;
  std::condition_variable stopped_cv_;
  bool stopped_ = false;
  std::vector<std::thread> threads_;
};

class NullRpcClientFactory final : public nuraft::rpc_client_factory {
 public:
  nuraft::ptr<nuraft::rpc_client> create_client(const std::string&) override {
    return nullptr;
  }
};

// Provides a deterministic barrier around the coordinator's serialized role
// callbacks without exposing test-only lifecycle state from the reconciler.
class LeadershipProbe final : public MetaReconciler {
 public:
  void Start(MetaLeaderContext&) override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      ++starts_;
    }
    cv_.notify_all();
  }

  void CancelAndWait() override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      ++cancels_;
    }
    cv_.notify_all();
  }

  bool WaitForStarts(unsigned count, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return starts_ >= count; });
  }

  bool WaitForCancels(unsigned count, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return cancels_ >= count; });
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  unsigned starts_ = 0;
  unsigned cancels_ = 0;
};

class MetaControlledFailoverLifecycleGateTest : public testing::Test {
 protected:
  void SetUp() override {
    directory_ = std::filesystem::temp_directory_path() /
                 ("keylane_failover_lifecycle_" + std::to_string(::getpid()));
    std::filesystem::remove_all(directory_);

    ASSERT_TRUE(StartCeler().ok());
    ASSERT_TRUE(StartRaft().ok());
    ASSERT_TRUE(SeedTopology().ok());

    lifecycle_probe_ = std::make_shared<LeadershipProbe>();
    coordinator_->RunAsLeader(lifecycle_probe_);
    coordinator_->BecomeLeader();
    ASSERT_TRUE(lifecycle_probe_->WaitForStarts(1, 3s));

    BeginDataGeneration(/*generation=*/1);
    ASSERT_TRUE(AdoptCandidateSession().ok());
    ASSERT_TRUE(PublishCurrentProjections().ok());
    ASSERT_TRUE(ObserveCandidate({20, 33}).ok());
    ASSERT_TRUE(SubmitFailover().ok());
  }

  void TearDown() override {
    coordinator_.reset();
    if (reconciler_ != nullptr) reconciler_->Shutdown();
    reconciler_.reset();
    lifecycle_probe_.reset();

    if (server_ != nullptr && server_running_) {
      server_->shutdown();
      server_running_ = false;
    }
    if (machine_ != nullptr) machine_->WaitForSnapshotWriterIdle();
    server_.reset();
    if (scheduler_ != nullptr) {
      scheduler_->Shutdown();
      scheduler_.reset();
    }
    machine_.reset();
    state_mgr_.reset();
    wal_ = nullptr;

    if (runtime_started_) {
      executor_.WaitUntilIdle();
      runtime_.RequestStop();
      runtime_.WaitUntilStopped();
      runtime_started_ = false;
    }
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
  }

  absl::Status StartCeler() {
    auto initialized = std::make_shared<std::promise<absl::Status>>();
    std::future<absl::Status> ready = initialized->get_future();
    runtime_.Start(
        1,
        [initialized](unsigned, celer::Worker& worker) {
          const absl::Status status = worker.Init();
          initialized->set_value(status);
          if (!status.ok()) return 1;
          worker.Run();
          worker.Shutdown();
          worker.DestroyDetachedTasks();
          return 0;
        },
        false);
    runtime_started_ = true;
    absl::Status initialized_status = ready.get();
    if (!initialized_status.ok()) return initialized_status;
    executor_ = runtime_.GetForeignExecutor(0);
    return absl::OkStatus();
  }

  absl::Status StartRaft() {
    auto state_mgr =
        NuraftStateMgr::Open(directory_.string(), 1, "127.0.0.1:9601");
    if (!state_mgr.ok()) return state_mgr.status();
    state_mgr_ = nuraft::ptr<NuraftStateMgr>(std::move(*state_mgr));
    auto machine = MetaStateMachine::Open(directory_.string());
    if (!machine.ok()) return machine.status();
    machine_ = nuraft::ptr<MetaStateMachine>(std::move(*machine));

    scheduler_ = nuraft::cs_new<ThreadScheduler>();
    nuraft::raft_params params;
    params.with_election_timeout_lower(50);
    params.with_election_timeout_upper(100);
    params.with_hb_interval(25);
    params.with_snapshot_enabled(0);
    params.with_reserved_log_items(0);
    params.with_client_req_timeout(5000);
    params.return_method_ = nuraft::raft_params::async_handler;
    params.wait_for_sm_catchup_on_becoming_leader_ = true;
    nuraft::context* context = new nuraft::context(
        state_mgr_, machine_, /*listener=*/nullptr, /*logger=*/nullptr,
        nuraft::cs_new<NullRpcClientFactory>(), scheduler_, params);
    server_ = nuraft::cs_new<nuraft::raft_server>(
        context, nuraft::raft_server::init_options{});
    server_running_ = true;
    if (!WaitFor([this] { return server_->is_leader(); }, 10s)) {
      return absl::DeadlineExceededError("single Meta did not become leader");
    }

    nuraft::ptr<nuraft::log_store> log_store = state_mgr_->load_log_store();
    wal_ = static_cast<NuraftLogStore*>(log_store.get());
    MetaCoordinatorOptions options;
    options.foreign_executor_ = executor_;
    coordinator_ = std::make_unique<MetaCoordinator>(server_, *machine_, *wal_,
                                                     *observations_, options);
    coordinator_->AddValidateHook(ValidateFailoverProposal);
    return absl::OkStatus();
  }

  absl::Status Commit(MetaCommand command) {
    ++request_sequence_;
    std::visit(
        [this](auto& value) {
          value.request_id_.fill(
              static_cast<std::uint8_t>((request_sequence_ % 250) + 1));
        },
        command);
    auto applied = RunProposal(coordinator_->Propose(
        std::move(command),
        MetaCoordinatorTestPeer::Make(std::string(kActor))));
    if (!applied.ok()) return applied.status();
    if (applied->verdict_ != MetaAuditVerdict::kAccepted) {
      return absl::FailedPreconditionError(
          absl::StrCat("proposal rejected: ", applied->detail_));
    }
    return absl::OkStatus();
  }

  absl::Status SeedTopology() {
    RegisterNode former;
    former.node_id_ = std::string(kFormerNode);
    former.principal_ = "keylane://node/" + former.node_id_;
    former.endpoints_ = {"tcp://127.0.0.1:7001"};
    former.role_ = MetaNodeRole::kPrimary;
    if (absl::Status status = Commit(former); !status.ok()) return status;

    RegisterNode candidate;
    candidate.node_id_ = std::string(kCandidateNode);
    candidate.principal_ = "keylane://node/" + candidate.node_id_;
    candidate.endpoints_ = {"tcp://127.0.0.1:7002"};
    candidate.role_ = MetaNodeRole::kReplica;
    if (absl::Status status = Commit(candidate); !status.ok()) return status;

    CreateGroup create;
    create.group_id_ = std::string(kGroup);
    create.new_topology_epoch_ = 1;
    if (absl::Status status = Commit(create); !status.ok()) return status;

    AssignNodeToGroup assign_former;
    assign_former.group_id_ = std::string(kGroup);
    assign_former.node_id_ = std::string(kFormerNode);
    assign_former.assignment_id_ = former_assignment_;
    assign_former.role_ = MetaNodeRole::kPrimary;
    assign_former.expected_revision_ = 1;
    assign_former.new_topology_epoch_ = 2;
    if (absl::Status status = Commit(assign_former); !status.ok()) {
      return status;
    }

    AssignNodeToGroup assign_candidate;
    assign_candidate.group_id_ = std::string(kGroup);
    assign_candidate.node_id_ = std::string(kCandidateNode);
    assign_candidate.assignment_id_ = candidate_assignment_;
    assign_candidate.role_ = MetaNodeRole::kReplica;
    assign_candidate.expected_revision_ = 2;
    assign_candidate.new_topology_epoch_ = 3;
    if (absl::Status status = Commit(assign_candidate); !status.ok()) {
      return status;
    }

    PutPopulationManifest manifest;
    manifest.entries_ = {{1, 1}, {2, 1}};
    manifest.manifest_digest_ =
        MetaPopulationManifestStore::CanonicalDigest(manifest.entries_);
    manifest_digest_ = manifest.manifest_digest_;
    if (absl::Status status = Commit(manifest); !status.ok()) return status;

    SetGroupReplicationState population;
    population.group_id_ = std::string(kGroup);
    population.new_population_manifest_revision_ = 1;
    population.new_population_manifest_digest_ = manifest_digest_;
    population.new_partition_replication_epoch_ = 1;
    population.new_topology_epoch_ = 4;
    if (absl::Status status = Commit(population); !status.ok()) return status;

    PutPolicy policy;
    policy.policy_id_ = "lease-policy";
    policy.version_ = 1;
    policy.content_ = "finite-lease";
    policy.content_hash_ = MetaSha256(policy.content_);
    if (absl::Status status = Commit(policy); !status.ok()) return status;

    BeginGroupTerm term;
    term.group_id_ = std::string(kGroup);
    term.expected_term_ = 0;
    term.new_term_ = 1;
    if (absl::Status status = Commit(term); !status.ok()) return status;

    ActivateAuthority activate;
    activate.group_id_ = std::string(kGroup);
    activate.expected_term_ = 1;
    activate.new_owner_ = std::string(kFormerNode);
    activate.grant_ = {.lease_duration_ms_ = 5000,
                       .policy_id_ = "lease-policy",
                       .policy_version_ = 1};
    activate.new_authority_version_ = 1;
    activate.new_topology_epoch_ = 5;
    activate.new_config_epoch_ = 1;
    return Commit(activate);
  }

  void BeginDataGeneration(std::uint64_t generation) {
    data_generation_ = generation;
    source_session_ = Bytes<16>(static_cast<std::uint8_t>(0x50 + generation));
    candidate_session_ =
        Bytes<16>(static_cast<std::uint8_t>(0x60 + generation));
    runtime_status_->BeginLeadership(generation);
    runtime_status_->SetLeaderAuthorityEligible(generation, true);
  }

  absl::Status AdoptCandidateSession() {
    return observations_->AdoptSession(
        MetaObservationIdentity{std::string(kCandidateNode), candidate_boot_,
                                data_generation_},
        NowUnixMillis());
  }

  absl::Status PublishCurrentProjections() {
    const MetaCommittedView view = coordinator_->CommittedView();
    auto source = MetaControlProjector::ProjectNode(view, kFormerNode);
    if (!source.ok()) return source.status();
    auto candidate = MetaControlProjector::ProjectNode(view, kCandidateNode);
    if (!candidate.ok()) return candidate.status();
    runtime_status_->PublishCurrent(std::string(kFormerNode), Hex(source_boot_),
                                    source_session_, parent_history_,
                                    data_generation_, data_generation_,
                                    view.applied_index(), source->full_state);
    runtime_status_->PublishCurrent(
        std::string(kCandidateNode), Hex(candidate_boot_), candidate_session_,
        candidate_history_, data_generation_, data_generation_,
        view.applied_index(), candidate->full_state);
    const control::HeartbeatHealth healthy{
        .storage_ready = true,
        .population_ready = true,
        .draining = false,
        .active_groups = 1,
        .summary = "ready",
    };
    const std::int64_t received = NowUnixMillis();
    runtime_status_->RecordHealth(kFormerNode, source_session_, healthy,
                                  received);
    runtime_status_->RecordHealth(kCandidateNode, candidate_session_, healthy,
                                  received);
    return absl::OkStatus();
  }

  absl::Status ObserveCandidate(std::vector<std::uint64_t> frontier) {
    const MetaCommittedView view = coordinator_->CommittedView();
    const auto group = view.topology().FindGroup(std::string(kGroup));
    if (!group.has_value()) return absl::NotFoundError("group is missing");
    MetaCandidateProgressObs progress{
        .node_id_ = std::string(kCandidateNode),
        .boot_incarnation_ = candidate_boot_,
        .session_generation_ = data_generation_,
        .group_id_ = std::string(kGroup),
        .assignment_id_ = candidate_assignment_,
        .group_term_ = group->record_.group_term_,
        .population_manifest_revision_ = 1,
        .population_manifest_digest_ = manifest_digest_,
        .partition_replication_epoch_ = 1,
        .replication_history_id_ = candidate_history_,
        .source_node_id_ = std::string(kFormerNode),
        .source_assignment_id_ = former_assignment_,
        .source_boot_incarnation_ = source_boot_,
        .source_replication_history_id_ = parent_history_,
        .applied_next_lsns_ = std::move(frontier),
        .applied_flow_vector_ = "2:ready",
        .backlog_coverage_ = "complete",
        .readiness_ = "ready",
        .storage_ready_ = true,
        .population_ready_ = true,
    };
    const MetaObservationIdentity identity{std::string(kCandidateNode),
                                           candidate_boot_, data_generation_};
    return observations_->Ingest(
        MetaObservation{.identity_ = identity, .payload_ = std::move(progress)},
        MetaStoresFacts(view.stores()), NowUnixMillis());
  }

  absl::Status SubmitFailover() {
    const MetaCommittedView view = coordinator_->CommittedView();
    auto submit = BuildControlledFailoverSubmission(
        kGroup, operation_id_, Bytes<16>(0x42), view, *observations_,
        NowUnixMillis(), 120'000);
    if (!submit.ok()) return submit.status();
    auto decoded = DecodeFailoverIntent(submit->intent_);
    if (!decoded.ok()) return decoded.status();
    intent_ = *decoded;
    return Commit(std::move(*submit));
  }

  std::optional<MetaOperationRecord> Operation() const {
    return coordinator_->CommittedView().operation().FindOperation(
        operation_id_);
  }

  std::optional<FailoverPhaseStage> Stage() const {
    const auto operation = Operation();
    if (!operation.has_value() || operation->kind_phase_blob_.empty()) {
      return std::nullopt;
    }
    auto phase = DecodeFailoverPhase(operation->kind_phase_blob_);
    if (!phase.ok()) return std::nullopt;
    return phase->stage_;
  }

  bool GateHeld() const {
    auto lease = membership_gate_->TryAcquire();
    return lease == nullptr;
  }

  absl::Status CommitCurrentDirective(std::string result) {
    const auto operation = Operation();
    if (!operation.has_value() || operation->current_directives_.size() != 1) {
      return absl::FailedPreconditionError("one current directive is required");
    }
    const MetaCurrentDirective& current =
        operation->current_directives_.front();
    CommitDirectiveResult receipt;
    receipt.operation_id_ = operation_id_;
    receipt.directive_id_ = current.spec_.directive_id_;
    receipt.attempt_id_ = current.spec_.attempt_id_;
    receipt.directive_revision_ = current.directive_revision_;
    receipt.recipient_node_id_ = current.spec_.recipient_node_id_;
    receipt.recipient_boot_id_ =
        current.spec_.recipient_node_id_ == current.spec_.source_node_id_
            ? current.spec_.source_boot_id_
            : current.spec_.target_boot_id_;
    receipt.assignment_id_ = current.spec_.assignment_id_;
    receipt.status_ = MetaDirectiveResultStatus::kSucceeded;
    receipt.result_ = std::move(result);
    receipt.result_hash_ = MetaSha256(receipt.result_);
    return Commit(std::move(receipt));
  }

  absl::Status CommitFrozenSourceResult() {
    control::FrozenSourceEvidence evidence{
        .recovery_generation = intent_.recovery_generation_,
        .source_history_id = Hex(parent_history_),
        .final_next_lsns = {21, 34},
    };
    auto proof = control::ComputeFrozenSourceProofHash(evidence);
    if (!proof.ok()) return proof.status();
    evidence.proof_hash = *proof;
    auto encoded = control::EncodeFrozenSourceEvidence(evidence);
    if (!encoded.ok()) return encoded.status();
    return CommitCurrentDirective(std::move(*encoded));
  }

  absl::Status CommitPromotionPreparedResult() {
    control::PromotionPreparedEvidence evidence{
        .parent_history_id = Hex(parent_history_),
        .frozen_applied_next_lsns = {21, 34},
        .population_generation = 11,
        .population_digest = 13,
        .catalog_generation = 17,
        .catalog_dump_crc64 = 19,
        .child_history_id = std::string(40, 'c'),
    };
    auto encoded = control::EncodePromotionPreparedEvidence(evidence);
    if (!encoded.ok()) return encoded.status();
    if (absl::Status status = CommitCurrentDirective(*encoded); !status.ok()) {
      return status;
    }

    const MetaCommittedView view = coordinator_->CommittedView();
    MetaOperationEvidenceObs observed{
        .node_id_ = std::string(kCandidateNode),
        .boot_incarnation_ = candidate_boot_,
        .assignment_id_ = candidate_assignment_,
        .operation_id_ = operation_id_,
        .kind_phase_ = "promotion-prepare:prepared",
        .evidence_hash_ = MetaSha256(*encoded),
        .evidence_ = *encoded,
        .group_id_ = std::string(kGroup),
        .group_term_ = intent_.group_term_,
        .population_manifest_revision_ = 1,
        .partition_replication_epoch_ = 1,
        .replication_history_id_ = parent_history_,
    };
    const MetaObservationIdentity identity{std::string(kCandidateNode),
                                           candidate_boot_, data_generation_};
    return observations_->Ingest(
        MetaObservation{.identity_ = identity, .payload_ = std::move(observed)},
        MetaStoresFacts(view.stores()), NowUnixMillis());
  }

  absl::Status ConfirmCandidateServingLease() {
    const MetaCommittedView view = coordinator_->CommittedView();
    auto projection = MetaControlProjector::ProjectNode(view, kCandidateNode);
    if (!projection.ok()) return projection.status();
    runtime_status_->PublishCurrent(
        std::string(kCandidateNode), Hex(candidate_boot_), candidate_session_,
        candidate_history_, data_generation_, data_generation_,
        view.applied_index(), projection->full_state);
    const auto grant = view.grant().GroupState(kGroup);
    if (!grant.has_value() || !grant->grant_.has_value()) {
      return absl::FailedPreconditionError("candidate grant is missing");
    }
    control::LeaseGranted decision{
        .nonce = Bytes<16>(0x73),
        .leader_id = 1,
        .raft_term = 9,
        .leadership_generation = data_generation_,
        .data_boot_id = Hex(candidate_boot_),
        .projection_hash = projection->full_state.projection_hash,
        .group_id = std::string(kGroup),
        .assignment_id = candidate_assignment_,
        .group_term = intent_.group_term_,
        .authority_version = intent_.authority_version_ + 1,
        .grant_revision = grant->grant_->grant_revision_,
        .granted_duration_ms = 1000,
    };
    const std::int64_t written = NowUnixMillis();
    const control::HeartbeatHealth healthy{
        .storage_ready = true,
        .population_ready = true,
        .draining = false,
        .active_groups = 1,
        .summary = "ready",
    };
    // Production records one heartbeat's health before its decision, then a
    // later ready heartbeat confirms that exact still-live grant.
    runtime_status_->RecordHealth(kCandidateNode, candidate_session_, healthy,
                                  written);
    runtime_status_->RecordLeaseDecisionWritten(
        kCandidateNode, candidate_session_, decision, written);
    runtime_status_->RecordHealth(kCandidateNode, candidate_session_, healthy,
                                  written + 1);
    return absl::OkStatus();
  }

  celer::Runtime runtime_;
  celer::ForeignExecutor executor_;
  bool runtime_started_ = false;
  std::filesystem::path directory_;
  nuraft::ptr<NuraftStateMgr> state_mgr_;
  nuraft::ptr<MetaStateMachine> machine_;
  NuraftLogStore* wal_ = nullptr;
  nuraft::ptr<ThreadScheduler> scheduler_;
  nuraft::ptr<nuraft::raft_server> server_;
  bool server_running_ = false;
  std::shared_ptr<MetaObservationStore> observations_ =
      std::make_shared<MetaObservationStore>();
  std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status_ =
      std::make_shared<MetaDataControlRuntimeStatus>();
  std::shared_ptr<MetaMembershipGate> membership_gate_ =
      std::make_shared<MetaMembershipGate>();
  std::unique_ptr<MetaCoordinator> coordinator_;
  std::shared_ptr<LeadershipProbe> lifecycle_probe_;
  std::shared_ptr<MetaControlledFailoverReconciler> reconciler_;
  std::uint64_t request_sequence_ = 0;
  std::uint64_t data_generation_ = 0;
  MetaAssignmentId former_assignment_ = Bytes<16>(0x11);
  MetaAssignmentId candidate_assignment_ = Bytes<16>(0x12);
  MetaBootIncarnation source_boot_ = Bytes<20>(0x21);
  MetaBootIncarnation candidate_boot_ = Bytes<20>(0x22);
  MetaReplicationHistoryId parent_history_ = Bytes<20>(0x31);
  MetaReplicationHistoryId candidate_history_ = Bytes<20>(0x61);
  MetaHash256 manifest_digest_{};
  MetaOperationId operation_id_ = Bytes<16>(0x41);
  FailoverIntent intent_;
  control::WireId128 source_session_{};
  control::WireId128 candidate_session_{};
};

TEST_F(MetaControlledFailoverLifecycleGateTest,
       ResumesAcrossLeadershipGraceAndReleasesHandoff) {
  reconciler_ = std::make_shared<MetaControlledFailoverReconciler>(
      executor_, membership_gate_, observations_, runtime_status_, kGraceMs);
  coordinator_->RunAsLeader(reconciler_);

  ASSERT_TRUE(WaitFor([this] { return GateHeld(); }, 3s));
  ASSERT_TRUE(WaitFor(
      [this] { return Stage() == FailoverPhaseStage::kSourceHolding; }, 3s));
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto operation = Operation();
        return operation.has_value() &&
               operation->current_directives_.size() == 1 &&
               operation->current_directives_.front().spec_.kind_ ==
                   kMetaDirectiveAuthorizeSource &&
               operation->current_directives_.front().spec_.payload_.empty();
      },
      3s));
  ASSERT_TRUE(CommitCurrentDirective("source-held").ok());

  // Demotion may race one final idempotent phase checkpoint, but the probe's
  // cancel callback is reached only after the real loop has drained and
  // released its topology-workflow lease.
  coordinator_->BecomeFollower();
  ASSERT_TRUE(lifecycle_probe_->WaitForCancels(1, 3s));
  ASSERT_TRUE(WaitFor([this] { return observations_->size() == 0; }, 1s));
  EXPECT_FALSE(GateHeld());
  runtime_status_->EndLeadership(/*leadership_generation=*/1);

  BeginDataGeneration(/*generation=*/2);
  coordinator_->BecomeLeader();
  ASSERT_TRUE(lifecycle_probe_->WaitForStarts(2, 3s));
  ASSERT_TRUE(WaitFor([this] { return GateHeld(); }, 3s));

  // A new leader begins with no session/FDS/candidate evidence. The bounded
  // gap is not a participant failure and must leave the durable operation
  // resumable rather than aborting at the first empty snapshot.
  std::this_thread::sleep_for(100ms);
  const auto during_grace = Operation();
  ASSERT_TRUE(during_grace.has_value());
  EXPECT_NE(during_grace->lifecycle_, MetaOperationLifecycle::kCompleted);
  EXPECT_NE(during_grace->lifecycle_, MetaOperationLifecycle::kAborted);

  ASSERT_TRUE(AdoptCandidateSession().ok());
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(ObserveCandidate({20, 33}).ok());

  // The resumed owner replays from the committed receipt and then makes the
  // only authority cut: term 2 is fenced before any candidate activation.
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto group = coordinator_->CommittedView().topology().FindGroup(
            std::string(kGroup));
        return group.has_value() && group->record_.group_term_ == 2;
      },
      3s));
  {
    const MetaCommittedView fenced = coordinator_->CommittedView();
    const auto group = fenced.topology().FindGroup(std::string(kGroup));
    const auto grant = fenced.grant().GroupState(kGroup);
    ASSERT_TRUE(group.has_value());
    ASSERT_TRUE(grant.has_value());
    EXPECT_EQ(group->record_.owner_, kFormerNode);
    EXPECT_TRUE(grant->fenced_);
    EXPECT_FALSE(grant->grant_.has_value());
  }
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(ObserveCandidate({21, 34}).ok());
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto operation = Operation();
        return operation.has_value() &&
               operation->current_directives_.size() == 1 &&
               operation->current_directives_.front().spec_.kind_ ==
                   kMetaDirectiveAuthorizeSource &&
               !operation->current_directives_.front().spec_.payload_.empty();
      },
      3s));
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(CommitFrozenSourceResult().ok());
  ASSERT_TRUE(ObserveCandidate({21, 34}).ok());

  ASSERT_TRUE(WaitFor(
      [this] {
        const auto operation = Operation();
        return operation.has_value() &&
               operation->current_directives_.size() == 1 &&
               operation->current_directives_.front().spec_.kind_ ==
                   kMetaDirectivePromotionPrepare;
      },
      3s));
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(CommitPromotionPreparedResult().ok());

  ASSERT_TRUE(WaitFor(
      [this] {
        const auto group = coordinator_->CommittedView().topology().FindGroup(
            std::string(kGroup));
        return group.has_value() && group->record_.owner_ == kCandidateNode &&
               group->record_.authority_version_ == 2;
      },
      3s));
  ASSERT_TRUE(WaitFor(
      [this] { return Stage() == FailoverPhaseStage::kAuthorityActivated; },
      3s));
  ASSERT_TRUE(ConfirmCandidateServingLease().ok());

  ASSERT_TRUE(WaitFor(
      [this] {
        const auto operation = Operation();
        return operation.has_value() &&
               operation->lifecycle_ == MetaOperationLifecycle::kCompleted;
      },
      3s));
  const auto completed = Operation();
  ASSERT_TRUE(completed.has_value());
  auto outcome = DecodeControlledFailoverOutcome(completed->terminal_result_);
  ASSERT_TRUE(outcome.ok()) << outcome.status();
  EXPECT_TRUE(outcome->succeeded_);
  EXPECT_EQ(outcome->loss_, FailoverLossClassification::kExact);
  EXPECT_EQ(outcome->proven_next_lsns_, (std::vector<std::uint64_t>{21, 34}));

  // Completion first writes the release tombstone. Clear is allowed only
  // after the exact old boot acknowledges an FDS without this generation's
  // source hold.
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto recovery =
            coordinator_->CommittedView().failover_recovery().Find(
                std::string(kGroup));
        return recovery.has_value() && !recovery->hold_required_ &&
               !recovery->recovery_required_;
      },
      3s));
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(WaitFor(
      [this] {
        return !coordinator_->CommittedView()
                    .failover_recovery()
                    .Find(std::string(kGroup))
                    .has_value();
      },
      3s));
  EXPECT_FALSE(GateHeld());
}

TEST_F(MetaControlledFailoverLifecycleGateTest,
       ArchivedExactHandoffDowngradesAfterSourceGraceExpires) {
  reconciler_ = std::make_shared<MetaControlledFailoverReconciler>(
      executor_, membership_gate_, observations_, runtime_status_, kGraceMs);
  coordinator_->RunAsLeader(reconciler_);

  ASSERT_TRUE(WaitFor(
      [this] { return Stage() == FailoverPhaseStage::kSourceHolding; }, 3s));
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto operation = Operation();
        return operation.has_value() &&
               operation->current_directives_.size() == 1 &&
               operation->current_directives_.front().spec_.kind_ ==
                   kMetaDirectiveAuthorizeSource &&
               operation->current_directives_.front().spec_.payload_.empty();
      },
      3s));
  ASSERT_TRUE(CommitCurrentDirective("source-held").ok());
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto group = coordinator_->CommittedView().topology().FindGroup(
            std::string(kGroup));
        return group.has_value() && group->record_.group_term_ == 2;
      },
      3s));

  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(ObserveCandidate({20, 33}).ok());
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto operation = Operation();
        return operation.has_value() &&
               operation->current_directives_.size() == 1 &&
               operation->current_directives_.front().spec_.kind_ ==
                   kMetaDirectiveAuthorizeSource &&
               !operation->current_directives_.front().spec_.payload_.empty();
      },
      3s));
  ASSERT_TRUE(PublishCurrentProjections().ok());
  ASSERT_TRUE(CommitFrozenSourceResult().ok());
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto recovery =
            coordinator_->CommittedView().failover_recovery().Find(kGroup);
        return recovery.has_value() &&
               recovery->proof_state_ == MetaFailoverProofState::kExact;
      },
      3s));

  runtime_status_->Remove(kCandidateNode, &candidate_session_);
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto operation = Operation();
        return operation.has_value() &&
               operation->lifecycle_ == MetaOperationLifecycle::kAborted;
      },
      3s));
  ASSERT_TRUE(WaitFor(
      [this] {
        const auto recovery =
            coordinator_->CommittedView().failover_recovery().Find(kGroup);
        return recovery.has_value() && recovery->hold_required_ &&
               recovery->recovery_required_ &&
               recovery->proof_state_ == MetaFailoverProofState::kExact;
      },
      3s));

  const auto terminal = Operation();
  ASSERT_TRUE(terminal.has_value());
  ArchiveOperations archive;
  archive.operation_seqs_ = {terminal->operation_seq_};
  ASSERT_TRUE(Commit(archive).ok());
  EXPECT_FALSE(Operation().has_value());
  EXPECT_TRUE(coordinator_->CommittedView()
                  .operation()
                  .FindArchived(operation_id_)
                  .has_value());

  runtime_status_->Remove(kFormerNode, &source_session_);
  std::this_thread::sleep_for(100ms);
  const auto during_grace =
      coordinator_->CommittedView().failover_recovery().Find(kGroup);
  ASSERT_TRUE(during_grace.has_value());
  EXPECT_EQ(during_grace->proof_state_, MetaFailoverProofState::kExact);

  ASSERT_TRUE(WaitFor(
      [this] {
        const auto recovery =
            coordinator_->CommittedView().failover_recovery().Find(kGroup);
        return recovery.has_value() &&
               recovery->proof_state_ == MetaFailoverProofState::kUnavailable;
      },
      3s));
  const auto unavailable =
      coordinator_->CommittedView().failover_recovery().Find(kGroup);
  ASSERT_TRUE(unavailable.has_value());
  ASSERT_TRUE(unavailable->frozen_proof_.has_value());
  EXPECT_EQ(unavailable->frozen_proof_->final_next_lsns_,
            (std::vector<std::uint64_t>{21, 34}));
}

}  // namespace
}  // namespace keylane::meta
