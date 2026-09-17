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

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "lavik/meta/automatic_failover_reconciler.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/control_projector.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/meta/data_control_server.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/nuraft_log_store.h"
#include "lavik/meta/nuraft_state_mgr.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/proposal_executor.h"
#include "lavik/meta/state_machine.h"
#include "libnuraft/nuraft.hxx"
#include "support/test_data_path.h"

namespace lavik::meta {

// Trusted test peer for the passkey-protected proposal boundary.
class MetaCoordinatorTestPeer {
 public:
  static AuthenticatedPrincipal Make(std::string principal) {
    return AuthenticatedPrincipal(std::move(principal), MetaPrincipalPasskey{});
  }
};

namespace {

using namespace std::chrono_literals;

template <std::size_t N>
std::array<std::uint8_t, N> Bytes(std::uint8_t value) {
  std::array<std::uint8_t, N> result{};
  result.fill(value);
  return result;
}

template <std::size_t N>
std::string HexBytes(const std::array<std::uint8_t, N>& value) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string result;
  result.reserve(N * 2);
  for (const std::uint8_t byte : value) {
    result.push_back(kDigits[byte >> 4]);
    result.push_back(kDigits[byte & 0x0f]);
  }
  return result;
}

std::string Node(std::uint8_t value) {
  std::string result(40, '0');
  constexpr char kHex[] = "0123456789abcdef";
  result[38] = kHex[value >> 4];
  result[39] = kHex[value & 0x0f];
  return result;
}

bool WaitUntil(const std::function<bool()>& predicate,
               std::chrono::milliseconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(5ms);
  }
  return predicate();
}

template <typename T>
T RunTaskSync(bycorf::Task<T> task) {
  std::promise<void> done;
  auto signal = done.get_future();
  task.SetCompletionCallback(
      &done, [](void* context, std::coroutine_handle<>) noexcept {
        static_cast<std::promise<void>*>(context)->set_value();
      });
  auto handle = std::move(task).ReleaseHandle();
  if (!handle) {
    ADD_FAILURE() << "task has no coroutine frame";
    return T{};
  }
  handle.resume();
  if (signal.wait_for(25s) != std::future_status::ready) {
    ADD_FAILURE() << "task did not complete";
    handle.destroy();
    return T{};
  }
  T result = std::move(handle.promise().value_);
  handle.destroy();
  return result;
}

class ThreadScheduler final : public nuraft::delayed_task_scheduler {
 public:
  ~ThreadScheduler() override { Shutdown(); }

  void schedule(nuraft::ptr<nuraft::delayed_task>& task,
                nuraft::int32 delay_ms) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) return;
    threads_.emplace_back([this, task, delay_ms] {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        stopped_cv_.wait_for(lock, std::chrono::milliseconds(delay_ms),
                             [this] { return stopped_; });
        if (stopped_) return;
      }
      task->execute();
    });
  }

  void Shutdown() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
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

  std::mutex mutex_;
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

class MetaAutomaticFailoverReconcilerTest : public ::testing::Test {
 protected:
  struct SeedState {
    std::string owner_ = Node(1);
    MetaAssignmentId owner_assignment_ = Bytes<16>(0x21);
    MetaOperationId controlled_operation_id_ = Bytes<16>(0x31);
  };

  // Ordinary fixture writes need the normal completion budget under parallel
  // load. Only the explicit proposal-timeout test shortens it.
  virtual std::uint64_t ProposeTimeoutMs() const { return 5'000; }

  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    dir_ = test::TestDataDirectory() /
           ("lavik_automatic_reconciler_" + std::string(info->name()) + "_" +
            std::to_string(::getpid()));
    std::error_code ignored;
    std::filesystem::remove_all(dir_, ignored);

    runtime_ = std::make_unique<bycorf::Runtime>();
    std::promise<absl::Status> initialized;
    auto initialized_result = initialized.get_future();
    runtime_->Start(
        1,
        [&initialized](unsigned, bycorf::Worker& worker) {
          const absl::Status status = worker.Init();
          initialized.set_value(status);
          if (!status.ok()) return 1;
          worker.Run();
          worker.Shutdown();
          worker.DestroyDetachedTasks();
          return 0;
        },
        false);
    ASSERT_TRUE(initialized_result.get().ok());
    executor_ = runtime_->GetForeignExecutor(0);

    const NuraftMemberConfig local{1, "127.0.0.1:19601", "lavik://meta/1",
                                   "127.0.0.1:19701", "127.0.0.1:19801"};
    NuraftStateMgrOpenOptions manager_options{.data_dir_ = dir_,
                                              .local_member_ = local};
    manager_options.initial_cluster_ = std::vector{local};
    auto manager = NuraftStateMgr::Open(std::move(manager_options));
    ASSERT_TRUE(manager.ok()) << manager.status();
    manager_ = nuraft::ptr<NuraftStateMgr>(std::move(*manager));
    auto machine = MetaStateMachine::Open(dir_);
    ASSERT_TRUE(machine.ok()) << machine.status();
    machine_ = nuraft::ptr<MetaStateMachine>(std::move(*machine));

    scheduler_ = nuraft::cs_new<ThreadScheduler>();
    nuraft::raft_params params;
    params.with_election_timeout_lower(500);
    params.with_election_timeout_upper(900);
    params.with_hb_interval(50);
    params.with_snapshot_enabled(0);
    params.with_reserved_log_items(0);
    params.with_client_req_timeout(5'000);
    params.return_method_ = nuraft::raft_params::async_handler;
    params.wait_for_sm_catchup_on_becoming_leader_ = true;
    nuraft::context* context = new nuraft::context(
        manager_, machine_, /*listener=*/nullptr, /*logger=*/nullptr,
        nuraft::cs_new<NullRpcClientFactory>(), scheduler_, params);
    nuraft::raft_server::init_options init_options;
    init_options.raft_callback_ = [this](nuraft::cb_func::Type type,
                                         nuraft::cb_func::Param*) {
      std::lock_guard<std::mutex> lock(role_mutex_);
      if (coordinator_ == nullptr) return nuraft::cb_func::Ok;
      if (type == nuraft::cb_func::BecomeLeader) {
        coordinator_->BecomeLeader();
      } else if (type == nuraft::cb_func::BecomeFollower) {
        coordinator_->BecomeFollower();
      }
      return nuraft::cb_func::Ok;
    };
    server_ = nuraft::cs_new<nuraft::raft_server>(context, init_options);

    nuraft::ptr<nuraft::log_store> store = manager_->load_log_store();
    wal_ = static_cast<NuraftLogStore*>(store.get());
    MetaCoordinatorOptions coordinator_options;
    coordinator_options.foreign_executor_ = executor_;
    coordinator_options.propose_timeout_ms_ = ProposeTimeoutMs();
    coordinator_options.proposal_executor_ = &proposal_executor_;
    coordinator_ = std::make_unique<MetaCoordinator>(
        server_, *machine_, *wal_, observations_, coordinator_options);
    ASSERT_TRUE(WaitUntil([this] { return server_->is_leader(); }, 15s));
    // Harmless if the organic callback already arrived; it closes the tiny
    // fixture-only race between election and callback target attachment.
    coordinator_->BecomeLeader();
  }

  void TearDown() override {
    ReleaseReconcilerExecutor();
    ReleaseProposalExecutor();
    {
      std::lock_guard<std::mutex> lock(role_mutex_);
      coordinator_.reset();
    }
    if (reconciler_ != nullptr) reconciler_->Shutdown();
    reconciler_.reset();
    if (server_ != nullptr) server_->shutdown();
    if (machine_ != nullptr) machine_->WaitForSnapshotWriterIdle();
    server_.reset();
    if (scheduler_ != nullptr) scheduler_->Shutdown();
    scheduler_.reset();
    machine_.reset();
    manager_.reset();
    wal_ = nullptr;
    executor_.WaitUntilIdle();
    runtime_->RequestStop();
    runtime_->WaitUntilStopped();
    EXPECT_EQ(runtime_->exit_code(), 0);
    runtime_.reset();
    std::error_code ignored;
    std::filesystem::remove_all(dir_, ignored);
  }

  void ProposeAccepted(const MetaCommand& command) {
    auto result = RunTaskSync(coordinator_->Propose(
        command,
        MetaCoordinatorTestPeer::Make("lavik://operator/automatic-test")));
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result->verdict_, MetaAuditVerdict::kAccepted) << result->detail_;
  }

  SeedState SeedCluster(bool controlled_request = false) {
    SeedState state;
    ClusterCreateManifestV1 manifest;
    manifest.schema_version_ = 1;
    manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101",
                               "tcp://127.0.0.1:7301", "tcp://127.0.0.1:7201"}};
    manifest.data_nodes_ = {{state.owner_, "tcp://127.0.0.1:6379"}};
    manifest.groups_ = {{"g1", state.owner_, {}}};
    manifest.slot_ranges_ = {{0, 16'383, "g1"}};

    SubmitOperation root;
    root.request_id_ = Bytes<16>(0x01);
    root.operation_id_ = Bytes<16>(0x02);
    root.kind_ = std::string(kMetaClusterCreateOperationKind);
    auto root_intent = EncodeClusterCreateRequest(manifest, root.operation_id_);
    if (!root_intent.ok()) {
      ADD_FAILURE() << root_intent.status();
      return state;
    }
    root.intent_ = *root_intent;
    root.intent_hash_ = MetaSha256(root.intent_);
    ProposeAccepted(root);

    RegisterNode owner;
    owner.request_id_ = Bytes<16>(0x04);
    owner.node_id_ = state.owner_;
    owner.principal_ = "lavik://node/" + state.owner_;
    owner.endpoints_ = {"10.0.0.1:7000"};
    owner.role_ = MetaNodeRole::kPrimary;
    ProposeAccepted(owner);

    CreateGroup group;
    group.request_id_ = Bytes<16>(0x05);
    group.group_id_ = "g1";
    group.new_topology_epoch_ = 1;
    ProposeAccepted(group);

    AssignNodeToGroup assign;
    assign.request_id_ = Bytes<16>(0x06);
    assign.group_id_ = "g1";
    assign.node_id_ = state.owner_;
    assign.assignment_id_ = state.owner_assignment_;
    assign.role_ = MetaNodeRole::kPrimary;
    assign.expected_revision_ = 1;
    assign.new_topology_epoch_ = 2;
    ProposeAccepted(assign);

    PutPolicy automatic;
    automatic.request_id_ = Bytes<16>(0x07);
    automatic.policy_id_ = std::string(kAutomaticUncontrolledFailoverPolicyId);
    automatic.version_ = 1;
    automatic.content_ =
        R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":1000})";
    ProposeAccepted(automatic);

    PutPolicy lease;
    lease.request_id_ = Bytes<16>(0x08);
    lease.policy_id_ = std::string(kAuthorityLeasePolicyId);
    lease.version_ = 1;
    lease.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
    ProposeAccepted(lease);

    BeginGroupTerm term;
    term.request_id_ = Bytes<16>(0x09);
    term.group_id_ = "g1";
    term.expected_term_ = 0;
    term.new_term_ = 1;
    ProposeAccepted(term);

    ActivateAuthority activate;
    activate.request_id_ = Bytes<16>(0x0a);
    activate.group_id_ = "g1";
    activate.expected_term_ = 1;
    activate.new_owner_ = state.owner_;
    activate.new_topology_epoch_ = 3;
    ProposeAccepted(activate);

    // Created is a durable aggregate invariant, not permission to provision
    // the remaining state afterward. Complete only after both required
    // Policies and the current Owner authority are present so every prefix of
    // this fixture can survive snapshot round-trip validation.
    CompleteOperation complete;
    complete.request_id_ = Bytes<16>(0x03);
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ProposeAccepted(complete);

    if (controlled_request) {
      FailoverOperationIntent intent;
      intent.group_id_ = "g1";
      intent.absolute_deadline_unix_ms_ = 2'000'000'000'000ULL;
      auto encoded_intent = EncodeFailoverOperationIntent(intent);
      if (!encoded_intent.ok()) {
        ADD_FAILURE() << encoded_intent.status();
        return state;
      }
      SubmitOperation submit;
      submit.request_id_ = Bytes<16>(0x0b);
      submit.operation_id_ = state.controlled_operation_id_;
      submit.kind_ = std::string(kFailoverOperationKind);
      submit.intent_ = *encoded_intent;
      submit.intent_hash_ = MetaSha256(submit.intent_);
      ProposeAccepted(submit);
    }
    return state;
  }

  void InstallReconciler(std::function<absl::StatusOr<MetaRequestId>()> next_id,
                         std::uint64_t grace_ms = 100,
                         std::uint32_t observation_ttl_ms = 1'000) {
    MetaAutomaticFailoverReconcilerOptions options;
    options.data_control_runtime_status_ = data_runtime_;
    options.diagnostics_ = diagnostics_;
    options.observation_ttl_ms_ = observation_ttl_ms;
    options.observation_grace_ms_ = grace_ms;
    options.poll_interval_ = 5ms;
    options.now_steady_ms_ = [this] {
      return now_steady_ms_.load(std::memory_order_acquire);
    };
    options.next_id_ = std::move(next_id);
    reconciler_ = std::make_shared<MetaAutomaticFailoverReconciler>(
        executor_, std::move(options));
    coordinator_->AddValidateHook(reconciler_->validation_hook());
    coordinator_->RunAsLeader(reconciler_);
  }

  std::function<absl::StatusOr<MetaRequestId>()> CountingIds(
      std::atomic<int>& count, std::uint8_t first = 0x70) {
    return [&count, first]() -> absl::StatusOr<MetaRequestId> {
      const int offset = count.fetch_add(1, std::memory_order_acq_rel);
      return Bytes<16>(static_cast<std::uint8_t>(first + offset));
    };
  }

  // Poll one atomic cut without treating the async first publication (or a
  // leadership reset) as a failed assertion or a default detector state.
  bool WaitForGroupStatus(
      const std::function<bool(const MetaAutomaticFailoverStatus&)>& predicate,
      std::chrono::milliseconds timeout = 5s) const {
    return WaitUntil(
        [&] {
          const auto snapshot = diagnostics_->Snapshot();
          return snapshot.statuses_.size() == 1 &&
                 predicate(snapshot.statuses_.front());
        },
        timeout);
  }

  // Warmup starts when the worker observes eligibility, not when the test
  // publishes it. Keep the manual clock fixed until that observation is made.
  bool WaitForLeadershipWarmup(std::uint64_t generation) const {
    return WaitForGroupStatus([generation](const auto& status) {
      return status.anchor_.leadership_generation_ == generation &&
             status.blocker_ == MetaAutomaticFailoverBlocker::kLeadershipWarmup;
    });
  }

  // Use only after publication is established, including invariant checks
  // where losing the previously observed diagnostic is itself a failure.
  MetaAutomaticFailoverStatus GroupStatus() const {
    const auto snapshot = diagnostics_->Snapshot();
    EXPECT_EQ(snapshot.statuses_.size(), 1u);
    return snapshot.statuses_.empty() ? MetaAutomaticFailoverStatus{}
                                      : snapshot.statuses_.front();
  }

  void StartEligibleGeneration(std::uint64_t generation) {
    data_runtime_->BeginLeadership(generation);
    data_runtime_->SetLeaderAuthorityEligible(generation, true);
  }

  absl::Status PublishOwnerHeartbeat(
      const MetaCommittedView& view, const SeedState& seed,
      MetaNodeHealthObs health, bool causally_confirm_lease,
      std::int64_t observed_at_unix_ms, std::uint64_t observed_at_steady_ms,
      std::uint32_t effective_lease_duration_ms = 5'000) {
    auto projected = MetaControlProjector::ProjectNode(view, seed.owner_);
    if (!projected.ok()) return projected.status();
    const auto group =
        std::find_if(projected->full_state.groups.begin(),
                     projected->full_state.groups.end(),
                     [](const cluster::control::WireDesiredGroup& candidate) {
                       return candidate.group_id == "g1";
                     });
    if (group == projected->full_state.groups.end() ||
        !group->owner_node_id.has_value() ||
        !group->owner_assignment_id.has_value()) {
      return absl::FailedPreconditionError(
          "test projection lacks the current Owner authority");
    }
    // Exercise the same effective lease transformation as production so
    // detector tests can distinguish the effective lease from global Policy.
    if (absl::Status limited = detail::ApplyLeadershipValidityLimit(
            *projected, effective_lease_duration_ms);
        !limited.ok()) {
      return limited;
    }

    const MetaBootIncarnation boot = Bytes<20>(0x11);
    const MetaReplicationHistoryId history = Bytes<20>(0x22);
    const auto session_id = Bytes<16>(0x33);
    const MetaObservationIdentity identity{seed.owner_, boot, 1};
    if (absl::Status adopted =
            observations_.AdoptSession(identity, observed_at_unix_ms, history);
        !adopted.ok()) {
      return adopted;
    }
    data_runtime_->PublishCurrent(
        seed.owner_, std::string(40, '1'), session_id, history,
        /*replication_flow_count=*/1, /*session_generation=*/1,
        /*leadership_generation=*/1,
        /*validated_committed_high_water=*/view.applied_index(),
        projected->full_state);

    const MetaObservedOwnerProjection owner_projection{
        .group_id_ = group->group_id,
        .owner_node_id_ = *group->owner_node_id,
        .owner_assignment_id_ = *group->owner_assignment_id,
        .group_term_ = group->group_term,
        .control_revision_ = projected->full_state.control_revision,
        .authority_lease_duration_ms_ =
            projected->full_state.authority_lease_duration_ms,
    };
    const MetaStoresFacts facts(view.stores());
    auto first = observations_.ReplaceHeartbeat(
        identity, health, std::nullopt, std::nullopt, std::nullopt,
        owner_projection, /*heartbeat_sequence=*/1, std::nullopt, facts,
        observed_at_unix_ms, observed_at_steady_ms);
    for (const absl::Status* status :
         {&first.boot_status_, &first.health_status_, &first.candidate_status_,
          &first.failover_status_}) {
      if (!status->ok()) return *status;
    }
    if (!causally_confirm_lease) return absl::OkStatus();

    auto confirmed = observations_.ReplaceHeartbeat(
        identity, std::move(health), std::nullopt, std::nullopt, std::nullopt,
        owner_projection, /*heartbeat_sequence=*/2,
        /*confirmed_grant_sequence=*/1, facts, observed_at_unix_ms,
        observed_at_steady_ms);
    for (const absl::Status* status :
         {&confirmed.boot_status_, &confirmed.health_status_,
          &confirmed.candidate_status_, &confirmed.failover_status_}) {
      if (!status->ok()) return *status;
    }
    return absl::OkStatus();
  }

  absl::Status ContinueOwnerHeartbeat(
      const MetaCommittedView& view, const SeedState& seed,
      std::uint64_t heartbeat_sequence,
      std::optional<std::uint64_t> confirmed_ack_sequence,
      std::uint32_t effective_lease_duration_ms,
      std::int64_t observed_at_unix_ms, std::uint64_t observed_at_steady_ms,
      bool draining = false, bool storage_ready = true,
      bool population_ready = true) {
    auto projected = MetaControlProjector::ProjectNode(view, seed.owner_);
    if (!projected.ok()) return projected.status();
    if (absl::Status limited = detail::ApplyLeadershipValidityLimit(
            *projected, effective_lease_duration_ms);
        !limited.ok()) {
      return limited;
    }
    const auto group =
        std::find_if(projected->full_state.groups.begin(),
                     projected->full_state.groups.end(),
                     [](const cluster::control::WireDesiredGroup& candidate) {
                       return candidate.group_id == "g1";
                     });
    if (group == projected->full_state.groups.end() ||
        !group->owner_node_id.has_value() ||
        !group->owner_assignment_id.has_value()) {
      return absl::FailedPreconditionError(
          "test projection lacks the current Owner authority");
    }
    const MetaObservedOwnerProjection owner_projection{
        .group_id_ = group->group_id,
        .owner_node_id_ = *group->owner_node_id,
        .owner_assignment_id_ = *group->owner_assignment_id,
        .group_term_ = group->group_term,
        .control_revision_ = projected->full_state.control_revision,
        .authority_lease_duration_ms_ =
            projected->full_state.authority_lease_duration_ms,
    };
    const MetaObservationIdentity identity{seed.owner_, Bytes<20>(0x11), 1};
    const MetaStoresFacts facts(view.stores());
    auto result = observations_.ReplaceHeartbeat(
        identity,
        MetaNodeHealthObs{.storage_ready_ = storage_ready,
                          .population_ready_ = population_ready,
                          .draining_ = draining,
                          .active_groups_ = 1},
        std::nullopt, std::nullopt, std::nullopt, owner_projection,
        heartbeat_sequence, confirmed_ack_sequence, facts, observed_at_unix_ms,
        observed_at_steady_ms);
    for (const absl::Status* status :
         {&result.boot_status_, &result.health_status_,
          &result.candidate_status_, &result.failover_status_}) {
      if (!status->ok()) return *status;
    }
    return absl::OkStatus();
  }

  absl::Status PublishOwnerFds(const MetaCommittedView& view,
                               const SeedState& seed,
                               std::uint32_t effective_lease_duration_ms) {
    auto projected = MetaControlProjector::ProjectNode(view, seed.owner_);
    if (!projected.ok()) return projected.status();
    if (absl::Status limited = detail::ApplyLeadershipValidityLimit(
            *projected, effective_lease_duration_ms);
        !limited.ok()) {
      return limited;
    }
    data_runtime_->PublishCurrent(
        seed.owner_, std::string(40, '1'), Bytes<16>(0x33), Bytes<20>(0x22),
        /*replication_flow_count=*/1, /*session_generation=*/1,
        /*leadership_generation=*/1,
        /*validated_committed_high_water=*/view.applied_index(),
        projected->full_state);
    return absl::OkStatus();
  }

  absl::Status RecordPossibleOwnerLease(const SeedState& seed,
                                        std::uint64_t heartbeat_sequence) {
    const auto observed = observations_.OwnerObservationFor(seed.owner_);
    if (!observed.has_value() || !observed->owner_projection_.has_value()) {
      return absl::FailedPreconditionError(
          "test Owner observation has no installed projection");
    }
    const MetaObservedOwnerProjection& projection =
        *observed->owner_projection_;
    return observations_.RecordOwnerLeaseDecisionAttempt(
        observed->identity_, heartbeat_sequence,
        cluster::control::LeaseGranted{
            .data_boot_id = HexBytes(observed->identity_.boot_incarnation_),
            .control_revision = projection.control_revision_,
            .group_id = projection.group_id_,
            .assignment_id = projection.owner_assignment_id_,
            .group_term = projection.group_term_,
            .granted_duration_ms = projection.authority_lease_duration_ms_,
        });
  }

  absl::Status RecordOwnerLeaseDecision(
      const SeedState& seed, std::uint64_t heartbeat_sequence,
      const cluster::control::LeaseDecision& decision) {
    const auto observed = observations_.OwnerObservationFor(seed.owner_);
    if (!observed.has_value()) {
      return absl::FailedPreconditionError(
          "test Owner observation has no current session");
    }
    return observations_.RecordOwnerLeaseDecisionAttempt(
        observed->identity_, heartbeat_sequence, decision);
  }

  absl::Status RecordOwnerLeaseDecisionWritten(
      const SeedState& seed, std::uint64_t heartbeat_sequence,
      const cluster::control::LeaseDecision& decision) {
    const auto observed = observations_.OwnerObservationFor(seed.owner_);
    if (!observed.has_value()) {
      return absl::FailedPreconditionError(
          "test Owner observation has no current session");
    }
    return observations_.RecordOwnerLeaseDecisionWritten(
        observed->identity_, heartbeat_sequence, decision);
  }

  void BlockProposalExecutor() {
    ASSERT_TRUE(proposal_executor_
                    .Submit([this] {
                      std::unique_lock<std::mutex> lock(proposal_block_mutex_);
                      proposal_block_entered_ = true;
                      proposal_block_cv_.notify_all();
                      proposal_block_cv_.wait(
                          lock, [this] { return release_proposal_block_; });
                    })
                    .ok());
    std::unique_lock<std::mutex> lock(proposal_block_mutex_);
    ASSERT_TRUE(proposal_block_cv_.wait_for(
        lock, 2s, [this] { return proposal_block_entered_; }));
  }

  void ReleaseProposalExecutor() {
    {
      std::lock_guard<std::mutex> lock(proposal_block_mutex_);
      release_proposal_block_ = true;
    }
    proposal_block_cv_.notify_all();
  }

  void BlockReconcilerExecutor() {
    ASSERT_TRUE(executor_.Notify([this]() noexcept {
      std::unique_lock<std::mutex> lock(reconciler_block_mutex_);
      reconciler_block_entered_ = true;
      reconciler_block_cv_.notify_all();
      reconciler_block_cv_.wait(lock,
                                [this] { return release_reconciler_block_; });
    }));
    std::unique_lock<std::mutex> lock(reconciler_block_mutex_);
    ASSERT_TRUE(reconciler_block_cv_.wait_for(
        lock, 2s, [this] { return reconciler_block_entered_; }));
  }

  void ReleaseReconcilerExecutor() {
    {
      std::lock_guard<std::mutex> lock(reconciler_block_mutex_);
      release_reconciler_block_ = true;
    }
    reconciler_block_cv_.notify_all();
  }

  std::filesystem::path dir_;
  std::unique_ptr<bycorf::Runtime> runtime_;
  bycorf::ForeignExecutor executor_;
  nuraft::ptr<NuraftStateMgr> manager_;
  nuraft::ptr<MetaStateMachine> machine_;
  NuraftLogStore* wal_ = nullptr;
  nuraft::ptr<ThreadScheduler> scheduler_;
  nuraft::ptr<nuraft::raft_server> server_;
  MetaObservationStore observations_;
  MetaProposalExecutor proposal_executor_;
  std::unique_ptr<MetaCoordinator> coordinator_;
  std::shared_ptr<MetaDataControlRuntimeStatus> data_runtime_ =
      std::make_shared<MetaDataControlRuntimeStatus>();
  std::shared_ptr<MetaAutomaticFailoverDiagnosticsRegistry> diagnostics_ =
      std::make_shared<MetaAutomaticFailoverDiagnosticsRegistry>();
  std::shared_ptr<MetaAutomaticFailoverReconciler> reconciler_;
  std::atomic<std::uint64_t> now_steady_ms_{10'000};
  std::atomic<std::int64_t> now_unix_ms_{1'000'000};
  std::mutex role_mutex_;
  std::mutex proposal_block_mutex_;
  std::condition_variable proposal_block_cv_;
  bool proposal_block_entered_ = false;
  bool release_proposal_block_ = false;
  std::mutex reconciler_block_mutex_;
  std::condition_variable reconciler_block_cv_;
  bool reconciler_block_entered_ = false;
  bool release_reconciler_block_ = false;
};

class MetaAutomaticFailoverReconcilerTimeoutTest
    : public MetaAutomaticFailoverReconcilerTest {
 protected:
  std::uint64_t ProposeTimeoutMs() const override { return 100; }
};

TEST_F(MetaAutomaticFailoverReconcilerTest,
       DiagnosticsCatchUpAfterConfigurationOnlyCommit) {
  SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids));
  StartEligibleGeneration(1);
  ASSERT_TRUE(WaitForLeadershipWarmup(1));

  const auto before = machine_->last_commit_index();
  ASSERT_TRUE(WaitUntil([&] {
    return diagnostics_->Snapshot().evaluated_applied_index_ == before;
  }));
  const auto stores_high_water = coordinator_->CommittedHighWater();
  // A new leader commits a Raft configuration without a Meta command event.
  // Status still compares the detector's cut with this full applied cursor;
  // it must recover without waiting for an unrelated topology mutation.
  nuraft::ptr<nuraft::cluster_config> config;
  machine_->commit_config(before + 1, config);
  ASSERT_EQ(machine_->last_commit_index(), before + 1);
  EXPECT_EQ(coordinator_->CommittedHighWater(), stores_high_water);
  EXPECT_TRUE(WaitUntil([&] {
    return diagnostics_->Snapshot().evaluated_applied_index_ == before + 1;
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);
  EXPECT_EQ(GroupStatus().blocker_,
            MetaAutomaticFailoverBlocker::kLeadershipWarmup);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       NewGenerationRepeatsWarmupAndAFullDebounce) {
  SeedCluster();
  std::atomic<int> generated_ids{0};
  BlockReconcilerExecutor();
  InstallReconciler(CountingIds(generated_ids));
  StartEligibleGeneration(1);

  // Force the pre-publication window regardless of worker scheduling. Even an
  // unconditional predicate must keep waiting while no Group cut exists.
  EXPECT_FALSE(WaitForGroupStatus([](const auto&) { return true; }, 0ms));
  ReleaseReconcilerExecutor();
  ASSERT_TRUE(WaitForLeadershipWarmup(1));

  now_steady_ms_.store(10'100, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.accumulated_suspect_ms_ == 0;
  }));
  now_steady_ms_.store(11'099, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.accumulated_suspect_ms_ == 999;
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);

  // A new leadership generation discards both the old warmup and its 999 ms
  // suspicion; neither interval is allowed to leak into the new bracket.
  now_steady_ms_.store(20'000, std::memory_order_release);
  StartEligibleGeneration(2);
  ASSERT_TRUE(WaitForLeadershipWarmup(2));
  now_steady_ms_.store(20'100, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.accumulated_suspect_ms_ == 0;
  }));
  now_steady_ms_.store(21'099, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.accumulated_suspect_ms_ == 999;
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);

  now_steady_ms_.store(21'100, std::memory_order_release);
  ASSERT_TRUE(WaitUntil([&] {
    const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
    return group.has_value() && group->failover_transition_.has_value();
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       EligibilityLossBetweenPollsRepeatsWarmupAndAFullDebounce) {
  SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids));
  StartEligibleGeneration(1);

  ASSERT_TRUE(WaitForLeadershipWarmup(1));
  now_steady_ms_.store(10'100, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.accumulated_suspect_ms_ == 0;
  }));
  now_steady_ms_.store(11'099, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.accumulated_suspect_ms_ == 999;
  }));

  // Hold the detector worker so the false -> true eligibility interruption is
  // entirely between two snapshots. A boolean-only status loses this ABA edge
  // and lets the old 999 ms suspicion trigger immediately after recovery.
  BlockReconcilerExecutor();
  data_runtime_->SetLeaderAuthorityEligible(1, false);
  data_runtime_->SetLeaderAuthorityEligible(1, true);
  now_steady_ms_.store(11'100, std::memory_order_release);
  ReleaseReconcilerExecutor();

  ASSERT_TRUE(WaitUntil(
      [&] {
        const auto snapshot = diagnostics_->Snapshot();
        const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
        return (snapshot.statuses_.size() == 1 &&
                snapshot.statuses_[0].blocker_ ==
                    MetaAutomaticFailoverBlocker::kLeadershipWarmup) ||
               (group.has_value() && group->failover_transition_.has_value());
      },
      1s));
  EXPECT_FALSE(machine_->StoresSnapshot()
                   .topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
  const auto recovered_diagnostics = diagnostics_->Snapshot();
  ASSERT_EQ(recovered_diagnostics.statuses_.size(), 1u);
  EXPECT_EQ(recovered_diagnostics.leader_authority_eligibility_revision_, 3u);
  EXPECT_EQ(recovered_diagnostics.statuses_[0]
                .anchor_.leader_authority_eligibility_revision_,
            3u);
  EXPECT_EQ(GroupStatus().blocker_,
            MetaAutomaticFailoverBlocker::kLeadershipWarmup);
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);

  now_steady_ms_.store(11'200, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.accumulated_suspect_ms_ == 0;
  }));
  now_steady_ms_.store(12'199, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.accumulated_suspect_ms_ == 999;
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);

  now_steady_ms_.store(12'200, std::memory_order_release);
  ASSERT_TRUE(WaitUntil([&] {
    const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
    return group.has_value() && group->failover_transition_.has_value();
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       ProposalRevalidatesEligibilityContinuityImmediatelyBeforeAppend) {
  SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(
      [&]() -> absl::StatusOr<MetaRequestId> {
        const int offset =
            generated_ids.fetch_add(1, std::memory_order_acq_rel);
        if (offset == 1) {
          // The detector already reached its edge from an eligible cut.
          // Revoking and restoring eligibility here lands strictly between that
          // cut and Propose's hook; the final boolean alone looks unchanged.
          data_runtime_->SetLeaderAuthorityEligible(1, false);
          data_runtime_->SetLeaderAuthorityEligible(1, true);
        }
        return Bytes<16>(static_cast<std::uint8_t>(0x80 + offset));
      },
      /*grace_ms=*/0);
  StartEligibleGeneration(1);

  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.leadership_generation_ == 1 &&
           snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ == MetaAutomaticFailoverState::kSuspect;
  }));
  const std::uint64_t before = machine_->last_commit_index();
  now_steady_ms_.store(11'000, std::memory_order_release);
  ASSERT_TRUE(WaitUntil(
      [&] { return generated_ids.load(std::memory_order_acquire) == 2; }));
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ ==
               MetaAutomaticFailoverState::kSuspect &&
           snapshot.statuses_[0].accumulated_suspect_ms_ == 0;
  }));
  EXPECT_EQ(machine_->last_commit_index(), before);
  EXPECT_FALSE(machine_->StoresSnapshot()
                   .topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       ServiceableRecoveryImmediatelyBeforeAppendSuppressesBegin) {
  const SeedState seed = SeedCluster();
  std::promise<absl::Status> injected;
  std::future<absl::Status> injection = injected.get_future();
  std::atomic<bool> injected_once{false};
  coordinator_->AddValidateHook(
      [&](const MetaCommand& command, const MetaCommittedView& view,
          const MetaObservationStore&, std::int64_t proposal_now_unix_ms) {
        const auto* begin = std::get_if<BeginUncontrolledFailover>(&command);
        if (begin == nullptr ||
            begin->trigger_reason_ == MetaAutomaticFailoverReason::kManual ||
            injected_once.exchange(true, std::memory_order_acq_rel)) {
          return absl::OkStatus();
        }
        now_unix_ms_.store(proposal_now_unix_ms, std::memory_order_release);
        absl::Status status = PublishOwnerHeartbeat(
            view, seed,
            MetaNodeHealthObs{.storage_ready_ = true,
                              .population_ready_ = true,
                              .draining_ = false,
                              .active_groups_ = 1},
            /*causally_confirm_lease=*/true, proposal_now_unix_ms,
            now_steady_ms_.load(std::memory_order_acquire));
        injected.set_value(status);
        return status;
      });
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids, 0x80), /*grace_ms=*/0);
  StartEligibleGeneration(1);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ == MetaAutomaticFailoverState::kSuspect;
  }));

  const std::uint64_t before = machine_->last_commit_index();
  now_steady_ms_.store(11'000, std::memory_order_release);
  ASSERT_EQ(injection.wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(injection.get().ok());
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ == MetaAutomaticFailoverState::kHealthy;
  }));

  EXPECT_EQ(machine_->last_commit_index(), before);
  EXPECT_FALSE(machine_->StoresSnapshot()
                   .topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       FailureReasonChangeImmediatelyBeforeAppendPreservesAdmission) {
  const SeedState seed = SeedCluster();
  std::promise<absl::Status> injected;
  std::future<absl::Status> injection = injected.get_future();
  std::atomic<bool> injected_once{false};
  std::atomic<std::uint8_t> proposed_reason{0};
  coordinator_->AddValidateHook(
      [&](const MetaCommand& command, const MetaCommittedView& view,
          const MetaObservationStore&, std::int64_t proposal_now_unix_ms) {
        const auto* begin = std::get_if<BeginUncontrolledFailover>(&command);
        if (begin == nullptr ||
            begin->trigger_reason_ == MetaAutomaticFailoverReason::kManual ||
            injected_once.exchange(true, std::memory_order_acq_rel)) {
          return absl::OkStatus();
        }
        proposed_reason.store(static_cast<std::uint8_t>(begin->trigger_reason_),
                              std::memory_order_release);
        now_unix_ms_.store(proposal_now_unix_ms, std::memory_order_release);
        absl::Status status = PublishOwnerHeartbeat(
            view, seed,
            MetaNodeHealthObs{.storage_ready_ = false,
                              .population_ready_ = true,
                              .draining_ = false,
                              .active_groups_ = 1},
            /*causally_confirm_lease=*/false, proposal_now_unix_ms,
            now_steady_ms_.load(std::memory_order_acquire));
        injected.set_value(status);
        return status;
      });
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids, 0x88), /*grace_ms=*/0);
  StartEligibleGeneration(1);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ ==
               MetaAutomaticFailoverState::kSuspect &&
           snapshot.statuses_[0].current_reason_ ==
               MetaOwnerServiceabilityReason::kSessionMissing;
  }));

  now_steady_ms_.store(11'000, std::memory_order_release);
  ASSERT_EQ(injection.wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(injection.get().ok());
  ASSERT_TRUE(WaitUntil(
      [&] {
        const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
        return group.has_value() && group->failover_transition_.has_value();
      },
      1s))
      << "changing between exact failure reasons must not restart debounce";

  const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(group->failover_transition_->transition_id_, Bytes<16>(0x89));
  EXPECT_EQ(
      proposed_reason.load(std::memory_order_acquire),
      static_cast<std::uint8_t>(MetaAutomaticFailoverReason::kSessionMissing));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       UncertainRetryKeepsIdentityAcrossHealthAndPolicyChanges) {
  const SeedState seed = SeedCluster();
  std::promise<void> first_attempt_seen;
  std::future<void> first_attempt = first_attempt_seen.get_future();
  std::atomic<int> automatic_attempts{0};
  coordinator_->AddValidateHook([&](const MetaCommand& command,
                                    const MetaCommittedView&,
                                    const MetaObservationStore&, std::int64_t) {
    const auto* begin = std::get_if<BeginUncontrolledFailover>(&command);
    if (begin == nullptr ||
        begin->trigger_reason_ == MetaAutomaticFailoverReason::kManual) {
      return absl::OkStatus();
    }
    const int attempt =
        automatic_attempts.fetch_add(1, std::memory_order_acq_rel);
    if (attempt == 0) {
      first_attempt_seen.set_value();
      return absl::InternalError("injected uncertain append outcome");
    }
    return absl::OkStatus();
  });
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids, 0x90), /*grace_ms=*/0);
  StartEligibleGeneration(1);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ == MetaAutomaticFailoverState::kSuspect;
  }));

  now_steady_ms_.store(11'000, std::memory_order_release);
  ASSERT_EQ(first_attempt.wait_for(5s), std::future_status::ready);
  // The injected steady clock remains below the first 100 ms retry deadline,
  // so this gives the reconciler time to classify the returned Internal as an
  // uncertain append without permitting a second proposal yet.
  std::this_thread::sleep_for(100ms);
  ASSERT_EQ(automatic_attempts.load(std::memory_order_acquire), 1);

  PutPolicy longer_threshold;
  longer_threshold.request_id_ = Bytes<16>(0x0c);
  longer_threshold.policy_id_ =
      std::string(kAutomaticUncontrolledFailoverPolicyId);
  longer_threshold.version_ = 2;
  longer_threshold.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":10000})";
  ProposeAccepted(longer_threshold);
  ASSERT_TRUE(
      PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                            MetaNodeHealthObs{.storage_ready_ = true,
                                              .population_ready_ = true,
                                              .draining_ = false,
                                              .active_groups_ = 1},
                            /*causally_confirm_lease=*/true,
                            now_unix_ms_.load(std::memory_order_acquire),
                            now_steady_ms_.load(std::memory_order_acquire))
          .ok());

  now_steady_ms_.store(11'100, std::memory_order_release);
  ASSERT_TRUE(WaitUntil([&] {
    const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
    return group.has_value() && group->failover_transition_.has_value();
  }));

  const MetaStores stores = machine_->StoresSnapshot();
  const auto group = stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_EQ(group->failover_transition_->transition_id_, Bytes<16>(0x91));
  const auto automatic = stores.policy_.CurrentAutomaticUncontrolledFailover();
  ASSERT_TRUE(automatic.has_value());
  EXPECT_EQ(automatic->version_, 2u);
  EXPECT_EQ(automatic->suspect_after_ms_, 10000u);
  EXPECT_EQ(automatic_attempts.load(std::memory_order_acquire), 2);
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTimeoutTest,
       UncertainRetryBackoffStartsAfterProposalReturns) {
  SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids, 0x90), /*grace_ms=*/0);
  StartEligibleGeneration(1);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ == MetaAutomaticFailoverState::kSuspect;
  }));

  const std::uint64_t before = machine_->last_commit_index();
  BlockProposalExecutor();
  now_steady_ms_.store(11'000, std::memory_order_release);
  ASSERT_TRUE(WaitUntil(
      [&] { return generated_ids.load(std::memory_order_acquire) == 2; }));
  // Move the injected clock far past the time captured before Propose, then
  // let that proposal time out. A backoff measured from the stale pre-Propose
  // cut immediately queues a duplicate behind the barrier; a backoff measured
  // after Propose returns does not.
  now_steady_ms_.store(50'000, std::memory_order_release);
  std::this_thread::sleep_for(150ms);
  std::this_thread::sleep_for(150ms);
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);

  ReleaseProposalExecutor();
  ASSERT_TRUE(
      WaitUntil([&] { return machine_->last_commit_index() >= before + 1; }));
  ASSERT_TRUE(WaitUntil([&] {
    const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
    return group.has_value() && group->failover_transition_.has_value();
  }));
  const auto transition = machine_->StoresSnapshot()
                              .topology_.FindGroup("g1")
                              ->failover_transition_;
  ASSERT_TRUE(transition.has_value());
  EXPECT_EQ(transition->transition_id_, Bytes<16>(0x91));
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(machine_->last_commit_index(), before + 1);
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       AutomaticBeginAtomicallyPreemptsPristineControlledRequest) {
  const SeedState seed = SeedCluster(/*controlled_request=*/true);
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids, 0xa0), /*grace_ms=*/0);
  StartEligibleGeneration(1);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].state_ == MetaAutomaticFailoverState::kSuspect;
  }));
  now_steady_ms_.store(11'000, std::memory_order_release);

  ASSERT_TRUE(WaitUntil([&] {
    const auto group = machine_->StoresSnapshot().topology_.FindGroup("g1");
    return group.has_value() && group->failover_transition_.has_value();
  }));
  const MetaStores stores = machine_->StoresSnapshot();
  const auto group = stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  ASSERT_TRUE(group->failover_transition_.has_value());
  EXPECT_FALSE(group->failover_transition_->candidate_action_.has_value());
  const auto operation =
      stores.operation_.FindOperation(seed.controlled_operation_id_);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_, MetaOperationLifecycle::kAborted);
  EXPECT_EQ(operation->revision_, 1u);
  EXPECT_EQ(operation->terminal_result_,
            "preempted by automatic uncontrolled failover");
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       OrdinaryHeartbeatsCannotKeepMissingCausalConfirmationPendingForever) {
  const SeedState seed = SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids));
  StartEligibleGeneration(1);
  ASSERT_TRUE(PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                    MetaNodeHealthObs{.storage_ready_ = true,
                                                      .population_ready_ = true,
                                                      .draining_ = false,
                                                      .active_groups_ = 1},
                                    /*causally_confirm_lease=*/false,
                                    /*observed_at_unix_ms=*/1'000'000,
                                    /*observed_at_steady_ms=*/10'000,
                                    /*effective_lease_duration_ms=*/250)
                  .ok());

  ASSERT_TRUE(WaitForLeadershipWarmup(1));
  now_steady_ms_.store(10'100, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kBlocked &&
           status.blocker_ == MetaAutomaticFailoverBlocker::kCausalLeasePending;
  }));

  now_unix_ms_.store(1'000'250, std::memory_order_release);
  now_steady_ms_.store(10'250, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed, 2,
                                     std::nullopt, 250,
                                     /*observed_at_unix_ms=*/1'000'250,
                                     /*observed_at_steady_ms=*/10'250,
                                     /*draining=*/true)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ == MetaOwnerServiceabilityReason::kDraining;
  })) << "causal progress remains fresh at the exact lease boundary";

  now_unix_ms_.store(1'000'251, std::memory_order_release);
  now_steady_ms_.store(10'251, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed, 3,
                                     std::nullopt, 250,
                                     /*observed_at_unix_ms=*/1'000'251,
                                     /*observed_at_steady_ms=*/10'251)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ ==
               MetaOwnerServiceabilityReason::kHeartbeatExpired;
  }));
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       StaleProjectionCannotHideKnownExpiredOwnerLease) {
  const SeedState seed = SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids), /*grace_ms=*/100,
                    /*observation_ttl_ms=*/30'000);
  StartEligibleGeneration(1);
  ASSERT_TRUE(PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                    MetaNodeHealthObs{.storage_ready_ = true,
                                                      .population_ready_ = true,
                                                      .draining_ = false,
                                                      .active_groups_ = 1},
                                    /*causally_confirm_lease=*/true,
                                    /*observed_at_unix_ms=*/1'000'000,
                                    /*observed_at_steady_ms=*/10'000,
                                    /*effective_lease_duration_ms=*/5'000)
                  .ok());

  ASSERT_TRUE(WaitForLeadershipWarmup(1));
  now_steady_ms_.store(10'100, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kHealthy;
  }));

  // Advance the committed and runtime FDS without another Owner heartbeat.
  // The observation remains a valid old-projection cut whose own 5 s
  // effective lease, not the replacement Policy's 250 ms duration, bounds the
  // last authority Data could still hold.
  PutPolicy lease;
  lease.request_id_ = Bytes<16>(0x0c);
  lease.policy_id_ = std::string(kAuthorityLeasePolicyId);
  lease.version_ = 2;
  lease.content_ = R"({"kind":"authority-lease-v1","duration_ms":250})";
  ProposeAccepted(lease);
  const MetaCommittedView advanced = coordinator_->CommittedView();
  auto projected = MetaControlProjector::ProjectNode(advanced, seed.owner_);
  ASSERT_TRUE(projected.ok()) << projected.status();
  ASSERT_TRUE(detail::ApplyLeadershipValidityLimit(*projected, 250).ok());
  data_runtime_->PublishCurrent(
      seed.owner_, std::string(40, '1'), Bytes<16>(0x33), Bytes<20>(0x22),
      /*replication_flow_count=*/1, /*session_generation=*/1,
      /*leadership_generation=*/1,
      /*validated_committed_high_water=*/advanced.applied_index(),
      projected->full_state);

  now_steady_ms_.store(10'250, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kBlocked &&
           status.blocker_ == MetaAutomaticFailoverBlocker::kStaleOwnerAnchor;
  })) << "the replacement duration cannot shorten the old lease at its "
         "boundary";
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);

  now_steady_ms_.store(10'251, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kBlocked &&
           status.blocker_ == MetaAutomaticFailoverBlocker::kStaleOwnerAnchor;
  })) << "a Policy update cannot retroactively shorten an installed lease";

  now_steady_ms_.store(15'000, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kBlocked &&
           status.blocker_ == MetaAutomaticFailoverBlocker::kStaleOwnerAnchor;
  })) << "the old lease remains possibly active at received + D";

  now_steady_ms_.store(15'001, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ ==
               MetaOwnerServiceabilityReason::kHeartbeatExpired &&
           status.accumulated_suspect_ms_ == 0;
  })) << "known expiry must outrank the ordinary cross-source anchor tear";

  now_steady_ms_.store(16'000, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.accumulated_suspect_ms_ == 999;
  }));
  EXPECT_FALSE(machine_->StoresSnapshot()
                   .topology_.FindGroup("g1")
                   ->failover_transition_.has_value());

  now_steady_ms_.store(16'001, std::memory_order_release);
  ASSERT_TRUE(WaitUntil([&] {
    return machine_->StoresSnapshot()
        .topology_.FindGroup("g1")
        ->failover_transition_.has_value();
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       FirstPostHandoffGrantWaitsForCausalConfirmation) {
  const SeedState seed = SeedCluster();
  PutPolicy lease;
  lease.request_id_ = Bytes<16>(0x0c);
  lease.policy_id_ = std::string(kAuthorityLeasePolicyId);
  lease.version_ = 2;
  lease.content_ = R"({"kind":"authority-lease-v1","duration_ms":6000})";
  ProposeAccepted(lease);

  StartEligibleGeneration(1);
  ASSERT_TRUE(PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                    MetaNodeHealthObs{.storage_ready_ = true,
                                                      .population_ready_ = true,
                                                      .draining_ = false,
                                                      .active_groups_ = 1},
                                    /*causally_confirm_lease=*/false,
                                    /*observed_at_unix_ms=*/1'000'000,
                                    /*observed_at_steady_ms=*/10'000,
                                    /*effective_lease_duration_ms=*/6'000)
                  .ok());
  for (std::uint64_t sequence = 2; sequence <= 7; ++sequence) {
    const std::uint64_t elapsed_ms = (sequence - 1) * 2'000;
    ASSERT_TRUE(ContinueOwnerHeartbeat(
                    coordinator_->CommittedView(), seed, sequence, std::nullopt,
                    /*effective_lease_duration_ms=*/6'000,
                    /*observed_at_unix_ms=*/1'000'000 + elapsed_ms,
                    /*observed_at_steady_ms=*/10'000 + elapsed_ms)
                    .ok());
  }

  const auto runtime = data_runtime_->Snapshot();
  ASSERT_EQ(runtime.nodes_.size(), 1u);
  ASSERT_EQ(runtime.nodes_[0].groups_.size(), 1u);
  const auto session_id = Bytes<16>(0x33);
  const cluster::control::LeaseDenied handoff_pending{
      .reason = cluster::control::LeaseDenialReason::kAuthorityHandoffPending,
  };
  ASSERT_TRUE(RecordOwnerLeaseDecision(seed, 7, handoff_pending).ok());
  data_runtime_->RecordLeaseDecisionWritten(
      seed.owner_, session_id, /*heartbeat_sequence=*/7, handoff_pending,
      /*written_unix_ms=*/1'012'000);

  std::atomic<int> generated_ids{0};
  now_unix_ms_.store(1'012'000, std::memory_order_release);
  now_steady_ms_.store(22'000, std::memory_order_release);
  InstallReconciler(CountingIds(generated_ids), /*grace_ms=*/0,
                    /*observation_ttl_ms=*/6'000);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].blocker_ ==
               MetaAutomaticFailoverBlocker::kAuthorityHandoff;
  }));

  PutPolicy replacement_lease;
  replacement_lease.request_id_ = Bytes<16>(0x0d);
  replacement_lease.policy_id_ = std::string(kAuthorityLeasePolicyId);
  replacement_lease.version_ = 3;
  replacement_lease.content_ =
      R"({"kind":"authority-lease-v1","duration_ms":250})";
  ProposeAccepted(replacement_lease);
  ASSERT_TRUE(
      PublishOwnerFds(coordinator_->CommittedView(), seed, /*D=*/250).ok());
  now_unix_ms_.store(1'012'100, std::memory_order_release);
  now_steady_ms_.store(22'100, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                     /*heartbeat_sequence=*/8, std::nullopt,
                                     /*effective_lease_duration_ms=*/250,
                                     /*observed_at_unix_ms=*/1'012'100,
                                     /*observed_at_steady_ms=*/22'100)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.blocker_ == MetaAutomaticFailoverBlocker::kAuthorityHandoff;
  })) << "same-authority FDS replacement must retain the handoff marker";

  const auto replacement_runtime = data_runtime_->Snapshot();
  ASSERT_EQ(replacement_runtime.nodes_.size(), 1u);
  ASSERT_EQ(replacement_runtime.nodes_[0].groups_.size(), 1u);
  const MetaDataControlRuntimeNode& replacement_owner =
      replacement_runtime.nodes_[0];
  const MetaDataControlRuntimeGroup& replacement_group =
      replacement_owner.groups_[0];
  ASSERT_TRUE(RecordPossibleOwnerLease(seed, 8).ok());
  data_runtime_->RecordLeaseDecisionWritten(
      seed.owner_, session_id, /*heartbeat_sequence=*/8,
      cluster::control::LeaseGranted{
          .leadership_generation = replacement_owner.leadership_generation_,
          .data_boot_id = replacement_owner.boot_id_,
          .control_revision = replacement_owner.control_revision_,
          .group_id = replacement_group.group_id_,
          .assignment_id = replacement_group.assignment_id_,
          .group_term = replacement_group.group_term_,
          .granted_duration_ms = 250,
      },
      /*written_unix_ms=*/1'012'100);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.blocker_ != MetaAutomaticFailoverBlocker::kAuthorityHandoff;
  }));
  ASSERT_EQ(GroupStatus().state_, MetaAutomaticFailoverState::kBlocked);
  EXPECT_EQ(GroupStatus().blocker_,
            MetaAutomaticFailoverBlocker::kCausalLeasePending);

  // The legal 1 s SUSPECT threshold is longer than this replacement's 250 ms
  // lease. A healthy Owner must not be fenced before heartbeat 9 confirms
  // Grant 8.
  now_steady_ms_.store(22'200, std::memory_order_release);
  EXPECT_FALSE(WaitUntil(
      [&] {
        return machine_->StoresSnapshot()
            .topology_.FindGroup("g1")
            ->failover_transition_.has_value();
      },
      100ms));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);

  now_unix_ms_.store(1'012'300, std::memory_order_release);
  now_steady_ms_.store(22'300, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                     /*heartbeat_sequence=*/9,
                                     /*confirmed_ack_sequence=*/8,
                                     /*effective_lease_duration_ms=*/250,
                                     /*observed_at_unix_ms=*/1'012'300,
                                     /*observed_at_steady_ms=*/22'300)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kHealthy;
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       NewerHandoffMarkerOutranksStaleRuntimeDecisionButDoesNotLatch) {
  const SeedState seed = SeedCluster();
  StartEligibleGeneration(1);
  ASSERT_TRUE(PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                    MetaNodeHealthObs{.storage_ready_ = true,
                                                      .population_ready_ = true,
                                                      .draining_ = false,
                                                      .active_groups_ = 1},
                                    /*causally_confirm_lease=*/false,
                                    /*observed_at_unix_ms=*/1'000'000,
                                    /*observed_at_steady_ms=*/10'000,
                                    /*effective_lease_duration_ms=*/5'000)
                  .ok());
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                     /*heartbeat_sequence=*/2, std::nullopt,
                                     /*effective_lease_duration_ms=*/5'000,
                                     /*observed_at_unix_ms=*/1'000'010,
                                     /*observed_at_steady_ms=*/10'010)
                  .ok());

  const cluster::control::LeaseDenied node_not_ready{
      .reason = cluster::control::LeaseDenialReason::kNodeNotReady,
  };
  data_runtime_->RecordLeaseDecisionWritten(
      seed.owner_, Bytes<16>(0x33), /*heartbeat_sequence=*/1, node_not_ready,
      /*written_unix_ms=*/1'000'000);
  const cluster::control::LeaseDenied handoff_pending{
      .reason = cluster::control::LeaseDenialReason::kAuthorityHandoffPending,
  };
  ASSERT_TRUE(RecordOwnerLeaseDecision(seed, 2, handoff_pending).ok());

  std::atomic<int> generated_ids{0};
  now_unix_ms_.store(1'000'010, std::memory_order_release);
  now_steady_ms_.store(10'010, std::memory_order_release);
  InstallReconciler(CountingIds(generated_ids), /*grace_ms=*/0,
                    /*observation_ttl_ms=*/5'000);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].blocker_ ==
               MetaAutomaticFailoverBlocker::kAuthorityHandoff;
  })) << "a decision for heartbeat N-1 cannot supersede the pre-send marker "
         "for heartbeat N";

  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                     /*heartbeat_sequence=*/3, std::nullopt,
                                     /*effective_lease_duration_ms=*/5'000,
                                     /*observed_at_unix_ms=*/1'000'020,
                                     /*observed_at_steady_ms=*/10'020,
                                     /*draining=*/false,
                                     /*storage_ready=*/false)
                  .ok());
  ASSERT_TRUE(RecordOwnerLeaseDecision(seed, 3, node_not_ready).ok());
  ASSERT_TRUE(RecordOwnerLeaseDecisionWritten(seed, 3, node_not_ready).ok());
  data_runtime_->RecordLeaseDecisionWritten(
      seed.owner_, Bytes<16>(0x33), /*heartbeat_sequence=*/3, node_not_ready,
      /*written_unix_ms=*/1'000'020);

  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ ==
               MetaOwnerServiceabilityReason::kStorageUnready;
  })) << "a successfully written newer decision must not leave automatic "
         "failover permanently blocked by an old handoff marker";
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       PossibleOldGrantSurvivesConfirmationAndFdsReplacementUntilExpiry) {
  const SeedState seed = SeedCluster();
  PutPolicy lease;
  lease.request_id_ = Bytes<16>(0x0c);
  lease.policy_id_ = std::string(kAuthorityLeasePolicyId);
  lease.version_ = 2;
  lease.content_ = R"({"kind":"authority-lease-v1","duration_ms":6000})";
  ProposeAccepted(lease);

  StartEligibleGeneration(1);
  ASSERT_TRUE(PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                    MetaNodeHealthObs{.storage_ready_ = true,
                                                      .population_ready_ = true,
                                                      .draining_ = false,
                                                      .active_groups_ = 1},
                                    /*causally_confirm_lease=*/false,
                                    /*observed_at_unix_ms=*/1'000'000,
                                    /*observed_at_steady_ms=*/10'000,
                                    /*effective_lease_duration_ms=*/6'000)
                  .ok());
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                     /*heartbeat_sequence=*/7, std::nullopt,
                                     /*effective_lease_duration_ms=*/6'000,
                                     /*observed_at_unix_ms=*/1'012'000,
                                     /*observed_at_steady_ms=*/22'000)
                  .ok());

  const auto runtime = data_runtime_->Snapshot();
  ASSERT_EQ(runtime.nodes_.size(), 1u);
  ASSERT_EQ(runtime.nodes_[0].groups_.size(), 1u);
  const MetaDataControlRuntimeNode& owner = runtime.nodes_[0];
  const MetaDataControlRuntimeGroup& group = owner.groups_[0];
  ASSERT_TRUE(RecordPossibleOwnerLease(seed, 7).ok());
  data_runtime_->RecordLeaseDecisionWritten(
      seed.owner_, Bytes<16>(0x33), /*heartbeat_sequence=*/7,
      cluster::control::LeaseGranted{
          .leadership_generation = owner.leadership_generation_,
          .data_boot_id = owner.boot_id_,
          .control_revision = owner.control_revision_,
          .group_id = group.group_id_,
          .assignment_id = group.assignment_id_,
          .group_term = group.group_term_,
          .granted_duration_ms = 6'000,
      },
      /*written_unix_ms=*/1'012'000);

  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                     /*heartbeat_sequence=*/8,
                                     /*confirmed_ack_sequence=*/7,
                                     /*effective_lease_duration_ms=*/6'000,
                                     /*observed_at_unix_ms=*/1'012'100,
                                     /*observed_at_steady_ms=*/22'100)
                  .ok());

  PutPolicy replacement_lease;
  replacement_lease.request_id_ = Bytes<16>(0x0d);
  replacement_lease.policy_id_ = std::string(kAuthorityLeasePolicyId);
  replacement_lease.version_ = 3;
  replacement_lease.content_ =
      R"({"kind":"authority-lease-v1","duration_ms":250})";
  ProposeAccepted(replacement_lease);
  ASSERT_TRUE(
      PublishOwnerFds(coordinator_->CommittedView(), seed, /*D=*/250).ok());
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                     /*heartbeat_sequence=*/9, std::nullopt,
                                     /*effective_lease_duration_ms=*/250,
                                     /*observed_at_unix_ms=*/1'013'000,
                                     /*observed_at_steady_ms=*/23'000)
                  .ok());

  std::atomic<int> generated_ids{0};
  now_steady_ms_.store(28'000, std::memory_order_release);
  InstallReconciler(CountingIds(generated_ids), /*grace_ms=*/0,
                    /*observation_ttl_ms=*/30'000);
  ASSERT_TRUE(WaitUntil([&] {
    const auto snapshot = diagnostics_->Snapshot();
    return snapshot.statuses_.size() == 1 &&
           snapshot.statuses_[0].blocker_ ==
               MetaAutomaticFailoverBlocker::kCausalLeasePending;
  })) << "confirmation and a shorter replacement cannot erase an older "
         "possibly-live Grant at received + D";

  now_steady_ms_.store(28'001, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ ==
               MetaOwnerServiceabilityReason::kHeartbeatExpired;
  })) << "missing causal progress becomes exact failure only after D";

  now_steady_ms_.store(29'000, std::memory_order_release);
  EXPECT_FALSE(machine_->StoresSnapshot()
                   .topology_.FindGroup("g1")
                   ->failover_transition_.has_value());
  now_steady_ms_.store(29'001, std::memory_order_release);
  ASSERT_TRUE(WaitUntil([&] {
    return machine_->StoresSnapshot()
        .topology_.FindGroup("g1")
        ->failover_transition_.has_value();
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 2);
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       RepeatedCausalConfirmationCannotKeepOwnerServiceableForever) {
  const SeedState seed = SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids));
  StartEligibleGeneration(1);
  ASSERT_TRUE(PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                    MetaNodeHealthObs{.storage_ready_ = true,
                                                      .population_ready_ = true,
                                                      .draining_ = false,
                                                      .active_groups_ = 1},
                                    /*causally_confirm_lease=*/true,
                                    /*observed_at_unix_ms=*/1'000'000,
                                    /*observed_at_steady_ms=*/10'000,
                                    /*effective_lease_duration_ms=*/250)
                  .ok());
  ASSERT_TRUE(WaitForLeadershipWarmup(1));
  now_steady_ms_.store(10'100, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kHealthy;
  }));

  now_unix_ms_.store(1'000'250, std::memory_order_release);
  now_steady_ms_.store(10'250, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed, 3,
                                     /*confirmed_ack_sequence=*/1, 250,
                                     /*observed_at_unix_ms=*/1'000'250,
                                     /*observed_at_steady_ms=*/10'250,
                                     /*draining=*/true)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ == MetaOwnerServiceabilityReason::kDraining;
  })) << "repeated confirmation remains fresh at the exact lease boundary";

  now_unix_ms_.store(1'000'251, std::memory_order_release);
  now_steady_ms_.store(10'251, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed, 4,
                                     /*confirmed_ack_sequence=*/1, 250,
                                     /*observed_at_unix_ms=*/1'000'251,
                                     /*observed_at_steady_ms=*/10'251)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ ==
               MetaOwnerServiceabilityReason::kHeartbeatExpired;
  }));
}

TEST_F(MetaAutomaticFailoverReconcilerTest,
       BackwardWallClockDoesNotExpireFreshHeartbeatOrCausalProgress) {
  const SeedState seed = SeedCluster();
  std::atomic<int> generated_ids{0};
  InstallReconciler(CountingIds(generated_ids));
  StartEligibleGeneration(1);
  ASSERT_TRUE(PublishOwnerHeartbeat(coordinator_->CommittedView(), seed,
                                    MetaNodeHealthObs{.storage_ready_ = true,
                                                      .population_ready_ = true,
                                                      .draining_ = false,
                                                      .active_groups_ = 1},
                                    /*causally_confirm_lease=*/true,
                                    /*observed_at_unix_ms=*/1'000'000,
                                    /*observed_at_steady_ms=*/10'000,
                                    /*effective_lease_duration_ms=*/250)
                  .ok());

  ASSERT_TRUE(WaitForLeadershipWarmup(1));
  // Owner freshness shares the detector's monotonic clock. Moving the wall
  // clock backwards while steady time advances by 100 ms cannot age it.
  now_unix_ms_.store(999'000, std::memory_order_release);
  now_steady_ms_.store(10'100, std::memory_order_release);
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kHealthy;
  }));
  EXPECT_EQ(generated_ids.load(std::memory_order_acquire), 0);

  // Heartbeats continue while CLOCK_REALTIME is behind. A newly confirmed
  // lease received in this interval must not inherit the lower wall timestamp:
  // correcting the wall clock does not mean the lease aged by the size of the
  // correction.
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed, 3,
                                     /*confirmed_ack_sequence=*/2, 250,
                                     /*observed_at_unix_ms=*/999'000,
                                     /*observed_at_steady_ms=*/10'100)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kHealthy;
  }));

  now_unix_ms_.store(1'000'001, std::memory_order_release);
  now_steady_ms_.store(10'101, std::memory_order_release);
  EXPECT_FALSE(WaitUntil(
      [&] {
        return GroupStatus().state_ != MetaAutomaticFailoverState::kHealthy;
      },
      100ms))
      << "wall-clock recovery cannot age a just-confirmed lease";

  now_unix_ms_.store(1'000'250, std::memory_order_release);
  now_steady_ms_.store(10'350, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed, 4,
                                     /*confirmed_ack_sequence=*/2, 250,
                                     /*observed_at_unix_ms=*/1'000'250,
                                     /*observed_at_steady_ms=*/10'350,
                                     /*draining=*/true)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ == MetaOwnerServiceabilityReason::kDraining;
  })) << "causal progress remains fresh at received + effective lease TTL";

  now_unix_ms_.store(1'000'251, std::memory_order_release);
  now_steady_ms_.store(10'351, std::memory_order_release);
  ASSERT_TRUE(ContinueOwnerHeartbeat(coordinator_->CommittedView(), seed, 5,
                                     /*confirmed_ack_sequence=*/2, 250,
                                     /*observed_at_unix_ms=*/1'000'251,
                                     /*observed_at_steady_ms=*/10'351)
                  .ok());
  ASSERT_TRUE(WaitForGroupStatus([](const auto& status) {
    return status.state_ == MetaAutomaticFailoverState::kSuspect &&
           status.current_reason_ ==
               MetaOwnerServiceabilityReason::kHeartbeatExpired;
  })) << "causal progress expires only after the exact TTL boundary";
}

}  // namespace
}  // namespace lavik::meta
