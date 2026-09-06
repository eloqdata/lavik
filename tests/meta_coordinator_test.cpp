// Tests for the in-process coordinator API, which hides NuRaft from control
// sessions and operation reconcilers.
//
// Two slices:
//   1. Component tests (MetaCoordinatorComponentTest): a MetaCoordinator over a
//      bare MetaStateMachine + WAL v2 NuraftLogStore with NO raft_server,
//      driven by direct SM commit() calls. Covers the subscription contract
//      (atomic {view, cursor, subscription} triple, strict commit order, the
//      documented replay duplicate-index/dedup rule, bounded-queue backpressure
//      cancel, handle-destruction unsubscribe), the view-backed
//      MetaCommittedFacts adapter, and the no-server fast-fail of Propose.
//   2. Single-node raft_server integration (MetaCoordinatorServerTest): real
//      elections and commits over the real adapters (NuraftStateMgr + WAL v2 +
//      MetaStateMachine), mirroring meta_state_machine_test.cpp's
//      ThreadScheduler/NullRpcClientFactory harness. Covers Propose (actor
//      injection, verdict from the audit store), NOT_LEADER, the three
//      fail-safe gates with constructor-injected thresholds, ValidateProposal
//      hooks, uncertain-outcome semantics (timeout/cancel; reconcile via the
//      committed view), the required continuation scheduler, and the
//      RunAsLeader reconciler lifecycle (mock reconciler, idempotent
//      continuation across cancel/restart and across a full server restart
//      with WAL replay).
//
// All Propose results are driven through RunTaskSync. The server fixture
// explicitly injects an inline scheduler because it has no Celer worker;
// production has no inline fallback and schedules through MetaCelerBridge.

#include "meta/meta_coordinator.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "libnuraft/nuraft.hxx"
#include "meta/meta_commands.h"
#include "meta/meta_hash.h"
#include "meta/meta_observation_store.h"
#include "meta/meta_state_machine.h"
#include "meta/nuraft_log_store.h"
#include "meta/nuraft_state_mgr.h"

// Trusted test peer for the passkey-protected principal boundary. Tests use
// the same privileged construction path as ctl and authenticated sessions.
namespace keylane::meta {
class MetaCoordinatorTestPeer {
 public:
  static AuthenticatedPrincipal Make(std::string principal) {
    return AuthenticatedPrincipal(std::move(principal), MetaPrincipalPasskey{});
  }
};
}  // namespace keylane::meta

namespace {

using keylane::meta::AuthenticatedPrincipal;
using keylane::meta::BeginGroupTerm;
using keylane::meta::CreateGroup;
using keylane::meta::MetaApplyResult;
using keylane::meta::MetaAuditVerdict;
using keylane::meta::MetaCommand;
using keylane::meta::MetaCommitCallback;
using keylane::meta::MetaCommitEvent;
using keylane::meta::MetaCoordinator;
using keylane::meta::MetaCoordinatorOptions;
using keylane::meta::MetaCoordinatorTestPeer;
using keylane::meta::MetaLeaderContext;
using keylane::meta::MetaObservationIdentity;
using keylane::meta::MetaObservationStore;
using keylane::meta::MetaOperationId;
using keylane::meta::MetaReconciler;
using keylane::meta::MetaRequestId;
using keylane::meta::MetaStateMachine;
using keylane::meta::MetaStoresFacts;
using keylane::meta::MetaSubscriptionStart;
using keylane::meta::NuraftLogStore;
using keylane::meta::NuraftStateMgr;
using keylane::meta::RegisterNode;
using keylane::meta::SubmitOperation;
using keylane::meta::TransitionOperationPhase;

constexpr std::string_view kTestPrincipal = "keylane://operator/test-entry";

// ---------------------------------------------------------------------------
// Small shared helpers (same conventions as meta_state_machine_test.cpp)
// ---------------------------------------------------------------------------

std::filesystem::path MakeTestDir(const char* suite, const char* name) {
  const ::testing::TestInfo* info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::filesystem::path dir = std::filesystem::temp_directory_path() /
                              ("keylane_meta_test_" + std::string(suite) + "_" +
                               name + "_" + info->test_suite_name() + "_" +
                               info->name() + "_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  return dir;
}

void RemoveTestDir(const std::filesystem::path& dir) {
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

bool WaitFor(const std::function<bool()>& predicate,
             std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}

MetaRequestId MakeRequestId(std::uint8_t seed) {
  MetaRequestId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = static_cast<std::uint8_t>(seed + i);
  }
  return id;
}

MetaOperationId MakeOperationId(std::uint8_t seed) {
  MetaOperationId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = static_cast<std::uint8_t>(seed ^ static_cast<std::uint8_t>(i));
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

std::string MakeNodePrincipal(std::uint8_t seed) {
  return "keylane://node/" + MakeNodeId(seed);
}

// RegisterNode with the actor deliberately left EMPTY: Propose must inject it.
RegisterNode MakeRegister(std::uint8_t seed) {
  RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.principal_ = MakeNodePrincipal(seed);
  cmd.endpoints_ = {"10.0.0.1:7000"};
  cmd.capability_mask_ = 0x5;
  cmd.role_ = keylane::meta::MetaNodeRole::kReplica;
  return cmd;
}

// Drives one seam Task to completion from a plain thread. The completion
// callback gives the happens-before edge for reading the promise value; a
// suspended task destroyed on the timeout path detaches its NuRaft waiter
// (the coordinator's awaiter contract), so this cannot dangle.
template <typename T>
T RunTaskSync(celer::Task<T> task) {
  std::promise<void> done;
  std::future<void> signal = done.get_future();
  // completion_fn is a noexcept function pointer; the lambda must say so.
  task.SetCompletionCallback(
      &done, [](void* ctx, std::coroutine_handle<>) noexcept {
        static_cast<std::promise<void>*>(ctx)->set_value();
      });
  auto handle = std::move(task).ReleaseHandle();
  if (!handle) {
    ADD_FAILURE() << "task has no coroutine frame";
    return T{};
  }
  handle.resume();
  if (signal.wait_for(std::chrono::seconds(25)) != std::future_status::ready) {
    ADD_FAILURE() << "task did not complete in time";
    handle.destroy();
    return T{};
  }
  T result = std::move(handle.promise().value_);
  handle.destroy();
  return result;
}

AuthenticatedPrincipal TestPrincipal() {
  return MetaCoordinatorTestPeer::Make(std::string(kTestPrincipal));
}

// Thread-safe event recorder for subscription callbacks.
struct RecordedEvents {
  mutable std::mutex mu_;
  std::vector<MetaCommitEvent> events_;

  MetaCommitCallback Callback() {
    return [this](const MetaCommitEvent& event) {
      std::lock_guard<std::mutex> lock(mu_);
      events_.push_back(event);
    };
  }
  std::vector<MetaCommitEvent> Snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return events_;
  }
  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return events_.size();
  }
};

// ---------------------------------------------------------------------------
// Component tests: coordinator over a bare SM + WAL, no raft_server.
// ---------------------------------------------------------------------------

class MetaCoordinatorComponentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = MakeTestDir("w5", "component");
    auto machine = MetaStateMachine::Open(dir_);
    ASSERT_TRUE(machine.ok()) << machine.status();
    machine_ = std::move(*machine);
    auto wal = NuraftLogStore::Open(dir_ / "wal");
    ASSERT_TRUE(wal.ok()) << wal.status();
    wal_ = std::move(*wal);
  }

  void TearDown() override {
    coordinator_.reset();
    machine_.reset();
    wal_.reset();
    RemoveTestDir(dir_);
  }

  void MakeCoordinator(MetaCoordinatorOptions options = {}) {
    coordinator_ = std::make_unique<MetaCoordinator>(
        nuraft::ptr<nuraft::raft_server>(nullptr), *machine_, *wal_,
        observations_, std::move(options));
  }

  // Direct SM commit; fires the coordinator's commit-event sink inline.
  void Commit(std::uint64_t log_idx, const MetaCommand& cmd) {
    auto encoded = MetaStateMachine::EncodeCommand(cmd);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    ASSERT_NE(machine_->commit(log_idx, **encoded), nullptr);
  }

  std::filesystem::path dir_;
  std::unique_ptr<MetaStateMachine> machine_;
  std::unique_ptr<NuraftLogStore> wal_;
  MetaObservationStore observations_;
  std::unique_ptr<MetaCoordinator> coordinator_;
};

TEST_F(MetaCoordinatorComponentTest, SubscriptionTripleIsAtomicAndOrdered) {
  MakeCoordinator();
  for (std::uint8_t ii = 0; ii < 3; ++ii) {
    Commit(ii + 1, MakeRegister(static_cast<std::uint8_t>(0x10 + ii)));
  }

  RecordedEvents recorded;
  MetaSubscriptionStart start =
      coordinator_->SubscribeCommitted(recorded.Callback());
  // The atomic triple: the view reflects exactly the cursor — all three
  // committed commands are in the view, none are delivered as events.
  EXPECT_EQ(start.cursor_, 3u);
  EXPECT_EQ(start.view_.applied_index(), 3u);
  EXPECT_EQ(start.view_.identity().NodeCount(), 3u);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(recorded.size(), 0u);

  // Post-cursor commits stream in strict commit order with their verdicts.
  Commit(4, MakeRegister(0x14));
  Commit(5, MakeRegister(0x15));
  ASSERT_TRUE(
      WaitFor([&] { return recorded.size() == 2u; }, std::chrono::seconds(10)));
  const auto events = recorded.Snapshot();
  ASSERT_EQ(events.size(), 2u);
  EXPECT_EQ(events[0].log_index_, 4u);
  EXPECT_EQ(events[0].result_.verdict_, MetaAuditVerdict::kAccepted);
  EXPECT_EQ(events[1].log_index_, 5u);
  EXPECT_EQ(events[1].result_.verdict_, MetaAuditVerdict::kAccepted);
}

TEST_F(MetaCoordinatorComponentTest, SubscriptionReplayDuplicatesAndDedupRule) {
  // Contract: replay may deliver the same index twice; subscribers dedup by
  // index against their watermark (initially view.applied_index()).
  MakeCoordinator();
  RecordedEvents recorded;
  MetaSubscriptionStart start =
      coordinator_->SubscribeCommitted(recorded.Callback());
  EXPECT_EQ(start.cursor_, 0u);

  const MetaCommand first = MakeRegister(0x21);
  Commit(1, first);
  Commit(2, MakeRegister(0x22));
  Commit(2, MakeRegister(0x22));  // replay of the same (index, command)
  ASSERT_TRUE(
      WaitFor([&] { return recorded.size() == 3u; }, std::chrono::seconds(10)));
  const auto events = recorded.Snapshot();
  ASSERT_EQ(events.size(), 3u);
  EXPECT_EQ(events[0].log_index_, 1u);
  EXPECT_EQ(events[1].log_index_, 2u);
  EXPECT_EQ(events[2].log_index_, 2u);  // duplicate delivery, as documented

  // The documented subscriber-side dedup rule collapses the stream.
  std::uint64_t watermark = start.view_.applied_index();
  std::vector<std::uint64_t> effective;
  for (const MetaCommitEvent& event : events) {
    if (event.log_index_ <= watermark) continue;
    effective.push_back(event.log_index_);
    watermark = event.log_index_;
  }
  EXPECT_EQ(effective, (std::vector<std::uint64_t>{1u, 2u}));
  // And the replayed commit produced no extra state or audit record.
  EXPECT_EQ(coordinator_->CommittedView().identity().NodeCount(), 2u);
  EXPECT_EQ(coordinator_->CommittedView().audit().size(), 2u);
}

TEST_F(MetaCoordinatorComponentTest,
       SubscriptionOverflowCancelsWithResyncFlag) {
  MakeCoordinator();
  RecordedEvents recorded;
  std::mutex latch_mu;
  std::condition_variable latch_cv;
  bool release = false;
  MetaSubscriptionStart start = coordinator_->SubscribeCommitted(
      [&](const MetaCommitEvent& event) {
        recorded.Callback()(event);
        // Block the dispatcher thread so the bounded queue fills up.
        std::unique_lock<std::mutex> lock(latch_mu);
        latch_cv.wait(lock, [&] { return release; });
      },
      /*queue_capacity=*/2);

  for (std::uint64_t idx = 1; idx <= 4; ++idx) {
    Commit(idx, MakeRegister(static_cast<std::uint8_t>(0x30 + idx)));
  }
  ASSERT_TRUE(WaitFor([&] { return start.subscription_->cancelled(); },
                      std::chrono::seconds(10)));
  EXPECT_TRUE(start.subscription_->needs_resync());
  const std::size_t delivered_at_cancel = recorded.size();
  EXPECT_LE(delivered_at_cancel, 2u);

  // After the blocked callback returns, a cancelled subscription must never
  // fire again — even for commits that arrive after the cancellation.
  {
    std::lock_guard<std::mutex> lock(latch_mu);
    release = true;
  }
  latch_cv.notify_all();
  Commit(5, MakeRegister(0x35));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(recorded.size(), delivered_at_cancel);
}

TEST_F(MetaCoordinatorComponentTest,
       SubscriptionHandleDestroyUnsubscribesAndWaitsInFlight) {
  MakeCoordinator();
  RecordedEvents recorded;
  {
    MetaSubscriptionStart start =
        coordinator_->SubscribeCommitted(recorded.Callback());
    Commit(1, MakeRegister(0x41));
    ASSERT_TRUE(WaitFor([&] { return recorded.size() == 1u; },
                        std::chrono::seconds(10)));
    // Handle destruction unsubscribes.
  }
  Commit(2, MakeRegister(0x42));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(recorded.size(), 1u);

  // Destruction blocks until an in-flight callback returns (no UAF window).
  std::mutex latch_mu;
  std::condition_variable latch_cv;
  bool entered = false;
  bool release = false;
  {
    MetaSubscriptionStart start =
        coordinator_->SubscribeCommitted([&](const MetaCommitEvent&) {
          std::unique_lock<std::mutex> lock(latch_mu);
          entered = true;
          latch_cv.notify_all();
          latch_cv.wait(lock, [&] { return release; });
        });
    Commit(3, MakeRegister(0x43));
    ASSERT_TRUE(WaitFor(
        [&] {
          std::lock_guard<std::mutex> lock(latch_mu);
          return entered;
        },
        std::chrono::seconds(10)));
    std::atomic<bool> destroyed{false};
    std::thread destroyer([&] {
      start.subscription_.reset();
      destroyed.store(true);
    });
    // The destroyer must still be waiting: the callback has not returned.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(destroyed.load());
    {
      std::lock_guard<std::mutex> lock(latch_mu);
      release = true;
    }
    latch_cv.notify_all();
    destroyer.join();
    EXPECT_TRUE(destroyed.load());
  }
}

TEST_F(MetaCoordinatorComponentTest, ProposeWithoutServerFailsFast) {
  MakeCoordinator();
  auto result =
      RunTaskSync(coordinator_->Propose(MakeRegister(0x51), TestPrincipal()));
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kFailedPrecondition);
}

TEST_F(MetaCoordinatorComponentTest, CommittedViewFactsAnswerFromStores) {
  MakeCoordinator();
  Commit(1, MakeRegister(0x61));
  CreateGroup group;
  group.request_id_ = MakeRequestId(0x62);
  group.group_id_ = "g1";
  group.new_topology_epoch_ = 1;
  Commit(2, group);
  BeginGroupTerm term;
  term.request_id_ = MakeRequestId(0x63);
  term.group_id_ = "g1";
  term.expected_term_ = 0;
  term.new_term_ = 1;  // terms advance exactly one step (T-1 -> T)
  Commit(3, term);
  SubmitOperation submit;
  submit.request_id_ = MakeRequestId(0x64);
  submit.operation_id_ = MakeOperationId(0x64);
  submit.kind_ = "migration";
  submit.intent_ = "intent";
  submit.intent_hash_ = keylane::meta::MetaSha256(submit.intent_);
  Commit(4, submit);

  // The adapter the obs store's freshness queries run against.
  const auto view = coordinator_->CommittedView();
  MetaStoresFacts facts(view.stores());
  EXPECT_TRUE(facts.IsActiveNode(MakeNodeId(0x61)));
  EXPECT_FALSE(facts.IsActiveNode(MakeNodeId(0x62)));
  EXPECT_EQ(facts.CurrentGroupTerm("g1"), 1u);
  EXPECT_EQ(facts.CurrentGroupTerm("no-such-group"), 0u);
  EXPECT_EQ(facts.CurrentPopulationManifestId("g1"), 0u);
  EXPECT_TRUE(facts.OperationNonTerminal(MakeOperationId(0x64)));
  EXPECT_FALSE(facts.OperationNonTerminal(MakeOperationId(0x65)));
  EXPECT_FALSE(facts.HistoryBoundToOperation(MakeOperationId(0x64), 1));
}

// ---------------------------------------------------------------------------
// raft_server integration (single node, real adapters)
// ---------------------------------------------------------------------------

// Minimal real-time scheduler: one thread per delayed task (same pattern as
// meta_state_machine_test.cpp; cancelled tasks exit cheaply because
// delayed_task::execute() re-checks the cancellation flag).
class ThreadScheduler : public nuraft::delayed_task_scheduler {
 public:
  ~ThreadScheduler() override { Shutdown(); }

  void schedule(nuraft::ptr<nuraft::delayed_task>& task,
                nuraft::int32 milliseconds) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) return;
    threads_.emplace_back([this, task, milliseconds]() {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        stopped_cv_.wait_for(lock, std::chrono::milliseconds(milliseconds),
                             [this] { return stopped_; });
      }
      if (!stopped_) task->execute();
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
  }

 private:
  void cancel_impl(nuraft::ptr<nuraft::delayed_task>& task) override {}

  std::mutex mutex_;
  std::condition_variable stopped_cv_;
  bool stopped_ = false;
  std::vector<std::thread> threads_;
};

// Single-node clusters never open peer connections.
class NullRpcClientFactory : public nuraft::rpc_client_factory {
 public:
  nuraft::ptr<nuraft::rpc_client> create_client(
      const std::string& endpoint) override {
    return nullptr;
  }
};

struct ServerKnobs {
  int client_req_timeout_ms_ = 5000;
  int election_ms_low_ = 150;
  int election_ms_high_ = 300;
};

class MetaCoordinatorServerTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = MakeTestDir("w5", "server"); }
  void TearDown() override {
    StopServer();
    RemoveTestDir(dir_);
  }

  void OpenStorage() {
    auto mgr = NuraftStateMgr::Open(dir_, /*server_id=*/1, "127.0.0.1:9601");
    ASSERT_TRUE(mgr.ok()) << mgr.status();
    mgr_ = nuraft::ptr<NuraftStateMgr>(std::move(*mgr));
    auto machine = MetaStateMachine::Open(dir_);
    ASSERT_TRUE(machine.ok()) << machine.status();
    machine_ = nuraft::ptr<MetaStateMachine>(std::move(*machine));
  }

  // Launches the raft core WITHOUT waiting for leadership: tests wire the
  // coordinator and reconcilers first, so the organic BecomeLeader callback
  // (gated by wait_for_sm_catchup) is never missed.
  void LaunchServer(const ServerKnobs& knobs = {}) {
    scheduler_ = nuraft::cs_new<ThreadScheduler>();
    nuraft::raft_params params;
    params.with_election_timeout_lower(knobs.election_ms_low_);
    params.with_election_timeout_upper(knobs.election_ms_high_);
    params.with_hb_interval(50);
    params.with_snapshot_enabled(0);
    params.with_reserved_log_items(0);
    params.with_client_req_timeout(knobs.client_req_timeout_ms_);
    params.return_method_ = nuraft::raft_params::async_handler;
    // Production parity (meta_main): the BecomeLeader callback fires only
    // after the SM caught up — RunAsLeader reconcilers never see a partial
    // replay through CommittedView.
    params.wait_for_sm_catchup_on_becoming_leader_ = true;

    nuraft::context* ctx = new nuraft::context(
        mgr_, machine_, /*listener=*/nullptr, /*logger=*/nullptr,
        nuraft::cs_new<NullRpcClientFactory>(), scheduler_, params);
    nuraft::raft_server::init_options init_opts;
    init_opts.raft_callback_ = [this](nuraft::cb_func::Type type,
                                      nuraft::cb_func::Param*) {
      // NuRaft may invoke this while holding raft_server::lock_; the
      // coordinator's Become* methods are O(1) queue pushes by contract.
      std::lock_guard<std::mutex> lock(role_mu_);
      MetaCoordinator* target = forward_target_;
      if (target == nullptr) return nuraft::cb_func::Ok;
      if (type == nuraft::cb_func::BecomeLeader) {
        target->BecomeLeader();
      } else if (type == nuraft::cb_func::BecomeFollower) {
        target->BecomeFollower();
      }
      return nuraft::cb_func::Ok;
    };
    server_ = nuraft::cs_new<nuraft::raft_server>(ctx, init_opts);
    server_running_ = true;
  }

  void StartServer(const ServerKnobs& knobs = {}) {
    OpenStorage();
    LaunchServer(knobs);
  }

  void MakeCoordinator(MetaCoordinatorOptions options = {}) {
    nuraft::ptr<nuraft::log_store> store = mgr_->load_log_store();
    wal_ = static_cast<NuraftLogStore*>(store.get());
    // This fixture drives Tasks from an ordinary test thread and has no Celer
    // worker. Keep that exceptional execution policy explicit rather than
    // relying on a production-dangerous inline fallback in MetaCoordinator.
    if (!options.schedule_resume_) {
      options.schedule_resume_ = [](std::coroutine_handle<> continuation) {
        continuation.resume();
      };
    }
    coordinator_ = std::make_unique<MetaCoordinator>(
        server_, *machine_, *wal_, observations_, std::move(options));
    {
      std::lock_guard<std::mutex> lock(role_mu_);
      forward_target_ = coordinator_.get();
    }
  }

  void StopServer() {
    {
      std::lock_guard<std::mutex> lock(role_mu_);
      forward_target_ = nullptr;
    }
    // Teardown contract (meta_coordinator.h): the coordinator dies before the
    // state machine it references; raft shutdown resolves in-flight proposes
    // as CANCELLED, which the coordinator destructor drains.
    coordinator_.reset();
    if (server_ && server_running_) {
      server_->shutdown();
      server_running_ = false;
    }
    if (machine_) {
      machine_->WaitForSnapshotWriterIdle();
    }
    if (server_) {
      server_.reset();
    }
    if (scheduler_) {
      scheduler_->Shutdown();
      scheduler_.reset();
    }
    machine_.reset();
    mgr_.reset();
    wal_ = nullptr;
  }

  // Mid-test stop of only the raft core (the uncertain-outcome cancel path);
  // the rest of teardown stays with StopServer.
  void ShutdownRaft() {
    if (server_ && server_running_) {
      server_->shutdown();
      server_running_ = false;
    }
  }

  void WaitLeader() {
    ASSERT_TRUE(WaitFor([this] { return server_->is_leader(); },
                        std::chrono::seconds(15)));
  }

  absl::StatusOr<MetaApplyResult> ProposeSync(const MetaCommand& cmd) {
    return RunTaskSync(
        coordinator_->Propose(MetaCommand(cmd), TestPrincipal()));
  }

  std::filesystem::path dir_;
  nuraft::ptr<NuraftStateMgr> mgr_;
  nuraft::ptr<MetaStateMachine> machine_;
  NuraftLogStore* wal_ = nullptr;  // owned by mgr_
  nuraft::ptr<ThreadScheduler> scheduler_;
  nuraft::ptr<nuraft::raft_server> server_;
  bool server_running_ = false;
  MetaObservationStore observations_;
  std::unique_ptr<MetaCoordinator> coordinator_;
  std::mutex role_mu_;
  MetaCoordinator* forward_target_ = nullptr;
};

TEST_F(MetaCoordinatorServerTest, ProposeInjectsActorAndReturnsAuditVerdict) {
  StartServer();
  MakeCoordinator();
  WaitLeader();

  auto accepted = ProposeSync(MakeRegister(0x11));
  ASSERT_TRUE(accepted.ok()) << accepted.status();
  EXPECT_EQ(accepted->verdict_, MetaAuditVerdict::kAccepted);
  EXPECT_GE(accepted->log_index_, 1u);
  EXPECT_EQ(accepted->command_tag_,
            keylane::meta::MetaCommandTag::kRegisterNode);

  // The coordinator injected the actor and propose-time readable clock; the
  // committed command's audit record carries both.
  const auto stores = machine_->StoresSnapshot();
  ASSERT_TRUE(stores.identity_.FindNode(MakeNodeId(0x11)).has_value());
  const auto audit = stores.audit_.Find(accepted->log_index_);
  ASSERT_TRUE(audit.has_value());
  EXPECT_EQ(audit->record_.actor_principal_, kTestPrincipal);
  EXPECT_FALSE(audit->record_.readable_time_.empty());
  EXPECT_NE(audit->record_.readable_time_.find('T'), std::string::npos);

  // A domain rejection surfaces as the apply VERDICT (from the audit store),
  // not as a propose-level error: the index was committed and consumed.
  RegisterNode conflict = MakeRegister(0x12);
  conflict.principal_ = MakeNodePrincipal(0x11);  // principal already bound
  auto rejected = ProposeSync(conflict);
  ASSERT_TRUE(rejected.ok()) << rejected.status();
  EXPECT_EQ(rejected->verdict_, MetaAuditVerdict::kRejected);
  EXPECT_FALSE(rejected->detail_.empty());
  EXPECT_FALSE(machine_->StoresSnapshot()
                   .identity_.FindNode(MakeNodeId(0x12))
                   .has_value());
}

TEST_F(MetaCoordinatorServerTest,
       AttachedCoordinatorRequiresContinuationScheduler) {
  StartServer();
  nuraft::ptr<nuraft::log_store> store = mgr_->load_log_store();
  wal_ = static_cast<NuraftLogStore*>(store.get());

  EXPECT_THROW(
      {
        MetaCoordinator coordinator(server_, *machine_, *wal_, observations_,
                                    MetaCoordinatorOptions{});
      },
      std::invalid_argument);
}

TEST_F(MetaCoordinatorServerTest, ProposeNotLeaderThenLeader) {
  // A wide election window keeps the first self-election seconds away, so the
  // first Propose deterministically lands while the node is still a follower
  // (skip_initial_election_timeout_ is NOT an option: NuRaft reads it as
  // "wait to be contacted by a leader", which never comes for a one-node
  // group).
  StartServer({.election_ms_low_ = 3000, .election_ms_high_ = 6000});
  MakeCoordinator();
  auto not_leader = ProposeSync(MakeRegister(0x21));
  ASSERT_FALSE(not_leader.ok());
  EXPECT_EQ(not_leader.status().code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_NE(not_leader.status().message().find("not leader"), std::string::npos)
      << not_leader.status();

  WaitLeader();
  auto accepted = ProposeSync(MakeRegister(0x22));
  ASSERT_TRUE(accepted.ok()) << accepted.status();
  EXPECT_EQ(accepted->verdict_, MetaAuditVerdict::kAccepted);
}

TEST_F(MetaCoordinatorServerTest, FailSafeWalGate) {
  StartServer();
  WaitLeader();
  // Constructor-injected threshold: zero tolerated uncompacted WAL bytes. The
  // boot config entry alone already exceeds that, so the gate must trip.
  MetaCoordinatorOptions options;
  options.max_uncompacted_wal_bytes_ = 0;
  MakeCoordinator(options);
  auto gated = ProposeSync(MakeRegister(0x31));
  ASSERT_FALSE(gated.ok());
  EXPECT_EQ(gated.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_NE(gated.status().message().find("WAL"), std::string::npos)
      << gated.status();
  // Fail-safe means nothing was appended: no audit record, no state change.
  EXPECT_EQ(machine_->StoresSnapshot().audit_.size(), 0u);
}

TEST_F(MetaCoordinatorServerTest, FailSafeSnapshotFailureGate) {
  StartServer();
  WaitLeader();
  // Zero tolerated consecutive snapshot failures: the gate trips at the
  // current (zero) count. This proves the wiring; reaching a real failure
  // count would require faulting the snapshot writer's file IO.
  MetaCoordinatorOptions options;
  options.max_consecutive_snapshot_failures_ = 0;
  MakeCoordinator(options);
  auto gated = ProposeSync(MakeRegister(0x32));
  ASSERT_FALSE(gated.ok());
  EXPECT_EQ(gated.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_NE(gated.status().message().find("snapshot"), std::string::npos)
      << gated.status();
}

TEST_F(MetaCoordinatorServerTest, FailSafeAuditWindowGate) {
  StartServer({.client_req_timeout_ms_ = 25000});
  WaitLeader();

  // Select strict-export through the replicated command before filling the
  // window. The setup command is itself audited and counts toward capacity.
  const std::uint64_t before = server_->get_committed_log_idx();
  std::vector<nuraft::ptr<nuraft::buffer>> logs;
  logs.reserve(keylane::meta::kMaxMetaAuditWindowRecords);
  keylane::meta::SetAuditPolicy policy;
  policy.request_id_ = MakeRequestId(0x31);
  policy.policy_ = keylane::meta::MetaAuditPolicy::kStrictExport;
  policy.attestation_ = "test-strict-export";
  auto encoded_policy = MetaStateMachine::EncodeCommand(policy);
  ASSERT_TRUE(encoded_policy.ok()) << encoded_policy.status();
  logs.push_back(*encoded_policy);
  const MetaCommand filler = MakeRegister(0x33);
  for (std::uint32_t ii = 1; ii < keylane::meta::kMaxMetaAuditWindowRecords;
       ++ii) {
    auto encoded = MetaStateMachine::EncodeCommand(filler);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    logs.push_back(*encoded);
  }
  auto batch = server_->append_entries(logs);
  ASSERT_NE(batch, nullptr);
  ASSERT_TRUE(
      WaitFor([&] { return batch->has_result(); }, std::chrono::seconds(25)));
  ASSERT_EQ(batch->get_result_code(), nuraft::cmd_result_code::OK)
      << batch->get_result_str();
  ASSERT_EQ(machine_->StoresSnapshot().audit_.size(),
            keylane::meta::kMaxMetaAuditWindowRecords);
  ASSERT_GE(machine_->last_commit_index(),
            before + keylane::meta::kMaxMetaAuditWindowRecords);

  // The window is full: a privileged Propose must fail safe
  // (RESOURCE_EXHAUSTED) instead of appending past capacity (which the audit
  // store would treat as a fail-stop wiring bug).
  MetaCoordinatorOptions options;
  options.propose_timeout_ms_ = 300;
  MakeCoordinator(options);
  auto gated = ProposeSync(MakeRegister(0x34));
  ASSERT_FALSE(gated.ok());
  EXPECT_EQ(gated.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_NE(gated.status().message().find("audit"), std::string::npos)
      << gated.status();
  EXPECT_EQ(machine_->StoresSnapshot().audit_.size(),
            keylane::meta::kMaxMetaAuditWindowRecords);

  // A prune owns exclusive audit headroom until Raft resolves it, even when
  // the caller times out first. Without that reservation, a second prune can
  // observe the same full prefix, become a no-op after the first commits, and
  // make its own audit append overflow the fixed window.
  const auto full = machine_->StoresSnapshot();
  std::uint64_t first_audit_index = before + 1;
  while (first_audit_index <= machine_->last_commit_index() &&
         !full.audit_.Find(first_audit_index).has_value()) {
    ++first_audit_index;
  }
  ASSERT_LE(first_audit_index, machine_->last_commit_index());
  server_->pause_state_machine_execution(5000);
  keylane::meta::PruneAudit first_prune;
  first_prune.request_id_ = MakeRequestId(0x35);
  first_prune.through_log_index_ = first_audit_index;
  auto uncertain = ProposeSync(first_prune);
  ASSERT_FALSE(uncertain.ok());
  EXPECT_EQ(uncertain.status().code(), absl::StatusCode::kDeadlineExceeded);

  keylane::meta::PruneAudit overlapping = first_prune;
  overlapping.request_id_ = MakeRequestId(0x36);
  auto reserved = ProposeSync(overlapping);
  ASSERT_FALSE(reserved.ok());
  EXPECT_EQ(reserved.status().code(), absl::StatusCode::kResourceExhausted);

  server_->resume_state_machine_execution();
  ASSERT_TRUE(WaitFor(
      [&] {
        return machine_->StoresSnapshot().audit_.pruned_floor() >=
               first_audit_index;
      },
      std::chrono::seconds(10)));
  EXPECT_EQ(machine_->StoresSnapshot().audit_.size(),
            keylane::meta::kMaxMetaAuditWindowRecords);
}

TEST_F(MetaCoordinatorServerTest, ValidateHooksObserveAndRejectBeforeAppend) {
  StartServer();
  MakeCoordinator();
  WaitLeader();
  ASSERT_TRUE(ProposeSync(MakeRegister(0x41)).ok());

  struct HookObservation {
    std::string group_id_seen_;
    std::size_t node_count_seen_ = 0;
    const MetaObservationStore* obs_seen_ = nullptr;
  };
  std::vector<HookObservation> observations_log;
  coordinator_->AddValidateHook(
      [&](const MetaCommand& cmd, const keylane::meta::MetaCommittedView& view,
          const MetaObservationStore& obs) -> absl::Status {
        HookObservation record;
        if (const auto* create = std::get_if<CreateGroup>(&cmd)) {
          record.group_id_seen_ = create->group_id_;
        }
        record.node_count_seen_ = view.identity().NodeCount();
        record.obs_seen_ = &obs;
        observations_log.push_back(std::move(record));
        return absl::OkStatus();
      });
  coordinator_->AddValidateHook(
      [](const MetaCommand& cmd, const keylane::meta::MetaCommittedView&,
         const MetaObservationStore&) -> absl::Status {
        if (const auto* create = std::get_if<CreateGroup>(&cmd);
            create != nullptr && create->group_id_ == "forbidden") {
          return absl::Status(absl::StatusCode::kFailedPrecondition,
                              "hook policy: group id is forbidden");
        }
        return absl::OkStatus();
      });

  // Rejection: the hook's status surfaces verbatim and NOTHING was appended
  // (no audit record, no topology change) — hooks run before encode/append.
  CreateGroup forbidden;
  forbidden.request_id_ = MakeRequestId(0x42);
  forbidden.group_id_ = "forbidden";
  forbidden.new_topology_epoch_ = 1;
  const std::size_t audit_before = machine_->StoresSnapshot().audit_.size();
  auto rejected = ProposeSync(forbidden);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_NE(rejected.status().message().find("forbidden"), std::string::npos);
  EXPECT_EQ(machine_->StoresSnapshot().audit_.size(), audit_before);
  EXPECT_FALSE(machine_->StoresSnapshot().topology_.GroupExists("forbidden"));

  // Pass-through: both hooks ran in registration order against the same
  // atomic view and the coordinator's observation store.
  CreateGroup allowed;
  allowed.request_id_ = MakeRequestId(0x43);
  allowed.group_id_ = "g1";
  allowed.new_topology_epoch_ = 1;
  auto accepted = ProposeSync(allowed);
  ASSERT_TRUE(accepted.ok()) << accepted.status();
  EXPECT_EQ(accepted->verdict_, MetaAuditVerdict::kAccepted);
  ASSERT_EQ(observations_log.size(), 2u);
  EXPECT_EQ(observations_log[0].group_id_seen_, "forbidden");
  EXPECT_EQ(observations_log[1].group_id_seen_, "g1");
  EXPECT_EQ(observations_log[1].node_count_seen_, 1u);
  EXPECT_EQ(observations_log[1].obs_seen_, &observations_);
  EXPECT_TRUE(machine_->StoresSnapshot().topology_.GroupExists("g1"));
}

TEST_F(MetaCoordinatorServerTest,
       UncertainOutcomeTimeoutAndCancelAreReconcilable) {
  StartServer({.client_req_timeout_ms_ = 600});
  // The seam bounds the round trip itself (NuRaft's async_handler mode has
  // no client-side timeout): inject a short one.
  MetaCoordinatorOptions options;
  options.propose_timeout_ms_ = 300;
  MakeCoordinator(options);
  WaitLeader();
  ASSERT_TRUE(ProposeSync(MakeRegister(0x51)).ok());

  // Timeout-then-commit — the strong uncertain-outcome case. Pausing SM
  // execution (NuRaft's public pause_state_machine_execution) stalls the
  // apply without touching the append path: the entry lands in the WAL and
  // reaches (single-node) quorum, but the cmd_result cannot complete until
  // the SM runs it, so the client round times out first.
  server_->pause_state_machine_execution(5000);
  ASSERT_TRUE(server_->is_state_machine_execution_paused());
  const std::uint64_t slot_before_timeout = wal_->next_slot();
  auto timed_out = ProposeSync(MakeRegister(0x52));
  ASSERT_FALSE(timed_out.ok());
  EXPECT_EQ(timed_out.status().code(), absl::StatusCode::kDeadlineExceeded);
  EXPECT_NE(timed_out.status().message().find("uncertain"), std::string::npos)
      << timed_out.status();
  // The entry was genuinely in flight: appended to the WAL, never applied.
  EXPECT_GT(wal_->next_slot(), slot_before_timeout);
  EXPECT_FALSE(machine_->StoresSnapshot()
                   .identity_.FindNode(MakeNodeId(0x52))
                   .has_value());

  // "May still have committed", realized: after resume, the timed-out command
  // commits and its effect and audit record appear. A caller that treated the
  // timeout as failure and retried a NON-idempotent command would now have
  // double-applied it — the seam's commands are idempotent by design, and the
  // documented reconciliation is via CommittedView.
  server_->resume_state_machine_execution();
  ASSERT_TRUE(WaitFor(
      [this] {
        return machine_->StoresSnapshot()
            .identity_.FindNode(MakeNodeId(0x52))
            .has_value();
      },
      std::chrono::seconds(10)));
  EXPECT_EQ(machine_->StoresSnapshot().audit_.size(), 2u);

  // Cancel path: an in-flight propose resolves CANCELLED on shutdown — the
  // same uncertain-outcome class (the entry is durable in the WAL and may be
  // committed by a future leader). Wait for the WAL append first so the
  // propose is genuinely in flight when the server stops.
  server_->pause_state_machine_execution(5000);
  const std::uint64_t slot_before = wal_->next_slot();
  auto task = coordinator_->Propose(MakeRegister(0x53), TestPrincipal());
  std::promise<void> done;
  std::future<void> signal = done.get_future();
  task.SetCompletionCallback(
      &done, [](void* ctx, std::coroutine_handle<>) noexcept {
        static_cast<std::promise<void>*>(ctx)->set_value();
      });
  auto handle = std::move(task).ReleaseHandle();
  handle.resume();
  ASSERT_TRUE(WaitFor([&] { return wal_->next_slot() > slot_before; },
                      std::chrono::seconds(10)));
  ShutdownRaft();
  ASSERT_EQ(signal.wait_for(std::chrono::seconds(15)),
            std::future_status::ready);
  auto cancelled = std::move(handle.promise().value_);
  handle.destroy();
  ASSERT_FALSE(cancelled.ok());
  EXPECT_EQ(cancelled.status().code(), absl::StatusCode::kCancelled);
  EXPECT_NE(cancelled.status().message().find("uncertain"), std::string::npos)
      << cancelled.status();
}

// Mock operation reconciler: reconciles one fixed operation to
// Running through LeaderContext::Propose only. Idempotent by construction —
// every run first reconciles from the committed view, so a restart that finds
// the operation already Running proposes nothing.
class MockReconciler : public keylane::meta::MetaReconciler {
 public:
  explicit MockReconciler(MetaOperationId op_id) : op_id_(op_id) {}
  ~MockReconciler() override {
    if (thread_.joinable()) thread_.join();
  }

  void Start(MetaLeaderContext& ctx) override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      ++starts_;
    }
    thread_ = std::thread([this, &ctx] { ReconcileOnce(ctx); });
  }

  void CancelAndWait() override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      ++cancels_;
    }
    if (thread_.joinable()) thread_.join();
  }

  int starts() const {
    std::lock_guard<std::mutex> lock(mu_);
    return starts_;
  }
  int cancels() const {
    std::lock_guard<std::mutex> lock(mu_);
    return cancels_;
  }
  int reconcile_done() const {
    std::lock_guard<std::mutex> lock(mu_);
    return reconcile_done_;
  }
  int submit_attempts() const {
    std::lock_guard<std::mutex> lock(mu_);
    return submit_attempts_;
  }
  bool reached_running() const {
    std::lock_guard<std::mutex> lock(mu_);
    return reached_running_;
  }
  bool resumed_at_running() const {
    std::lock_guard<std::mutex> lock(mu_);
    return resumed_at_running_;
  }

 private:
  // reconcile_done_ must count EVERY run (including failed ones) — tests wait
  // on it as the "Start's work finished" signal.
  struct DoneGuard {
    ~DoneGuard() {
      std::lock_guard<std::mutex> lock(self->mu_);
      ++self->reconcile_done_;
    }
    MockReconciler* self;
  };

  void ReconcileOnce(MetaLeaderContext& ctx) {
    DoneGuard done_guard{this};
    auto view = ctx.CommittedView();
    auto record = view.operation().FindOperation(op_id_);
    if (!record.has_value()) {
      SubmitOperation submit;
      submit.request_id_ = MakeRequestId(0x71);
      submit.operation_id_ = op_id_;
      submit.kind_ = "migration";
      submit.intent_ = "move-slot-1";
      submit.intent_hash_ = keylane::meta::MetaSha256(submit.intent_);
      auto proposed = RunTaskSync(ctx.Propose(std::move(submit)));
      {
        std::lock_guard<std::mutex> lock(mu_);
        ++submit_attempts_;
      }
      if (!proposed.ok() || proposed->verdict_ != MetaAuditVerdict::kAccepted) {
        return;
      }
      record = ctx.CommittedView().operation().FindOperation(op_id_);
      if (!record.has_value()) return;
    }
    if (record->lifecycle_ ==
        keylane::meta::MetaOperationLifecycle::kSubmitted) {
      TransitionOperationPhase transition;
      transition.request_id_ = MakeRequestId(0x72);
      transition.operation_id_ = op_id_;
      transition.expected_revision_ = record->revision_;
      transition.kind_phase_blob_ = "running";
      auto proposed = RunTaskSync(ctx.Propose(std::move(transition)));
      if (proposed.ok() && proposed->verdict_ == MetaAuditVerdict::kAccepted) {
        std::lock_guard<std::mutex> lock(mu_);
        reached_running_ = true;
      }
    } else if (record->lifecycle_ ==
               keylane::meta::MetaOperationLifecycle::kRunning) {
      // The whole point: an already-Running operation is NOT resubmitted.
      std::lock_guard<std::mutex> lock(mu_);
      resumed_at_running_ = true;
    }
  }

  MetaOperationId op_id_;
  mutable std::mutex mu_;
  std::thread thread_;
  int starts_ = 0;
  int cancels_ = 0;
  int reconcile_done_ = 0;
  int submit_attempts_ = 0;
  bool reached_running_ = false;
  bool resumed_at_running_ = false;
};

TEST_F(MetaCoordinatorServerTest, ReconcilerStartCancelRestartIsIdempotent) {
  StartServer();
  MakeCoordinator();
  auto reconciler = std::make_shared<MockReconciler>(MakeOperationId(0x81));
  coordinator_->RunAsLeader(reconciler);
  WaitLeader();
  ASSERT_TRUE(WaitFor([&] { return reconciler->reconcile_done() >= 1; },
                      std::chrono::seconds(15)));
  EXPECT_EQ(reconciler->starts(), 1);
  EXPECT_EQ(reconciler->submit_attempts(), 1);
  EXPECT_TRUE(reconciler->reached_running());

  MetaObservationIdentity old_epoch_session;
  old_epoch_session.node_id_ = MakeNodeId(0x81);
  old_epoch_session.boot_incarnation_.fill(0x44);
  old_epoch_session.session_generation_ = 9;
  ASSERT_TRUE(observations_.AdoptSession(old_epoch_session, 1000).ok());
  ASSERT_TRUE(
      observations_.CurrentGeneration(old_epoch_session.node_id_).has_value());

  // BecomeFollower cancels and JOINS the reconciler; BecomeFollower is
  // driven directly here because a single-node raft group cannot demote
  // itself (NuRaft yield_leadership is a no-op for a one-node group).
  coordinator_->BecomeFollower();
  ASSERT_TRUE(WaitFor([&] { return reconciler->cancels() == 1; },
                      std::chrono::seconds(10)));
  EXPECT_TRUE(WaitFor(
      [&] {
        return !observations_.CurrentGeneration(old_epoch_session.node_id_)
                    .has_value();
      },
      std::chrono::seconds(10)));
  const std::size_t audit_at_cancel = machine_->StoresSnapshot().audit_.size();

  // Re-arm: the reconciler reconciles from the committed view, finds the
  // operation already Running, and proposes nothing.
  coordinator_->BecomeLeader();
  ASSERT_TRUE(WaitFor([&] { return reconciler->reconcile_done() >= 2; },
                      std::chrono::seconds(10)));
  EXPECT_EQ(reconciler->starts(), 2);
  EXPECT_EQ(reconciler->submit_attempts(), 1);
  EXPECT_TRUE(reconciler->resumed_at_running());
  EXPECT_EQ(machine_->StoresSnapshot().audit_.size(), audit_at_cancel);
  EXPECT_EQ(machine_->StoresSnapshot().operation_.LiveCount(), 1u);

  // Registering another reconciler while already leader starts it without a
  // new BecomeLeader edge.
  auto second = std::make_shared<MockReconciler>(MakeOperationId(0x82));
  coordinator_->RunAsLeader(second);
  ASSERT_TRUE(
      WaitFor([&] { return second->starts() == 1; }, std::chrono::seconds(10)));
}

TEST_F(MetaCoordinatorServerTest,
       ReconcilerSurvivesServerRestartWithoutDuplicateSubmit) {
  auto reconciler = std::make_shared<MockReconciler>(MakeOperationId(0x91));
  StartServer();
  MakeCoordinator();
  coordinator_->RunAsLeader(reconciler);
  WaitLeader();
  ASSERT_TRUE(WaitFor([&] { return reconciler->reconcile_done() >= 1; },
                      std::chrono::seconds(15)));
  ASSERT_TRUE(reconciler->reached_running());
  ASSERT_EQ(machine_->StoresSnapshot().audit_.size(), 2u);
  const std::uint64_t committed_before = machine_->last_commit_index();
  StopServer();  // coordinator dtor cancels+joins the reconciler
  EXPECT_EQ(reconciler->cancels(), 1);

  // Full restart on the same directory: the recovered SM replays the durable
  // WAL through commit() once the re-elected leader's current-term entry
  // reaches quorum. The same reconciler instance is re-registered.
  OpenStorage();
  LaunchServer();
  MakeCoordinator();
  coordinator_->RunAsLeader(reconciler);
  WaitLeader();
  ASSERT_TRUE(WaitFor([&] { return reconciler->reconcile_done() >= 2; },
                      std::chrono::seconds(15)));

  // Idempotent continuation: no duplicate submit; the replay rewrote the same
  // audit records keyed by the same log indexes (window did not grow).
  EXPECT_EQ(reconciler->submit_attempts(), 1);
  EXPECT_TRUE(reconciler->resumed_at_running());
  const auto stores = machine_->StoresSnapshot();
  EXPECT_EQ(stores.operation_.LiveCount(), 1u);
  EXPECT_EQ(stores.audit_.size(), 2u);
  EXPECT_TRUE(stores.audit_.VerifyChain());
  EXPECT_GE(machine_->last_commit_index(), committed_before);

  // The committed stream is alive on the new leader: a fresh subscription
  // gets the recovered state in its view and the next commit as an event.
  RecordedEvents recorded;
  MetaSubscriptionStart start =
      coordinator_->SubscribeCommitted(recorded.Callback());
  EXPECT_TRUE(
      start.view_.operation().FindOperation(MakeOperationId(0x91)).has_value());
  EXPECT_LE(start.cursor_, start.view_.applied_index());
  auto proposed = ProposeSync(MakeRegister(0x92));
  ASSERT_TRUE(proposed.ok()) << proposed.status();
  ASSERT_TRUE(
      WaitFor([&] { return recorded.size() == 1u; }, std::chrono::seconds(10)));
  const auto events = recorded.Snapshot();
  ASSERT_EQ(events.size(), 1u);
  EXPECT_EQ(events[0].log_index_, proposed->log_index_);
  EXPECT_EQ(events[0].result_.verdict_, MetaAuditVerdict::kAccepted);
}

}  // namespace
