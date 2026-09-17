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

// Tests for the in-process coordinator API, which hides NuRaft from control
// sessions and operation reconcilers.
//
// Two slices:
//   1. Component tests (MetaCoordinatorComponentTest): a MetaCoordinator over a
//      bare MetaStateMachine + WAL v1 NuraftLogStore with NO raft_server,
//      driven by direct SM commit() calls. Covers the subscription contract
//      (atomic {view, cursor, subscription} triple, strict commit order, the
//      documented replay duplicate-index/dedup rule, bounded-queue backpressure
//      cancel, handle-destruction unsubscribe), the view-backed
//      MetaCommittedFacts adapter, and the no-server fast-fail of Propose.
//   2. Single-node raft_server integration (MetaCoordinatorServerTest): real
//      elections and commits over the real adapters (NuraftStateMgr + WAL v1 +
//      MetaStateMachine), mirroring meta_state_machine_test.cpp's
//      ThreadScheduler/NullRpcClientFactory harness. Covers Propose (actor
//      injection, verdict from the audit store), NOT_LEADER, the three
//      fail-safe gates with constructor-injected thresholds, ValidateProposal
//      hooks, uncertain-outcome semantics (timeout/cancel; reconcile via the
//      committed view), the required continuation executor, and the
//      RunAsLeader reconciler lifecycle (mock reconciler, idempotent
//      continuation across cancel/restart and across a full server restart
//      with WAL replay).
//
// All Propose results are driven through RunTaskSync. The server fixture
// explicitly opts into inline resume because it has no Bycorf worker;
// production has no inline fallback and schedules through ForeignExecutor.

#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/coordinator.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/nuraft_log_store.h"
#include "lavik/meta/nuraft_state_mgr.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/state_machine.h"
#include "libnuraft/nuraft.hxx"
#include "spdlog/sinks/ostream_sink.h"
#include "spdlog/spdlog.h"
#include "support/test_data_path.h"

// Trusted test peer for the passkey-protected principal boundary. Tests use
// the same privileged construction path as ctl and authenticated sessions.
namespace lavik::meta {
class MetaCoordinatorTestPeer {
 public:
  static AuthenticatedPrincipal Make(std::string principal) {
    return AuthenticatedPrincipal(std::move(principal), MetaPrincipalPasskey{});
  }
};
}  // namespace lavik::meta

namespace {

using lavik::meta::AuthenticatedPrincipal;
using lavik::meta::BeginGroupTerm;
using lavik::meta::CreateGroup;
using lavik::meta::MetaApplyResult;
using lavik::meta::MetaAuditVerdict;
using lavik::meta::MetaCommand;
using lavik::meta::MetaCommitCallback;
using lavik::meta::MetaCommitEvent;
using lavik::meta::MetaCoordinator;
using lavik::meta::MetaCoordinatorOptions;
using lavik::meta::MetaCoordinatorTestPeer;
using lavik::meta::MetaLeaderContext;
using lavik::meta::MetaLeadershipRelay;
using lavik::meta::MetaObservationIdentity;
using lavik::meta::MetaObservationStore;
using lavik::meta::MetaOperationId;
using lavik::meta::MetaReconciler;
using lavik::meta::MetaRequestId;
using lavik::meta::MetaStateMachine;
using lavik::meta::MetaStoresFacts;
using lavik::meta::MetaSubscriptionStart;
using lavik::meta::NuraftLogStore;
using lavik::meta::NuraftStateMgr;
using lavik::meta::RegisterNode;
using lavik::meta::SubmitOperation;
using lavik::meta::TransitionOperationPhase;

constexpr std::string_view kTestPrincipal = "lavik://operator/test-entry";

// ---------------------------------------------------------------------------
// Small shared helpers (same conventions as meta_state_machine_test.cpp)
// ---------------------------------------------------------------------------

std::filesystem::path MakeTestDir(const char* suite, const char* name) {
  const ::testing::TestInfo* info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::filesystem::path dir = lavik::test::TestDataDirectory() /
                              ("lavik_meta_test_" + std::string(suite) + "_" +
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

class ScopedLogCapture {
 public:
  ScopedLogCapture()
      : original_(spdlog::default_logger()),
        sink_(std::make_shared<spdlog::sinks::ostream_sink_mt>(stream_)),
        logger_(
            std::make_shared<spdlog::logger>("meta-coordinator-test", sink_)) {
    logger_->set_level(spdlog::level::trace);
    logger_->set_pattern("%v");
    spdlog::set_default_logger(logger_);
  }

  ~ScopedLogCapture() {
    logger_->flush();
    spdlog::set_default_logger(std::move(original_));
  }

  std::string Take() {
    logger_->flush();
    std::string result = stream_.str();
    stream_.str("");
    stream_.clear();
    return result;
  }

 private:
  std::ostringstream stream_;
  std::shared_ptr<spdlog::logger> original_;
  std::shared_ptr<spdlog::sinks::ostream_sink_mt> sink_;
  std::shared_ptr<spdlog::logger> logger_;
};

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

template <std::size_t N>
std::array<std::uint8_t, N> MakeFixedId(std::uint8_t seed) {
  std::array<std::uint8_t, N> id{};
  id.fill(seed);
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
  return "lavik://node/" + MakeNodeId(seed);
}

// RegisterNode with the actor deliberately left EMPTY: Propose must inject it.
RegisterNode MakeRegister(std::uint8_t seed) {
  RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.principal_ = MakeNodePrincipal(seed);
  cmd.endpoints_ = {"10.0.0.1:7000"};

  cmd.role_ = lavik::meta::MetaNodeRole::kReplica;
  return cmd;
}

// Drives one seam Task to completion from a plain thread. The completion
// callback gives the happens-before edge for reading the promise value; a
// suspended task destroyed on the timeout path detaches its NuRaft waiter
// (the coordinator's awaiter contract), so this cannot dangle.
template <typename T>
T RunTaskSync(bycorf::Task<T> task) {
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

// Deliberately stalls the coordinator's leadership worker at both lifecycle
// calls. Production reconcilers return quickly from Start, but the barrier
// makes a rapid Leader -> Follower -> Leader sequence deterministic: all
// three callbacks arrive before the worker can infer a final role.
class BlockingLeadershipReconciler final : public MetaReconciler {
 public:
  void Start(MetaLeaderContext&) override {
    std::unique_lock<std::mutex> lock(mu_);
    ++starts_;
    events_.push_back("start-" + std::to_string(starts_));
    cv_.notify_all();
    if (starts_ == 1) {
      first_start_entered_ = true;
      cv_.notify_all();
      cv_.wait(lock, [&] { return release_first_start_; });
    }
  }

  void CancelAndWait() override {
    std::unique_lock<std::mutex> lock(mu_);
    ++cancels_;
    events_.push_back("cancel-enter");
    cancel_entered_ = true;
    cv_.notify_all();
    cv_.wait(lock, [&] { return release_cancel_; });
    events_.push_back("cancel-exit");
    cv_.notify_all();
  }

  bool WaitForFirstStart(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return first_start_entered_; });
  }

  bool WaitForCancel(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return cancel_entered_; });
  }

  bool WaitForSecondStart(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return starts_ >= 2; });
  }

  void ReleaseFirstStart() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      release_first_start_ = true;
    }
    cv_.notify_all();
  }

  void ReleaseCancel() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      release_cancel_ = true;
    }
    cv_.notify_all();
  }

  void ReleaseAll() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      release_first_start_ = true;
      release_cancel_ = true;
    }
    cv_.notify_all();
  }

  int starts() const {
    std::lock_guard<std::mutex> lock(mu_);
    return starts_;
  }

  std::vector<std::string> events() const {
    std::lock_guard<std::mutex> lock(mu_);
    return events_;
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  int starts_ = 0;
  int cancels_ = 0;
  bool first_start_entered_ = false;
  bool cancel_entered_ = false;
  bool release_first_start_ = false;
  bool release_cancel_ = false;
  std::vector<std::string> events_;
};

TEST_F(MetaCoordinatorComponentTest,
       RapidDemotionIsABarrierBeforeLeaderRestart) {
  MakeCoordinator();
  auto reconciler = std::make_shared<BlockingLeadershipReconciler>();
  coordinator_->RunAsLeader(reconciler);

  // Model a stopped Bycorf worker during assembly: all three callbacks reach
  // the process bridge before it can attach to the coordinator. The relay and
  // coordinator must retain the ordered edges, not merely the final role.
  MetaLeadershipRelay relay;
  relay.RecordLeaderEdge();
  relay.RecordFollowerEdge();
  relay.RecordLeaderEdge();
  relay.Attach(*coordinator_);
  const bool started = reconciler->WaitForFirstStart(std::chrono::seconds(2));
  EXPECT_TRUE(started);
  if (!started) {
    reconciler->ReleaseAll();
    return;
  }

  MetaObservationIdentity old_session;
  old_session.node_id_ = MakeNodeId(0x7a);
  old_session.boot_incarnation_.fill(0x7b);
  old_session.session_generation_ = 41;
  const absl::Status adopted = observations_.AdoptSession(old_session, 1000);
  EXPECT_TRUE(adopted.ok()) << adopted;
  if (!adopted.ok()) {
    reconciler->ReleaseAll();
    relay.DetachAndStop();
    return;
  }

  // The later edges are already queued while the leadership worker is stalled
  // in the first Start. A final-role bool would collapse them and leave old
  // authority live.
  reconciler->ReleaseFirstStart();

  const bool cancel_started =
      reconciler->WaitForCancel(std::chrono::milliseconds(500));
  EXPECT_TRUE(cancel_started);
  EXPECT_EQ(reconciler->starts(), 1)
      << "the next leader start must wait for CancelAndWait";
  reconciler->ReleaseCancel();

  const bool restarted =
      reconciler->WaitForSecondStart(std::chrono::seconds(2));
  reconciler->ReleaseAll();
  EXPECT_TRUE(restarted);
  EXPECT_EQ(reconciler->events(),
            (std::vector<std::string>{"start-1", "cancel-enter", "cancel-exit",
                                      "start-2"}));
  EXPECT_FALSE(
      observations_.CurrentGeneration(old_session.node_id_).has_value());
  relay.DetachAndStop();
}

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
  submit.intent_hash_ = lavik::meta::MetaSha256(submit.intent_);
  Commit(4, submit);

  // The adapter the obs store's freshness queries run against.
  const auto view = coordinator_->CommittedView();
  EXPECT_EQ(view.applied_index(), 4u);
  MetaStoresFacts facts(view.stores());
  EXPECT_TRUE(facts.IsActiveNode(MakeNodeId(0x61)));
  EXPECT_FALSE(facts.IsActiveNode(MakeNodeId(0x62)));
  EXPECT_EQ(facts.CurrentGroupTerm("g1"), 1u);
  EXPECT_EQ(facts.CurrentGroupTerm("no-such-group"), 0u);
  EXPECT_EQ(facts.CurrentPopulationManifestRevision("g1"), 0u);
  lavik::meta::MetaReplicationHistoryId unbound_history{};
  unbound_history.back() = 1;
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
  struct FailSafeControlledFailoverState {
    std::string owner_ = MakeNodeId(0x91);
    std::string candidate_ = MakeNodeId(0x92);
    lavik::meta::MetaAssignmentId owner_assignment_ = MakeFixedId<16>(0xa1);
    lavik::meta::MetaAssignmentId candidate_assignment_ = MakeFixedId<16>(0xa2);
    lavik::meta::MetaOperationId operation_id_ = MakeOperationId(0xa3);
    lavik::meta::MetaFailoverTransitionId transition_id_ =
        MakeFixedId<16>(0xa4);
    lavik::meta::MetaFailoverCandidateAction action_;
    std::uint64_t deadline_unix_ms_ = 2'000'000'000'000ULL;
    std::uint64_t transition_revision_ = 0;
  };

  void SetUp() override { dir_ = MakeTestDir("w5", "server"); }
  void TearDown() override {
    StopServer();
    RemoveTestDir(dir_);
  }

  void OpenStorage() {
    const lavik::meta::NuraftMemberConfig local{
        1, "127.0.0.1:9601", "lavik://meta/1", "127.0.0.1:9701",
        "127.0.0.1:9801"};
    lavik::meta::NuraftStateMgrOpenOptions options{.data_dir_ = dir_,
                                                   .local_member_ = local};
    if (!std::filesystem::exists(std::filesystem::path(dir_) /
                                 "cluster_config.dat")) {
      options.initial_cluster_ = std::vector{local};
    }
    auto mgr = NuraftStateMgr::Open(std::move(options));
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
    // This fixture drives Tasks from an ordinary test thread and has no Bycorf
    // worker. Keep that exceptional execution policy explicit rather than
    // relying on a production-dangerous inline fallback in MetaCoordinator.
    if (!options.foreign_executor_.valid()) {
      options.inline_resume_for_testing_ = true;
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
    // Teardown contract (coordinator.h): the coordinator dies before the
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

  void ProposeAccepted(const MetaCommand& command,
                       std::uint64_t* log_index = nullptr) {
    auto result = ProposeSync(command);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result->verdict_, MetaAuditVerdict::kAccepted) << result->detail_;
    if (log_index != nullptr) *log_index = result->log_index_;
  }

  void SeedControlledFailover(FailSafeControlledFailoverState& state,
                              bool begin_transition) {
    lavik::meta::ClusterCreateManifestV1 manifest;
    manifest.schema_version_ = 1;
    manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101",
                               "tcp://127.0.0.1:7301", "tcp://127.0.0.1:7201"}};
    manifest.data_nodes_ = {{state.owner_, "tcp://127.0.0.1:6379"}};
    manifest.groups_ = {{"g1", state.owner_, {}}};
    manifest.slot_ranges_ = {{0, 16383, "g1"}};

    SubmitOperation root;
    root.request_id_ = MakeRequestId(0x91);
    root.operation_id_ = MakeOperationId(0x91);
    root.kind_ = std::string(lavik::meta::kMetaClusterCreateOperationKind);
    auto root_intent =
        lavik::meta::EncodeClusterCreateRequest(manifest, root.operation_id_);
    ASSERT_TRUE(root_intent.ok()) << root_intent.status();
    root.intent_ = *root_intent;
    root.intent_hash_ = lavik::meta::MetaSha256(root.intent_);
    ProposeAccepted(root);

    lavik::meta::PutPolicy automatic;
    automatic.request_id_ = MakeRequestId(0x96);
    automatic.policy_id_ =
        std::string(lavik::meta::kAutomaticUncontrolledFailoverPolicyId);
    automatic.version_ = 1;
    automatic.content_ =
        R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
    ProposeAccepted(automatic);

    lavik::meta::PutPolicy policy;
    policy.request_id_ = MakeRequestId(0x9b);
    policy.policy_id_ = std::string(lavik::meta::kAuthorityLeasePolicyId);
    policy.version_ = 1;
    policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
    ProposeAccepted(policy);

    lavik::meta::CompleteOperation complete_root;
    complete_root.request_id_ = MakeRequestId(0x92);
    complete_root.operation_id_ = root.operation_id_;
    complete_root.expected_revision_ = 0;
    complete_root.result_ = "cluster-created";
    ProposeAccepted(complete_root);

    RegisterNode owner = MakeRegister(0x91);
    owner.node_id_ = state.owner_;
    owner.principal_ = "lavik://node/" + state.owner_;
    owner.role_ = lavik::meta::MetaNodeRole::kPrimary;
    ProposeAccepted(owner);

    RegisterNode candidate = MakeRegister(0x92);
    candidate.node_id_ = state.candidate_;
    candidate.principal_ = "lavik://node/" + state.candidate_;
    candidate.role_ = lavik::meta::MetaNodeRole::kReplica;
    ProposeAccepted(candidate);

    CreateGroup group;
    group.request_id_ = MakeRequestId(0x93);
    group.group_id_ = "g1";
    group.new_topology_epoch_ = 1;
    ProposeAccepted(group);

    lavik::meta::AssignNodeToGroup assign_owner;
    assign_owner.request_id_ = MakeRequestId(0x94);
    assign_owner.group_id_ = "g1";
    assign_owner.node_id_ = state.owner_;
    assign_owner.assignment_id_ = state.owner_assignment_;
    assign_owner.role_ = lavik::meta::MetaNodeRole::kPrimary;
    assign_owner.expected_revision_ = 1;
    assign_owner.new_topology_epoch_ = 2;
    ProposeAccepted(assign_owner);

    lavik::meta::AssignNodeToGroup assign_candidate;
    assign_candidate.request_id_ = MakeRequestId(0x95);
    assign_candidate.group_id_ = "g1";
    assign_candidate.node_id_ = state.candidate_;
    assign_candidate.assignment_id_ = state.candidate_assignment_;
    assign_candidate.role_ = lavik::meta::MetaNodeRole::kReplica;
    assign_candidate.expected_revision_ = 2;
    assign_candidate.new_topology_epoch_ = 3;
    ProposeAccepted(assign_candidate);

    BeginGroupTerm term;
    term.request_id_ = MakeRequestId(0x97);
    term.group_id_ = "g1";
    term.expected_term_ = 0;
    term.new_term_ = 1;
    ProposeAccepted(term);

    lavik::meta::ActivateAuthority activate;
    activate.request_id_ = MakeRequestId(0x98);
    activate.group_id_ = "g1";
    activate.expected_term_ = 1;
    activate.new_owner_ = state.owner_;
    activate.new_topology_epoch_ = 4;
    ProposeAccepted(activate);

    lavik::meta::FailoverOperationIntent intent;
    intent.group_id_ = "g1";
    intent.absolute_deadline_unix_ms_ = state.deadline_unix_ms_;
    auto encoded_intent = lavik::meta::EncodeFailoverOperationIntent(intent);
    ASSERT_TRUE(encoded_intent.ok()) << encoded_intent.status();
    SubmitOperation submit;
    submit.request_id_ = MakeRequestId(0x99);
    submit.operation_id_ = state.operation_id_;
    submit.kind_ = std::string(lavik::meta::kFailoverOperationKind);
    submit.intent_ = *encoded_intent;
    submit.intent_hash_ = lavik::meta::MetaSha256(submit.intent_);
    ProposeAccepted(submit);

    state.action_.action_id_ = MakeFixedId<16>(0xa5);
    state.action_.candidate_.node_id_ = state.candidate_;
    state.action_.candidate_.assignment_id_ = state.candidate_assignment_;
    state.action_.candidate_.boot_id_ =
        MakeFixedId<lavik::meta::kMetaBootIncarnationBytes>(0xa6);
    state.action_.domain_.source_group_term_ = 1;
    state.action_.domain_.source_node_id_ = state.owner_;
    state.action_.domain_.source_assignment_id_ = state.owner_assignment_;
    state.action_.domain_.source_boot_id_ =
        MakeFixedId<lavik::meta::kMetaBootIncarnationBytes>(0xa7);
    state.action_.domain_.source_history_id_ =
        MakeFixedId<lavik::meta::kMetaReplicationHistoryIdBytes>(0xa8);
    state.action_.domain_.flow_count_ = 2;

    if (!begin_transition) return;
    lavik::meta::BeginControlledFailover begin;
    begin.request_id_ = MakeRequestId(0x9a);
    begin.group_id_ = "g1";
    begin.transition_id_ = state.transition_id_;
    begin.target_term_ = 2;
    begin.candidate_action_ = state.action_;
    begin.operation_id_ = state.operation_id_;
    begin.expected_operation_revision_ = 0;
    begin.absolute_deadline_unix_ms_ = state.deadline_unix_ms_;
    begin.expected_owner_node_id_ = state.owner_;
    begin.expected_owner_assignment_id_ = state.owner_assignment_;
    begin.expected_membership_revision_ = 3;
    begin.expected_group_term_ = 1;
    begin.expected_population_manifest_revision_ = 0;
    begin.expected_population_manifest_digest_.fill(0);
    begin.expected_partition_replication_epoch_ = 0;
    ProposeAccepted(begin, &state.transition_revision_);
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
  EXPECT_EQ(accepted->command_tag_, lavik::meta::MetaCommandTag::kRegisterNode);

  // The coordinator injected the actor and propose-time readable clock; the
  // committed command's audit record carries both.
  const auto stores = machine_->StoresSnapshot();
  ASSERT_TRUE(stores.identity_.FindNode(MakeNodeId(0x11)).has_value());
  const auto audit = stores.audit_.Find(accepted->log_index_);
  ASSERT_TRUE(audit.has_value());
  EXPECT_EQ(audit->actor_principal_, kTestPrincipal);
  EXPECT_FALSE(audit->readable_time_.empty());
  EXPECT_NE(audit->readable_time_.find('T'), std::string::npos);

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
       AttachedCoordinatorRequiresContinuationExecutor) {
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
  MakeCoordinator();
  WaitLeader();

  lavik::meta::PutPopulationManifest put;
  put.request_id_ = MakeRequestId(0x30);
  put.entries_ = {{1, 1}};
  put.manifest_digest_ =
      lavik::meta::MetaPopulationManifestStore::CanonicalDigest(put.entries_);
  ASSERT_TRUE(ProposeSync(put).ok());
  const std::size_t audit_before_gate =
      machine_->StoresSnapshot().audit_.size();

  // Constructor-injected threshold: zero tolerated uncompacted WAL bytes. The
  // boot config entry alone already exceeds that, so the gate must trip.
  {
    std::lock_guard<std::mutex> lock(role_mu_);
    forward_target_ = nullptr;
  }
  coordinator_.reset();
  MetaCoordinatorOptions options;
  options.max_uncompacted_wal_bytes_ = 0;
  MakeCoordinator(options);
  auto gated = ProposeSync(MakeRegister(0x31));
  ASSERT_FALSE(gated.ok());
  EXPECT_EQ(gated.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_NE(gated.status().message().find("WAL"), std::string::npos)
      << gated.status();
  // Fail-safe means nothing was appended: no audit record, no state change.
  EXPECT_EQ(machine_->StoresSnapshot().audit_.size(), audit_before_gate);

  // A prune-shaped no-op is rejected before append; fresh request ids cannot
  // use idempotency to grow the WAL after the hard gate has fired.
  const std::uint64_t before_no_op = machine_->last_commit_index();
  lavik::meta::PrunePopulationManifest no_op;
  no_op.request_id_ = MakeRequestId(0x32);
  no_op.manifest_digest_.fill(0x44);
  auto ineffective = ProposeSync(no_op);
  ASSERT_FALSE(ineffective.ok());
  EXPECT_EQ(ineffective.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_no_op);

  lavik::meta::PrunePopulationManifest prune;
  prune.request_id_ = MakeRequestId(0x33);
  prune.manifest_digest_ = put.manifest_digest_;
  auto recovery = ProposeSync(prune);
  ASSERT_TRUE(recovery.ok()) << recovery.status();
  EXPECT_EQ(recovery->verdict_, MetaAuditVerdict::kAccepted);
  EXPECT_FALSE(machine_->StoresSnapshot().population_manifest_.Contains(
      put.manifest_digest_));
}

TEST_F(MetaCoordinatorServerTest, FailSafeSnapshotFailureGate) {
  StartServer();
  MakeCoordinator();
  WaitLeader();
  lavik::meta::PutPopulationManifest put;
  put.request_id_ = MakeRequestId(0x31);
  put.entries_ = {{2, 1}};
  put.manifest_digest_ =
      lavik::meta::MetaPopulationManifestStore::CanonicalDigest(put.entries_);
  ASSERT_TRUE(ProposeSync(put).ok());

  // Zero tolerated consecutive snapshot failures: the gate trips at the
  // current (zero) count. This proves the wiring; reaching a real failure
  // count would require faulting the snapshot writer's file IO.
  {
    std::lock_guard<std::mutex> lock(role_mu_);
    forward_target_ = nullptr;
  }
  coordinator_.reset();
  MetaCoordinatorOptions options;
  options.max_consecutive_snapshot_failures_ = 0;
  MakeCoordinator(options);
  auto gated = ProposeSync(MakeRegister(0x32));
  ASSERT_FALSE(gated.ok());
  EXPECT_EQ(gated.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_NE(gated.status().message().find("snapshot"), std::string::npos)
      << gated.status();

  const std::uint64_t before_no_op = machine_->last_commit_index();
  lavik::meta::PrunePopulationManifest no_op;
  no_op.request_id_ = MakeRequestId(0x33);
  no_op.manifest_digest_.fill(0x44);
  auto ineffective = ProposeSync(no_op);
  ASSERT_FALSE(ineffective.ok());
  EXPECT_EQ(ineffective.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_no_op);

  lavik::meta::PrunePopulationManifest prune;
  prune.request_id_ = MakeRequestId(0x34);
  prune.manifest_digest_ = put.manifest_digest_;
  auto recovery = ProposeSync(prune);
  ASSERT_TRUE(recovery.ok()) << recovery.status();
  EXPECT_EQ(recovery->verdict_, MetaAuditVerdict::kAccepted);
}

TEST_F(MetaCoordinatorServerTest,
       FailSafeGateAllowsEffectivePreBeginControlledAbortOnlyOnce) {
  StartServer();
  MakeCoordinator();
  WaitLeader();

  FailSafeControlledFailoverState failover;
  SeedControlledFailover(failover, /*begin_transition=*/false);
  const auto before = machine_->StoresSnapshot();
  const auto before_group = before.topology_.FindGroup("g1");
  const auto before_grant = before.topology_.AuthorityFor("g1");
  ASSERT_TRUE(before_group.has_value());
  ASSERT_TRUE(before_grant.has_value());

  {
    std::lock_guard<std::mutex> lock(role_mu_);
    forward_target_ = nullptr;
  }
  coordinator_.reset();
  MetaCoordinatorOptions options;
  options.max_consecutive_snapshot_failures_ = 0;
  MakeCoordinator(options);

  lavik::meta::AbortControlledFailover abort;
  abort.request_id_ = MakeRequestId(0xb1);
  abort.operation_id_ = failover.operation_id_;
  abort.expected_operation_revision_ = 0;
  abort.group_id_ = "g1";
  abort.reason_ = "no eligible candidate";
  auto aborted = ProposeSync(abort);
  ASSERT_TRUE(aborted.ok()) << aborted.status();
  EXPECT_EQ(aborted->verdict_, MetaAuditVerdict::kAccepted);

  const auto after = machine_->StoresSnapshot();
  const auto operation = after.operation_.FindOperation(failover.operation_id_);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_,
            lavik::meta::MetaOperationLifecycle::kAborted);
  EXPECT_EQ(operation->revision_, 1u);
  EXPECT_EQ(operation->terminal_result_, abort.reason_);
  EXPECT_FALSE(operation->data_loss_possible_);
  EXPECT_EQ(after.topology_.FindGroup("g1"), before_group);
  const auto after_grant = after.topology_.AuthorityFor("g1");
  ASSERT_TRUE(after_grant.has_value());
  EXPECT_EQ(*after_grant, *before_grant);

  // A fresh request id cannot turn the idempotent post-state into another WAL
  // recovery record: fail-safe recovery must make forward progress each time.
  abort.request_id_ = MakeRequestId(0xb2);
  const std::uint64_t before_retry = machine_->last_commit_index();
  auto retried = ProposeSync(abort);
  ASSERT_FALSE(retried.ok());
  EXPECT_EQ(retried.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_retry);
}

TEST_F(MetaCoordinatorServerTest,
       FailSafeGateAllowsExactPostBeginControlledAbortButNotCutover) {
  StartServer();
  MakeCoordinator();
  WaitLeader();

  FailSafeControlledFailoverState failover;
  SeedControlledFailover(failover, /*begin_transition=*/true);
  const auto before = machine_->StoresSnapshot();
  const auto before_group = before.topology_.FindGroup("g1");
  const auto before_grant = before.topology_.AuthorityFor("g1");
  ASSERT_TRUE(before_group.has_value());
  ASSERT_TRUE(before_group->failover_transition_.has_value());
  ASSERT_TRUE(before_grant.has_value());

  {
    std::lock_guard<std::mutex> lock(role_mu_);
    forward_target_ = nullptr;
  }
  coordinator_.reset();
  MetaCoordinatorOptions options;
  options.max_consecutive_snapshot_failures_ = 0;
  MakeCoordinator(options);

  const lavik::meta::MetaFailoverTransitionRef transition{
      failover.transition_id_, failover.transition_revision_};
  lavik::meta::AbortControlledFailover stale_abort;
  stale_abort.request_id_ = MakeRequestId(0xb3);
  stale_abort.operation_id_ = failover.operation_id_;
  stale_abort.expected_operation_revision_ = 0;
  stale_abort.group_id_ = "g1";
  stale_abort.expected_transition_ = lavik::meta::MetaFailoverTransitionRef{
      failover.transition_id_, failover.transition_revision_ - 1};
  stale_abort.reason_ = "stale transition must not be cleared";
  const std::uint64_t before_stale_abort = machine_->last_commit_index();
  auto stale_aborted = ProposeSync(stale_abort);
  ASSERT_FALSE(stale_aborted.ok());
  EXPECT_EQ(stale_aborted.status().code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_stale_abort);

  lavik::meta::DegradeControlledFailover degrade;
  degrade.request_id_ = MakeRequestId(0xb4);
  degrade.operation_id_ = failover.operation_id_;
  degrade.expected_operation_revision_ = 0;
  degrade.group_id_ = "g1";
  degrade.expected_transition_ = transition;
  degrade.expected_candidate_action_ = failover.action_;
  degrade.reason_ = "source unavailable";
  const std::uint64_t before_degrade = machine_->last_commit_index();
  auto degraded = ProposeSync(degrade);
  ASSERT_FALSE(degraded.ok());
  EXPECT_EQ(degraded.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_degrade);

  lavik::meta::CommitControlledFailover commit;
  commit.request_id_ = MakeRequestId(0xb5);
  commit.operation_id_ = failover.operation_id_;
  commit.expected_operation_revision_ = 0;
  commit.group_id_ = "g1";
  commit.expected_transition_ = transition;
  commit.action_id_ = failover.action_.action_id_;
  commit.authorized_revision_ = failover.transition_revision_;
  commit.expected_candidate_ = failover.action_.candidate_;
  commit.expected_owner_node_id_ = failover.owner_;
  commit.expected_owner_assignment_id_ = failover.owner_assignment_;
  commit.expected_membership_revision_ = 3;
  commit.expected_group_term_ = 1;
  commit.expected_population_manifest_revision_ = 0;
  commit.expected_population_manifest_digest_.fill(0);
  commit.expected_partition_replication_epoch_ = 0;
  commit.new_topology_epoch_ = 5;
  const std::uint64_t before_commit = machine_->last_commit_index();
  auto committed = ProposeSync(commit);
  ASSERT_FALSE(committed.ok());
  EXPECT_EQ(committed.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_commit);

  lavik::meta::AbortControlledFailover abort;
  abort.request_id_ = MakeRequestId(0xb6);
  abort.operation_id_ = failover.operation_id_;
  abort.expected_operation_revision_ = 0;
  abort.group_id_ = "g1";
  abort.expected_transition_ = transition;
  abort.reason_ = "fail-safe cancelled controlled failover";
  ScopedLogCapture logs;
  auto aborted = ProposeSync(abort);
  ASSERT_TRUE(aborted.ok()) << aborted.status();
  EXPECT_EQ(aborted->verdict_, MetaAuditVerdict::kAccepted);
  const std::string first_abort_log = logs.Take();
  EXPECT_NE(first_abort_log.find("failover event=abort"), std::string::npos);
  EXPECT_NE(first_abort_log.find("action=a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5"),
            std::string::npos);

  const auto after = machine_->StoresSnapshot();
  const auto operation = after.operation_.FindOperation(failover.operation_id_);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_,
            lavik::meta::MetaOperationLifecycle::kAborted);
  EXPECT_EQ(operation->revision_, 1u);
  EXPECT_EQ(operation->terminal_result_, abort.reason_);
  EXPECT_FALSE(operation->data_loss_possible_);
  auto expected_group = *before_group;
  expected_group.failover_transition_.reset();
  EXPECT_EQ(after.topology_.FindGroup("g1"), expected_group);
  const auto after_grant = after.topology_.AuthorityFor("g1");
  ASSERT_TRUE(after_grant.has_value());
  EXPECT_EQ(*after_grant, *before_grant);

  const auto abort_audit = after.audit_.Find(aborted->log_index_);
  ASSERT_TRUE(abort_audit.has_value());
  abort.actor_.principal_ = abort_audit->actor_principal_;
  abort.actor_.readable_time_ = abort_audit->readable_time_;
  auto encoded_abort = MetaStateMachine::EncodeCommand(MetaCommand{abort});
  ASSERT_TRUE(encoded_abort.ok()) << encoded_abort.status();
  machine_->commit(aborted->log_index_, **encoded_abort);
  EXPECT_EQ(logs.Take().find("failover event=abort"), std::string::npos)
      << "exact replay cannot relabel the cleared action as none";

  abort.request_id_ = MakeRequestId(0xb7);
  const std::uint64_t before_retry = machine_->last_commit_index();
  auto retried = ProposeSync(abort);
  ASSERT_FALSE(retried.ok());
  EXPECT_EQ(retried.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_retry);
}

TEST_F(MetaCoordinatorServerTest,
       FailSafeSnapshotGateAllowsTerminalizeArchivePruneRecovery) {
  StartServer();
  MakeCoordinator();
  WaitLeader();

  SubmitOperation complete_target;
  complete_target.request_id_ = MakeRequestId(0x35);
  complete_target.operation_id_ = MakeOperationId(0x35);
  complete_target.kind_ = "migration";
  complete_target.intent_ = "complete-then-archive";
  complete_target.intent_hash_ =
      lavik::meta::MetaSha256(complete_target.intent_);
  auto submitted_complete = ProposeSync(complete_target);
  ASSERT_TRUE(submitted_complete.ok()) << submitted_complete.status();

  SubmitOperation abort_target = complete_target;
  abort_target.request_id_ = MakeRequestId(0x36);
  abort_target.operation_id_ = MakeOperationId(0x36);
  abort_target.intent_ = "abort-then-archive";
  abort_target.intent_hash_ = lavik::meta::MetaSha256(abort_target.intent_);
  auto submitted_abort = ProposeSync(abort_target);
  ASSERT_TRUE(submitted_abort.ok()) << submitted_abort.status();

  // Recreate only the coordinator with an already-tripped fail-safe gate;
  // the same live Raft server and state machine retain the terminal operation.
  {
    std::lock_guard<std::mutex> lock(role_mu_);
    forward_target_ = nullptr;
  }
  coordinator_.reset();
  MetaCoordinatorOptions options;
  options.max_consecutive_snapshot_failures_ = 0;
  MakeCoordinator(options);

  // Emergency terminalization may not introduce a new variable-length result
  // while snapshot recovery is already gated.
  lavik::meta::CompleteOperation growing_complete;
  growing_complete.request_id_ = MakeRequestId(0x37);
  growing_complete.operation_id_ = complete_target.operation_id_;
  growing_complete.expected_revision_ = 0;
  growing_complete.result_ = "not-admitted-during-fail-safe";
  const std::uint64_t before_growing = machine_->last_commit_index();
  auto growing = ProposeSync(growing_complete);
  ASSERT_FALSE(growing.ok());
  EXPECT_EQ(growing.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_growing);

  lavik::meta::CompleteOperation complete = growing_complete;
  complete.request_id_ = MakeRequestId(0x38);
  complete.result_.clear();
  auto completed = ProposeSync(complete);
  ASSERT_TRUE(completed.ok()) << completed.status();

  lavik::meta::AbortOperation abort;
  abort.request_id_ = MakeRequestId(0x39);
  abort.operation_id_ = abort_target.operation_id_;
  abort.expected_revision_ = 0;
  auto aborted = ProposeSync(abort);
  ASSERT_TRUE(aborted.ok()) << aborted.status();

  lavik::meta::ArchiveOperations archive;
  archive.request_id_ = MakeRequestId(0x3a);
  archive.operation_seqs_ = {submitted_complete->log_index_,
                             submitted_abort->log_index_};
  auto recovery = ProposeSync(archive);
  ASSERT_TRUE(recovery.ok()) << recovery.status();
  EXPECT_EQ(recovery->verdict_, MetaAuditVerdict::kAccepted);
  EXPECT_TRUE(coordinator_->CommittedView()
                  .operation()
                  .FindArchived(complete_target.operation_id_)
                  .has_value());

  lavik::meta::PruneOperationArchive prune;
  prune.request_id_ = MakeRequestId(0x3b);
  prune.operation_seqs_ = archive.operation_seqs_;
  auto pruned = ProposeSync(prune);
  ASSERT_TRUE(pruned.ok()) << pruned.status();
  const auto view = coordinator_->CommittedView();
  EXPECT_FALSE(view.operation().OperationKnown(complete_target.operation_id_));
  EXPECT_FALSE(view.operation().OperationKnown(abort_target.operation_id_));

  // The same variant with a fresh id is now a no-op and must not reach Raft.
  prune.request_id_ = MakeRequestId(0x3c);
  const std::uint64_t before_repeat = machine_->last_commit_index();
  auto repeated = ProposeSync(prune);
  ASSERT_FALSE(repeated.ok());
  EXPECT_EQ(repeated.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(machine_->last_commit_index(), before_repeat);
}

TEST_F(MetaCoordinatorServerTest,
       FailSafeRecoveryReservationFollowsUncertainRaftOutcome) {
  StartServer();
  MakeCoordinator();
  WaitLeader();

  SubmitOperation submit;
  submit.request_id_ = MakeRequestId(0x3d);
  submit.operation_id_ = MakeOperationId(0x3d);
  submit.kind_ = "migration";
  submit.intent_ = "uncertain-archive";
  submit.intent_hash_ = lavik::meta::MetaSha256(submit.intent_);
  auto submitted = ProposeSync(submit);
  ASSERT_TRUE(submitted.ok()) << submitted.status();
  lavik::meta::AbortOperation abort;
  abort.request_id_ = MakeRequestId(0x3e);
  abort.operation_id_ = submit.operation_id_;
  abort.expected_revision_ = 0;
  ASSERT_TRUE(ProposeSync(abort).ok());

  {
    std::lock_guard<std::mutex> lock(role_mu_);
    forward_target_ = nullptr;
  }
  coordinator_.reset();
  MetaCoordinatorOptions options;
  options.max_consecutive_snapshot_failures_ = 0;
  options.propose_timeout_ms_ = 250;
  MakeCoordinator(options);

  server_->pause_state_machine_execution(5000);
  lavik::meta::ArchiveOperations archive;
  archive.request_id_ = MakeRequestId(0x3f);
  archive.operation_seqs_ = {submitted->log_index_};
  auto uncertain = ProposeSync(archive);
  ASSERT_FALSE(uncertain.ok());
  EXPECT_EQ(uncertain.status().code(), absl::StatusCode::kDeadlineExceeded);

  archive.request_id_ = MakeRequestId(0x40);
  auto overlapping = ProposeSync(archive);
  ASSERT_FALSE(overlapping.ok());
  EXPECT_EQ(overlapping.status().code(), absl::StatusCode::kResourceExhausted);

  server_->resume_state_machine_execution();
  ASSERT_TRUE(WaitFor(
      [&] {
        return machine_->StoresSnapshot()
            .operation_.FindArchived(submit.operation_id_)
            .has_value();
      },
      std::chrono::seconds(10)));

  lavik::meta::PruneOperationArchive prune;
  prune.request_id_ = MakeRequestId(0x41);
  prune.operation_seqs_ = archive.operation_seqs_;
  auto recovered = ProposeSync(prune);
  ASSERT_TRUE(recovered.ok()) << recovered.status();
}

TEST_F(MetaCoordinatorServerTest, FailSafeAuditWindowGate) {
  StartServer({.client_req_timeout_ms_ = 25000});
  WaitLeader();

  // Select strict-export through the replicated command before filling the
  // window. The setup command is itself audited and counts toward capacity.
  const std::uint64_t before = server_->get_committed_log_idx();
  std::vector<nuraft::ptr<nuraft::buffer>> logs;
  logs.reserve(lavik::meta::kMaxMetaAuditWindowRecords);
  lavik::meta::SetAuditPolicy policy;
  policy.request_id_ = MakeRequestId(0x31);
  policy.policy_ = lavik::meta::MetaAuditPolicy::kStrictExport;
  policy.attestation_ = "test-strict-export";
  auto encoded_policy = MetaStateMachine::EncodeCommand(policy);
  ASSERT_TRUE(encoded_policy.ok()) << encoded_policy.status();
  logs.push_back(*encoded_policy);
  const MetaCommand filler = MakeRegister(0x33);
  for (std::uint32_t ii = 1; ii < lavik::meta::kMaxMetaAuditWindowRecords;
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
            lavik::meta::kMaxMetaAuditWindowRecords);
  ASSERT_GE(machine_->last_commit_index(),
            before + lavik::meta::kMaxMetaAuditWindowRecords);

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
            lavik::meta::kMaxMetaAuditWindowRecords);

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
  lavik::meta::PruneAudit first_prune;
  first_prune.request_id_ = MakeRequestId(0x35);
  first_prune.through_log_index_ = first_audit_index;
  auto uncertain = ProposeSync(first_prune);
  ASSERT_FALSE(uncertain.ok());
  EXPECT_EQ(uncertain.status().code(), absl::StatusCode::kDeadlineExceeded);

  lavik::meta::PruneAudit overlapping = first_prune;
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
            lavik::meta::kMaxMetaAuditWindowRecords);
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
  std::vector<std::int64_t> hook_times;
  coordinator_->AddValidateHook(
      [&](const MetaCommand& cmd, const lavik::meta::MetaCommittedView& view,
          const MetaObservationStore& obs,
          std::int64_t proposal_now_unix_ms) -> absl::Status {
        HookObservation record;
        if (const auto* create = std::get_if<CreateGroup>(&cmd)) {
          record.group_id_seen_ = create->group_id_;
        }
        record.node_count_seen_ = view.identity().NodeCount();
        record.obs_seen_ = &obs;
        observations_log.push_back(std::move(record));
        hook_times.push_back(proposal_now_unix_ms);
        return absl::OkStatus();
      });
  coordinator_->AddValidateHook(
      [&](const MetaCommand& cmd, const lavik::meta::MetaCommittedView&,
          const MetaObservationStore&,
          std::int64_t proposal_now_unix_ms) -> absl::Status {
        hook_times.push_back(proposal_now_unix_ms);
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
  ASSERT_EQ(hook_times.size(), 4u);
  EXPECT_EQ(hook_times[0], hook_times[1]);
  EXPECT_EQ(hook_times[2], hook_times[3]);
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
class MockReconciler : public lavik::meta::MetaReconciler {
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
      submit.intent_hash_ = lavik::meta::MetaSha256(submit.intent_);
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
    if (record->lifecycle_ == lavik::meta::MetaOperationLifecycle::kSubmitted) {
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
               lavik::meta::MetaOperationLifecycle::kRunning) {
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
