#include "meta/meta_coordinator.h"

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <deque>
#include <map>
#include <mutex>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>

#include "meta/meta_state_machine.h"
#include "meta/nuraft_log_store.h"
#include "spdlog/spdlog.h"
// NuRaft's headers are not -Wpedantic-clean; see nuraft_scheduler.h.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/async.hxx"
#include "libnuraft/buffer.hxx"
#include "libnuraft/raft_server.hxx"
#include "libnuraft/srv_config.hxx"
#pragma GCC diagnostic pop

namespace keylane::meta {

// Shared because a Raft completion may arrive after the proposing coroutine
// timed out or was dropped. Reservations therefore never call back through a
// potentially destroyed coordinator.
class MetaAuditProposalGate {
 public:
  std::mutex mu_;
  std::uint64_t regular_reservations_ = 0;
  bool prune_reserved_ = false;
};

// ---------------------------------------------------------------------------
// Subscription machinery (the opaque MetaSubscriptionCore of the header).
// ---------------------------------------------------------------------------

namespace {

class AuditReservation {
 public:
  AuditReservation(std::shared_ptr<MetaAuditProposalGate> gate, bool prune)
      : gate_(std::move(gate)), prune_(prune) {}
  ~AuditReservation() {
    if (gate_ == nullptr) return;
    std::lock_guard<std::mutex> lock(gate_->mu_);
    if (prune_) {
      gate_->prune_reserved_ = false;
    } else if (gate_->regular_reservations_ > 0) {
      --gate_->regular_reservations_;
    }
  }

 private:
  std::shared_ptr<MetaAuditProposalGate> gate_;
  bool prune_;
};

struct Subscriber {
  std::uint64_t id_ = 0;
  MetaCommitCallback callback_;
  std::size_t capacity_ = 0;
  std::deque<MetaCommitEvent> queue_;  // FIFO, bounded by capacity_
  bool cancelled_ = false;
  bool needs_resync_ = false;
  // Pinned while the dispatch thread runs the callback; the handle's
  // destructor waits for this before erasing (no callback-after-free).
  bool callback_in_flight_ = false;
};

}  // namespace

class MetaSubscriptionCore {
 public:
  std::mutex mu_;
  // Dispatch wakeup, and the condition for unsubscribe/in-flight waits.
  std::condition_variable cv_;
  std::map<std::uint64_t, std::shared_ptr<Subscriber>> subs_;
  std::uint64_t next_id_ = 1;
  // Highest log index whose committed effects are known to be reflected in
  // the state machine's stores: the sink's high-water mark, seeded from the
  // SM's durable commit watermark at coordinator construction. Snapshot
  // installs advance the stores past it without events — the documented
  // resync case (meta_coordinator.h, MetaSubscriptionStart).
  std::uint64_t high_water_ = 0;
  // Set by the O(1) commit sink and consumed by the dispatch worker. The
  // worker snapshots committed stores only after apply releases the state
  // machine lock, then purges observations made stale by that commit.
  bool observations_dirty_ = false;
  bool dispatch_stop_ = false;
};

MetaCommitSubscription::MetaCommitSubscription(
    std::shared_ptr<MetaSubscriptionCore> core, std::uint64_t id)
    : core_(std::move(core)), id_(id) {}

MetaCommitSubscription::~MetaCommitSubscription() {
  const std::shared_ptr<MetaSubscriptionCore> core = core_;
  if (core == nullptr) return;
  std::unique_lock<std::mutex> lock(core->mu_);
  const auto it = core->subs_.find(id_);
  if (it == core->subs_.end()) return;  // already torn down by the coordinator
  it->second->cancelled_ = true;        // the dispatcher skips it from now on
  core->cv_.wait(lock, [&] { return !it->second->callback_in_flight_; });
  core->subs_.erase(it);
}

bool MetaCommitSubscription::cancelled() const {
  const std::shared_ptr<MetaSubscriptionCore> core = core_;
  if (core == nullptr) return true;
  std::lock_guard<std::mutex> lock(core->mu_);
  const auto it = core->subs_.find(id_);
  return it == core->subs_.end() || it->second->cancelled_;
}

bool MetaCommitSubscription::needs_resync() const {
  const std::shared_ptr<MetaSubscriptionCore> core = core_;
  if (core == nullptr) return true;
  std::lock_guard<std::mutex> lock(core->mu_);
  const auto it = core->subs_.find(id_);
  return it == core->subs_.end() || it->second->needs_resync_;
}

// ---------------------------------------------------------------------------
// MetaStoresFacts
// ---------------------------------------------------------------------------

bool MetaStoresFacts::IsActiveNode(std::string_view node_id) const {
  return stores_.identity_.IsActiveNode(std::string(node_id));
}

uint64_t MetaStoresFacts::CurrentGroupTerm(std::string_view group_id) const {
  // Conservative "unknown" per the MetaCommittedFacts contract: 0.
  return stores_.grant_.CurrentGroupTerm(group_id).value_or(0);
}

uint64_t MetaStoresFacts::CurrentPopulationManifestId(
    std::string_view group_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() ? group->record_.population_manifest_id_ : 0;
}

bool MetaStoresFacts::OperationNonTerminal(const MetaOperationId& id) const {
  // Archived tombstones resolve to terminal summaries, so only a live
  // non-terminal record answers true.
  const auto record = stores_.operation_.FindOperation(id);
  if (!record.has_value()) return false;
  return record->lifecycle_ == MetaOperationLifecycle::kSubmitted ||
         record->lifecycle_ == MetaOperationLifecycle::kRunning;
}

bool MetaStoresFacts::HistoryBoundToOperation(const MetaOperationId& id,
                                              uint64_t history_id) const {
  const auto record = stores_.operation_.FindOperation(id);
  return record.has_value() && record->replication_history_id_ != 0 &&
         record->replication_history_id_ == history_id;
}

// ---------------------------------------------------------------------------
// Propose internals
// ---------------------------------------------------------------------------

namespace {

using CmdResult = nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>;

// Shared wait state for one Propose round trip. The NuRaft completion handler
// (any NuRaft thread, or inline on the caller for an already-completed
// result) fills the raw outcome and resumes the suspended coroutine through
// the resume hook; if the Propose task was destroyed while suspended, the
// awaiter detached the handle and the completion just drops. Shared ownership
// is what makes the drop-safe path possible (same pattern as the ctl
// server's AsyncReply).
struct ProposeWaiter {
  std::mutex mu_;
  std::coroutine_handle<> awaiting_{};
  bool ready_ = false;
  bool detached_ = false;
  nuraft::cmd_result_code code_ = nuraft::cmd_result_code::CANCELLED;
  std::uint64_t log_index_ = 0;
  bool has_exception_ = false;
  MetaProposeResumeHook resume_hook_;
  // Released only when NuRaft resolves the append, not when the caller's
  // local deadline wins. That distinction closes the uncertain-tail audit
  // overflow race.
  std::unique_ptr<AuditReservation> audit_reservation_;
};

// Awaiter for the NuRaft round trip. The destructor runs on every exit from
// the co_await expression — including destruction of a suspended frame — and
// detaches the waiter so a late completion can never resume a dead coroutine.
class ProposeAwaiter {
 public:
  explicit ProposeAwaiter(std::shared_ptr<ProposeWaiter> waiter) noexcept
      : waiter_(std::move(waiter)) {}

  bool await_ready() noexcept {
    std::lock_guard<std::mutex> lock(waiter_->mu_);
    return waiter_->ready_;
  }
  bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
    std::lock_guard<std::mutex> lock(waiter_->mu_);
    if (waiter_->ready_) return false;  // completed before we parked
    waiter_->awaiting_ = awaiting;
    return true;
  }
  // The waiter fields are read by the coroutine body after resumption; both
  // completion paths (inline during when_ready, or through the resume hook)
  // establish the happens-before edge, so no relock is needed there.
  void await_resume() noexcept {}

  ~ProposeAwaiter() {
    std::lock_guard<std::mutex> lock(waiter_->mu_);
    waiter_->detached_ = true;
    waiter_->awaiting_ = {};
  }

 private:
  std::shared_ptr<ProposeWaiter> waiter_;
};

// Shared resume step for both completion paths (raft callback, timeout).
void ResumePropose(const ProposeWaiter& waiter,
                   std::coroutine_handle<> to_resume) {
  if (!to_resume) return;
  if (waiter.resume_hook_) {
    waiter.resume_hook_(to_resume);
  } else {
    to_resume.resume();
  }
}

void CompletePropose(const std::shared_ptr<ProposeWaiter>& waiter,
                     CmdResult& result, nuraft::ptr<std::exception>& err) {
  std::coroutine_handle<> to_resume;
  {
    std::lock_guard<std::mutex> lock(waiter->mu_);
    // FIRST-WINS for the caller-visible result. A late Raft completion still
    // releases its audit reservation: the unresolved append, not the local
    // coroutine lifetime, owns that headroom.
    if (waiter->ready_) {
      waiter->audit_reservation_.reset();
      return;
    }
    waiter->code_ = result.get_result_code();
    // Shutdown delivers CANCELLED together with a "Request cancelled."
    // exception — the code is the signal, the exception only colour.
    waiter->has_exception_ = (err != nullptr);
    if (waiter->code_ == nuraft::cmd_result_code::OK) {
      // The state machine's commit() return carries the applied log index.
      nuraft::ptr<nuraft::buffer>& payload = result.get();
      if (payload != nullptr && payload->size() >= sizeof(std::uint64_t)) {
        payload->pos(0);
        waiter->log_index_ = payload->get_ulong();
      }
    }
    waiter->ready_ = true;
    if (!waiter->detached_ && waiter->awaiting_) {
      to_resume = waiter->awaiting_;
    }
    waiter->audit_reservation_.reset();
  }
  ResumePropose(*waiter, to_resume);
}

MetaCommandTag CommandTagOf(const MetaCommand& command) {
  // The MetaCommand variant is declared in tag order (meta_commands.h:
  // kRegisterNode=1 .. kSetGroupReplicationState=25); pin both ends and the
  // size so a
  // future reorder breaks the build here instead of mislabeling results.
  static_assert(std::variant_size_v<MetaCommand> == 25);
  static_assert(
      std::is_same_v<std::variant_alternative_t<0, MetaCommand>, RegisterNode>);
  static_assert(std::is_same_v<std::variant_alternative_t<24, MetaCommand>,
                               SetGroupReplicationState>);
  return static_cast<MetaCommandTag>(command.index() + 1);
}

// UTC "YYYY-MM-DDTHH:MM:SS.mmmZ" — far under kMaxMetaActorReadableTimeBytes.
// Twin of the ctl server's FormatReadableTime (meta_ctl_server.cpp): both are
// trusted-entry propose stamps; keep the format identical for audit
// readability. If one ever changes, change both.
std::string FormatReadableTime() {
  const auto now = std::chrono::system_clock::now();
  const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now.time_since_epoch())
                          .count();
  const std::time_t secs = static_cast<std::time_t>(millis / 1000);
  std::tm tm{};
  ::gmtime_r(&secs, &tm);
  // Sized for the theoretical worst case of the %0Nd ints (~75 bytes).
  char buf[96];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                tm.tm_min, tm.tm_sec, static_cast<int>(millis % 1000));
  return buf;
}

std::int64_t NowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

absl::Status UncertainOutcome(absl::StatusCode code, const std::string& what) {
  return absl::Status(
      code,
      "meta: propose " + what +
          "; outcome uncertain — the command may still have committed; "
          "reconcile via CommittedView using the command's idempotency key "
          "(commands are replay/idempotency-safe by design)");
}

}  // namespace

// ---------------------------------------------------------------------------
// MetaProposeTimer: one thread walking a deadline queue of proposal waiters.
// NuRaft's async_handler return method resolves cmd_results only on commit or
// shutdown — it has no client-side timeout — so the seam bounds the wait
// itself. Expiry resolves the waiter as TIMEOUT (first-wins against a late
// raft completion). The thread holds only weak waiter references and never
// touches coordinator state, so destruction just stops and joins.
// ---------------------------------------------------------------------------

class MetaProposeTimer {
 public:
  MetaProposeTimer() : thread_([this] { Main(); }) {}
  ~MetaProposeTimer() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
  }
  MetaProposeTimer(const MetaProposeTimer&) = delete;
  MetaProposeTimer& operator=(const MetaProposeTimer&) = delete;

  void Arm(std::chrono::steady_clock::time_point deadline,
           std::weak_ptr<ProposeWaiter> waiter) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      queue_.emplace(std::make_pair(deadline, seq_++), std::move(waiter));
    }
    cv_.notify_all();
  }

 private:
  void Main() {
    std::unique_lock<std::mutex> lock(mu_);
    for (;;) {
      if (stop_) return;
      if (queue_.empty()) {
        cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
        continue;
      }
      const auto earliest = queue_.begin();
      const auto now = std::chrono::steady_clock::now();
      if (earliest->first.first > now) {
        // Wake at the deadline, or early when Arm notifies (the new entry may
        // be earlier than the current earliest).
        cv_.wait_until(lock, earliest->first.first);
        continue;
      }
      std::shared_ptr<ProposeWaiter> waiter = earliest->second.lock();
      queue_.erase(earliest);
      // The resume runs arbitrary continuations — never under the queue lock.
      lock.unlock();
      ResolveTimeout(std::move(waiter));
      lock.lock();
    }
  }

  static void ResolveTimeout(std::shared_ptr<ProposeWaiter> waiter) {
    if (waiter == nullptr) return;
    std::coroutine_handle<> to_resume;
    {
      std::lock_guard<std::mutex> lock(waiter->mu_);
      if (waiter->ready_) return;  // the raft completion won
      waiter->code_ = nuraft::cmd_result_code::TIMEOUT;
      waiter->ready_ = true;
      if (!waiter->detached_ && waiter->awaiting_) {
        to_resume = waiter->awaiting_;
      }
    }
    ResumePropose(*waiter, to_resume);
  }

  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  std::map<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>,
           std::weak_ptr<ProposeWaiter>>
      queue_;
  std::uint64_t seq_ = 0;
  std::thread thread_;
};

// Decrements the coordinator's in-flight count on every coroutine exit,
// normal or by frame destruction mid-flight (a dropped Propose task). That
// second path is what keeps the coordinator destructor's drain wait from
// hanging on abandoned tasks.
class MetaCoordinator::InFlightGuard {
 public:
  explicit InFlightGuard(MetaCoordinator& coordinator)
      : coordinator_(coordinator) {
    coordinator_.InFlightEnter();
  }
  ~InFlightGuard() { coordinator_.InFlightLeave(); }

 private:
  MetaCoordinator& coordinator_;
};

// ---------------------------------------------------------------------------
// MetaCoordinator
// ---------------------------------------------------------------------------

MetaCoordinator::MetaCoordinator(nuraft::ptr<nuraft::raft_server> server,
                                 MetaStateMachine& state_machine,
                                 NuraftLogStore& log_store,
                                 MetaObservationStore& observations,
                                 MetaCoordinatorOptions options)
    : server_(std::move(server)),
      state_machine_(state_machine),
      log_store_(log_store),
      observations_(observations),
      options_(std::move(options)),
      audit_gate_(std::make_shared<MetaAuditProposalGate>()),
      propose_timer_(std::make_unique<MetaProposeTimer>()),
      sub_core_(std::make_shared<MetaSubscriptionCore>()),
      leader_context_(*this,
                      AuthenticatedPrincipal(options_.coordinator_principal_,
                                             MetaPrincipalPasskey{})) {
  // A coordinator assembled onto already-committed state (recovery,
  // assembly ordering) starts its coverage mark at the durable watermark:
  // everything at or below it is covered by the initial view, never by
  // events this process will deliver.
  sub_core_->high_water_ = state_machine_.last_commit_index();

  // The sink must be O(1) and non-blocking — it runs on the commit thread
  // under the SM state mutex (meta_state_machine.h). It captures the core by
  // weak_ptr so a sink call racing the destructor can never touch a dead
  // coordinator; the destructor detaches the sink before tearing down.
  std::weak_ptr<MetaSubscriptionCore> weak_core = sub_core_;
  state_machine_.SetCommitEventSink(
      [weak_core](std::uint64_t log_index, const MetaApplyResult& result) {
        const std::shared_ptr<MetaSubscriptionCore> core = weak_core.lock();
        if (core == nullptr) return;
        std::lock_guard<std::mutex> lock(core->mu_);
        if (log_index > core->high_water_) {
          core->high_water_ = log_index;
        }
        core->observations_dirty_ = true;
        bool any_queued = false;
        for (const auto& entry : core->subs_) {
          const std::shared_ptr<Subscriber>& sub = entry.second;
          if (sub->cancelled_) continue;
          if (sub->queue_.size() >= sub->capacity_) {
            // Backpressure contract: overflow cancels the subscription and
            // flags mandatory resync. The event is NOT delivered — the
            // consumer must rebuild from a fresh view, never guess.
            sub->cancelled_ = true;
            sub->needs_resync_ = true;
            sub->queue_.clear();
            continue;
          }
          sub->queue_.push_back(MetaCommitEvent{log_index, result});
          any_queued = true;
        }
        // Revalidation needs a wake even when there are no subscriptions.
        if (any_queued || core->observations_dirty_) core->cv_.notify_all();
      });

  try {
    dispatch_thread_ = std::thread(&MetaCoordinator::DispatchMain, this);
    leadership_thread_ = std::thread(&MetaCoordinator::LeadershipMain, this);
  } catch (const std::system_error&) {
    // Half-started assembly: stop what exists before propagating.
    stopping_.store(true, std::memory_order_release);
    {
      std::lock_guard<std::mutex> lock(sub_core_->mu_);
      sub_core_->dispatch_stop_ = true;
    }
    sub_core_->cv_.notify_all();
    leadership_cv_.notify_all();
    if (dispatch_thread_.joinable()) dispatch_thread_.join();
    if (leadership_thread_.joinable()) leadership_thread_.join();
    state_machine_.SetCommitEventSink({});
    throw;
  }
}

MetaCoordinator::~MetaCoordinator() {
  stopping_.store(true, std::memory_order_release);
  // Detach first: no new sink calls after this returns (an in-flight call
  // holds its own core reference and finishes on its own).
  state_machine_.SetCommitEventSink({});

  // Cancel and join reconcilers on the leadership thread (it owns every
  // Start/CancelAndWait call).
  {
    std::lock_guard<std::mutex> lock(leadership_mu_);
    desired_leader_ = false;
  }
  leadership_cv_.notify_all();
  if (leadership_thread_.joinable()) leadership_thread_.join();

  // Cancel every subscription, stop the dispatcher, join it. Handles that
  // outlive the coordinator observe cancelled() through the shared core.
  {
    std::lock_guard<std::mutex> lock(sub_core_->mu_);
    for (const auto& entry : sub_core_->subs_) {
      entry.second->cancelled_ = true;
      entry.second->queue_.clear();
    }
    sub_core_->dispatch_stop_ = true;
  }
  sub_core_->cv_.notify_all();
  if (dispatch_thread_.joinable()) dispatch_thread_.join();

  // Drain in-flight proposals. raft_server::shutdown() (teardown contract,
  // file header) resolves their cmd_results; wedged ones are resolved by the
  // propose timer (still alive here); dropped Propose tasks decrement via
  // their InFlightGuard. Either way this terminates.
  {
    std::unique_lock<std::mutex> lock(gate_mu_);
    gate_cv_.wait(lock, [&] { return in_flight_ == 0; });
  }

  // Only now stop the timer: in-flight proposals needed it for their
  // timeout resolution. Any waiter entries left in its queue are weak and
  // simply expire.
  propose_timer_.reset();
}

void MetaCoordinator::InFlightEnter() {
  std::lock_guard<std::mutex> lock(gate_mu_);
  ++in_flight_;
}

void MetaCoordinator::InFlightLeave() {
  std::lock_guard<std::mutex> lock(gate_mu_);
  if (in_flight_ > 0) --in_flight_;
  if (in_flight_ == 0) gate_cv_.notify_all();
}

absl::Status MetaCoordinator::NotLeaderStatus() const {
  std::string hint = "none";
  const std::int32_t leader = server_->get_leader();
  if (leader >= 0) {
    hint = "id=" + std::to_string(leader);
    const nuraft::ptr<nuraft::srv_config> config =
        server_->get_srv_config(leader);
    if (config != nullptr) {
      hint += " endpoint=" + config->get_endpoint();
    }
  }
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "meta: not leader; known leader: " + hint);
}

MetaStores MetaCoordinator::AtomicStoresSnapshot(std::uint64_t& applied_index,
                                                 std::uint64_t& high_water) {
  const std::shared_ptr<MetaSubscriptionCore> core = sub_core_;
  // Two-phase bracketed read, NO core lock held across SM calls (lock order).
  // Accept when neither watermark moved across the capture:
  //   - high_water_ (sink, set inside the SM's apply critical section) stable
  //     => no command commit reflected in the captured stores is newer than
  //        it;
  //   - last_commit_index() (set atomically with the stores for snapshot
  //     installs, right after them for command commits) stable => no install
  //     landed mid-capture.
  // Together the accepted stores reflect exactly the committed prefix through
  // li1. Retries are rare: control-plane commit rates are low.
  for (;;) {
    std::uint64_t hw1;
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      hw1 = core->high_water_;
    }
    const std::uint64_t li1 = state_machine_.last_commit_index();
    MetaStores stores = state_machine_.StoresSnapshot();
    const std::uint64_t li2 = state_machine_.last_commit_index();
    std::uint64_t hw2;
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      hw2 = core->high_water_;
    }
    if (hw1 == hw2 && li1 == li2) {
      applied_index = li1;
      high_water = hw1;
      return stores;
    }
  }
}

MetaCommittedView MetaCoordinator::CommittedView() {
  std::uint64_t applied_index = 0;
  std::uint64_t high_water = 0;
  return MetaCommittedView(AtomicStoresSnapshot(applied_index, high_water),
                           applied_index);
}

MetaSubscriptionStart MetaCoordinator::SubscribeCommitted(
    MetaCommitCallback callback, std::size_t queue_capacity) {
  const std::shared_ptr<MetaSubscriptionCore> core = sub_core_;
  const std::size_t capacity = queue_capacity != 0
                                   ? queue_capacity
                                   : options_.default_subscription_capacity_;
  // Same atomic capture as AtomicStoresSnapshot, with registration folded
  // into the accepting critical section: the subscriber's queue starts empty
  // and the sink appends every later commit, so events are exactly the
  // commits with log_index > cursor, in order.
  for (;;) {
    std::uint64_t hw1;
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      hw1 = core->high_water_;
    }
    const std::uint64_t li1 = state_machine_.last_commit_index();
    MetaStores stores = state_machine_.StoresSnapshot();
    const std::uint64_t li2 = state_machine_.last_commit_index();
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      if (core->high_water_ == hw1 && li1 == li2) {
        auto sub = std::make_shared<Subscriber>();
        sub->id_ = core->next_id_++;
        sub->callback_ = std::move(callback);
        sub->capacity_ = capacity;
        core->subs_[sub->id_] = sub;
        return MetaSubscriptionStart{
            MetaCommittedView(std::move(stores), li1), hw1,
            std::unique_ptr<MetaCommitSubscription>(
                new MetaCommitSubscription(core, sub->id_))};
      }
    }
  }
}

void MetaCoordinator::AddValidateHook(MetaValidateHook hook) {
  hooks_.push_back(std::move(hook));
}

void MetaCoordinator::DispatchMain() {
  const std::shared_ptr<MetaSubscriptionCore> core = sub_core_;
  std::unique_lock<std::mutex> lock(core->mu_);
  while (!core->dispatch_stop_) {
    core->cv_.wait(lock, [&] {
      if (core->dispatch_stop_) return true;
      if (core->observations_dirty_) return true;
      for (const auto& entry : core->subs_) {
        const std::shared_ptr<Subscriber>& sub = entry.second;
        if (!sub->cancelled_ && !sub->callback_in_flight_ &&
            !sub->queue_.empty()) {
          return true;
        }
      }
      return false;
    });
    if (core->dispatch_stop_) break;
    if (core->observations_dirty_) {
      core->observations_dirty_ = false;
      lock.unlock();
      // Do not hold the subscription mutex across state-machine or
      // observation-store calls. A commit racing this snapshot sets the dirty
      // bit again, guaranteeing another pass after this one.
      const MetaStores stores = state_machine_.StoresSnapshot();
      const MetaStoresFacts facts(stores);
      observations_.RevalidateAll(facts, NowUnixMs());
      lock.lock();
      continue;
    }
    for (const auto& entry : core->subs_) {
      const std::shared_ptr<Subscriber>& sub = entry.second;
      if (sub->cancelled_ || sub->callback_in_flight_ || sub->queue_.empty()) {
        continue;
      }
      MetaCommitEvent event = std::move(sub->queue_.front());
      sub->queue_.pop_front();
      sub->callback_in_flight_ = true;
      MetaCommitCallback callback = sub->callback_;
      lock.unlock();
      // Subscriber code on the dispatch thread. Per-subscription delivery is
      // strictly FIFO; one event per round keeps cancellation responsive.
      callback(event);
      lock.lock();
      sub->callback_in_flight_ = false;
      core->cv_.notify_all();  // wake a handle destructor waiting on in-flight
      break;
    }
  }
}

void MetaCoordinator::RunAsLeader(std::shared_ptr<MetaReconciler> reconciler) {
  {
    std::lock_guard<std::mutex> lock(leadership_mu_);
    if (stopping_.load(std::memory_order_acquire)) return;
    reconcilers_.push_back(ReconcilerEntry{std::move(reconciler), false});
  }
  leadership_cv_.notify_all();
}

void MetaCoordinator::BecomeLeader() {
  {
    std::lock_guard<std::mutex> lock(leadership_mu_);
    if (stopping_.load(std::memory_order_acquire)) return;
    desired_leader_ = true;
  }
  leadership_cv_.notify_all();
}

void MetaCoordinator::BecomeFollower() {
  {
    std::lock_guard<std::mutex> lock(leadership_mu_);
    if (stopping_.load(std::memory_order_acquire)) return;
    desired_leader_ = false;
  }
  leadership_cv_.notify_all();
}

void MetaCoordinator::LeadershipMain() {
  std::unique_lock<std::mutex> lock(leadership_mu_);
  const auto has_unstarted = [&] {
    if (!applied_leader_) return false;
    for (const ReconcilerEntry& entry : reconcilers_) {
      if (!entry.started_) return true;
    }
    return false;
  };
  while (!stopping_.load(std::memory_order_acquire)) {
    leadership_cv_.wait(lock, [&] {
      return stopping_.load(std::memory_order_acquire) ||
             desired_leader_ != applied_leader_ || has_unstarted();
    });
    if (stopping_.load(std::memory_order_acquire)) break;
    if (desired_leader_ == applied_leader_ && !has_unstarted()) continue;

    if (desired_leader_) {
      applied_leader_ = true;
      // Soft evidence is scoped to one leadership epoch. Reset before any
      // reconciler starts so it can only act on freshly authenticated reports.
      observations_.ResetForLeadershipChange();
      for (ReconcilerEntry& entry : reconcilers_) {
        if (!entry.started_) {
          entry.started_ = true;
          // Reconciler calls run under leadership_mu_ on purpose: the
          // reconciler contract never calls back into leadership, and this
          // serialization is what makes the Start/CancelAndWait pairing
          // strict per reconciler.
          entry.reconciler_->Start(leader_context_);
        }
      }
    } else {
      applied_leader_ = false;
      // Cancel in reverse registration order (stack discipline for
      // reconcilers that depend on earlier ones).
      for (auto it = reconcilers_.rbegin(); it != reconcilers_.rend(); ++it) {
        if (it->started_) {
          it->reconciler_->CancelAndWait();
          it->started_ = false;
        }
      }
      observations_.ResetForLeadershipChange();
    }
  }
  // Teardown: cancel and join whatever is still running.
  for (auto it = reconcilers_.rbegin(); it != reconcilers_.rend(); ++it) {
    if (it->started_) {
      it->reconciler_->CancelAndWait();
      it->started_ = false;
    }
  }
  applied_leader_ = false;
}

celer::Task<absl::StatusOr<MetaApplyResult>> MetaCoordinator::Propose(
    MetaCommand command, AuthenticatedPrincipal principal) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return absl::Status(absl::StatusCode::kCancelled,
                           "meta: coordinator is stopping");
  }
  if (server_ == nullptr) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "meta: no raft server attached");
  }
  if (!server_->is_leader()) {
    co_return NotLeaderStatus();
  }

  // Counted from here to every exit (normal or frame destruction) so the
  // destructor can drain caller coroutines. Audit headroom follows the Raft
  // completion separately and can outlive a timed-out caller.
  InFlightGuard in_flight(*this);

  // One atomic committed view serves the fail-safe gates AND the validate
  // hooks (KB-scale copy at control-plane proposal rates; cheaper than
  // letting each hook snapshot independently, and consistent across them).
  std::uint64_t applied_index = 0;
  std::uint64_t high_water = 0;
  MetaCommittedView view(AtomicStoresSnapshot(applied_index, high_water),
                         applied_index);

  // Reserve audit headroom before validation/encoding. Regular proposals may
  // share the remaining capacity; prune is exclusive because its net effect
  // depends on the exact committed prefix. The reservation moves to the Raft
  // waiter below, so an uncertain client timeout cannot free space while its
  // append is still unresolved.
  std::unique_ptr<AuditReservation> audit_reservation;
  {
    std::lock_guard<std::mutex> lock(audit_gate_->mu_);
    const MetaAuditStore& audit = view.stores().audit_;
    const auto* prune = std::get_if<PruneAudit>(&command);
    const bool valid_prune =
        prune != nullptr && audit.Find(prune->through_log_index_).has_value();
    const bool prune_busy =
        audit_gate_->prune_reserved_ || audit_gate_->regular_reservations_ != 0;
    const bool regular_overflow =
        audit_gate_->prune_reserved_ || audit.NeedsExport() ||
        audit.size() + audit_gate_->regular_reservations_ + 1 >
            audit.capacity();
    const bool prune_overflow =
        !valid_prune &&
        (audit.NeedsExport() || audit.size() + 1 > audit.capacity());
    if ((prune != nullptr && (prune_busy || prune_overflow)) ||
        (prune == nullptr && regular_overflow)) {
      co_return absl::Status(
          absl::StatusCode::kResourceExhausted,
          "meta: audit headroom unavailable; wait for pending proposals or "
          "export and prune the full window (fail-safe, plan §2)");
    }
    if (prune != nullptr) {
      audit_gate_->prune_reserved_ = true;
      audit_reservation =
          std::make_unique<AuditReservation>(audit_gate_, /*prune=*/true);
    } else {
      ++audit_gate_->regular_reservations_;
      audit_reservation =
          std::make_unique<AuditReservation>(audit_gate_, /*prune=*/false);
    }
  }

  // Remaining fail-safe gates do not need audit-gate serialization.
  {
    const std::uint64_t uncompacted = log_store_.UncompactedBytes();
    if (uncompacted > options_.max_uncompacted_wal_bytes_) {
      co_return absl::Status(
          absl::StatusCode::kResourceExhausted,
          "meta: uncompacted WAL bytes " + std::to_string(uncompacted) +
              " exceed limit " +
              std::to_string(options_.max_uncompacted_wal_bytes_) +
              "; snapshot/compaction outstanding (fail-safe, plan §3)");
    }
    const std::uint64_t snapshot_failures =
        state_machine_.consecutive_snapshot_failures();
    if (snapshot_failures >= options_.max_consecutive_snapshot_failures_) {
      co_return absl::Status(
          absl::StatusCode::kResourceExhausted,
          "meta: " + std::to_string(snapshot_failures) +
              " consecutive snapshot failures reached the limit " +
              std::to_string(options_.max_consecutive_snapshot_failures_) +
              " (fail-safe, plan §3)");
    }
  }

  // ValidateProposal plugins (leader-local; plan §2). First rejection aborts
  // the proposal before anything is encoded or appended.
  for (const MetaValidateHook& hook : hooks_) {
    const absl::Status status = hook(command, view, observations_);
    if (!status.ok()) co_return status;
  }

  // §3 升级契约: proposals use the committed active write schema. The
  // codec emits the current or immediately preceding version; anything else
  // means the binary/format window moved, so fail before writing a format
  // peers cannot read.
  if (view.active_write_schema() < kMetaMinReadableSchemaVersion ||
      view.active_write_schema() > kMetaCurrentSchemaVersion) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "meta: committed active_write_schema " +
                               std::to_string(view.active_write_schema()) +
                               " is outside this binary's write window");
  }

  // Actor injection (plan §2 审计模型): the trusted entry's principal plus a
  // propose-time readable timestamp. The clock read is legal HERE — the
  // proposal entry point; apply only copies the text into the audit record.
  const std::string readable_time = FormatReadableTime();
  std::visit(
      [&](auto& cmd) {
        cmd.actor_.principal_ = principal.principal();
        cmd.actor_.readable_time_ = readable_time;
      },
      command);

  auto encoded =
      MetaStateMachine::EncodeCommand(command, view.active_write_schema());
  if (!encoded.ok()) co_return encoded.status();

  std::vector<nuraft::ptr<nuraft::buffer>> logs;
  logs.push_back(*encoded);
  nuraft::ptr<CmdResult> result = server_->append_entries(logs);
  if (result == nullptr) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "meta: raft append returned no result handle");
  }

  auto waiter = std::make_shared<ProposeWaiter>();
  waiter->resume_hook_ = options_.resume_hook_;
  waiter->audit_reservation_ = std::move(audit_reservation);
  // The seam's own round-trip bound (NuRaft's async_handler mode has no
  // client-side timeout). First-wins against the raft completion.
  propose_timer_->Arm(
      std::chrono::steady_clock::now() +
          std::chrono::milliseconds(options_.propose_timeout_ms_),
      waiter);
  result->when_ready(
      [waiter](CmdResult& completed, nuraft::ptr<std::exception>& err) {
        CompletePropose(waiter, completed, err);
      });
  co_await ProposeAwaiter(waiter);

  // The waiter is filled (see ProposeAwaiter for the happens-before).
  switch (waiter->code_) {
    case nuraft::cmd_result_code::OK:
      break;
    case nuraft::cmd_result_code::TIMEOUT:
      co_return UncertainOutcome(absl::StatusCode::kDeadlineExceeded,
                                 "timed out");
    case nuraft::cmd_result_code::CANCELLED:
      co_return UncertainOutcome(absl::StatusCode::kCancelled,
                                 "was cancelled (shutdown or leadership loss)");
    case nuraft::cmd_result_code::NOT_LEADER:
      co_return NotLeaderStatus();
    default:
      co_return UncertainOutcome(
          absl::StatusCode::kInternal,
          std::string("failed with raft code ") +
              std::to_string(static_cast<int>(waiter->code_)) +
              (waiter->has_exception_ ? " (exception attached)" : ""));
  }
  if (waiter->log_index_ == 0) {
    co_return UncertainOutcome(absl::StatusCode::kInternal,
                               "committed without a log index");
  }

  // OK means the entry committed AND this leader's SM applied it (the commit
  // result payload is commit()'s return). The verdict comes from the audit
  // record at the command's log index — the same record the operator audit
  // trail persists, so propose-time reporting can never drift from it.
  const auto audit =
      state_machine_.StoresSnapshot().audit_.Find(waiter->log_index_);
  if (!audit.has_value()) {
    co_return absl::Status(
        absl::StatusCode::kInternal,
        "meta: committed at log index " + std::to_string(waiter->log_index_) +
            " but its audit record is missing (concurrent export/prune?)");
  }
  co_return MetaApplyResult{audit->record_.verdict_,
                            audit->record_.verdict_detail_, waiter->log_index_,
                            CommandTagOf(command)};
}

celer::Task<absl::StatusOr<MetaApplyResult>> MetaLeaderContext::Propose(
    MetaCommand command) {
  return coordinator_->Propose(std::move(command), actor_);
}

MetaCommittedView MetaLeaderContext::CommittedView() {
  return coordinator_->CommittedView();
}

MetaSubscriptionStart MetaLeaderContext::SubscribeCommitted(
    MetaCommitCallback callback, std::size_t queue_capacity) {
  return coordinator_->SubscribeCommitted(std::move(callback), queue_capacity);
}

const MetaObservationStore& MetaLeaderContext::Observations() const {
  return coordinator_->Observations();
}

}  // namespace keylane::meta
