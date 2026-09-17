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

#pragma once

// MetaCoordinator is the in-process API for control sessions and ordinary
// reconcilers: consumers see commands, committed views, commit subscriptions,
// and reconciler lifecycle rather than Raft types. Process-wired membership
// and genesis-barrier reconcilers additionally receive the raft_server for
// configuration changes or peer progress, but still submit state-machine
// effects only through the coordinator.
//
// ASSEMBLY, OWNERSHIP, AND DESTRUCTION ORDER
//
// The coordinator is assembled at process wiring time from pieces the caller
// owns: it holds a refcounted nuraft::ptr on the raft_server (an in-flight
// Propose must never see a destroyed core) and plain references to the
// MetaStateMachine, the WAL v1 NuraftLogStore (fail-safe gate only), and the
// leader-local MetaObservationStore. Those three references MUST outlive the
// coordinator. Destruction contract (mirrors the state machine's shutdown
// contract in state_machine.h):
//   stop ingress -> proposal_executor::Shutdown() -> raft_launcher::shutdown()
//   -> MetaStateMachine::WaitForSnapshotWriterIdle() -> destroy coordinator
//   -> destroy state machine / log store.
// Executor shutdown first submits every accepted mutation to NuRaft; launcher
// shutdown then resolves pending cmd_results (normally CANCELLED). The
// coordinator destructor drains in-flight proposals, cancels and joins all
// registered reconcilers, detaches the commit-event sink, and cancels every
// live subscription before it returns. A Propose task dropped by its caller
// detaches (below). The commit-event sink is detached under the state machine's
// sink mutex, so the commit thread never calls into a half-destroyed
// coordinator.
//
// THREAD MODEL
//
//   - Propose is a bycorf::Task coroutine. Everything before the first
//     suspension — leader check, fail-safe gates, ValidateProposal hooks,
//     actor injection, encoding — runs SYNCHRONOUSLY on the
//     caller's thread. Hooks that read the observation store therefore
//     require the caller to run on the coordinator's owner thread (the bycorf
//     worker in production). The observation store is internally serialized
//     because commit-driven revalidation runs on the dispatch thread.
//   - The commit round trip suspends. Submission goes through the injected
//     proposal executor before entering NuRaft's mutation path, so WAL work
//     never blocks the production Bycorf worker. Short read-only role/config
//     checks remain on the caller. Completion then goes through the injected
//     options.foreign_executor_ — production resumes through Bycorf's target
//     worker mailbox so the continuation (and the awaiting reconciler) lands
//     back on its owner. There is deliberately no implicit inline fallback:
//     a coordinator attached to Raft requires this executor, preventing a
//     NuRaft or timeout thread from accidentally running Bycorf-owned code.
//     Plain-thread tests opt into an explicit inline policy. A Propose task
//     destroyed while suspended is SAFE: the awaiter detaches, and the late
//     NuRaft completion fills a shared waiter and resumes nothing.
//   - Commit events: the MetaStateMachine invokes the coordinator's sink from
//     its commit thread, under the state mutex, right after ApplyCommitted
//     (MetaCommitEventSink, state_machine.h). The sink is O(1) and never
//     blocks (bounded enqueue + notify only). A dedicated dispatch thread per
//     coordinator delivers queued events to subscriber callbacks; callbacks
//     run on THAT thread, serialize per subscription, and must be quick and
//     never block indefinitely.
//   - Leadership transitions: BecomeLeader()/BecomeFollower() are wired from
//     the NuRaft init_options::raft_callback_ (meta_main; tests may drive
//     them directly). NuRaft may hold raft_server::lock_ while invoking the
//     callback, so both are O(1) queue pushes onto the coordinator's
//     leadership thread; reconciler Start()/CancelAndWait() always run on
//     that thread, strictly serialized per reconciler.
//   - Propose timeouts: a dedicated timer thread walks a deadline queue
//     (options_.propose_timeout_ms_). It touches only weak references to
//     proposal waiters, never coordinator state, so it is teardown-safe.
//     Completion is FIRST-WINS between the raft callback and the timeout.
//   - Lock order: SM state mutex -> SM sink mutex -> subscription core mutex.
//     The coordinator never calls into the state machine while holding the
//     subscription core mutex (the atomic triple uses a two-phase retry read,
//     see SubscribeCommitted), so the order is never inverted.
//
// TRUST BOUNDARY
//
// Every privileged command carries an ActorContext that only a TRUSTED ENTRY
// may inject. Propose takes an AuthenticatedPrincipal, which can only be
// CONSTRUCTED with a MetaPrincipalPasskey — a passkey whose own constructor
// is private and friended to exactly the trusted entries: the ctl surface
// (MetaCtlServer, authorized by UDS credentials, mTLS identity, or explicit
// plaintext-listener reachability), the
// coordinator itself (it mints the internal actor LeaderContext proposes
// with), and the
// test peer. Copying an existing principal inside trusted code is allowed;
// untrusted code can never mint one. The coordinator stamps readable_time
// from the system clock at propose time — reading the clock HERE is legal
// because this is the proposal entry point. Apply never reads a clock; it
// copies the injected text into the audit record.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/runtime/foreign_executor.h"
#include "bycorf/runtime/task.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/proposal_executor.h"
#include "lavik/meta/state_apply.h"
// NuRaft's headers are not -Wpedantic-clean.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/ptr.hxx"
#pragma GCC diagnostic pop

namespace nuraft {
class raft_server;
}

namespace lavik::meta {

class MetaStateMachine;
class NuraftLogStore;
class MetaCoordinator;
// Opaque propose-timeout machinery (coordinator.cpp); see
// MetaCoordinatorOptions::propose_timeout_ms_.
class MetaProposeTimer;
class MetaProposalGate;

// ---------------------------------------------------------------------------
// Trust boundary: passkey + transport-authorized principal (see the file
// header).
// ---------------------------------------------------------------------------

// Construction token for AuthenticatedPrincipal. Only the friended trusted
// entries can create one; everyone else can carry and inspect the principal
// but never mint it.
class MetaPrincipalPasskey {
 private:
  MetaPrincipalPasskey() = default;
  friend class MetaCoordinator;
  friend class MetaCtlServer;            // transport-authorized ctl entry
  friend class MetaCoordinatorTestPeer;  // tests/meta_coordinator_test.cpp
};

// The transport-authorized identity of a privileged proposer, injected into
// the command's ActorContext by Propose. In plaintext TCP mode this is the
// explicit fixed unauthenticated actor documented by MetaCtlServer. Value
// type; copyable inside trusted code, but only constructible through
// MetaPrincipalPasskey.
class AuthenticatedPrincipal {
 public:
  AuthenticatedPrincipal(std::string principal, MetaPrincipalPasskey)
      : principal_(std::move(principal)) {}
  const std::string& principal() const { return principal_; }
  bool operator==(const AuthenticatedPrincipal&) const = default;

 private:
  std::string principal_;
};

// ---------------------------------------------------------------------------
// MetaCommittedView: one atomic read of the committed aggregate.
// A single snapshot object pairing a deep copy of MetaStores with the applied
// index they reflect — consumers never observe a cross-store tear. Produced
// only by the coordinator (the constructor is just a value bundle; the
// atomicity comes from the coordinator's capture protocol).
// ---------------------------------------------------------------------------

class MetaCommittedView {
 public:
  MetaCommittedView(MetaStores stores, std::uint64_t applied_index)
      : stores_(std::move(stores)), applied_index_(applied_index) {}

  const MetaStores& stores() const { return stores_; }
  // Highest raft log index whose effects the stores reflect.
  std::uint64_t applied_index() const { return applied_index_; }

  const MetaIdentityStore& identity() const { return stores_.identity_; }
  const MetaTopologyStore& topology() const { return stores_.topology_; }
  const MetaPolicyStore& policy() const { return stores_.policy_; }
  const MetaOperationStore& operation() const { return stores_.operation_; }
  const MetaPopulationManifestStore& population_manifest() const {
    return stores_.population_manifest_;
  }
  const MetaAuditStore& audit() const { return stores_.audit_; }

 private:
  MetaStores stores_;
  std::uint64_t applied_index_;
};

// MetaCommittedFacts (observation freshness queries, observation_store.h)
// answered from committed stores — the production implementation the obs store
// header refers to. Holds a reference: the referenced stores must outlive the
// adapter (bind it to a MetaCommittedView for the duration of a query batch).
class MetaStoresFacts : public MetaCommittedFacts {
 public:
  explicit MetaStoresFacts(const MetaStores& stores) : stores_(stores) {}

  bool IsActiveNode(std::string_view node_id) const override;
  uint64_t CurrentGroupTerm(std::string_view group_id) const override;
  uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const override;
  MetaHash256 CurrentPopulationManifestDigest(
      std::string_view group_id) const override;
  uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const override;
  bool AssignmentMatches(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment_id) const override;
  bool IsOwnerAssignment(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment_id) const override;
  bool MayReportFencedOwnerCandidate(
      const MetaCandidateProgressObs& candidate) const override;
  bool IsCurrentFailoverCandidate(
      std::string_view node_id,
      const MetaBootIncarnation& boot_id) const override;
  std::optional<FailoverTransitionView> FailoverTransitionById(
      const MetaFailoverTransitionId& transition_id) const override;

 private:
  const MetaStores& stores_;
};

// ---------------------------------------------------------------------------
// Committed-stream subscription.
// ---------------------------------------------------------------------------

// One delivered commit: the applied log index and its apply verdict (replay
// idempotency means a repeated index always carries the same verdict).
struct MetaCommitEvent {
  std::uint64_t log_index_ = 0;
  MetaApplyResult result_;
};

using MetaCommitCallback = std::function<void(const MetaCommitEvent&)>;

// Opaque subscription machinery shared between the coordinator and live
// handles (defined in coordinator.cpp); the shared_ptr keeps it alive
// for handles that outlive the coordinator.
class MetaSubscriptionCore;

// Subscription handle. Destruction unsubscribes, blocking until any in-flight
// callback for this subscription has returned (so the callback never touches
// freed subscriber state). Never destroy the handle from inside its own
// callback — the destructor waits for that very callback.
class MetaCommitSubscription {
 public:
  ~MetaCommitSubscription();
  MetaCommitSubscription(const MetaCommitSubscription&) = delete;
  MetaCommitSubscription& operator=(const MetaCommitSubscription&) = delete;

  // Overflow cancellation: once the bounded per-subscriber queue overflows,
  // the subscription is cancelled, no callback fires again, and
  // needs_resync() reports that the consumer must re-SubscribeCommitted from
  // a fresh view. cancelled() is
  // also true after the owning coordinator is destroyed.
  bool cancelled() const;
  bool needs_resync() const;

 private:
  friend class MetaCoordinator;
  MetaCommitSubscription(std::shared_ptr<MetaSubscriptionCore> core,
                         std::uint64_t id);
  std::shared_ptr<MetaSubscriptionCore> core_;
  std::uint64_t id_;
};

// The atomic triple: the complete committed view, the cursor it was
// captured against, and the live subscription — captured as one consistent
// unit, so "everything committed" = view + events with log_index > cursor.
//
// Delivery contract:
//   - Events arrive in strict commit order, one per committed command.
//   - REPLAY may deliver the same index twice because apply is repeatable;
//     the subscriber dedups by index. The documented subscriber discipline:
//     start with watermark = view.applied_index(), skip events with
//     log_index <= watermark, otherwise process and advance the watermark.
//   - Normally cursor == view.applied_index(). When committed state arrived
//     via a snapshot INSTALL (a catching-up follower), the install replaces
//     state without per-entry events: then view.applied_index() > cursor and
//     the entries in between are covered by the view, never by events. The
//     watermark rule above absorbs both cases. Leader-side consumers never
//     rely on this gap: RunAsLeader reconcilers resubscribe on every
//     BecomeLeader.
//   - Config-only commits carry no command and produce no events; the event
//     stream is therefore NOT index-contiguous. Key on log_index, never on
//     arrival count.
//   - The stream survives leadership changes of this process: subscriptions
//     live on the coordinator, not on the raft role.
struct MetaSubscriptionStart {
  MetaCommittedView view_;
  std::uint64_t cursor_ = 0;
  std::unique_ptr<MetaCommitSubscription> subscription_;
};

// ---------------------------------------------------------------------------
// ValidateProposal plugins separate leader-local validation from committed
// apply validation.
// Hooks are the landing point for operation-specific phase rules and any
// check that needs volatile context (observation freshness): they run ONLY on
// the leader, synchronously inside Propose BEFORE encoding/append, in
// registration order, and the first non-OK status aborts the proposal with
// that status — nothing is appended, no audit record is written. What a hook
// approves must be baked into the command as an immutable evidence summary;
// apply never re-checks hooks. Hooks receive the same atomic view Propose
// used for its fail-safe gates and the leader-local observation store (read
// it only on the coordinator's owner thread, see the file header). The final
// argument is one wall-clock cut shared by every hook for that proposal, so
// TTL/deadline outcomes cannot depend on hook registration order.
// ---------------------------------------------------------------------------

using MetaValidateHook = std::function<absl::Status(
    const MetaCommand&, const MetaCommittedView&, const MetaObservationStore&,
    std::int64_t proposal_now_unix_ms)>;

// ---------------------------------------------------------------------------
// RunAsLeader: reconciler lifecycle.
// ---------------------------------------------------------------------------

class MetaReconciler;

// The leader-scoped facade a reconciler sees. Propose uses the coordinator's
// internal trusted principal (options.coordinator_principal_). The context
// reference is valid from Start() until CancelAndWait() returns; a reconciler
// must not touch it afterwards. Propose on a context whose leadership was
// lost fails with NOT_LEADER at the raft layer like any other proposal.
class MetaLeaderContext {
 public:
  bycorf::Task<absl::StatusOr<MetaApplyResult>> Propose(MetaCommand command);
  MetaCommittedView CommittedView();
  // O(1) applied cursor, including Raft configurations without commit events.
  // A changed cursor requires a fresh CommittedView before publishing a cut.
  std::uint64_t AppliedIndex() const;
  MetaSubscriptionStart SubscribeCommitted(MetaCommitCallback callback,
                                           std::size_t queue_capacity = 0);
  const MetaObservationStore& Observations() const;

 private:
  friend class MetaCoordinator;
  MetaLeaderContext(MetaCoordinator& coordinator, AuthenticatedPrincipal actor)
      : coordinator_(&coordinator), actor_(std::move(actor)) {}
  MetaCoordinator* coordinator_;
  AuthenticatedPrincipal actor_;
};

// A leader-scoped control loop. Start() is called on the coordinator's
// leadership thread after BecomeLeader (which NuRaft's
// wait_for_sm_catchup_on_becoming_leader_ gates until the SM has caught up:
// the reconciler's first CommittedView already reflects the re-confirmed
// prefix). Start must return quickly — spawn the real work on the
// reconciler's own thread/coroutine. CancelAndWait() runs on BecomeFollower
// (and during coordinator teardown) and must not return until the reconciler
// has fully stopped touching the context. Calls are strictly serialized per
// reconciler: Start, then CancelAndWait, then possibly Start again.
//
// Idempotency contract: reconcilers advance ONLY through Propose;
// operation idempotency keys, expected_revision CAS, and the apply layer's
// replay idempotency make retries and restarts safe. After any leadership
// change the reconciler rebuilds from CommittedView() and must tolerate
// finding its previous work already committed.
class MetaReconciler {
 public:
  virtual ~MetaReconciler() = default;
  virtual void Start(MetaLeaderContext& context) = 0;
  virtual void CancelAndWait() = 0;
};

// ---------------------------------------------------------------------------
// MetaCoordinator
// ---------------------------------------------------------------------------

struct MetaCoordinatorOptions {
  // Fail-safe gates are constructor-injected so tests exercise them
  // with tiny thresholds):
  //   Propose returns kResourceExhausted while the WAL holds more than this
  //   many uncompacted bytes (i.e. a snapshot/compaction is outstanding).
  std::uint64_t max_uncompacted_wal_bytes_ = kMaxMetaUncompactedWalBytes;
  //   ... or when consecutive snapshot write failures have REACHED this
  //   count (0 = no tolerance: always gate; 3 = gate at 3 failures).
  std::uint64_t max_consecutive_snapshot_failures_ = 3;
  // The audit gate itself has no numeric knob: it reads the committed
  // store's capacity and reserves headroom until NuRaft resolves each append,
  // including after a caller-side uncertain timeout.
  std::size_t default_subscription_capacity_ = 1024;
  // Propose round-trip timeout. NuRaft's async_handler return method has NO
  // client-side timeout (only its blocking mode enforces
  // client_req_timeout_), so the seam bounds the wait itself: on expiry the
  // proposal resolves kDeadlineExceeded with the uncertain-outcome message;
  // a later raft completion for the same entry is dropped (first-wins).
  std::uint64_t propose_timeout_ms_ = 5000;
  // Principal the LeaderContext mints for reconciler proposals.
  std::string coordinator_principal_ = "lavik://meta/coordinator";
  // Non-owning executor shared with ctl membership/snapshot operations in
  // production. The caller must keep it alive until coordinator destruction.
  // When omitted, the coordinator owns a private executor for component tests.
  MetaProposalExecutor* proposal_executor_ = nullptr;
  // Required whenever the coordinator is attached to a raft_server. Every
  // in-flight proposal retains a copy because a late completion can outlive
  // the caller-side timeout and coordinator.
  bycorf::ForeignExecutor foreign_executor_{};
  // Component tests without a Bycorf runtime must opt in explicitly. Production
  // assembly must never enable this or NuRaft/timer threads could run
  // worker-owned continuations inline.
  bool inline_resume_for_testing_ = false;
};

class MetaCoordinator {
 public:
  // `server` may be null when only the committed-view/subscription side is
  // needed (assembly ordering, component tests); Propose then fails fast with
  // kFailedPrecondition. See the file header for the ownership and teardown
  // contract. The constructor attaches the commit-event sink to the state
  // machine and starts the dispatch, leadership, and propose-timer threads. It
  // throws std::invalid_argument when a non-null server has neither a foreign
  // executor nor the explicit component-test inline policy.
  MetaCoordinator(nuraft::ptr<nuraft::raft_server> server,
                  MetaStateMachine& state_machine, NuraftLogStore& log_store,
                  MetaObservationStore& observations,
                  MetaCoordinatorOptions options = {});
  ~MetaCoordinator();
  MetaCoordinator(const MetaCoordinator&) = delete;
  MetaCoordinator& operator=(const MetaCoordinator&) = delete;

  // The one write path. Returns the committed apply outcome carried directly
  // by the state-machine completion (independent of audit retention) — or a
  // status:
  //   - kFailedPrecondition: not the leader (the message carries the known
  //     leader id/endpoint when Raft knows one), no server attached, or a
  //     ValidateProposal hook rejection (hook status propagated verbatim).
  //   - kResourceExhausted: executor/full strict-audit or durability gates
  //     (uncompacted WAL, consecutive snapshot failures).
  //   - kDeadlineExceeded / kCancelled / kInternal: the raft round timed out,
  //     was cancelled (shutdown/leadership loss), or failed. These are
  //     UNCERTAIN OUTCOMES: the command may still have committed.
  //     The message says so; the caller reconciles against CommittedView()
  //     using the command's idempotency key instead of assuming failure —
  //     safe because every command is replay/idempotency-safe by design.
  bycorf::Task<absl::StatusOr<MetaApplyResult>> Propose(
      MetaCommand command, AuthenticatedPrincipal principal);

  // Registers a ValidateProposal plugin (see the MetaValidateHook contract).
  // Call during assembly, before the coordinator can go leader; not
  // thread-safe against in-flight Propose calls.
  void AddValidateHook(MetaValidateHook hook);

  // One atomic read of the committed aggregate (see MetaCommittedView).
  MetaCommittedView CommittedView();

  // O(1) full applied cursor, including configuration commits that do not
  // change MetaStores or notify subscribers. This is a freshness hint, not
  // an atomic pairing with a separately captured store snapshot.
  std::uint64_t AppliedIndex() const;

  // O(1) MetaStores-change watermark published synchronously by command apply
  // and snapshot install. Unlike last_commit_index this excludes Raft
  // configuration entries, which cannot change a Data projection. Session
  // lease gates use it to detect a committed store change before an
  // asynchronous subscription callback reaches their worker.
  std::uint64_t CommittedHighWater() const;

  // See MetaSubscriptionStart for the delivery contract. queue_capacity == 0
  // selects options_.default_subscription_capacity_.
  MetaSubscriptionStart SubscribeCommitted(MetaCommitCallback callback,
                                           std::size_t queue_capacity = 0);

  // Read-only access to the leader-local observation store (already
  // query-time filtered and internally synchronized by the store).
  const MetaObservationStore& Observations() const { return observations_; }

  // Registers a reconciler to run while this node leads (see MetaReconciler
  // for the lifecycle contract). Registration while already leader starts the
  // reconciler without waiting for a new transition.
  void RunAsLeader(std::shared_ptr<MetaReconciler> reconciler);

  // Raft role edges, wired from NuRaft's init_options::raft_callback_
  // (BecomeLeader/BecomeFollower). O(1), non-blocking, safe from NuRaft
  // callback threads. Every edge is queued in arrival order: in particular, a
  // Follower edge is an uncancellable barrier whose CancelAndWait and volatile
  // observation reset finish before a later Leader edge may restart work. The
  // seam relies on transition EVENTS, not state polling; process assembly uses
  // MetaLeadershipRelay below so edges racing coordinator attachment are not
  // lost.
  void BecomeLeader();
  void BecomeFollower();

 private:
  friend class MetaCommitSubscription;
  // Coroutine-frame RAII decrement of the proposal in-flight counter
  // (coordinator.cpp).
  class InFlightGuard;

  void InFlightEnter();
  void InFlightLeave();
  absl::Status NotLeaderStatus() const;

  // The (stores, applied index) atomic capture behind CommittedView and
  // SubscribeCommitted: two-phase retry read that never holds the
  // subscription core mutex across a state machine call (lock order, see the
  // file header). high_water receives the sink-tracked coverage mark (the
  // subscription cursor; normally == applied_index, see
  // MetaSubscriptionStart for the snapshot-install case).
  MetaStores AtomicStoresSnapshot(std::uint64_t& applied_index,
                                  std::uint64_t& high_water);

  void DispatchMain();
  void LeadershipMain();

  nuraft::ptr<nuraft::raft_server> server_;  // refcounted; may be null
  MetaStateMachine& state_machine_;
  NuraftLogStore& log_store_;
  MetaObservationStore& observations_;
  const MetaCoordinatorOptions options_;
  // Declared before the observing pointer so the fallback owner outlives it.
  std::unique_ptr<MetaProposalExecutor> owned_proposal_executor_;
  MetaProposalExecutor* proposal_executor_;

  std::vector<MetaValidateHook> hooks_;  // assembly-time only

  // Coroutine lifetime accounting for teardown. Proposal headroom and
  // fail-safe recovery have a separate shared gate whose reservations follow
  // the Raft completion, including after a caller-visible timeout.
  std::mutex gate_mu_;
  std::condition_variable gate_cv_;
  std::uint64_t in_flight_ = 0;
  std::shared_ptr<MetaProposalGate> proposal_gate_;

  std::atomic<bool> stopping_{false};

  // Proposal deadline enforcement (see options_.propose_timeout_ms_). The
  // opaque machinery lives in coordinator.cpp and holds only weak
  // references to proposal waiters, so it is teardown-safe.
  std::unique_ptr<MetaProposeTimer> propose_timer_;

  // Shared with live subscription handles so a handle may outlive the
  // coordinator (it then observes cancelled()==true).
  std::shared_ptr<MetaSubscriptionCore> sub_core_;
  std::thread dispatch_thread_;

  // Leadership state: callers only append to the event queue. The leadership
  // thread owns reconcilers_ and applied_leader_, and performs every
  // Start/CancelAndWait call without holding leadership_mu_. This keeps Raft
  // callbacks O(1) even while a reconciler is draining.
  enum class LeadershipEventKind : std::uint8_t {
    kRegister,
    kBecomeLeader,
    kBecomeFollower,
  };
  struct LeadershipEvent {
    LeadershipEventKind kind_;
    std::shared_ptr<MetaReconciler> reconciler_;
  };
  std::mutex leadership_mu_;
  std::condition_variable leadership_cv_;
  std::deque<LeadershipEvent> leadership_events_;
  bool applied_leader_ = false;
  struct ReconcilerEntry {
    std::shared_ptr<MetaReconciler> reconciler_;
    bool started_ = false;
  };
  std::vector<ReconcilerEntry> reconcilers_;
  std::thread leadership_thread_;
  MetaLeaderContext leader_context_;
};

// Process-wiring bridge between NuRaft role callbacks and MetaCoordinator.
// The callback records its exact edge synchronously, then asks the Bycorf
// worker to Drain; this preserves callback order even if worker notifications
// are delayed or coalesced. NuRaft can emit edges before the coordinator is
// assembled, so Attach drains the retained prefix too. DetachAndStop is a
// lifetime/order barrier: after it returns no callback can enqueue into the old
// coordinator. Its target is non-owning and must remain alive from Attach
// through DetachAndStop.
class MetaLeadershipRelay {
 public:
  void RecordLeaderEdge() noexcept;
  void RecordFollowerEdge() noexcept;
  // Worker-side drain; a pre-attach call leaves the events retained.
  void Drain() noexcept;
  void Attach(MetaCoordinator& coordinator);
  void DetachAndStop() noexcept;

 private:
  enum class Role : std::uint8_t { kLeader, kFollower };
  void Record(Role role) noexcept;
  static void Forward(MetaCoordinator& coordinator, Role role);

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Role> pending_;
  MetaCoordinator* target_ = nullptr;
  bool attached_once_ = false;
  bool draining_ = false;
  bool stopped_ = false;
};

}  // namespace lavik::meta
