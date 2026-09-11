#pragma once

// NuraftStateMgr: NuRaft `state_mgr` with on-disk server state, cluster
// config, and log store for the metadata control plane.
//
// Owns Raft metadata durability in one data directory:
//   - `log-<first_idx>.seg` WAL v1 segment set, owned via NuraftLogStore
//   - `srv_state.dat`       current term / voted_for / catch-up flags
//   - `cluster_config.dat`  last saved cluster configuration
//   - `initial_bindings.dat` immutable genesis descriptor set while its
//                            committed identity projection is incomplete
//   - `initial_bindings_complete.dat`
//                            durable tombstone that prevents a completed
//                            zero-index genesis from reopening that window
//   - `waiting_joiner.dat`  election-disabled lifecycle marker, present only
//                           until a future member applies its committed config
//   - `raft_started.dat`    proves NuRaft began persisting consensus state, so
//                           loss of vote/WAL files cannot look pristine
//   - `transport_bindings.dat`
//                            exact config descriptor baseline and replay
//                            watermark for ordinary process restart
//
// Durability contract with the Raft core:
//   - save_state() must be durable before it returns: the core persists
//     term/voted_for through this call right before answering a vote request
//     (handle_vote.cxx), and a lost vote after it was granted can cause two
//     leaders in one term (split brain). Every save therefore goes
//     tmp-file + fdatasync + rename + directory fsync, and only then returns.
//   - save_config() uses the same atomic-rename discipline: the core saves
//     the config when a conf log commits, and load_config() must never
//     resurrect a configuration older than the last committed one.
//   - Recovery order is fixed by raft_server's constructor:
//     load_log_store() -> load_config() -> read_state() ->
//     state_machine::last_commit_index() -> last_snapshot(). This class
//     opens the log store at Open() time, so every later load_log_store()
//     returns the same shared instance.
//   - A pristine first boot atomically persists either the complete genesis
//     config supplied by the initial manifest or a one-server placeholder
//     for an election-disabled future joiner. The manifest is never consulted
//     after this boundary.
//
// system_exit(): NuRaft reports unrecoverable internal errors through this
// hook (N16/N19/N20/N21/N23, e.g. log flush failure or commit-order
// inversion; see libnuraft/error_code.hxx). The fail-stop policy is uniform:
// log the code at critical level and abort(). A Raft core that lost confidence
// in its own consistency must not keep serving.
//
// Threading and IO model: identical to NuraftLogStore — synchronous,
// mutex-serialized file IO on whatever NuRaft thread made the call. NuRaft
// invokes save_state/save_config from its request/background threads, which
// may block on storage; durability IO stays out of celer coroutines.
//
// Failure behavior: Open() reports via absl::Status. Inside the NuRaft
// overrides there is no error channel and proceeding after a failed
// term/vote/config write is unsafe, so any IO error there logs fatal and
// aborts (same as system_exit).

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "libnuraft/buffer.hxx"
#include "libnuraft/state_mgr.hxx"

namespace keylane::meta {

class NuraftLogStore;

// Complete durable descriptor for one Raft member. The Raft endpoint lives in
// NuRaft's native srv_config field; the remaining fields are encoded in aux.
struct NuraftMemberConfig {
  std::int32_t server_id_ = 0;
  std::string raft_endpoint_;
  std::string principal_;
  std::string data_control_endpoint_;
  std::string ctl_endpoint_;
  bool operator==(const NuraftMemberConfig&) const = default;
};

// Durable lifecycle selected by Open(). Initial-cluster nodes may elect from
// the complete genesis config, ordinary restarts trust only durable state, and
// waiting joiners remain election-disabled until an existing leader adds them.
enum class NuraftStartupMode {
  kInitialCluster,
  kRestart,
  kWaitingJoiner,
};

struct NuraftStateMgrOpenOptions {
  std::string data_dir_;
  // Describes this process's stable id/principal and local listener binds.
  // The initial vector, then cluster_config.dat, owns durable advertised
  // endpoints; Data-control and ctl routes may intentionally name proxies.
  NuraftMemberConfig local_member_;
  // Presence means an explicit initial-cluster request, even if the vector is
  // invalid or empty. Absence selects restart-or-waiting-joiner behavior.
  std::optional<std::vector<NuraftMemberConfig>> initial_cluster_;
};

class NuraftStateMgr : public nuraft::state_mgr {
 public:
  // Classifies `data_dir` before creating any Raft transport. Genesis is
  // accepted only for a pristine directory; restart requires a valid local
  // config; a pristine open without a manifest creates a waiting joiner.
  static absl::StatusOr<std::unique_ptr<NuraftStateMgr>> Open(
      NuraftStateMgrOpenOptions options);

  // Fixed classification for this process lifetime; completing a lifecycle
  // marker changes restart behavior but does not mutate the current mode.
  NuraftStartupMode startup_mode() const { return startup_mode_; }
  // Remains true across election-time config copies and process restarts until
  // the membership reconciler confirms every genesis identity binding. A real
  // membership change also clears it before its descriptor can authorize
  // transport.
  bool initial_bindings_pending() const {
    return initial_bindings_pending_.load(std::memory_order_acquire);
  }
  // Atomically publishes the exact transport descriptor baseline and closes
  // genesis grace. `applied_index` must be the non-zero state-machine index at
  // which all genesis bindings are visible. I/O failure leaves a recoverable
  // marker prefix and is returned to the caller.
  absl::Status CompleteInitialBindings(std::uint64_t applied_index);
  // A waiting joiner may temporarily authenticate configured peers while its
  // state machine replays the identity bindings preceding the add-server
  // config entry. This durable boundary, unlike a config-index heuristic,
  // survives a crash without granting grace to ordinary dynamic configs.
  bool waiting_joiner_catchup_pending() const {
    return waiting_joiner_marker_.load(std::memory_order_acquire);
  }
  // Closes waiting-joiner grace only after the installed config includes the
  // local identity, all of its bindings are visible, and the state machine has
  // applied at least that config index. It returns FailedPrecondition for an
  // early/zero/pre-add boundary and preserves the marker on I/O failure so
  // restart can resume convergence.
  absl::Status CompleteWaitingJoinerCatchup(std::uint64_t applied_index);
  // Authorizes only descriptors in the last fully converged config, and only
  // while the local state machine is replaying below its durable watermark.
  // A config descriptor change invalidates the old baseline transactionally.
  bool transport_binding_replay_pending(std::uint64_t applied_index) const;

  nuraft::ptr<nuraft::cluster_config> load_config() override;
  void save_config(const nuraft::cluster_config& config) override;
  void save_state(const nuraft::srv_state& state) override;
  nuraft::ptr<nuraft::srv_state> read_state() override;
  nuraft::ptr<nuraft::log_store> load_log_store() override;
  nuraft::int32 server_id() override;
  void system_exit(int exit_code) override;

 private:
  NuraftStateMgr(std::string data_dir, int32_t server_id,
                 NuraftStartupMode startup_mode,
                 nuraft::ptr<NuraftLogStore> log_store,
                 nuraft::ptr<nuraft::srv_state> initial_state,
                 nuraft::ptr<nuraft::cluster_config> initial_config,
                 nuraft::ptr<nuraft::cluster_config> initial_binding_config,
                 nuraft::ptr<nuraft::cluster_config> transport_binding_config,
                 std::uint64_t transport_binding_index,
                 bool raft_started_marker);

  absl::Status PublishTransportBindingBaselineLocked(
      const nuraft::cluster_config& config, std::uint64_t applied_index,
      bool transactional_with_config);

  // Serializes `blob` to `name` inside the data directory with
  // tmp-write/fdatasync/rename/dir-fsync; aborts the process on IO errors.
  void WriteFileAtomically(const std::string& name, const nuraft::buffer& blob,
                           const char* what);

  const std::string data_dir_;
  const int32_t server_id_;
  const NuraftStartupMode startup_mode_;

  mutable std::mutex mutex_;
  nuraft::ptr<NuraftLogStore> log_store_;
  nuraft::ptr<nuraft::srv_state> state_;
  nuraft::ptr<nuraft::cluster_config> config_;
  nuraft::ptr<nuraft::cluster_config> initial_binding_config_;
  nuraft::ptr<nuraft::cluster_config> transport_binding_config_;
  std::uint64_t transport_binding_index_ = 0;
  bool raft_started_marker_ = false;
  std::atomic<bool> initial_bindings_pending_{false};
  std::atomic<bool> waiting_joiner_marker_{false};
};

}  // namespace keylane::meta
