#pragma once

// NuraftStateMgr: NuRaft `state_mgr` with on-disk server state, cluster
// config, and log store for the issue-#19 metadata control plane.
//
// Owns the three pieces of Raft metadata durability in one data directory:
//   - `raft_log.dat`        log store, owned via NuraftLogStore
//   - `srv_state.dat`       current term / voted_for / catch-up flags
//   - `cluster_config.dat`  last saved cluster configuration
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
//   - A first boot (no files yet) returns nullptr from read_state() and a
//     one-server initial cluster_config containing only this server, as the
//     core requires.
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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "absl/status/statusor.h"
#include "libnuraft/buffer.hxx"
#include "libnuraft/state_mgr.hxx"

namespace keylane::meta {

class NuraftLogStore;

class NuraftStateMgr : public nuraft::state_mgr {
 public:
  // Opens `data_dir` (creating it if needed) and loads any previously
  // persisted state. `endpoint` is only used to build the first-boot
  // one-server cluster config; once a config has been saved it is read back
  // from disk instead.
  static absl::StatusOr<std::unique_ptr<NuraftStateMgr>> Open(
      const std::string& data_dir, int32_t server_id,
      const std::string& endpoint);

  nuraft::ptr<nuraft::cluster_config> load_config() override;
  void save_config(const nuraft::cluster_config& config) override;
  void save_state(const nuraft::srv_state& state) override;
  nuraft::ptr<nuraft::srv_state> read_state() override;
  nuraft::ptr<nuraft::log_store> load_log_store() override;
  nuraft::int32 server_id() override;
  void system_exit(int exit_code) override;

 private:
  NuraftStateMgr(std::string data_dir, int32_t server_id,
                 nuraft::ptr<NuraftLogStore> log_store,
                 nuraft::ptr<nuraft::srv_state> initial_state,
                 nuraft::ptr<nuraft::cluster_config> initial_config);

  // Serializes `blob` to `name` inside the data directory with
  // tmp-write/fdatasync/rename/dir-fsync; aborts the process on IO errors.
  void WriteFileAtomically(const std::string& name, const nuraft::buffer& blob,
                           const char* what);

  const std::string data_dir_;
  const int32_t server_id_;

  std::mutex mutex_;
  nuraft::ptr<NuraftLogStore> log_store_;
  nuraft::ptr<nuraft::srv_state> state_;
  nuraft::ptr<nuraft::cluster_config> config_;
};

}  // namespace keylane::meta
