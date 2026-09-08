#pragma once

// MetaStateMachine is the metadata control plane's NuRaft `state_machine`.
// It owns the committed aggregate `MetaStores` (seven stores) and applies every
// committed entry through
// the deterministic pure function ApplyCommitted (see state_apply.h).
//
// commit() and failure classification:
//   - commit() decodes the entry with the committed command codec. A DECODE
//   FAILURE
//     IS FAIL-STOP (spdlog::critical + abort, the system_exit policy): the
//     same byte sequence fails identically on every node, so aborting cannot
//     fork the group. This is strictly separate from a domain rejection,
//     which is a cleanly decoded command refused by ApplyCommitted: the index
//     is consumed, an audit record written, committed state unchanged, and
//     the commit thread keeps going.
//   - ACTOR SOURCING: ApplyCommitted takes the trusted entry's injected
//     ActorContext fields (actor_principal, readable_time) and copies them
//     into the audit record. commit() reads them from the decoded command's
//     `actor_` field — the wire encoding carries both as ordinary bounded
//     strings (commands.h), so every node applies with the same actor.
//     Unforgeability is an entry-layer property (only the trusted
//     ctl/coordinator entries construct commands), not this
//     codec's.
//
// Recovery and durability of last_commit_index, verified against the pinned
// NuRaft revision:
//   The stores are volatile and rebuilt by replay; durability comes from
//   snapshots. The commit index is persisted ONLY as part of a snapshot (it
//   equals the snapshot's last_log_idx). After a restart the core initializes
//   quick_commit_index_/sm_commit_index_ from last_commit_index() (the
//   snapshot index) and does NOT immediately replay the WAL tail: the tail is
//   re-confirmed through the normal leader path once the new leader's
//   current-term entry reaches quorum, and an uncommitted tail is eventually
//   either legally committed as a prefix or truncated. No node exposes tail
//   state before quorum re-confirmation.
//   Consequently commit() MAY BE INVOKED REPEATEDLY for the same index: a
//   committed entry already applied before a crash is re-applied after it.
//   Correctness rests on replay idempotency (absolute-value
//   commands + "effect already present with identical content -> idempotent
//   accept" + audit records keyed by log index), never on at-most-once
//   delivery. Persisting last_commit_index per commit would be wrong: NuRaft
//   never re-applies entries at or below last_commit_index(), so a watermark
//   newer than the durable stores would silently drop acknowledged writes.
//
// Snapshot contract:
//   - EXACT CUT POINT: create_snapshot() serializes MetaStores under the
//     state mutex synchronously (KB-scale), so the captured bytes hold the
//     state of exactly the snapshot's last_log_idx. Automatic snapshots run
//     on the commit thread at a commit boundary (handle_commit.cxx:
//     snapshot_and_compact right after sm_commit_index_ advances), and MANUAL
//     snapshots must go through create_snapshot({serialize_commit_=true}) or
//     schedule_snapshot_creation() — the ctl `snapshot` path in meta_main
//     does — so no commit can interleave between the index read and the
//     capture. Without that serialization the captured state could be newer
//     than the snapshot index (safe via replay idempotency, but not exact).
//   - ASYNC WRITE: the framed image is handed to the dedicated snapshot
//     writer thread, which performs the file IO (tmp + fdatasync + rename +
//     directory fsync) and only then invokes when_done — the commit thread
//     never blocks on durability IO. The core compacts the log only after
//     when_done(true) (on_snapshot_completed), so a durable snapshot always
//     precedes log truncation. A write failure rejects the snapshot via
//     when_done(false), which only skips this compaction round; consecutive
//     failures are counted by consecutive_snapshot_failures() for replay
//     observability and alerting.
//   - SIZE FAIL-SAFE: MetaStores::Serialize fails beyond
//     kMaxMetaSnapshotBytes; create_snapshot rejects the round and
//     alerts instead of silently truncating.
//   - TRANSMISSION: NuRaft's logical-object API; object N is the Nth
//     kSnapshotObjectBytes chunk of the snapshot's serialized MetaStores
//     envelope (bounded objects, O(1) per read). An open read stream pins its
//     snapshot against pruning until free_user_snp_ctx — pruning mid-stream
//     would make the core restart the follower's sync from object zero with
//     the newest snapshot on every catch-up round.
//   - RECEIVE SIDE: save_logical_snp_obj accumulates the envelope and writes
//     the snapshot file before apply_snapshot() loads it — a received
//     snapshot is durable before it is applied; apply_snapshot() returning
//     false is the core's designed path into state_mgr::system_exit.
//     Assembly is POSITIONALLY EXACTLY-ONCE: NuRaft has no in-flight guard on
//     install_snapshot requests (the leader resends the current sync-ctx
//     offset on every append tick until a response advances it, client
//     timeouts recreate the request on a fresh connection, and stale
//     responses can regress the ctx offset), so the same object may arrive
//     any number of times. Offset-addressed assembly absorbs duplicates, but
//     a plain byte stream cannot. The receiver appends only the
//     object whose id equals the assembly cursor receiving_next_obj_
//     (is_first_obj restarts assembly, covering the leader's sync-ctx
//     timeout/restart), acknowledges every object with the cursor value
//     (pull semantics: stale/duplicate responses can never pull the leader
//     past a hole), and writes the file only when the is_last object
//     completes the sequence — a holed stream never produces a file. The
//     framed checksum + strict envelope decode remain the backstop, not the
//     mechanism.
//
// Snapshot files (`snapshot_<last_log_idx>.dat`, all integers little-endian):
//   magic u32 ("MSN1") | meta_len u32 | meta (nuraft::snapshot::serialize()) |
//   payload_len u32 | payload (MetaStores envelope) | checksum u32
//   checksum := FNV-1a32 over every byte between magic and itself.
// The latest snapshot file is retained, plus any pinned by an open read
// stream.
//
// Threading and IO model: NuRaft calls commit/create_snapshot/read...obj on
// its commit and snapshot threads; all state is mutex-guarded. Only the
// writer thread performs create-side durability IO; receive-side writes stay
// inline on the core's snapshot-sync thread. Lock order
// is mutex_ -> io_mutex_; the writer thread never holds both in the reverse
// order.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/statusor.h"
#include "keylane/meta/state_apply.h"
#include "libnuraft/state_machine.hxx"

namespace keylane::meta {

// Commit-event sink for MetaCoordinator.
// Invoked from commit() with the applied log index and its apply result. The
// contract is strict because of WHERE it is invoked: synchronously on the
// commit thread, AFTER ApplyCommitted, while the state mutex is still held.
// The sink must therefore be O(1) and never block, never throw, and never
// call back into the state machine (that would self-deadlock) — it may only
// hand the event off (e.g. enqueue + notify). commit_config() and
// apply_snapshot() do NOT fire it: config commits carry no command, and a
// snapshot install replaces state without per-entry events (consumers resync
// via a fresh view, see coordinator.h).
using MetaCommitEventSink =
    std::function<void(std::uint64_t log_index, const MetaApplyResult&)>;

class MetaStateMachine : public nuraft::state_machine {
 public:
  // Byte granularity of the bounded logical snapshot objects streamed to
  // followers.
  static constexpr uint64_t kSnapshotObjectBytes = 64u << 10;  // 64 KiB

  // Opens `data_dir` (creating it if needed) and loads the latest snapshot,
  // if any. Without a snapshot the machine starts empty at commit index 0.
  static absl::StatusOr<std::unique_ptr<MetaStateMachine>> Open(
      const std::string& data_dir);

  ~MetaStateMachine() override;

  // Command payload encoding for proposers and tests: the committed wire
  // encoding wrapped in a NuRaft buffer.
  static absl::StatusOr<nuraft::ptr<nuraft::buffer>> EncodeCommand(
      const MetaCommand& command);

  // Atomic copy of the whole committed aggregate (the CommittedView
  // building block; KB-scale). All seven stores move together — readers never
  // observe a cross-store tear.
  MetaStores StoresSnapshot() const;

  // Targeted read-only queries of the committed state, for the ctl surface
  // (ctl_server.h getop/getnode/completeop). A full StoresSnapshot()
  // per request would deep-copy every store, which the gate drivers'
  // high-frequency checks cannot afford once the audit window grows. Same
  // non-linearizable semantics as any raw SM read: the caller gets this
  // node's currently applied state, which lags behind the leader on a
  // follower.
  std::optional<MetaOperationRecord> FindOperation(
      const MetaOperationId& id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stores_.operation_.FindOperation(id);
  }
  std::optional<MetaNodeRecord> FindNode(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stores_.identity_.FindNode(node_id);
  }
  // Consecutive create_snapshot write failures since the last success. The
  // coordinator gates further proposals at the configured threshold.
  uint64_t consecutive_snapshot_failures() const {
    return consecutive_snapshot_failures_.load();
  }

  // Blocks until the snapshot writer has finished every queued job, including
  // its when_done. PART OF THE SHUTDOWN CONTRACT: raft_server::shutdown()
  // joins the commit thread — the only producer of automatic snapshot jobs —
  // so no new jobs arrive afterwards, but an in-flight when_done must still
  // reach the core while it is alive. Callers (meta_main, tests) must run
  // shutdown() -> WaitForSnapshotWriterIdle() -> destroy the raft_server, in
  // that order, or the completion callback lands on a destroyed server.
  void WaitForSnapshotWriterIdle();

  // Registers the single commit-event sink (see MetaCommitEventSink for the
  // contract). At most one consumer — the coordinator — attaches, at assembly
  // time; passing an empty function detaches (teardown). Thread-safe against
  // the commit thread; a commit already inside the sink call completes with
  // the previous sink.
  void SetCommitEventSink(MetaCommitEventSink sink);

  nuraft::ptr<nuraft::buffer> commit(nuraft::ulong log_idx,
                                     nuraft::buffer& data) override;
  void commit_config(nuraft::ulong log_idx,
                     nuraft::ptr<nuraft::cluster_config>& new_conf) override;
  nuraft::ptr<nuraft::snapshot> last_snapshot() override;
  nuraft::ulong last_commit_index() override;
  // Highest log index that replaced or applied MetaStores. Raft membership
  // configuration entries advance last_commit_index() but not this cursor,
  // because they cannot change a Data node projection.
  nuraft::ulong state_change_index() const noexcept {
    return last_state_change_idx_.load(std::memory_order_acquire);
  }
  void create_snapshot(
      nuraft::snapshot& s,
      nuraft::async_result<bool>::handler_type& when_done) override;
  int read_logical_snp_obj(nuraft::snapshot& s, void*& user_snp_ctx,
                           nuraft::ulong obj_id,
                           nuraft::ptr<nuraft::buffer>& data_out,
                           bool& is_last_obj) override;
  void save_logical_snp_obj(nuraft::snapshot& s, nuraft::ulong& obj_id,
                            nuraft::buffer& data, bool is_first_obj,
                            bool is_last_obj) override;
  void free_user_snp_ctx(void*& user_snp_ctx) override;
  bool apply_snapshot(nuraft::snapshot& s) override;

 private:
  struct SnapshotData {
    nuraft::ptr<nuraft::snapshot> snapshot_;
    std::string envelope_;  // serialized MetaStores (the streamed payload)
  };

  // One durable-snapshot job for the writer thread: the exact-cut capture
  // taken under the state mutex in create_snapshot, plus the completion
  // handler NuRaft expects exactly once.
  struct SnapshotJob {
    uint64_t idx_ = 0;
    nuraft::ptr<nuraft::snapshot> snapshot_;
    std::string envelope_;
    std::vector<uint8_t> image_;  // fully framed file content
    nuraft::async_result<bool>::handler_type when_done_;
  };

  explicit MetaStateMachine(std::string data_dir);

  void WriterMain();

 private:
  // Caller must hold io_mutex_. Writes snapshot_<idx>.dat from the framed
  // image (tmp file + fdatasync + rename + directory fsync).
  absl::Status WriteSnapshotImage(uint64_t log_idx,
                                  const std::vector<uint8_t>& image);
  // Takes io_mutex_ itself (safe with mutex_ held: lock order is
  // mutex_ -> io_mutex_). Loads snapshot_<idx>.dat: validates framing, then
  // sets `data` (the MetaStores envelope stays undecoded — callers
  // deserialize it so every decode failure is their fail-stop-class error
  // path).
  absl::Status ReadSnapshotFileLocked(uint64_t log_idx, SnapshotData& data);
  // Caller must hold mutex_. Drops snapshot files and cached copies other
  // than `keep_idx`, except snapshots pinned by an in-flight read stream.
  void PruneSnapshotsLocked(uint64_t keep_idx);

  const std::string data_dir_;

  mutable std::mutex mutex_;
  MetaStores stores_;
  // The coordinator commit-event sink (see MetaCommitEventSink). Own mutex,
  // always taken AFTER mutex_ (lock order mutex_ -> sink_mutex_), so
  // registration and teardown never block the commit thread on more than a
  // pointer swap.
  std::mutex sink_mutex_;
  MetaCommitEventSink commit_event_sink_;
  // Snapshots keyed by last log index; the latest is retained, plus any
  // still pinned by an in-flight read stream.
  std::map<uint64_t, SnapshotData> snapshots_;
  // Open read streams by snapshot log index (see PruneSnapshotsLocked and the
  // header's transmission contract).
  std::map<uint64_t, uint32_t> pinned_snapshots_;
  nuraft::ptr<nuraft::snapshot> last_snapshot_;
  // Receive-side accumulation for an in-flight install_snapshot.
  std::string receiving_bytes_;
  uint64_t receiving_idx_ = 0;
  // Assembly cursor: count of objects appended so far; the next object to
  // accept (see the header's receive-side contract).
  uint64_t receiving_next_obj_ = 0;

  std::atomic<uint64_t> last_committed_idx_{0};
  std::atomic<uint64_t> last_state_change_idx_{0};
  std::atomic<uint64_t> consecutive_snapshot_failures_{0};

  // Serializes snapshot file IO between the writer thread and the
  // receive-side path (both write snapshot_<idx>.dat via the same tmp name).
  std::mutex io_mutex_;

  // Dedicated snapshot writer: create_snapshot hands it the framed image and
  // it performs the durability IO off the commit thread (see the header).
  std::mutex writer_mutex_;
  std::condition_variable writer_cv_;
  std::queue<SnapshotJob> writer_queue_;
  bool writer_stopping_ = false;
  // Idle signal for WaitForSnapshotWriterIdle: set while a job (including its
  // when_done) is in flight.
  std::condition_variable writer_idle_cv_;
  bool writer_job_running_ = false;
  std::thread writer_thread_;
};

}  // namespace keylane::meta
