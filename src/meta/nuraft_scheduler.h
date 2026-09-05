#pragma once

// NuRaft delayed_task_scheduler on celer timers, plus the foreign-thread
// ingress bridge every meta-transport adapter shares.
//
// THREAD MODEL (the load-bearing decision of this adapter)
//
// NuRaft calls schedule()/cancel() from its own threads (the bg commit/append
// threads and internal task threads), never from a celer worker. celer has no
// public foreign-thread submission path, and its fast paths are provably
// unsafe from foreign threads:
//   - cross_core SubmitTo/SubmitTaskTo/PostRequest read ThisWorker() TLS that
//     is only populated by SetThisWorker inside Worker::Run
//     (celer/src/runtime/worker.cpp:1140). On a foreign thread
//     MarkWakeWorker indexes an empty wake_pending_ vector
//     (celer/include/celer/runtime/cross_core.h:265-280): undefined behavior.
//   - the per-sender SPSC lane (cross_core.h:73-119) is single-producer;
//     several NuRaft threads would race on tail_.
//   - the MPSC mailbox overflow queue (WorkerMailbox::requests_) IS
//     multi-producer-safe, but Worker::cross_core_ is private with no
//     accessor, so it is unreachable without modifying celer.
//
// MetaCelerBridge therefore implements ingress with a mutex-protected inbox
// plus a non-blocking socketpair wakeup, using only public celer APIs:
//   - NuRaft thread: Post(fn) pushes fn under a mutex and writes one byte to
//     the socketpair write end (SOCK_NONBLOCK; EAGAIN means a wakeup is
//     already pending and is dropped — every wakeup drains the WHOLE inbox,
//     so coalescing is safe). Post never blocks and never runs fn inline.
//   - worker thread: Run() registers the socketpair read end as a Connection
//     (mirroring ConnectTcp's registration, tcp_stream.cpp:722-734) and loops
//     on TcpStream::ReadSome; each wake drains the inbox and executes the
//     closures as fn(worker). Closures must be O(1) and non-blocking: they
//     run inline in the drain loop.
//
// INVARIANTS
//   - All celer object access (Worker, timers, streams) happens on the single
//     worker that runs Run(); NuRaft threads only ever touch Post()/Stop().
//   - The bridge is shared_ptr-owned; Run's coroutine frame holds a reference,
//     so the bridge outlives its drain loop. Adapters (scheduler, client
//     factory, listener) each hold their own reference.
//   - Posts accepted before Stop() are guaranteed to execute (the stop wakeup
//     is just another inbox drain); Posts after Stop() are dropped. Dropping
//     only happens during teardown, matching asio_service's io_svc.stop()
//     behavior of abandoning queued work.
//
// FAILURE BEHAVIOR
//   - Stop() is idempotent and foreign-thread safe. Destruction requires that
//     no thread can Post any more (trivially true once the NuRaft adapters
//     holding shared_ptrs are gone) and that the worker has quiesced; the
//     drain loop also exits on its own when the worker shuts down, because
//     Worker::Run's teardown closes every registered Connection
//     (worker.cpp:1151-1158), which fails the pending ReadSome.
//
// SCHEDULER SEMANTICS (contract with NuRaft)
//   - schedule(task, ms) first clears the task's cancelled flag via
//     task->reset(): NuRaft reuses delayed_task objects across
//     cancel/schedule cycles (asio_service.cxx:2366-2382 does the same).
//   - Re-arming a task with a pending fire cancels the previous fire first,
//     mirroring asio's expires_after on the reused steady_timer.
//   - cancel_impl invokes TimerCancelHandle::Cancel(), which is explicitly
//     safe from any thread (celer/include/celer/io/storage.h:166-186): from a
//     foreign thread it only sets an atomic flag and the pending fire then
//     resolves as kCancelled at its original deadline
//     (storage.cpp:307-315 — the cancelled flag wins over a raced CQE). The
//     coroutine then skips execution, which is exactly NuRaft's two-phase
//     cancel (delayed_task_scheduler::cancel = cancel_impl + task->cancel();
//     execute() re-checks the flag). The cancel handle lives in the task's
//     impl_ctx_, so NuRaft frees it when the task dies; a pending timer
//     coroutine holds its own shared_ptr copy, so task destruction never
//     strands the coroutine.
//   - The task's exec() runs on the celer worker thread. NuRaft's timer
//     callbacks only take its internal locks, so this matches the asio
//     threading model.

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
// NuRaft's headers are not -Wpedantic-clean (__nocopy__/__interface_body__
// leave an extra ';'); they are pinned third-party sources, so silence the
// warning at the include site instead of patching the submodule.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/delayed_task.hxx"
#include "libnuraft/delayed_task_scheduler.hxx"
#include "libnuraft/ptr.hxx"
#pragma GCC diagnostic pop

namespace celer {
class Worker;
}

namespace keylane::meta {

// Foreign-thread -> celer worker mailbox; see the file-level comment for the
// threading contract. One bridge is typically shared by the scheduler, the
// RPC client factory, and the RPC listener of one meta Raft instance.
class MetaCelerBridge : public std::enable_shared_from_this<MetaCelerBridge> {
 public:
  // Executed on the worker thread by the drain loop. Must be O(1) and never
  // block or suspend.
  using WorkItem = std::function<void(celer::Worker&)>;

  // Creates the wakeup socketpair. No celer objects are touched, so this is
  // safe on any thread and before any worker exists.
  static absl::StatusOr<std::shared_ptr<MetaCelerBridge>> Create();

  // Safe once no thread can call Post()/Stop() any more; see the file-level
  // comment for the teardown contract.
  ~MetaCelerBridge();

  MetaCelerBridge(const MetaCelerBridge&) = delete;
  MetaCelerBridge& operator=(const MetaCelerBridge&) = delete;

  // Enqueue work for the worker. Callable from ANY thread (this is the whole
  // point), never blocks, never runs the item inline. Items posted after
  // Stop() are silently dropped.
  void Post(WorkItem work);

  // Drain loop. Spawn it on the chosen worker from that worker's thread
  // (e.g. WorkerMain: `worker.Spawn(bridge->Run(worker));`). Items posted
  // before Run starts are delivered once it does. Completes after Stop() or
  // when the worker shuts down.
  celer::Task<absl::Status> Run(celer::Worker& worker);

  // Idempotent, any thread. The drain loop exits after delivering everything
  // posted before the stop; later Posts are dropped.
  void Stop() noexcept;
  bool stopped() const noexcept {
    return stopped_.load(std::memory_order_acquire);
  }

 private:
  explicit MetaCelerBridge(int wake_read_fd, int wake_write_fd) noexcept
      : wake_read_fd_(wake_read_fd), wake_write_fd_(wake_write_fd) {}

  void Wake() noexcept;
  void DrainInbox(celer::Worker& worker);  // worker thread only

  // wake_read_fd_ is owned by *this until Run() hands it to the worker's
  // connection table (indicated by resetting it to -1).
  int wake_read_fd_ = -1;
  int wake_write_fd_ = -1;
  std::mutex inbox_mu_;
  std::deque<WorkItem> inbox_;  // guarded by inbox_mu_
  std::atomic<bool> stopped_{false};
};

// NuRaft delayed_task_scheduler backed by per-schedule celer cancellable
// timers on the bridge's worker. schedule()/cancel() may be called from any
// NuRaft thread; all timer mechanics run on the worker via the bridge.
class NuraftDelayedTaskScheduler : public nuraft::delayed_task_scheduler {
 public:
  explicit NuraftDelayedTaskScheduler(std::shared_ptr<MetaCelerBridge> bridge)
      : bridge_(std::move(bridge)) {}

  void schedule(nuraft::ptr<nuraft::delayed_task>& task,
                nuraft::int32 milliseconds) override;

 private:
  void cancel_impl(nuraft::ptr<nuraft::delayed_task>& task) override;

  std::shared_ptr<MetaCelerBridge> bridge_;
};

}  // namespace keylane::meta
