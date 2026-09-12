#pragma once

// NuRaft's public mutation APIs may synchronously enter storage or internal
// locks before returning their asynchronous result handle. Meta ingress runs
// on a single celer worker, so those calls must cross this bounded executor:
// the worker only queues work and later resumes through celer::ForeignExecutor.

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "absl/status/status.h"

namespace keylane::meta {

class MetaProposalExecutor {
 public:
  // Work owns its completion state and must translate expected exceptions
  // into that task's failure result. Letting an exception escape is fail-fast:
  // silently abandoning a waiter would wedge control-plane progress.
  using Work = std::function<void()>;

  explicit MetaProposalExecutor(std::size_t capacity = 1024);
  ~MetaProposalExecutor();
  MetaProposalExecutor(const MetaProposalExecutor&) = delete;
  MetaProposalExecutor& operator=(const MetaProposalExecutor&) = delete;

  // Enqueues without waiting. Accepted work is drained during Shutdown;
  // overload is reported to ingress instead of blocking the celer worker.
  absl::Status Submit(Work work);
  void Shutdown() noexcept;

 private:
  void Run() noexcept;

  const std::size_t capacity_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Work> queue_;
  bool accepting_ = true;
  std::thread thread_;
};

// NuRaft accepts only one membership change at a time. Initial cluster creation
// shares this gate so membership cannot race its singleton lifecycle admission.
// Every Admin listener and both workflow reconcilers share one gate. After
// intent submission the background owner holds the lease; the durable Creating
// lifecycle bridges handoff and restart. No mutex is held across suspension.
class MetaMembershipGate
    : public std::enable_shared_from_this<MetaMembershipGate> {
 public:
  class Lease {
   public:
    ~Lease();
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;

   private:
    friend class MetaMembershipGate;
    explicit Lease(std::shared_ptr<MetaMembershipGate> gate)
        : gate_(std::move(gate)) {}
    std::shared_ptr<MetaMembershipGate> gate_;
  };

  // Does not wait for the active workflow: a null lease means the caller must
  // reject the request before making any committed mutation.
  std::unique_ptr<Lease> TryAcquire();

 private:
  void Release() noexcept;
  std::mutex mu_;
  bool held_ = false;
};

}  // namespace keylane::meta
