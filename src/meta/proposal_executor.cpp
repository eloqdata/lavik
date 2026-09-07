#include "keylane/meta/proposal_executor.h"

#include <utility>

#include "absl/status/status.h"

namespace keylane::meta {

MetaProposalExecutor::MetaProposalExecutor(std::size_t capacity)
    : capacity_(capacity), thread_(&MetaProposalExecutor::Run, this) {}

MetaProposalExecutor::~MetaProposalExecutor() { Shutdown(); }

absl::Status MetaProposalExecutor::Submit(Work work) {
  if (!work) return absl::InvalidArgumentError("empty proposal work item");
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!accepting_) {
      return absl::CancelledError("meta proposal executor is stopping");
    }
    if (capacity_ == 0 || queue_.size() >= capacity_) {
      return absl::ResourceExhaustedError("meta proposal executor is full");
    }
    queue_.push_back(std::move(work));
  }
  cv_.notify_one();
  return absl::OkStatus();
}

void MetaProposalExecutor::Shutdown() noexcept {
  {
    std::lock_guard<std::mutex> lock(mu_);
    accepting_ = false;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

void MetaProposalExecutor::Run() noexcept {
  for (;;) {
    Work work;
    {
      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait(lock, [this] { return !queue_.empty() || !accepting_; });
      if (queue_.empty()) return;
      work = std::move(queue_.front());
      queue_.pop_front();
    }
    // Every accepted item owns a waiter or reply state. Its task boundary
    // must resolve that state on expected failure; an escaping exception is
    // process-fatal instead of leaving an invisible, permanently stuck round.
    work();
  }
}

MetaMembershipGate::Lease::~Lease() {
  if (gate_ != nullptr) gate_->Release();
}

std::unique_ptr<MetaMembershipGate::Lease> MetaMembershipGate::TryAcquire() {
  std::lock_guard<std::mutex> lock(mu_);
  if (held_) return nullptr;
  held_ = true;
  return std::unique_ptr<Lease>(new Lease(shared_from_this()));
}

void MetaMembershipGate::Release() noexcept {
  std::lock_guard<std::mutex> lock(mu_);
  held_ = false;
}

}  // namespace keylane::meta
