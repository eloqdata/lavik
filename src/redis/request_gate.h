#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>

namespace keylane {

// Tracks commands that may outlive the socket read which admitted them. The
// shutdown thread closes the gate before stopping workers, then waits until all
// admitted commands have left.
//
// Online traffic updates only the counter owned by its current worker. This
// avoids bouncing one global read-modify-write cache line between every worker.
// The closed bit is sharded with the count, so the normal path does not even
// read a cache line shared with other workers.
class RequestGate {
 public:
  void Prepare(unsigned worker_count) {
    assert(worker_count != 0);
    counters_ = std::make_unique<Counter[]>(worker_count);
    worker_count_ = worker_count;
  }

  [[nodiscard]] bool TryEnter(unsigned worker_id) noexcept {
    assert(worker_id < worker_count_);
    Counter& counter = counters_[worker_id];
    std::uint64_t state = counter.state_.load(std::memory_order_acquire);
    while ((state & kClosed) == 0) {
      if (counter.state_.compare_exchange_weak(state, state + 1,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
        return true;
      }
    }
    return false;
  }

  void Leave(unsigned worker_id) noexcept {
    assert(worker_id < worker_count_);
    LeaveCounter(counters_[worker_id]);
  }

  void Close() noexcept {
    // Closing shards in sequence is safe: a worker may still enter until its
    // own shard is closed, but Close closes every shard before the caller can
    // begin WaitUntilEmpty. Each shard's closed bit and count share one atomic,
    // so an entry racing Close is either rejected or included in the count.
    for (unsigned worker_id = 0; worker_id < worker_count_; ++worker_id) {
      counters_[worker_id].state_.fetch_or(kClosed, std::memory_order_acq_rel);
    }
  }

  void WaitUntilEmpty() const noexcept {
    assert(closed());
    for (unsigned worker_id = 0; worker_id < worker_count_; ++worker_id) {
      const Counter& counter = counters_[worker_id];
      std::uint64_t state = counter.state_.load(std::memory_order_acquire);
      while ((state & kCountMask) != 0) {
        counter.state_.wait(state, std::memory_order_acquire);
        state = counter.state_.load(std::memory_order_acquire);
      }
    }
  }

  [[nodiscard]] bool closed() const noexcept {
    for (unsigned worker_id = 0; worker_id < worker_count_; ++worker_id) {
      if ((counters_[worker_id].state_.load(std::memory_order_acquire) &
           kClosed) == 0) {
        return false;
      }
    }
    return worker_count_ != 0;
  }

 private:
  static constexpr std::uint64_t kClosed = 1ULL << 63;
  static constexpr std::uint64_t kCountMask = ~kClosed;

  // Keylane's supported CPUs have 64-byte cache lines. Array stride matters:
  // alignment alone would not prevent adjacent workers sharing a line if this
  // object were ever extended with another small field.
  struct alignas(64) Counter {
    std::atomic<std::uint64_t> state_{0};
  };
  static_assert(sizeof(Counter) == 64);

  void LeaveCounter(Counter& counter) noexcept {
    const std::uint64_t previous =
        counter.state_.fetch_sub(1, std::memory_order_acq_rel);
    assert((previous & kCountMask) != 0);
    // atomic::notify itself is normally cheap, but it is unnecessary online.
    // Once closed, notify only the zero transition that can release the waiter.
    if ((previous & kClosed) != 0 && (previous & kCountMask) == 1) {
      counter.state_.notify_all();
    }
  }

  std::unique_ptr<Counter[]> counters_;
  unsigned worker_count_ = 0;
};

}  // namespace keylane
