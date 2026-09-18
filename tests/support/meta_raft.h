// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>

#include "lavik/meta/identity_verifier.h"
#include "lavik/meta/raft.h"

namespace lavik::test {

// A real C ABI / Go / etcd WAL fixture. The listener uses a kernel-assigned
// port; the single voter never dials its advertised descriptor.
inline meta::MetaRaftOptions SingleMetaOptions(
    const std::filesystem::path& dir) {
  meta::MetaRaftOptions options;
  options.id_ = 1;
  options.data_dir_ = dir;
  options.listen_ = "127.0.0.1:0";
  options.local_raft_ = "127.0.0.1:9601";
  options.local_data_ = "127.0.0.1:9701";
  options.local_admin_ = "127.0.0.1:9801";
  options.heartbeat_ms_ = 50;
  options.election_ms_ = 150;
  options.snapshot_distance_ = 0;
  options.reserved_log_items_ = 0;
  if (!std::filesystem::exists(dir / "RAFT")) {
    const meta::MetaMemberIdentity identity{
        1, "lavik://meta/1", options.local_data_, options.local_admin_};
    options.initial_.push_back(std::make_shared<meta::MetaRaftMember>(
        1, 0, "127.0.0.1:9601", identity.EncodeAux()));
  }
  return options;
}

// Holds only the application executor. A timed release makes failed assertions
// unable to strand fixture shutdown; successful paths explicitly release it.
class MetaApplyBarrier {
 public:
  void Pause() {
    std::lock_guard lock(mutex_);
    until_ = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  }
  void Resume() {
    std::lock_guard lock(mutex_);
    until_ = {};
    ready_.notify_all();
  }
  void Wait() {
    std::unique_lock lock(mutex_);
    ready_.wait_until(lock, until_,
                      [this] { return until_ == decltype(until_){}; });
  }
  bool paused() {
    std::lock_guard lock(mutex_);
    return std::chrono::steady_clock::now() < until_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable ready_;
  std::chrono::steady_clock::time_point until_{};
};

}  // namespace lavik::test
