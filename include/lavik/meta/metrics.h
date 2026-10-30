/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "absl/status/statusor.h"

namespace lavik::meta {

class MetaRaft;
class MetaCoordinator;
class MetaDataControlServer;
class MetaDataControlRuntimeStatus;

// Fixed-size event accounting. Recording never formats, gathers, allocates,
// crosses the Go ABI, or waits for the metrics worker. Independent fields do
// not form a transactional snapshot with Raft or Data-control observations.
class MetaProposalMetrics {
 public:
  enum class Outcome {
    kAppliedSuccess,
    kAppliedRejected,
    kAdmissionRejected,
    kOutcomeUnknown
  };
  class Attempt {
   public:
    explicit Attempt(MetaProposalMetrics& metrics);
    ~Attempt();
    Attempt(const Attempt&) = delete;
    Attempt& operator=(const Attempt&) = delete;
    void SetOutcome(Outcome outcome) { outcome_ = outcome; }

   private:
    MetaProposalMetrics& metrics_;
    std::chrono::steady_clock::time_point start_;
    Outcome outcome_ = Outcome::kAdmissionRejected;
  };
  // Scrape-only rendering; safe while callers record their first result.
  std::string Collect() const;

 private:
  static constexpr std::array<double, 12> kBounds{
      .0001, .0005, .001, .005, .01, .05, .1, .3, 1, 5, 30, 120};
  struct Result {
    std::atomic<std::uint64_t> nanoseconds{0};
    std::array<std::atomic<std::uint64_t>, kBounds.size() + 1> buckets{};
  };
  std::array<Result, 4> results_{};
  std::atomic<std::uint64_t> inflight_{0};
};

// A standalone, bounded HTTP worker. Start binds synchronously, so failures
// are startup errors. Publish only transfers a compact set of owned sources;
// collection reads their existing immutable views and atomic counters.
// Stop closes ingress, cancels connections and joins collection before any
// source runtime may be shut down. No callback runs on a protocol worker.
class MetaMetricsServer {
 public:
  enum class Phase { kStarting, kRecovering, kRunning, kStopping };
  struct Sources {
    std::shared_ptr<MetaRaft> raft;
    std::shared_ptr<MetaCoordinator> coordinator;
    std::shared_ptr<MetaDataControlServer> data_control;
    std::shared_ptr<MetaDataControlRuntimeStatus> leadership;
  };
  static absl::StatusOr<std::unique_ptr<MetaMetricsServer>> Start(
      const std::string& bind, std::uint16_t port);
  ~MetaMetricsServer();
  void SetPhase(Phase phase);
  void Publish(Sources sources);
  void Stop();

 private:
  struct Impl;
  explicit MetaMetricsServer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace lavik::meta
