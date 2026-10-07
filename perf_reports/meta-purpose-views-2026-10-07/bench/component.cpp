/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include <algorithm>
#include <atomic>
#include <barrier>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>

#include "fixture.h"
#include "metrics.h"

using namespace bench;
using namespace measurement;

int warmup_ms = 25;

struct Samples {
  Counters counters;
  std::vector<double> latency;
  void Add(double elapsed, Counters count) {
    counters += count;
    latency.push_back(elapsed);
  }
  void Print(const std::string& shape, const std::string& reader,
             const std::string& phase) {
    std::sort(latency.begin(), latency.end());
    const double n = latency.size();
    auto q = [&](double p) {
      return latency[std::min(latency.size() - 1, size_t(n * p))];
    };
    std::cout << shape << ',' << reader << ',' << phase << ',' << n << ','
              << counters.bytes / n << ',' << counters.allocations / n << ','
              << counters.frees / n << ',' << counters.freed_usable_bytes / n
              << ',' << counters.wait_ns / n << ',' << counters.hold_ns / n
              << ',' << q(.5) << ',' << q(.95) << ',' << q(.99) << '\n';
  }
};

template <class Capture>
void Measure(const std::string& shape, const std::string& reader,
             int iterations, Capture capture) {
  Samples captures, releases;
  // Equal-duration warmup avoids comparing a cold fast reader with the much
  // longer aggregate-copy path's already-warm CPU and allocator state.
  const auto warmed = Clock::now() + std::chrono::milliseconds(warmup_ms);
  while (Clock::now() < warmed) {
    auto owned = capture();
    asm volatile("" : : "g"(&owned) : "memory");
  }
  for (int i = -3; i < iterations; ++i) {
    std::optional<decltype(capture())> view;
    Start();
    const auto begin = Clock::now();
    view.emplace(capture());
    const auto elapsed = Ns(begin, Clock::now());
    auto count = Stop();
    if (i >= 0) captures.Add(elapsed, count);
    Start();
    const auto end = Clock::now();
    view.reset();
    const auto released = Ns(end, Clock::now());
    count = Stop();
    if (i >= 0) releases.Add(released, count);
  }
  captures.Print(shape, reader, "capture");
  releases.Print(shape, reader, "destroy");
}

std::vector<Shape> Matrix() {
  std::vector<Shape> result;
  Shape base;
  base.name = "small";
  base.nodes = 8;
  base.groups = 2;
  base.directives = 1;
  result.push_back(base);
  for (int audit : {1024, 65536}) {
    auto s = base;
    s.name = "audit_" + std::to_string(audit);
    s.audit = audit;
    result.push_back(s);
  }
  for (int ops : {1, 16, 128}) {
    auto s = base;
    s.name = "unrelated_ops_" + std::to_string(ops);
    s.operations += ops;
    s.op_bytes = 262144;
    result.push_back(s);
  }
  for (int archives : {1, 128}) {
    auto s = base;
    s.name = "archives_" + std::to_string(archives);
    s.archives = archives;
    s.archive_bytes = 65536;
    result.push_back(s);
  }
  {
    auto s = base;
    s.name = "policies_32";
    s.policy_versions = 32;
    result.push_back(s);
  }
  {
    auto s = base;
    s.name = "manifests_128";
    s.manifests = 128;
    s.entries = 128;
    result.push_back(s);
  }
  for (int bytes : {65536, 262144}) {
    auto s = base;
    s.name = "selected_" + std::to_string(bytes);
    s.selected_bytes = bytes;
    result.push_back(s);
  }
  for (auto [nodes, groups] : {std::pair{128, 32}, std::pair{4096, 512}}) {
    for (bool transitions : {false, true}) {
      auto s = base;
      s.nodes = nodes;
      s.groups = groups;
      s.transitions = transitions ? groups : 0;
      s.directives = transitions ? 0 : 1;
      s.name = "facts_" + std::to_string(nodes) + "_" +
               std::to_string(s.transitions);
      result.push_back(s);
    }
  }
  for (int directives : {3, 96}) {
    auto s = base;
    s.nodes = 128;
    s.groups = 32;
    s.directives = directives;
    s.name = "directives_" + std::to_string(directives);
    result.push_back(s);
  }
  for (int audit : {0, 1024, 65536}) {
    auto s = base;
    s.name = "combined_" + std::to_string(audit);
    s.nodes = 128;
    s.groups = 32;
    s.operations = 129;
    s.op_bytes = 65536;
    s.archives = 32;
    s.archive_bytes = 65536;
    s.policy_versions = 32;
    s.manifests = 64;
    s.entries = 128;
    s.audit = audit;
    s.directives = 12;
    result.push_back(s);
  }
  return result;
}

void Contention(const Shape& shape, const std::string& image) {
  auto opened = m::MetaStateMachine::Open("");
  Check(opened.status());
  auto sm = std::move(*opened);
  Check(sm->Install(100000, image));
  m::RegisterNode command;
  command.node_id_ = Node(1);
  command.principal_ = "lavik://node/" + command.node_id_;
  command.endpoints_ = {"tcp://127.0.0.1:10000"};
  auto encoded = m::MetaStateMachine::EncodeCommand(command);
  Check(encoded.status());
  Samples reads, writes;
  std::barrier gate(2);
  std::exception_ptr reader_error;
  std::jthread reader([&](std::stop_token stop) {
    gate.arrive_and_wait();
    try {
      auto next = Clock::now();
      while (!stop.stop_requested()) {
        Start();
        const auto start = Clock::now();
        {
#ifdef LEGACY
          auto cut = sm->StoresSnapshot();
#else
          auto cut = sm->CaptureDataPublication();
#endif
        }
        auto elapsed = Ns(start, Clock::now());
        auto count = Stop();
        reads.Add(elapsed, count);
        next += std::chrono::milliseconds(10);
        std::this_thread::sleep_until(next);
      }
    } catch (...) {
      Stop();
      reader_error = std::current_exception();
    }
  });
  gate.arrive_and_wait();
  const auto begin = Clock::now();
  std::uint64_t applied = 0;
  while (Clock::now() - begin < std::chrono::seconds(2)) {
    Start();
    const auto start = Clock::now();
    auto reply = sm->commit(100001 + applied, **encoded);
    auto elapsed = Ns(start, Clock::now());
    auto count = Stop();
    if (!reply) throw std::runtime_error("missing apply reply");
    auto decoded = m::DecodeMetaApplyResult(std::string_view(
        reinterpret_cast<const char*>(reply->data_begin()), reply->size()));
    Check(decoded.status());
    if (decoded->verdict_ != m::MetaAuditVerdict::kAccepted)
      throw std::runtime_error("rejected measured apply: " + decoded->detail_);
    writes.Add(elapsed, count);
    ++applied;
  }
  const auto elapsed = Ns(begin, Clock::now());
  reader.request_stop();
  reader.join();
  if (reader_error) std::rethrow_exception(reader_error);
  writes.Print(shape.name, "apply", "contended");
  reads.Print(shape.name, "publication", "paced_100hz");
  std::cerr << "apply_per_second," << shape.name << ','
            << applied * 1e9 / elapsed << '\n';
}

int main(int argc, char** argv) {
  try {
    const int iterations = argc > 1 ? std::stoi(argv[1]) : 60;
    if (argc > 2) warmup_ms = std::stoi(argv[2]);
    std::cout
        << "shape,reader,phase,samples,requested_bytes,allocations,frees,freed_"
           "usable_bytes,lock_wait_ns,lock_hold_ns,p50_ns,p95_ns,p99_ns\n";
    for (const auto& shape : Matrix()) {
      auto stores = Fixture(shape);
      auto image = stores.Serialize();
      Check(image.status());
      auto opened = m::MetaStateMachine::Open("");
      Check(opened.status());
      auto sm = std::move(*opened);
      Check(sm->Install(100000, *image));
      m::RegisterNode encoded_command;
      encoded_command.node_id_ = Node(1);
      Measure(shape.name, "command_encode", iterations, [&] {
        return m::MetaStateMachine::EncodeCommand(encoded_command);
      });
#ifdef LEGACY
      for (const auto* reader :
           {"proposal", "facts", "create_discovery", "membership_discovery",
            "failover_discovery", "automatic_detection", "publication",
            "admin_group", "operation_status", "audit_export",
            "operation_export", "cursor"})
        Measure(shape.name, reader, iterations,
                [&] { return sm->StoresSnapshot(); });
#else
      m::RegisterNode command;
      command.node_id_ = Node(1);
      Measure(shape.name, "proposal", iterations,
              [&] { return sm->CaptureProposal(command); });
      Measure(shape.name, "facts", iterations,
              [&] { return sm->CaptureObservationFacts(); });
      Measure(shape.name, "create_discovery", iterations,
              [&] { return sm->CaptureClusterCreateDiscovery(); });
      Measure(shape.name, "membership_discovery", iterations,
              [&] { return sm->CaptureMembershipDiscovery(); });
      Measure(shape.name, "failover_discovery", iterations,
              [&] { return sm->CaptureFailoverDiscovery(); });
      Measure(shape.name, "automatic_detection", iterations,
              [&] { return sm->CaptureAutomaticDetectionView(); });
      Measure(shape.name, "publication", iterations,
              [&] { return sm->CaptureDataPublication(); });
      Measure(shape.name, "admin_group", iterations,
              [&] { return sm->CaptureAdminGroup("g0"); });
      Measure(shape.name, "operation_status", iterations,
              [&] { return sm->CaptureOperationStatus(Id<16>(1000)); });
      Measure(shape.name, "audit_export", iterations,
              [&] { return sm->CaptureAuditExport(); });
      Measure(shape.name, "operation_export", iterations,
              [&] { return sm->CaptureOperationArchiveExport(); });
      Measure(shape.name, "cursor", iterations,
              [&] { return sm->CaptureCommittedCursor(); });
#endif
      Measure(shape.name, "snapshot", std::min(iterations, 10),
              [&] { return sm->Capture(100000); });
      if (shape.name == "small" || shape.name.starts_with("combined_"))
        Contention(shape, *image);
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
