/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Standalone microbenchmark: build lavik_tlsf_range_allocator_bench in Release.
// Measures metadata operations only, not payload access, I/O, or contention.
// Every reported operation frees one live interval and attempts a replacement;
// trace reads and loop overhead are included equally in the free-list baseline.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <span>
#include <string_view>
#include <vector>

#include "lavik/tlsf_range_allocator.h"

namespace {

using Allocator = lavik::TlsfRangeAllocator;
using Allocation = Allocator::Allocation;
constexpr std::uint32_t kCapacity = 8192;
constexpr std::uint32_t kLive = 256;
constexpr std::uint32_t kLargeLive = 16;
constexpr std::size_t kTraceLength = 4096;
constexpr unsigned kWarmup = 100000;
constexpr unsigned kIterations = 1000000;
constexpr unsigned kSamples = 7;

struct Step {
  std::uint32_t slot;
  std::uint32_t units;
  std::uint32_t bytes;
};

struct Result {
  double ns_per_pair;
  unsigned failures;
};

inline void Consume(Allocation allocation) {
  // Keep both the result and memory effects observable without adding an
  // instruction or a function call to the timed loop on GCC/Clang.
  asm volatile("" : : "r"(allocation.offset), "r"(allocation.units) : "memory");
}

enum class Workload { kSmall, kMixed, kLarge };

std::array<Step, kTraceLength> MakeTrace(Workload workload) {
  std::uint32_t state = 2026;
  auto random = [&] {
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    return state;
  };
  constexpr std::array<std::uint32_t, 12> sizes{1, 1, 1,  2,  3,  4,
                                                6, 8, 10, 16, 32, 64};
  constexpr std::array<std::uint32_t, 10> large_sizes{1,  2,   8,   32,  65,
                                                      96, 129, 257, 513, 1024};
  std::array<Step, kTraceLength> trace;
  for (auto& step : trace) {
    step.slot = random() % (workload == Workload::kLarge ? kLargeLive : kLive);
    if (workload == Workload::kSmall)
      step.units = 1;
    else if (workload == Workload::kMixed)
      step.units = sizes[random() % sizes.size()];
    else
      step.units = large_sizes[random() % large_sizes.size()];
    // Precompute equivalent inputs so the adapter comparison times just the
    // byte-to-unit conversion, not random generation or a different workload.
    step.bytes = (step.units - 1) * 4096 + (1 + random() % 8) * 512;
  }
  return trace;
}

template <bool kConvertBytes>
Result RunTlsf(std::span<const Step> trace, std::uint32_t live_count = kLive) {
  std::vector<Allocator::Metadata> metadata(kCapacity);
  Allocator allocator;
  if (!allocator.Init(metadata)) std::abort();
  std::array<Allocation, kLive> live;
  for (auto& allocation : std::span(live).first(live_count)) {
    allocation = allocator.Allocate(1);
    if (!allocation) std::abort();
  }
  auto run = [&](unsigned iterations) {
    unsigned failures = 0;
    for (unsigned i = 0; i < iterations; ++i) {
      const Step& step = trace[i & (kTraceLength - 1)];
      auto& allocation = live[step.slot];
      if (allocation) allocator.Free(allocation);
      // A return and a later acquisition are separate observable operations,
      // even for a free list whose immediate push/pop could otherwise vanish.
      asm volatile("" : : : "memory");
      const std::uint32_t units =
          kConvertBytes ? (step.bytes + 4095) >> 12 : step.units;
      allocation = allocator.Allocate(units);
      failures += !allocation;
      Consume(allocation);
    }
    return failures;
  };
  run(kWarmup);
  const auto begin = std::chrono::steady_clock::now();
  const auto failures = run(kIterations);
  const auto elapsed = std::chrono::steady_clock::now() - begin;
  for (auto allocation : live) {
    if (allocation) allocator.Free(allocation);
  }
  if (allocator.free_units() != kCapacity) std::abort();
  return {
      std::chrono::duration<double, std::nano>(elapsed).count() / kIterations,
      failures};
}

Result RunFixed(std::span<const Step> trace) {
  std::vector<std::uint32_t> free(kCapacity);
  std::iota(free.begin(), free.end(), 0U);
  std::array<Allocation, kLive> live;
  for (auto& allocation : live) {
    allocation = {.offset = free.back(), .units = 1};
    free.pop_back();
  }
  auto run = [&](unsigned iterations) {
    for (unsigned i = 0; i < iterations; ++i) {
      auto& allocation = live[trace[i & (kTraceLength - 1)].slot];
      free.push_back(allocation.offset);
      asm volatile("" : : : "memory");
      allocation = {.offset = free.back(), .units = 1};
      free.pop_back();
      Consume(allocation);
    }
  };
  run(kWarmup);
  const auto begin = std::chrono::steady_clock::now();
  run(kIterations);
  const auto elapsed = std::chrono::steady_clock::now() - begin;
  return {
      std::chrono::duration<double, std::nano>(elapsed).count() / kIterations,
      0};
}

void Report(std::string_view name, std::array<Result, kSamples> samples) {
  std::sort(samples.begin(), samples.end(),
            [](auto a, auto b) { return a.ns_per_pair < b.ns_per_pair; });
  const auto median = samples[kSamples / 2];
  std::cout << std::left << std::setw(29) << name << std::right << std::fixed
            << std::setprecision(2) << std::setw(9) << median.ns_per_pair
            << " ns/pair  range=" << samples.front().ns_per_pair << ".."
            << samples.back().ns_per_pair << "  failures=" << median.failures
            << '/' << kIterations << '\n';
}

}  // namespace

int main() {
#ifndef NDEBUG
  std::cerr << "Use a Release build for allocator timing.\n";
  return 1;
#endif
  const auto small = MakeTrace(Workload::kSmall);
  const auto mixed = MakeTrace(Workload::kMixed);
  const auto large = MakeTrace(Workload::kLarge);
  std::cout << "capacity_units=" << kCapacity << " live=" << kLive
            << " large_live=" << kLargeLive << " trace_steps=" << kTraceLength
            << " warmup=" << kWarmup << " iterations=" << kIterations
            << " samples=" << kSamples
            << " metadata_bytes=" << sizeof(Allocator::Metadata) * kCapacity
            << " control_bytes=" << sizeof(Allocator) << '\n';
  std::array<Result, kSamples> fixed, small_units, small_bytes, mixed_units,
      mixed_bytes, large_units, large_bytes;
  for (unsigned i = 0; i < kSamples; ++i) {
    fixed[i] = RunFixed(small);
    // Alternate the equivalent adapter/unit runs to reduce ordering bias.
    if (i % 2 == 0) {
      small_units[i] = RunTlsf<false>(small);
      small_bytes[i] = RunTlsf<true>(small);
      mixed_units[i] = RunTlsf<false>(mixed);
      mixed_bytes[i] = RunTlsf<true>(mixed);
      large_units[i] = RunTlsf<false>(large, kLargeLive);
      large_bytes[i] = RunTlsf<true>(large, kLargeLive);
    } else {
      small_bytes[i] = RunTlsf<true>(small);
      small_units[i] = RunTlsf<false>(small);
      mixed_bytes[i] = RunTlsf<true>(mixed);
      mixed_units[i] = RunTlsf<false>(mixed);
      large_bytes[i] = RunTlsf<true>(large, kLargeLive);
      large_units[i] = RunTlsf<false>(large, kLargeLive);
    }
  }
  Report("fixed free-list / 1 unit", fixed);
  Report("TLSF / 1 unit", small_units);
  Report("TLSF / byte adapter / 1 unit", small_bytes);
  Report("TLSF / mixed 1..64 units", mixed_units);
  Report("TLSF / byte adapter / mixed", mixed_bytes);
  Report("TLSF / mixed 1..1024 units", large_units);
  Report("TLSF / byte adapter / large", large_bytes);
}
