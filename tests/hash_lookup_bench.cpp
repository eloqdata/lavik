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

// Pure-memory lookup: includes header opening and query hashing, excludes
// encoding, allocation, I/O, CRC and replies. Scalar v2 isolates the benefit of
// SIMD from the indexed representation itself. No worker/runtime is started.
// Build the lavik_hash_lookup_bench target in a Release build, then run
// taskset -c 2 <build-dir>/lavik_hash_lookup_bench. The optional first argument
// changes lookups per repetition; omitted arguments select the defaults below.
#include <time.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "lavik/storage/detail/hash_codec.h"

using namespace lavik::storage;
namespace {
std::uint64_t sink = 0;

[[gnu::noinline]] std::uint64_t Lookup(std::string_view payload,
                                       std::string_view query, bool scalar) {
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) {
    std::cerr << reader.status() << '\n';
    std::abort();
  }
  auto found = reader->Find(query, scalar);
  if (!found.ok()) {
    std::cerr << found.status() << '\n';
    std::abort();
  }
  return *found ? 1 + (**found).value_.size() +
                      static_cast<unsigned char>((**found).field_.back())
                : 0;
}

std::string Key(std::size_t i, bool missing = false) {
  auto suffix = std::to_string(i);
  return std::string(missing ? "absent" : "field-") +
         std::string(10 - suffix.size(), '0') + suffix;
}

std::uint64_t ThreadNanoseconds() {
  timespec now{};
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now) != 0) std::abort();
  return static_cast<std::uint64_t>(now.tv_sec) * 1000000000 + now.tv_nsec;
}

double Measure(const std::string& bytes,
               const std::vector<std::string>& queries, bool scalar,
               std::size_t iterations) {
  std::uint64_t sum = 0;
  // Thread CPU time excludes scheduler preemption by unrelated work. This is
  // codec compute cost, not request latency; pin the process for stable caches.
  const auto begin = ThreadNanoseconds();
  for (std::size_t i = 0; i < iterations; ++i)
    sum += Lookup(bytes, queries[i & (queries.size() - 1)], scalar);
  const auto end = ThreadNanoseconds();
  sink ^= sum;
  return static_cast<double>(end - begin) / iterations;
}
}  // namespace

int main(int argc, char** argv) {
  // One optional argument changes samples per repetition; defaults are reported
  // so a saved CSV identifies its actual measurement policy.
  std::size_t iterations = 100000;
  if (argc > 2) return 2;
  if (argc == 2) iterations = std::stoull(argv[1]);
  if (iterations == 0) return 2;
#if defined(__AVX2__)
  constexpr auto backend = "AVX2-32";
#elif defined(__SSE2__)
  constexpr auto backend = "SSE2-16";
#elif defined(__aarch64__)
  constexpr auto backend = "NEON-16";
#else
  constexpr auto backend = "scalar";
#endif
  constexpr std::size_t repetitions = 7;
  std::cout << "# SIMD=" << backend << ", iterations=" << iterations
            << ", repetitions=" << repetitions
            << ", statistic=median, clock=thread_cpu, compiler=" << __VERSION__
            << '\n';
  std::cout << "kind,count,query,v1_bytes,v2_bytes,v1_ns,v2_scalar_ns,v2_simd_"
               "ns,v1_speedup,simd_speedup\n";
  std::mt19937 random(2026);
  for (bool set : {false, true}) {
    for (std::size_t count : {4, 16, 64, 128, 256, 512}) {
      HashValue value;
      for (std::size_t i = 0; i < count; ++i)
        value.entries_.push_back(
            {.field_ = Key(i),
             .value_ = set ? "" : std::string(32, 'a' + i % 26)});
      const auto legacy = EncodeHashValue(value, kLegacyHashValueVersion);
      const auto indexed = EncodeHashValue(value);
      if (!legacy.ok() || !indexed.ok()) return 1;
      for (const std::string mode : {"first", "last", "hit_random", "miss"}) {
        std::vector<std::string> queries;
        for (unsigned i = 0; i < 1024; ++i) {
          const auto at = mode == "first"  ? 0
                          : mode == "last" ? count - 1
                                           : random() % count;
          queries.push_back(Key(at, mode == "miss"));
          const auto expected = Lookup(*legacy, queries.back(), false);
          if (Lookup(*indexed, queries.back(), false) != expected ||
              Lookup(*indexed, queries.back(), true) != expected)
            return 1;
        }
        std::array<std::vector<double>, 3> times;
        // Alternate measurement order to avoid consistently favoring one path.
        for (std::size_t repeat = 0; repeat < repetitions; ++repeat)
          for (std::size_t position = 0; position < 3; ++position) {
            const auto variant = (position + repeat) % 3;
            times[variant].push_back(Measure(variant == 0 ? *legacy : *indexed,
                                             queries, variant == 1,
                                             iterations));
          }
        std::array<double, 3> median;
        for (unsigned i = 0; i < 3; ++i) {
          std::sort(times[i].begin(), times[i].end());
          median[i] = times[i][repetitions / 2];
        }
        std::cout << (set ? "set" : "hash") << ',' << count << ',' << mode
                  << ',' << legacy->size() << ',' << indexed->size() << ','
                  << std::fixed << std::setprecision(2) << median[0] << ','
                  << median[1] << ',' << median[2] << ','
                  << median[0] / median[2] << ',' << median[1] / median[2]
                  << '\n';
      }
    }
  }
  std::cerr << "checksum=" << sink << '\n';
}
