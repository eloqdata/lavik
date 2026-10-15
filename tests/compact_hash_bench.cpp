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

// Compare the old owned codec path with direct encoded access in one binary.
// Each iteration starts from the same immutable snapshot and actually changes
// values. This isolates CPU/allocation cost; it does not model storage IO.
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "lavik/storage/detail/hash_codec.h"
#include "lavik/storage/detail/hash_read.h"

using namespace lavik::storage;

// Reproduce the baseline batch index, including its compact position slots.
struct FieldPositionHash {
  using is_transparent = void;
  const std::vector<HashEntry>* entries;
  std::size_t operator()(std::string_view field) const {
    return absl::Hash<std::string_view>{}(field);
  }
  std::size_t operator()(std::size_t position) const {
    return (*this)((*entries)[position].field_);
  }
};

struct FieldPositionEqual {
  using is_transparent = void;
  const std::vector<HashEntry>* entries;
  bool operator()(std::size_t left, std::size_t right) const {
    return (*entries)[left].field_ == (*entries)[right].field_;
  }
  bool operator()(std::size_t left, std::string_view right) const {
    return (*entries)[left].field_ == right;
  }
  bool operator()(std::string_view left, std::size_t right) const {
    return left == (*entries)[right].field_;
  }
};

int main(int argc, char** argv) {
  const std::size_t iterations = argc > 1 ? std::stoull(argv[1]) : 20000;
  if (iterations == 0) return 1;
  std::uint64_t checksum = 0;
  std::cout << "fields,payload_bytes,operation,path,median_ns_per_op\n";
  for (std::size_t count : {1, 16, 64, 128}) {
    HashValue original;
    for (std::size_t i = 0; i < count; ++i) {
      std::string field = "field-" + std::to_string(i);
      field.resize(32, 'f');
      original.entries_.push_back({.digest_ = ComputeDigest(field),
                                   .field_ = field,
                                   .value_ = std::string(64, 'a')});
    }
    const auto encoded = EncodeHashValue(original).value();
    const std::string replacement(64, 'b');
    for (std::size_t operands : {1, 8}) {
      if (operands > count) continue;
      std::vector<std::string_view> fields, values;
      for (std::size_t i = 0; i < operands; ++i) {
        fields.push_back(original.entries_[count - 1 - i].field_);
        values.push_back(replacement);
      }
      for (bool direct : {false, true}) {
        std::vector<double> samples;
        for (unsigned run = 0; run < 7; ++run) {
          const auto start = std::chrono::steady_clock::now();
          for (std::size_t i = 0; i < iterations; ++i) {
            if (direct) {
              auto edited =
                  EditCompactHash(encoded, count, fields, values, 16383)
                      .value()
                      .value();
              checksum += edited.payload_.size();
            } else {
              auto decoded = DecodeHashValue(encoded).value();
              // Match the old single-field lookup; use a position index for
              // batches, as ExecuteHashLocked does (included below).
              if (operands == 1) {
                const auto digest = ComputeDigest(fields[0]);
                for (auto& entry : decoded.entries_) {
                  if (entry.digest_ == digest && entry.field_ == fields[0]) {
                    entry.value_ = values[0];
                    break;
                  }
                }
              } else {
                absl::flat_hash_set<std::size_t, FieldPositionHash,
                                    FieldPositionEqual>
                    positions(0, FieldPositionHash{&decoded.entries_},
                              FieldPositionEqual{&decoded.entries_});
                positions.reserve(decoded.entries_.size() + operands);
                for (std::size_t j = 0; j < decoded.entries_.size(); ++j)
                  positions.insert(j);
                for (std::size_t j = 0; j < operands; ++j)
                  decoded.entries_[*positions.find(fields[j])].value_ =
                      values[j];
              }
              checksum += EncodeHashValue(decoded)->size();
            }
          }
          samples.push_back(std::chrono::duration<double, std::nano>(
                                std::chrono::steady_clock::now() - start)
                                .count() /
                            iterations);
        }
        std::sort(samples.begin(), samples.end());
        std::cout << count << ',' << encoded.size() << ",hset" << operands
                  << ',' << (direct ? "direct" : "owned") << ',' << samples[3]
                  << '\n';
      }
    }
    const auto& field = original.entries_.back().field_;
    for (bool direct : {false, true}) {
      std::vector<double> samples;
      for (unsigned run = 0; run < 7; ++run) {
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t i = 0; i < iterations; ++i) {
          std::string result;
          if (direct) {
            result = *FindCompactHashField(encoded, count, field).value();
          } else {
            auto decoded = DecodeHashValue(encoded).value();
            const auto digest = ComputeDigest(field);
            for (const auto& entry : decoded.entries_) {
              if (entry.digest_ == digest && entry.field_ == field) {
                result = entry.value_;
                break;
              }
            }
          }
          checksum += result.size();
        }
        samples.push_back(std::chrono::duration<double, std::nano>(
                              std::chrono::steady_clock::now() - start)
                              .count() /
                          iterations);
      }
      std::sort(samples.begin(), samples.end());
      std::cout << count << ',' << encoded.size() << ",hget,"
                << (direct ? "direct" : "owned") << ',' << samples[3] << '\n';
    }
  }
  std::cerr << "checksum=" << checksum << '\n';
}
