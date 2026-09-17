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

#pragma once

// MetaPolicyStore owns the small set of versioned, global cluster policies
// that are part of Meta Committed State. Policy families are compiled in: a
// caller cannot create a new schema by choosing an arbitrary policy id.
//
// The raw compact JSON is retained unchanged for administration and exact
// replay identity. Admission and snapshot restore validate it; consumers use
// typed current-policy accessors and never parse JSON themselves.
// Versions start at 1 and are consecutive. Each family retains the newest 32
// versions; admitting a later version evicts the oldest atomically.
//
// Failure classes: command validation returns MetaDomainRejectError; corrupt
// or invariant-breaking snapshot bytes return MetaFailStopError.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/encoding.h"

namespace lavik::meta {

inline constexpr std::string_view kAutomaticUncontrolledFailoverPolicyId =
    "lavik.automatic-uncontrolled-failover-v1";
inline constexpr std::string_view kAuthorityLeasePolicyId =
    "lavik.authority-lease-v1";

inline constexpr std::uint64_t kMinimumAutomaticFailoverSuspectAfterMs = 1'000;
inline constexpr std::uint64_t kMaximumAutomaticFailoverSuspectAfterMs =
    86'400'000;
inline constexpr std::uint64_t kDefaultAutomaticFailoverSuspectAfterMs = 5'000;

inline constexpr std::uint64_t kMinimumAuthorityLeaseDurationMs = 100;
inline constexpr std::uint64_t kMaximumAuthorityLeaseDurationMs = 86'400'000;
inline constexpr std::uint64_t kDefaultAuthorityLeaseDurationMs = 5'000;

struct MetaAutomaticUncontrolledFailoverPolicy {
  std::uint64_t version_ = 0;
  std::uint64_t suspect_after_ms_ = 0;
  bool operator==(const MetaAutomaticUncontrolledFailoverPolicy&) const =
      default;
};

struct MetaAuthorityLeasePolicy {
  std::uint64_t version_ = 0;
  std::uint64_t duration_ms_ = 0;
  bool operator==(const MetaAuthorityLeasePolicy&) const = default;
};

// Strict decoders for the two registered raw formats. They accept field
// reordering but reject whitespace, missing/duplicate/unknown fields, escaped
// names and values, non-integer numbers, overflow, and values outside the
// documented range. The returned version is zero until installed in a store.
absl::StatusOr<MetaAutomaticUncontrolledFailoverPolicy>
DecodeAutomaticUncontrolledFailoverPolicy(std::string_view raw);
absl::StatusOr<MetaAuthorityLeasePolicy> DecodeAuthorityLeasePolicy(
    std::string_view raw);

struct MetaPolicyVersionView {
  std::string policy_id_;
  std::uint64_t version_ = 0;
  std::string content_;
  bool operator==(const MetaPolicyVersionView&) const = default;
};

class MetaPolicyStore {
 public:
  // Applies a fully validated document, or accepts an exact same-version raw
  // replay. Every rejection leaves history and byte accounting unchanged.
  absl::Status Apply(const PutPolicy& cmd);

  // Historical lookup covers only retained versions; an evicted, unknown, or
  // absent family returns nullopt. Versions() is family-id then version ordered
  // and returns independent raw-document copies.
  std::optional<MetaPolicyVersionView> FindVersion(const std::string& policy_id,
                                                   std::uint64_t version) const;
  std::optional<std::uint64_t> LatestVersion(
      const std::string& policy_id) const;
  std::vector<MetaPolicyVersionView> Versions() const;

  // Decodes the newest retained raw document into its registered schema.
  // nullopt means the required family is absent or internal state is invalid;
  // Created-state validation treats either condition as fail-stop corruption.
  std::optional<MetaAutomaticUncontrolledFailoverPolicy>
  CurrentAutomaticUncontrolledFailover() const;
  std::optional<MetaAuthorityLeasePolicy> CurrentAuthorityLease() const;

  // Aggregate retained raw bytes and number of installed registered families.
  std::uint64_t TotalContentBytes() const { return total_content_bytes_; }
  std::size_t PolicyCount() const { return policies_.size(); }

  // Current snapshot format: schema marker, sorted registered families, and
  // ascending (version, raw) history. No content hash or retired tombstone is
  // encoded. Old layouts are intentionally unsupported.
  std::string Serialize() const;
  // Exact durable size without allocating or copying snapshot bytes.
  std::uint64_t SerializedSize() const;
  static absl::StatusOr<MetaPolicyStore> Deserialize(std::string_view bytes);

 private:
  void WriteSnapshot(MetaWriter& writer) const;
  std::map<std::string, std::map<std::uint64_t, std::string>> policies_;
  std::uint64_t total_content_bytes_ = 0;
};

}  // namespace lavik::meta
