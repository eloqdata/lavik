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

#include "lavik/meta/policy_store.h"

#include <cctype>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <variant>

#include "absl/strings/str_cat.h"

namespace lavik::meta {
namespace {

using JsonValue = std::variant<std::string, bool, std::uint64_t>;
using JsonObject = std::map<std::string, JsonValue>;

class CompactJsonObjectParser {
 public:
  explicit CompactJsonObjectParser(std::string_view raw) : raw_(raw) {}

  absl::StatusOr<JsonObject> Parse() {
    for (const unsigned char c : raw_) {
      if (std::isspace(c)) {
        return MetaDomainRejectError("policy JSON must be compact");
      }
    }
    if (!Consume('{')) return Error("expected object");

    JsonObject result;
    if (Consume('}')) {
      if (!AtEnd()) return Error("trailing policy JSON bytes");
      return result;
    }

    while (true) {
      auto name = ParseString();
      if (!name.ok()) return name.status();
      if (!Consume(':')) return Error("expected ':' after policy field");
      auto value = ParseValue();
      if (!value.ok()) return value.status();
      if (!result.emplace(std::move(*name), std::move(*value)).second) {
        return Error("duplicate policy field");
      }
      if (Consume('}')) break;
      if (!Consume(',')) return Error("expected ',' between policy fields");
    }
    if (!AtEnd()) return Error("trailing policy JSON bytes");
    return result;
  }

 private:
  bool AtEnd() const { return offset_ == raw_.size(); }

  bool Consume(char expected) {
    if (AtEnd() || raw_[offset_] != expected) return false;
    ++offset_;
    return true;
  }

  absl::Status Error(std::string_view detail) const {
    return MetaDomainRejectError(absl::StrCat(detail, " at byte ", offset_));
  }

  absl::StatusOr<std::string> ParseString() {
    if (!Consume('"')) return Error("expected JSON string");
    const std::size_t start = offset_;
    while (!AtEnd() && raw_[offset_] != '"') {
      const unsigned char c = raw_[offset_];
      // Registered schemas are ASCII. Reject escapes instead of accepting
      // alternate byte spellings for field names and kind discriminators.
      if (c == '\\' || c < 0x20 || c > 0x7e) {
        return Error("unsupported character in policy string");
      }
      ++offset_;
    }
    if (!Consume('"')) return Error("unterminated JSON string");
    return std::string(raw_.substr(start, offset_ - start - 1));
  }

  absl::StatusOr<JsonValue> ParseValue() {
    if (!AtEnd() && raw_[offset_] == '"') {
      auto string = ParseString();
      if (!string.ok()) return string.status();
      return JsonValue(std::in_place_type<std::string>, std::move(*string));
    }
    if (raw_.substr(offset_).starts_with("true")) {
      offset_ += 4;
      return JsonValue(true);
    }
    if (raw_.substr(offset_).starts_with("false")) {
      offset_ += 5;
      return JsonValue(false);
    }
    if (AtEnd() || raw_[offset_] < '0' || raw_[offset_] > '9') {
      return Error("unsupported policy JSON value");
    }
    if (raw_[offset_] == '0' && offset_ + 1 < raw_.size() &&
        raw_[offset_ + 1] >= '0' && raw_[offset_ + 1] <= '9') {
      return Error("leading zero in policy integer");
    }
    std::uint64_t value = 0;
    while (!AtEnd() && raw_[offset_] >= '0' && raw_[offset_] <= '9') {
      const std::uint64_t digit =
          static_cast<std::uint64_t>(raw_[offset_] - '0');
      if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10) {
        return Error("policy integer overflow");
      }
      value = value * 10 + digit;
      ++offset_;
    }
    return JsonValue(value);
  }

  std::string_view raw_;
  std::size_t offset_ = 0;
};

absl::Status RequireExactFields(const JsonObject& object,
                                std::initializer_list<std::string_view> names) {
  if (object.size() != names.size()) {
    return MetaDomainRejectError("missing or unknown policy field");
  }
  for (const std::string_view name : names) {
    if (!object.contains(std::string(name))) {
      return MetaDomainRejectError(absl::StrCat("missing policy field ", name));
    }
  }
  return absl::OkStatus();
}

absl::Status RequireKind(const JsonObject& object, std::string_view expected) {
  const JsonValue& kind = object.at("kind");
  const auto* value = std::get_if<std::string>(&kind);
  if (value == nullptr || *value != expected) {
    return MetaDomainRejectError("policy kind does not match policy family");
  }
  return absl::OkStatus();
}

absl::Status ValidateRegisteredDocument(std::string_view policy_id,
                                        std::string_view raw) {
  if (policy_id == kAutomaticUncontrolledFailoverPolicyId) {
    auto decoded = DecodeAutomaticUncontrolledFailoverPolicy(raw);
    return decoded.ok() ? absl::OkStatus() : decoded.status();
  }
  if (policy_id == kAuthorityLeasePolicyId) {
    auto decoded = DecodeAuthorityLeasePolicy(raw);
    return decoded.ok() ? absl::OkStatus() : decoded.status();
  }
  if (policy_id == kCandidateRecoveryPolicyId) {
    auto decoded = DecodeCandidateRecoveryPolicy(raw);
    return decoded.ok() ? absl::OkStatus() : decoded.status();
  }
  return MetaDomainRejectError("unregistered policy family");
}

absl::Status SnapshotFailure(const absl::Status& status) {
  return MetaFailStopError(
      absl::StrCat("invalid policy snapshot: ", status.message()));
}

}  // namespace

absl::StatusOr<MetaAutomaticUncontrolledFailoverPolicy>
DecodeAutomaticUncontrolledFailoverPolicy(std::string_view raw) {
  auto object = CompactJsonObjectParser(raw).Parse();
  if (!object.ok()) return object.status();
  if (auto status = RequireExactFields(*object, {"kind", "suspect_after_ms"});
      !status.ok()) {
    return status;
  }
  if (auto status = RequireKind(*object, "automatic-uncontrolled-failover-v1");
      !status.ok()) {
    return status;
  }
  const JsonValue& suspect_after = object->at("suspect_after_ms");
  const auto* suspect_after_value = std::get_if<std::uint64_t>(&suspect_after);
  if (suspect_after_value == nullptr) {
    return MetaDomainRejectError(
        "automatic failover policy field type mismatch");
  }
  if (*suspect_after_value < kMinimumAutomaticFailoverSuspectAfterMs ||
      *suspect_after_value > kMaximumAutomaticFailoverSuspectAfterMs) {
    return MetaDomainRejectError("suspect_after_ms outside supported range");
  }
  return MetaAutomaticUncontrolledFailoverPolicy{
      .suspect_after_ms_ = *suspect_after_value,
  };
}

absl::StatusOr<MetaAuthorityLeasePolicy> DecodeAuthorityLeasePolicy(
    std::string_view raw) {
  auto object = CompactJsonObjectParser(raw).Parse();
  if (!object.ok()) return object.status();
  if (auto status = RequireExactFields(*object, {"kind", "duration_ms"});
      !status.ok()) {
    return status;
  }
  if (auto status = RequireKind(*object, "authority-lease-v1"); !status.ok()) {
    return status;
  }
  const JsonValue& duration = object->at("duration_ms");
  const auto* duration_value = std::get_if<std::uint64_t>(&duration);
  if (duration_value == nullptr) {
    return MetaDomainRejectError("authority lease policy field type mismatch");
  }
  if (*duration_value < kMinimumAuthorityLeaseDurationMs ||
      *duration_value > kMaximumAuthorityLeaseDurationMs) {
    return MetaDomainRejectError("duration_ms outside supported range");
  }
  return MetaAuthorityLeasePolicy{.duration_ms_ = *duration_value};
}

absl::StatusOr<MetaCandidateRecoveryPolicy> DecodeCandidateRecoveryPolicy(
    std::string_view raw) {
  auto object = CompactJsonObjectParser(raw).Parse();
  if (!object.ok()) return object.status();
  if (auto status = RequireExactFields(*object, {"kind", "budget_ms"});
      !status.ok())
    return status;
  if (auto status = RequireKind(*object, "candidate-recovery-v1"); !status.ok())
    return status;
  const auto* budget = std::get_if<std::uint64_t>(&object->at("budget_ms"));
  if (budget == nullptr || *budget > kMaximumCandidateRecoveryBudgetMs) {
    return MetaDomainRejectError("budget_ms outside supported range");
  }
  return MetaCandidateRecoveryPolicy{.budget_ms_ = *budget};
}

absl::Status MetaPolicyStore::Apply(const PutPolicy& cmd) {
  if (cmd.policy_id_.empty() || cmd.policy_id_.size() > kMaxMetaPolicyIdBytes) {
    return MetaDomainRejectError("policy_id empty or over cap");
  }
  if (cmd.content_.empty() || cmd.content_.size() > kMaxMetaPayloadBytes) {
    return MetaDomainRejectError("policy content empty or over cap");
  }
  if (auto status = ValidateRegisteredDocument(cmd.policy_id_, cmd.content_);
      !status.ok()) {
    return status;
  }

  const auto policy_it = policies_.find(cmd.policy_id_);
  if (policy_it != policies_.end()) {
    const auto& versions = policy_it->second;
    if (const auto existing = versions.find(cmd.version_);
        existing != versions.end()) {
      if (existing->second == cmd.content_) return absl::OkStatus();
      return MetaDomainRejectError(
          "policy version already has different raw content");
    }
    const std::uint64_t current = versions.rbegin()->first;
    if (current == std::numeric_limits<std::uint64_t>::max() ||
        cmd.version_ != current + 1) {
      return MetaDomainRejectError(
          "policy version must be exactly current + 1");
    }
  } else if (cmd.version_ != 1) {
    return MetaDomainRejectError("first policy version must be 1");
  }

  std::uint64_t evicted_bytes = 0;
  if (policy_it != policies_.end() &&
      policy_it->second.size() == kMaxMetaPolicyVersionsPerPolicy) {
    evicted_bytes = policy_it->second.begin()->second.size();
  }
  if (total_content_bytes_ - evicted_bytes + cmd.content_.size() >
      kMaxMetaPolicyTotalBytes) {
    return MetaDomainRejectError("policy total byte cap reached");
  }

  auto& versions = policies_[cmd.policy_id_];
  versions.emplace(cmd.version_, cmd.content_);
  total_content_bytes_ += cmd.content_.size();
  if (versions.size() > kMaxMetaPolicyVersionsPerPolicy) {
    total_content_bytes_ -= versions.begin()->second.size();
    versions.erase(versions.begin());
  }
  return absl::OkStatus();
}

std::optional<MetaPolicyVersionView> MetaPolicyStore::FindVersion(
    const std::string& policy_id, std::uint64_t version) const {
  const auto policy = policies_.find(policy_id);
  if (policy == policies_.end()) return std::nullopt;
  const auto entry = policy->second.find(version);
  if (entry == policy->second.end()) return std::nullopt;
  return MetaPolicyVersionView{policy_id, version, entry->second};
}

std::optional<std::uint64_t> MetaPolicyStore::LatestVersion(
    const std::string& policy_id) const {
  const auto policy = policies_.find(policy_id);
  if (policy == policies_.end() || policy->second.empty()) return std::nullopt;
  return policy->second.rbegin()->first;
}

std::vector<MetaPolicyVersionView> MetaPolicyStore::Versions() const {
  std::vector<MetaPolicyVersionView> result;
  for (const auto& [policy_id, versions] : policies_) {
    for (const auto& [version, state] : versions) {
      result.push_back({policy_id, version, state});
    }
  }
  return result;
}

std::optional<MetaAutomaticUncontrolledFailoverPolicy>
MetaPolicyStore::CurrentAutomaticUncontrolledFailover() const {
  const auto policy =
      policies_.find(std::string(kAutomaticUncontrolledFailoverPolicyId));
  if (policy == policies_.end() || policy->second.empty()) return std::nullopt;
  const auto& [version, state] = *policy->second.rbegin();
  auto decoded = DecodeAutomaticUncontrolledFailoverPolicy(state);
  if (!decoded.ok()) return std::nullopt;
  decoded->version_ = version;
  return *decoded;
}

std::optional<MetaAuthorityLeasePolicy> MetaPolicyStore::CurrentAuthorityLease()
    const {
  const auto policy = policies_.find(std::string(kAuthorityLeasePolicyId));
  if (policy == policies_.end() || policy->second.empty()) return std::nullopt;
  const auto& [version, state] = *policy->second.rbegin();
  auto decoded = DecodeAuthorityLeasePolicy(state);
  if (!decoded.ok()) return std::nullopt;
  decoded->version_ = version;
  return *decoded;
}

std::optional<MetaCandidateRecoveryPolicy>
MetaPolicyStore::CurrentCandidateRecovery() const {
  const auto policy = policies_.find(std::string(kCandidateRecoveryPolicyId));
  if (policy == policies_.end() || policy->second.empty()) return std::nullopt;
  const auto& [version, state] = *policy->second.rbegin();
  auto decoded = DecodeCandidateRecoveryPolicy(state);
  if (!decoded.ok()) return std::nullopt;
  decoded->version_ = version;
  return *decoded;
}

void MetaPolicyStore::WriteSnapshot(MetaWriter& writer) const {
  writer.WriteU16(kMetaFormatVersion);
  writer.WriteCount(static_cast<std::uint32_t>(policies_.size()));
  for (const auto& [policy_id, versions] : policies_) {
    writer.WriteString(policy_id);
    writer.WriteCount(static_cast<std::uint32_t>(versions.size()));
    for (const auto& [version, state] : versions) {
      writer.WriteU64(version);
      writer.WriteString(state);
    }
  }
}

std::string MetaPolicyStore::Serialize() const {
  MetaWriter writer;
  WriteSnapshot(writer);
  return writer.TakeBuffer();
}

std::uint64_t MetaPolicyStore::SerializedSize() const {
  MetaWriter counter(false);
  WriteSnapshot(counter);
  return counter.size();
}

absl::StatusOr<MetaPolicyStore> MetaPolicyStore::Deserialize(
    std::string_view bytes) {
  MetaReader reader(bytes);
  auto schema = reader.ReadU16();
  if (!schema.ok()) return schema.status();
  if (*schema != kMetaFormatVersion) {
    return MetaFailStopError("unknown policy snapshot schema_version");
  }
  auto policy_count = reader.ReadCount(3);
  if (!policy_count.ok()) return policy_count.status();

  MetaPolicyStore store;
  for (std::uint32_t i = 0; i < *policy_count; ++i) {
    auto policy_id = reader.ReadString(kMaxMetaPolicyIdBytes);
    if (!policy_id.ok()) return policy_id.status();
    if (*policy_id != kAutomaticUncontrolledFailoverPolicyId &&
        *policy_id != kAuthorityLeasePolicyId &&
        *policy_id != kCandidateRecoveryPolicyId) {
      return MetaFailStopError("unregistered policy family in snapshot");
    }
    if (store.policies_.contains(std::string(*policy_id))) {
      return MetaFailStopError("duplicate policy family in snapshot");
    }
    auto version_count = reader.ReadCount(kMaxMetaPolicyVersionsPerPolicy);
    if (!version_count.ok()) return version_count.status();
    if (*version_count == 0) {
      return MetaFailStopError("policy family has no versions in snapshot");
    }

    std::map<std::uint64_t, std::string> versions;
    std::uint64_t previous = 0;
    std::uint64_t first = 0;
    for (std::uint32_t v = 0; v < *version_count; ++v) {
      auto version = reader.ReadU64();
      if (!version.ok()) return version.status();
      auto content = reader.ReadString(kMaxMetaPayloadBytes);
      if (!content.ok()) return content.status();
      if (v == 0) {
        first = *version;
        if (first == 0) {
          return MetaFailStopError("policy version zero in snapshot");
        }
      } else if (previous == std::numeric_limits<std::uint64_t>::max() ||
                 *version != previous + 1) {
        return MetaFailStopError("non-consecutive policy history in snapshot");
      }
      if (auto status = ValidateRegisteredDocument(*policy_id, *content);
          !status.ok()) {
        return SnapshotFailure(status);
      }
      versions.emplace(*version, std::string(*content));
      store.total_content_bytes_ += content->size();
      previous = *version;
    }
    if (first != 1 && *version_count != kMaxMetaPolicyVersionsPerPolicy) {
      return MetaFailStopError(
          "short policy history does not start at version 1");
    }
    if (store.total_content_bytes_ > kMaxMetaPolicyTotalBytes) {
      return MetaFailStopError("policy total byte cap exceeded in snapshot");
    }
    store.policies_.emplace(std::string(*policy_id), std::move(versions));
  }
  if (auto status = reader.Finish(); !status.ok()) return status;
  return store;
}

}  // namespace lavik::meta
