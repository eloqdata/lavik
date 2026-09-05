#include "meta/meta_policy_store.h"

#include "absl/strings/str_cat.h"
#include "meta/meta_hash.h"

namespace keylane::meta {

// The shared SHA-256 lives in meta/meta_hash.h (header-only; the meta plane
// links no crypto library — see that header).

namespace {

absl::Status CheckPolicyId(const std::string& policy_id) {
  if (policy_id.empty() || policy_id.size() > kMaxMetaPolicyIdBytes) {
    return MetaDomainRejectError("policy_id empty or over cap");
  }
  return absl::OkStatus();
}

}  // namespace

MetaHash256 MetaPolicyStore::ContentHash(std::string_view content) {
  return MetaSha256(content);
}

absl::Status MetaPolicyStore::Apply(const PutPolicy& cmd) {
  if (auto st = CheckPolicyId(cmd.policy_id_); !st.ok()) return st;
  // Non-empty so the total-byte cap also bounds the version count (header).
  if (cmd.content_.empty() || cmd.content_.size() > kMaxMetaPayloadBytes) {
    return MetaDomainRejectError("content empty or over cap");
  }
  // Content-hash addressing: the declared hash must match the content.
  if (ContentHash(cmd.content_) != cmd.content_hash_) {
    return MetaDomainRejectError(
        absl::StrCat("content_hash mismatch for ", cmd.policy_id_));
  }
  const auto policy_it = policies_.find(cmd.policy_id_);
  if (policy_it != policies_.end()) {
    const auto& versions = policy_it->second;
    if (const auto it = versions.find(cmd.version_); it != versions.end()) {
      // Replay: same slot, same content, still active -> idempotent accept.
      // Retired or different content is a conflict.
      if (!it->second.retired_ && it->second.content_ == cmd.content_) {
        return absl::OkStatus();
      }
      return MetaDomainRejectError(
          absl::StrCat("version ", cmd.version_, " of ", cmd.policy_id_,
                       " already exists with different state"));
    }
    if (cmd.version_ <= versions.rbegin()->first) {
      return MetaDomainRejectError(
          absl::StrCat("version not monotonic for ", cmd.policy_id_));
    }
    if (versions.size() >= kMaxMetaPolicyVersionsPerPolicy) {
      return MetaDomainRejectError("policy version cap reached");
    }
  }
  if (total_content_bytes_ + cmd.content_.size() > kMaxMetaPolicyTotalBytes) {
    return MetaDomainRejectError("policy total byte cap reached");
  }
  VersionState state;
  state.content_ = cmd.content_;
  state.content_hash_ = cmd.content_hash_;
  policies_[cmd.policy_id_].emplace(cmd.version_, std::move(state));
  total_content_bytes_ += cmd.content_.size();
  return absl::OkStatus();
}

absl::Status MetaPolicyStore::Apply(const RetirePolicy& cmd) {
  if (auto st = CheckPolicyId(cmd.policy_id_); !st.ok()) return st;
  const auto policy_it = policies_.find(cmd.policy_id_);
  if (policy_it == policies_.end()) {
    return MetaDomainRejectError(
        absl::StrCat("unknown policy ", cmd.policy_id_));
  }
  const auto it = policy_it->second.find(cmd.version_);
  if (it == policy_it->second.end()) {
    return MetaDomainRejectError(
        absl::StrCat("unknown version ", cmd.version_, " of ", cmd.policy_id_));
  }
  // Replay: already retired -> idempotent accept. Retirement keeps the
  // content (tombstone) and never reverses.
  //
  // The guard against retiring a version still referenced by an active grant
  // or non-terminal operation is cross-store: MetaStateApply performs
  // it using IsVersionPresent/IsVersionActive plus the grant/operation stores.
  it->second.retired_ = true;
  return absl::OkStatus();
}

bool MetaPolicyStore::IsVersionPresent(const std::string& policy_id,
                                       std::uint64_t version) const {
  const auto policy = policies_.find(policy_id);
  return policy != policies_.end() && policy->second.contains(version);
}

bool MetaPolicyStore::IsVersionActive(const std::string& policy_id,
                                      std::uint64_t version) const {
  const auto policy = policies_.find(policy_id);
  if (policy == policies_.end()) return false;
  const auto it = policy->second.find(version);
  return it != policy->second.end() && !it->second.retired_;
}

std::optional<MetaPolicyVersionView> MetaPolicyStore::FindVersion(
    const std::string& policy_id, std::uint64_t version) const {
  const auto policy = policies_.find(policy_id);
  if (policy == policies_.end()) return std::nullopt;
  const auto it = policy->second.find(version);
  if (it == policy->second.end()) return std::nullopt;
  MetaPolicyVersionView view;
  view.policy_id_ = policy_id;
  view.version_ = version;
  view.content_ = it->second.content_;
  view.content_hash_ = it->second.content_hash_;
  view.retired_ = it->second.retired_;
  return view;
}

std::optional<std::uint64_t> MetaPolicyStore::LatestVersion(
    const std::string& policy_id) const {
  const auto policy = policies_.find(policy_id);
  if (policy == policies_.end() || policy->second.empty()) return std::nullopt;
  return policy->second.rbegin()->first;
}

// Envelope: schema_version u16 | policy count u32 | per policy (sorted):
// policy_id, version count u32, versions ascending (version u64, retired u8,
// content, hash). See the header for the strictness contract.
std::string MetaPolicyStore::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaCurrentSchemaVersion);
  w.WriteCount(static_cast<std::uint32_t>(policies_.size()));
  for (const auto& [policy_id, versions] : policies_) {
    w.WriteString(policy_id);
    w.WriteCount(static_cast<std::uint32_t>(versions.size()));
    for (const auto& [version, state] : versions) {
      w.WriteU64(version);
      w.WriteU8(state.retired_ ? 1 : 0);
      w.WriteString(state.content_);
      WriteFixedArray(w, state.content_hash_);
    }
  }
  return w.TakeBuffer();
}

absl::StatusOr<MetaPolicyStore> MetaPolicyStore::Deserialize(
    std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version < kMetaMinReadableSchemaVersion ||
      *version > kMetaCurrentSchemaVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  // Every policy holds at least one non-empty content byte, so the total
  // byte cap also bounds the policy count.
  auto policy_count = r.ReadCount(kMaxMetaPolicyTotalBytes);
  if (!policy_count.ok()) return policy_count.status();

  MetaPolicyStore store;
  for (std::uint32_t i = 0; i < *policy_count; ++i) {
    auto policy_id = r.ReadString(kMaxMetaPolicyIdBytes);
    if (!policy_id.ok()) return policy_id.status();
    auto version_count = r.ReadCount(kMaxMetaPolicyVersionsPerPolicy);
    if (!version_count.ok()) return version_count.status();
    if (policy_id->empty()) {
      return MetaFailStopError("empty policy_id in snapshot");
    }
    if (*version_count == 0) {
      return MetaFailStopError("policy with no versions in snapshot");
    }
    if (store.policies_.contains(std::string(*policy_id))) {
      return MetaFailStopError("duplicate policy_id in snapshot");
    }
    std::map<std::uint64_t, VersionState> versions;
    for (std::uint32_t v = 0; v < *version_count; ++v) {
      auto version_no = r.ReadU64();
      if (!version_no.ok()) return version_no.status();
      auto retired = r.ReadU8();
      if (!retired.ok()) return retired.status();
      if (*retired > 1) {
        return MetaFailStopError("retired tag must be 0 or 1");
      }
      auto content = r.ReadString(kMaxMetaPayloadBytes);
      if (!content.ok()) return content.status();
      auto hash = ReadFixedArray<32>(r);
      if (!hash.ok()) return hash.status();
      if (content->empty()) {
        return MetaFailStopError("empty content in snapshot");
      }
      // Content-hash addressing holds in the snapshot too: a corrupt
      // snapshot fails identically on every node.
      if (ContentHash(*content) != *hash) {
        return MetaFailStopError("content_hash mismatch in snapshot");
      }
      VersionState state;
      state.content_ = std::string(*content);
      state.content_hash_ = *hash;
      state.retired_ = *retired == 1;
      if (!versions.emplace(*version_no, std::move(state)).second) {
        return MetaFailStopError("duplicate policy version in snapshot");
      }
      store.total_content_bytes_ += content->size();
    }
    if (store.total_content_bytes_ > kMaxMetaPolicyTotalBytes) {
      return MetaFailStopError("policy total byte cap exceeded in snapshot");
    }
    store.policies_.emplace(std::string(*policy_id), std::move(versions));
  }
  if (auto st = r.Finish(); !st.ok()) return st;
  return store;
}

}  // namespace keylane::meta
