#pragma once

// MetaPolicyStore is the metadata control plane's committed policy store. It
// holds versioned policy documents addressed by
// content hash: policy_id -> version -> {content, content_hash, retired}.
//
// Invariants:
//   - content_hash_ is verified at apply: it must equal SHA-256(content), or
//     the command is rejected. The
//     same check runs at snapshot load, fail-stop on mismatch.
//   - Versions are strictly monotonic per policy_id: a PutPolicy must carry a
//     version greater than every existing version of that policy (gaps are
//     legal). An existing version slot is immutable: re-putting the same
//     version with identical content is an idempotent accept, different
//     content is a rejection.
//   - Retired is terminal and content-retaining: RetirePolicy flips the
//     flag; the version keeps its content (tombstone), still counts against
//     every cap, and can never be re-put or reactivated.
//   - State is size-bounded: at most
//     kMaxMetaPolicyVersionsPerPolicy versions per policy_id and
//     kMaxMetaPolicyTotalBytes content bytes across all policies (active +
//     retired). Content must be non-empty so the byte cap also bounds the
//     version count. Over-cap applies are rejected, never silently
//     truncated.
//
// Cross-store scope: whether a version is still referenced by an active
// grant or a non-terminal operation is NOT known here — those references
// live in the grant and operation stores. This store exposes the facts the
// apply dispatcher needs for the RetirePolicy guard (IsVersionPresent /
// IsVersionActive / FindVersion); it performs the cross-store check.
//
// Replay idempotency: re-applying a command
// whose exact post-effect is already present (PutPolicy: same version slot,
// same content, active; RetirePolicy: version already retired) is an
// idempotent accept (no-op); conflicting content is a domain rejection.
//
// Failure classes: domain rejections return MetaDomainRejectError
// (kDomainReject); deserialization failures are fail-stop (kFailStop).
//
// Scope: pure in-memory function of command + committed state — no IO, no
// locks, never reads the local clock, never touches observation state.
//
// Serialization: u16 schema_version envelope; policies sorted by policy_id,
// versions ascending; byte output is deterministic so equal states
// serialize to equal bytes.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"

namespace keylane::meta {

// Read view of one policy version.
struct MetaPolicyVersionView {
  std::string policy_id_;
  std::uint64_t version_ = 0;
  std::string content_;
  MetaHash256 content_hash_{};
  bool retired_ = false;
  bool operator==(const MetaPolicyVersionView&) const = default;
};

class MetaPolicyStore {
 public:
  // SHA-256 of a policy document. Proposers use it to build PutPolicy
  // commands; the store uses the same function to verify content_hash_ at
  // apply and snapshot load.
  static MetaHash256 ContentHash(std::string_view content);

  // Domain-validated apply of the policy commands. Each returns
  // absl::OkStatus() on apply or idempotent accept, and a kDomainReject
  // status otherwise; state is unchanged on rejection.
  absl::Status Apply(const PutPolicy& cmd);
  absl::Status Apply(const RetirePolicy& cmd);

  // Fact queries for the apply dispatcher's RetirePolicy reference guard and
  // for reads. "Present" means the version slot exists, active or
  // retired; "active" means present and not retired.
  bool IsVersionPresent(const std::string& policy_id,
                        std::uint64_t version) const;
  bool IsVersionActive(const std::string& policy_id,
                       std::uint64_t version) const;
  std::optional<MetaPolicyVersionView> FindVersion(const std::string& policy_id,
                                                   std::uint64_t version) const;
  // Highest version of the policy, any status; nullopt when unknown.
  std::optional<std::uint64_t> LatestVersion(
      const std::string& policy_id) const;
  std::vector<MetaPolicyVersionView> Versions() const;
  // Content bytes across all versions of all policies, including retired
  // tombstones (they occupy state until the state itself is compacted).
  std::uint64_t TotalContentBytes() const { return total_content_bytes_; }
  std::size_t PolicyCount() const { return policies_.size(); }

  // Snapshot support: u16 schema_version envelope, deterministic bytes.
  // Serialize cannot fail (state is bounded and hash-valid by
  // construction). Deserialize is strict and every failure is fail-stop,
  // including in-byte invariant violations (hash mismatch, non-increasing
  // versions, cap overflow).
  std::string Serialize() const;
  static absl::StatusOr<MetaPolicyStore> Deserialize(std::string_view bytes);

 private:
  struct VersionState {
    std::string content_;
    MetaHash256 content_hash_{};
    bool retired_ = false;
  };

  // policy_id -> (version -> state); both maps sorted for deterministic
  // serialization.
  std::map<std::string, std::map<std::uint64_t, VersionState>> policies_;
  std::uint64_t total_content_bytes_ = 0;
};

}  // namespace keylane::meta
