#include "keylane/meta/identity_verifier.h"

#include <array>
#include <charconv>
#include <limits>
#include <vector>

#include "absl/strings/str_cat.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/encoding.h"
#include "keylane/numeric_endpoint.h"

namespace keylane::meta {
namespace {

constexpr std::string_view kNodePrefix = "keylane://node/";
constexpr std::string_view kMetaPrefix = "keylane://meta/";
constexpr std::string_view kOperatorPrefix = "keylane://operator/";
constexpr std::string_view kAuxPrefix = "KMI1|";

bool IsLowerHex(std::string_view text) {
  for (const char c : text) {
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
  }
  return true;
}

bool IsOperatorName(std::string_view text) {
  if (text.empty()) return false;
  for (const char c : text) {
    const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '.' || c == '_' ||
                    c == '-' || c == ':' || c == '@' || c == '/';
    if (!ok) return false;
  }
  return true;
}

absl::StatusOr<std::uint32_t> ParseCanonicalDecimal(std::string_view text) {
  if (text.empty() || (text.size() > 1 && text.front() == '0')) {
    return absl::InvalidArgumentError("non-canonical decimal server id");
  }
  std::uint32_t value = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size() ||
      value == 0 ||
      value > static_cast<std::uint32_t>(
                  std::numeric_limits<std::int32_t>::max())) {
    return absl::InvalidArgumentError("invalid meta server id");
  }
  return value;
}

}  // namespace

absl::StatusOr<MetaPrincipalIdentity> ParseMetaPrincipal(
    std::string_view principal) {
  if (principal.size() > kMaxMetaPrincipalBytes) {
    return absl::InvalidArgumentError("principal exceeds the committed cap");
  }
  if (principal.starts_with(kNodePrefix)) {
    const std::string_view node_id = principal.substr(kNodePrefix.size());
    if (node_id.size() != 40 || !IsLowerHex(node_id)) {
      return absl::InvalidArgumentError(
          "data-node principal must end in 40 lowercase hex digits");
    }
    return MetaPrincipalIdentity{std::string(principal),
                                 MetaPrincipalRole::kDataNode,
                                 std::string(node_id)};
  }
  if (principal.starts_with(kMetaPrefix)) {
    const std::string_view id = principal.substr(kMetaPrefix.size());
    auto parsed = ParseCanonicalDecimal(id);
    if (!parsed.ok()) return parsed.status();
    return MetaPrincipalIdentity{std::string(principal),
                                 MetaPrincipalRole::kMetaMember,
                                 std::string(id)};
  }
  if (principal.starts_with(kOperatorPrefix)) {
    const std::string_view name = principal.substr(kOperatorPrefix.size());
    if (!IsOperatorName(name)) {
      return absl::InvalidArgumentError(
          "operator principal has an invalid or empty name");
    }
    return MetaPrincipalIdentity{
        std::string(principal), MetaPrincipalRole::kOperator, {}};
  }
  return absl::InvalidArgumentError("unrecognized Keylane principal URI");
}

absl::StatusOr<MetaPrincipalIdentity> AuthenticateMetaUriSans(
    std::span<const std::string> uri_sans) {
  std::vector<MetaPrincipalIdentity> recognized;
  for (const std::string& san : uri_sans) {
    if (!std::string_view(san).starts_with("keylane://")) continue;
    auto parsed = ParseMetaPrincipal(san);
    if (!parsed.ok()) return parsed.status();
    recognized.push_back(std::move(*parsed));
  }
  if (recognized.empty()) {
    return absl::UnauthenticatedError(
        "certificate has no canonical Keylane URI SAN principal");
  }
  if (recognized.size() != 1) {
    return absl::UnauthenticatedError(
        "certificate has multiple Keylane URI SAN principals");
  }
  return std::move(recognized.front());
}

absl::StatusOr<MetaPrincipalIdentity> AuthenticateLocalOperator(
    uid_t peer_uid, std::span<const uid_t> allowed_uids) {
  for (const uid_t allowed : allowed_uids) {
    if (peer_uid == allowed) {
      const std::string principal =
          absl::StrCat("keylane://operator/uid-", peer_uid);
      return MetaPrincipalIdentity{principal, MetaPrincipalRole::kOperator, {}};
    }
  }
  return absl::PermissionDeniedError(
      "Unix peer uid is not authorized for Meta administration");
}

absl::Status ValidateDataNodePrincipal(std::string_view node_id,
                                       std::string_view principal) {
  auto parsed = ParseMetaPrincipal(principal);
  if (!parsed.ok()) return MetaDomainRejectError(parsed.status().message());
  if (parsed->role_ != MetaPrincipalRole::kDataNode ||
      parsed->subject_id_ != node_id) {
    return MetaDomainRejectError(
        "certificate principal does not match the registered node_id");
  }
  return absl::OkStatus();
}

std::string MetaMemberIdentity::EncodeAux() const {
  return absl::StrCat(kAuxPrefix, server_id_, "|", principal_, "|",
                      data_control_endpoint_, "|", ctl_endpoint_);
}

absl::StatusOr<MetaMemberIdentity> MetaMemberIdentity::DecodeAux(
    std::string_view aux) {
  if (!aux.starts_with(kAuxPrefix)) {
    return absl::InvalidArgumentError("missing KMI1 member identity prefix");
  }
  aux.remove_prefix(kAuxPrefix.size());
  std::array<std::string_view, 4> fields;
  for (std::size_t index = 0; index < fields.size() - 1; ++index) {
    const std::size_t separator = aux.find('|');
    if (separator == std::string_view::npos) {
      return absl::InvalidArgumentError("truncated KMI1 member identity");
    }
    fields[index] = aux.substr(0, separator);
    aux.remove_prefix(separator + 1);
  }
  fields.back() = aux;
  const std::string_view server_id_text = fields[0];
  const std::string_view principal_text = fields[1];

  auto server_id = ParseCanonicalDecimal(server_id_text);
  if (!server_id.ok()) return server_id.status();
  auto principal = ParseMetaPrincipal(principal_text);
  if (!principal.ok()) return principal.status();
  if (principal->role_ != MetaPrincipalRole::kMetaMember ||
      principal->subject_id_ != server_id_text) {
    return absl::InvalidArgumentError(
        "member principal does not match its server id");
  }
  const auto data_control = keylane::ParseNumericEndpoint(fields[2]);
  const auto ctl = keylane::ParseNumericEndpoint(fields[3]);
  if (!data_control.has_value() || !ctl.has_value() ||
      keylane::FormatNumericEndpoint(*data_control) != fields[2] ||
      keylane::FormatNumericEndpoint(*ctl) != fields[3]) {
    return absl::InvalidArgumentError(
        "member endpoints are not canonical numeric endpoints");
  }
  return MetaMemberIdentity{static_cast<std::int32_t>(*server_id),
                            std::string(principal_text), std::string(fields[2]),
                            std::string(fields[3])};
}

absl::Status VerifyRaftPeerIdentity(std::int32_t claimed_server_id,
                                    std::span<const std::string> uri_sans,
                                    std::string_view expected_member_aux) {
  auto expected = MetaMemberIdentity::DecodeAux(expected_member_aux);
  if (!expected.ok()) {
    return absl::PermissionDeniedError(
        absl::StrCat("Raft member has no valid committed identity: ",
                     expected.status().message()));
  }
  if (claimed_server_id <= 0 || expected->server_id_ != claimed_server_id) {
    return absl::PermissionDeniedError(
        "Raft request source id does not match member identity");
  }
  auto authenticated = AuthenticateMetaUriSans(uri_sans);
  if (!authenticated.ok()) return authenticated.status();
  if (authenticated->role_ != MetaPrincipalRole::kMetaMember ||
      authenticated->principal_ != expected->principal_) {
    return absl::PermissionDeniedError(
        "Raft peer certificate principal does not match member binding");
  }
  return absl::OkStatus();
}

absl::Status AuthorizeMetaAccess(const MetaPrincipalIdentity& identity,
                                 MetaAccess access,
                                 std::string_view target_node_id) {
  if (identity.role_ == MetaPrincipalRole::kOperator) {
    return absl::OkStatus();
  }
  if (identity.role_ == MetaPrincipalRole::kDataNode) {
    if (access == MetaAccess::kStatus) return absl::OkStatus();
    if (access == MetaAccess::kObservationWrite &&
        target_node_id == identity.subject_id_) {
      return absl::OkStatus();
    }
  }
  return absl::PermissionDeniedError(
      "principal is not authorized for this Meta control operation");
}

}  // namespace keylane::meta
