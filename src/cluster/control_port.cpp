#include "keylane/cluster/control_port.h"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

namespace keylane::cluster {
namespace {

template <typename Integer>
bool ParseUnsigned(std::string_view text, Integer* result) {
  static_assert(std::is_unsigned_v<Integer>);
  if (text.empty()) return false;
  Integer parsed = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, parsed);
  if (error != std::errc{} || parsed_end != end) return false;
  *result = parsed;
  return true;
}

// nodes.conf lines are space-separated words (Redis never emits tabs here).
std::vector<std::string_view> SplitWords(std::string_view line) {
  std::vector<std::string_view> words;
  while (!line.empty()) {
    const std::size_t begin = line.find_first_not_of(' ');
    if (begin == std::string_view::npos) break;
    line.remove_prefix(begin);
    const std::size_t end = line.find(' ');
    words.push_back(line.substr(0, end));
    if (end == std::string_view::npos) break;
    line.remove_prefix(end + 1);
  }
  return words;
}

// Wildcard bind hosts match node entries by port alone (see SelfMatch).
bool IsWildcardHost(std::string_view host) {
  return host.empty() || host == "0.0.0.0" || host == "::";
}

// Parses `<host>:<port>@<cport>` with an optional `,hostname` suffix and
// optional [brackets] around an IPv6 host. The bus port is validated but
// dropped: the data plane never speaks the cluster bus protocol.
absl::StatusOr<std::pair<std::string, std::uint16_t>> ParseNodeAddress(
    std::string_view token) {
  // Redis 7.2 appends `,hostname` after the bus port; treat everything from
  // the first comma as metadata.
  if (const std::size_t comma = token.find(',');
      comma != std::string_view::npos) {
    token = token.substr(0, comma);
  }
  const std::size_t at = token.find('@');
  if (at == std::string_view::npos) {
    return absl::InvalidArgumentError(absl::StrCat(
        "node address '", token, "' misses the '@cport' bus port"));
  }
  std::uint16_t bus_port = 0;
  if (!ParseUnsigned(token.substr(at + 1), &bus_port)) {
    return absl::InvalidArgumentError(
        absl::StrCat("node address '", token, "' has an invalid bus port"));
  }
  const std::string_view endpoint = token.substr(0, at);
  std::string_view host;
  std::string_view port_text;
  if (endpoint.starts_with('[')) {
    const std::size_t close = endpoint.find(']');
    if (close == std::string_view::npos || close + 1 >= endpoint.size() ||
        endpoint[close + 1] != ':') {
      return absl::InvalidArgumentError(
          absl::StrCat("malformed bracketed IPv6 address '", endpoint, "'"));
    }
    host = endpoint.substr(1, close - 1);
    port_text = endpoint.substr(close + 2);
  } else {
    const std::size_t colon = endpoint.rfind(':');
    if (colon == std::string_view::npos) {
      return absl::InvalidArgumentError(
          absl::StrCat("node address '", endpoint, "' misses a port"));
    }
    host = endpoint.substr(0, colon);
    port_text = endpoint.substr(colon + 1);
    // A bare IPv6 literal would mis-split at its last colon; only the
    // bracketed form is accepted.
    if (host.find(':') != std::string_view::npos) {
      return absl::InvalidArgumentError(absl::StrCat(
          "IPv6 node address '", endpoint, "' must use [brackets]"));
    }
  }
  if (host.empty()) {
    return absl::InvalidArgumentError(
        absl::StrCat("node address '", endpoint, "' has an empty host"));
  }
  std::uint16_t port = 0;
  if (!ParseUnsigned(port_text, &port)) {
    return absl::InvalidArgumentError(
        absl::StrCat("node address '", endpoint, "' has an invalid port"));
  }
  return std::pair{std::string(host), port};
}

// Parses one slot token, `<slot>` or `<first>-<last>` (both inclusive).
// Bracketed tokens are Redis migration markers ([slot-<-id] / [slot->-id]);
// v1 has no importing/migrating flow, so they are rejected explicitly instead
// of being silently dropped, which would misstate slot ownership.
absl::StatusOr<SlotRange> ParseSlotToken(std::string_view token) {
  if (token.starts_with('[')) {
    return absl::InvalidArgumentError(
        absl::StrCat("unsupported slot migration marker '", token, "'"));
  }
  std::uint16_t first = 0;
  std::uint16_t last = 0;
  if (const std::size_t dash = token.find('-');
      dash == std::string_view::npos) {
    if (!ParseUnsigned(token, &first)) {
      return absl::InvalidArgumentError(
          absl::StrCat("malformed slot '", token, "'"));
    }
    last = first;
  } else {
    if (!ParseUnsigned(token.substr(0, dash), &first) ||
        !ParseUnsigned(token.substr(dash + 1), &last)) {
      return absl::InvalidArgumentError(
          absl::StrCat("malformed slot range '", token, "'"));
    }
    if (first > last) {
      return absl::InvalidArgumentError(
          absl::StrCat("descending slot range '", token, "'"));
    }
  }
  if (last >= kSlotCount) {
    return absl::InvalidArgumentError(
        absl::StrCat("slot out of range in '", token, "'"));
  }
  return SlotRange{first, last};
}

struct ParsedNode {
  NodeDescriptor node_;
  std::optional<NodeId> primary_id_;
  bool is_primary_ = true;
  std::vector<SlotRange> slots_;
};

absl::Status LineError(std::size_t line_number, std::string_view message) {
  return absl::InvalidArgumentError(
      absl::StrCat("nodes.conf:", line_number, ": ", message));
}

// Adds the line number to an inner parser error without losing its code.
absl::Status LineError(std::size_t line_number, const absl::Status& status) {
  return absl::Status(status.code(), absl::StrCat("nodes.conf:", line_number,
                                                  ": ", status.message()));
}

absl::StatusOr<ParsedNode> ParseNodeLine(
    const std::vector<std::string_view>& fields, std::size_t line_number) {
  // id addr flags master-id ping pong epoch link-state [slots...]
  if (fields.size() < 8) {
    return LineError(line_number, absl::StrCat("node line has ", fields.size(),
                                               " fields, want at least 8"));
  }
  ParsedNode parsed;
  const std::optional<NodeId> node_id = NodeId::Parse(fields[0]);
  if (!node_id.has_value()) {
    return LineError(line_number,
                     absl::StrCat("malformed node id '", fields[0], "'"));
  }
  parsed.node_.node_id_ = *node_id;
  auto address = ParseNodeAddress(fields[1]);
  if (!address.ok()) return LineError(line_number, address.status());
  parsed.node_.SetHost(address->first);
  parsed.node_.port_ = address->second;

  // Only the role flags carry routing meaning; myself/fail?/fail/handshake/
  // noaddr/noflags describe gossip state the static adapter does not track
  // and are tolerated.
  bool master = false;
  bool slave = false;
  std::string_view flags = fields[2];
  while (!flags.empty()) {
    const std::size_t comma = flags.find(',');
    const std::string_view flag = flags.substr(0, comma);
    if (flag == "master") master = true;
    if (flag == "slave") slave = true;
    if (comma == std::string_view::npos) break;
    flags.remove_prefix(comma + 1);
  }
  if (master == slave) {
    return LineError(line_number,
                     master ? "node is flagged both master and slave"
                            : "node is flagged neither master nor slave");
  }
  parsed.is_primary_ = master;

  if (slave) {
    const std::optional<NodeId> primary_id = NodeId::Parse(fields[3]);
    if (!primary_id.has_value()) {
      return LineError(
          line_number,
          absl::StrCat("replica has a malformed primary id '", fields[3], "'"));
    }
    parsed.primary_id_ = *primary_id;
  } else if (fields[3] != "-") {
    return LineError(line_number, "primary line carries a primary id");
  }

  // ping/pong are gossip bookkeeping; validated for shape, otherwise unused.
  std::uint64_t ignored = 0;
  if (!ParseUnsigned(fields[4], &ignored) ||
      !ParseUnsigned(fields[5], &ignored)) {
    return LineError(line_number, "malformed ping/pong timestamps");
  }
  if (!ParseUnsigned(fields[6], &parsed.node_.config_epoch_)) {
    return LineError(line_number,
                     absl::StrCat("malformed config epoch '", fields[6], "'"));
  }
  if (fields[7] == "connected") {
    parsed.node_.link_connected_ = true;
  } else if (fields[7] == "disconnected") {
    parsed.node_.link_connected_ = false;
  } else {
    return LineError(line_number,
                     absl::StrCat("unknown link state '", fields[7], "'"));
  }

  if (fields.size() > 8) {
    // Redis never writes slots on a replica line; treat it as corruption
    // rather than silently re-owning the ranges.
    if (slave) return LineError(line_number, "replica line carries slots");
    for (std::size_t i = 8; i < fields.size(); ++i) {
      auto range = ParseSlotToken(fields[i]);
      if (!range.ok()) return LineError(line_number, range.status());
      parsed.slots_.push_back(*range);
    }
  }
  return parsed;
}

}  // namespace

absl::StatusOr<std::shared_ptr<const ServingState>> StaticClusterControl::Parse(
    std::string_view content, const SelfMatch& self,
    std::uint16_t cluster_tls_port, bool storage_ready,
    std::size_t worker_count) {
  std::vector<ParsedNode> nodes;
  std::uint64_t topology_epoch = 0;
  std::size_t line_number = 0;
  while (!content.empty()) {
    const std::size_t newline = content.find('\n');
    std::string_view line = content.substr(0, newline);
    if (newline == std::string_view::npos) {
      content = {};
    } else {
      content.remove_prefix(newline + 1);
    }
    ++line_number;
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    const std::vector<std::string_view> fields = SplitWords(line);
    if (fields.empty()) continue;       // blank line
    if (fields[0] == "vars") continue;  // currentEpoch/lastVoteEpoch
    auto node = ParseNodeLine(fields, line_number);
    if (!node.ok()) return node.status();
    node->node_.tls_port_ = cluster_tls_port;  // uniform TLS port assumption
    topology_epoch = std::max(topology_epoch, node->node_.config_epoch_);
    nodes.push_back(std::move(*node));
  }
  if (nodes.size() > kNoNodeIndex) {
    return absl::ResourceExhaustedError(
        "nodes.conf has too many nodes for 32-bit node indices");
  }

  // Identify the local entry. The shared file carries no myself mark, so the
  // bind address selects it; a wildcard bind matches on the port alone. The
  // match must be unique or startup would serve under an ambiguous identity.
  const bool wildcard = IsWildcardHost(self.host_);
  NodeIndex matched_index = kNoNodeIndex;
  std::size_t matches = 0;
  for (std::size_t i = 0; i < nodes.size(); ++i) {
    const ParsedNode& node = nodes[i];
    if ((wildcard || node.node_.host() == self.host_) &&
        node.node_.port_ == self.port_) {
      ++matches;
      matched_index = static_cast<NodeIndex>(i);
    }
  }
  if (matches != 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("nodes.conf holds ", matches, " entries for ", self.host_,
                     ":", self.port_, ", want exactly one"));
  }

  // Replica wiring is validated up front: a replica pointing at an absent or
  // non-primary node would corrupt group assembly below.
  for (ParsedNode& node : nodes) {
    if (node.is_primary_) continue;
    NodeIndex primary_index = kNoNodeIndex;
    for (std::size_t i = 0; i < nodes.size(); ++i) {
      if (nodes[i].node_.node_id_ == *node.primary_id_) {
        primary_index = static_cast<NodeIndex>(i);
        break;
      }
    }
    if (primary_index == kNoNodeIndex) {
      return absl::InvalidArgumentError(absl::StrCat(
          "replica ", node.node_.node_id_.ToHexString(),
          " points at unknown primary ", node.primary_id_->ToHexString()));
    }
    if (!nodes[primary_index].is_primary_) {
      return absl::InvalidArgumentError(absl::StrCat(
          "replica ", node.node_.node_id_.ToHexString(),
          " points at non-primary ", node.primary_id_->ToHexString()));
    }
    node.node_.primary_node_index_ = primary_index;
  }

  ServingStateBuilder builder;
  builder.SetTopologyEpoch(topology_epoch);
  builder.SetSelfNodeIndex(matched_index);
  builder.SetInFlightStripeCount(worker_count);
  for (const ParsedNode& node : nodes) builder.AddNode(node.node_);
  for (std::size_t node_index = 0; node_index < nodes.size(); ++node_index) {
    const ParsedNode& node = nodes[node_index];
    // Only slot-owning primaries form a group. An empty primary (mid
    // scale-out) owns nothing, and its replicas serve nothing either.
    if (!node.is_primary_ || node.slots_.empty()) continue;
    GroupView group;
    // The static adapter defines its opaque group id as the primary's Redis
    // node id. Keep GroupId textual because Meta-controlled groups use their
    // committed group namespace instead.
    group.group_id_ = node.node_.node_id_.ToHexString();
    group.primary_node_index_ = static_cast<NodeIndex>(node_index);
    for (std::size_t replica_index = 0; replica_index < nodes.size();
         ++replica_index) {
      if (nodes[replica_index].node_.primary_node_index_ == node_index) {
        group.replica_node_indices_.push_back(
            static_cast<NodeIndex>(replica_index));
      }
    }
    // Statically configured primaries hold a permanent grant; the file has
    // no term concept, so group_term_ stays 0. Reloads still change the
    // authority token through group identity (a moved range lands on a new
    // group id) and through readiness.
    group.granted_ = true;
    group.population_ready_ = storage_ready;
    group.storage_ready_ = storage_ready;
    group.config_epoch_ = node.node_.config_epoch_;
    group.slot_ranges_ = node.slots_;
    builder.AddGroup(std::move(group));
  }
  // Remaining validation (unique ids, existing primaries, in-range and
  // non-overlapping slots) belongs to the builder.
  return builder.Build();
}

StaticClusterControl::StaticClusterControl(std::string path, SelfMatch self,
                                           std::uint16_t cluster_tls_port,
                                           std::size_t worker_count)
    : path_(std::move(path)),
      self_(std::move(self)),
      cluster_tls_port_(cluster_tls_port),
      worker_count_(worker_count) {}

void StaticClusterControl::SetStorageReady(bool ready) {
  storage_ready_.store(ready, std::memory_order_release);
}

celer::Task<absl::Status>
StaticClusterControl::LoseStorageReadinessTransition(
    NodeControlInstaller& installer) {
  std::unique_lock lock(refresh_mutex_);
  // Never let a reload parsed after terminal storage failure carry the old
  // startup-ready bit. Keeping the writer lock until NodeControl has joined
  // every capability also prevents a parse begun earlier from publishing
  // stale readiness behind the failure barrier.
  storage_ready_.store(false, std::memory_order_release);
  co_return co_await installer.LoseStorageReadinessTransition();
}

absl::Status StaticClusterControl::RefreshTarget(
    NodeControlInstaller& installer) {
  std::lock_guard lock(refresh_mutex_);
  // Any failure returns before Publish, so the cache keeps the previously
  // published state (fencing transitions are only ever published complete).
  // Holding the static-adapter writer lock across file IO and installation is
  // intentional: reload is rare, and it prevents a stale parse from being
  // published after worker 0 has announced storage readiness.
  std::ifstream input(path_, std::ios::binary);
  if (!input.is_open()) {
    return absl::NotFoundError(
        absl::StrCat("cannot open cluster nodes file '", path_, "'"));
  }
  const std::string content{std::istreambuf_iterator<char>(input),
                            std::istreambuf_iterator<char>()};
  if (input.bad()) {
    return absl::UnknownError(
        absl::StrCat("cannot read cluster nodes file '", path_, "'"));
  }
  auto state =
      Parse(content, self_, cluster_tls_port_,
            storage_ready_.load(std::memory_order_acquire), worker_count_);
  if (!state.ok()) return state.status();
  const absl::Status readiness =
      installer.SetStorageReady(storage_ready_.load(std::memory_order_acquire));
  if (!readiness.ok()) return readiness;
  Sha256Digest semantic_hash{};
  std::uint64_t content_hash = (*state)->content_hash();
  for (std::size_t i = 0; i < sizeof(content_hash); ++i) {
    semantic_hash[i] = static_cast<std::uint8_t>(content_hash & 0xff);
    content_hash >>= 8;
  }
  const ProjectionBasis basis{
      .source_meta_applied_index_ = ++refresh_revision_,
      .projection_hash_ = semantic_hash,
  };
  return installer.InstallFullState(
      PreparedFullState{.serving_state_ = std::move(*state),
                        .object_hash_ = semantic_hash,
                        .control_groups_ = {}},
      basis);
}

void InMemoryClusterControl::SetTarget(
    std::shared_ptr<const ServingState> state) {
  pending_ = std::move(state);
}

absl::Status InMemoryClusterControl::RefreshTarget(
    NodeControlInstaller& installer) {
  // Nothing pending is a no-op, not an error: the last published state stays
  // in effect until the test installs a new target.
  if (pending_ == nullptr) return absl::OkStatus();
  Sha256Digest semantic_hash{};
  std::uint64_t content_hash = pending_->content_hash();
  for (std::size_t i = 0; i < sizeof(content_hash); ++i) {
    semantic_hash[i] = static_cast<std::uint8_t>(content_hash & 0xff);
    content_hash >>= 8;
  }
  return installer.InstallFullState(
      PreparedFullState{.serving_state_ = std::move(pending_),
                        .object_hash_ = semantic_hash,
                        .control_groups_ = {}},
      ProjectionBasis{.source_meta_applied_index_ = ++refresh_revision_,
                      .projection_hash_ = semantic_hash});
}

}  // namespace keylane::cluster
