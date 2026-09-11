#include "keylane/meta/nuraft_state_mgr.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <set>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/identity_verifier.h"
#include "keylane/meta/nuraft_log_store.h"
#include "keylane/numeric_endpoint.h"
#include "libnuraft/buffer.hxx"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/error_code.hxx"
#include "libnuraft/srv_config.hxx"
#include "libnuraft/srv_state.hxx"
#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {

constexpr std::string_view kInitialBindingsMarker = "KIB1";
constexpr std::string_view kInitialBindingsCompleteMarker = "KIC1";
constexpr std::string_view kWaitingJoinerMarker = "KWJ1";
constexpr std::string_view kRaftStartedMarker = "KRS1";
constexpr std::string_view kTransportBindingsMarker = "KTB1";
constexpr std::size_t kMaxClusterConfigBytes = 4 * 1024 * 1024;
constexpr std::size_t kMaxServerStateBytes = 64 * 1024;
constexpr std::size_t kTransportBindingsHeaderBytes =
    kTransportBindingsMarker.size() + sizeof(std::uint64_t);

absl::Status ErrnoStatus(const char* op, const std::string& path) {
  return absl::ErrnoToStatus(errno, std::string(op) + " failed on " + path);
}

absl::Status PwriteAll(int fd, const uint8_t* data, size_t len) {
  size_t done = 0;
  while (done < len) {
    ssize_t written = ::pwrite(fd, data + done, len - done, done);
    if (written < 0) {
      if (errno == EINTR) continue;
      return absl::ErrnoToStatus(errno, "pwrite");
    }
    done += static_cast<size_t>(written);
  }
  return absl::OkStatus();
}

absl::Status FsyncDirectory(const std::string& dir) {
  int dir_fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dir_fd < 0) return ErrnoStatus("open(dir)", dir);
  int rc = ::fsync(dir_fd);
  int saved_errno = errno;
  ::close(dir_fd);
  if (rc < 0) {
    errno = saved_errno;
    return ErrnoStatus("fsync(dir)", dir);
  }
  return absl::OkStatus();
}

absl::Status WriteFileAtomicallyAt(const std::string& data_dir,
                                   const std::string& name,
                                   const nuraft::buffer& blob) {
  const std::string path = data_dir + "/" + name;
  const std::string tmp_path = path + ".tmp";
  int fd =
      ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) return ErrnoStatus("open", tmp_path);
  absl::Status status = PwriteAll(fd, blob.data_begin(), blob.size());
  if (status.ok() && ::fdatasync(fd) < 0) {
    status = ErrnoStatus("fdatasync", tmp_path);
  }
  const int close_rc = ::close(fd);
  if (status.ok() && close_rc < 0) status = ErrnoStatus("close", tmp_path);
  if (!status.ok()) return status;
  if (::rename(tmp_path.c_str(), path.c_str()) < 0) {
    return ErrnoStatus("rename", path);
  }
  return FsyncDirectory(data_dir);
}

absl::Status RemoveFileDurably(const std::string& data_dir,
                               const std::string& name) {
  const std::string path = data_dir + "/" + name;
  if (::unlink(path.c_str()) < 0 && errno != ENOENT) {
    return ErrnoStatus("unlink", path);
  }
  return FsyncDirectory(data_dir);
}

absl::Status RenameFileDurably(const std::string& data_dir,
                               const std::string& from,
                               const std::string& to) {
  const std::string from_path = data_dir + "/" + from;
  const std::string to_path = data_dir + "/" + to;
  if (::rename(from_path.c_str(), to_path.c_str()) < 0) {
    return ErrnoStatus("rename", from_path + " -> " + to_path);
  }
  return FsyncDirectory(data_dir);
}

nuraft::ptr<nuraft::buffer> BufferFrom(std::string_view bytes) {
  nuraft::ptr<nuraft::buffer> result = nuraft::buffer::alloc(bytes.size());
  if (!bytes.empty()) {
    std::memcpy(result->data_begin(), bytes.data(), bytes.size());
  }
  return result;
}

bool BufferEquals(const nuraft::ptr<nuraft::buffer>& buffer,
                  std::string_view expected) {
  return buffer != nullptr && buffer->size() == expected.size() &&
         std::memcmp(buffer->data_begin(), expected.data(), expected.size()) ==
             0;
}

nuraft::ptr<nuraft::buffer> EncodeInitialBindingsMarker(
    const nuraft::cluster_config& config) {
  const nuraft::ptr<nuraft::buffer> serialized = config.serialize();
  nuraft::ptr<nuraft::buffer> marker =
      nuraft::buffer::alloc(kInitialBindingsMarker.size() + serialized->size());
  std::memcpy(marker->data_begin(), kInitialBindingsMarker.data(),
              kInitialBindingsMarker.size());
  std::memcpy(marker->data_begin() + kInitialBindingsMarker.size(),
              serialized->data_begin(), serialized->size());
  return marker;
}

absl::StatusOr<nuraft::ptr<nuraft::cluster_config>> DecodeInitialBindingsMarker(
    const nuraft::ptr<nuraft::buffer>& marker) {
  if (marker == nullptr || marker->size() <= kInitialBindingsMarker.size() ||
      std::memcmp(marker->data_begin(), kInitialBindingsMarker.data(),
                  kInitialBindingsMarker.size()) != 0) {
    return absl::DataLossError("invalid durable initial-binding marker");
  }
  const std::size_t payload_size =
      marker->size() - kInitialBindingsMarker.size();
  nuraft::ptr<nuraft::buffer> payload = nuraft::buffer::alloc(payload_size);
  std::memcpy(payload->data_begin(),
              marker->data_begin() + kInitialBindingsMarker.size(),
              payload_size);
  try {
    return nuraft::cluster_config::deserialize(*payload);
  } catch (const std::exception& error) {
    return absl::DataLossError(
        std::string("invalid durable initial-binding config: ") + error.what());
  } catch (...) {
    return absl::DataLossError("invalid durable initial-binding config");
  }
}

void EncodeU64(std::uint64_t value, std::uint8_t* out) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    *out++ = static_cast<std::uint8_t>(value >> shift);
  }
}

std::uint64_t DecodeU64(const std::uint8_t* in) {
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < sizeof(value); ++i) {
    value = (value << 8) | in[i];
  }
  return value;
}

struct TransportBindingBaseline {
  nuraft::ptr<nuraft::cluster_config> config_;
  std::uint64_t applied_index_ = 0;
};

nuraft::ptr<nuraft::buffer> EncodeTransportBindingBaseline(
    const nuraft::cluster_config& config, std::uint64_t applied_index) {
  const nuraft::ptr<nuraft::buffer> serialized = config.serialize();
  nuraft::ptr<nuraft::buffer> baseline = nuraft::buffer::alloc(
      kTransportBindingsHeaderBytes + serialized->size());
  std::memcpy(baseline->data_begin(), kTransportBindingsMarker.data(),
              kTransportBindingsMarker.size());
  EncodeU64(applied_index,
            baseline->data_begin() + kTransportBindingsMarker.size());
  std::memcpy(baseline->data_begin() + kTransportBindingsHeaderBytes,
              serialized->data_begin(), serialized->size());
  return baseline;
}

absl::StatusOr<TransportBindingBaseline> DecodeTransportBindingBaseline(
    const nuraft::ptr<nuraft::buffer>& baseline) {
  if (baseline == nullptr || baseline->size() <= kTransportBindingsHeaderBytes ||
      std::memcmp(baseline->data_begin(), kTransportBindingsMarker.data(),
                  kTransportBindingsMarker.size()) != 0) {
    return absl::DataLossError("invalid durable transport-binding baseline");
  }
  TransportBindingBaseline decoded;
  decoded.applied_index_ =
      DecodeU64(baseline->data_begin() + kTransportBindingsMarker.size());
  if (decoded.applied_index_ == 0) {
    return absl::DataLossError(
        "durable transport-binding watermark must be non-zero");
  }
  const std::size_t payload_size =
      baseline->size() - kTransportBindingsHeaderBytes;
  nuraft::ptr<nuraft::buffer> payload = nuraft::buffer::alloc(payload_size);
  std::memcpy(payload->data_begin(),
              baseline->data_begin() + kTransportBindingsHeaderBytes,
              payload_size);
  try {
    decoded.config_ = nuraft::cluster_config::deserialize(*payload);
  } catch (const std::exception& error) {
    return absl::DataLossError(
        std::string("invalid durable transport-binding config: ") +
        error.what());
  } catch (...) {
    return absl::DataLossError("invalid durable transport-binding config");
  }
  return decoded;
}

// save_state/save_config have no error channel and skipping durability is
// unsafe (vote loss → split brain), so IO failure there is fatal.
[[noreturn]] void FatalStateError(const char* what, const std::string& path,
                                  const absl::Status& status) {
  spdlog::critical("nuraft state manager: {} on {} failed unrecoverably: {}",
                   what, path, status.message());
  std::abort();
}

// Reads a bounded file; returns nullptr when it does not exist (first boot).
absl::StatusOr<nuraft::ptr<nuraft::buffer>> ReadWholeFile(
    const std::string& path, std::size_t max_bytes) {
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) return nuraft::ptr<nuraft::buffer>(nullptr);
    return ErrnoStatus("open", path);
  }
  struct stat file_status {};
  if (::fstat(fd, &file_status) < 0) {
    absl::Status status = ErrnoStatus("fstat", path);
    ::close(fd);
    return status;
  }
  if (file_status.st_size < 0 ||
      static_cast<std::uint64_t>(file_status.st_size) > max_bytes) {
    ::close(fd);
    return absl::DataLossError("durable Meta file exceeds its size bound: " +
                               path);
  }
  std::vector<uint8_t> bytes;
  bytes.reserve(static_cast<std::size_t>(file_status.st_size));
  uint8_t chunk[4096];
  while (true) {
    ssize_t got = ::read(fd, chunk, sizeof(chunk));
    if (got < 0) {
      if (errno == EINTR) continue;
      absl::Status status = ErrnoStatus("read", path);
      ::close(fd);
      return status;
    }
    if (got == 0) break;
    if (bytes.size() + static_cast<std::size_t>(got) > max_bytes) {
      ::close(fd);
      return absl::DataLossError(
          "durable Meta file grew beyond its size bound: " + path);
    }
    bytes.insert(bytes.end(), chunk, chunk + got);
  }
  ::close(fd);
  nuraft::ptr<nuraft::buffer> out = nuraft::buffer::alloc(bytes.size());
  if (!bytes.empty()) {
    std::memcpy(out->data_begin(), bytes.data(), bytes.size());
  }
  return out;
}

bool CanonicalEndpoint(std::string_view text) {
  const auto endpoint = keylane::ParseNumericEndpoint(text);
  return endpoint.has_value() &&
         keylane::FormatNumericEndpoint(*endpoint) == text;
}

absl::Status ValidateMember(const NuraftMemberConfig& member) {
  if (member.server_id_ <= 0 ||
      member.principal_ !=
          "keylane://meta/" + std::to_string(member.server_id_)) {
    return absl::InvalidArgumentError("invalid Meta member id or principal");
  }
  if (!CanonicalEndpoint(member.raft_endpoint_) ||
      !CanonicalEndpoint(member.data_control_endpoint_) ||
      !CanonicalEndpoint(member.ctl_endpoint_)) {
    return absl::InvalidArgumentError(
        "Meta member endpoints must be canonical numeric endpoints");
  }
  const MetaMemberIdentity identity{member.server_id_, member.principal_,
                                    member.data_control_endpoint_,
                                    member.ctl_endpoint_};
  if (!MetaMemberIdentity::DecodeAux(identity.EncodeAux()).ok()) {
    return absl::InvalidArgumentError("invalid Meta member descriptor");
  }
  return absl::OkStatus();
}

absl::Status NormalizeInitialMembers(std::vector<NuraftMemberConfig>* members,
                                     const NuraftMemberConfig& local_member) {
  if (members == nullptr || members->empty()) {
    return absl::InvalidArgumentError("initial Meta config is empty");
  }
  std::sort(members->begin(), members->end(),
            [](const auto& left, const auto& right) {
              return left.server_id_ < right.server_id_;
            });
  std::set<std::int32_t> ids;
  std::set<std::string> raft;
  std::set<std::string> data_control;
  std::set<std::string> ctl;
  for (const auto& member : *members) {
    if (absl::Status status = ValidateMember(member); !status.ok()) {
      return status;
    }
    if (!ids.insert(member.server_id_).second ||
        !raft.insert(member.raft_endpoint_).second ||
        !data_control.insert(member.data_control_endpoint_).second ||
        !ctl.insert(member.ctl_endpoint_).second) {
      return absl::InvalidArgumentError(
          "initial Meta config contains a duplicate identity or endpoint");
    }
  }
  const auto local = std::lower_bound(members->begin(), members->end(),
                                      local_member.server_id_,
                                      [](const auto& member, std::int32_t id) {
                                        return member.server_id_ < id;
                                      });
  // The initial Raft address is both NuRaft's advertised endpoint and this
  // process's listener address, so a mismatch would publish an unreachable
  // genesis member. Data-control and ctl values are durable advertised
  // endpoints; their local bind addresses may intentionally sit behind a
  // proxy or load balancer and therefore are not compared here.
  if (local == members->end() || local->principal_ != local_member.principal_ ||
      local->raft_endpoint_ != local_member.raft_endpoint_) {
    return absl::FailedPreconditionError(
        "local Meta identity or Raft endpoint does not match initial config");
  }
  return absl::OkStatus();
}

nuraft::ptr<nuraft::cluster_config> MakeConfig(
    const std::vector<NuraftMemberConfig>& members) {
  auto config = nuraft::cs_new<nuraft::cluster_config>();
  for (const auto& member : members) {
    const MetaMemberIdentity identity{member.server_id_, member.principal_,
                                      member.data_control_endpoint_,
                                      member.ctl_endpoint_};
    config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
        member.server_id_, /*dc_id=*/0, member.raft_endpoint_,
        identity.EncodeAux(), /*learner=*/false, /*priority=*/1));
  }
  return config;
}

absl::Status ValidateLoadedConfig(
    const nuraft::ptr<nuraft::cluster_config>& config,
    const NuraftMemberConfig& local_member, bool require_local) {
  if (!config || config->get_servers().empty() ||
      config->is_async_replication() || !config->get_user_ctx().empty()) {
    return absl::DataLossError("unsupported or empty durable Meta config");
  }
  std::set<std::int32_t> ids;
  std::set<std::string> raft;
  std::set<std::string> data_control;
  std::set<std::string> ctl;
  std::optional<NuraftMemberConfig> loaded_local;
  for (const auto& server : config->get_servers()) {
    if (!server)
      return absl::DataLossError("null server in durable Meta config");
    auto identity = MetaMemberIdentity::DecodeAux(server->get_aux());
    if (!identity.ok() || identity->server_id_ != server->get_id()) {
      return absl::DataLossError(
          "invalid member descriptor in durable Meta config");
    }
    NuraftMemberConfig member{
        .server_id_ = server->get_id(),
        .raft_endpoint_ = server->get_endpoint(),
        .principal_ = identity->principal_,
        .data_control_endpoint_ = identity->data_control_endpoint_,
        .ctl_endpoint_ = identity->ctl_endpoint_,
    };
    if (absl::Status status = ValidateMember(member); !status.ok()) {
      return absl::DataLossError(status.message());
    }
    if (!ids.insert(member.server_id_).second ||
        !raft.insert(member.raft_endpoint_).second ||
        !data_control.insert(member.data_control_endpoint_).second ||
        !ctl.insert(member.ctl_endpoint_).second) {
      return absl::DataLossError(
          "duplicate identity or endpoint in durable Meta config");
    }
    if (member.server_id_ == local_member.server_id_) loaded_local = member;
  }
  if (require_local && !loaded_local.has_value()) {
    return absl::FailedPreconditionError(
        "local Meta identity is absent from durable config");
  }
  // Restart gets no manifest. The durable config remains authoritative for
  // every advertised endpoint while process arguments select local binds;
  // only the stable principal must agree with the requested server id.
  if (loaded_local.has_value() &&
      loaded_local->principal_ != local_member.principal_) {
    return absl::FailedPreconditionError(
        "local Meta principal does not match durable config");
  }
  return absl::OkStatus();
}

bool SameMemberDescriptors(const nuraft::ptr<nuraft::cluster_config>& left,
                           const nuraft::ptr<nuraft::cluster_config>& right) {
  if (left == nullptr || right == nullptr ||
      left->get_servers().size() != right->get_servers().size()) {
    return false;
  }
  for (const auto& expected : left->get_servers()) {
    if (expected == nullptr) return false;
    const auto actual = right->get_server(expected->get_id());
    if (actual == nullptr ||
        actual->get_endpoint() != expected->get_endpoint() ||
        actual->get_aux() != expected->get_aux() ||
        actual->get_dc_id() != expected->get_dc_id() ||
        actual->get_priority() != expected->get_priority() ||
        actual->is_learner() != expected->is_learner() ||
        actual->is_new_joiner() != expected->is_new_joiner()) {
      return false;
    }
  }
  return true;
}

bool SameConfigVersion(const nuraft::ptr<nuraft::cluster_config>& left,
                       const nuraft::ptr<nuraft::cluster_config>& right) {
  return SameMemberDescriptors(left, right) &&
         left->get_log_idx() == right->get_log_idx() &&
         left->get_prev_log_idx() == right->get_prev_log_idx();
}

absl::StatusOr<bool> HasWalSegment(const std::string& data_dir) {
  std::error_code error;
  for (std::filesystem::directory_iterator it(data_dir, error), end;
       !error && it != end; it.increment(error)) {
    const std::string name = it->path().filename().string();
    if (name.starts_with("log-") && name.ends_with(".seg")) return true;
  }
  if (error) {
    return absl::ErrnoToStatus(error.value(),
                               "cannot inspect durable Meta WAL");
  }
  return false;
}

}  // namespace

absl::StatusOr<std::unique_ptr<NuraftStateMgr>> NuraftStateMgr::Open(
    NuraftStateMgrOpenOptions options) {
  if (options.data_dir_.empty()) {
    return absl::InvalidArgumentError("Meta data directory is empty");
  }
  if (absl::Status status = ValidateMember(options.local_member_);
      !status.ok()) {
    return status;
  }
  if (::mkdir(options.data_dir_.c_str(), 0755) < 0 && errno != EEXIST) {
    return ErrnoStatus("mkdir", options.data_dir_);
  }

  std::error_code filesystem_error;
  if (!std::filesystem::is_directory(options.data_dir_, filesystem_error) ||
      filesystem_error) {
    return absl::FailedPreconditionError(
        "Meta data path is not a readable directory");
  }

  absl::StatusOr<nuraft::ptr<nuraft::buffer>> config_blob = ReadWholeFile(
      options.data_dir_ + "/cluster_config.dat", kMaxClusterConfigBytes);
  if (!config_blob.ok()) return config_blob.status();
  absl::StatusOr<nuraft::ptr<nuraft::buffer>> waiting_blob = ReadWholeFile(
      options.data_dir_ + "/waiting_joiner.dat", kWaitingJoinerMarker.size());
  if (!waiting_blob.ok()) return waiting_blob.status();
  absl::StatusOr<nuraft::ptr<nuraft::buffer>> initial_bindings_blob =
      ReadWholeFile(options.data_dir_ + "/initial_bindings.dat",
                    kInitialBindingsMarker.size() + kMaxClusterConfigBytes);
  if (!initial_bindings_blob.ok()) return initial_bindings_blob.status();
  absl::StatusOr<nuraft::ptr<nuraft::buffer>> initial_complete_blob =
      ReadWholeFile(options.data_dir_ + "/initial_bindings_complete.dat",
                    kInitialBindingsCompleteMarker.size());
  if (!initial_complete_blob.ok()) return initial_complete_blob.status();
  absl::StatusOr<nuraft::ptr<nuraft::buffer>> raft_started_blob = ReadWholeFile(
      options.data_dir_ + "/raft_started.dat", kRaftStartedMarker.size());
  if (!raft_started_blob.ok()) return raft_started_blob.status();
  absl::StatusOr<nuraft::ptr<nuraft::buffer>> transport_bindings_blob =
      ReadWholeFile(options.data_dir_ + "/transport_bindings.dat",
                    kTransportBindingsHeaderBytes + kMaxClusterConfigBytes);
  if (!transport_bindings_blob.ok()) return transport_bindings_blob.status();
  absl::StatusOr<nuraft::ptr<nuraft::buffer>> transport_bindings_next_blob =
      ReadWholeFile(options.data_dir_ + "/transport_bindings.next",
                    kTransportBindingsHeaderBytes + kMaxClusterConfigBytes);
  if (!transport_bindings_next_blob.ok()) {
    return transport_bindings_next_blob.status();
  }
  const bool waiting_marker = *waiting_blob != nullptr;
  const bool initial_complete = *initial_complete_blob != nullptr;
  const bool raft_started_marker = *raft_started_blob != nullptr;
  std::optional<TransportBindingBaseline> transport_baseline;
  std::optional<TransportBindingBaseline> transport_next;
  if (*transport_bindings_blob != nullptr) {
    auto decoded = DecodeTransportBindingBaseline(*transport_bindings_blob);
    if (!decoded.ok()) return decoded.status();
    transport_baseline = std::move(*decoded);
  }
  if (*transport_bindings_next_blob != nullptr) {
    auto decoded =
        DecodeTransportBindingBaseline(*transport_bindings_next_blob);
    if (!decoded.ok()) return decoded.status();
    transport_next = std::move(*decoded);
  }
  if (waiting_marker && !BufferEquals(*waiting_blob, kWaitingJoinerMarker)) {
    return absl::DataLossError("invalid durable waiting-joiner marker");
  }
  if (initial_complete &&
      !BufferEquals(*initial_complete_blob, kInitialBindingsCompleteMarker)) {
    return absl::DataLossError("invalid durable initial-binding completion");
  }
  if (raft_started_marker &&
      !BufferEquals(*raft_started_blob, kRaftStartedMarker)) {
    return absl::DataLossError("invalid durable Raft-started marker");
  }
  if (waiting_marker &&
      (*initial_bindings_blob != nullptr || initial_complete)) {
    return absl::DataLossError(
        "waiting-joiner and genesis lifecycle markers conflict");
  }
  nuraft::ptr<nuraft::cluster_config> config;
  nuraft::ptr<nuraft::cluster_config> initial_binding_config;
  NuraftStartupMode startup_mode = NuraftStartupMode::kRestart;
  if (*config_blob) {
    if (options.initial_cluster_.has_value()) {
      return absl::FailedPreconditionError(
          "initial cluster manifest cannot be replayed on initialized Meta "
          "state");
    }
    if ((*config_blob)->size() == 0) {
      return absl::DataLossError("durable Meta config is empty");
    }
    try {
      config = nuraft::cluster_config::deserialize(**config_blob);
    } catch (const std::exception& error) {
      return absl::DataLossError(std::string("invalid durable Meta config: ") +
                                 error.what());
    } catch (...) {
      return absl::DataLossError("invalid durable Meta config");
    }
    if (absl::Status status = ValidateLoadedConfig(
            config, options.local_member_, /*require_local=*/!waiting_marker);
        !status.ok()) {
      return status;
    }
    if (*initial_bindings_blob != nullptr) {
      auto decoded = DecodeInitialBindingsMarker(*initial_bindings_blob);
      if (!decoded.ok()) return decoded.status();
      if (absl::Status status = ValidateLoadedConfig(
              *decoded, options.local_member_, /*require_local=*/true);
          !status.ok()) {
        return absl::DataLossError(
            std::string("invalid initial-binding descriptor set: ") +
            std::string(status.message()));
      }
      if (initial_complete) {
        // CompleteInitialBindings publishes the tombstone before removing the
        // active marker. Finish that crash prefix without reopening grace.
        if (absl::Status status =
                RemoveFileDurably(options.data_dir_, "initial_bindings.dat");
            !status.ok()) {
          return status;
        }
      } else if (SameMemberDescriptors(*decoded, config)) {
        initial_binding_config = std::move(*decoded);
      } else {
        return absl::DataLossError(
            "durable config changed before genesis bindings completed");
      }
    } else if (!initial_complete && !waiting_marker &&
               config->get_log_idx() == 0 && config->get_prev_log_idx() == 0) {
      // Initial config is published before its marker. A crash between those
      // two atomic writes is recoverable without reopening the manifest. A
      // transport baseline proves that genesis completed, in which case a
      // missing tombstone is data loss rather than this publication prefix.
      if (transport_baseline.has_value() || transport_next.has_value()) {
        return absl::FailedPreconditionError(
            "completed zero-index genesis is missing its completion "
            "tombstone");
      }
      if (raft_started_marker) {
        return absl::FailedPreconditionError(
            "started zero-index genesis is missing its initial-binding "
            "marker");
      }
      initial_binding_config = config;
      const nuraft::ptr<nuraft::buffer> marker =
          EncodeInitialBindingsMarker(*config);
      if (absl::Status status = WriteFileAtomicallyAt(
              options.data_dir_, "initial_bindings.dat", *marker);
          !status.ok()) {
        return status;
      }
    }
    // Retain the waiting marker after NuRaft installs the member config. It is
    // the durable authority for the narrow catch-up grace and is cleared only
    // after the state machine applies every descriptor binding through that
    // config entry.
    if (waiting_marker) startup_mode = NuraftStartupMode::kWaitingJoiner;
  } else {
    std::vector<std::string> entries;
    for (std::filesystem::directory_iterator
             it(options.data_dir_, filesystem_error),
         end;
         !filesystem_error && it != end; it.increment(filesystem_error)) {
      entries.push_back(it->path().filename().string());
    }
    if (filesystem_error) {
      return absl::ErrnoToStatus(filesystem_error.value(),
                                 "cannot inspect Meta data directory");
    }
    const bool recoverable_waiting_prefix =
        waiting_marker &&
        entries == std::vector<std::string>{"waiting_joiner.dat"};
    if (!entries.empty() && !recoverable_waiting_prefix) {
      return absl::FailedPreconditionError(
          "Meta data directory is partially initialized without durable "
          "config");
    }
    std::vector<NuraftMemberConfig> members;
    if (options.initial_cluster_.has_value()) {
      if (waiting_marker) {
        return absl::FailedPreconditionError(
            "initial cluster manifest cannot replace waiting-joiner state");
      }
      members = std::move(*options.initial_cluster_);
      startup_mode = NuraftStartupMode::kInitialCluster;
    } else {
      members.push_back(options.local_member_);
      startup_mode = NuraftStartupMode::kWaitingJoiner;
      if (!waiting_marker) {
        const nuraft::ptr<nuraft::buffer> marker =
            BufferFrom(kWaitingJoinerMarker);
        if (absl::Status status = WriteFileAtomicallyAt(
                options.data_dir_, "waiting_joiner.dat", *marker);
            !status.ok()) {
          return status;
        }
      }
    }
    if (absl::Status status =
            NormalizeInitialMembers(&members, options.local_member_);
        !status.ok()) {
      return status;
    }
    config = MakeConfig(members);
    const nuraft::ptr<nuraft::buffer> blob = config->serialize();
    if (absl::Status status = WriteFileAtomicallyAt(
            options.data_dir_, "cluster_config.dat", *blob);
        !status.ok()) {
      return status;
    }
    if (startup_mode == NuraftStartupMode::kInitialCluster) {
      const nuraft::ptr<nuraft::buffer> marker =
          EncodeInitialBindingsMarker(*config);
      if (absl::Status status = WriteFileAtomicallyAt(
              options.data_dir_, "initial_bindings.dat", *marker);
          !status.ok()) {
        return status;
      }
      initial_binding_config = config;
    }
  }

  nuraft::ptr<nuraft::cluster_config> transport_binding_config;
  std::uint64_t transport_binding_index = 0;
  for (const auto* candidate : {&transport_baseline, &transport_next}) {
    if (!candidate->has_value()) continue;
    if (absl::Status status =
            ValidateLoadedConfig((*candidate)->config_, options.local_member_,
                                 /*require_local=*/false);
        !status.ok()) {
      return absl::DataLossError(
          std::string("invalid transport-binding descriptor set: ") +
          std::string(status.message()));
    }
  }
  const bool baseline_matches =
      transport_baseline.has_value() &&
      SameConfigVersion(transport_baseline->config_, config);
  const bool next_matches = transport_next.has_value() &&
                            SameConfigVersion(transport_next->config_, config);
  if (next_matches && !baseline_matches) {
    // save_config durably publishes the candidate before cluster_config.dat,
    // then promotes it. Complete the only crash prefix in which config is new
    // but the old transport baseline is still visible.
    if (absl::Status status =
            RenameFileDurably(options.data_dir_, "transport_bindings.next",
                              "transport_bindings.dat");
        !status.ok()) {
      return status;
    }
    transport_binding_config = transport_next->config_;
    transport_binding_index = transport_next->applied_index_;
  } else if (baseline_matches) {
    if (transport_next.has_value()) {
      // Config still names the current baseline, so a candidate is from the
      // prefix before cluster_config.dat was replaced.
      if (absl::Status status =
              RemoveFileDurably(options.data_dir_, "transport_bindings.next");
          !status.ok()) {
        return status;
      }
    }
    transport_binding_config = transport_baseline->config_;
    transport_binding_index = transport_baseline->applied_index_;
  } else if (transport_baseline.has_value() || transport_next.has_value()) {
    return absl::DataLossError(
        "transport-binding baseline conflicts with durable Meta config");
  }

  absl::StatusOr<nuraft::ptr<nuraft::buffer>> state_blob =
      ReadWholeFile(options.data_dir_ + "/srv_state.dat", kMaxServerStateBytes);
  if (!state_blob.ok()) return state_blob.status();
  nuraft::ptr<nuraft::srv_state> state;
  if (*state_blob) {
    if ((*state_blob)->size() == 0) {
      return absl::DataLossError("durable Meta server state is empty");
    }
    try {
      state = nuraft::srv_state::deserialize(**state_blob);
    } catch (const std::exception& error) {
      return absl::DataLossError(std::string("invalid durable server state: ") +
                                 error.what());
    } catch (...) {
      return absl::DataLossError("invalid durable server state");
    }
  }

  auto has_wal = HasWalSegment(options.data_dir_);
  if (!has_wal.ok()) return has_wal.status();
  if (initial_binding_config != nullptr && !raft_started_marker &&
      (state != nullptr || *has_wal)) {
    return absl::FailedPreconditionError(
        "active genesis with Raft state is missing its started marker");
  }

  // Config indices advance only after NuRaft has durably stored the matching
  // config entry. A completion tombstone likewise proves that genesis binding
  // commands committed. Missing vote or WAL files beyond those boundaries is
  // data loss, not a pristine directory that may be rebuilt from config.
  const bool requires_complete_raft_state =
      !waiting_marker &&
      (raft_started_marker || initial_complete ||
       transport_binding_config != nullptr || config->get_log_idx() != 0 ||
       config->get_prev_log_idx() != 0);
  const bool requires_transport_baseline = requires_complete_raft_state &&
                                           initial_binding_config == nullptr &&
                                           !waiting_marker;
  if (requires_transport_baseline && transport_binding_config == nullptr) {
    return absl::FailedPreconditionError(
        "initialized Meta directory is missing its transport-binding "
        "baseline");
  }
  if (requires_complete_raft_state && state == nullptr) {
    return absl::FailedPreconditionError(
        "initialized Meta directory is missing durable server state");
  }
  if (requires_complete_raft_state && !*has_wal) {
    return absl::FailedPreconditionError(
        "initialized Meta directory is missing its durable WAL");
  }

  absl::StatusOr<std::unique_ptr<NuraftLogStore>> log_store =
      NuraftLogStore::Open(options.data_dir_);
  if (!log_store.ok()) return log_store.status();

  return std::unique_ptr<NuraftStateMgr>(new NuraftStateMgr(
      std::move(options.data_dir_), options.local_member_.server_id_,
      startup_mode, nuraft::ptr<NuraftLogStore>(std::move(*log_store)), state,
      config, initial_binding_config, transport_binding_config,
      transport_binding_index, raft_started_marker));
}

NuraftStateMgr::NuraftStateMgr(
    std::string data_dir, int32_t server_id, NuraftStartupMode startup_mode,
    nuraft::ptr<NuraftLogStore> log_store,
    nuraft::ptr<nuraft::srv_state> initial_state,
    nuraft::ptr<nuraft::cluster_config> initial_config,
    nuraft::ptr<nuraft::cluster_config> initial_binding_config,
    nuraft::ptr<nuraft::cluster_config> transport_binding_config,
    std::uint64_t transport_binding_index, bool raft_started_marker)
    : data_dir_(std::move(data_dir)),
      server_id_(server_id),
      startup_mode_(startup_mode),
      log_store_(std::move(log_store)),
      state_(std::move(initial_state)),
      config_(std::move(initial_config)),
      initial_binding_config_(std::move(initial_binding_config)),
      transport_binding_config_(std::move(transport_binding_config)),
      transport_binding_index_(transport_binding_index),
      raft_started_marker_(raft_started_marker),
      initial_bindings_pending_(initial_binding_config_ != nullptr),
      waiting_joiner_marker_(startup_mode ==
                             NuraftStartupMode::kWaitingJoiner) {}

nuraft::ptr<nuraft::cluster_config> NuraftStateMgr::load_config() {
  std::lock_guard<std::mutex> lock(mutex_);
  return config_;
}

void NuraftStateMgr::save_config(const nuraft::cluster_config& config) {
  nuraft::ptr<nuraft::buffer> blob = config.serialize();
  nuraft::ptr<nuraft::cluster_config> next_config =
      nuraft::cluster_config::deserialize(*blob);
  std::lock_guard<std::mutex> lock(mutex_);
  if (initial_bindings_pending_.load(std::memory_order_relaxed)) {
    if (!SameMemberDescriptors(initial_binding_config_, next_config)) {
      FatalStateError(
          "change membership before genesis bindings converged", data_dir_,
          absl::FailedPreconditionError(
              "membership gate invariant was violated"));
    }
    WriteFileAtomically("cluster_config.dat", *blob, "save_config");
    config_ = std::move(next_config);
    return;
  }
  if (waiting_joiner_marker_.load(std::memory_order_relaxed)) {
    // A new joiner cannot assert convergence merely because NuRaft installed
    // its config. Transport clears this lifecycle only after the identity
    // projection catches up through the config entry.
    WriteFileAtomically("cluster_config.dat", *blob, "save_config");
    config_ = std::move(next_config);
    return;
  }

  const std::uint64_t config_index = std::max(
      static_cast<std::uint64_t>(next_config->get_log_idx()),
      static_cast<std::uint64_t>(next_config->get_prev_log_idx()));
  const std::uint64_t applied_index =
      std::max(transport_binding_index_, config_index);
  const absl::Status status = PublishTransportBindingBaselineLocked(
      *next_config, applied_index, /*transactional_with_config=*/true);
  if (!status.ok()) {
    FatalStateError("save config and transport-binding baseline", data_dir_,
                    status);
  }
  config_ = std::move(next_config);
}

absl::Status NuraftStateMgr::CompleteInitialBindings(
    std::uint64_t applied_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!initial_bindings_pending_.load(std::memory_order_relaxed)) {
    return absl::OkStatus();
  }
  if (applied_index == 0) {
    return absl::FailedPreconditionError(
        "initial bindings cannot complete at state-machine index zero");
  }
  if (absl::Status status = PublishTransportBindingBaselineLocked(
          *config_, applied_index, /*transactional_with_config=*/false);
      !status.ok()) {
    return status;
  }
  const nuraft::ptr<nuraft::buffer> complete =
      BufferFrom(kInitialBindingsCompleteMarker);
  if (absl::Status status = WriteFileAtomicallyAt(
          data_dir_, "initial_bindings_complete.dat", *complete);
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          RemoveFileDurably(data_dir_, "initial_bindings.dat");
      !status.ok()) {
    return status;
  }
  initial_binding_config_.reset();
  initial_bindings_pending_.store(false, std::memory_order_release);
  return absl::OkStatus();
}

absl::Status NuraftStateMgr::CompleteWaitingJoinerCatchup(
    std::uint64_t applied_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!waiting_joiner_marker_.load(std::memory_order_relaxed)) {
    return absl::OkStatus();
  }
  // A joining node first replays the inviter's pre-add config. Its bindings
  // may already be converged, but clearing the marker at that cut would make
  // a crash reject the still-legitimate local id before the add config lands.
  if (config_->get_server(server_id_) == nullptr) {
    return absl::FailedPreconditionError(
        "waiting joiner config does not include its local identity");
  }
  if (applied_index == 0 ||
      applied_index < static_cast<std::uint64_t>(config_->get_log_idx())) {
    return absl::FailedPreconditionError(
        "waiting joiner has not applied its installed config");
  }
  if (absl::Status status = PublishTransportBindingBaselineLocked(
          *config_, applied_index, /*transactional_with_config=*/false);
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          RemoveFileDurably(data_dir_, "waiting_joiner.dat");
      !status.ok()) {
    return status;
  }
  waiting_joiner_marker_.store(false, std::memory_order_release);
  return absl::OkStatus();
}

bool NuraftStateMgr::transport_binding_replay_pending(
    std::uint64_t applied_index) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return transport_binding_config_ != nullptr &&
         SameMemberDescriptors(transport_binding_config_, config_) &&
         applied_index < transport_binding_index_;
}

absl::Status NuraftStateMgr::PublishTransportBindingBaselineLocked(
    const nuraft::cluster_config& config, std::uint64_t applied_index,
    bool transactional_with_config) {
  if (applied_index == 0) {
    return absl::FailedPreconditionError(
        "transport-binding watermark must be non-zero");
  }
  const nuraft::ptr<nuraft::buffer> baseline =
      EncodeTransportBindingBaseline(config, applied_index);
  if (transactional_with_config) {
    if (absl::Status status = WriteFileAtomicallyAt(
            data_dir_, "transport_bindings.next", *baseline);
        !status.ok()) {
      return status;
    }
    const nuraft::ptr<nuraft::buffer> config_blob = config.serialize();
    if (absl::Status status = WriteFileAtomicallyAt(
            data_dir_, "cluster_config.dat", *config_blob);
        !status.ok()) {
      return status;
    }
    if (absl::Status status = RenameFileDurably(
            data_dir_, "transport_bindings.next", "transport_bindings.dat");
        !status.ok()) {
      return status;
    }
  } else if (absl::Status status = WriteFileAtomicallyAt(
                 data_dir_, "transport_bindings.dat", *baseline);
             !status.ok()) {
    return status;
  }
  const nuraft::ptr<nuraft::buffer> serialized = config.serialize();
  transport_binding_config_ = nuraft::cluster_config::deserialize(*serialized);
  transport_binding_index_ = applied_index;
  return absl::OkStatus();
}

void NuraftStateMgr::save_state(const nuraft::srv_state& state) {
  nuraft::ptr<nuraft::buffer> blob = state.serialize();
  std::lock_guard<std::mutex> lock(mutex_);
  if (!raft_started_marker_) {
    const nuraft::ptr<nuraft::buffer> marker = BufferFrom(kRaftStartedMarker);
    // Publish the irreversible lifecycle boundary first. A crash before the
    // following vote write is then detected as partial durable state instead
    // of being mistaken for a never-started genesis process.
    WriteFileAtomically("raft_started.dat", *marker, "mark Raft started");
    raft_started_marker_ = true;
  }
  // Durable before return: the core answers the vote right after this call.
  WriteFileAtomically("srv_state.dat", *blob, "save_state");
  state_ = nuraft::srv_state::deserialize(*blob);
}

nuraft::ptr<nuraft::srv_state> NuraftStateMgr::read_state() {
  std::lock_guard<std::mutex> lock(mutex_);
  return state_;
}

nuraft::ptr<nuraft::log_store> NuraftStateMgr::load_log_store() {
  std::lock_guard<std::mutex> lock(mutex_);
  return log_store_;
}

nuraft::int32 NuraftStateMgr::server_id() { return server_id_; }

void NuraftStateMgr::system_exit(int exit_code) {
  const char* message = "unknown";
  const int index = -exit_code;
  if (index >= 0 && index <= 23) message = nuraft::raft_err_msg[index];
  spdlog::critical(
      "nuraft state manager: system_exit({}: {}); aborting per fail-stop "
      "policy",
      exit_code, message);
  std::abort();
}

void NuraftStateMgr::WriteFileAtomically(const std::string& name,
                                         const nuraft::buffer& blob,
                                         const char* what) {
  const absl::Status status = WriteFileAtomicallyAt(data_dir_, name, blob);
  if (!status.ok()) FatalStateError(what, data_dir_ + "/" + name, status);
}

}  // namespace keylane::meta
