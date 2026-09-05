#include "meta/nuraft_state_mgr.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <utility>

#include "absl/status/status.h"
#include "libnuraft/buffer.hxx"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/error_code.hxx"
#include "libnuraft/srv_config.hxx"
#include "libnuraft/srv_state.hxx"
#include "meta/meta_encoding.h"
#include "meta/meta_identity_verifier.h"
#include "meta/nuraft_log_store.h"
#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {

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

// save_state/save_config have no error channel and skipping durability is
// unsafe (vote loss → split brain), so IO failure there is fatal.
[[noreturn]] void FatalStateError(const char* what, const std::string& path,
                                  const absl::Status& status) {
  spdlog::critical("nuraft state manager: {} on {} failed unrecoverably: {}",
                   what, path, status.message());
  std::abort();
}

// Reads the whole file; returns nullptr when it does not exist (first boot).
absl::StatusOr<nuraft::ptr<nuraft::buffer>> ReadWholeFile(
    const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) return nuraft::ptr<nuraft::buffer>(nullptr);
    return ErrnoStatus("open", path);
  }
  std::vector<uint8_t> bytes;
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
    bytes.insert(bytes.end(), chunk, chunk + got);
  }
  ::close(fd);
  nuraft::ptr<nuraft::buffer> out = nuraft::buffer::alloc(bytes.size());
  if (!bytes.empty()) {
    std::memcpy(out->data_begin(), bytes.data(), bytes.size());
  }
  return out;
}

}  // namespace

absl::StatusOr<std::unique_ptr<NuraftStateMgr>> NuraftStateMgr::Open(
    const std::string& data_dir, int32_t server_id,
    const std::string& endpoint) {
  if (::mkdir(data_dir.c_str(), 0755) < 0 && errno != EEXIST) {
    return ErrnoStatus("mkdir", data_dir);
  }

  absl::StatusOr<std::unique_ptr<NuraftLogStore>> log_store =
      NuraftLogStore::Open(data_dir);
  if (!log_store.ok()) return log_store.status();

  absl::StatusOr<nuraft::ptr<nuraft::buffer>> state_blob =
      ReadWholeFile(data_dir + "/srv_state.dat");
  if (!state_blob.ok()) return state_blob.status();
  nuraft::ptr<nuraft::srv_state> state;
  if (*state_blob) state = nuraft::srv_state::deserialize(**state_blob);

  absl::StatusOr<nuraft::ptr<nuraft::buffer>> config_blob =
      ReadWholeFile(data_dir + "/cluster_config.dat");
  if (!config_blob.ok()) return config_blob.status();
  nuraft::ptr<nuraft::cluster_config> config;
  if (*config_blob) {
    config = nuraft::cluster_config::deserialize(**config_blob);
  } else {
    // First boot: the core requires the initial config to contain this
    // server, and no committed config may ever be rolled back to it.
    config = nuraft::cs_new<nuraft::cluster_config>();
    const MetaMemberIdentity identity{
        server_id, "keylane://meta/" + std::to_string(server_id),
        kMetaMinReadableSchemaVersion, kMetaCurrentSchemaVersion};
    config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
        server_id, /*dc_id=*/0, endpoint, identity.EncodeAux(),
        /*learner=*/false));
  }

  return std::unique_ptr<NuraftStateMgr>(new NuraftStateMgr(
      data_dir, server_id, nuraft::ptr<NuraftLogStore>(std::move(*log_store)),
      state, config));
}

NuraftStateMgr::NuraftStateMgr(
    std::string data_dir, int32_t server_id,
    nuraft::ptr<NuraftLogStore> log_store,
    nuraft::ptr<nuraft::srv_state> initial_state,
    nuraft::ptr<nuraft::cluster_config> initial_config)
    : data_dir_(std::move(data_dir)),
      server_id_(server_id),
      log_store_(std::move(log_store)),
      state_(std::move(initial_state)),
      config_(std::move(initial_config)) {}

nuraft::ptr<nuraft::cluster_config> NuraftStateMgr::load_config() {
  std::lock_guard<std::mutex> lock(mutex_);
  return config_;
}

void NuraftStateMgr::save_config(const nuraft::cluster_config& config) {
  nuraft::ptr<nuraft::buffer> blob = config.serialize();
  std::lock_guard<std::mutex> lock(mutex_);
  WriteFileAtomically("cluster_config.dat", *blob, "save_config");
  config_ = nuraft::cluster_config::deserialize(*blob);
}

void NuraftStateMgr::save_state(const nuraft::srv_state& state) {
  nuraft::ptr<nuraft::buffer> blob = state.serialize();
  std::lock_guard<std::mutex> lock(mutex_);
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
  const std::string path = data_dir_ + "/" + name;
  const std::string tmp_path = path + ".tmp";

  int fd =
      ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) FatalStateError(what, tmp_path, ErrnoStatus("open", tmp_path));
  absl::Status status = PwriteAll(fd, blob.data_begin(), blob.size());
  if (status.ok() && ::fdatasync(fd) < 0) {
    status = ErrnoStatus("fdatasync", tmp_path);
  }
  int close_rc = ::close(fd);
  if (status.ok() && close_rc < 0) status = ErrnoStatus("close", tmp_path);
  if (!status.ok()) FatalStateError(what, tmp_path, status);

  if (::rename(tmp_path.c_str(), path.c_str()) < 0) {
    FatalStateError(what, path, ErrnoStatus("rename", path));
  }
  absl::Status dir_status = FsyncDirectory(data_dir_);
  if (!dir_status.ok()) FatalStateError(what, data_dir_, dir_status);
}

}  // namespace keylane::meta
