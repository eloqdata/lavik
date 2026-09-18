/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/statusor.h"

namespace lavik::meta {

class MetaStateMachine;

// An owned byte string at the C++ proposal boundary. The Go bridge copies it
// during submission; neither runtime borrows the other's managed memory.
class MetaRaftBuffer {
 public:
  static std::shared_ptr<MetaRaftBuffer> alloc(std::size_t size) {
    return std::make_shared<MetaRaftBuffer>(size);
  }
  explicit MetaRaftBuffer(std::size_t size) : bytes_(size) {}
  std::uint8_t* data_begin() { return bytes_.data(); }
  const std::uint8_t* data_begin() const { return bytes_.data(); }
  std::size_t size() const { return bytes_.size(); }

 private:
  std::vector<std::uint8_t> bytes_;
};

enum class MetaRaftResultCode { OK, CANCELLED, NOT_LEADER, FAILED, TIMEOUT };

// Completion registration and resolution are each single-shot. The small
// per-request mutex never covers user callbacks, disk I/O, or a runtime wait.
class MetaRaftResult {
 public:
  using handler_type2 =
      std::function<void(MetaRaftResult&, std::shared_ptr<std::exception>&)>;
  void when_ready(handler_type2 handler);
  void Complete(MetaRaftResultCode code, std::shared_ptr<MetaRaftBuffer> data,
                std::uint64_t index = 0);
  // Result fields are immutable after completion. Read them from the handler
  // or after has_result() observes publication; submission alone is no proof.
  MetaRaftResultCode get_result_code() const { return code_; }
  std::shared_ptr<MetaRaftBuffer>& get() { return data_; }
  std::uint64_t index() const { return index_; }
  bool has_result() const {
    std::lock_guard lock(mutex_);
    return ready_;
  }
  bool get_accepted() const { return code_ == MetaRaftResultCode::OK; }
  std::string get_result_str() const {
    return std::to_string(static_cast<int>(code_));
  }

 private:
  mutable std::mutex mutex_;
  bool ready_ = false;
  bool registered_ = false;
  handler_type2 handler_;
  MetaRaftResultCode code_ = MetaRaftResultCode::CANCELLED;
  std::shared_ptr<MetaRaftBuffer> data_;
  std::uint64_t index_ = 0;
};

// Application-facing member descriptors contain no consensus-library types.
class MetaRaftMember {
 public:
  MetaRaftMember(std::int32_t id, std::int32_t dc, std::string endpoint,
                 std::string aux, bool learner = false,
                 std::int32_t priority = 1)
      : id_(id),
        dc_(dc),
        endpoint_(std::move(endpoint)),
        aux_(std::move(aux)),
        learner_(learner),
        priority_(priority) {}
  std::int32_t get_id() const { return id_; }
  std::int32_t get_dc_id() const { return dc_; }
  const std::string& get_endpoint() const { return endpoint_; }
  const std::string& get_aux() const { return aux_; }
  bool is_learner() const { return learner_; }
  bool is_new_joiner() const { return false; }
  std::int32_t get_priority() const { return priority_; }

 private:
  std::int32_t id_, dc_;
  std::string endpoint_, aux_;
  bool learner_;
  std::int32_t priority_;
};

class MetaRaftConfig {
 public:
  std::uint64_t get_log_idx() const { return index_; }
  const std::vector<std::shared_ptr<MetaRaftMember>>& get_servers() const {
    return members_;
  }
  bool is_async_replication() const { return false; }
  std::string get_user_ctx() const { return {}; }
  std::uint64_t index_ = 0;
  std::vector<std::shared_ptr<MetaRaftMember>> members_;
};

struct MetaRaftPeerProgress {
  std::int32_t id_ = 0;
  std::uint64_t last_sm_committed_idx_ = 0;
  std::uint64_t last_succ_resp_us_ = 0;
};

struct MetaRaftOptions {
  std::int32_t id_ = 0;
  std::string data_dir_, listen_, local_raft_, local_data_, local_admin_;
  std::string tls_ca_, tls_cert_, tls_key_;
  std::vector<std::shared_ptr<MetaRaftMember>> initial_;
  std::uint32_t heartbeat_ms_ = 100;
  std::uint32_t election_ms_ = 300;
  std::uint32_t client_timeout_ms_ = 5000;
  std::uint64_t snapshot_distance_ = 1000;
  std::uint64_t reserved_log_items_ = 100;
  // Invoked from the Go protocol owner. Must only record an ordered edge and
  // enqueue a bounded Bycorf notification; never wait for the worker.
  std::function<void(bool, std::uint64_t)> role_;
  // Optional fault-injection seam on the application executor. Production
  // leaves it empty; blocking this hook must not block ticks, WAL, or sockets.
  std::function<void()> before_apply_;
};

// MetaRaft is the sole C++/Go ownership boundary. Go owns consensus, peer
// sockets, WAL, and snapshot files. A Go application executor calls the C++
// state machine serially; a separate C++ observer publishes immutable status.
// Shutdown joins every callback producer before destroying either side.
class MetaRaft : public std::enable_shared_from_this<MetaRaft> {
 public:
  static absl::StatusOr<std::shared_ptr<MetaRaft>> Open(
      MetaRaftOptions options, MetaStateMachine& machine);
  ~MetaRaft();
  void shutdown();
  bool is_leader() const {
    return (authority_.load(std::memory_order_acquire) & (kStopped | 1)) == 1;
  }
  bool is_leader_alive() const { return is_leader(); }
  bool is_leader_sm_fully_caught_up() const {
    return is_leader() && caught_up_.load(std::memory_order_acquire);
  }
  std::int32_t get_id() const { return options_.id_; }
  std::int32_t get_leader() const { return leader_id_.load(); }
  std::uint64_t get_term() const { return term_.load(); }
  std::uint64_t get_committed_log_idx() const { return committed_.load(); }
  std::uint64_t get_last_snapshot_idx() const { return snapshot_.load(); }
  std::uint64_t UncompactedBytes() const { return uncompacted_.load(); }
  std::uint64_t FirstLogIndex() const { return first_index_.load(); }
  std::uint64_t VoteRejections() const { return vote_rejections_.load(); }
  std::uint64_t VoteGrants() const { return vote_grants_.load(); }
  std::uint64_t RpcFailures() const { return rpc_failures_.load(); }
  std::uint64_t GcFailures() const { return gc_failures_.load(); }
  std::uint64_t PendingBytes() const { return pending_bytes_.load(); }
  std::uint64_t DurableIndex() const { return durable_.load(); }
  std::shared_ptr<MetaRaftConfig> get_config() const { return config_.load(); }
  std::shared_ptr<MetaRaftMember> get_srv_config(std::int32_t id) const;
  std::vector<MetaRaftPeerProgress> get_peer_info_all() const;
  bool initial_bindings_pending() const;

  std::shared_ptr<MetaRaftResult> append_entries(
      const std::vector<std::shared_ptr<MetaRaftBuffer>>& entries);
  std::shared_ptr<MetaRaftResult> add_srv(const MetaRaftMember& member);
  std::shared_ptr<MetaRaftResult> remove_srv(std::int32_t id);
  void yield_leadership(bool immediate_yield = false);
  struct create_snapshot_options {
    bool serialize_commit_ = true;
  };
  std::uint64_t create_snapshot(create_snapshot_options);
  struct Parameters {
    std::uint32_t client_req_timeout_ = 5000;
  };
  Parameters get_current_params() const {
    return {options_.client_timeout_ms_};
  }

 private:
  MetaRaft(MetaRaftOptions options, MetaStateMachine& machine);
  void Observe();
  bool RefreshStatus();
  void OnRole(std::uint64_t term, std::uint64_t leader, bool is_leader,
              bool caught_up, std::uint64_t resign_index) noexcept;
  void OnResult(std::uint64_t ticket, std::uint64_t index, int code,
                const void* data, std::uint64_t size);
  std::pair<std::uint64_t, std::shared_ptr<MetaRaftResult>> NewResult();

  const MetaRaftOptions options_;
  MetaStateMachine& machine_;
  std::uint64_t handle_ = 0;
  // One CAS binds authority to its revocation generation. An old Go role
  // callback cannot race a caller's resignation and set the leader bit again.
  static constexpr std::uint64_t kStopped = std::uint64_t{1} << 63;
  std::atomic<std::uint64_t> authority_{0};  // stop bit, generation, leader bit
  std::atomic<bool> caught_up_{false}, stopping_{false};
  bool relayed_leader_ = false;  // Only the serial Go role callback owns this.
  std::atomic<std::int32_t> leader_id_{-1};
  std::atomic<std::uint64_t> term_{0}, committed_{0}, snapshot_{0},
      uncompacted_{0}, first_index_{1}, durable_{0}, rpc_failures_{0},
      gc_failures_{0}, pending_bytes_{0}, vote_rejections_{0}, vote_grants_{0};
  std::atomic<std::shared_ptr<MetaRaftConfig>> config_;
  std::atomic<std::shared_ptr<const std::vector<MetaRaftPeerProgress>>>
      progress_;
  std::atomic<std::shared_ptr<const std::vector<std::uint32_t>>> genesis_;
  // Only the proposal executor and Go result callbacks share this bounded
  // registry. Bycorf workers do not enter it; callbacks run after unlocking.
  std::mutex requests_mutex_;
  std::map<std::uint64_t, std::shared_ptr<MetaRaftResult>> requests_;
  std::uint64_t next_ticket_ = 0;
  std::thread observer_;
};

}  // namespace lavik::meta
