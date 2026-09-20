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

#include "replication_internal.h"

namespace lavik {
namespace replication_internal {

absl::StatusOr<RedisPsyncReply> ParseRedisPsyncReply(std::string_view line) {
  const std::vector<std::string_view> words = SplitWords(line);
  if (words.empty()) {
    return absl::InvalidArgumentError("empty Redis PSYNC response");
  }
  if (words[0] == "+FULLRESYNC") {
    RedisPsyncReply reply;
    reply.full_ = true;
    if (words.size() != 3 || !IsReplicationId(words[1]) ||
        !ParseUnsigned(words[2], &reply.offset_)) {
      return absl::InvalidArgumentError(
          "invalid FULLRESYNC response from Redis");
    }
    reply.replid_ = std::string(words[1]);
    return reply;
  }
  if (words[0] == "+CONTINUE") {
    RedisPsyncReply reply;
    if (words.size() == 2) {
      if (!IsReplicationId(words[1])) {
        return absl::InvalidArgumentError(
            "invalid replid in Redis CONTINUE response");
      }
      reply.replid_ = std::string(words[1]);
    } else if (words.size() != 1) {
      return absl::InvalidArgumentError("invalid CONTINUE response from Redis");
    }
    return reply;
  }
  return absl::FailedPreconditionError(
      absl::StrCat("Redis PSYNC failed: ", line));
}

Task<absl::StatusOr<std::string>> ReceiveRedisRdb(TcpStream& stream) {
  auto header = co_await ReadLine(stream);
  if (!header.ok()) co_return header.status();
  if (header->empty() || header->front() != '$' ||
      header->starts_with("$EOF:")) {
    co_return absl::InvalidArgumentError(absl::StrCat(
        "Redis PSYNC did not provide a length-delimited RDB: ", *header));
  }
  std::uint64_t length = 0;
  if (!ParseUnsigned(std::string_view(*header).substr(1), &length) ||
      length == 0 || length > std::numeric_limits<std::size_t>::max()) {
    co_return absl::InvalidArgumentError("invalid Redis RDB bulk length");
  }

  std::array<char, 64> path_template{};
  constexpr std::string_view prefix = "/tmp/lavik-redis-rdb-XXXXXX";
  std::copy(prefix.begin(), prefix.end(), path_template.begin());
  const int fd = ::mkstemp(path_template.data());
  if (fd < 0) {
    co_return absl::InternalError(absl::StrCat(
        "cannot create temporary Redis RDB: ", std::strerror(errno)));
  }
  (void)::fcntl(fd, F_SETFD, FD_CLOEXEC);
  TemporaryRedisRdb file(fd, path_template.data());

  std::array<std::byte, 256 * 1024> buffer{};
  std::uint64_t remaining = length;
  while (remaining != 0) {
    const std::size_t wanted = static_cast<std::size_t>(
        std::min<std::uint64_t>(remaining, buffer.size()));
    auto read =
        co_await stream.ReadSome(std::span<std::byte>(buffer).first(wanted));
    if (!read.ok()) co_return read.status();
    if (*read == 0) {
      co_return absl::UnavailableError(
          "Redis closed connection during RDB transfer");
    }
    absl::Status written = WriteFileAll(
        file.fd(), std::span<const std::byte>(buffer).first(*read));
    if (!written.ok()) co_return written;
    remaining -= *read;
  }
  // Redis replication uses a bulk-style length header but does not append the
  // RESP bulk string CRLF after the RDB payload. The next byte is already the
  // first byte of the incremental command stream.
  absl::Status closed = file.Close();
  if (!closed.ok()) co_return closed;
  co_return file.ReleasePath();
}

bool SameRedisSlotLayout(const RedisClusterTopology& left,
                         const RedisClusterTopology& right) {
  if (left.masters_.size() != right.masters_.size()) return false;
  for (const RedisClusterMaster& expected : left.masters_) {
    if (std::none_of(right.masters_.begin(), right.masters_.end(),
                     [&](const RedisClusterMaster& current) {
                       return current.slots_ == expected.slots_;
                     })) {
      return false;
    }
  }
  return true;
}

std::vector<std::uint16_t> RedisSlotsVector(const RedisSlotSet& slots) {
  std::vector<std::uint16_t> result;
  result.reserve(slots.count());
  for (std::size_t slot = 0; slot < slots.size(); ++slot) {
    if (slots.test(slot)) result.push_back(static_cast<std::uint16_t>(slot));
  }
  return result;
}

std::string FormatRedisSlots(const RedisSlotSet& slots) {
  std::string result;
  for (std::size_t first = 0; first < slots.size();) {
    if (!slots.test(first)) {
      ++first;
      continue;
    }
    std::size_t last = first;
    while (last + 1 < slots.size() && slots.test(last + 1)) ++last;
    if (!result.empty()) result.push_back(',');
    absl::StrAppend(&result, first);
    if (last != first) absl::StrAppend(&result, "-", last);
    first = last + 1;
  }
  return result;
}

absl::StatusOr<RedisClusterTopology> ParseRedisClusterNodes(
    std::string_view body) {
  RedisClusterTopology topology;
  RedisSlotSet assigned;
  while (!body.empty()) {
    const std::size_t newline = body.find('\n');
    std::string_view line = body.substr(0, newline);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    if (newline == std::string_view::npos) {
      body = {};
    } else {
      body.remove_prefix(newline + 1);
    }
    if (line.empty()) continue;
    const std::vector<std::string_view> fields = SplitWords(line);
    if (fields.size() < 8) {
      return absl::InvalidArgumentError("malformed CLUSTER NODES line");
    }
    const bool myself = HasCommaFlag(fields[2], "myself");
    const bool master = HasCommaFlag(fields[2], "master");
    if (myself) topology.self_id_ = std::string(fields[0]);
    if (!master || HasCommaFlag(fields[2], "fail") ||
        HasCommaFlag(fields[2], "fail?") ||
        HasCommaFlag(fields[2], "handshake") ||
        HasCommaFlag(fields[2], "noaddr")) {
      continue;
    }
    RedisClusterMaster entry;
    entry.node_id_ = std::string(fields[0]);
    entry.myself_ = myself;
    auto endpoint = ParseRedisClusterAddress(fields[1]);
    if (!endpoint.ok()) return endpoint.status();
    entry.endpoint_ = std::move(*endpoint);
    for (std::size_t i = 8; i < fields.size(); ++i) {
      const std::string_view token = fields[i];
      if (token.starts_with('[')) {
        return absl::FailedPreconditionError(
            "Redis Cluster is migrating or importing slots");
      }
      std::uint16_t first = 0;
      std::uint16_t last = 0;
      const std::size_t dash = token.find('-');
      if (dash == std::string_view::npos) {
        if (!ParseUnsigned(token, &first)) {
          return absl::InvalidArgumentError("invalid Redis Cluster slot");
        }
        last = first;
      } else if (!ParseUnsigned(token.substr(0, dash), &first) ||
                 !ParseUnsigned(token.substr(dash + 1), &last) ||
                 first > last) {
        return absl::InvalidArgumentError("invalid Redis Cluster slot range");
      }
      if (last >= storage::kLogicalStorageShards) {
        return absl::InvalidArgumentError("Redis Cluster slot is out of range");
      }
      for (std::uint32_t slot = first; slot <= last; ++slot) {
        if (assigned.test(slot)) {
          return absl::FailedPreconditionError(
              absl::StrCat("Redis Cluster masters overlap at slot ", slot));
        }
        assigned.set(slot);
        entry.slots_.set(slot);
      }
    }
    // Redis permits an empty master during scale-out. It is not a replication
    // source until it owns at least one slot.
    if (entry.slots_.any()) topology.masters_.push_back(std::move(entry));
  }
  if (topology.self_id_.empty()) {
    return absl::FailedPreconditionError(
        "CLUSTER NODES did not identify the connected node");
  }
  if (assigned.count() != storage::kLogicalStorageShards) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Redis Cluster has incomplete slot coverage: ", assigned.count(), "/",
        storage::kLogicalStorageShards));
  }
  if (std::none_of(topology.masters_.begin(), topology.masters_.end(),
                   [&](const RedisClusterMaster& node) {
                     return node.node_id_ == topology.self_id_;
                   })) {
    return absl::FailedPreconditionError(
        "the connected Redis Cluster node is not a slot-owning master");
  }
  return topology;
}

absl::StatusOr<ReplicaOfConfig> ParseRedisClusterAddress(
    std::string_view address) {
  if (const std::size_t comma = address.find(',');
      comma != std::string_view::npos) {
    address = address.substr(0, comma);
  }
  if (const std::size_t bus = address.find('@');
      bus != std::string_view::npos) {
    address = address.substr(0, bus);
  }
  std::string_view host;
  std::string_view port_text;
  if (address.starts_with('[')) {
    const std::size_t close = address.find(']');
    if (close == std::string_view::npos || close + 1 >= address.size() ||
        address[close + 1] != ':') {
      return absl::InvalidArgumentError("invalid Redis Cluster IPv6 address");
    }
    host = address.substr(1, close - 1);
    port_text = address.substr(close + 2);
  } else {
    const std::size_t colon = address.rfind(':');
    if (colon == std::string_view::npos) {
      return absl::InvalidArgumentError("invalid Redis Cluster node address");
    }
    host = address.substr(0, colon);
    port_text = address.substr(colon + 1);
  }
  std::uint16_t port = 0;
  if (host.empty() || !ParseUnsigned(port_text, &port) || port == 0) {
    return absl::InvalidArgumentError("invalid Redis Cluster node endpoint");
  }
  return ReplicaOfConfig{std::string(host), port};
}

Task<absl::StatusOr<std::string>> ReadRedisBulkReply(TcpStream& stream) {
  auto header = co_await ReadLine(stream);
  if (!header.ok()) co_return header.status();
  if (header->empty()) {
    co_return absl::InvalidArgumentError("empty Redis reply");
  }
  if (header->front() == '-') {
    co_return absl::FailedPreconditionError(*header);
  }
  if (header->front() != '$') {
    co_return absl::InvalidArgumentError(
        absl::StrCat("Redis bulk reply expected: ", *header));
  }
  std::uint64_t length = 0;
  if (!ParseUnsigned(std::string_view(*header).substr(1), &length) ||
      length > 16ULL * 1024 * 1024) {
    co_return absl::InvalidArgumentError("invalid Redis bulk reply length");
  }
  auto body = co_await ReadExact(stream, static_cast<std::size_t>(length) + 2);
  if (!body.ok()) co_return body.status();
  if (!body->ends_with("\r\n")) {
    co_return absl::InvalidArgumentError("Redis bulk reply is not terminated");
  }
  body->resize(static_cast<std::size_t>(length));
  co_return std::move(*body);
}

}  // namespace replication_internal

auto ReplicationManager::ReplicationGroup::SetUpstream(
    std::optional<ReplicaOfConfig> upstream) -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, upstream = std::move(upstream)]() mutable {
          return SetUpstream(std::move(upstream));
        });
  }
  if (meta_managed_) {
    co_return absl::FailedPreconditionError(
        "REPLICAOF is unavailable in Meta-managed mode");
  }
  if (replication_shutdown_requested_) {
    co_return absl::CancelledError(
        "replication reconfiguration stopped for process shutdown");
  }
  if (failed_stopped_.load(std::memory_order_acquire)) {
    AssertStateOwner();
    co_return absl::FailedPreconditionError(absl::StrCat(
        "replication is failed-stopped until restart: ", failure_reason_));
  }
  if (upstream.has_value() &&
      (upstream->host_.empty() || upstream->port_ == 0)) {
    co_return absl::InvalidArgumentError("invalid replication upstream");
  }
  if (upstream.has_value() && upstream->port_ == listen_port_ &&
      (upstream->host_ == "127.0.0.1" || upstream->host_ == "::1" ||
       EqualCaseInsensitive(upstream->host_, "localhost"))) {
    co_return absl::InvalidArgumentError(
        "replication upstream resolves to this server");
  }

  const auto observed_epoch = role_epoch_.load(std::memory_order_acquire);
  std::optional<UpstreamDiscovery> discovery;
  if (upstream.has_value()) {
    auto prepared = co_await PrepareRedisUpstream(*upstream);
    // Authenticate and obtain a valid PSYNC response before retiring a
    // healthy subscription or closing local admission.
    if (!prepared.ok()) co_return prepared.status();
    discovery = std::move(*prepared);
    if (replication_shutdown_requested_ ||
        role_epoch_.load(std::memory_order_acquire) != observed_epoch) {
      co_return absl::CancelledError(
          "replication changed during upstream discovery");
    }
  }

  // A coordinator may already own automatic teardown. Do not race its
  // candidate abort with the explicit transition's cancellation and join.
  for (;;) {
    bool teardown_running = false;
    {
      AssertStateOwner();
      if (replica_reconfiguration_running_) {
        co_return absl::FailedPreconditionError(
            "another replication role transition is active");
      }
      teardown_running = replica_session_teardown_running_;
      if (!teardown_running) replica_reconfiguration_running_ = true;
    }
    if (!teardown_running) break;
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  struct ReconfigurationGuard {
    bool& running_;
    ~ReconfigurationGuard() {
      AssertStateOwner();
      running_ = false;
    }
  } reconfiguration_guard{replica_reconfiguration_running_};

  // Stop old upstream ingress while database admission is still open. A
  // command that already crossed the apply-FIFO boundary may be waiting for
  // that admission; closing it first would deadlock role transition against
  // the flow that must publish the final applied cursor. Complete read-ahead
  // events that have not started apply remain unacknowledged and are outside
  // the frozen promotion frontier. The reconfiguration gate must already
  // be closed before moving the session: changing role_epoch alone cannot
  // stop the coordinator from retrying with that new epoch while join
  // suspends. Such a retry could invalidate the population being promoted.
  std::optional<std::uint64_t> detached_role_epoch;
  std::vector<std::shared_ptr<ReplicaSession>> draining_sessions;
  bool retire_source_before_gates = false;
  {
    AssertStateOwner();
    const bool had_upstream = upstream_.has_value();
    if (had_upstream || upstream.has_value()) {
      detached_role_epoch =
          role_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
      StoreRole(upstream.has_value() ? ReplicationRole::kConnecting
                                     : ReplicationRole::kSyncing,
                std::memory_order_release);
      // A master full-sync flow can hold the command gates while waiting
      // for its target to acknowledge the catalog. Retire source egress
      // before this transition tries to acquire those same gates.
      retire_source_before_gates = !had_upstream && upstream.has_value();
      if (active_replica_session_ != nullptr) {
        draining_sessions.push_back(std::move(active_replica_session_));
      }
      for (const auto& source : redis_sources_) {
        if (source->session_ == nullptr) continue;
        draining_sessions.push_back(std::move(source->session_));
      }
    }
  }
  for (const auto& session : draining_sessions) {
    absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
    if (!stopped.ok()) {
      LatchReplicationFailure(session->FailStopReason().value_or(
          absl::StrCat("replica cancellation/join outcome is uncertain: ",
                       stopped.message())));
      co_return stopped;
    }
    if (std::optional<std::string> fail_stop = session->FailStopReason();
        fail_stop.has_value()) {
      // The session can cross an uncertain promotion boundary before this
      // role change takes ownership of teardown. A successful join cannot
      // make that storage outcome knowable, and aborting it would mutate
      // state after the uncertainty point.
      LatchReplicationFailure(*fail_stop);
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", *fail_stop));
    }
    if (session->session_id_ != 0) {
      absl::Status discarded =
          co_await storage_->AbortReplicaRoot(session->session_id_);
      if (!discarded.ok()) {
        LatchReplicationFailure(absl::StrCat(
            "replica abort outcome is uncertain: ", discarded.message()));
        co_return discarded;
      }
    }
  }
  if (retire_source_before_gates) {
    absl::Status drained = co_await DrainSourceEgress();
    if (!drained.ok()) co_return drained;
  }

  while (!CloseAllCommandDbGates()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  struct RoleGateGuard {
    ~RoleGateGuard() { OpenAllCommandDbGates(); }
  } role_gate;
  while (CommandDbOperationsActive()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }

  // FUNCTION and FCALL take database admission before the Function guard.
  // Close and drain that admission first so promotion cannot hold the guard
  // while waiting for a command that is itself waiting for the guard. Once
  // the gates are closed, no new catalog user can enter before token capture.
  std::unique_ptr<FunctionCatalogOperationGuard> promotion_catalog_guard;
  if (!upstream.has_value()) {
    promotion_catalog_guard = co_await AcquireFunctionCatalogOperation();
  }

  absl::Status quiesced = co_await storage_->QuiesceExpiration();
  if (!quiesced.ok()) co_return quiesced;
  bool expiration_quiesced = true;
  struct ExpirationResumeGuard {
    storage::StorageEngine* storage_ = nullptr;
    bool* quiesced_ = nullptr;
    ~ExpirationResumeGuard() {
      if (storage_ != nullptr && quiesced_ != nullptr && *quiesced_) {
        storage_->ResumeExpiration();
      }
    }
  } expiration_resume{storage_, &expiration_quiesced};
  if (upstream.has_value()) {
    // Stop maintenance before the destructive replica transition. Tomb
    // Raider checks this boundary again at its safe yield points.
    absl::Status maintenance = co_await storage_->QuiesceTombRaiderForReplica();
    if (!maintenance.ok()) co_return maintenance;
    storage_->SetExpirationAuthority(false);
    storage_->ResumeExpiration();
    expiration_quiesced = false;
  }

  if (failed_stopped_.load(std::memory_order_acquire)) {
    AssertStateOwner();
    co_return absl::FailedPreconditionError(absl::StrCat(
        "replication is failed-stopped until restart: ", failure_reason_));
  }

  std::vector<std::shared_ptr<ReplicaSession>> cancelled;
  bool start_upstream = false;
  bool discard_incomplete_root = false;
  bool promotion_required = false;
  storage::PromotionBase promotion_base;
  {
    AssertStateOwner();
    if (failed_stopped_.load(std::memory_order_relaxed)) {
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", failure_reason_));
    }
    const bool had_upstream = upstream_.has_value();
    const bool retry_promotion =
        !upstream.has_value() && pending_promotion_.has_value();
    bool dataset_valid = native_dataset_valid_.load(std::memory_order_acquire);
    for (const auto& source : redis_sources_) {
      dataset_valid = dataset_valid ||
                      source->dataset_valid_.load(std::memory_order_acquire);
    }
    // A lost transport changes ONLINE to CONNECTING, but the last committed
    // replica root remains a valid promotion candidate. Only an explicit
    // source switch whose replacement full sync has not reached its cut
    // publishes an empty dataset on REPLICAOF NO ONE.
    discard_incomplete_root = !retry_promotion && !upstream.has_value() &&
                              had_upstream && !dataset_valid;
    promotion_required = retry_promotion || (!upstream.has_value() &&
                                             had_upstream && dataset_valid);
    if (retry_promotion) {
      promotion_base = *pending_promotion_;
    } else if (promotion_required) {
      promotion_base.group_id_ = group_id_;
      promotion_base.parent_history_id_ =
          upstream_history_id_.value_or("redis-parent");
      promotion_base.parent_frontier_.history_context_ =
          promotion_base.parent_history_id_;
      if (!redis_sources_.empty()) {
        promotion_base.parent_frontier_.flow_cursors_.reserve(
            redis_sources_.size());
        for (const auto& source : redis_sources_) {
          promotion_base.parent_frontier_.flow_cursors_.push_back(
              source->offset_.load(std::memory_order_acquire) + 1);
        }
      } else if (applied_frontier_ != nullptr) {
        auto snapshot = applied_frontier_->TrySnapshot();
        if (!snapshot.ok()) co_return snapshot.status();
        promotion_base.parent_frontier_.flow_cursors_ = std::move(*snapshot);
      }
      promotion_base.storage_accumulator_ = absl::StrCat(
          "role-epoch:", role_epoch_.load(std::memory_order_relaxed));
      // Retain only the frozen parent-side proof. Population and catalog
      // tokens are refreshed on every retry under their own durability
      // barriers.
      pending_promotion_ = promotion_base;
    }
    if (upstream.has_value()) pending_promotion_.reset();
    SetDesiredUpstream(upstream);
    if (upstream.has_value()) {
      native_dataset_valid_.store(false, std::memory_order_release);
    }
    if (active_replica_session_ != nullptr) {
      cancelled.push_back(std::move(active_replica_session_));
    }
    for (const auto& source : redis_sources_) {
      if (source->session_ != nullptr) cancelled.push_back(source->session_);
    }
    redis_sources_.clear();
    redis_full_sync_session_id_ = 0;
    expected_redis_topology_.reset();
    redis_cluster_ = false;
    redis_topology_fault_ = false;
    redis_topology_monitor_started_ = false;
    initial_redis_connection_pending_ =
        upstream.has_value() && !discovery.has_value();
    replica_session_id_ = 0;
    source_worker_count_ = 0;
    upstream_node_id_.reset();
    upstream_history_id_.reset();
    // An explicit topology change is not an automatic reconnect. Local
    // writes may have occurred while promoted or while following another
    // source, so none of the old per-flow cursors are safe for CONTINUE.
    applied_frontier_.reset();
    std::uint64_t next_epoch = 0;
    if (detached_role_epoch.has_value()) {
      next_epoch = *detached_role_epoch;
    } else {
      next_epoch = role_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
    }
    const bool redis = discovery.has_value();
    redis_psync_.store(redis, std::memory_order_release);
    if (redis) {
      auto source = std::make_shared<RedisSource>();
      source->upstream_ = *upstream;
      source->role_epoch_ = next_epoch;
      source->prepared_ = std::move(discovery->prepared_);
      redis_cluster_ = discovery->redis_cluster_;
      if (redis_cluster_) {
        expected_redis_topology_ = std::move(discovery->topology_);
        source->node_id_ = discovery->self_->node_id_;
        source->slots_ = discovery->self_->slots_;
      } else {
        source->node_id_ = "standalone";
        source->slots_.set();
      }
      redis_sources_.push_back(std::move(source));
    }
    StoreRole(upstream_.has_value() ? ReplicationRole::kConnecting
              : promotion_required  ? ReplicationRole::kSyncing
                                    : ReplicationRole::kMaster,
              std::memory_order_release);
    start_upstream = upstream_.has_value();
  }
  if (promotion_required) {
    // From this point onward any failed durability or child-history step is
    // fail-closed. The role remains syncing and request handling observes
    // LOADING until a later promotion attempt or full sync succeeds.
    storage_->SetReplicaLoading(true);
  }
  for (const auto& session : cancelled) session->Cancel();
  for (const auto& session : cancelled) {
    absl::Status stopped = co_await CancelAndWaitForReplicaFlows(session);
    if (!stopped.ok()) {
      LatchReplicationFailure(session->FailStopReason().value_or(
          absl::StrCat("replica cancellation/join outcome is uncertain: ",
                       stopped.message())));
      co_return stopped;
    }
    if (std::optional<std::string> fail_stop = session->FailStopReason();
        fail_stop.has_value()) {
      // A concurrent session may have crossed an uncertain promotion/cut
      // before reconfiguration took ownership of its teardown. Successful
      // join does not make that storage outcome knowable, and aborting the
      // candidate would be another mutation after the uncertainty point.
      LatchReplicationFailure(*fail_stop);
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", *fail_stop));
    }
    if (session->session_id_ != 0) {
      absl::Status discarded =
          co_await storage_->AbortReplicaRoot(session->session_id_);
      if (!discarded.ok()) {
        LatchReplicationFailure(absl::StrCat(
            "replica abort outcome is uncertain: ", discarded.message()));
        co_return discarded;
      }
    }
  }
  if (start_upstream) {
    absl::Status retired;
    if (retire_source_before_gates) {
      retired = co_await DisableSourceHistory();
    } else {
      retired = co_await RetireSourceHistory();
    }
    if (!retired.ok()) co_return retired;
  }
  if (discard_incomplete_root) {
    absl::Status cleared = co_await storage_->FlushAllDetach();
    if (!cleared.ok()) {
      LatchReplicationFailure(
          absl::StrCat("replica empty-root cleanup outcome is uncertain: ",
                       cleared.message()));
      co_return cleared;
    }
  }
  if (promotion_required) {
    auto prepared = co_await PreparePromotion(std::move(promotion_base));
    if (!prepared.ok()) co_return prepared.status();
    ActivatePreparedPromotion();
    expiration_quiesced = false;
  }
  if (!start_upstream && !promotion_required) {
    if (!storage_->ReplicaRecoveryFenced()) {
      storage_->SetReplicaLoading(false);
      storage_->SetExpirationAuthority(true);
    }
    storage_->ResumeExpiration();
    expiration_quiesced = false;
  }
  {
    AssertStateOwner();
    replica_reconfiguration_running_ = false;
    if (failed_stopped_.load(std::memory_order_relaxed)) {
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", failure_reason_));
    }
  }
  if (start_upstream && StorageIsReady()) StartCoordinator();
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::AddUpstream(ReplicaOfConfig upstream)
    -> Task<absl::Status> {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, upstream = std::move(upstream)]() mutable {
          return AddUpstream(std::move(upstream));
        });
  }
  if (meta_managed_) {
    co_return absl::FailedPreconditionError(
        "ADDREPLICAOF is unavailable in Meta-managed mode");
  }
  if (failed_stopped_.load(std::memory_order_acquire)) {
    AssertStateOwner();
    co_return absl::FailedPreconditionError(absl::StrCat(
        "replication is failed-stopped until restart: ", failure_reason_));
  }
  if (!redis_psync_.load(std::memory_order_acquire) || !redis_cluster_ ||
      !expected_redis_topology_.has_value()) {
    co_return absl::FailedPreconditionError(
        "ADDREPLICAOF requires an active Redis Cluster upstream");
  }
  if (redis_topology_fault_) {
    co_return absl::FailedPreconditionError(
        "Redis Cluster topology is faulted; use REPLICAOF to rebuild it");
  }
  auto discovery = co_await PrepareRedisUpstream(upstream);
  if (!discovery.ok()) co_return discovery.status();
  if (!discovery->redis_cluster_ || !discovery->topology_.has_value() ||
      !discovery->self_.has_value()) {
    co_return absl::FailedPreconditionError(
        "ADDREPLICAOF source is not a Redis Cluster master");
  }
  std::shared_ptr<RedisSource> source;
  {
    AssertStateOwner();
    if (failed_stopped_.load(std::memory_order_relaxed)) {
      co_return absl::FailedPreconditionError(absl::StrCat(
          "replication is failed-stopped until restart: ", failure_reason_));
    }
    if (!redis_psync_.load(std::memory_order_relaxed) || !redis_cluster_ ||
        !expected_redis_topology_.has_value() || redis_topology_fault_) {
      co_return absl::FailedPreconditionError(
          "Redis Cluster replication changed while adding the source");
    }
    if (!SameRedisSlotLayout(*expected_redis_topology_,
                             *discovery->topology_)) {
      co_return absl::FailedPreconditionError(
          "Redis Cluster slot topology changed; reconfigure with "
          "REPLICAOF");
    }
    for (const auto& current : redis_sources_) {
      const RedisSlotSet overlap = current->slots_ & discovery->self_->slots_;
      if (overlap.any()) {
        std::size_t slot = 0;
        while (!overlap.test(slot)) ++slot;
        co_return absl::AlreadyExistsError(
            absl::StrCat("Redis replication slot overlap at slot ", slot));
      }
    }
    source = std::make_shared<RedisSource>();
    source->upstream_ = std::move(upstream);
    source->prepared_ = std::move(discovery->prepared_);
    source->node_id_ = discovery->self_->node_id_;
    source->slots_ = discovery->self_->slots_;
    source->role_epoch_ = role_epoch_.load(std::memory_order_relaxed);
    redis_sources_.push_back(source);
  }
  if (StorageIsReady()) StartRedisCoordinator(source);
  RefreshRedisRole();
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::StoreRedisLink(
    const std::shared_ptr<RedisSource>& source, bool up) noexcept -> void {
  if (source->link_up_.exchange(up, std::memory_order_acq_rel) != up) {
    source->link_state_changed_nanos_.store(SteadyNanos(),
                                            std::memory_order_release);
  }
}

auto ReplicationManager::ReplicationGroup::QueryRedisClusterTopology(
    const ReplicaOfConfig& upstream)
    -> Task<absl::StatusOr<std::optional<RedisClusterTopology>>> {
  auto connected = co_await ConnectTcp(upstream.host_, upstream.port_,
                                       tls_context_, &outbound_sockets_);
  if (!connected.ok()) co_return connected.status();
  TcpStream stream = std::move(*connected);
  ScopedSocketSetMembership membership(&outbound_sockets_, stream.NativeFd());
  absl::Status status =
      co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
  if (!status.ok()) {
    stream.Close().IgnoreError();
    co_return status;
  }
  auto result = co_await QueryRedisClusterTopology(stream);
  stream.Close().IgnoreError();
  co_return result;
}

auto ReplicationManager::ReplicationGroup::QueryRedisClusterTopology(
    TcpStream& stream)
    -> Task<absl::StatusOr<std::optional<RedisClusterTopology>>> {
  const std::vector<std::string> command{"CLUSTER", "NODES"};
  const std::string encoded_command = EncodeRespCommand(command);
  auto status = co_await WriteText(stream, encoded_command);
  if (!status.ok()) co_return status;
  auto body = co_await ReadRedisBulkReply(stream);
  if (!body.ok()) {
    const std::string message(body.status().message());
    if (body.status().code() == absl::StatusCode::kFailedPrecondition &&
        message.find("cluster support disabled") != std::string::npos) {
      co_return std::optional<RedisClusterTopology>{};
    }
    co_return body.status();
  }
  auto topology = ParseRedisClusterNodes(*body);
  if (!topology.ok()) co_return topology.status();
  co_return std::optional<RedisClusterTopology>(std::move(*topology));
}

auto ReplicationManager::ReplicationGroup::DiscoverRedis(TcpStream& stream)
    -> Task<absl::StatusOr<UpstreamDiscovery>> {
  auto topology = co_await QueryRedisClusterTopology(stream);
  if (!topology.ok()) co_return topology.status();
  UpstreamDiscovery result;
  if (!topology->has_value()) co_return result;
  result.redis_cluster_ = true;
  result.topology_ = std::move(**topology);
  for (const RedisClusterMaster& master : result.topology_->masters_) {
    if (master.node_id_ == result.topology_->self_id_) {
      result.self_ = master;
      break;
    }
  }
  if (!result.self_.has_value()) {
    co_return absl::FailedPreconditionError(
        "connected Redis node is not a slot-owning cluster master");
  }
  co_return result;
}

auto ReplicationManager::ReplicationGroup::PrepareRedisUpstreamConnection(
    const ReplicaOfConfig& upstream,
    const std::shared_ptr<TimedSocketContext>& transport)
    -> Task<absl::StatusOr<UpstreamDiscovery>> {
  auto connected = co_await ConnectTcp(
      upstream.host_, upstream.port_, tls_context_, &transport->sockets_, true);
  if (!connected.ok()) co_return connected.status();
  auto prepared = std::make_shared<PreparedRedisConnection>(
      std::move(*connected), transport);
  auto& stream = prepared->stream_;
  absl::Status status =
      co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
  if (!status.ok()) co_return status;
  auto discovery = co_await DiscoverRedis(stream);
  if (!discovery.ok()) co_return discovery.status();
  // The actual Redis handshake determines compatibility. Neither product
  // identity probes nor a native-protocol fallback are part of REPLICAOF.
  auto fresh = std::make_shared<RedisSource>();
  auto reply = co_await StartRedisPsync(stream, fresh);
  if (!reply.ok()) co_return reply.status();
  if (!reply->full_) {
    co_return absl::FailedPreconditionError(
        "Redis accepted partial sync without a valid local dataset");
  }
  prepared->reply_ = std::move(*reply);
  discovery->prepared_ = std::move(prepared);
  co_return std::move(*discovery);
}

auto ReplicationManager::ReplicationGroup::PrepareRedisUpstream(
    const ReplicaOfConfig& upstream)
    -> Task<absl::StatusOr<UpstreamDiscovery>> {
  // A preparation timeout cancels only the new socket; the existing
  // subscription remains live until a normal PSYNC handshake succeeds.
  auto transport = std::make_shared<TimedSocketContext>(&outbound_sockets_);
  const auto epoch = role_epoch_.load(std::memory_order_relaxed);
  bycorf::ThisWorker().self_->Spawn(WatchTimedSockets(transport, [this, epoch] {
    return !replication_shutdown_requested_ &&
           role_epoch_.load(std::memory_order_relaxed) == epoch;
  }));
  auto result = co_await PrepareRedisUpstreamConnection(upstream, transport);
  // Retire the watchdog without closing the socket that will consume RDB.
  // Process shutdown still owns it throughout role transition and adoption.
  if (result.ok() && !result->prepared_->TransferTo(&outbound_sockets_)) {
    result = absl::CancelledError("Redis replication stopped during handshake");
  }
  transport->finished_ = true;
  while (!transport->watcher_finished_) {
    auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  co_return result;
}

auto ReplicationManager::ReplicationGroup::ConnectInitialRedisUpstream()
    -> Task<absl::Status> {
  while (true) {
    ReplicaOfConfig upstream;
    std::uint64_t role_epoch = 0;
    {
      AssertStateOwner();
      if (replication_shutdown_requested_ || !upstream_.has_value() ||
          !initial_redis_connection_pending_) {
        coordinator_started_ = false;
        co_return absl::OkStatus();
      }
      upstream = *upstream_;
      role_epoch = role_epoch_.load(std::memory_order_relaxed);
    }
    auto discovery = co_await PrepareRedisUpstream(upstream);
    if (!discovery.ok()) {
      spdlog::warn("Redis replication handshake for {}:{} failed: {}",
                   upstream.host_, upstream.port_,
                   discovery.status().message());
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, kReconnectDelay);
      if (!slept.ok()) {
        coordinator_started_ = false;
        co_return slept;
      }
      continue;
    }
    {
      AssertStateOwner();
      if (replication_shutdown_requested_ || !upstream_.has_value() ||
          *upstream_ != upstream ||
          role_epoch_.load(std::memory_order_relaxed) != role_epoch ||
          !initial_redis_connection_pending_) {
        coordinator_started_ = false;
        co_return absl::CancelledError("initial upstream was replaced");
      }
      initial_redis_connection_pending_ = false;
      {
        redis_psync_.store(true, std::memory_order_release);
        redis_cluster_ = discovery->redis_cluster_;
        auto source = std::make_shared<RedisSource>();
        source->upstream_ = upstream;
        source->role_epoch_ = role_epoch;
        source->prepared_ = std::move(discovery->prepared_);
        if (redis_cluster_) {
          expected_redis_topology_ = std::move(discovery->topology_);
          source->node_id_ = discovery->self_->node_id_;
          source->slots_ = discovery->self_->slots_;
        } else {
          source->node_id_ = "standalone";
          source->slots_.set();
        }
        redis_sources_.push_back(std::move(source));
      }
    }
    coordinator_started_ = false;
    StartCoordinator();
    co_return absl::OkStatus();
  }
}

auto ReplicationManager::ReplicationGroup::RedisSourceRegistered(
    const std::shared_ptr<RedisSource>& source) const -> bool {
  return std::find(redis_sources_.begin(), redis_sources_.end(), source) !=
         redis_sources_.end();
}

auto ReplicationManager::ReplicationGroup::NextRedisFullSyncSessionId() noexcept
    -> std::uint64_t {
  for (;;) {
    const std::uint64_t candidate = next_redis_full_sync_session_id_.fetch_add(
        1, std::memory_order_relaxed);
    if (candidate != 0) return candidate;
  }
}

auto ReplicationManager::ReplicationGroup::BeginRedisFullSyncAttempt(
    const std::shared_ptr<RedisSource>& source)
    -> Task<absl::StatusOr<std::uint64_t>> {
  // Serialize the short durable invalidation/activation decisions. RDB
  // imports use the same mutex, but network receipt does not, so sources can
  // still download concurrently without allowing an old all-source snapshot
  // to activate across a newer FULLRESYNC.
  std::uint64_t session_id = 0;
  std::vector<std::shared_ptr<ReplicaSession>> replaced_sessions;
  absl::Status invalidated;
  {
    co_await redis_fullsync_mutex_.Lock();
    bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                        bycorf::ThisWorker().self_);
    {
      AssertStateOwner();
      if (replication_shutdown_requested_ ||
          !redis_psync_.load(std::memory_order_relaxed) ||
          !RedisSourceRegistered(source) || redis_topology_fault_ ||
          source->role_epoch_ != role_epoch_.load(std::memory_order_relaxed)) {
        co_return absl::CancelledError("Redis full sync source was replaced");
      }
      if (redis_full_sync_session_id_ == 0) {
        redis_full_sync_session_id_ = NextRedisFullSyncSessionId();
        for (const auto& current : redis_sources_) {
          current->dataset_valid_.store(false, std::memory_order_release);
          current->full_sync_session_id_ = 0;
          current->replid_.reset();
          if (current != source && current->session_ != nullptr) {
            replaced_sessions.push_back(current->session_);
          }
        }
      }
      session_id = redis_full_sync_session_id_;
      source->dataset_valid_.store(false, std::memory_order_release);
      source->full_sync_session_id_ = 0;
      source->replid_.reset();
    }
    RefreshRedisRole();
    invalidated = co_await storage_->BeginReplicaFullSync(session_id);
  }
  absl::Status stopped = absl::OkStatus();
  for (const auto& session : replaced_sessions) {
    absl::Status current = co_await CancelAndWaitForReplicaFlows(session);
    if (stopped.ok() && !current.ok()) stopped = current;
  }
  if (!invalidated.ok()) co_return invalidated;
  if (!stopped.ok()) co_return stopped;
  co_return session_id;
}

auto ReplicationManager::ReplicationGroup::WaitForRedisFullSyncActivation(
    const std::shared_ptr<RedisSource>& source) -> Task<absl::Status> {
  while (storage_->ReplicaRecoveryFenced()) {
    {
      AssertStateOwner();
      if (replication_shutdown_requested_ || !RedisSourceRegistered(source) ||
          redis_topology_fault_ ||
          source->role_epoch_ != role_epoch_.load(std::memory_order_relaxed)) {
        co_return absl::CancelledError(
            "Redis full sync was cancelled before population activation");
      }
    }
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::RefreshRedisRole() -> void {
  bool ready = false;
  bool syncing = false;
  {
    AssertStateOwner();
    if (!redis_psync_.load(std::memory_order_relaxed)) return;
    RedisSlotSet registered;
    bool valid = !redis_sources_.empty() && !redis_topology_fault_;
    for (const auto& source : redis_sources_) {
      registered |= source->slots_;
      valid = valid && source->dataset_valid_.load(std::memory_order_acquire);
      syncing = syncing || source->syncing_.load(std::memory_order_acquire);
    }
    const bool complete =
        !redis_cluster_ ||
        (expected_redis_topology_.has_value() &&
         registered.count() == storage::kLogicalStorageShards &&
         redis_sources_.size() == expected_redis_topology_->masters_.size());
    ready = valid && complete;
  }
  StoreRole(ready ? ReplicationRole::kOnline
                  : (syncing ? ReplicationRole::kSyncing
                             : ReplicationRole::kConnecting),
            std::memory_order_release);
}

auto ReplicationManager::ReplicationGroup::StartRedisTopologyMonitor() -> void {
  if (replication_shutdown_requested_ || redis_topology_monitor_started_ ||
      !redis_cluster_) {
    return;
  }
  redis_topology_monitor_started_ = true;
  const std::uint64_t epoch = role_epoch_.load(std::memory_order_acquire);
  bycorf::ThisWorker().self_->SpawnRoot(RedisTopologyMonitor(epoch));
}

auto ReplicationManager::ReplicationGroup::FaultRedisTopology(
    std::string_view reason) -> void {
  std::vector<std::shared_ptr<ReplicaSession>> sessions;
  {
    AssertStateOwner();
    if (redis_topology_fault_) return;
    redis_topology_fault_ = true;
    for (const auto& source : redis_sources_) {
      StoreRedisLink(source, false);
      source->syncing_ = false;
      if (source->session_ != nullptr) sessions.push_back(source->session_);
    }
  }
  for (const auto& session : sessions) session->Cancel();
  spdlog::error(
      "Redis Cluster topology changed; replication stopped and Lavik "
      "entered LOADING: {}",
      reason);
  RefreshRedisRole();
}

auto ReplicationManager::ReplicationGroup::ApplyStableRedisTopology(
    RedisClusterTopology topology) -> void {
  std::vector<std::shared_ptr<ReplicaSession>> replaced;
  {
    AssertStateOwner();
    for (const auto& source : redis_sources_) {
      auto current =
          std::find_if(topology.masters_.begin(), topology.masters_.end(),
                       [&](const RedisClusterMaster& master) {
                         return master.slots_ == source->slots_;
                       });
      if (current == topology.masters_.end()) continue;
      if (source->node_id_ == current->node_id_ &&
          source->upstream_ == current->endpoint_) {
        continue;
      }
      spdlog::warn(
          "Redis Cluster master for slots {} changed from {} {}:{} to {} "
          "{}:{}",
          FormatRedisSlots(source->slots_), source->node_id_,
          source->upstream_.host_, source->upstream_.port_, current->node_id_,
          current->endpoint_.host_, current->endpoint_.port_);
      source->node_id_ = current->node_id_;
      source->upstream_ = current->endpoint_;
      StoreRedisLink(source, false);
      if (source->session_ != nullptr) replaced.push_back(source->session_);
    }
    expected_redis_topology_ = std::move(topology);
    if (!redis_sources_.empty())
      SetDesiredUpstream(redis_sources_.front()->upstream_);
  }
  for (const auto& session : replaced) session->Cancel();
}

auto ReplicationManager::ReplicationGroup::RedisTopologyMonitor(
    std::uint64_t role_epoch) -> Task<absl::Status> {
  unsigned incompatible_observations = 0;
  while (true) {
    absl::Status slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                   kRedisTopologyPollInterval);
    if (!slept.ok()) co_return slept;
    std::vector<ReplicaOfConfig> endpoints;
    {
      AssertStateOwner();
      if (replication_shutdown_requested_ ||
          !redis_psync_.load(std::memory_order_relaxed) || !redis_cluster_ ||
          redis_topology_fault_ ||
          role_epoch_.load(std::memory_order_relaxed) != role_epoch) {
        if (role_epoch_.load(std::memory_order_relaxed) == role_epoch) {
          redis_topology_monitor_started_ = false;
        }
        co_return absl::OkStatus();
      }
      endpoints.reserve(redis_sources_.size());
      for (const auto& source : redis_sources_) {
        endpoints.push_back(source->upstream_);
      }
    }

    absl::StatusOr<std::optional<RedisClusterTopology>> observed =
        absl::UnavailableError("no Redis Cluster source was reachable");
    for (const ReplicaOfConfig& endpoint : endpoints) {
      observed = co_await QueryRedisClusterTopology(endpoint);
      if (observed.ok() ||
          observed.status().code() != absl::StatusCode::kUnavailable) {
        break;
      }
    }
    if (!observed.ok() &&
        observed.status().code() == absl::StatusCode::kUnavailable) {
      spdlog::warn("Redis Cluster topology check unavailable: {}",
                   observed.status().message());
      continue;
    }

    bool compatible = false;
    if (observed.ok() && observed->has_value()) {
      AssertStateOwner();
      compatible = expected_redis_topology_.has_value() &&
                   SameRedisSlotLayout(*expected_redis_topology_, **observed);
    }
    if (compatible) {
      incompatible_observations = 0;
      ApplyStableRedisTopology(std::move(**observed));
      continue;
    }

    ++incompatible_observations;
    const std::string reason =
        observed.ok()
            ? "source no longer reports a complete Redis Cluster topology"
            : std::string(observed.status().message());
    if (incompatible_observations < 2) {
      spdlog::warn("Redis Cluster topology change awaiting confirmation: {}",
                   reason);
      continue;
    }
    FaultRedisTopology(reason);
    redis_topology_monitor_started_ = false;
    co_return absl::FailedPreconditionError(reason);
  }
}

auto ReplicationManager::ReplicationGroup::StartRedisCoordinator(
    const std::shared_ptr<RedisSource>& source) -> void {
  if (replication_shutdown_requested_ || source->coordinator_started_) return;
  source->coordinator_started_ = true;
  spdlog::info("starting Redis replication coordinator for {}:{}",
               source->upstream_.host_, source->upstream_.port_);
  bycorf::ThisWorker().self_->SpawnRoot(RedisCoordinator(source));
}

auto ReplicationManager::ReplicationGroup::RedisCoordinator(
    std::shared_ptr<RedisSource> source) -> Task<absl::Status> {
  if (replication_shutdown_requested_) {
    source->coordinator_started_ = false;
    co_return absl::CancelledError(
        "Redis replication stopped for process shutdown");
  }
  if (source->node_id_.empty()) {
    auto discovery = co_await PrepareRedisUpstream(source->upstream_);
    if (!discovery.ok()) {
      spdlog::warn("failed to discover Redis source {}:{}: {}",
                   source->upstream_.host_, source->upstream_.port_,
                   discovery.status().message());
      source->coordinator_started_ = false;
      RefreshRedisRole();
      co_return discovery.status();
    }
    {
      AssertStateOwner();
      if (replication_shutdown_requested_ || !RedisSourceRegistered(source) ||
          source->role_epoch_ != role_epoch_.load(std::memory_order_relaxed)) {
        source->coordinator_started_ = false;
        co_return absl::CancelledError("Redis source was replaced");
      }
      source->prepared_ = std::move(discovery->prepared_);
      redis_cluster_ = discovery->redis_cluster_;
      if (redis_cluster_) {
        expected_redis_topology_ = std::move(discovery->topology_);
        source->node_id_ = discovery->self_->node_id_;
        source->slots_ = discovery->self_->slots_;
      } else {
        source->node_id_ = "standalone";
        source->slots_.set();
      }
    }
    RefreshRedisRole();
    if (redis_cluster_) StartRedisTopologyMonitor();
  }

  while (true) {
    auto session = std::make_shared<ReplicaSession>(&outbound_sockets_);
    {
      AssertStateOwner();
      if (replication_shutdown_requested_ ||
          !redis_psync_.load(std::memory_order_relaxed) ||
          !RedisSourceRegistered(source) || redis_topology_fault_ ||
          source->role_epoch_ != role_epoch_.load(std::memory_order_relaxed)) {
        break;
      }
      source->session_ = session;
    }
    absl::Status connected = co_await RunRedisReplicaSession(source, session);
    session->Cancel();
    {
      AssertStateOwner();
      StoreRedisLink(source, false);
      source->syncing_ = false;
      if (source->session_ == session) source->session_.reset();
    }
    RefreshRedisRole();

    bool retry = false;
    {
      AssertStateOwner();
      retry =
          !replication_shutdown_requested_ &&
          redis_psync_.load(std::memory_order_relaxed) &&
          RedisSourceRegistered(source) && !redis_topology_fault_ &&
          source->role_epoch_ == role_epoch_.load(std::memory_order_relaxed);
    }
    if (!retry) break;
    spdlog::warn("Redis replication connection to {}:{} ended: {}",
                 source->upstream_.host_, source->upstream_.port_,
                 connected.message());
    absl::Status slept =
        co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, kReconnectDelay);
    if (!slept.ok()) {
      source->coordinator_started_ = false;
      co_return slept;
    }
  }
  source->coordinator_started_ = false;
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ImportRedisRdb(
    const std::string& path, const std::shared_ptr<RedisSource>& source)
    -> Task<absl::Status> {
  auto reader = rdb::FileReader::Open(path);
  if (!reader.ok()) co_return reader.status();

  std::uint64_t entries = 0;
  std::uint64_t skipped = 0;
  std::vector<std::string> function_libraries;
  while (true) {
    auto next = reader->NextStreaming();
    if (!next.ok()) co_return next.status();
    if (!next->has_value()) break;
    auto drained = reader->DrainCollection();
    if (!drained.ok()) co_return drained;
    if ((**next).kind_ == rdb::FileEntryKind::kValue) {
      ++entries;
    } else if ((**next).kind_ == rdb::FileEntryKind::kFunctionLibrary) {
      function_libraries.push_back((**next).function_code_);
    } else {
      ++skipped;
    }
  }
  absl::Status functions_validated =
      co_await ValidateLuaFunctionCatalog(function_libraries);
  if (!functions_validated.ok()) co_return functions_validated;
  reader->Rewind();

  co_await redis_fullsync_mutex_.Lock();
  bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                      bycorf::ThisWorker().self_);

  while (!CloseAllCommandDbGates()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  struct CommandGateGuard {
    ~CommandGateGuard() { OpenAllCommandDbGates(); }
  } command_gate_guard;
  while (CommandDbOperationsActive()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  {
    AssertStateOwner();
    if (!redis_psync_.load(std::memory_order_relaxed) ||
        source->role_epoch_ != role_epoch_.load(std::memory_order_relaxed) ||
        !RedisSourceRegistered(source) || redis_topology_fault_) {
      co_return absl::CancelledError(
          "Redis full sync was cancelled by a role or topology change");
    }
  }

  const std::vector<std::uint16_t> slots = RedisSlotsVector(source->slots_);
  absl::Status cleared = co_await storage_->ResetPartitionsDetach(slots);
  if (!cleared.ok()) co_return cleared;

  std::uint64_t imported = 0;
  std::uint64_t expired = 0;
  while (true) {
    auto next = reader->NextStreaming();
    if (!next.ok()) {
      (void)co_await storage_->ResetPartitionsDetach(slots);
      co_return next.status();
    }
    if (!next->has_value()) break;
    rdb::FileEntry entry = std::move(**next);
    if (entry.kind_ != rdb::FileEntryKind::kValue) {
      if (entry.kind_ == rdb::FileEntryKind::kFunctionLibrary) {
        continue;
      } else if (entry.kind_ == rdb::FileEntryKind::kSkippedModuleValue) {
        spdlog::warn(
            "Redis PSYNC skipped unsupported Module value db={} "
            "key-bytes={}",
            entry.db_id_, entry.key_.size());
      } else if (entry.kind_ == rdb::FileEntryKind::kSkippedModuleAux) {
        spdlog::warn("Redis PSYNC skipped unsupported Module auxiliary data");
      }
      continue;
    }
    const std::uint16_t slot = storage::RedisSlot(entry.key_);
    if (!source->slots_.test(slot)) {
      (void)co_await storage_->ResetPartitionsDetach(slots);
      co_return absl::FailedPreconditionError(absl::StrCat(
          "Redis RDB key belongs to slot ", slot, " outside source ownership"));
    }
    auto result = co_await rdb::RestoreFileEntry(storage_, &*reader, entry);
    if (!result.ok() || result->busy_) {
      const absl::Status failure =
          result.ok() ? absl::AlreadyExistsError("duplicate key in Redis RDB")
                      : result.status();
      absl::Status discarded = co_await storage_->ResetPartitionsDetach(slots);
      if (!discarded.ok()) {
        co_return absl::InternalError(absl::StrCat(
            "Redis RDB import failed: ", failure.message(),
            "; failed to discard partial import: ", discarded.message()));
      }
      co_return failure;
    }
    if (result->changed_) {
      ++imported;
    } else {
      ++expired;
    }
  }
  absl::Status functions_installed =
      co_await ReplaceLuaFunctionCatalog(function_libraries);
  if (!functions_installed.ok()) {
    (void)co_await storage_->ResetPartitionsDetach(slots);
    co_return functions_installed;
  }
  spdlog::info(
      "Redis PSYNC loaded RDB version={} entries={} imported={} expired={} "
      "unsupported-skipped={} slots={}",
      reader->version(), entries, imported, expired, skipped,
      FormatRedisSlots(source->slots_));
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ExpectRedisReply(
    TcpStream& stream, std::vector<std::string> command,
    std::string_view expected) -> Task<absl::Status> {
  const std::string encoded = EncodeRespCommand(command);
  absl::Status sent = co_await WriteText(stream, encoded);
  if (!sent.ok()) co_return sent;
  auto reply = co_await ReadLine(stream);
  if (!reply.ok()) co_return reply.status();
  if (*reply != expected) {
    co_return absl::FailedPreconditionError(
        absl::StrCat("Redis replication handshake failed: ", *reply));
  }
  co_return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::SendRedisAck(
    TcpStream& stream, const std::shared_ptr<RedisSource>& source)
    -> Task<absl::Status> {
  const std::vector<std::string> command{
      "REPLCONF", "ACK",
      absl::StrCat(source->offset_.load(std::memory_order_acquire))};
  const std::string encoded = EncodeRespCommand(command);
  co_return co_await WriteText(stream, encoded);
}

auto ReplicationManager::ReplicationGroup::ValidateRedisSourceCommand(
    const ReplicatedCommand& command,
    const std::shared_ptr<RedisSource>& source) -> absl::Status {
  if (!redis_cluster_) return absl::OkStatus();
  RespCommand wire{.args_ = command.args_};
  auto request = BuildCommandRequest(std::move(wire), command.db_id_);
  if (!request.ok()) return request.status();
  if (request->kind_ == CommandKind::kFlushDb ||
      request->kind_ == CommandKind::kFlushAll ||
      request->kind_ == CommandKind::kFunction) {
    return absl::OkStatus();
  }
  if (request->spec_ == nullptr) {
    return absl::InvalidArgumentError("unknown Redis replication command");
  }
  auto keys = DetermineKeys(*request->spec_, request->args_);
  if (!keys.ok() || keys->count() == 0) {
    return absl::InvalidArgumentError(
        "Redis replication command has no routable key");
  }
  for (std::uint16_t index = keys->first_; index <= keys->last_;
       index = static_cast<std::uint16_t>(index + keys->step_)) {
    const std::uint16_t slot = storage::RedisSlot(request->args_[index]);
    if (!source->slots_.test(slot)) {
      return absl::FailedPreconditionError(
          absl::StrCat("Redis command key belongs to slot ", slot,
                       " outside source ownership"));
    }
  }
  return absl::OkStatus();
}

auto ReplicationManager::ReplicationGroup::ResetRedisSourceSlots(
    const std::shared_ptr<RedisSource>& source) -> Task<absl::Status> {
  co_await redis_fullsync_mutex_.Lock();
  bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                      bycorf::ThisWorker().self_);
  while (!CloseAllCommandDbGates()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  struct CommandGateGuard {
    ~CommandGateGuard() { OpenAllCommandDbGates(); }
  } reopen;
  while (CommandDbOperationsActive()) {
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  {
    AssertStateOwner();
    if (!RedisSourceRegistered(source) || redis_topology_fault_) {
      co_return absl::CancelledError("Redis source was replaced");
    }
  }
  const std::vector<std::uint16_t> slots = RedisSlotsVector(source->slots_);
  co_return co_await storage_->ResetPartitionsDetach(slots);
}

auto ReplicationManager::ReplicationGroup::ConsumeRedisCommandStream(
    TcpStream& stream, const std::shared_ptr<RedisSource>& source)
    -> Task<absl::Status> {
  RedisCommandStream commands(&stream);
  std::uint8_t db_id = source->selected_db_;
  bool in_multi = false;
  std::uint64_t transaction_bytes = 0;
  std::vector<ReplicatedCommand> transaction;
  while (true) {
    auto wire = co_await commands.Next();
    if (!wire.ok()) co_return wire.status();
    if (source->role_epoch_ != role_epoch_.load(std::memory_order_acquire)) {
      co_return absl::CancelledError(
          "Redis replication source was detached before command apply");
    }
    if (wire->command_.args_.empty()) {
      co_return absl::InvalidArgumentError(
          "empty command in Redis replication stream");
    }
    const std::string name = wire->command_.args_.front();
    if (EqualCaseInsensitive(name, "SELECT")) {
      unsigned selected = 0;
      if (wire->command_.args_.size() != 2 ||
          !ParseUnsigned(wire->command_.args_[1], &selected) ||
          selected >= storage_->database_count()) {
        co_return absl::InvalidArgumentError(
            "invalid SELECT in Redis replication stream");
      }
      db_id = static_cast<std::uint8_t>(selected);
      if (in_multi) {
        transaction_bytes += wire->bytes_;
      } else {
        source->selected_db_ = db_id;
        source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
      }
      continue;
    }
    if (EqualCaseInsensitive(name, "PING")) {
      if (in_multi) {
        co_return absl::InvalidArgumentError(
            "PING inside Redis replicated transaction");
      }
      source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
      absl::Status acked = co_await SendRedisAck(stream, source);
      if (!acked.ok()) co_return acked;
      continue;
    }
    if (EqualCaseInsensitive(name, "REPLCONF")) {
      if (in_multi || wire->command_.args_.size() != 3 ||
          !EqualCaseInsensitive(wire->command_.args_[1], "GETACK")) {
        co_return absl::InvalidArgumentError(
            "unsupported REPLCONF in Redis replication stream");
      }
      source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
      absl::Status acked = co_await SendRedisAck(stream, source);
      if (!acked.ok()) co_return acked;
      continue;
    }
    if (EqualCaseInsensitive(name, "MULTI")) {
      if (in_multi || wire->command_.args_.size() != 1) {
        co_return absl::InvalidArgumentError(
            "invalid MULTI in Redis replication stream");
      }
      in_multi = true;
      transaction.clear();
      transaction_bytes = wire->bytes_;
      continue;
    }
    if (EqualCaseInsensitive(name, "EXEC")) {
      if (!in_multi || wire->command_.args_.size() != 1) {
        co_return absl::InvalidArgumentError(
            "EXEC without MULTI in Redis replication stream");
      }
      transaction_bytes += wire->bytes_;
      for (const ReplicatedCommand& command : transaction) {
        absl::Status valid = ValidateRedisSourceCommand(command, source);
        if (!valid.ok()) co_return valid;
      }
      absl::Status applied =
          co_await ApplyRedisReplicatedTransaction(transaction);
      if (!applied.ok()) co_return applied;
      source->selected_db_ = db_id;
      source->offset_.fetch_add(transaction_bytes, std::memory_order_acq_rel);
      transaction.clear();
      transaction_bytes = 0;
      in_multi = false;
      continue;
    }
    ReplicatedCommand command{.db_id_ = db_id,
                              .args_ = std::move(wire->command_.args_)};
    if (in_multi) {
      transaction_bytes += wire->bytes_;
      transaction.push_back(std::move(command));
      continue;
    }
    absl::Status valid = ValidateRedisSourceCommand(command, source);
    if (!valid.ok()) co_return valid;
    if (redis_cluster_ && (EqualCaseInsensitive(name, "FLUSHDB") ||
                           EqualCaseInsensitive(name, "FLUSHALL"))) {
      if (command.args_.size() > 2 ||
          (command.args_.size() == 2 &&
           !EqualCaseInsensitive(command.args_[1], "ASYNC") &&
           !EqualCaseInsensitive(command.args_[1], "SYNC"))) {
        co_return absl::InvalidArgumentError(
            "invalid Redis replicated flush command");
      }
      absl::Status reset = co_await ResetRedisSourceSlots(source);
      if (!reset.ok()) co_return reset;
      source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
      continue;
    }
    absl::Status applied = co_await ApplyRedisReplicatedCommand(command);
    if (!applied.ok()) {
      co_return absl::Status(
          applied.code(), absl::StrCat("failed to apply Redis command '", name,
                                       "': ", applied.message()));
    }
    source->offset_.fetch_add(wire->bytes_, std::memory_order_acq_rel);
  }
}

auto ReplicationManager::ReplicationGroup::StartRedisPsync(
    TcpStream& stream, const std::shared_ptr<RedisSource>& source)
    -> Task<absl::StatusOr<RedisPsyncReply>> {
  absl::Status status;
  {
    std::vector<std::string> command{"PING"};
    status = co_await ExpectRedisReply(stream, std::move(command), "+PONG");
    if (!status.ok()) co_return status;
  }
  {
    std::vector<std::string> command{"REPLCONF", "listening-port",
                                     absl::StrCat(listen_port_)};
    status = co_await ExpectRedisReply(stream, std::move(command), "+OK");
    if (!status.ok()) co_return status;
  }
  // Do not advertise the EOF capability: length-delimited RDB transfer lets
  // us consume exactly the snapshot bytes without scanning for a delimiter.
  {
    std::vector<std::string> command{"REPLCONF", "capa", "psync2"};
    status = co_await ExpectRedisReply(stream, std::move(command), "+OK");
    if (!status.ok()) co_return status;
  }

  std::optional<std::string> replid;
  {
    AssertStateOwner();
    if (redis_full_sync_session_id_ == 0 &&
        source->dataset_valid_.load(std::memory_order_acquire)) {
      replid = source->replid_;
    }
  }
  const bool can_continue = replid.has_value();
  std::vector<std::string> command;
  command.reserve(3);
  command.emplace_back("PSYNC");
  if (can_continue) {
    command.push_back(*replid);
    command.push_back(
        absl::StrCat(source->offset_.load(std::memory_order_acquire) + 1));
  } else {
    command.emplace_back("?");
    command.emplace_back("-1");
  }
  const std::string encoded = EncodeRespCommand(command);
  status = co_await WriteText(stream, encoded);
  if (!status.ok()) co_return status;
  auto response = co_await ReadLine(stream);
  if (!response.ok()) co_return response.status();
  auto parsed = ParseRedisPsyncReply(*response);
  if (!parsed.ok()) co_return parsed.status();
  co_return std::move(*parsed);
}

auto ReplicationManager::ReplicationGroup::RedisFollowerOnline(
    TcpStream& stream, const std::shared_ptr<RedisSource>& source)
    -> Task<absl::Status> {
  if (role_epoch_.load(std::memory_order_acquire) != source->role_epoch_) {
    co_return absl::CancelledError("replication role epoch was replaced");
  }
  absl::Status activated = co_await WaitForRedisFullSyncActivation(source);
  if (!activated.ok()) co_return activated;
  StoreRedisLink(source, true);
  source->syncing_ = false;
  RefreshRedisRole();
  absl::Status status = co_await SendRedisAck(stream, source);
  if (!status.ok()) co_return status;
  spdlog::info("Redis PSYNC follower online with {}:{} slots={}",
               source->upstream_.host_, source->upstream_.port_,
               FormatRedisSlots(source->slots_));
  co_return co_await ConsumeRedisCommandStream(stream, source);
}

auto ReplicationManager::ReplicationGroup::CompleteRedisFullSync(
    TcpStream& stream, const std::shared_ptr<RedisSource>& source,
    std::string replid, std::uint64_t offset) -> Task<absl::Status> {
  source->syncing_ = true;
  source->dataset_valid_ = false;
  RefreshRedisRole();
  auto full_sync_session = co_await BeginRedisFullSyncAttempt(source);
  if (!full_sync_session.ok()) co_return full_sync_session.status();
  auto rdb_path = co_await ReceiveRedisRdb(stream);
  if (!rdb_path.ok()) co_return rdb_path.status();
  absl::Status status = co_await ImportRedisRdb(*rdb_path, source);
  (void)::unlink(rdb_path->c_str());
  if (!status.ok()) co_return status;

  {
    co_await redis_fullsync_mutex_.Lock();
    bycorf::UnlockGuard fullsync_unlock(&redis_fullsync_mutex_,
                                        bycorf::ThisWorker().self_);
    source->offset_.store(offset, std::memory_order_release);
    source->selected_db_ = 0;
    source->dataset_valid_ = true;
    bool population_complete = true;
    std::string population_accumulator;
    {
      AssertStateOwner();
      if (redis_full_sync_session_id_ != *full_sync_session) {
        co_return absl::CancelledError(
            "Redis full sync attempt was superseded during import");
      }
      source->replid_ = std::move(replid);
      source->full_sync_session_id_ = *full_sync_session;
      if (redis_sources_.front() == source) {
        upstream_node_id_ = source->replid_;
        upstream_history_id_ = source->replid_;
      }
      source_worker_count_ = static_cast<unsigned>(redis_sources_.size());
      RedisSlotSet registered_slots;
      for (const auto& current : redis_sources_) {
        registered_slots |= current->slots_;
        const bool valid =
            current->dataset_valid_.load(std::memory_order_acquire) &&
            current->replid_.has_value() &&
            current->full_sync_session_id_ == *full_sync_session;
        population_complete &= valid;
        if (valid) {
          absl::StrAppend(&population_accumulator, *current->replid_, ":",
                          current->offset_.load(std::memory_order_acquire),
                          ";");
        }
      }
      if (redis_cluster_) {
        population_complete =
            population_complete && !redis_topology_fault_ &&
            expected_redis_topology_.has_value() &&
            registered_slots.count() == storage::kLogicalStorageShards &&
            redis_sources_.size() == expected_redis_topology_->masters_.size();
      }
    }
    if (population_complete) {
      const auto bytes = std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(population_accumulator.data()),
          population_accumulator.size());
      status = co_await storage_->CompleteReplicaFullSync(
          *full_sync_session, storage::PopulationToken{
                                  .generation_ = *full_sync_session,
                                  .digest_ = storage::Crc64(bytes),
                              });
      if (!status.ok()) {
        AssertStateOwner();
        if (redis_full_sync_session_id_ == *full_sync_session) {
          source->dataset_valid_.store(false, std::memory_order_release);
          source->full_sync_session_id_ = 0;
          source->replid_.reset();
        }
        co_return status;
      }
      {
        AssertStateOwner();
        if (redis_full_sync_session_id_ != *full_sync_session) {
          co_return absl::CancelledError(
              "Redis full sync activation was superseded");
        }
        redis_full_sync_session_id_ = 0;
      }
    }
  }
  spdlog::info("Redis FULLRESYNC completed from {}:{} at offset {}",
               source->upstream_.host_, source->upstream_.port_,
               source->offset_.load(std::memory_order_acquire));
  co_return co_await RedisFollowerOnline(stream, source);
}

auto ReplicationManager::ReplicationGroup::RunRedisReplicaSession(
    const std::shared_ptr<RedisSource>& source,
    const std::shared_ptr<ReplicaSession>& session) -> Task<absl::Status> {
  session->active_flows_.fetch_add(1, std::memory_order_acq_rel);
  ReplicaFlowActivityGuard activity(&session->active_flows_);
  TcpStream stream;
  std::optional<RedisPsyncReply> prepared_reply;
  if (source->prepared_ != nullptr) {
    prepared_reply = std::move(source->prepared_->reply_);
    stream = source->prepared_->TakeStream();
    source->prepared_.reset();
    if (!session->sockets_.Add(stream.NativeFd())) {
      stream.Close().IgnoreError();
      co_return absl::CancelledError("Redis source was cancelled");
    }
  } else {
    auto connected =
        co_await ConnectTcp(source->upstream_.host_, source->upstream_.port_,
                            tls_context_, &session->sockets_);
    if (!connected.ok()) co_return connected.status();
    stream = std::move(*connected);
    auto authenticated =
        co_await AuthenticateUpstream(stream, masteruser_, masterauth_);
    if (!authenticated.ok()) {
      session->sockets_.Remove(stream.NativeFd());
      stream.Close().IgnoreError();
      co_return authenticated;
    }
  }
  const int fd = stream.NativeFd();
  session->connected_flows_.store(1, std::memory_order_release);
  ReplicationConnectionMetricGuard connection_metric(
      ReplicationConnectionKind::kControl);
  absl::Status result = co_await RunRedisConnectedSession(
      source, stream, std::move(prepared_reply));
  session->connected_flows_.store(0, std::memory_order_release);
  session->sockets_.Remove(fd);
  stream.Close().IgnoreError();
  co_return result;
}

auto ReplicationManager::ReplicationGroup::RunRedisConnectedSession(
    const std::shared_ptr<RedisSource>& source, TcpStream& stream,
    std::optional<RedisPsyncReply> prepared_reply) -> Task<absl::Status> {
  absl::StatusOr<RedisPsyncReply> reply =
      prepared_reply.has_value()
          ? absl::StatusOr<RedisPsyncReply>(std::move(*prepared_reply))
          : co_await StartRedisPsync(stream, source);
  if (!reply.ok()) co_return reply.status();

  if (reply->full_) {
    co_return co_await CompleteRedisFullSync(
        stream, source, std::move(*reply->replid_), reply->offset_);
  }
  bool valid_cursor = false;
  {
    AssertStateOwner();
    valid_cursor = source->dataset_valid_.load(std::memory_order_acquire) &&
                   source->replid_.has_value();
  }
  if (!valid_cursor) {
    co_return absl::FailedPreconditionError(
        "Redis accepted partial sync without a valid local dataset");
  }
  if (reply->replid_.has_value()) {
    AssertStateOwner();
    source->replid_ = std::move(reply->replid_);
  }
  spdlog::info("Redis partial resynchronization continued from offset {}",
               source->offset_.load(std::memory_order_acquire));
  co_return co_await RedisFollowerOnline(stream, source);
}

}  // namespace lavik
