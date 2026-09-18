/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include "lavik/meta/raft.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <stdexcept>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "lavik/meta/identity_verifier.h"
#include "lavik/meta/raft_bridge.h"
#include "lavik/meta/state_machine.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {

std::string Quote(std::string_view value) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result = "\"";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') {
      result += '\\';
      result += static_cast<char>(c);
    } else if (c < 0x20) {
      result += "\\u00";
      result += hex[c >> 4];
      result += hex[c & 15];
    } else {
      result += static_cast<char>(c);
    }
  }
  return result + '"';
}

std::string MemberJson(const MetaRaftMember& member) {
  auto identity = MetaMemberIdentity::DecodeAux(member.get_aux());
  if (!identity.ok())
    throw std::invalid_argument("invalid Raft member identity");
  return absl::StrCat("{\"id\":", member.get_id(),
                      ",\"raft\":", Quote(member.get_endpoint()),
                      ",\"data\":", Quote(identity->data_control_endpoint_),
                      ",\"admin\":", Quote(identity->ctl_endpoint_),
                      ",\"principal\":", Quote(identity->principal_), "}");
}

std::string ConfigJson(const MetaRaftOptions& options) {
  std::string advertised =
      options.local_raft_.empty() ? options.listen_ : options.local_raft_;
  for (const auto& member : options.initial_) {
    if (member->get_id() == options.id_) advertised = member->get_endpoint();
  }
  // Advertised routes may name a proxy. They are independent of local binds.
  const MetaRaftMember local(
      options.id_, 0, advertised,
      MetaMemberIdentity{options.id_,
                         absl::StrCat("lavik://meta/", options.id_),
                         options.local_data_, options.local_admin_}
          .EncodeAux());
  std::string initial = "[";
  for (const auto& member : options.initial_) {
    if (initial.size() > 1) initial += ',';
    initial += MemberJson(*member);
  }
  initial += ']';
  return absl::StrCat(
      "{\"local\":", MemberJson(local), ",\"initial\":", initial,
      ",\"dir\":", Quote(options.data_dir_),
      ",\"listen\":", Quote(options.listen_),
      ",\"tls_ca\":", Quote(options.tls_ca_),
      ",\"tls_cert\":", Quote(options.tls_cert_),
      ",\"tls_key\":", Quote(options.tls_key_),
      ",\"heartbeat_ms\":", options.heartbeat_ms_,
      ",\"election_ticks\":", options.election_ms_ / options.heartbeat_ms_,
      ",\"snapshot_distance\":", options.snapshot_distance_,
      ",\"reserved_log_items\":", options.reserved_log_items_, "}");
}

bool CopyOutput(std::string_view value, LavikRaftBytes* output) {
  output->size = value.size();
  output->data = std::malloc(value.size());
  if (!output->data && !value.empty()) return false;
  if (!value.empty()) std::memcpy(output->data, value.data(), value.size());
  return true;
}

class StatusReader {
 public:
  explicit StatusReader(const LavikRaftBytes& bytes)
      : data_(static_cast<const unsigned char*>(bytes.data)),
        remaining_(bytes.size) {}
  std::uint64_t Integer() {
    if (remaining_ < 8) throw std::runtime_error("truncated Raft status");
    std::uint64_t value = 0;
    for (unsigned i = 0; i != 8; ++i)
      value |= std::uint64_t(data_[i]) << (i * 8);
    data_ += 8;
    remaining_ -= 8;
    return value;
  }
  std::string Text() {
    const auto length = Integer();
    if (length > remaining_ || length > (1u << 20))
      throw std::runtime_error("invalid Raft status field");
    std::string result(reinterpret_cast<const char*>(data_), length);
    data_ += length;
    remaining_ -= length;
    return result;
  }
  bool done() const { return remaining_ == 0; }

 private:
  const unsigned char* data_;
  std::uint64_t remaining_;
};

}  // namespace

void MetaRaftResult::when_ready(handler_type2 handler) {
  bool call = false;
  {
    std::lock_guard lock(mutex_);
    if (registered_) throw std::logic_error("Raft completion registered twice");
    registered_ = true;
    if (ready_)
      call = true;
    else
      handler_ = std::move(handler);
  }
  if (call) {
    std::shared_ptr<std::exception> error;
    handler(*this, error);
  }
}

void MetaRaftResult::Complete(MetaRaftResultCode code,
                              std::shared_ptr<MetaRaftBuffer> data,
                              std::uint64_t index) {
  handler_type2 handler;
  {
    std::lock_guard lock(mutex_);
    if (ready_) std::terminate();
    ready_ = true;
    code_ = code;
    data_ = std::move(data);
    index_ = index;
    handler = std::move(handler_);
  }
  if (handler) {
    std::shared_ptr<std::exception> error;
    handler(*this, error);
  }
}

MetaRaft::MetaRaft(MetaRaftOptions options, MetaStateMachine& machine)
    : options_(std::move(options)), machine_(machine) {
  auto config = std::make_shared<MetaRaftConfig>();
  config->members_ = options_.initial_;
  config_.store(std::move(config));
  progress_.store(std::make_shared<const std::vector<MetaRaftPeerProgress>>());
  genesis_.store(std::make_shared<const std::vector<std::uint32_t>>());
}

absl::StatusOr<std::shared_ptr<MetaRaft>> MetaRaft::Open(
    MetaRaftOptions options, MetaStateMachine& machine) {
  if (options.heartbeat_ms_ == 0 ||
      options.election_ms_ < 3 * options.heartbeat_ms_ ||
      options.election_ms_ % options.heartbeat_ms_ != 0) {
    return absl::InvalidArgumentError(
        "Raft election lower bound must be an integral >= 3 heartbeat ticks");
  }
  auto self =
      std::shared_ptr<MetaRaft>(new MetaRaft(std::move(options), machine));
  LavikRaftCallbacks callbacks{};
  callbacks.apply = [](uintptr_t owner, uint64_t index, void* data,
                       uint64_t size, LavikRaftBytes* output) -> int {
    try {
      auto* raft = reinterpret_cast<MetaRaft*>(owner);
      if (raft->options_.before_apply_) raft->options_.before_apply_();
      MetaRaftBuffer input(size);
      if (size) std::memcpy(input.data_begin(), data, size);
      auto result = raft->machine_.commit(index, input);
      return CopyOutput(std::string_view(
                            reinterpret_cast<const char*>(result->data_begin()),
                            result->size()),
                        output)
                 ? 0
                 : 1;
    } catch (...) {
      return 1;
    }
  };
  callbacks.advance = [](uintptr_t owner, uint64_t index) {
    try {
      reinterpret_cast<MetaRaft*>(owner)->machine_.Advance(index);
    } catch (...) {
      std::terminate();
    }
  };
  callbacks.install = [](uintptr_t owner, uint64_t index, void* data,
                         uint64_t size) -> int {
    try {
      return reinterpret_cast<MetaRaft*>(owner)
                     ->machine_
                     .Install(index, std::string_view(
                                         static_cast<const char*>(data), size))
                     .ok()
                 ? 0
                 : 1;
    } catch (...) {
      return 1;
    }
  };
  callbacks.identities = [](uintptr_t owner, LavikRaftBytes* output) -> int {
    try {
      const auto bindings =
          reinterpret_cast<MetaRaft*>(owner)->machine_.MetaBindings();
      std::string result = "[";
      for (const auto& binding : *bindings) {
        if (result.size() > 1) result += ',';
        result += absl::StrCat(
            "{\"id\":", binding.server_id_,
            ",\"principal\":", Quote(binding.principal_),
            ",\"data\":", Quote(binding.data_control_endpoint_),
            ",\"admin\":", Quote(binding.ctl_endpoint_.value_or("")),
            ",\"retired\":", binding.retired_ ? "true" : "false", "}");
      }
      result += ']';
      return CopyOutput(result, output) ? 0 : 1;
    } catch (...) {
      return 1;
    }
  };
  callbacks.capture = [](uintptr_t owner, uint64_t index,
                         LavikRaftBytes* output) -> int {
    try {
      auto result = reinterpret_cast<MetaRaft*>(owner)->machine_.Capture(index);
      return result.ok() && CopyOutput(*result, output) ? 0 : 1;
    } catch (...) {
      return 1;
    }
  };
  callbacks.role = [](uintptr_t owner, uint64_t term, uint64_t leader,
                      int is_leader, int caught_up, uint64_t resign_index) {
    reinterpret_cast<MetaRaft*>(owner)->OnRole(term, leader, is_leader,
                                               caught_up, resign_index);
  };
  callbacks.result = [](uintptr_t owner, uint64_t ticket, uint64_t index,
                        int code, void* data, uint64_t size) {
    try {
      reinterpret_cast<MetaRaft*>(owner)->OnResult(ticket, index, code, data,
                                                   size);
    } catch (...) {
      std::terminate();
    }
  };
  callbacks.fatal = [](uintptr_t, void* data, uint64_t size) {
    spdlog::critical("etcd Raft fail-stop: {}",
                     std::string_view(static_cast<const char*>(data), size));
    std::terminate();
  };
  std::string config;
  try {
    config = ConfigJson(self->options_);
  } catch (const std::exception& error) {
    return absl::InvalidArgumentError(error.what());
  }
  LavikRaftBytes error{};
  self->handle_ = lavik_raft_open(config.data(), config.size(),
                                  reinterpret_cast<uintptr_t>(self.get()),
                                  &callbacks, &error);
  std::unique_ptr<void, decltype(&std::free)> error_buffer(error.data,
                                                           &std::free);
  if (self->handle_ == 0)
    return absl::InternalError(
        std::string(static_cast<const char*>(error.data), error.size));
  if (!self->RefreshStatus())
    return absl::InternalError("invalid initial etcd Raft status");
  self->observer_ = std::thread([ptr = self.get()] { ptr->Observe(); });
  return self;
}

MetaRaft::~MetaRaft() { shutdown(); }

void MetaRaft::OnRole(std::uint64_t term, std::uint64_t leader, bool is_leader,
                      bool caught_up, std::uint64_t resign_index) noexcept {
  const auto previous_term = term_.exchange(term);
  leader_id_.store(leader ? static_cast<std::int32_t>(leader) : -1);
  caught_up_.store(caught_up, std::memory_order_release);
  // Revocation precedes the slower ordered relay. Readers cannot keep granting
  // authority while a snapshot or apply event delays the Bycorf mailbox.
  auto current = authority_.load(std::memory_order_acquire);
  bool allowed;
  do {
    allowed = is_leader && caught_up && !(current & kStopped) &&
              (current >> 1) <= resign_index;
  } while (!authority_.compare_exchange_weak(
      current, (current & ~std::uint64_t{1}) | allowed,
      std::memory_order_acq_rel));
  const bool previous = std::exchange(relayed_leader_, allowed);
  if ((previous != allowed || (!allowed && previous_term != term)) &&
      options_.role_) {
    try {
      options_.role_(allowed, term);
    } catch (...) {
      std::terminate();
    }
  }
}

void MetaRaft::shutdown() {
  if (stopping_.exchange(true)) return;
  authority_.fetch_or(kStopped, std::memory_order_acq_rel);
  caught_up_.store(false);
  if (observer_.joinable()) observer_.join();
  if (handle_ != 0) {
    lavik_raft_close(handle_);
    handle_ = 0;
  }
}

void MetaRaft::Observe() {
  while (!stopping_.load()) {
    if (!RefreshStatus()) std::terminate();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

bool MetaRaft::RefreshStatus() {
  LavikRaftBytes bytes{};
  if (lavik_raft_status(handle_, &bytes) != 0) return false;
  std::unique_ptr<void, decltype(&std::free)> buffer(bytes.data, &std::free);
  try {
    StatusReader reader(bytes);
    if (reader.Integer() != 1) return false;
    // Role callbacks are authoritative. A sampled status must never revive a
    // role already revoked by the protocol owner after the sample was copied.
    for (int i = 0; i != 4; ++i) reader.Integer();
    committed_.store(reader.Integer());
    reader.Integer();
    durable_.store(reader.Integer());
    snapshot_.store(reader.Integer());
    uncompacted_.store(reader.Integer());
    first_index_.store(reader.Integer());
    pending_bytes_.store(reader.Integer());
    gc_failures_.store(reader.Integer());
    rpc_failures_.store(reader.Integer());
    vote_rejections_.store(reader.Integer());
    vote_grants_.store(reader.Integer());
    machine_.SetSnapshotFailures(reader.Integer());
    const auto config_index = reader.Integer();
    const auto genesis_count = reader.Integer();
    if (genesis_count > 5) return false;
    auto genesis = std::make_shared<std::vector<std::uint32_t>>();
    for (std::uint64_t i = 0; i != genesis_count; ++i)
      genesis->push_back(reader.Integer());
    genesis_.store(std::move(genesis));
    const auto count = reader.Integer();
    if (count > 4096) return false;
    auto config = std::make_shared<MetaRaftConfig>();
    auto progress = std::make_shared<std::vector<MetaRaftPeerProgress>>();
    config->index_ = config_index;
    for (std::uint64_t i = 0; i != count; ++i) {
      const auto id = reader.Integer();
      auto endpoint = reader.Text();
      auto data = reader.Text();
      auto admin = reader.Text();
      auto principal = reader.Text();
      const auto applied = reader.Integer();
      const auto age = reader.Integer();
      const auto learner = reader.Integer();
      if (id == 0 || id > INT32_MAX || learner > 1) return false;
      config->members_.push_back(std::make_shared<MetaRaftMember>(
          id, 0, std::move(endpoint),
          MetaMemberIdentity{static_cast<std::int32_t>(id),
                             std::move(principal), std::move(data),
                             std::move(admin)}
              .EncodeAux(),
          learner));
      if (id != static_cast<std::uint64_t>(options_.id_))
        progress->push_back({static_cast<std::int32_t>(id), applied, age});
    }
    if (!reader.Text().empty() || !reader.done()) return false;
    config_.store(std::move(config));
    progress_.store(std::move(progress));
    return true;
  } catch (...) {
    return false;
  }
}

std::shared_ptr<MetaRaftMember> MetaRaft::get_srv_config(
    std::int32_t id) const {
  for (const auto& member : get_config()->get_servers())
    if (member->get_id() == id) return member;
  return {};
}

std::vector<MetaRaftPeerProgress> MetaRaft::get_peer_info_all() const {
  return *progress_.load();
}

bool MetaRaft::initial_bindings_pending() const {
  const auto genesis = genesis_.load();
  if (genesis->empty()) return false;
  const auto bindings = machine_.MetaBindings();
  for (auto id : *genesis) {
    if (std::none_of(bindings->begin(), bindings->end(),
                     [id](const auto& item) { return item.server_id_ == id; }))
      return true;
  }
  return false;
}

std::pair<std::uint64_t, std::shared_ptr<MetaRaftResult>>
MetaRaft::NewResult() {
  auto result = std::make_shared<MetaRaftResult>();
  std::lock_guard lock(requests_mutex_);
  if (stopping_.load() || requests_.size() >= 256) {
    result->Complete(MetaRaftResultCode::CANCELLED, {});
    return {0, std::move(result)};
  }
  const auto ticket = ++next_ticket_;
  requests_.emplace(ticket, result);
  return {ticket, std::move(result)};
}

void MetaRaft::OnResult(std::uint64_t ticket, std::uint64_t index, int code,
                        const void* data, std::uint64_t size) {
  std::shared_ptr<MetaRaftResult> result;
  {
    std::lock_guard lock(requests_mutex_);
    auto found = requests_.find(ticket);
    if (found == requests_.end()) std::terminate();
    result = std::move(found->second);
    requests_.erase(found);
  }
  auto payload = MetaRaftBuffer::alloc(size);
  if (size) std::memcpy(payload->data_begin(), data, size);
  result->Complete(code == 0   ? MetaRaftResultCode::OK
                   : code == 2 ? MetaRaftResultCode::NOT_LEADER
                               : MetaRaftResultCode::CANCELLED,
                   std::move(payload), index);
}

std::shared_ptr<MetaRaftResult> MetaRaft::append_entries(
    const std::vector<std::shared_ptr<MetaRaftBuffer>>& entries) {
  auto [ticket, result] = NewResult();
  if (!ticket) return result;
  if (entries.size() != 1 || !entries[0]) {
    OnResult(ticket, 0, 3, nullptr, 0);
    return result;
  }
  const int code = lavik_raft_propose(handle_, ticket, entries[0]->data_begin(),
                                      entries[0]->size());
  if (code != 0) OnResult(ticket, 0, code, nullptr, 0);
  return result;
}

std::uint64_t MetaRaft::create_snapshot(create_snapshot_options) {
  auto [ticket, result] = NewResult();
  if (!ticket) return 0;
  auto completion = std::make_shared<std::promise<std::uint64_t>>();
  auto future = completion->get_future();
  result->when_ready([completion](MetaRaftResult& value, auto&) {
    completion->set_value(
        value.get_result_code() == MetaRaftResultCode::OK ? value.index() : 0);
  });
  const int code = lavik_raft_snapshot(handle_, ticket);
  if (code != 0) OnResult(ticket, 0, code, nullptr, 0);
  return future.get();
}

std::shared_ptr<MetaRaftResult> MetaRaft::add_srv(
    const MetaRaftMember& member) {
  auto [ticket, result] = NewResult();
  if (!ticket) return result;
  if (member.get_dc_id() != 0 || member.get_priority() != 1) {
    OnResult(ticket, 0, 3, nullptr, 0);
    return result;
  }
  std::string data;
  try {
    data = MemberJson(member);
  } catch (const std::exception&) {
    OnResult(ticket, 0, 3, nullptr, 0);
    return result;
  }
  const int code = lavik_raft_member(handle_, ticket, data.data(), data.size(),
                                     0, member.is_learner());
  if (code) OnResult(ticket, 0, code, nullptr, 0);
  return result;
}

std::shared_ptr<MetaRaftResult> MetaRaft::remove_srv(std::int32_t id) {
  auto [ticket, result] = NewResult();
  if (!ticket) return result;
  auto data = absl::StrCat("{\"id\":", id, "}");
  const int code =
      lavik_raft_member(handle_, ticket, data.data(), data.size(), 1, 0);
  if (code) OnResult(ticket, 0, code, nullptr, 0);
  return result;
}

void MetaRaft::yield_leadership(bool) {
  auto current = authority_.load(std::memory_order_acquire);
  std::uint64_t generation;
  do {
    if (current & kStopped) return;
    generation = (current >> 1) + 1;
    if (generation >= (kStopped >> 1)) std::terminate();
  } while (!authority_.compare_exchange_weak(current, generation << 1,
                                             std::memory_order_acq_rel));
  caught_up_.store(false, std::memory_order_release);
  // The protocol owner sends the ordered follower edge. The synchronous CAS
  // above already stops grants, including while an older callback is in flight.
  lavik_raft_resign(handle_, generation);
}

}  // namespace lavik::meta
