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

#include "lavik/meta/sentinel_server.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <deque>
#include <future>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "bycorf/net/tcp_listener.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_transport.h"
#include "lavik/meta/raft.h"
#include "lavik/meta/sentinel_discovery.h"
#include "lavik/meta/state_machine.h"
#include "lavik/numeric_endpoint.h"
#include "lavik/password_authenticator.h"
#include "lavik/resp.h"
#include "lavik/version.h"

namespace lavik::meta {
namespace {

struct SentinelSession {
  bool authenticated_;
  std::uint64_t id_;
  std::string name_;
  std::string library_name_;
  std::string library_version_;
  ReplyBuilder reply_{};
  std::set<std::string> subscriptions_;
  std::size_t subscription_bytes_ = 0;
  bool authority_bound_ = false;
  std::int64_t authority_term_ = -1;
  std::uint64_t leader_term_ = 0;
  std::uint64_t eligibility_revision_ = 0, continuity_ = 0;
  bycorf::Connection* connection_ = nullptr;
  std::deque<std::string> output_;
  std::size_t output_bytes_ = 0;
  bool closing_ = false, writer_done_ = false;
  bycorf::AsyncNotification output_changed_, writer_finished_;
};

// kDrop closes the session without writing any reply. It exists solely for
// the discovery verbs on a node that cannot currently speak with authority:
// real Sentinel clients rotate their seed list only when a query goes
// unanswered, and a follower has no truthful answer to give (no private
// redirect exists on this surface). Every other refusal stays an explicit
// error reply so deterministic validation feedback never looks like a
// network failure.
enum class SessionAction { kContinue, kReset, kClose, kDrop };

// Worker-confined discovery inputs and per-worker caches. The dependencies
// outlive the server by process assembly order; the committed-view cache is
// rebuilt when the applied index advances. Each cut owns a share of the
// immutable view, so a cache refresh cannot invalidate a cut
// still being encoded or observed. Nothing here blocks across workers on the
// request path beyond the registries' short snapshot locks.
struct DiscoverySource {
  std::shared_ptr<MetaRaft> raft_;
  MetaStateMachine* state_machine_ = nullptr;
  std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status_;
  std::shared_ptr<MetaAutomaticFailoverDiagnosticsRegistry> diagnostics_;
  std::uint32_t observation_ttl_ms_ = 0;
  std::uint64_t leader_observation_grace_ms_ = 0;

  std::uint64_t continuity_ = 0;
  std::shared_ptr<const MetaCommittedStatusView> cached_view_;

  // Observation-grace bookkeeping: the window opens on leader admission
  // and reopens when the runtime registry attaches the new Raft term,
  // so a handoff never turns a never-yet-observed node
  // into a false down mark. Both edges reset the same start point.
  bool leader_admitted_ = false;
  std::uint64_t grace_leader_term_ = 0;
  std::chrono::steady_clock::time_point authority_since_{};
};

// Assembles one discovery cut when this node currently holds a caught-up
// leadership, else nullopt. The Raft leader term brackets the snapshots; the
// runtime's captured term anchors the observation-grace window.
std::optional<MetaDiscoveryCut> AuthoritativeDiscoveryCut(
    DiscoverySource& source) {
  const auto raft_term = source.raft_->leader_term();
  const bool authoritative = raft_term >= 0;
  const auto now = std::chrono::steady_clock::now();
  if (authoritative && !source.leader_admitted_) {
    source.authority_since_ = now;
  }
  source.leader_admitted_ = authoritative;
  if (!authoritative) return std::nullopt;

  MetaDiscoveryCut cut;
  cut.raft_term_ = raft_term;
  cut.local_meta_id_ = source.raft_->get_id();
  if (const auto config = source.raft_->get_config()) {
    for (const auto& peer : config->get_servers())
      if (peer && !peer->is_new_joiner())
        cut.effective_meta_ids_.push_back(peer->get_id());
  }
  cut.runtime_ = source.runtime_status_->Snapshot();
  if (cut.runtime_.leader_term_ != 0 &&
      (cut.runtime_.leader_term_ != static_cast<std::uint64_t>(raft_term) ||
       !cut.runtime_.leader_authority_eligible_))
    return std::nullopt;
  if (cut.runtime_.leader_term_ != source.grace_leader_term_) {
    source.grace_leader_term_ = cut.runtime_.leader_term_;
    source.authority_since_ = now;
  }
  cut.observation_grace_active_ =
      now - source.authority_since_ <
      std::chrono::milliseconds(source.leader_observation_grace_ms_);
  cut.observation_ttl_ms_ = source.observation_ttl_ms_;
  cut.diagnostics_ = source.diagnostics_->Snapshot();
  // Read the applied index before the locked snapshot. If a commit lands
  // between them, the snapshot sees it; if one lands afterward, the next cut
  // refreshes. Advance() can move this index without changing stores, but the
  // detector joins on this exact index, so a state-change-only key would
  // leave its otherwise-current diagnostics mismatched indefinitely.
  const std::uint64_t applied_index =
      source.state_machine_->last_commit_index();
  if (!source.cached_view_ ||
      applied_index != source.cached_view_->applied_index_) {
    source.cached_view_ = std::make_shared<const MetaCommittedStatusView>(
        source.state_machine_->StatusSnapshot());
  }
  cut.committed_ = source.cached_view_;
  cut.now_unix_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  if (source.raft_->leader_term() != raft_term) return std::nullopt;
  return cut;
}

bool BindAuthority(DiscoverySource& source, SentinelSession& session,
                   const MetaDiscoveryCut& cut) {
  if (session.authority_bound_ &&
      (session.authority_term_ != cut.raft_term_ ||
       session.leader_term_ != cut.runtime_.leader_term_ ||
       session.eligibility_revision_ !=
           cut.runtime_.leader_authority_eligibility_revision_ ||
       session.continuity_ != source.continuity_))
    return false;
  session.authority_bound_ = true;
  session.authority_term_ = cut.raft_term_;
  session.leader_term_ = cut.runtime_.leader_term_;
  session.eligibility_revision_ =
      cut.runtime_.leader_authority_eligibility_revision_;
  session.continuity_ = source.continuity_;
  return true;
}

bool AuthorityCurrent(const DiscoverySource& source,
                      const SentinelSession& session) {
  if (!session.authority_bound_) return true;
  const auto state = source.runtime_status_->LeadershipState();
  return source.raft_->leader_term() == session.authority_term_ &&
         source.continuity_ == session.continuity_ &&
         state.leader_term_ == session.leader_term_ &&
         state.leader_authority_eligibility_revision_ ==
             session.eligibility_revision_ &&
         (state.leader_term_ == 0 || state.leader_authority_eligible_);
}

bool ValidAttribute(std::string_view value) {
  return std::all_of(value.begin(), value.end(), [](unsigned char byte) {
    return byte >= '!' && byte <= '~';
  });
}

constexpr std::string_view kWrongPassword =
    "WRONGPASS invalid username-password pair or user is disabled.";

// Redis's formatted command errors use C-string tokens and replace CR/LF with
// spaces. Keep that wire contract without allowing an argument to inject a
// second RESP reply. Request and reply ceilings still bound these messages.
std::string ErrorToken(std::string_view token) {
  std::string result(token.substr(0, token.find('\0')));
  std::replace(result.begin(), result.end(), '\r', ' ');
  std::replace(result.begin(), result.end(), '\n', ' ');
  return result;
}

// Redis 7.2 parses HELLO and SETINFO option names as C strings, unlike the
// length-aware command lookup and password/username comparison.
bool OptionIs(std::string_view token, std::string_view expected) {
  return absl::EqualsIgnoreCase(token.substr(0, token.find('\0')), expected);
}

void ExecuteHello(const PasswordAuthenticator& authenticator,
                  SentinelSession& session, std::span<const std::string> args) {
  auto& reply = session.reply_;
  auto version = reply.version();
  std::optional<std::pair<std::string_view, std::string_view>> credentials;
  std::optional<std::string_view> name;
  if (args.size() > 1) {
    long long protocol = 0;
    const auto& text = args[1];
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), protocol);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
        std::to_string(protocol) != text) {
      reply.AppendError(
          "ERR Protocol version is not an integer or out of range");
      return;
    }
    if (args[1] == "2")
      version = RespVersion::k2;
    else if (args[1] == "3")
      version = RespVersion::k3;
    else {
      reply.AppendError("NOPROTO unsupported protocol version");
      return;
    }
    for (std::size_t i = 2; i < args.size();) {
      if (OptionIs(args[i], "AUTH") && i + 2 < args.size()) {
        credentials.emplace(args[i + 1], args[i + 2]);
        i += 3;
      } else if (OptionIs(args[i], "SETNAME") && i + 1 < args.size()) {
        name = args[i + 1];
        if (!ValidAttribute(*name)) {
          reply.AppendError(
              "ERR Client names cannot contain spaces, newlines or special "
              "characters.");
          return;
        }
        i += 2;
      } else {
        reply.AppendError("ERR Syntax error in HELLO option '" +
                          ErrorToken(args[i]) + "'");
        return;
      }
    }
  }
  if (credentials &&
      !authenticator.Authenticate(credentials->first, credentials->second)) {
    reply.AppendError(kWrongPassword);
    return;
  }
  if (!session.authenticated_ && !credentials) {
    reply.AppendError(
        "NOAUTH HELLO must be called with the client already authenticated, "
        "otherwise the HELLO <proto> AUTH <user> <pass> option can be used to "
        "authenticate the client and select the RESP protocol version at the "
        "same time");
    return;
  }
  // Validation precedes every state change, including successful credentials
  // accompanied by an invalid SETNAME. A failed negotiation cannot log in.
  if (credentials) session.authenticated_ = true;
  if (name) session.name_ = *name;
  reply.SetVersion(version);
  // Redis Sentinel HELLO has six fields and no Data role. Protocol identity
  // does not imply support for Sentinel election or management commands.
  reply.AppendMapHeader(6);
  reply.AppendBulkString("server");
  reply.AppendBulkString("lavik");
  reply.AppendBulkString("version");
  reply.AppendBulkString(kVersion);
  reply.AppendBulkString("proto");
  reply.AppendInteger(static_cast<unsigned>(version));
  reply.AppendBulkString("id");
  reply.AppendInteger(static_cast<long long>(std::min<std::uint64_t>(
      session.id_, std::numeric_limits<long long>::max())));
  reply.AppendBulkString("mode");
  reply.AppendBulkString("sentinel");
  reply.AppendBulkString("modules");
  reply.AppendArrayHeader(0);
}

// SENTINEL subcommand dispatch. The six discovery verbs answer from
// committed authority through the pure mapping layer; management and unknown
// subcommands keep their explicit refusal on every node role because that
// refusal is a deterministic answer, not a topology claim.
SessionAction ExecuteSentinelCommand(DiscoverySource& source,
                                     SentinelSession& session,
                                     std::span<const std::string> args) {
  auto& reply = session.reply_;
  if (args.size() < 2) {
    reply.AppendError("ERR wrong number of arguments for 'sentinel' command");
    return SessionAction::kContinue;
  }
  const auto is = [&](std::string_view subcommand) {
    return absl::EqualsIgnoreCase(args[1], subcommand);
  };
  const auto arity_error = [&](std::string_view subcommand) {
    reply.AppendError("ERR wrong number of arguments for 'sentinel|" +
                      std::string(subcommand) + "' command");
    return SessionAction::kContinue;
  };
  const bool address = is("GET-MASTER-ADDR-BY-NAME");
  const bool master = is("MASTER");
  const bool masters = is("MASTERS");
  const bool replicas = is("REPLICAS");
  const bool slaves = is("SLAVES");
  const bool sentinels = is("SENTINELS");
  // Arity is validated before any authority check: the answer cannot depend
  // on committed state, so a follower may (and should) report it explicitly.
  if (address && args.size() != 3)
    return arity_error("get-master-addr-by-name");
  if (master && args.size() != 3) return arity_error("master");
  if (masters && args.size() != 2) return arity_error("masters");
  if (replicas && args.size() != 3) return arity_error("replicas");
  if (slaves && args.size() != 3) return arity_error("slaves");
  if (sentinels && args.size() != 3) return arity_error("sentinels");
  if (!address && !master && !masters && !replicas && !slaves && !sentinels) {
    reply.AppendError("ERR SENTINEL subcommand is not supported");
    return SessionAction::kContinue;
  }
  const std::optional<MetaDiscoveryCut> cut = AuthoritativeDiscoveryCut(source);
  if (!cut.has_value()) {
    return SessionAction::kDrop;
  }
  if (!BindAuthority(source, session, *cut)) return SessionAction::kDrop;
  if (address) {
    EncodeDiscoveryAddressReply(reply, *cut, args[2]);
  } else if (sentinels) {
    EncodeDiscoverySentinelsReply(reply, *cut, args[2]);
  } else if (master) {
    EncodeDiscoveryMasterReply(reply, *cut, args[2]);
  } else if (masters) {
    EncodeDiscoveryMastersReply(reply, *cut);
  } else {
    // SLAVES is Redis's deprecated alias of REPLICAS; both share one reply.
    EncodeDiscoveryReplicasReply(reply, *cut, args[2]);
  }
  return SessionAction::kContinue;
}

SessionAction ExecuteConnectionCommand(
    const PasswordAuthenticator& authenticator, DiscoverySource& discovery,
    SentinelSession& session, std::span<const std::string> args,
    const MetaSentinelServerOptions& limits) {
  auto& reply = session.reply_;
  reply.Reset();
  if (!AuthorityCurrent(discovery, session)) return SessionAction::kDrop;
  if (args.empty()) return SessionAction::kContinue;
  const auto is = [&](std::string_view command) {
    return absl::EqualsIgnoreCase(args[0], command);
  };
  if (!session.subscriptions_.empty() && reply.version() == RespVersion::k2 &&
      !is("SUBSCRIBE") && !is("UNSUBSCRIBE") && !is("PING") && !is("RESET") &&
      !is("QUIT")) {
    reply.AppendError(
        "ERR only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT / RESET allowed in "
        "this context");
    return SessionAction::kContinue;
  }
  if (is("HELLO")) {
    ExecuteHello(authenticator, session, args);
  } else if (is("AUTH")) {
    if (args.size() < 2) {
      reply.AppendError("ERR wrong number of arguments for 'auth' command");
    } else if (args.size() > 3) {
      reply.AppendError("ERR syntax error");
    } else if (!authenticator.required() && args.size() == 2) {
      reply.AppendError(
          "ERR AUTH <password> called without any password configured for the "
          "default "
          "user. Are you sure your configuration is correct?");
    } else if (authenticator.Authenticate(
                   args.size() == 2 ? std::string_view("default") : args[1],
                   args.back())) {
      session.authenticated_ = true;
      reply.AppendSimpleString("OK");
    } else {
      reply.AppendError(kWrongPassword);
    }
  } else if (is("QUIT")) {
    if (args.size() != 1) {
      reply.AppendError("ERR wrong number of arguments for 'quit' command");
    } else {
      reply.AppendSimpleString("OK");
      return SessionAction::kClose;
    }
  } else if (is("RESET")) {
    if (args.size() != 1) {
      reply.AppendError("ERR wrong number of arguments for 'reset' command");
    } else {
      session.authenticated_ = !authenticator.required();
      session.subscriptions_.clear();
      session.subscription_bytes_ = 0;
      // RESET clears protocol state, not this TCP connection's authority
      // lifetime: queued/in-flight discovery frames retain their revocation
      // protection even when RESET is pipelined behind them.
      session.name_.clear();
      session.library_name_.clear();
      session.library_version_.clear();
      reply.SetVersion(RespVersion::k2);
      reply.AppendSimpleString("RESET");
      return SessionAction::kReset;
    }
  } else if (is("CLIENT") && args.size() == 1) {
    reply.AppendError("ERR wrong number of arguments for 'client' command");
  } else if (is("CLIENT") && args.size() >= 2 &&
             absl::EqualsIgnoreCase(args[1], "SETNAME") && args.size() != 3) {
    // Redis checks registered subcommand arity before its authentication gate.
    reply.AppendError(
        "ERR wrong number of arguments for 'client|setname' command");
  } else if (is("CLIENT") && args.size() >= 2 &&
             absl::EqualsIgnoreCase(args[1], "SETINFO") && args.size() != 4) {
    reply.AppendError(
        "ERR wrong number of arguments for 'client|setinfo' command");
  } else if (!session.authenticated_) {
    reply.AppendError("NOAUTH Authentication required.");
  } else if (is("SUBSCRIBE") || is("UNSUBSCRIBE")) {
    const bool subscribe = is("SUBSCRIBE");
    if (subscribe && args.size() == 1) {
      reply.AppendError(
          "ERR wrong number of arguments for 'subscribe' command");
      return SessionAction::kContinue;
    }
    auto cut = AuthoritativeDiscoveryCut(discovery);
    if (!cut) return SessionAction::kDrop;
    if (!BindAuthority(discovery, session, *cut)) return SessionAction::kDrop;
    std::vector<std::string> channels(args.begin() + 1, args.end());
    if (!subscribe && channels.empty()) {
      channels.assign(session.subscriptions_.begin(),
                      session.subscriptions_.end());
      if (channels.empty()) {
        reply.AppendPushHeader(3);
        reply.AppendBulkString("unsubscribe");
        reply.AppendNullBulkString();
        reply.AppendInteger(0);
        return SessionAction::kContinue;
      }
    }
    for (const auto& channel : channels) {
      if (subscribe) {
        if (!session.subscriptions_.contains(channel) &&
            (session.subscriptions_.size() >= limits.subscription_limit_ ||
             channel.size() >
                 limits.query_limit_ - std::min(limits.query_limit_,
                                                session.subscription_bytes_)))
          return SessionAction::kDrop;
        if (session.subscriptions_.insert(channel).second)
          session.subscription_bytes_ += channel.size();
      } else if (session.subscriptions_.erase(channel)) {
        session.subscription_bytes_ -= channel.size();
      }
      reply.AppendPushHeader(3);
      reply.AppendBulkString(subscribe ? "subscribe" : "unsubscribe");
      reply.AppendBulkString(channel);
      reply.AppendInteger(session.subscriptions_.size());
    }
  } else if (is("PING")) {
    if (!session.subscriptions_.empty() && reply.version() == RespVersion::k2 &&
        args.size() <= 2) {
      reply.AppendArrayHeader(2);
      reply.AppendBulkString("pong");
      reply.AppendBulkString(args.size() == 2 ? std::string_view(args[1]) : "");
    } else if (args.size() == 1)
      reply.AppendSimpleString("PONG");
    else if (args.size() == 2)
      reply.AppendBulkString(args[1]);
    else
      reply.AppendError("ERR wrong number of arguments for 'ping' command");
  } else if (is("CLIENT")) {
    if (args.size() >= 2 && absl::EqualsIgnoreCase(args[1], "SETNAME")) {
      if (!ValidAttribute(args[2])) {
        reply.AppendError(
            "ERR Client names cannot contain spaces, newlines or special "
            "characters.");
      } else {
        session.name_ = args[2];
        reply.AppendSimpleString("OK");
      }
    } else if (args.size() >= 2 && absl::EqualsIgnoreCase(args[1], "SETINFO")) {
      if (!OptionIs(args[2], "LIB-NAME") && !OptionIs(args[2], "LIB-VER")) {
        reply.AppendError("ERR Unrecognized option '" + ErrorToken(args[2]) +
                          "'");
      } else if (!ValidAttribute(args[3])) {
        reply.AppendError("ERR " + ErrorToken(args[2]) +
                          " cannot contain spaces, newlines or "
                          "special characters.");
      } else {
        (OptionIs(args[2], "LIB-NAME") ? session.library_name_
                                       : session.library_version_) = args[3];
        reply.AppendSimpleString("OK");
      }
    } else {
      reply.AppendError(
          "ERR CLIENT subcommand is not supported by the Sentinel server");
    }
  } else if (is("SENTINEL")) {
    return ExecuteSentinelCommand(discovery, session, args);
  } else {
    // No fallback to either Data or Admin dispatch, including inline RESP.
    // Do not echo arguments: they may contain passwords or unbounded text.
    reply.AppendError("ERR command is not supported by the Sentinel server");
  }
  return SessionAction::kContinue;
}

// Closing an io_uring listener alone need not complete its pending accept.
// Match the existing Meta ingress lifecycle: wake it with a local connection,
// let the accept coroutine retire the listener, then join its frame.
absl::StatusOr<int> OpenAcceptWake(const NumericEndpoint& endpoint) {
  auto addresses = bycorf::ResolveTcpAddresses(endpoint.host_, endpoint.port_);
  if (!addresses.ok()) return addresses.status();
  const auto& address = addresses->front();
  const int fd = ::socket(address.address_.ss_family,
                          SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) return absl::ErrnoToStatus(errno, "Sentinel accept wake socket");
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&address.address_),
                address.length_) != 0 &&
      errno != EINPROGRESS && errno != EALREADY && errno != EISCONN) {
    const auto status =
        absl::ErrnoToStatus(errno, "Sentinel accept wake connect");
    (void)::close(fd);
    return status;
  }
  return fd;
}

}  // namespace

struct MetaSentinelServer::Core {
  Core(bycorf::ForeignExecutor executor,
       MetaSentinelDiscoveryDependencies discovery,
       MetaSentinelServerOptions options, NumericEndpoint endpoint)
      : executor_(executor),
        authenticator_(options.requirepass_),
        options_(std::move(options)),
        endpoint_(std::move(endpoint)) {
    // Only the immutable digest is needed after construction.
    options_.requirepass_.clear();
    discovery_ = DiscoverySource{
        .raft_ = std::move(discovery.raft_),
        .state_machine_ = discovery.state_machine_,
        .runtime_status_ = std::move(discovery.runtime_status_),
        .diagnostics_ = std::move(discovery.diagnostics_),
        .observation_ttl_ms_ = discovery.observation_ttl_ms_,
        .leader_observation_grace_ms_ = discovery.leader_observation_grace_ms_,
        .cached_view_ = {},
    };
  }

  void Close(SentinelSession& session) {
    session.closing_ = true;
    if (session.connection_->file_.fd_ >= 0)
      (void)::shutdown(session.connection_->file_.fd_, SHUT_RDWR);
    session.output_changed_.NotifyAll(*worker_);
  }
  void RevokeDiscovery() {
    ++discovery_.continuity_;
    events_.Reset();
    for (auto [connection, session] : live_) {
      (void)connection;
      if (session->authority_bound_) Close(*session);
    }
  }
  bool Enqueue(SentinelSession& session, std::string message) {
    if (session.closing_ || !AuthorityCurrent(discovery_, session) ||
        message.size() >
            options_.reply_limit_ -
                std::min(options_.reply_limit_, session.output_bytes_) ||
        message.size() >
            options_.total_output_limit_ -
                std::min(options_.total_output_limit_, output_bytes_)) {
      Close(session);
      return false;
    }
    session.output_bytes_ += message.size();
    output_bytes_ += message.size();
    session.output_.push_back(std::move(message));
    session.output_changed_.NotifyAll(*worker_);
    return true;
  }
  bycorf::Task<absl::Status> WriteSession(bycorf::TcpStream* stream,
                                          SentinelSession* session) {
    cluster::control::ControlDeadlineWatchdog deadline(
        *worker_, [this, session] { Close(*session); });
    while (!shutdown_) {
      if (session->output_.empty()) {
        if (session->closing_) break;
        co_await session->output_changed_.Wait();
        continue;
      }
      if (!AuthorityCurrent(discovery_, *session)) {
        Close(*session);
        break;
      }
      // The front string remains alive and charged throughout a suspended
      // write.
      const auto& frame = session->output_.front();
      if (!deadline.Arm(options_.progress_timeout_).ok()) {
        Close(*session);
        break;
      }
      const auto written = co_await stream->WriteAll(
          std::as_bytes(std::span(frame.data(), frame.size())));
      (void)deadline.Disarm();
      session->output_bytes_ -= frame.size();
      output_bytes_ -= frame.size();
      session->output_.pop_front();
      if (!written.ok()) {
        Close(*session);
        break;
      }
    }
    output_bytes_ -= session->output_bytes_;
    session->output_bytes_ = 0;
    session->output_.clear();
    session->writer_done_ = true;
    session->writer_finished_.NotifyAll(*worker_);
    co_return absl::OkStatus();
  }
  void Subscribe() {
    auto signal = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto start =
        context_->SubscribeCommitted([signal](const MetaCommitEvent& event) {
          signal->store(event.log_index_, std::memory_order_release);
        });
    committed_signal_ = std::move(signal);
    subscription_ = std::move(start.subscription_);
  }
  static bycorf::Task<absl::Status> Monitor(CorePtr core) {
    while (!core->shutdown_) {
      for (auto [connection, session] : core->live_) {
        (void)connection;
        if (!AuthorityCurrent(core->discovery_, *session))
          core->Close(*session);
      }
      if (core->context_ && core->subscription_ &&
          core->subscription_->cancelled()) {
        core->RevokeDiscovery();
        core->subscription_.reset();
        core->Subscribe();
      }
      auto cut = AuthoritativeDiscoveryCut(core->discovery_);
      if (!cut) {
        core->events_.Reset();
      } else {
        const auto term = cut->raft_term_;
        const auto runtime_term = cut->runtime_.leader_term_;
        const auto revision =
            cut->runtime_.leader_authority_eligibility_revision_;
        if (term != core->event_term_ ||
            runtime_term != core->event_runtime_term_ ||
            revision != core->event_revision_) {
          core->events_.Reset();
          core->event_term_ = term;
          core->event_runtime_term_ = runtime_term;
          core->event_revision_ = revision;
        }
        // Notifications describe observed committed cuts. They may coalesce;
        // the subscription signals lost continuity, not a durable event
        // history.
        if (!core->committed_signal_ ||
            cut->committed_->applied_index_ >=
                core->committed_signal_->load(std::memory_order_acquire)) {
          for (const auto& event : core->events_.Observe(*cut)) {
            for (auto [connection, session] : core->live_) {
              (void)connection;
              if (!session->subscriptions_.contains(event.channel_)) continue;
              ReplyBuilder frame(session->reply_.version());
              frame.AppendPushHeader(3);
              frame.AppendBulkString("message");
              frame.AppendBulkString(event.channel_);
              frame.AppendBulkString(event.payload_);
              core->Enqueue(*session, std::string(frame.View()));
            }
          }
        }
      }
      // Release this iteration's snapshots before suspending. A cache refresh
      // in the next iteration then retains old storage only for active reads.
      cut.reset();
      if (!(co_await bycorf::SleepFor(*core->worker_,
                                      std::chrono::milliseconds(10)))
               .ok())
        break;
    }
    core->monitor_running_ = false;
    core->NotifyDrained();
    co_return absl::OkStatus();
  }
  void NotifyDrained() {
    if (!shutdown_ || accepting_ || monitor_running_ || !sessions_.empty())
      return;
    for (auto& waiter : drain_waiters_) waiter->set_value();
    drain_waiters_.clear();
  }

  bycorf::ForeignExecutor executor_;
  const PasswordAuthenticator authenticator_;
  MetaSentinelServerOptions options_;
  NumericEndpoint endpoint_;
  // Discovery inputs are read and the caches written only on the Meta worker.
  DiscoverySource discovery_;
  // All mutable state below is confined to the Meta worker. The main thread
  // observes bind/drain completion through one-shot promises, never a shared
  // request-path lock or a concurrently traversed session registry.
  bycorf::Worker* worker_ = nullptr;
  bycorf::TcpListener listener_;
  std::uint64_t next_connection_id_ = 0;
  bool accepting_ = false;
  bool shutdown_ = false;
  int wake_fd_ = -1;
  std::vector<bycorf::Connection*> sessions_;
  std::map<bycorf::Connection*, SentinelSession*> live_;
  std::size_t output_bytes_ = 0;
  bool monitor_running_ = false;
  MetaDiscoveryEvents events_;
  std::int64_t event_term_ = -1;
  std::uint64_t event_runtime_term_ = 0, event_revision_ = 0;
  MetaLeaderContext* context_ = nullptr;
  std::unique_ptr<MetaCommitSubscription> subscription_;
  std::shared_ptr<std::atomic<std::uint64_t>> committed_signal_;

  std::vector<std::shared_ptr<std::promise<void>>> drain_waiters_;
};

class MetaSentinelServer::SessionBorrow {
 public:
  SessionBorrow(CorePtr core, bycorf::Connection* connection)
      : core_(std::move(core)), connection_(connection) {
    core_->sessions_.push_back(connection_);
    bycorf::BorrowConnectionStorage(connection_);
  }
  SessionBorrow(SessionBorrow&& other) noexcept
      : core_(std::move(other.core_)),
        connection_(std::exchange(other.connection_, nullptr)) {}
  ~SessionBorrow() {
    if (connection_ == nullptr) return;
    const auto it = std::find(core_->sessions_.begin(), core_->sessions_.end(),
                              connection_);
    *it = core_->sessions_.back();
    core_->sessions_.pop_back();
    bycorf::ReleaseConnectionStorage(connection_);
    core_->NotifyDrained();
  }
  bycorf::Connection* connection() const { return connection_; }

 private:
  CorePtr core_;
  bycorf::Connection* connection_;
};

absl::StatusOr<std::shared_ptr<MetaSentinelServer>> MetaSentinelServer::Create(
    bycorf::ForeignExecutor executor,
    MetaSentinelDiscoveryDependencies discovery,
    MetaSentinelServerOptions options) {
  auto endpoint = ParseConcreteNumericEndpoint(options.address_);
  if (!endpoint) {
    return absl::InvalidArgumentError(
        "Sentinel address must be a concrete numeric IP and nonzero port");
  }
  if (options.maxclients_ == 0 || options.query_limit_ == 0 ||
      options.reply_limit_ < 128 ||
      options.total_output_limit_ < options.reply_limit_ ||
      options.subscription_limit_ == 0 ||
      options.progress_timeout_.count() <= 0) {
    return absl::InvalidArgumentError("invalid Sentinel resource limits");
  }
  // Discovery is part of the surface contract: without authority inputs the
  // six verbs could only ever drop connections, so missing dependencies are a
  // startup error rather than a silent capability loss.
  if (discovery.raft_ == nullptr || discovery.state_machine_ == nullptr ||
      discovery.runtime_status_ == nullptr ||
      discovery.diagnostics_ == nullptr || discovery.observation_ttl_ms_ == 0 ||
      discovery.leader_observation_grace_ms_ == 0) {
    return absl::InvalidArgumentError(
        "Sentinel discovery dependencies must be valid");
  }
  return std::shared_ptr<MetaSentinelServer>(new MetaSentinelServer(
      std::make_shared<Core>(executor, std::move(discovery), std::move(options),
                             std::move(*endpoint))));
}

MetaSentinelServer::MetaSentinelServer(CorePtr core) : core_(std::move(core)) {}
MetaSentinelServer::~MetaSentinelServer() { Shutdown(); }

absl::Status MetaSentinelServer::Start() {
  if (started_ || stopped_)
    return absl::FailedPreconditionError("Sentinel already started or stopped");
  auto result = std::make_shared<std::promise<absl::Status>>();
  auto done = result->get_future();
  if (!core_->executor_.Notify([core = core_, result]() noexcept {
        auto& worker = *bycorf::ThisWorker().self_;
        core->worker_ = &worker;
        auto status = core->listener_.Bind(&worker, core->endpoint_.host_,
                                           core->endpoint_.port_, 128, false);
        if (status.ok()) {
          core->accepting_ = true;
          worker.Spawn(AcceptLoop(core));
          core->monitor_running_ = true;
          worker.Spawn(Core::Monitor(core));
        }
        result->set_value(std::move(status));
      }))
    return absl::UnavailableError("Meta worker is stopping");
  started_ = true;
  return done.get();
}

void MetaSentinelServer::Start(MetaLeaderContext& context) {
  if (!core_->executor_.Notify([core = core_, context = &context]() noexcept {
        if (core->shutdown_) return;
        core->RevokeDiscovery();
        core->context_ = context;
        core->Subscribe();
      }))
    std::terminate();
}

void MetaSentinelServer::CancelAndWait() {
  if (stopped_) return;
  auto result = std::make_shared<std::promise<void>>();
  auto done = result->get_future();
  if (!core_->executor_.Notify([core = core_, result]() noexcept {
        core->RevokeDiscovery();
        core->context_ = nullptr;
        core->subscription_.reset();
        core->committed_signal_.reset();
        result->set_value();
      }))
    std::terminate();
  done.get();
}

void MetaSentinelServer::Shutdown() {
  if (!started_ || stopped_) return;
  auto result = std::make_shared<std::promise<void>>();
  auto done = result->get_future();
  if (!core_->executor_.Notify([core = core_, result]() noexcept {
        core->drain_waiters_.push_back(result);
        core->shutdown_ = true;
        core->context_ = nullptr;
        core->subscription_.reset();
        for (auto [connection, session] : core->live_) {
          (void)connection;
          core->Close(*session);
        }
        if (core->accepting_) {
          auto wake = OpenAcceptWake(core->endpoint_);
          if (wake.ok()) {
            core->wake_fd_ = *wake;
          } else {
            // Socket exhaustion must not make shutdown fatal. Shutting down
            // the existing Linux TCP listener wakes its armed io_uring accept
            // without allocating an fd. Close also prevents completion from
            // rearming accept. Keep the listener object and accepting_ alive
            // until AcceptLoop consumes the completion and reports its exit.
            (void)::shutdown(core->listener_.NativeFd(), SHUT_RDWR);
            (void)core->listener_.Close();
          }
        } else {
          (void)core->listener_.Close();
        }
        // BeginClose may resume a session later; its frame-owned borrow keeps
        // the connection alive until all body-local watchdogs have unwound.
        const auto sessions = core->sessions_;
        for (auto* connection : sessions) {
          if (connection->file_.fd_ >= 0)
            (void)::shutdown(connection->file_.fd_, SHUT_RDWR);
          core->worker_->BeginClose(connection,
                                    absl::CancelledError("Sentinel shutdown"),
                                    bycorf::CloseMode::kLocalClose);
        }
        core->NotifyDrained();
      }))
    std::terminate();
  done.get();
  stopped_ = true;
}

bycorf::Task<absl::Status> MetaSentinelServer::AcceptLoop(CorePtr core) {
  while (!core->shutdown_) {
    auto accepted = co_await core->listener_.Accept();
    if (!accepted.ok()) {
      if (core->shutdown_) break;
      const auto slept = co_await bycorf::SleepFor(
          *core->worker_, std::chrono::milliseconds(10));
      if (!slept.ok()) break;
      continue;
    }
    auto* connection = *accepted;
    if (core->shutdown_ ||
        core->sessions_.size() >= core->options_.maxclients_) {
      if (!core->shutdown_) {
        constexpr std::string_view error =
            "-ERR max number of clients reached\r\n";
        // Rejected sockets must not allocate waiting writers or session tasks.
        // This small reply is best effort; an unwritable peer is simply closed.
        (void)::send(connection->file_.fd_, error.data(), error.size(),
                     MSG_DONTWAIT | MSG_NOSIGNAL);
      }
      core->worker_->BeginClose(
          connection, absl::ResourceExhaustedError("Sentinel admission closed"),
          bycorf::CloseMode::kLocalClose);
      if (core->shutdown_) break;
    } else {
      bycorf::TcpStream stream(connection);
      if (!stream.SetReadAhead(false).ok()) {
        (void)stream.Close();
      } else {
        core->worker_->Spawn(SessionLoop(core, std::move(stream),
                                         SessionBorrow(core, connection)));
      }
    }
    co_await bycorf::Yield(*core->worker_);
  }
  if (core->wake_fd_ >= 0) {
    (void)::close(core->wake_fd_);
    core->wake_fd_ = -1;
  }
  (void)core->listener_.Close();
  core->accepting_ = false;
  core->NotifyDrained();
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> MetaSentinelServer::SessionLoop(
    CorePtr core, bycorf::TcpStream stream, SessionBorrow borrow) {
  // Parameter destruction follows body locals, even if Spawn discards the
  // unstarted frame. That ordering is the connection-storage lifetime barrier.
  auto* connection = borrow.connection();
  bool expired = false;
  auto expire = [connection, &expired] {
    expired = true;
    if (connection->file_.fd_ >= 0)
      (void)::shutdown(connection->file_.fd_, SHUT_RDWR);
  };
  using cluster::control::ControlDeadlineWatchdog;
  ControlDeadlineWatchdog authentication_deadline(*core->worker_, expire);
  ControlDeadlineWatchdog read_deadline(*core->worker_, expire);

  const auto timeout = core->options_.progress_timeout_;
  SentinelSession session{};
  session.authenticated_ = !core->authenticator_.required();
  session.id_ = ++core->next_connection_id_;
  if (!session.authenticated_ && !authentication_deadline.Arm(timeout).ok()) {
    (void)stream.Close();
    co_return absl::UnavailableError("Sentinel authentication timer failed");
  }
  session.connection_ = connection;
  core->live_.emplace(connection, &session);
  core->worker_->Spawn(core->WriteSession(&stream, &session));
  RespCommandParser parser(core->options_.query_limit_);
  std::array<std::byte, 4096> buffer;
  std::string pending;
  unsigned commands = 0;
  bool drop = false;
  bool graceful = false;
  while (!drop && !expired && !core->shutdown_ && !session.closing_) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.ok() || *read == 0 || expired || session.closing_) break;
    // The parser can leave a trailing CR unconsumed while awaiting its LF.
    // Preserve only that suffix across reads; consumed bytes belong to the
    // parser. The staging buffer is bounded by one read chunk plus the suffix.
    pending.append(reinterpret_cast<const char*>(buffer.data()), *read);
    std::string_view input(pending);
    while (!input.empty() && !drop && !expired && !core->shutdown_) {
      // A partial request has a total deadline, not a per-byte timeout that
      // slow-drip input could renew indefinitely. Auth has its own deadline.
      if (parser.idle() && !read_deadline.Arm(timeout).ok()) {
        drop = true;
        break;
      }
      auto parsed = parser.Parse(input);
      input.remove_prefix(parsed.consumed_);
      if (parsed.state_ == RespParseState::kNeedMoreData) {
        if (parser.idle()) (void)read_deadline.Disarm();
        break;
      }
      (void)read_deadline.Disarm();
      if (parsed.state_ == RespParseState::kError) {
        session.reply_.Reset();
        session.reply_.AppendError(
            "ERR Protocol error or Sentinel request limit exceeded");
        drop = true;
      } else {
        const bool was_authenticated = session.authenticated_;
        const auto action = ExecuteConnectionCommand(
            core->authenticator_, core->discovery_, session,
            parsed.command_.args_, core->options_);
        drop =
            action == SessionAction::kClose || action == SessionAction::kDrop;
        if (session.authenticated_) {
          (void)authentication_deadline.Disarm();
        } else if (action == SessionAction::kReset && was_authenticated) {
          // RESET starts a new authentication window only when leaving an
          // authenticated session. Anonymous RESET traffic must not renew
          // the initial deadline and hold an admission slot indefinitely.
          if (!authentication_deadline.Arm(timeout).ok()) drop = true;
        }
        // kDrop is the discovery no-reply contract: the loop exits without
        // entering the write path below.
        if (action == SessionAction::kDrop) {
          // A pipelined AUTH on a follower may already have a queued reply.
          // Drain only connection-local replies; a revoked authority session
          // must discard every pending discovery frame instead.
          graceful = !session.authority_bound_;
          break;
        }
      }
      if (session.subscriptions_.size() > core->options_.subscription_limit_ ||
          session.subscription_bytes_ > core->options_.query_limit_) {
        core->Close(session);
        drop = true;
        break;
      }
      auto reply = session.reply_.View();
      if (reply.size() > core->options_.reply_limit_) {
        session.reply_.Reset();
        reply = session.reply_.AppendError("ERR Sentinel reply limit exceeded");
        drop = true;
      }
      if (!core->Enqueue(session, std::string(reply))) {
        drop = true;
        break;
      }
      graceful = drop;
      if (++commands % 32 == 0) co_await bycorf::Yield(*core->worker_);
    }
    pending.erase(0, pending.size() - input.size());
  }
  session.closing_ = true;
  if (!graceful) core->Close(session);
  session.output_changed_.NotifyAll(*core->worker_);
  while (!session.writer_done_) co_await session.writer_finished_.Wait();
  core->live_.erase(connection);
  (void)stream.Close();
  co_return absl::OkStatus();
}

}  // namespace lavik::meta
