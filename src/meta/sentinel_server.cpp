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
#include <cerrno>
#include <charconv>
#include <future>
#include <limits>
#include <optional>
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
};

enum class SessionAction { kContinue, kReset, kClose };

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

SessionAction ExecuteConnectionCommand(
    const PasswordAuthenticator& authenticator, SentinelSession& session,
    std::span<const std::string> args) {
  auto& reply = session.reply_;
  reply.Reset();
  if (args.empty()) return SessionAction::kContinue;
  const auto is = [&](std::string_view command) {
    return absl::EqualsIgnoreCase(args[0], command);
  };
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
  } else if (is("PING")) {
    if (args.size() == 1)
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
    reply.AppendError("ERR SENTINEL subcommand is not supported");
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
  Core(bycorf::ForeignExecutor executor, MetaSentinelServerOptions options,
       NumericEndpoint endpoint)
      : executor_(executor),
        authenticator_(options.requirepass_),
        options_(std::move(options)),
        endpoint_(std::move(endpoint)) {
    // Only the immutable digest is needed after construction.
    options_.requirepass_.clear();
  }

  void NotifyDrained() {
    if (!shutdown_ || accepting_ || !sessions_.empty()) return;
    for (auto& waiter : drain_waiters_) waiter->set_value();
    drain_waiters_.clear();
  }

  bycorf::ForeignExecutor executor_;
  const PasswordAuthenticator authenticator_;
  MetaSentinelServerOptions options_;
  NumericEndpoint endpoint_;
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
    bycorf::ForeignExecutor executor, MetaSentinelServerOptions options) {
  auto endpoint = ParseNumericEndpoint(options.address_);
  if (!endpoint || endpoint->host_ == "0.0.0.0" || endpoint->host_ == "::" ||
      endpoint->host_ == "::ffff:0.0.0.0") {
    return absl::InvalidArgumentError(
        "Sentinel address must be a concrete numeric IP and nonzero port");
  }
  if (options.maxclients_ == 0 || options.query_limit_ == 0 ||
      options.reply_limit_ < 128 || options.progress_timeout_.count() <= 0) {
    return absl::InvalidArgumentError("invalid Sentinel resource limits");
  }
  return std::shared_ptr<MetaSentinelServer>(
      new MetaSentinelServer(std::make_shared<Core>(
          executor, std::move(options), std::move(*endpoint))));
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
        }
        result->set_value(std::move(status));
      }))
    return absl::UnavailableError("Meta worker is stopping");
  started_ = true;
  return done.get();
}

void MetaSentinelServer::Shutdown() {
  if (!started_ || stopped_) return;
  auto result = std::make_shared<std::promise<void>>();
  auto done = result->get_future();
  if (!core_->executor_.Notify([core = core_, result]() noexcept {
        core->drain_waiters_.push_back(result);
        core->shutdown_ = true;
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
  ControlDeadlineWatchdog write_deadline(*core->worker_, expire);
  const auto timeout = core->options_.progress_timeout_;
  SentinelSession session{.authenticated_ = !core->authenticator_.required(),
                          .id_ = ++core->next_connection_id_,
                          .name_ = {},
                          .library_name_ = {},
                          .library_version_ = {}};
  if (!session.authenticated_ && !authentication_deadline.Arm(timeout).ok()) {
    (void)stream.Close();
    co_return absl::UnavailableError("Sentinel authentication timer failed");
  }
  RespCommandParser parser(core->options_.query_limit_);
  std::array<std::byte, 4096> buffer;
  std::string pending;
  unsigned commands = 0;
  bool drop = false;
  while (!drop && !expired && !core->shutdown_) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.ok() || *read == 0 || expired) break;
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
            core->authenticator_, session, parsed.command_.args_);
        drop = action == SessionAction::kClose;
        if (session.authenticated_) {
          (void)authentication_deadline.Disarm();
        } else if (action == SessionAction::kReset && was_authenticated) {
          // RESET starts a new authentication window only when leaving an
          // authenticated session. Anonymous RESET traffic must not renew
          // the initial deadline and hold an admission slot indefinitely.
          if (!authentication_deadline.Arm(timeout).ok()) drop = true;
        }
      }
      auto reply = session.reply_.View();
      if (reply.size() > core->options_.reply_limit_) {
        session.reply_.Reset();
        reply = session.reply_.AppendError("ERR Sentinel reply limit exceeded");
        drop = true;
      }
      if (!write_deadline.Arm(timeout).ok()) {
        drop = true;
        break;
      }
      auto written = co_await stream.WriteAll(
          std::as_bytes(std::span(reply.data(), reply.size())));
      (void)write_deadline.Disarm();
      if (!written.ok()) {
        drop = true;
        break;
      }
      if (++commands % 32 == 0) co_await bycorf::Yield(*core->worker_);
    }
    pending.erase(0, pending.size() - input.size());
  }
  (void)stream.Close();
  co_return absl::OkStatus();
}

}  // namespace lavik::meta
