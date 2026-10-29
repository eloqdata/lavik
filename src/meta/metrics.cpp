/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include "lavik/meta/metrics.h"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <locale>
#include <sstream>
#include <thread>
#include <vector>

#include "lavik/meta/coordinator.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/meta/data_control_server.h"
#include "lavik/meta/raft.h"
#include "lavik/meta/raft_bridge.h"

namespace lavik::meta {
namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t kMaxConnections = 32;
constexpr std::size_t kMaxHeader = 16 * 1024;
constexpr std::size_t kMaxResponse = 8 * 1024 * 1024;
constexpr auto kDeadline = std::chrono::seconds(3);

void Family(std::ostream& out, const char* name, const char* type,
            const char* help) {
  out << "# HELP " << name << ' ' << help << "\n# TYPE " << name << ' ' << type
      << '\n';
}
void Gauge(std::ostream& out, const char* name, double value,
           const char* help) {
  Family(out, name, "gauge", help);
  out << name << ' ' << value << '\n';
}
std::string Response(int code, std::string_view reason, std::string_view body) {
  return "HTTP/1.1 " + std::to_string(code) + " " + std::string(reason) +
         "\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
         "Connection: close\r\nContent-Length: " +
         std::to_string(body.size()) + "\r\n\r\n" + std::string(body);
}
}  // namespace

MetaProposalMetrics::Attempt::Attempt(MetaProposalMetrics& metrics)
    : metrics_(metrics), start_(Clock::now()) {
  metrics_.inflight_.fetch_add(1, std::memory_order_relaxed);
}
MetaProposalMetrics::Attempt::~Attempt() {
  const auto elapsed = Clock::now() - start_;
  auto& result = metrics_.results_[static_cast<std::size_t>(outcome_)];
  const double seconds = std::chrono::duration<double>(elapsed).count();
  const auto bucket = std::lower_bound(kBounds.begin(), kBounds.end(), seconds);
  result.nanoseconds.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count(),
      std::memory_order_relaxed);
  result.buckets[bucket - kBounds.begin()].fetch_add(1,
                                                     std::memory_order_relaxed);
  metrics_.inflight_.fetch_sub(1, std::memory_order_relaxed);
}
std::string MetaProposalMetrics::Collect() const {
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out.precision(17);
  Gauge(
      out, "lavik_meta_proposals_inflight", inflight_.load(),
      "Calls awaiting their first caller result, including executor queueing.");
  Family(out, "lavik_meta_proposals_total", "counter",
         "First caller outcomes; unknown may still commit later.");
  Family(out, "lavik_meta_proposal_duration_seconds", "histogram",
         "From Propose entry to first caller result, including queueing.");
  constexpr std::array names{"applied_success", "applied_rejected",
                             "admission_rejected", "outcome_unknown"};
  for (std::size_t i = 0; i < names.size(); ++i) {
    const auto& result = results_[i];
    std::uint64_t cumulative = 0;
    for (std::size_t b = 0; b <= kBounds.size(); ++b) {
      cumulative += result.buckets[b].load(std::memory_order_relaxed);
      out << "lavik_meta_proposal_duration_seconds_bucket{result=\"" << names[i]
          << "\",le=\"";
      if (b == kBounds.size())
        out << "+Inf";
      else
        out << kBounds[b];
      out << "\"} " << cumulative << '\n';
    }
    out << "lavik_meta_proposal_duration_seconds_count{result=\"" << names[i]
        << "\"} " << cumulative << '\n'
        << "lavik_meta_proposals_total{result=\"" << names[i] << "\"} "
        << cumulative << '\n'
        << "lavik_meta_proposal_duration_seconds_sum{result=\"" << names[i]
        << "\"} " << double(result.nanoseconds.load()) / 1e9 << '\n';
  }
  return out.str();
}

struct MetaMetricsServer::Impl {
  int listener = -1;
  int wake = -1;
  std::thread worker;
  std::atomic<bool> stopping{false};
  std::atomic<Phase> phase{Phase::kStarting};
  std::atomic<std::shared_ptr<const Sources>> sources;
  // These are confined to the metrics worker, never shared with producers.
  std::uint64_t errors = 0, collections = 0;
  double collection_seconds = 0;
  struct Connection {
    int fd;
    Clock::time_point deadline;
    std::string input, output;
    std::size_t written = 0;
    bool metric_response = false;
  };
  std::vector<Connection> clients;

  ~Impl() {
    if (listener >= 0) close(listener);
    if (wake >= 0) close(wake);
  }

  absl::StatusOr<std::string> Collect() {
    auto source = sources.load(std::memory_order_acquire);
    std::string upstream;
    if (source && source->raft) {
      auto collected = source->raft->CollectMetrics();
      if (!collected.ok()) return collected.status();
      upstream = std::move(*collected);
    } else {
      LavikRaftBytes bytes{};
      const int code = lavik_raft_metrics(0, &bytes);
      std::unique_ptr<void, decltype(&std::free)> owner(bytes.data, &std::free);
      if (code != 0) return absl::InternalError("upstream metrics unavailable");
      upstream.assign(static_cast<const char*>(bytes.data), bytes.size);
    }
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out.precision(17);
    out << upstream;
    Family(out, "lavik_meta_phase", "gauge", "Process lifecycle phase.");
    constexpr std::array phases{"starting", "recovering", "running",
                                "stopping"};
    const auto current = static_cast<std::size_t>(phase.load());
    for (std::size_t i = 0; i < phases.size(); ++i)
      out << "lavik_meta_phase{phase=\"" << phases[i] << "\"} "
          << (i == current) << '\n';
    Family(out, "lavik_meta_source_available", "gauge",
           "Whether the named source has been assembled; unavailable fields "
           "are omitted.");
    out << "lavik_meta_source_available{source=\"raft\"} "
        << bool(source && source->raft) << '\n'
        << "lavik_meta_source_available{source=\"proposal\"} "
        << bool(source && source->coordinator) << '\n'
        << "lavik_meta_source_available{source=\"data_control\"} "
        << bool(source && source->data_control) << '\n';
    if (source && source->raft)
      Gauge(out, "lavik_meta_raft_leader_authority",
            source->raft->leader_term() >= 0,
            "Local usable Raft leader authority, independent of leader hints.");
    if (source && source->coordinator)
      out << source->coordinator->proposal_metrics().Collect();
    if (source && source->leadership) {
      const auto state = source->leadership->MetricsLeadership();
      Gauge(out, "lavik_meta_data_control_leadership_age_seconds",
            std::chrono::duration<double>(Clock::now() - state->published_)
                .count(),
            "Time since the last leadership publication; not a quorum "
            "freshness certificate.");
      Gauge(out, "lavik_meta_data_control_eligible",
            state->state_.leader_authority_eligible_,
            "Published Data-control eligibility; observational, not a lease "
            "certificate.");
      Gauge(out, "lavik_meta_data_control_term", state->state_.leader_term_,
            "Published Data-control leader term.");
    }
    if (source && source->data_control) {
      const auto counters = source->data_control->metrics();
      struct Field {
        const char* name;
        bool counter;
        std::uint64_t MetaDataControlMetricsSnapshot::* value;
      };
      constexpr Field fields[]{
          {"live_session_tasks", false,
           &MetaDataControlMetricsSnapshot::live_session_tasks_},
          {"live_authority_session_tasks", false,
           &MetaDataControlMetricsSnapshot::live_authority_session_tasks_},
          {"live_leader_tasks", false,
           &MetaDataControlMetricsSnapshot::live_leader_tasks_},
          {"active_sessions", false,
           &MetaDataControlMetricsSnapshot::active_sessions_},
          {"accepted_sessions_total", true,
           &MetaDataControlMetricsSnapshot::accepted_sessions_},
          {"redirected_sessions_total", true,
           &MetaDataControlMetricsSnapshot::redirected_sessions_},
          {"rejected_sessions_total", true,
           &MetaDataControlMetricsSnapshot::rejected_sessions_},
          {"protocol_errors_total", true,
           &MetaDataControlMetricsSnapshot::protocol_errors_},
          {"full_states_sent_total", true,
           &MetaDataControlMetricsSnapshot::full_states_sent_},
          {"lease_grants_total", true,
           &MetaDataControlMetricsSnapshot::lease_grants_},
          {"lease_denials_total", true,
           &MetaDataControlMetricsSnapshot::lease_denials_},
          {"observations_accepted_total", true,
           &MetaDataControlMetricsSnapshot::observations_accepted_},
          {"observations_rejected_total", true,
           &MetaDataControlMetricsSnapshot::observations_rejected_},
          {"directive_results_committed_total", true,
           &MetaDataControlMetricsSnapshot::directive_results_committed_},
      };
      for (auto field : fields) {
        const std::string name =
            std::string("lavik_meta_data_control_") + field.name;
        Family(out, name.c_str(), field.counter ? "counter" : "gauge",
               "Existing Data-control session statistic, process lifetime.");
        out << name << ' ' << counters.*(field.value) << '\n';
      }
    }
    Family(out, "lavik_meta_collection_errors_total", "counter",
           "Failed collections, including response size or deadline failure.");
    out << "lavik_meta_collection_errors_total " << errors << '\n';
    Family(out, "lavik_meta_collection_duration_seconds", "summary",
           "Completed prior collections on the independent metrics worker.");
    out << "lavik_meta_collection_duration_seconds_count " << collections
        << '\n'
        << "lavik_meta_collection_duration_seconds_sum " << collection_seconds
        << '\n';
    auto body = out.str();
    if (body.size() > kMaxResponse)
      return absl::ResourceExhaustedError("metrics response limit");
    return body;
  }

  void Serve(Connection& client, bool& collected) {
    const auto end = client.input.find("\r\n\r\n");
    if (end == std::string::npos) return;
    const auto line = client.input.substr(0, client.input.find("\r\n"));
    if (line != "GET /metrics HTTP/1.1" && line != "GET /metrics HTTP/1.0") {
      client.output = Response(404, "Not Found", "GET /metrics required\n");
    } else if (collected) {
      client.output =
          Response(503, "Service Unavailable", "metrics collection busy\n");
    } else {
      // At most one collection per poll batch and one retained metric body.
      // Slow readers cannot accumulate full responses or stall Raft workers.
      collected = true;
      const auto start = Clock::now();
      auto body = [&]() -> absl::StatusOr<std::string> {
        try {
          return Collect();
        } catch (const std::exception&) {
          return absl::InternalError("metrics collection exception");
        }
      }();
      collection_seconds +=
          std::chrono::duration<double>(Clock::now() - start).count();
      ++collections;
      if (!body.ok() || Clock::now() >= client.deadline) {
        ++errors;
        client.output =
            Response(503, "Service Unavailable", "metrics collection failed\n");
      } else {
        client.output = Response(200, "OK", *body);
        client.metric_response = true;
      }
    }
    client.input.clear();
  }

  void Run() {
    clients.reserve(kMaxConnections);
    while (!stopping.load(std::memory_order_acquire)) {
      std::vector<pollfd> fds{{wake, POLLIN, 0}, {listener, POLLIN, 0}};
      bool collected = false;
      int timeout = -1;
      const auto now = Clock::now();
      for (const auto& client : clients) {
        fds.push_back(
            {client.fd, short(client.output.empty() ? POLLIN : POLLOUT), 0});
        collected |= client.metric_response;
        const auto remaining =
            std::chrono::ceil<std::chrono::milliseconds>(client.deadline - now)
                .count();
        const int deadline_ms =
            static_cast<int>(std::max<std::int64_t>(0, remaining));
        timeout = timeout < 0 ? deadline_ms : std::min(timeout, deadline_ms);
      }
      // Poll is isolated from all application executors. The wake fd makes
      // teardown immediate, including partial headers and stalled readers.
      if (poll(fds.data(), fds.size(), timeout) < 0 && errno != EINTR) break;
      if (stopping.load(std::memory_order_acquire)) break;
      for (std::size_t i = clients.size(); i-- > 0;) {
        auto& client = clients[i];
        bool done = Clock::now() >= client.deadline;
        const auto events = fds[i + 2].revents;
        if (!done && (events & (POLLERR | POLLNVAL))) done = true;
        if (!done && client.output.empty() && (events & (POLLIN | POLLHUP))) {
          char chunk[4096];
          const auto received = recv(
              client.fd, chunk,
              std::min(sizeof(chunk), kMaxHeader - client.input.size()), 0);
          if (received > 0) {
            client.input.append(chunk, received);
            Serve(client, collected);
            if (client.output.empty() && client.input.size() == kMaxHeader)
              client.output = Response(431, "Request Header Fields Too Large",
                                       "header limit\n");
          } else if (received == 0 || (errno != EAGAIN && errno != EINTR))
            done = true;
        }
        done |= Clock::now() >= client.deadline;
        if (!done && !client.output.empty()) {
          const auto sent =
              send(client.fd, client.output.data() + client.written,
                   client.output.size() - client.written, MSG_NOSIGNAL);
          if (sent > 0)
            client.written += sent;
          else if (sent < 0 && errno != EAGAIN && errno != EINTR)
            done = true;
          done |= client.written == client.output.size();
        }
        if (done) {
          close(client.fd);
          clients.erase(clients.begin() + i);
        }
      }
      // Accept in bounded batches even under continuous connection pressure.
      if (fds[1].revents & POLLIN) {
        for (std::size_t i = 0; i < kMaxConnections; ++i) {
          int fd =
              accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (fd < 0) break;
          if (clients.size() == kMaxConnections)
            close(fd);
          else
            clients.push_back({fd, Clock::now() + kDeadline, {}, {}, 0});
        }
      }
    }
    for (auto& client : clients) close(client.fd);
    clients.clear();
  }
};

MetaMetricsServer::MetaMetricsServer(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
MetaMetricsServer::~MetaMetricsServer() { Stop(); }
absl::StatusOr<std::unique_ptr<MetaMetricsServer>> MetaMetricsServer::Start(
    const std::string& bind, std::uint16_t port) {
  auto impl = std::make_unique<Impl>();
  sockaddr_storage address{};
  socklen_t size;
  int family;
  auto* ipv4 = reinterpret_cast<sockaddr_in*>(&address);
  auto* ipv6 = reinterpret_cast<sockaddr_in6*>(&address);
  if (inet_pton(AF_INET, bind.c_str(), &ipv4->sin_addr) == 1) {
    family = AF_INET;
    size = sizeof(*ipv4);
    ipv4->sin_family = family;
    ipv4->sin_port = htons(port);
  } else if (inet_pton(AF_INET6, bind.c_str(), &ipv6->sin6_addr) == 1) {
    family = AF_INET6;
    size = sizeof(*ipv6);
    ipv6->sin6_family = family;
    ipv6->sin6_port = htons(port);
  } else
    return absl::InvalidArgumentError(
        "metrics-bind must be a numeric IP address");
  impl->listener =
      socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (impl->listener < 0) return absl::InternalError(std::strerror(errno));
  int reuse = 1;
  setsockopt(impl->listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  if (::bind(impl->listener, reinterpret_cast<sockaddr*>(&address), size) !=
          0 ||
      listen(impl->listener, kMaxConnections) != 0)
    return absl::UnavailableError(std::strerror(errno));
  impl->wake = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (impl->wake < 0) return absl::InternalError(std::strerror(errno));
  auto server = std::unique_ptr<MetaMetricsServer>(
      new MetaMetricsServer(std::move(impl)));
  server->impl_->worker =
      std::thread([state = server->impl_.get()] { state->Run(); });
  return server;
}
void MetaMetricsServer::SetPhase(Phase phase) { impl_->phase.store(phase); }
void MetaMetricsServer::Publish(Sources sources) {
  impl_->sources.store(std::make_shared<const Sources>(std::move(sources)),
                       std::memory_order_release);
}
void MetaMetricsServer::Stop() {
  if (!impl_->stopping.exchange(true)) {
    impl_->phase.store(Phase::kStopping);
    const std::uint64_t one = 1;
    const auto ignored = write(impl_->wake, &one, sizeof(one));
    (void)ignored;
  }
  if (impl_->worker.joinable()) impl_->worker.join();
  if (impl_->listener >= 0) {
    close(impl_->listener);
    impl_->listener = -1;
  }
  impl_->sources.store(nullptr);
}
}  // namespace lavik::meta
