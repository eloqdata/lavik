// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <netdb.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>

#include "lavik/client_endpoint.h"

namespace lavik {

// libc DNS cannot be cancelled. Only the bounded query's own inputs/results
// cross threads; abandoning a caller never leaves a worker or socket borrowed.
struct AsyncDnsQuery {
  ~AsyncDnsQuery() {
    if (addresses_) ::freeaddrinfo(addresses_);
  }
  std::string host_, service_;
  addrinfo* addresses_ = nullptr;
  int result_ = EAI_AGAIN;
  std::atomic<bool> done_{false};
  inline static std::atomic<unsigned> in_flight_{0};

  static std::shared_ptr<AsyncDnsQuery> Start(std::string_view host,
                                              std::uint16_t port) {
    auto query = std::make_shared<AsyncDnsQuery>();
    query->host_ = host;
    query->service_ = std::to_string(port);
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_NUMERICHOST;
    query->result_ =
        ::getaddrinfo(query->host_.c_str(), query->service_.c_str(), &hints,
                      &query->addresses_);
    if (query->result_ == 0) {
      query->done_.store(true, std::memory_order_release);
      return query;
    }
    unsigned active = in_flight_.load(std::memory_order_relaxed);
    do {
      if (active >= 32) return {};
    } while (!in_flight_.compare_exchange_weak(active, active + 1,
                                               std::memory_order_relaxed));
    hints.ai_flags = 0;
    try {
      std::thread([query, hints] {
        query->result_ =
            ::getaddrinfo(query->host_.c_str(), query->service_.c_str(), &hints,
                          &query->addresses_);
        query->done_.store(true, std::memory_order_release);
        in_flight_.fetch_sub(1, std::memory_order_relaxed);
      }).detach();
    } catch (const std::system_error&) {
      in_flight_.fetch_sub(1, std::memory_order_relaxed);
      return {};
    }
    return query;
  }
};

// Worker-local discovery cache. It keeps the last successful IP on transient
// failure, prunes retired declarations, and publishes immutable snapshots.
// DNS refresh changes neither the stored hostname nor Meta authority.
class ClientDnsCache {
 public:
  using Addresses = std::map<std::string, std::string>;
  std::shared_ptr<const Addresses> Refresh(const std::set<std::string>& hosts) {
    auto next = *snapshot_;
    std::erase_if(entries_, [&](const auto& item) {
      return !hosts.contains(item.first);
    });
    std::erase_if(
        next, [&](const auto& item) { return !hosts.contains(item.first); });
    const auto now = std::chrono::steady_clock::now();
    for (const auto& host : hosts) {
      auto& entry = entries_[host];
      if (entry.query && entry.query->done_.load(std::memory_order_acquire)) {
        if (entry.query->result_ == 0 && entry.query->addresses_) {
          char text[NI_MAXHOST]{};
          auto* address = entry.query->addresses_;
          if (::getnameinfo(address->ai_addr, address->ai_addrlen, text,
                            sizeof(text), nullptr, 0, NI_NUMERICHOST) == 0) {
            const std::string ip = text;
            const auto parsed = ParseConcreteNumericEndpoint(
                ip.find(':') == std::string::npos ? ip + ":1"
                                                  : "[" + ip + "]:1");
            if (parsed) next[host] = parsed->host_;
          }
        }
        entry.query.reset();
        entry.retry = now + std::chrono::seconds(5);
      }
      if (!entry.query && now >= entry.retry) {
        entry.query = AsyncDnsQuery::Start(host, 1);
        if (!entry.query) entry.retry = now + std::chrono::seconds(1);
      }
    }
    if (next != *snapshot_)
      snapshot_ = std::make_shared<const Addresses>(std::move(next));
    return snapshot_;
  }

 private:
  struct Entry {
    std::shared_ptr<AsyncDnsQuery> query;
    std::chrono::steady_clock::time_point retry{};
  };
  std::map<std::string, Entry> entries_;
  std::shared_ptr<const Addresses> snapshot_ =
      std::make_shared<const Addresses>();
};
}  // namespace lavik
