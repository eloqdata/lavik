// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <algorithm>
#include <charconv>
#include <optional>
#include <string>
#include <string_view>

#include "lavik/numeric_endpoint.h"

namespace lavik {

// A declared application route, not a resolved socket or evidence of health.
// Parsing is deterministic: Meta apply/snapshot recovery must never perform
// DNS.
struct ClientEndpoint {
  std::string host_;
  std::uint16_t port_ = 0;
  bool tls_ = false;
  bool tagged_ = false;
  bool hostname_ = false;
  // Runtime publication remembers the declaration across DNS refreshes.
  std::string declared_host_;
};

inline bool ValidClientHostname(std::string_view host) {
  if (host.empty() || host.size() > 253) return false;
  if (host.back() == '.') host.remove_suffix(1);
  if (host.empty() || host.back() == '.') return false;
  bool has_letter = false;
  while (!host.empty()) {
    const auto dot = host.find('.');
    auto label = host.substr(0, dot);
    if (label.empty() || label.size() > 63 || label.front() == '-' ||
        label.back() == '-')
      return false;
    for (char c : label) {
      const bool letter = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
      has_letter |= letter;
      if (!letter && !(c >= '0' && c <= '9') && c != '-') return false;
    }
    if (dot == std::string_view::npos) break;
    host.remove_prefix(dot + 1);
  }
  return has_letter;
}

inline std::optional<ClientEndpoint> ParseClientEndpoint(
    std::string_view text) {
  // Match the existing durable endpoint byte budget, including a scheme.
  if (text.size() > 256) return std::nullopt;
  ClientEndpoint out;
  if (text.starts_with("tcp://") || text.starts_with("tls://")) {
    out.tls_ = text.starts_with("tls://");
    out.tagged_ = true;
    text.remove_prefix(6);
  }
  if (auto numeric = ParseNumericEndpoint(text)) {
    if (!ParseConcreteNumericEndpoint(text)) return std::nullopt;
    out.host_ = numeric->host_;
    out.port_ = numeric->port_;
    return out;
  }
  const auto colon = text.find(':');
  if (colon == std::string_view::npos ||
      text.find(':', colon + 1) != std::string_view::npos)
    return std::nullopt;
  auto host = text.substr(0, colon);
  auto port = text.substr(colon + 1);
  if (!ValidClientHostname(host) || port.empty()) return std::nullopt;
  unsigned value = 0;
  auto parsed = std::from_chars(port.data(), port.data() + port.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != port.data() + port.size() ||
      value == 0 || value > 65535)
    return std::nullopt;
  out.host_ = host;
  std::transform(
      out.host_.begin(), out.host_.end(), out.host_.begin(), [](char c) {
        return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : c;
      });
  out.port_ = static_cast<std::uint16_t>(value);
  out.hostname_ = true;
  return out;
}

inline std::string FormatClientEndpoint(const ClientEndpoint& endpoint,
                                        bool with_transport = false) {
  const std::string address =
      endpoint.host_.find(':') != std::string::npos
          ? "[" + endpoint.host_ + "]:" + std::to_string(endpoint.port_)
          : endpoint.host_ + ":" + std::to_string(endpoint.port_);
  return with_transport && endpoint.tagged_
             ? std::string(endpoint.tls_ ? "tls://" : "tcp://") + address
             : address;
}

// Preserve a legacy route's spelling in durable state. Normalizing legacy TCP
// into a tagged string during replay would change immutable membership
// baselines.
inline std::optional<std::string> CanonicalClientEndpoint(
    std::string_view text) {
  auto endpoint = ParseClientEndpoint(text);
  if (!endpoint) return std::nullopt;
  return FormatClientEndpoint(*endpoint, true);
}

inline bool SameClientSocket(std::string_view a, std::string_view b) {
  auto left = ParseClientEndpoint(a);
  auto right = ParseClientEndpoint(b);
  return left && right && left->host_ == right->host_ &&
         left->port_ == right->port_;
}
}  // namespace lavik
