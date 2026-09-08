#pragma once

// Canonical parser for numeric TCP endpoints shared by startup configuration,
// Meta durable validation/projection, and the Data control client. Endpoint
// text is IPv4:port or [IPv6]:port; unbracketed IPv6 is rejected so the host
// and port boundary has exactly one interpretation.

#include <arpa/inet.h>

#include <array>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>

namespace keylane {

struct NumericEndpoint {
  std::string host_;
  std::uint16_t port_ = 0;

  friend bool operator==(const NumericEndpoint&,
                         const NumericEndpoint&) = default;
};

// Parses an unambiguous numeric endpoint and normalizes the returned IP text
// with inet_ntop. Transport tags such as tcp:// and tls:// belong to callers;
// strip them before entering this syntax boundary.
inline std::optional<NumericEndpoint> ParseNumericEndpoint(
    std::string_view endpoint) {
  std::string_view host;
  std::string_view port_text;
  if (!endpoint.empty() && endpoint.front() == '[') {
    const std::size_t close = endpoint.find(']');
    if (close == std::string_view::npos || close + 1 >= endpoint.size() ||
        endpoint[close + 1] != ':') {
      return std::nullopt;
    }
    host = endpoint.substr(1, close - 1);
    port_text = endpoint.substr(close + 2);
  } else {
    const std::size_t colon = endpoint.rfind(':');
    if (colon == std::string_view::npos || endpoint.find(':') != colon) {
      return std::nullopt;
    }
    host = endpoint.substr(0, colon);
    port_text = endpoint.substr(colon + 1);
  }
  if (host.empty() || port_text.empty()) return std::nullopt;

  std::uint32_t port = 0;
  const auto parsed = std::from_chars(
      port_text.data(), port_text.data() + port_text.size(), port);
  if (parsed.ec != std::errc{} ||
      parsed.ptr != port_text.data() + port_text.size() || port == 0 ||
      port > std::numeric_limits<std::uint16_t>::max()) {
    return std::nullopt;
  }

  std::array<char, INET6_ADDRSTRLEN> normalized{};
  const std::string host_text(host);
  in_addr address4{};
  if (::inet_pton(AF_INET, host_text.c_str(), &address4) == 1) {
    if (::inet_ntop(AF_INET, &address4, normalized.data(), normalized.size()) ==
        nullptr) {
      return std::nullopt;
    }
  } else {
    in6_addr address6{};
    if (::inet_pton(AF_INET6, host_text.c_str(), &address6) != 1 ||
        ::inet_ntop(AF_INET6, &address6, normalized.data(),
                    normalized.size()) == nullptr) {
      return std::nullopt;
    }
  }
  return NumericEndpoint{.host_ = normalized.data(),
                         .port_ = static_cast<std::uint16_t>(port)};
}

// Returns the unique host:port spelling for a parsed numeric endpoint. IPv6
// hosts are bracketed so the address/port boundary remains unambiguous.
inline std::string FormatNumericEndpoint(const NumericEndpoint& endpoint) {
  if (endpoint.host_.find(':') != std::string::npos) {
    return "[" + endpoint.host_ + "]:" + std::to_string(endpoint.port_);
  }
  return endpoint.host_ + ":" + std::to_string(endpoint.port_);
}

}  // namespace keylane
