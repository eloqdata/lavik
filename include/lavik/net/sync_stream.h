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

#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik::net {

using SyncDeadline = std::chrono::steady_clock::time_point;

struct SyncTlsOptions {
  std::string ca_file_;
  std::string certificate_file_;
  std::string private_key_file_;
  // Empty verifies the dialed numeric IP SAN. Discovery must leave it empty.
  std::string server_name_;
};

struct SyncTarget {
  enum class Transport { kUnix, kTcpPlaintext, kTcpMtls };
  Transport transport_ = Transport::kUnix;
  std::string endpoint_;
  SyncTlsOptions tls_;
};

// Bounded synchronous I/O for CLI and startup threads only. No runtime,
// worker, storage, Admin permission or Raft dependency. One absolute deadline
// covers connect, handshake and every byte of the exchange. A readable cancel
// fd interrupts every wait; ownership and contents of that fd stay with caller.
// TLS uses OpenSSL's socket BIO, so the executable must ignore SIGPIPE and
// handle the returned transport status (as both Data and lavik-ctl do).
class SyncStream {
 public:
  static absl::StatusOr<std::unique_ptr<SyncStream>> Connect(
      const SyncTarget& target, SyncDeadline deadline, int cancel_fd = -1);
  ~SyncStream();
  SyncStream(const SyncStream&) = delete;
  SyncStream& operator=(const SyncStream&) = delete;

  absl::Status WriteAll(std::string_view bytes);
  // EOF after a partial response is DataLoss, never a successful short read.
  absl::StatusOr<std::string> ReadExact(std::size_t length);
  absl::StatusOr<std::string> ReadLine(std::size_t max_bytes);
  // Returns all URI SANs from the verified TLS peer, preserving duplicates so
  // the protocol can enforce its own unique-principal rule.
  absl::StatusOr<std::vector<std::string>> PeerUriSans() const;

 private:
  struct Impl;
  explicit SyncStream(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace lavik::net
