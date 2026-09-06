#pragma once

// Assembly helpers for NuRaft's native Asio transport. NuRaft owns peer
// sockets, timers, and its Asio worker pool; Keylane optionally supplies mTLS
// contexts and always supplies replicated member verification, listener
// binding, and allocation bounds through its pinned patch hooks. In plaintext
// mode member ids are checked but are not cryptographically authenticated.

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/status/statusor.h"
#include "libnuraft/asio_service.hxx"
#include "libnuraft/ptr.hxx"

namespace keylane::meta {

class MetaStateMachine;
class NuraftStateMgr;

struct MetaAsioTransportConfig {
  std::string bind_address_;
  std::string tls_ca_cert_file_;
  std::string tls_cert_file_;
  std::string tls_key_file_;
  std::size_t io_threads_ = 2;
  std::uint32_t max_rpc_payload_bytes_ = 64u << 20;

  bool TlsEnabled() const { return !tls_ca_cert_file_.empty(); }
};

// Builds transport callbacks before any NuRaft thread starts. When mTLS is
// enabled it also validates the TLS files; the returned SSL_CTX providers
// transfer one server and one client context to NuRaft, so the options object
// must be consumed by one launcher.
absl::StatusOr<nuraft::asio_service::options> BuildMetaAsioOptions(
    const MetaAsioTransportConfig& config,
    nuraft::ptr<NuraftStateMgr> state_mgr,
    nuraft::ptr<MetaStateMachine> state_machine);

}  // namespace keylane::meta
