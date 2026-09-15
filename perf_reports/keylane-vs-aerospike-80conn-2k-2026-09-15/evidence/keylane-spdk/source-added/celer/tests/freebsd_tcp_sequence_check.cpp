// Exercise the real TCP input/output paths with an in-process Ethernet peer.
// A constant receive window keeps bulk ACKs on header prediction; changing
// that window after 2 GiB must not revive the initial-sequence-number check.
#include <arpa/inet.h>
#include <sys/random.h>
#include <time.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <span>

#include "../src/io/freebsd/abi.h"

namespace {
constexpr uint32_t kPeerIp = 0xc6130001;
constexpr uint32_t kServerIp = 0xc6130002;
constexpr std::array<uint8_t, 6> kPeerMac{2, 0, 0, 0, 0, 1};
constexpr std::array<uint8_t, 6> kServerMac{2, 0, 0, 0, 0, 2};

void Check(bool ok, const char* message) {
  if (!ok) {
    std::fprintf(stderr, "FAIL: %s\n", message);
    std::exit(1);
  }
}
void Put16(uint8_t* p, uint16_t value) {
  p[0] = value >> 8;
  p[1] = value;
}
void Put32(uint8_t* p, uint32_t value) {
  Put16(p, value >> 16);
  Put16(p + 2, value);
}
uint16_t Get16(const uint8_t* p) { return (uint16_t(p[0]) << 8) | p[1]; }
uint32_t Get32(const uint8_t* p) {
  return (uint32_t(Get16(p)) << 16) | Get16(p + 2);
}
uint16_t Checksum(std::span<const uint8_t> bytes, uint32_t sum = 0) {
  size_t i = 0;
  for (; i + 1 < bytes.size(); i += 2) sum += Get16(bytes.data() + i);
  if (i < bytes.size()) sum += uint32_t(bytes[i]) << 8;
  while (sum >> 16) sum = (sum & 65535) + (sum >> 16);
  return ~sum;
}
uint64_t Now(clockid_t clock) {
  timespec t{};
  Check(clock_gettime(clock, &t) == 0, "clock_gettime");
  return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
void* Allocate(size_t size, size_t alignment) {
  void* p = nullptr;
  if (posix_memalign(&p, alignment, size)) return nullptr;
  std::memset(p, 0xa5, size);
  return p;
}
void Random(void* buffer, size_t size) {
  auto* p = static_cast<uint8_t*>(buffer);
  while (size) {
    const ssize_t n = getrandom(p, size, 0);
    if (n < 0 && errno == EINTR) continue;
    Check(n > 0, "getrandom");
    p += n;
    size -= n;
  }
}

struct Peer {
  uint32_t next = 100001;
  uint32_t server_next = 0;
  uint64_t received = 0;
  bool syn_received = false;

  static int Transmit(void* context, const void* data, size_t size) {
    auto& peer = *static_cast<Peer*>(context);
    const auto* p = static_cast<const uint8_t*>(data);
    if (size < 54 || Get16(p + 12) != 0x0800 || p[23] != 6) return 0;
    const size_t ip_header = (p[14] & 15) * 4;
    const uint8_t* tcp = p + 14 + ip_header;
    const size_t tcp_header = (tcp[12] >> 4) * 4;
    Check(14 + Get16(p + 16) <= size, "complete output frame");
    Check(!(tcp[13] & 4), "unexpected TCP reset");
    if (tcp[13] & 2) {
      Check(!peer.syn_received, "single SYN-ACK");
      peer.syn_received = true;
      peer.server_next = Get32(tcp + 4) + 1;
    }
    const size_t payload = Get16(p + 16) - ip_header - tcp_header;
    if (payload) {
      Check(Get32(tcp + 4) == peer.server_next, "contiguous TCP output");
      peer.server_next += payload;
      peer.received += payload;
    }
    return 0;
  }

  void LearnNeighbor() {
    std::array<uint8_t, 42> frame{};
    std::memcpy(frame.data(), kServerMac.data(), 6);
    std::memcpy(frame.data() + 6, kPeerMac.data(), 6);
    Put16(frame.data() + 12, 0x0806);
    auto* arp = frame.data() + 14;
    Put16(arp, 1);
    Put16(arp + 2, 0x0800);
    arp[4] = 6;
    arp[5] = 4;
    Put16(arp + 6, 1);
    std::memcpy(arp + 8, kPeerMac.data(), 6);
    Put32(arp + 14, kPeerIp);
    Put32(arp + 24, kServerIp);
    celer_bsd_input(frame.data(), frame.size());
  }

  void Packet(uint8_t flags, uint16_t window, uint32_t ack,
              std::span<const uint8_t> payload = {}) {
    std::array<uint8_t, 128> frame{};
    std::memcpy(frame.data(), kServerMac.data(), 6);
    std::memcpy(frame.data() + 6, kPeerMac.data(), 6);
    Put16(frame.data() + 12, 0x0800);
    auto* ip = frame.data() + 14;
    auto* tcp = ip + 20;
    const size_t tcp_header = (flags & 2) ? 24 : 20;
    const size_t tcp_size = tcp_header + payload.size();
    Check(34 + tcp_size <= frame.size(), "input packet fits");
    ip[0] = 0x45;
    Put16(ip + 2, 20 + tcp_size);
    ip[8] = 64;
    ip[9] = 6;
    Put32(ip + 12, kPeerIp);
    Put32(ip + 16, kServerIp);
    Put16(ip + 10, Checksum({ip, 20}));
    Put16(tcp, 40000);
    Put16(tcp + 2, 16390);
    Put32(tcp + 4, (flags & 2) ? next - 1 : next);
    Put32(tcp + 8, ack);
    tcp[12] = (tcp_header / 4) << 4;
    tcp[13] = flags;
    Put16(tcp + 14, window);
    if (flags & 2) {
      tcp[20] = 2;
      tcp[21] = 4;
      Put16(tcp + 22, 1460);
    }
    if (!payload.empty())
      std::memcpy(tcp + tcp_header, payload.data(), payload.size());
    const uint32_t pseudo = (kPeerIp >> 16) + (kPeerIp & 65535) +
                            (kServerIp >> 16) + (kServerIp & 65535) + 6 +
                            tcp_size;
    Put16(tcp + 16, Checksum({tcp, tcp_size}, pseudo));
    celer_bsd_input(frame.data(), 34 + tcp_size);
  }
};
}  // namespace

int main() {
  celer_bsd_host host{
      Allocate,
      std::free,
      [] { return Now(CLOCK_MONOTONIC); },
      [] { return Now(CLOCK_REALTIME); },
      Random,
      [](const char* p, size_t n) { std::fwrite(p, 1, n, stderr); },
      std::abort};
  Check(celer_bsd_initialize(&host, 1) == 0, "initialize stack");
  Peer peer;
  celer_bsd_interface interface{};
  interface.address = htonl(kServerIp);
  interface.netmask = htonl(0xffffff00);
  std::memcpy(interface.mac, kServerMac.data(), 6);
  interface.mtu = 1500;
  interface.transmit = Peer::Transmit;
  interface.context = &peer;
  Check(celer_bsd_attach_interface(&interface) == 0, "attach interface");
  struct socket* listener = nullptr;
  struct socket* socket = nullptr;
  Check(celer_bsd_listen(interface.address, 16390, 16, &listener) == 0,
        "listen");
  peer.LearnNeighbor();
  peer.Packet(2, 65535, 0);
  Check(peer.syn_received, "receive SYN-ACK");
  peer.Packet(16, 65535, peer.server_next);
  Check(celer_bsd_accept(listener, &socket) == 0, "accept");

  // The fix must preserve rejection of an ACK preceding the initial sequence.
  const std::array<uint8_t, 1> request{'x'};
  peer.Packet(24, 65535, peer.server_next - 10, request);
  uint8_t byte = 0;
  size_t count = 0;
  Check(celer_bsd_receive(socket, &byte, 1, &count) == 35 && count == 0,
        "reject ghost ACK before bulk transfer");

  const std::array<uint8_t, 2048> reply{};
  constexpr uint64_t kBytes = uint64_t{3} << 30;
  for (uint64_t total = 0; total < kBytes; total += reply.size()) {
    size_t sent = 0;
    Check(celer_bsd_send(socket, reply.data(), reply.size(), &sent) == 0 &&
              sent == reply.size(),
          "bulk send makes progress");
    Check(peer.received == total + reply.size(), "bulk reply reaches peer");
    peer.Packet(16, 65535, peer.server_next);
    if ((total & ((1 << 20) - 1)) == 0) celer_bsd_poll();
  }
  // A window change forces slow ACK processing after signed sequence distance
  // from ISS has wrapped. An ordinary request must still enter the receive buf.
  peer.Packet(24, 65534, peer.server_next, request);
  Check(celer_bsd_receive(socket, &byte, 1, &count) == 0 && count == 1 &&
            byte == 'x',
        "valid request after 3 GiB and receive-window change");
  ++peer.next;
  Check(celer_bsd_close(socket) == 0, "close session");
  Check(celer_bsd_close(listener) == 0, "close listener");
  std::puts(
      "PASS: ghost ACK rejected; 3 GiB acknowledged on fast path; slow-path "
      "request accepted");
}
