#pragma once

// MetaHash: the meta plane's shared SHA-256 (FIPS 180-4), one-shot over a
// byte string. Header-only and self-contained: keylane_meta_core deliberately
// does not link OpenSSL (the meta-plane link surface carries no crypto
// library). Its callers need reproducible opaque digests for durable identity,
// integrity checks, and the audit rolling hash chain, so the standard algorithm
// is implemented locally rather than using a process-specific token.

#include <array>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "keylane/meta/commands.h"

namespace keylane::meta {
namespace meta_hash_detail {

// Round constants: first 32 bits of the fractional parts of the cube roots
// of the first 64 primes.
inline constexpr std::array<std::uint32_t, 64> kSha256RoundConstants = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

inline std::uint32_t Sha256RotateRight(std::uint32_t x, std::uint32_t n) {
  return (x >> n) | (x << (32 - n));
}

inline void Sha256Compress(std::array<std::uint32_t, 8>& state,
                           const std::uint8_t* block) {
  std::array<std::uint32_t, 64> w{};
  for (std::uint32_t i = 0; i < 16; ++i) {
    w[i] = (static_cast<std::uint32_t>(block[i * 4]) << 24) |
           (static_cast<std::uint32_t>(block[i * 4 + 1]) << 16) |
           (static_cast<std::uint32_t>(block[i * 4 + 2]) << 8) |
           static_cast<std::uint32_t>(block[i * 4 + 3]);
  }
  for (std::uint32_t i = 16; i < 64; ++i) {
    const std::uint32_t s0 = Sha256RotateRight(w[i - 15], 7) ^
                             Sha256RotateRight(w[i - 15], 18) ^
                             (w[i - 15] >> 3);
    const std::uint32_t s1 = Sha256RotateRight(w[i - 2], 17) ^
                             Sha256RotateRight(w[i - 2], 19) ^ (w[i - 2] >> 10);
    w[i] = w[i - 16] + s0 + w[i - 7] + s1;
  }
  std::uint32_t a = state[0], b = state[1], c = state[2], d = state[3],
                e = state[4], f = state[5], g = state[6], h = state[7];
  for (std::uint32_t i = 0; i < 64; ++i) {
    const std::uint32_t s1 = Sha256RotateRight(e, 6) ^
                             Sha256RotateRight(e, 11) ^
                             Sha256RotateRight(e, 25);
    const std::uint32_t ch = (e & f) ^ (~e & g);
    const std::uint32_t t1 = h + s1 + ch + kSha256RoundConstants[i] + w[i];
    const std::uint32_t s0 = Sha256RotateRight(a, 2) ^
                             Sha256RotateRight(a, 13) ^
                             Sha256RotateRight(a, 22);
    const std::uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
    const std::uint32_t t2 = s0 + maj;
    h = g;
    g = f;
    f = e;
    e = d + t1;
    d = c;
    c = b;
    b = a;
    a = t1 + t2;
  }
  state[0] += a;
  state[1] += b;
  state[2] += c;
  state[3] += d;
  state[4] += e;
  state[5] += f;
  state[6] += g;
  state[7] += h;
}

}  // namespace meta_hash_detail

// SHA-256 of `data` (FIPS 180-4). One-shot; callers hashing a concatenation
// (e.g. the audit chain's previous-hash || record-bytes) concatenate first.
inline MetaHash256 MetaSha256(std::string_view data) {
  // Initial state: first 32 bits of the fractional parts of the square roots
  // of the first 8 primes.
  std::array<std::uint32_t, 8> state = {0x6a09e667, 0xbb67ae85, 0x3c6ef372,
                                        0xa54ff53a, 0x510e527f, 0x9b05688c,
                                        0x1f83d9ab, 0x5be0cd19};
  const auto* bytes = reinterpret_cast<const std::uint8_t*>(data.data());
  std::size_t offset = 0;
  while (offset + 64 <= data.size()) {
    meta_hash_detail::Sha256Compress(state, bytes + offset);
    offset += 64;
  }
  // Padding: 0x80, zeros to 56 mod 64, then the bit length as u64 BE.
  std::array<std::uint8_t, 128> tail{};
  const std::size_t remaining = data.size() - offset;
  std::memcpy(tail.data(), bytes + offset, remaining);
  tail[remaining] = 0x80;
  const std::size_t tail_blocks = remaining < 56 ? 1 : 2;
  const std::uint64_t bit_length = static_cast<std::uint64_t>(data.size()) * 8;
  for (std::uint32_t i = 0; i < 8; ++i) {
    tail[tail_blocks * 64 - 1 - i] =
        static_cast<std::uint8_t>(bit_length >> (8 * i));
  }
  for (std::size_t block = 0; block < tail_blocks; ++block) {
    meta_hash_detail::Sha256Compress(state, tail.data() + block * 64);
  }
  MetaHash256 out{};
  for (std::uint32_t i = 0; i < 8; ++i) {
    for (std::uint32_t j = 0; j < 4; ++j) {
      out[i * 4 + j] = static_cast<std::uint8_t>(state[i] >> (24 - 8 * j));
    }
  }
  return out;
}

}  // namespace keylane::meta
