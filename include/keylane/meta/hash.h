#pragma once

// Durable intent and population-manifest fingerprints. OpenSSL owns the
// SHA-256 implementation; control framing and FDS do not use content hashes.

#include <openssl/sha.h>

#include <cstdlib>
#include <string_view>

#include "keylane/meta/commands.h"

namespace keylane::meta {

// Computes the stable SHA-256 representation used by existing durable data.
// Crypto-provider failure is fatal: returning a fabricated digest could alias
// different durable intents or population documents.
inline MetaHash256 MetaSha256(std::string_view data) {
  MetaHash256 digest{};
  if (SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(),
             digest.data()) == nullptr) {
    std::abort();
  }
  return digest;
}

}  // namespace keylane::meta
