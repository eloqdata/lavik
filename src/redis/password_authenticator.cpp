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

#include "lavik/password_authenticator.h"

#include <openssl/crypto.h>
#include <openssl/sha.h>

namespace lavik {

PasswordAuthenticator::PasswordAuthenticator(std::string_view password)
    : required_(!password.empty()) {
  static_assert(SHA256_DIGEST_LENGTH == 32);
  SHA256(reinterpret_cast<const unsigned char*>(password.data()),
         password.size(), digest_.data());
}

bool PasswordAuthenticator::Authenticate(
    std::string_view username, std::string_view password) const noexcept {
  if (!required_) return username == "default";
  std::array<unsigned char, SHA256_DIGEST_LENGTH> candidate{};
  SHA256(reinterpret_cast<const unsigned char*>(password.data()),
         password.size(), candidate.data());
  const bool password_matches =
      CRYPTO_memcmp(candidate.data(), digest_.data(), digest_.size()) == 0;
  return username == "default" && password_matches;
}

}  // namespace lavik
