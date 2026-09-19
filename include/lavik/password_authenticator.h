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

#include <array>
#include <string_view>

namespace lavik {

// Immutable default-user password verifier. Each listener supplies its own
// password; authentication state belongs to its connections, not this object.
// Only a SHA-256 digest is retained. This is not an ACL or an operator
// identity.
class PasswordAuthenticator {
 public:
  explicit PasswordAuthenticator(std::string_view password);

  bool required() const noexcept { return required_; }
  // With no configured password, the default user accepts any password. Each
  // command handler owns its AUTH error contract: Data retains its historical
  // behavior; Sentinel follows Redis 7.2's distinct one/two-argument forms.
  bool Authenticate(std::string_view username,
                    std::string_view password) const noexcept;

 private:
  bool required_;
  std::array<unsigned char, 32> digest_{};
};

}  // namespace lavik
