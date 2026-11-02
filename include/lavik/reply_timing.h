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
#include <cstdint>

namespace lavik {
// Optional, borrowed serialization observer. A builder's owner supplies the
// clock and nonblocking sink; the shared RESP protocol library has no runtime,
// storage or diagnostic dependency. Begin/End run synchronously on that owner.
class ReplyTimingObserver {
 public:
  virtual std::uint64_t BeginReplyBuild() noexcept = 0;
  virtual void EndReplyBuild(std::uint64_t token) noexcept = 0;

 protected:
  ~ReplyTimingObserver() = default;
};
}  // namespace lavik
