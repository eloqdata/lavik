#pragma once

#include <cstdint>

namespace keylane {

enum class RespVersion : std::uint8_t {
  k2 = 2,
  k3 = 3,
};

}  // namespace keylane
