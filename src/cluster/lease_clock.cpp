#include "keylane/cluster/lease_clock.h"

#include <time.h>

#include <chrono>
#include <exception>

#if !defined(__linux__) || !defined(CLOCK_BOOTTIME)
#error "finite cluster leases require Linux CLOCK_BOOTTIME"
#endif

namespace keylane::cluster {

LeaseTime LeaseClockNow() noexcept {
  timespec now{};
  if (::clock_gettime(CLOCK_BOOTTIME, &now) != 0) {
    // Falling back after deadlines have been created would mix clock epochs
    // and could revive expired authority. CLOCK_BOOTTIME is mandatory on the
    // supported Linux runtime, so an unavailable clock is a fail-stop fault.
    std::terminate();
  }
  const auto seconds = std::chrono::seconds(now.tv_sec);
  const auto nanoseconds = std::chrono::nanoseconds(now.tv_nsec);
  return LeaseTime(std::chrono::duration_cast<LeaseDuration>(seconds +
                                                             nanoseconds));
}

}  // namespace keylane::cluster
