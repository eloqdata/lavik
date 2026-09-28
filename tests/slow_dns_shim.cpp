// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
// Process-local resolver interposition for the asynchronous DNS contract test.
#include <dlfcn.h>
#include <netdb.h>
#include <unistd.h>

#include <cstring>

extern "C" int getaddrinfo(const char* host, const char* service,
                           const addrinfo* hints, addrinfo** result) {
  using Resolver =
      int (*)(const char*, const char*, const addrinfo*, addrinfo**);
  const auto resolve =
      reinterpret_cast<Resolver>(dlsym(RTLD_NEXT, "getaddrinfo"));
  if (host && std::strcmp(host, "slow.lavik.invalid") == 0 &&
      !(hints && (hints->ai_flags & AI_NUMERICHOST))) {
    usleep(500000);
    return EAI_AGAIN;
  }
  return resolve(host, service, hints, result);
}
