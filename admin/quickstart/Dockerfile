# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# Runtime tools only. Lavik and Node arrive in the verified release archive.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl openssh-client python3 redis-tools util-linux \
      libstdc++6 libnuma1 libuuid1 \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/release/current/runtime/bin:${PATH}"
