# Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
# Build the companion toolchain with Dockerfile.toolchain first.
ARG LAVIK_BUILD_IMAGE=lavik-admin-toolchain:local
FROM ${LAVIK_BUILD_IMAGE} AS build
COPY CMakeLists.txt /src/
COPY cmake /src/cmake
COPY app /src/app
COPY include /src/include
COPY src /src/src
COPY raft /src/raft
COPY bycorf /src/bycorf
COPY third_party /src/third_party
RUN cmake -S /src -B /build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DLAVIK_ENABLE_OPT=OFF -DBUILD_TESTING=OFF \
    && cmake --build /build --target lavik-ctl -j2

FROM node:24.15.0-bookworm-slim AS node
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
      libstdc++6 && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home lavik \
    && mkdir -p /data && chown lavik:lavik /data
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=build /build/lavik-ctl /usr/local/bin/lavik-ctl
COPY LICENSE NOTICE /opt/lavik-admin/
COPY --from=build /usr/share/common-licenses/Apache-2.0 /opt/lavik-admin/OPENSSL-LICENSE.txt
COPY --chown=lavik:lavik admin /opt/lavik-admin
ENV LAVIK_ADMIN_DATA=/data/lavik-admin LAVIK_ADMIN_BIND=0.0.0.0
USER lavik
WORKDIR /opt/lavik-admin
EXPOSE 4173
CMD ["node", "server.mjs"]
