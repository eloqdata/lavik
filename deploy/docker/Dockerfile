# syntax=docker/dockerfile:1
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

# Package the published beta, not whichever source revision builds this image.
# Both targets use the minimal release (kernel TCP + io_uring, without SPDK).
FROM --platform=$BUILDPLATFORM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 AS download
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
ARG TARGETARCH
COPY release.sha256 fetch-release.sh /tmp/release/
RUN bash /tmp/release/fetch-release.sh "$TARGETARCH"

FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates redis-tools tini util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 lavik \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /data lavik \
    && install -d -o 10001 -g 10001 -m 0700 /data
COPY --from=download /out/docs/ /usr/share/doc/lavik/
COPY --from=download /out/bin/lavik /usr/local/bin/lavik
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
LABEL org.opencontainers.image.title="Lavik" \
      org.opencontainers.image.version="0.1.0-beta.1" \
      org.opencontainers.image.revision="3955b98d43b312324aa8d52775df52cfb111c0d0" \
      org.opencontainers.image.source="https://github.com/eloqdata/lavik" \
      org.opencontainers.image.url="https://github.com/eloqdata/lavik/releases/tag/v0.1.0-beta.1" \
      org.opencontainers.image.licenses="Apache-2.0"
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
EXPOSE 6379
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["lavik"]

FROM runtime AS cluster
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=download /out/bin/lavik-meta /out/bin/lavik-ctl /usr/local/bin/
COPY --chmod=755 bootstrap-cluster.py /usr/local/bin/bootstrap-cluster
LABEL org.opencontainers.image.description="Lavik Data, Meta, and lavik-ctl for a native primary/follower cluster"
USER 10001:10001

# Keep a plain docker build suitable for a standalone server.
FROM runtime AS single
LABEL org.opencontainers.image.description="Lavik standalone server"
