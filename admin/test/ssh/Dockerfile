# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-server python3 ca-certificates redis-tools \
    && rm -rf /var/lib/apt/lists/* /etc/ssh/ssh_host_* \
    && useradd --create-home --uid 10001 --shell /bin/bash lavik \
    && usermod --password '*' lavik \
    && install -d -m 0700 -o lavik -g lavik /home/lavik/.ssh \
    && mkdir -p /run/sshd
COPY admin/test/ssh/start.sh /fixture-start.sh
CMD ["sh", "/fixture-start.sh"]
