#!/bin/sh
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# Disposable test hosts accept either a mounted public key or a lab password.
set -eu
ssh-keygen -A
if [ -f /fixture-key.pub ]; then
  install -m 0600 -o lavik -g lavik /fixture-key.pub /home/lavik/.ssh/authorized_keys
fi
if [ -f /fixture-password ]; then
  { printf 'lavik:'; cat /fixture-password; printf '\n'; } | chpasswd
fi
exec /usr/sbin/sshd -D -e
