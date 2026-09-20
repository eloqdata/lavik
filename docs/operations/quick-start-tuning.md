<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Quick startup tuning

Build Lavik, then generate a CPU plan for the current machine:

```bash
./scripts/build_release.sh
./scripts/plan_startup_tuning.sh > lavik-tuning.env
cat lavik-tuning.env
```

The script uses the default-route NIC. Override it with `--nic eth0` when
client traffic uses another interface. The generated file contains the exact
CPU set, worker count, buffer size, and start parameters for this machine. By
default it keeps about one quarter of the physical cores for the OS and NIC
IRQs. To choose that yourself, pass `--reserve-cores N`.

Provision storage once, then source the generated plan and start Lavik:

```bash
install -d -m 0700 /var/lib/lavik
fallocate -l 100G /var/lib/lavik/data

source ./lavik-tuning.env
taskset -c "$LAVIK_CPUSET" ./build/lavik \
  --threads "$LAVIK_THREADS" --pin-workers \
  --registered-buffer-mb-per-worker "$LAVIK_REGISTERED_BUFFER_MB_PER_WORKER" \
  --busy-poll-us "$LAVIK_BUSY_POLL_US" \
  --flush-max-ms "$LAVIK_FLUSH_MAX_MS" \
  --shutdown-checkpoint \
  --data-file /var/lib/lavik/data
```

`LAVIK_CPUSET` and `LAVIK_THREADS` pin workers to the selected CPUs.
`LAVIK_IRQ_CPUSET` is the disjoint CPU set to reserve for the OS and NIC IRQs.
Apply that reservation in your service manager or follow the
[IRQ tuning guide](irq-affinity-tuning.md) when manually configuring IRQs.

Use the same data path and generated CPU plan on restart. Storage files must
exist before Lavik starts; see [multi-device storage](multi-device-storage.md)
when using several files or raw devices.

The server and generated plan both default to `--flush-max-ms=100`. This bounds
the age of a partially filled write block before periodic flushing requests
submission; size-triggered writes can flush sooner. The setting remains
configurable, and the default write-submission size remains 128 KiB.
