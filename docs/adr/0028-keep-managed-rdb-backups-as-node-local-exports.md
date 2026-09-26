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

# Keep managed RDB backups as node-local exports

## Status

Accepted and implemented for [#199](https://github.com/eloqdata/lavik/issues/199).

## Decision

The compatibility reference for SORT_RO, SAVE, and BGSAVE in this work is
Redis 7.2.14, including Cluster restrictions and execution-context behavior.

SAVE and BGSAVE export the receiving Data Node's dataset as an RDB. Single
includes all logical databases; Cluster includes the local Group's DB0.
Recovery acceptance requires that Redis 7.2.14 can load the generated RDB and
recover its contents. It does not require a Lavik import test or a new managed
Group restore procedure. This keeps command enablement within the existing
export mechanism: managed restoration would additionally need to establish
population identity, replication history, and Meta authorization. An RDB
export supplies none of that authority or Clean Shutdown Proof.

Retain the existing backup lifecycle policy when authority is revoked or the
node changes role. Snapshot capture retains its serving-generation check;
after capture, scanning and file publication do not acquire a new requirement
to cancel or revalidate authority solely because of that transition. Existing
client-connection retirement and snapshot failure handling still apply.
This decision concerns the backup job's lifecycle; command replies, concurrent
save requests, and execution-context restrictions follow the selected Redis
version.

The issue's safety acceptance remains in force: slow I/O and population
replacement must not produce a mixed or partial successful export, access the
wrong population, or strand lifecycle progress. Those cases require validation
and correction of defects without introducing a new backup cancellation
policy. No managed restore flow or cluster-wide backup coordination is added.
