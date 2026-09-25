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

# Scope managed flush to the local Group

## Status

Accepted and implemented for [#98](https://github.com/eloqdata/lavik/issues/98).

Managed Single and Cluster admit `FLUSHDB` and `FLUSHALL` against the receiving
Data Node's Group under that Group's serving authority. In Single, `FLUSHDB`
clears the selected logical database and `FLUSHALL` clears DB0–15; Cluster
exposes only DB0, so both commands clear that Group's DB0. Replication carries
the mutation within that Group; other Groups are unaffected.

This preserves the shard-local scope of the Redis Open Source Cluster command
interface and keeps mutation authority aligned with the existing Group
boundary. Broadcasting a command to every Group and atomically clearing
multiple Groups are distinct additional contracts, neither of which is implied
by accepting these commands. The decision replaces #98's earlier requirement
to retain the Cluster prohibition.

Both managed modes use the same authority cut as Function catalog mutation:
check authority immediately before the first irreversible write, then retain
the in-flight drain through epoch commit, index detachment, and replication
publication. Later authority revocation does not turn a completed mutation
into a retryable rejection; a definitively successful mutation returns `OK`.

Epoch writes and synchronization retain the existing storage error policy:
short writes, write errors, and synchronization errors propagate to the caller,
and existing storage fault latches remain in force. FLUSH introduces neither
persistent I/O retry nor online repair of storage integrity failures. A storage
error after an irreversible write is not evidence that the database remained
unchanged, and must not be represented as a retryable authority rejection.
Preparation that can fail should finish before the irreversible cut so that
ordinary admission failures leave the dataset unchanged. Both SYNC and ASYNC
require the logical commit to finish; ASYNC only changes the subsequent
reclamation wait.

After durable logical clearing completes, replication follows the ordinary
asynchronous write contract. An invalid replication history does not turn the
local mutation into an uncertain result or introduce a flush-specific serving
fence: the client may receive `OK` while existing history reset and replica
rebuild restore replication. Old sessions and incomplete control barriers must
be cancelled rather than allowed to continue an incomplete history. This
retains the ordinary asynchronous replication loss window; it does not promise
replica acknowledgement before success. A rebuild pinned to the old history
still requires a matching control-plane task.

`SYNC` waits for detached-index retirement and propagates storage or wait
errors through the existing command error path. `ASYNC` starts reclamation
without waiting for it; later background failures retain their existing
diagnostics and storage fault handling rather than changing an already-sent
reply. Neither mode repeats completed clearing or retirement work to retry an
error. Both retain the existing cleanup scope: completion of detached-index
retirement does not promise that every obsolete block or extent is already
reusable. A reclamation error does not roll back the logical clearing.
