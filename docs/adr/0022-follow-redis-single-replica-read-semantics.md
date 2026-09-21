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

# Follow Redis Single replica read semantics

## Status

Accepted for implementation in [#90](https://github.com/eloqdata/lavik/issues/90).

Managed Single follows Redis's ordinary replica read semantics: clients can
read directly without issuing Cluster READONLY, and the default stale-data
policy permits reads of a retained complete population during replication
disconnection. The Redis `replica-serve-stale-data` policy defaults to `yes`;
when disabled, unavailable replication rejects data access with the standard
MASTERDOWN behavior. This chooses Redis client compatibility and read
availability over requiring an online replication link for every replica read;
it promises neither freshness nor read-your-writes.

Population validity remains a separate requirement. A replica without a valid
complete population cannot serve partial data. Destructive FULL closes read
admission and drains existing operations before reset, reopening only after
complete replacement is ready. Retaining reads through a link outage does not
require preserving the old population throughout destructive FULL. Expired
keys are hidden from ordinary reads without granting independent expiration
mutation authority.

This decision concerns Single client reads. Meta remains the authority for
roles and writes; managed replicas reject client dataset writes, and losing
Owner authority does not itself establish a readable replica role. Native
replication, Candidate Recovery, and Follow Owner retain their existing
protocols. Cluster retains its explicit READONLY contract. Implementing the
Single policy must separate population readability from link connectivity
while preserving reset and serving-generation barriers.

References: [Redis replica configuration](https://github.com/redis/redis/blob/7.2/redis.conf),
[replication and expiration semantics](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/).
