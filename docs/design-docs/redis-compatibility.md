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

# Redis 7.2 compatibility rationale

This historical reference preserves the choice of compatibility baseline and
wire-format research. It is not a current command inventory or an operational
guide. Current behavior belongs in [request serving](../architecture/02-request-serving.md),
[replication](../architecture/05-replication.md),
[Function catalog](../architecture/07-function-catalog.md), and
[grouped collections](../architecture/09-grouped-collections.md).

## Baseline and scope

The compatibility work selected Redis Open Source 7.2 semantics for exposed
commands: syntax, atomicity, errors, and reply shapes. This is a command-level
target, not a promise to implement every Redis command, deployment mode, or
later Redis extension. Protocol-version negotiation and intentional deviations
must be assessed against the current implementation and its tests.

The [vendored compatibility tests](../../tests/valkey/README.md) record their
upstream revision and scope. The executable's `COMMAND` output and command
table describe the exposed surface; passing one suite does not establish
complete Redis compatibility.

## Wire-format decisions

Comparison tests must inspect complete replies and resulting state, including
null array versus null bulk string, exact errors, and rejected-command effects.
RESP3 needs its own negotiated reply expectations. Substituting an arbitrary
newer Redis server as the oracle can obscure version-specific behavior.

The Sorted Set formatting research selected shortest round-trip digits with
Redis 7.2's fixed-versus-scientific notation and exponent spelling. Eligible
integer-valued doubles use ordinary decimal integer notation. Geo replies
have separate precision conventions: distance and coordinate replies do not
share one generic floating-point formatter. The implementation and its local
comments in `src/redis/zset_command.cpp` preserve those distinctions.

RDB object compatibility is separate from native storage compatibility.
`DUMP`/`RESTORE` and external Redis replication exchange Redis logical encodings;
Lavik's native records, indexes, epochs, and replication history retain their
own formats and lifecycles. An accepted RDB version does not imply support for
every object kind or extension in that version. Current decoding and import
boundaries belong in the architecture documents and `src/redis/rdb.cpp`.

## Historical implementation scope

Earlier implementations used whole-record collection values, a narrower
CLIENT command surface, and automatic native/Redis replication probing.
Those descriptions are superseded. They are not constraints on the current
implementation: grouped collection storage, the Function catalog, and
Meta-managed native replication have their own documented boundaries.
