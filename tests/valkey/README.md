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

# Valkey TCL compatibility tests

This directory vendors the first group of Valkey's TCL compatibility tests:
strings (including increment commands), lists, sets, sorted sets, hashes, and
streams, plus the generic `SORT` suite. The upstream snapshot is Valkey commit
`d2c8a4b91e8c0e6aefd1f5bc0bf582cddbe046b7`, which reports Redis compatibility
version 7.2.4; its BSD license is in `COPYING`.

The test cases retain the upstream assertions, with two portability adaptations:
the Hash ziplist fixtures compare named field/value pairs without depending on
`HGETALL` order, and the blocked-move WATCH fixture reads the EXEC result before
unblocking the move from another connection. The latter makes command ordering
explicit across Lavik's workers. The harness has Lavik-specific changes:
it limits the default suite list to the vendored data-structure files,
external-server cleanup tolerates unavailable housekeeping commands,
Valkey-only object-encoding configuration is emulated inside the harness, and
Valkey's internal `MEMORY USAGE` coverage probe returns a nonzero placeholder.
The runner uses one database, ignores server-internal object encoding, and
skips slow, replication, debug-only, and large-memory cases. RESP2 and RESP3
command semantics are both checked by the original assertions.

Build Lavik, then run all currently vendored suites:

```sh
tests/valkey/run-lavik
```

Run one suite or a named test:

```sh
tests/valkey/run-lavik --single unit/type/hash
tests/valkey/run-lavik --single unit/type/string --only 'SET and GET an item'
```

The server settings can be overridden with `LAVIK_BIN`, `LAVIK_HOST`,
`LAVIK_PORT`, `LAVIK_TEST_THREADS`, `LAVIK_TEST_DATA_SIZE`, and the other
`LAVIK_TEST_*` variables used by `run-lavik`. To test an already-running
server, invoke `tests/valkey/runtest` with `LAVIK_HOST` and `LAVIK_PORT`.
