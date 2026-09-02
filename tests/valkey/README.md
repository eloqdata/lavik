# Valkey TCL compatibility tests

This directory vendors the first group of Valkey's TCL compatibility tests:
strings (including increment commands), lists, sets, sorted sets, hashes, and
streams, plus the generic `SORT` suite. The upstream snapshot is Valkey commit
`d2c8a4b91e8c0e6aefd1f5bc0bf582cddbe046b7`, which reports Redis compatibility
version 7.2.4; its BSD license is in `COPYING`.

The test cases are copied unchanged. The harness has Keylane-specific changes:
it limits the default suite list to the vendored data-structure files,
external-server cleanup tolerates unavailable housekeeping commands,
Valkey-only object-encoding configuration is emulated inside the harness, and
Valkey's internal `MEMORY USAGE` coverage probe returns a nonzero placeholder.
The runner uses one database, ignores server-internal object encoding, and
skips slow, replication, debug-only, and large-memory cases. RESP2 and RESP3
command semantics are both checked by the original assertions.

Build Keylane, then run all currently vendored suites:

```sh
tests/valkey/run-keylane
```

Run one suite or a named test:

```sh
tests/valkey/run-keylane --single unit/type/hash
tests/valkey/run-keylane --single unit/type/string --only 'SET and GET an item'
```

The server settings can be overridden with `KEYLANE_BIN`, `KEYLANE_HOST`,
`KEYLANE_PORT`, `KEYLANE_TEST_THREADS`, `KEYLANE_TEST_DATA_SIZE`, and the other
`KEYLANE_TEST_*` variables used by `run-keylane`. To test an already-running
server, invoke `tests/valkey/runtest` with `KEYLANE_HOST` and `KEYLANE_PORT`.
