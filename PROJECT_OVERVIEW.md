# Celer Redis Overview

## Goal

`keylane` is a Redis/Valkey-protocol server built on top of the `celer` core runtime.

Supported commands follow the Redis 7.2 semantic baseline documented in
[`docs/redis-compatibility.md`](docs/redis-compatibility.md).

This repository should own:

- RESP parsing and serialization
- command dispatch
- shard-local in-memory database structures
- cross-worker request routing
- protocol-level connection/session state

This repository should not own:

- worker loop semantics
- TCP transport runtime
- generic I/O primitives

Those belong to `celer`.

## Intended First Milestone

A multi-worker, in-memory server with:

- RESP2 parsing
- one thread per core
- key-based routing to shards/workers
- no `MULTI/EXEC/WATCH` yet
- no replication yet
- no persistence yet

## Expected Layout

Repository layout:

- this repo contains a `celer/` git submodule
- initialize it with `git submodule update --init --recursive`

## Build

```bash
cmake -S . -B build
cmake --build build -j4
```

This repository currently uses `add_subdirectory(celer ...)` against the submodule checkout.

## Current Progress

The current integration status is:

- `celer` now owns worker thread creation, runtime start/stop/wait, and runtime completion notification
- `keylane` no longer owns the worker pool lifecycle directly
- `keylane` shutdown is now driven by a main-thread `eventfd` wakeup path rather than a dedicated signal-wait thread
- `keylane` TCP serving now goes through `celer::TcpServer`
- application code no longer calls `Worker::Spawn` directly
- RESP parsing, command execution, and reply encoding remain in `keylane`

Current application-facing shape in `keylane`:

- implement `TcpConnectionHandler::HandleRequests(TcpStream)`
- keep RESP/session logic inside that handler
- let `celer` own accept loops and internal session scheduling

## TODO

- keep RESP parsing, command dispatch, and database logic in `keylane`
- continue removing application-visible runtime details from `keylane`
- add shard-aware request routing on top of the current `celer::TcpServer` integration
