# Celer Redis Overview

## Goal

`celer_redis` is a Redis/Valkey-protocol server built on top of the `celer` core runtime.

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
