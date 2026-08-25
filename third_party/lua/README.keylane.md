# Vendored Lua

This directory is copied from Valkey 7.2.4 commit
`d2c8a4b91e8c0e6aefd1f5bc0bf582cddbe046b7`:

- `deps/lua/src/*.c` and `deps/lua/src/*.h`
- `deps/lua/COPYRIGHT`
- `src/solarisfixes.h`

Keylane builds the Lua 5.1 core plus Valkey's cjson, cmsgpack, struct, and bit
extensions. Keylane carries two local source adjustments:

- `lua_cjson.c` includes the vendored `solarisfixes.h` beside it instead of
  using Valkey's repository-relative path.
- `lua_loadbytecode`/`luaL_loadbytecode` provide a protected, server-internal
  loader for chunks produced by `lua_dump`. The normal Lua source loader stays
  text-only, preserving Valkey's protection against client-supplied bytecode.

To update Lua, replace these files as one unit from the chosen Valkey release,
retain both local adjustments, and run the EVAL/EVALSHA, SCRIPT, and
replication tests.
