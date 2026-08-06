# Valkey-derived hash-table code

`include/keylane/storage/scan_hash_map.h` is a modified C++ specialization of
the hash-table design and scan algorithm in Valkey's `src/hashtable.c`.

- Upstream project: Valkey
- Upstream revision: `21c0d49a0` (`8.0.8-180-g21c0d49a0`)
- Upstream file: `src/hashtable.c`
- License: BSD-3-Clause; the complete unmodified text is in `COPYING`

The adaptation retains the 64-byte bucket layout, chained overflow buckets,
two-table incremental expansion, and stateless reverse-bit scan cursor. It
removes Valkey runtime allocation, random-number, time, configuration, and
generic callback dependencies; specializes entries for Keylane's digest, key,
and storage location; and adds full-key collision checks.
