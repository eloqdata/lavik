/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include "lavik/meta/raft_bridge.h"

static inline int call_identities(LavikRaftCallbacks* c, uintptr_t owner,
                                  LavikRaftBytes* result) {
  return c->identities(owner, result);
}

/* cgo cannot call a C function pointer directly. These bounded trampolines
 * carry only fixed-width values and C-owned memory across the ABI. */
static inline int call_apply(LavikRaftCallbacks* c, uintptr_t owner,
                             uint64_t index, void* data, uint64_t size,
                             LavikRaftBytes* out) {
  return c->apply(owner, index, data, size, out);
}
static inline int call_install(LavikRaftCallbacks* c, uintptr_t owner,
                               uint64_t index, void* data, uint64_t size) {
  return c->install(owner, index, data, size);
}
static inline int call_capture(LavikRaftCallbacks* c, uintptr_t owner,
                               uint64_t index, LavikRaftBytes* out) {
  return c->capture(owner, index, out);
}
static inline void call_advance(LavikRaftCallbacks* c, uintptr_t owner,
                                uint64_t index) {
  c->advance(owner, index);
}
static inline void call_role(LavikRaftCallbacks* c, uintptr_t owner,
                             uint64_t term, uint64_t leader, int is_leader,
                             int caught_up, uint64_t resign_index) {
  c->role(owner, term, leader, is_leader, caught_up, resign_index);
}
static inline void call_result(LavikRaftCallbacks* c, uintptr_t owner,
                               uint64_t ticket, uint64_t index, int code,
                               void* data, uint64_t size) {
  c->result(owner, ticket, index, code, data, size);
}
static inline void call_fatal(LavikRaftCallbacks* c, uintptr_t owner,
                              void* data, uint64_t size) {
  c->fatal(owner, data, size);
}
