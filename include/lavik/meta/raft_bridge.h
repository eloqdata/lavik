/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* All input bytes are copied before the call returns. Callback output uses
 * malloc; Go copies and frees it before returning to its executor. No Go
 * pointer or borrowed C++ buffer survives a call. The error buffer returned by
 * lavik_raft_open and the output buffer returned by lavik_raft_status are
 * malloc-allocated and transferred to the caller, which must free them.
 * The owner remains alive until close. */
typedef struct LavikRaftBytes {
  void* data;
  uint64_t size;
} LavikRaftBytes;

typedef struct LavikRaftCallbacks {
  int (*apply)(uintptr_t owner, uint64_t index, void* data, uint64_t size,
               LavikRaftBytes* result);
  int (*install)(uintptr_t owner, uint64_t index, void* data, uint64_t size);
  int (*capture)(uintptr_t owner, uint64_t index, LavikRaftBytes* result);
  void (*advance)(uintptr_t owner, uint64_t index);
  int (*identities)(uintptr_t owner, LavikRaftBytes* result);
  /* Must revoke authority without waiting for another callback or worker. */
  void (*role)(uintptr_t owner, uint64_t term, uint64_t leader, int is_leader,
               int caught_up, uint64_t resign_index);
  void (*result)(uintptr_t owner, uint64_t ticket, uint64_t index, int code,
                 void* data, uint64_t size);
  void (*fatal)(uintptr_t owner, void* data, uint64_t size);
} LavikRaftCallbacks;

uint64_t lavik_raft_open(void* config, uint64_t size, uintptr_t owner,
                         LavikRaftCallbacks* callbacks, LavikRaftBytes* error);
int lavik_raft_propose(uint64_t handle, uint64_t ticket, void* data,
                       uint64_t size);
int lavik_raft_snapshot(uint64_t handle, uint64_t ticket);
int lavik_raft_member(uint64_t handle, uint64_t ticket, void* data,
                      uint64_t size, int remove, int learner);
void lavik_raft_resign(uint64_t handle, uint64_t resign_index);
int lavik_raft_status(uint64_t handle, LavikRaftBytes* output);
void lavik_raft_close(uint64_t handle);

#ifdef __cplusplus
}
#endif
