# Copyright (C) 2026 EloqData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)

project(lavik VERSION 0.1.0 LANGUAGES C CXX)

set(LAVIK_VERSION_SUFFIX "-dev" CACHE STRING
    "Lavik prerelease suffix, such as -dev or -beta.1 (empty for a release)")
set(LAVIK_VERSION "${PROJECT_VERSION}${LAVIK_VERSION_SUFFIX}")
set(LAVIK_GENERATED_INCLUDE_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${LAVIK_GENERATED_INCLUDE_DIR}/lavik")
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/version.h.in"
  "${LAVIK_GENERATED_INCLUDE_DIR}/lavik/version.h"
  @ONLY
)

option(BUILD_TESTING "Build lavik tests" OFF)
include(CTest)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(LAVIK_ENABLE_WERROR "Treat warnings as errors" OFF)
option(LAVIK_ENABLE_OPT "Enable aggressive optimization" ON)
option(LAVIK_STATIC_OPENSSL "Link OpenSSL statically" ON)
option(LAVIK_STATIC_CXX_RUNTIME
  "Link the GNU C++ and compiler runtimes statically" ON)
set(LAVIK_MARCH "native" CACHE STRING
  "Value passed to -march for optimized local builds (empty disables it)")
option(LAVIK_ENABLE_READ_LATENCY_TRACE "Enable detailed GET latency tracing" OFF)
option(LAVIK_ENABLE_SET_LATENCY_TRACE "Enable detailed SET latency tracing" OFF)
option(LAVIK_ENABLE_CROSS_CORE_HOP_COUNT "Count cross-core SubmitTaskTo transfers (test builds only)" OFF)
option(LAVIK_KERNEL_BYPASS
  "Include DPDK networking and SPDK storage (selected independently at startup)"
  OFF)
option(LAVIK_BUILD_FAULT_SERVER
  "Build the non-packageable server variant used by deterministic fault tests"
  OFF)

if (LAVIK_BUILD_FAULT_SERVER AND NOT BUILD_TESTING)
  message(FATAL_ERROR
    "LAVIK_BUILD_FAULT_SERVER requires BUILD_TESTING=ON")
endif()

if (LAVIK_ENABLE_OPT)
  include(CheckIPOSupported)
  check_ipo_supported(
    RESULT LAVIK_IPO_SUPPORTED
    OUTPUT LAVIK_IPO_ERROR
    LANGUAGES C CXX
  )
  if (LAVIK_IPO_SUPPORTED)
    # IPO properties are initialized when a target is created. Keep these
    # defaults ahead of every bundled target so Bycorf, Abseil, mimalloc, and
    # Lua participate in the same link-time optimization as the server.
    # Debug deliberately stays non-IPO so local builds do not pay an
    # optimization cost that is unrelated to correctness.
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO TRUE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_MINSIZEREL TRUE)
  else()
    message(WARNING
      "IPO is unavailable; continuing without LTO: ${LAVIK_IPO_ERROR}")
  endif()
endif()

set(LAVIK_ASAN_BUILD OFF)
string(CONCAT LAVIK_COMPILER_AND_LINKER_FLAGS
  "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} ${CMAKE_EXE_LINKER_FLAGS} "
  "${CMAKE_C_FLAGS_DEBUG} ${CMAKE_CXX_FLAGS_DEBUG} "
  "${CMAKE_C_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_RELEASE} "
  "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO} "
  "${CMAKE_C_FLAGS_MINSIZEREL} ${CMAKE_CXX_FLAGS_MINSIZEREL}")
if (LAVIK_COMPILER_AND_LINKER_FLAGS MATCHES "-fsanitize=[^ ]*address")
  set(LAVIK_ASAN_BUILD ON)
endif()
if (LAVIK_ASAN_BUILD AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR
    "AddressSanitizer builds require Clang. GCC does not preserve coroutine "
    "symmetric transfers under ASan and can overflow the native stack.")
endif()
set(LAVIK_CLANG_FORMAT_REQUIRED_VERSION "23.1.0")
string(REPLACE "." "\\." LAVIK_CLANG_FORMAT_VERSION_REGEX
  "${LAVIK_CLANG_FORMAT_REQUIRED_VERSION}")
find_program(LAVIK_CLANG_FORMAT NAMES clang-format-23 clang-format)
if (LAVIK_CLANG_FORMAT)
  execute_process(
    COMMAND ${LAVIK_CLANG_FORMAT} --version
    RESULT_VARIABLE LAVIK_CLANG_FORMAT_VERSION_RESULT
    OUTPUT_VARIABLE LAVIK_CLANG_FORMAT_VERSION_OUTPUT
    ERROR_VARIABLE LAVIK_CLANG_FORMAT_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE
  )
  # Formatting is repository-wide, so silently accepting another formatter
  # version would create unrelated churn in otherwise valid changes.
  if (NOT LAVIK_CLANG_FORMAT_VERSION_RESULT EQUAL 0 OR
      NOT LAVIK_CLANG_FORMAT_VERSION_OUTPUT MATCHES
        "clang-format version ${LAVIK_CLANG_FORMAT_VERSION_REGEX}([^0-9.]|$)")
    unset(LAVIK_CLANG_FORMAT CACHE)
    set(LAVIK_CLANG_FORMAT "")
  endif()
endif()
file(GLOB_RECURSE LAVIK_FORMAT_FILES CONFIGURE_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/app/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/lavik/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp
)
# CLI11.hpp is an upstream-generated single header; rewriting it makes local
# formatting depend on the generated artifact instead of its upstream source.
list(FILTER LAVIK_FORMAT_FILES EXCLUDE REGEX "/lavik/CLI11\\.hpp$")
if (LAVIK_CLANG_FORMAT)
  add_custom_target(format
    COMMAND ${LAVIK_CLANG_FORMAT} -i ${LAVIK_FORMAT_FILES}
    COMMENT "Formatting Lavik sources"
    VERBATIM
  )
  add_custom_target(format-check
    COMMAND ${LAVIK_CLANG_FORMAT} --dry-run --Werror ${LAVIK_FORMAT_FILES}
    COMMENT "Checking Lavik source formatting"
    VERBATIM
  )
else()
  message(STATUS
    "clang-format ${LAVIK_CLANG_FORMAT_REQUIRED_VERSION} not found; "
    "format targets are disabled")
endif()

set(BYCORF_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
# Keep the runtime dependency boundary aligned with Lavik, including when
# bypass is turned off again in an existing build directory.
set(BYCORF_KERNEL_BYPASS ${LAVIK_KERNEL_BYPASS} CACHE BOOL "" FORCE)
# This must be set before Bycorf's first find_package(OpenSSL). Static release
# artifacts can then run without libssl.so or libcrypto.so on the target.
set(OPENSSL_USE_STATIC_LIBS ${LAVIK_STATIC_OPENSSL} CACHE BOOL
  "Use static OpenSSL libraries" FORCE)
foreach(open_ssl_cache_var IN ITEMS OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY)
  if (LAVIK_STATIC_OPENSSL AND DEFINED ${open_ssl_cache_var} AND
      NOT "${${open_ssl_cache_var}}" MATCHES "\\${CMAKE_STATIC_LIBRARY_SUFFIX}$")
    unset(${open_ssl_cache_var} CACHE)
  elseif (NOT LAVIK_STATIC_OPENSSL AND DEFINED ${open_ssl_cache_var} AND
          "${${open_ssl_cache_var}}" MATCHES "\\${CMAKE_STATIC_LIBRARY_SUFFIX}$")
    unset(${open_ssl_cache_var} CACHE)
  endif()
endforeach()
find_package(OpenSSL REQUIRED)

set(LAVIK_BYCORF_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/bycorf" CACHE PATH
    "Bycorf checkout to build (allows an isolated runtime worktree)")
if (EXISTS "${LAVIK_BYCORF_SOURCE_DIR}/CMakeLists.txt")
  add_subdirectory("${LAVIK_BYCORF_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/bycorf")
else()
  message(FATAL_ERROR "bycorf submodule not found; run git submodule update --init --recursive")
endif()
target_compile_definitions(bycorf PUBLIC
  BYCORF_ENABLE_SUBMIT_TASK_COUNT=$<BOOL:${LAVIK_ENABLE_CROSS_CORE_HOP_COUNT}>
)

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/mimalloc/CMakeLists.txt)
  message(FATAL_ERROR
    "mimalloc submodule not found; run git submodule update --init --recursive")
endif()
set(MI_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(MI_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(MI_BUILD_OBJECT OFF CACHE BOOL "" FORCE)
set(MI_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(MI_OVERRIDE OFF CACHE BOOL "" FORCE)
set(MI_GUARDED OFF CACHE BOOL "" FORCE)
set(MI_NO_THP ON CACHE BOOL "" FORCE)
# Apply Lavik's explicit CPU target below. This prevents mimalloc from
# silently raising packaged arm64 builds to armv8.1-a or x86_64 to Haswell.
set(MI_NO_OPT_ARCH ON CACHE BOOL "" FORCE)
set(MI_TRACK_ASAN ${LAVIK_ASAN_BUILD} CACHE BOOL "" FORCE)
set(MI_EXTRA_CPPDEFS "MI_DEFAULT_ARENA_EAGER_COMMIT=1" CACHE STRING "" FORCE)
add_subdirectory(third_party/mimalloc)

option(LAVIK_BUILD_META
  "Build the lavik-meta Raft control plane (requires the recursive nuraft submodule)"
  ON)
if (LAVIK_BUILD_META)
  # Shared operator transport stays below lavik_meta_core so the CLI and
  # operator library have no NuRaft dependency.
  add_library(lavik_meta_admin STATIC
    src/meta/admin_client.cpp
    src/meta/cluster_create.cpp
    src/meta/cluster_status.cpp
    src/meta/failover_admin.cpp
  )
  target_include_directories(lavik_meta_admin PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(lavik_meta_admin PUBLIC
    absl::status
    absl::statusor
    OpenSSL::SSL
    OpenSSL::Crypto
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lavik_meta_admin PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nuraft/CMakeLists.txt)
    message(FATAL_ERROR
      "nuraft submodule not found; run git submodule update --init --recursive")
  endif()
  if (NOT EXISTS
      ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nuraft/asio/asio/include/asio.hpp)
    message(FATAL_ERROR
      "nuraft/asio submodule not found; run git submodule update --init --recursive")
  endif()
  # Apply the pinned Lavik patch set to an isolated build-tree copy. The
  # third-party checkout stays pristine, configure is repeatable, and a patch
  # that no longer applies fails before compilation instead of drifting.
  set(LAVIK_NURAFT_SOURCE_DIR
      ${CMAKE_CURRENT_BINARY_DIR}/_lavik_nuraft_source)
  file(REMOVE_RECURSE ${LAVIK_NURAFT_SOURCE_DIR})
  file(MAKE_DIRECTORY ${LAVIK_NURAFT_SOURCE_DIR})
  file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nuraft/
       DESTINATION ${LAVIK_NURAFT_SOURCE_DIR}
       PATTERN ".git" EXCLUDE)
  find_program(LAVIK_GIT_EXECUTABLE git REQUIRED)
  file(GLOB LAVIK_NURAFT_PATCHES CONFIGURE_DEPENDS
       ${CMAKE_CURRENT_SOURCE_DIR}/third_party/patches/nuraft/*.patch)
  list(SORT LAVIK_NURAFT_PATCHES)
  foreach(LAVIK_NURAFT_PATCH IN LISTS LAVIK_NURAFT_PATCHES)
    execute_process(
      # The isolated copy may live underneath Lavik's own worktree. Stop
      # repository discovery at the build root so git applies paths relative
      # to WORKING_DIRECTORY instead of silently skipping every path outside
      # the generated directory's prefix in the parent repository.
      COMMAND ${CMAKE_COMMAND} -E env
              GIT_CEILING_DIRECTORIES=${CMAKE_CURRENT_BINARY_DIR}
              ${LAVIK_GIT_EXECUTABLE} apply --check ${LAVIK_NURAFT_PATCH}
      WORKING_DIRECTORY ${LAVIK_NURAFT_SOURCE_DIR}
      RESULT_VARIABLE LAVIK_NURAFT_PATCH_CHECK)
    if (NOT LAVIK_NURAFT_PATCH_CHECK EQUAL 0)
      message(FATAL_ERROR "NuRaft patch no longer applies: ${LAVIK_NURAFT_PATCH}")
    endif()
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E env
              GIT_CEILING_DIRECTORIES=${CMAKE_CURRENT_BINARY_DIR}
              ${LAVIK_GIT_EXECUTABLE} apply ${LAVIK_NURAFT_PATCH}
      WORKING_DIRECTORY ${LAVIK_NURAFT_SOURCE_DIR}
      COMMAND_ERROR_IS_FATAL ANY)
  endforeach()
  unset(LAVIK_NURAFT_PATCH)
  unset(LAVIK_NURAFT_PATCHES)
  set(DISABLE_ASIO OFF)
  set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(ASIO_INCLUDE_DIR
      ${LAVIK_NURAFT_SOURCE_DIR}/asio/asio/include CACHE PATH "" FORCE)
  # NuRaft's tests subdirectory builds its asio-based harness whenever
  # BUILD_TESTING is on. Shadow lavik's global test enablement for this
  # subdirectory only, then restore it for lavik's own test targets.
  set(LAVIK_SAVED_BUILD_TESTING ${BUILD_TESTING})
  set(BUILD_TESTING OFF)
  add_subdirectory(${LAVIK_NURAFT_SOURCE_DIR}
                   ${CMAKE_CURRENT_BINARY_DIR}/_lavik_nuraft_build)
  set(BUILD_TESTING ${LAVIK_SAVED_BUILD_TESTING})
  unset(LAVIK_SAVED_BUILD_TESTING)
  # NuRaft's headers are not warning-clean under -Wpedantic/-Wunused-parameter;
  # consumers compile with -Wall -Wextra -Wpedantic, so treat them as SYSTEM.
  get_target_property(LAVIK_NURAFT_IFACE_INCLUDES static_lib
    INTERFACE_INCLUDE_DIRECTORIES)
  if (LAVIK_NURAFT_IFACE_INCLUDES)
    set_target_properties(static_lib PROPERTIES
      INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${LAVIK_NURAFT_IFACE_INCLUDES}")
  endif()
  unset(LAVIK_NURAFT_IFACE_INCLUDES)
endif()

set(LAVIK_LUA_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/lua/src")
add_library(lavik_lua STATIC
  ${LAVIK_LUA_SOURCE_DIR}/lapi.c
  ${LAVIK_LUA_SOURCE_DIR}/lauxlib.c
  ${LAVIK_LUA_SOURCE_DIR}/lbaselib.c
  ${LAVIK_LUA_SOURCE_DIR}/lcode.c
  ${LAVIK_LUA_SOURCE_DIR}/ldebug.c
  ${LAVIK_LUA_SOURCE_DIR}/ldo.c
  ${LAVIK_LUA_SOURCE_DIR}/ldump.c
  ${LAVIK_LUA_SOURCE_DIR}/lfunc.c
  ${LAVIK_LUA_SOURCE_DIR}/lgc.c
  ${LAVIK_LUA_SOURCE_DIR}/llex.c
  ${LAVIK_LUA_SOURCE_DIR}/lmathlib.c
  ${LAVIK_LUA_SOURCE_DIR}/lmem.c
  ${LAVIK_LUA_SOURCE_DIR}/lobject.c
  ${LAVIK_LUA_SOURCE_DIR}/lopcodes.c
  ${LAVIK_LUA_SOURCE_DIR}/lparser.c
  ${LAVIK_LUA_SOURCE_DIR}/lstate.c
  ${LAVIK_LUA_SOURCE_DIR}/lstring.c
  ${LAVIK_LUA_SOURCE_DIR}/lstrlib.c
  ${LAVIK_LUA_SOURCE_DIR}/ltable.c
  ${LAVIK_LUA_SOURCE_DIR}/ltablib.c
  ${LAVIK_LUA_SOURCE_DIR}/ltm.c
  ${LAVIK_LUA_SOURCE_DIR}/lundump.c
  ${LAVIK_LUA_SOURCE_DIR}/lvm.c
  ${LAVIK_LUA_SOURCE_DIR}/lzio.c
  ${LAVIK_LUA_SOURCE_DIR}/strbuf.c
  ${LAVIK_LUA_SOURCE_DIR}/fpconv.c
  ${LAVIK_LUA_SOURCE_DIR}/lua_cjson.c
  ${LAVIK_LUA_SOURCE_DIR}/lua_struct.c
  ${LAVIK_LUA_SOURCE_DIR}/lua_cmsgpack.c
  ${LAVIK_LUA_SOURCE_DIR}/lua_bit.c
)
target_include_directories(lavik_lua PUBLIC ${LAVIK_LUA_SOURCE_DIR})
target_compile_definitions(lavik_lua PRIVATE
  LUA_USE_POSIX
  # luaopen_cjson returns its module table but only publishes the Redis/Valkey
  # global when this compatibility define is enabled.
  ENABLE_CJSON_GLOBAL
)
target_link_libraries(lavik_lua PUBLIC m)

if (TARGET format AND TARGET bycorf-format)
  add_dependencies(format bycorf-format)
  add_dependencies(format-check bycorf-format-check)
endif()

# The generic Abseil CRC loop calls small load helpers for every input word.
# Debug -O0 plus AddressSanitizer gives each helper a fake stack frame, making
# payload checks several orders of magnitude slower while retaining the same
# sanitizer coverage. Optimize only the CRC implementation in Debug builds.
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  foreach(crc_target IN ITEMS absl_crc_internal absl_crc32c)
    if (TARGET ${crc_target})
      target_compile_options(${crc_target} PRIVATE $<$<CONFIG:Debug>:-O1>)
    endif()
  endforeach()
endif()

add_library(lavik_lease_clock STATIC
  src/cluster/lease_clock.cpp
)
add_library(lavik::lease_clock ALIAS lavik_lease_clock)
target_include_directories(lavik_lease_clock PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(lavik_lease_clock PUBLIC cxx_std_23)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lavik_lease_clock PRIVATE
    -Wall -Wextra -Wpedantic)
endif()

add_library(lavik_control_protocol STATIC
  src/cluster/control_protocol.cpp
)
add_library(lavik::control_protocol ALIAS lavik_control_protocol)
target_include_directories(lavik_control_protocol PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(lavik_control_protocol PUBLIC cxx_std_23)
target_link_libraries(lavik_control_protocol PUBLIC
  absl::status
  absl::statusor
)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lavik_control_protocol PRIVATE
    -Wall -Wextra -Wpedantic)
endif()

add_library(lavik_control_transport STATIC
  src/cluster/control_transport.cpp
)
add_library(lavik::control_transport ALIAS lavik_control_transport)
target_include_directories(lavik_control_transport PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(lavik_control_transport PUBLIC cxx_std_23)
target_link_libraries(lavik_control_transport PUBLIC
  lavik::control_protocol
  bycorf::core
  absl::status
  absl::statusor
)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lavik_control_transport PRIVATE
    -Wall -Wextra -Wpedantic)
endif()

add_library(lavik_module
  src/cluster/authority.cpp
  src/cluster/meta_client.cpp
  src/cluster/meta_control.cpp
  src/cluster/node_control.cpp
  src/cluster/runtime.cpp
  src/cluster/topology.cpp
  src/config.cpp
  src/logging.cpp
  src/storage/buffer_pool.cpp
  src/storage/engine.cpp
  src/storage/engine/alloc.cpp
  src/storage/engine/backup.cpp
  src/storage/engine/checkpoint.cpp
  src/storage/engine/compact_api.cpp
  src/storage/engine/defrag.cpp
  src/storage/engine/expire.cpp
  src/storage/engine/flush.cpp
  src/storage/engine/flush_db.cpp
  src/storage/engine/hash_api.cpp
  src/storage/engine/hash_codec.cpp
  src/storage/engine/grouped_hash.cpp
  src/storage/engine/grouped_collection.cpp
  src/storage/engine/grouped_sorted_rewrite.cpp
  src/storage/engine/ordered_compact_codec.cpp
  src/storage/engine/collection_compact_stream.cpp
  src/storage/engine/grouped_replication_source.cpp
  src/storage/engine/grouped_write.cpp
  src/storage/engine/grouped_read.cpp
  src/storage/engine/grouped_ordered_io.cpp
  src/storage/engine/grouped_list.cpp
  src/storage/engine/grouped_zset.cpp
  src/storage/engine/grouped_zset_members.cpp
  src/storage/engine/grouped_hash_random.cpp
  src/storage/engine/sorted_set_api.cpp
  src/storage/engine/transfer_api.cpp
  src/storage/engine/grouped_restore.cpp
  src/storage/engine/collection_ingest.cpp
  src/storage/engine/grouped_mutation.cpp
  src/storage/engine/grouped_ordered_mutation.cpp
  src/storage/engine/grouped_metadata.cpp
  src/storage/engine/replica_collection.cpp
  src/storage/engine/grouped_lifecycle.cpp
  src/storage/engine/grouped_object_index.cpp
  src/storage/engine/hash_tree.cpp
  src/storage/engine/init.cpp
  src/storage/engine/list_api.cpp
  src/storage/engine/list_tree.cpp
  src/storage/engine/metrics.cpp
  src/storage/engine/read.cpp
  src/storage/engine/recovery.cpp
  src/storage/engine/replication.cpp
  src/storage/engine/replication_log.cpp
  src/storage/engine/system_state.cpp
  src/storage/engine/tomb_raider.cpp
  src/storage/engine/tx_cleaner.cpp
  src/storage/engine/write.cpp
  src/storage/format.cpp
  src/memory.cpp
  src/metrics.cpp
  src/replication/command.cpp
  src/replication/replica_applied_frontier.cpp
  src/replication/replication_group.cpp
  src/replication/replication.cpp
  src/redis/resp.cpp
  src/redis/rdb.cpp
  src/redis/rdb_collection.cpp
  src/redis/rdb_import.cpp
  src/redis/backup.cpp
  src/redis/blocking_wait.cpp
  src/redis/cluster_command.cpp
  src/redis/command.cpp
  src/redis/function_catalog.cpp
  src/redis/hash_command.cpp
  src/redis/list_command.cpp
  src/redis/lua_eval.cpp
  src/redis/monitor.cpp
  src/redis/pubsub.cpp
  src/redis/set_command.cpp
  src/redis/slowlog.cpp
  src/redis/sort_command.cpp
  src/redis/string_command.cpp
  src/redis/stream_command.cpp
  src/redis/zset_command.cpp
  src/redis/command_table.cpp
  src/redis/server.cpp
  src/tx/tx_shard.cpp
  src/tx/transaction.cpp
)

add_library(lavik::module ALIAS lavik_module)

target_include_directories(lavik_module
  PUBLIC
    ${LAVIK_GENERATED_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_precompile_headers(lavik_module PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include/lavik/pch.h
)

target_compile_features(lavik_module PUBLIC cxx_std_23)
target_compile_definitions(lavik_module PUBLIC
  $<$<BOOL:${LAVIK_KERNEL_BYPASS}>:LAVIK_KERNEL_BYPASS=1>
  LAVIK_ASAN_BUILD=$<BOOL:${LAVIK_ASAN_BUILD}>
  LAVIK_ENABLE_READ_LATENCY_TRACE=$<BOOL:${LAVIK_ENABLE_READ_LATENCY_TRACE}>
  LAVIK_ENABLE_SET_LATENCY_TRACE=$<BOOL:${LAVIK_ENABLE_SET_LATENCY_TRACE}>
  LAVIK_ENABLE_CROSS_CORE_HOP_COUNT=$<BOOL:${LAVIK_ENABLE_CROSS_CORE_HOP_COUNT}>
  LAVIK_ENABLE_TEST_FAULTS=$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>
)
target_link_libraries(lavik_module PUBLIC
  lavik::lease_clock
  lavik::control_transport
  lavik::control_protocol
  bycorf::core
  bycorf::rpc
  absl::crc32c
  absl::flat_hash_map
  absl::inlined_vector
  absl::status
  absl::statusor
  absl::strings
  mimalloc-static
  lavik_lua
  OpenSSL::Crypto
)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(lavik_module PRIVATE -Wall -Wextra -Wpedantic)
  if (LAVIK_ENABLE_WERROR)
    target_compile_options(lavik_module PRIVATE -Werror)
  endif()
endif()

add_executable(lavik
  app/lavik.cpp
  app/mimalloc_new_delete.cpp
)
target_precompile_headers(lavik REUSE_FROM lavik_module)
target_link_libraries(lavik PRIVATE lavik::module)

if (LAVIK_BUILD_FAULT_SERVER)
  # This binary intentionally shares the test-instrumented module with the
  # ordinary test server in this build tree. The option is rejected unless
  # BUILD_TESTING is enabled, so packaging cannot accidentally ship it.
  add_executable(lavik_fault_server
    app/lavik.cpp
    app/mimalloc_new_delete.cpp
  )
  target_precompile_headers(lavik_fault_server REUSE_FROM lavik_module)
  target_link_libraries(lavik_fault_server PRIVATE lavik::module)
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  if (LAVIK_ENABLE_OPT)
    set(LAVIK_OPT_COMPILE_OPTIONS -O3)
    if (NOT LAVIK_MARCH STREQUAL "")
      list(APPEND LAVIK_OPT_COMPILE_OPTIONS "-march=${LAVIK_MARCH}")
    endif()
    # Bycorf is compiled as part of Lavik, so local native builds should tune
    # the transport and runtime too, not only the Redis/storage layer. Abseil's
    # accelerated CRC engines are compile-gated by architecture feature
    # macros; apply the explicit target to those translation units as well or
    # a native Lavik build silently uses the generic table-based checksum.
    set(LAVIK_OPT_TARGETS
        absl_crc_internal
        absl_crc32c
        bycorf
        bycorf_rpc
        lavik_control_protocol
        lavik_control_transport
        lavik_lease_clock
        lavik_module
        lavik)
    if (TARGET lavik_fault_server)
      list(APPEND LAVIK_OPT_TARGETS lavik_fault_server)
    endif()
    foreach(target IN LISTS LAVIK_OPT_TARGETS)
      target_compile_options(${target} PRIVATE ${LAVIK_OPT_COMPILE_OPTIONS})
    endforeach()
    if (NOT LAVIK_MARCH STREQUAL "")
      target_compile_options(mimalloc-static PRIVATE "-march=${LAVIK_MARCH}")
    endif()
  endif()
  if (LAVIK_STATIC_CXX_RUNTIME)
    target_link_options(lavik PRIVATE -static-libstdc++ -static-libgcc)
    if (TARGET lavik_fault_server)
      target_link_options(lavik_fault_server PRIVATE
        -static-libstdc++ -static-libgcc)
    endif()
  endif()
endif()

if (UNIX)
  add_custom_target(valkey-tcl-tests
    COMMAND ${CMAKE_COMMAND} -E env
            LAVIK_BIN=$<TARGET_FILE:lavik>
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/valkey/run-lavik
    DEPENDS lavik
    USES_TERMINAL
    COMMENT "Running vendored Valkey TCL data-structure tests"
  )
endif()

if (LAVIK_BUILD_META)
  # Architecture layering: Raft is a meta-plane concern. NuRaft is linked
  # only into lavik-meta; lavik, lavik_module, and the test binaries
  # must stay Raft-free so the data plane never sees consensus code.
  #
  # lavik_meta_core carries durability and control logic; Bycorf owns
  # foreign-thread completion ingress while NuRaft's native Asio service owns
  # peer transport and timers.
  set(LAVIK_META_CORE_SOURCES
    src/meta/audit_store.cpp
    src/meta/automatic_failover_detector.cpp
    src/meta/automatic_failover_reconciler.cpp
    src/meta/candidate_plan.cpp
    src/meta/cluster_create_reconciler.cpp
    src/meta/commands.cpp
    src/meta/control_projector.cpp
    src/meta/coordinator.cpp
    src/meta/ctl_server.cpp
    src/meta/data_control_server.cpp
    src/meta/data_control_runtime_status.cpp
    src/meta/encoding.cpp
    src/meta/failover.cpp
    src/meta/failover_reconciler.cpp
    src/meta/identity_verifier.cpp
    src/meta/identity_store.cpp
    src/meta/membership_reconciler.cpp
    src/meta/observation_store.cpp
    src/meta/owner_serviceability.cpp
    src/meta/operation_store.cpp
    src/meta/policy_store.cpp
    src/meta/population_manifest_store.cpp
    src/meta/proposal_executor.cpp
    src/meta/state_apply.cpp
    src/meta/state_machine.cpp
    src/meta/topology_store.cpp
    src/meta/value_codec.cpp
    src/meta/nuraft_asio_transport.cpp
    src/meta/nuraft_log_store.cpp
    src/meta/nuraft_state_mgr.cpp
  )
  add_library(lavik_meta_core STATIC ${LAVIK_META_CORE_SOURCES})
  target_include_directories(lavik_meta_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(lavik_meta_core PUBLIC
    OpenSSL::Crypto
    lavik_meta_admin
    lavik::lease_clock
    lavik::control_transport
    lavik::control_protocol
    NuRaft::static_lib
    bycorf::core
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lavik_meta_core PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  add_executable(lavik-meta
    app/lavik_meta.cpp
  )
  target_include_directories(lavik-meta PRIVATE
    ${LAVIK_GENERATED_INCLUDE_DIR}
  )
  target_compile_definitions(lavik-meta PRIVATE
    LAVIK_NURAFT_PINNED_COMMIT=\"0b01b185b46b8e482f6c384b636c35605121a41e\"
  )
  target_link_libraries(lavik-meta PRIVATE
    lavik_meta_core
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lavik-meta PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  # Direct administration and cluster readiness share one Raft-free CLI over
  # the Admin transport/operator library, without linking lavik_meta_core.
  add_executable(lavik-ctl
    app/lavik_ctl.cpp
  )
  target_include_directories(lavik-ctl PRIVATE
    ${LAVIK_GENERATED_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(lavik-ctl PRIVATE
    lavik_meta_admin
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lavik-ctl PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  # Negative guard for the layering rule above: fail at configure time if
  # nuraft ever leaks into the data-plane module's link interface.
  get_target_property(LAVIK_MODULE_IFACE_LIBS lavik_module
    INTERFACE_LINK_LIBRARIES)
  if (LAVIK_MODULE_IFACE_LIBS MATCHES "nuraft|NuRaft")
    message(FATAL_ERROR
      "lavik_module must not link NuRaft; Raft is meta-plane only")
  endif()
endif()

if (BUILD_TESTING)
  # Test-only executables do not ship, and their intentionally oversized
  # coroutine stress cases have triggered GCC IPO compiler ICEs. Preserve LTO
  # on the already-created production graph without adding IPO-specific risk
  # to those test sources.
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE FALSE)
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO FALSE)
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_MINSIZEREL FALSE)

  if (LAVIK_ENABLE_OPT AND LAVIK_IPO_SUPPORTED AND
      CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # Test sources stay ordinary objects, but their production dependencies
    # contain LLVM bitcode. Enable the linker plugin when consuming those
    # archives; otherwise Clang with GNU ld cannot read the production graph.
    add_link_options("$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-flto=thin>")
  endif()

  include(FetchContent)

  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_test(NAME lavik_runtime_backends
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/runtime_backends_smoke.py $<TARGET_FILE:lavik>)
  set_tests_properties(lavik_runtime_backends PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE)
  add_test(NAME lavik_version COMMAND lavik --version)
  set_tests_properties(lavik_version PROPERTIES
    PASS_REGULAR_EXPRESSION "lavik ${LAVIK_VERSION}")
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG 52eb8108c5bdec04579160ae17225d66034bd723 # v1.17.0
  )
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  include(GoogleTest)

  if (UNIX)
    # These policies must never share module/PCH inline definitions: each
    # executable compiles the public fault header independently of the server
    # configuration, including an actually erased ordinary Release policy.
    foreach(fault_mode IN ITEMS debug release release_fault)
      set(fault_target lavik_fault_injection_${fault_mode}_test)
      add_executable(${fault_target} tests/fault_injection_test.cpp)
      target_link_libraries(${fault_target} PRIVATE GTest::gtest_main)
      target_include_directories(${fault_target} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include)
      set_target_properties(${fault_target} PROPERTIES
        DISABLE_PRECOMPILE_HEADERS ON)
      if (fault_mode STREQUAL "debug")
        target_compile_options(${fault_target} PRIVATE -UNDEBUG)
        target_compile_definitions(${fault_target} PRIVATE
          LAVIK_ENABLE_TEST_FAULTS=0 LAVIK_FAULT_TEST_EXPECTED_ENABLED=1)
      elseif (fault_mode STREQUAL "release")
        target_compile_definitions(${fault_target} PRIVATE
          NDEBUG LAVIK_ENABLE_TEST_FAULTS=0
          LAVIK_FAULT_TEST_EXPECTED_ENABLED=0)
      else()
        target_compile_definitions(${fault_target} PRIVATE
          NDEBUG LAVIK_ENABLE_TEST_FAULTS=1
          LAVIK_FAULT_TEST_EXPECTED_ENABLED=1)
      endif()
      add_test(NAME lavik_fault_injection_${fault_mode}
        COMMAND ${fault_target})
      set_tests_properties(lavik_fault_injection_${fault_mode} PROPERTIES
        LABELS "unit;fault-policy" TIMEOUT 15)
    endforeach()
  endif()

  add_library(lavik_test_support STATIC
    tests/support/process.cpp
  )
  target_include_directories(lavik_test_support PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_compile_features(lavik_test_support PUBLIC cxx_std_23)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lavik_test_support PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  add_library(lavik_cluster_fault_harness STATIC
    tests/cluster/cluster_invariants.cpp
    tests/cluster/fault_controls.cpp
    tests/cluster/fault_harness.cpp
    tests/cluster/fault_trace.cpp
    tests/cluster/reference_model.cpp
  )
  target_include_directories(lavik_cluster_fault_harness PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_compile_features(lavik_cluster_fault_harness PUBLIC cxx_std_23)
  target_link_libraries(lavik_cluster_fault_harness PUBLIC
    absl::status
    absl::statusor
    absl::strings
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(lavik_cluster_fault_harness PRIVATE
      -Wall -Wextra -Wpedantic)
  endif()

  add_executable(lavik_cluster_model_tests
    tests/cluster/fault_controls_test.cpp
    tests/cluster/fault_harness_test.cpp
  )
  target_link_libraries(lavik_cluster_model_tests PRIVATE
    lavik_cluster_fault_harness
    GTest::gtest_main
  )
  target_compile_definitions(lavik_cluster_model_tests PRIVATE
    LAVIK_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
  )
  gtest_discover_tests(lavik_cluster_model_tests
    TEST_PREFIX cluster_model.
    PROPERTIES
      LABELS cluster-model
      TIMEOUT 15
  )

  if (LAVIK_BUILD_META)
    # The public operator model is tested without lavik_meta_core so this
    # target also guards the promised Raft-free client boundary.
    add_executable(lavik_cluster_status_tests
      tests/meta_cluster_create_test.cpp
      tests/meta_cluster_status_test.cpp
      tests/meta_failover_admin_test.cpp
    )
    target_link_libraries(lavik_cluster_status_tests PRIVATE
      lavik_meta_admin
      GTest::gtest_main
    )
    gtest_discover_tests(lavik_cluster_status_tests
      TEST_PREFIX cluster_status.
      PROPERTIES LABELS "meta;operator" TIMEOUT 30)

    # Meta-plane tests. Links lavik_meta_core (NuRaft); the data-plane unit
    # test binary stays Raft-free, while this binary also links the data module
    # for explicit Meta-projection-to-Data-validation contract tests.
    add_executable(lavik_meta_tests
      tests/meta_automatic_failover_detector_test.cpp
      tests/meta_automatic_failover_reconciler_test.cpp
      tests/meta_hash_test.cpp
      tests/meta_cluster_create_reconciler_test.cpp
      tests/meta_membership_reconciler_test.cpp
      tests/meta_coordinator_test.cpp
      tests/meta_candidate_plan_test.cpp
      tests/meta_control_projector_test.cpp
      tests/meta_data_control_server_test.cpp
      tests/meta_failover_transition_apply_test.cpp
      tests/meta_failover_transition_w2_test.cpp
      tests/meta_failover_aggregate_revision_test.cpp
      tests/meta_failover_observation_test.cpp
      tests/meta_failover_reconciler_test.cpp
      tests/meta_failover_test.cpp
      tests/meta_journal_test.cpp
      tests/meta_model_test.cpp
      tests/meta_observation_test.cpp
      tests/meta_owner_serviceability_test.cpp
      tests/meta_security_test.cpp
      tests/meta_state_machine_test.cpp
      tests/meta_store_test.cpp
      tests/meta_stores_test.cpp
    )
    target_link_libraries(lavik_meta_tests PRIVATE
      lavik_meta_core
      lavik_module
      GTest::gtest_main
    )
    target_compile_definitions(lavik_meta_tests PRIVATE
      LAVIK_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
    )
    gtest_discover_tests(lavik_meta_tests
      TEST_PREFIX meta.
      PROPERTIES
        LABELS meta
        TIMEOUT 30
    )

    # 3-node end-to-end smoke for the meta control plane: membership changes,
    # replication, crash/restart catch-up, automatic and manual snapshots.
    find_program(LAVIK_PYTHON3_EXECUTABLE NAMES python3)
    if (LAVIK_PYTHON3_EXECUTABLE)
      add_test(
        NAME meta_integration.smoke_3node
        COMMAND ${LAVIK_PYTHON3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/smoke_3node.py
                $<TARGET_FILE:lavik-meta>
                $<TARGET_FILE:lavik-ctl>
      )
      set_tests_properties(meta_integration.smoke_3node PROPERTIES
        LABELS meta-integration
        TIMEOUT 120
        RUN_SERIAL TRUE
      )

      add_test(
        NAME meta_integration.ctl_client
        COMMAND ${LAVIK_PYTHON3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/ctl_client.py
                $<TARGET_FILE:lavik-meta>
                $<TARGET_FILE:lavik-ctl>
      )
      set_tests_properties(meta_integration.ctl_client PROPERTIES
        LABELS meta-integration
        TIMEOUT 60
        RUN_SERIAL TRUE
      )

      # Metadata process gates: leader-change safety, WAL/snapshot
      # recovery matrix, membership-change semantics, transport
      # connect/cancel/timeout, mTLS, the observation
      # observation-lifecycle gate, and Meta partition safety.
      # The backpressure gate enforces a production RSS budget, so sanitizer
      # allocator quarantine would turn it into a measurement of ASan retention
      # instead of the bounded live Raft queues it is intended to cover.
      set(LAVIK_META_INTEGRATION_GATES
        initial_meta initial_recovery snapshot_vote leader_change recovery membership membership_recovery transport mtls observation_lifecycle
        partition
      )
      if (NOT LAVIK_ASAN_BUILD)
        list(APPEND LAVIK_META_INTEGRATION_GATES backpressure)
      endif()

      # Each spins up its own
      # multi-node lavik-meta clusters; RUN_SERIAL keeps port allocation
      # and timing margins deterministic.
      foreach(gate IN LISTS LAVIK_META_INTEGRATION_GATES)
        add_test(
          NAME meta_integration.gate_${gate}
          COMMAND ${LAVIK_PYTHON3_EXECUTABLE}
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_${gate}.py
                  $<TARGET_FILE:lavik-meta>
        )
        set_tests_properties(meta_integration.gate_${gate} PROPERTIES
          LABELS meta-integration
          TIMEOUT 180
          RUN_SERIAL TRUE
        )
      endforeach()
      set_tests_properties(meta_integration.gate_initial_recovery PROPERTIES
        TIMEOUT 300)

      # Real-process Meta <-> Data control gate.  It needs both executables,
      # unlike the Meta-only fault gates above.
      add_test(
        NAME meta_integration.gate_data_control
        COMMAND ${LAVIK_PYTHON3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_data_control.py
                $<TARGET_FILE:lavik-meta>
                $<TARGET_FILE:lavik>
      )
      set_tests_properties(meta_integration.gate_data_control PROPERTIES
        LABELS meta-integration
        TIMEOUT 180
        RUN_SERIAL TRUE
      )

      # The tracer-bullet test independently verifies the eventual public
      # Redis surface. Production cluster-create returns at Genesis commit and
      # deliberately has no redis-cli dependency.
      find_program(LAVIK_CLUSTER_CREATE_REDIS_CLI NAMES redis-cli)
      if (LAVIK_CLUSTER_CREATE_REDIS_CLI)
        add_test(
          NAME meta_integration.gate_cluster_create
          COMMAND ${LAVIK_PYTHON3_EXECUTABLE}
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_cluster_create.py
                  $<TARGET_FILE:lavik-meta>
                  $<TARGET_FILE:lavik>
                  $<TARGET_FILE:lavik-ctl>
                  ${LAVIK_CLUSTER_CREATE_REDIS_CLI}
        )
        set_tests_properties(meta_integration.gate_cluster_create PROPERTIES
          LABELS meta-integration
          TIMEOUT 300
          RUN_SERIAL TRUE
        )

        # lavik-meta's deterministic failover seams follow the ordinary
        # !NDEBUG policy; LAVIK_BUILD_FAULT_SERVER instruments only Data.
        # Do not register a Release gate that can only fail its required-hook
        # precondition.
        if (CMAKE_BUILD_TYPE STREQUAL "Debug")
          # Failover needs the complete Meta <-> Data <-> Redis surface, so
          # these cases are deliberately not part of the Meta-only gate loop
          # above. Keep each failure mode independently addressable and its
          # process files in a unique child of LAVIK_TEST_DATA_DIR (or /tmp)
          # on every run. A failed child is retained with its precise path,
          # while a successful rerun cannot inherit older durable state.
          # Let the harness read the caller's environment at test runtime.
          set(LAVIK_FAILOVER_INTEGRATION_CASES
            controlled
            leader-resume
            prepared-leader-resume
            live-leader-demotion
            candidate-abort
            lease-fence
            source-degrade-reselect
          )
          # Unix-domain listener paths have a platform limit near 108 bytes.
          # Process evidence stays in the descriptive per-case directory;
          # only this unused auxiliary admin listener gets a short build-tree path.
          set(failover_socket_root "${CMAKE_CURRENT_BINARY_DIR}/fs")
          file(MAKE_DIRECTORY "${failover_socket_root}")
          foreach(failover_case IN LISTS LAVIK_FAILOVER_INTEGRATION_CASES)
            string(REPLACE "-" "_" failover_test_suffix "${failover_case}")
            set(failover_workdir
              "${CMAKE_CURRENT_BINARY_DIR}/meta-integration/failover/${failover_case}")
            file(MAKE_DIRECTORY "${failover_workdir}")
            add_test(
              NAME meta_integration.gate_failover_${failover_test_suffix}
              COMMAND ${CMAKE_COMMAND} -E env
                      "PYTHONPYCACHEPREFIX=${failover_workdir}/pycache"
                      "LAVIK_FAILOVER_SOCKET_ROOT=${failover_socket_root}"
                      ${LAVIK_PYTHON3_EXECUTABLE}
                      ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_failover.py
                      $<TARGET_FILE:lavik-meta>
                      $<TARGET_FILE:lavik>
                      $<TARGET_FILE:lavik-ctl>
                      ${LAVIK_CLUSTER_CREATE_REDIS_CLI}
                      --case=${failover_case}
                      --require-fault-hook
            )
            set_tests_properties(
              meta_integration.gate_failover_${failover_test_suffix}
              PROPERTIES
              LABELS "meta-integration;failover"
              TIMEOUT 180
              RUN_SERIAL TRUE
            )
          endforeach()
        endif()
      endif()

      # Automatic failover does not invoke redis-cli. Register its real-process
      # matrix independently so a build without that optional executable still
      # exercises detection and transition recovery.
      if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(automatic_failover_socket_root "${CMAKE_CURRENT_BINARY_DIR}/afs")
        file(MAKE_DIRECTORY "${automatic_failover_socket_root}")
        set(LAVIK_AUTOMATIC_FAILOVER_INTEGRATION_CASES
          owner-kill
          owner-pause
          one-way-partition
          two-way-partition
          leader-threshold
          leader-proposal
          leader-commit
        )
        foreach(automatic_case IN LISTS
                LAVIK_AUTOMATIC_FAILOVER_INTEGRATION_CASES)
          string(REPLACE "-" "_" automatic_test_suffix
                 "${automatic_case}")
          set(automatic_workdir
            "${CMAKE_CURRENT_BINARY_DIR}/meta-integration/automatic-failover/${automatic_case}")
          file(MAKE_DIRECTORY "${automatic_workdir}")
          add_test(
            NAME meta_integration.gate_automatic_failover_${automatic_test_suffix}
            COMMAND ${CMAKE_COMMAND} -E env
                    "PYTHONPYCACHEPREFIX=${automatic_workdir}/pycache"
                    "LAVIK_FAILOVER_SOCKET_ROOT=${automatic_failover_socket_root}"
                    ${LAVIK_PYTHON3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_automatic_failover.py
                    $<TARGET_FILE:lavik-meta>
                    $<TARGET_FILE:lavik>
                    $<TARGET_FILE:lavik-ctl>
                    --case=${automatic_case}
                    --require-fault-hook
          )
          set_tests_properties(
            meta_integration.gate_automatic_failover_${automatic_test_suffix}
            PROPERTIES
            LABELS "meta-integration;failover;automatic-failover"
            TIMEOUT 180
            RUN_SERIAL TRUE
          )
        endforeach()
        foreach(recovery_case IN ITEMS clean-owner clean-replica crash-operator
                                       manual-fence before-publish after-publish
                                       before-consume after-consume
                                       incomplete-full eligible-candidate)
          string(REPLACE "-" "_" recovery_test_suffix "${recovery_case}")
          add_test(
            NAME meta_integration.gate_population_recovery_${recovery_test_suffix}
            COMMAND ${CMAKE_COMMAND} -E env
                    "LAVIK_FAILOVER_SOCKET_ROOT=${automatic_failover_socket_root}"
                    ${LAVIK_PYTHON3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_population_recovery.py
                    $<TARGET_FILE:lavik-meta>
                    $<TARGET_FILE:lavik>
                    $<TARGET_FILE:lavik-ctl>
                    --case=${recovery_case}
          )
          set_tests_properties(
            meta_integration.gate_population_recovery_${recovery_test_suffix}
            PROPERTIES LABELS "meta-integration;failover;population-recovery"
                       TIMEOUT 180 RUN_SERIAL TRUE
          )
        endforeach()
      endif()
    else()
      message(STATUS "python3 not found; meta integration tests are disabled")
    endif()
  endif()

  # bycorf's connect/timer verification harness, run under ctest. Lavik
  # forces BYCORF_BUILD_EXAMPLES=OFF, so the bycorf-side example target never
  # materializes in this build; compile the source directly here instead (it
  # only uses bycorf public headers). The suite deliberately sleeps past armed
  # deadlines to catch stale-timeout dispatches, hence the 90s budget.
  add_executable(bycorf_connect_timer_check
    "${LAVIK_BYCORF_SOURCE_DIR}/example/connect_timer_check.cpp"
  )
  target_link_libraries(bycorf_connect_timer_check PRIVATE bycorf::core)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(bycorf_connect_timer_check PRIVATE
      -Wall -Wextra -Wpedantic)
  endif()
  add_test(
    NAME bycorf_connect_timer_check
    COMMAND bycorf_connect_timer_check
  )
  set_tests_properties(bycorf_connect_timer_check PROPERTIES
    LABELS bycorf
    TIMEOUT 90
  )

  add_executable(lavik_cluster_fault
    tests/cluster/fault_harness_main.cpp
  )
  target_link_libraries(lavik_cluster_fault PRIVATE
    lavik_cluster_fault_harness
  )
  add_test(
    NAME cluster_soak_reference_model
    COMMAND lavik_cluster_fault --soak-seconds 1 --seed 1
  )
  set_tests_properties(cluster_soak_reference_model PROPERTIES
    LABELS cluster-soak
    TIMEOUT 15)

  if (UNIX)
    add_test(
      NAME cluster_hardware_safety_gate
      COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/cluster/hardware_gate.sh
    )
    set_tests_properties(cluster_hardware_safety_gate PROPERTIES
      LABELS cluster-hardware
      SKIP_RETURN_CODE 77
      TIMEOUT 15)
  endif()

  if (UNIX)
    add_executable(lavik_process_support_tests
      tests/support/process_test.cpp
    )
    target_link_libraries(lavik_process_support_tests PRIVATE
      lavik_test_support
      GTest::gtest_main
    )
    gtest_discover_tests(lavik_process_support_tests
      TEST_PREFIX cluster_integration.
      PROPERTIES
        LABELS cluster-integration
        TIMEOUT 15
    )

    add_executable(lavik_cluster_population_integration_test
      tests/cluster/population_integration_test.cpp
    )
    target_link_libraries(lavik_cluster_population_integration_test PRIVATE
      lavik_test_support
      GTest::gtest
    )
    add_test(
      NAME cluster_integration.population_fail_closed
      COMMAND lavik_cluster_population_integration_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(cluster_integration.population_fail_closed PROPERTIES
      LABELS cluster-integration
      TIMEOUT 30
      RUN_SERIAL TRUE
    )

    add_executable(lavik_cluster_replication_manager_integration_test
      tests/cluster/replication_manager_integration_test.cpp
    )
    target_link_libraries(lavik_cluster_replication_manager_integration_test PRIVATE
      lavik::module
      lavik_test_support
      GTest::gtest_main
    )
    target_compile_definitions(
      lavik_cluster_replication_manager_integration_test PRIVATE
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME cluster_integration.replication_manager_control_api
      COMMAND lavik_cluster_replication_manager_integration_test
    )
    set_tests_properties(
      cluster_integration.replication_manager_control_api PROPERTIES
      LABELS cluster-integration
      TIMEOUT 90
      RUN_SERIAL TRUE
    )

    add_executable(lavik_cluster_serving_generation_integration_test
      tests/cluster/serving_generation_integration_test.cpp
    )
    target_link_libraries(lavik_cluster_serving_generation_integration_test PRIVATE
      lavik_test_support
      GTest::gtest
    )
    target_compile_definitions(lavik_cluster_serving_generation_integration_test PRIVATE
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>)
    add_dependencies(lavik_cluster_serving_generation_integration_test lavik)
    add_test(
      NAME cluster_integration.serving_generation_fence
      COMMAND lavik_cluster_serving_generation_integration_test
              $<TARGET_FILE:lavik>
    )
    set_tests_properties(cluster_integration.serving_generation_fence PROPERTIES
      LABELS cluster-integration
      TIMEOUT 45
      RUN_SERIAL TRUE
    )

    # The injection hook is compiled only into Debug or the explicitly
    # non-packageable fault server. Do not register a release test that would
    # silently run without its fault.
    if (CMAKE_BUILD_TYPE STREQUAL "Debug" OR TARGET lavik_fault_server)
      set(LAVIK_REBUILD_FAILURE_SERVER_TARGET lavik)
      if (TARGET lavik_fault_server)
        set(LAVIK_REBUILD_FAILURE_SERVER_TARGET lavik_fault_server)
      endif()
      add_executable(lavik_cluster_rebuild_failure_integration_test
        tests/cluster/rebuild_failure_integration_test.cpp
      )
      target_link_libraries(lavik_cluster_rebuild_failure_integration_test PRIVATE
        lavik_test_support
        GTest::gtest
      )
      add_dependencies(lavik_cluster_rebuild_failure_integration_test
        ${LAVIK_REBUILD_FAILURE_SERVER_TARGET})
      add_test(
        NAME cluster_integration.rebuild_failure_stops
        COMMAND lavik_cluster_rebuild_failure_integration_test
                $<TARGET_FILE:${LAVIK_REBUILD_FAILURE_SERVER_TARGET}>
      )
      set_tests_properties(cluster_integration.rebuild_failure_stops PROPERTIES
        LABELS cluster-integration
        TIMEOUT 45
        RUN_SERIAL TRUE
      )

      add_executable(lavik_cluster_rebuild_protocol_integration_test
        tests/cluster/rebuild_protocol_integration_test.cpp
      )
      target_compile_definitions(lavik_cluster_rebuild_protocol_integration_test PRIVATE
        LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>)
      target_link_libraries(lavik_cluster_rebuild_protocol_integration_test PRIVATE
        lavik_test_support
        GTest::gtest
      )
      add_dependencies(lavik_cluster_rebuild_protocol_integration_test
        ${LAVIK_REBUILD_FAILURE_SERVER_TARGET})
      add_test(
        NAME cluster_integration.rebuild_protocol_guards
        COMMAND lavik_cluster_rebuild_protocol_integration_test
                $<TARGET_FILE:${LAVIK_REBUILD_FAILURE_SERVER_TARGET}>
      )
      set_tests_properties(cluster_integration.rebuild_protocol_guards PROPERTIES
        LABELS cluster-integration
        TIMEOUT 120
        RUN_SERIAL TRUE
      )
    endif()
  endif()

  add_executable(lavik_unit_tests
    tests/buffer_pool_test.cpp
    tests/cluster_authority_test.cpp
    tests/cluster_command_test.cpp
    tests/cluster_topology_test.cpp
    tests/config_test.cpp
    tests/control_protocol_test.cpp
    tests/control_transport_test.cpp
    tests/cross_core_active_sender_test.cpp
    tests/cross_worker_mutex_test.cpp
    tests/device_affinity_test.cpp
    tests/foreign_executor_test.cpp
    tests/grouped_hash_test.cpp
    tests/grouped_commit_decision_test.cpp
    tests/grouped_collection_test.cpp
    tests/grouped_sorted_rewrite_test.cpp
    tests/ordered_compact_codec_test.cpp
    tests/collection_compact_stream_test.cpp
    tests/grouped_ordered_object_test.cpp
    tests/record_payload_cursor_test.cpp
    tests/grouped_object_index_test.cpp
    tests/local_shared_ptr_test.cpp
    tests/logging_test.cpp
    tests/lua_eval_test.cpp
    tests/memory_test.cpp
    tests/meta_control_test.cpp
    tests/meta_client_test.cpp
    tests/node_control_test.cpp
    tests/resp_test.cpp
    tests/scan_hash_map_test.cpp
    tests/source_authorization_test.cpp
    tests/command_table_test.cpp
    tests/random_sample_test.cpp
    tests/rdb_test.cpp
    tests/rdb_collection_test.cpp
    tests/record_location_test.cpp
    tests/request_gate_test.cpp
    tests/replication_command_test.cpp
    tests/replication_group_test.cpp
    tests/replica_applied_frontier_test.cpp
    tests/ring_buffer_test.cpp
    tests/tx_lock_test.cpp
    tests/storage_format_test.cpp
    tests/storage_capacity_test.cpp
    tests/tx_cleaner_test.cpp
    tests/tcp_nodelay_accept_test.cpp
  )
  target_link_libraries(lavik_unit_tests
    PRIVATE lavik::module GTest::gtest_main
  )
  target_include_directories(lavik_unit_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/redis
    ${CMAKE_CURRENT_SOURCE_DIR}/src/replication
  )
  gtest_discover_tests(lavik_unit_tests)

  add_test(
    NAME lavik_cli.cluster_control_surface
    COMMAND ${CMAKE_COMMAND}
            -DLAVIK_EXECUTABLE=$<TARGET_FILE:lavik>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_cluster_cli.cmake
  )

  if (UNIX)
    find_program(LAVIK_REDIS_SERVER_EXECUTABLE redis-server)
    find_program(LAVIK_REDIS_CLI_EXECUTABLE redis-cli)
    if (LAVIK_REDIS_SERVER_EXECUTABLE AND LAVIK_REDIS_CLI_EXECUTABLE)
      add_test(
        NAME lavik_redis_cluster_psync_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/redis_cluster_psync_e2e.sh
                $<TARGET_FILE:lavik>
                ${LAVIK_REDIS_SERVER_EXECUTABLE}
                ${LAVIK_REDIS_CLI_EXECUTABLE}
      )
      set_tests_properties(lavik_redis_cluster_psync_e2e
        PROPERTIES TIMEOUT 120)
      add_test(
        NAME lavik_redis_export_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/redis_export_e2e.sh
                $<TARGET_FILE:lavik>
                ${LAVIK_REDIS_SERVER_EXECUTABLE}
                ${LAVIK_REDIS_CLI_EXECUTABLE}
      )
      set_tests_properties(lavik_redis_export_e2e PROPERTIES
        TIMEOUT 120
        RUN_SERIAL TRUE)
      add_test(
        NAME lavik_redis_export_backpressure_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/redis_export_e2e.sh
                $<TARGET_FILE:lavik>
                ${LAVIK_REDIS_SERVER_EXECUTABLE}
                ${LAVIK_REDIS_CLI_EXECUTABLE}
                backpressure
      )
      set_tests_properties(lavik_redis_export_backpressure_e2e PROPERTIES
        TIMEOUT 120
        RUN_SERIAL TRUE)
      add_test(
        NAME lavik_rdb_backup_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/rdb_backup_e2e.sh
                $<TARGET_FILE:lavik>
                ${LAVIK_REDIS_CLI_EXECUTABLE}
      )
      set_tests_properties(lavik_rdb_backup_e2e PROPERTIES
        TIMEOUT 180
        RUN_SERIAL TRUE)
    endif()

    add_executable(lavik_replication_log_e2e_test
      tests/replication_log_e2e_test.cpp
    )
    target_link_libraries(lavik_replication_log_e2e_test
      PRIVATE lavik::module
    )
    add_test(
      NAME lavik_replication_log_e2e
      COMMAND lavik_replication_log_e2e_test
    )
    set_tests_properties(lavik_replication_log_e2e PROPERTIES TIMEOUT 120)

    add_executable(lavik_replica_abort_reclaim_e2e_test
      tests/replica_abort_reclaim_e2e_test.cpp
    )
    target_link_libraries(lavik_replica_abort_reclaim_e2e_test
      PRIVATE lavik::module
    )
    add_test(
      NAME lavik_replica_abort_reclaim_e2e
      COMMAND lavik_replica_abort_reclaim_e2e_test
    )
    set_tests_properties(lavik_replica_abort_reclaim_e2e PROPERTIES
      TIMEOUT 180)

    add_executable(lavik_pubsub_e2e_test
      tests/pubsub_e2e_test.cpp
    )
    add_test(
      NAME lavik_pubsub_e2e
      COMMAND lavik_pubsub_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_pubsub_e2e PROPERTIES
      TIMEOUT 120
      RUN_SERIAL TRUE)

    find_program(LAVIK_REDIS_SERVER_EXECUTABLE redis-server)
    if (LAVIK_REDIS_SERVER_EXECUTABLE)
      set(LAVIK_SENTINEL_SERVER_TARGET lavik)
      if (TARGET lavik_fault_server)
        set(LAVIK_SENTINEL_SERVER_TARGET lavik_fault_server)
      endif()
      add_executable(lavik_sentinel_e2e_test
        tests/sentinel_e2e_test.cpp
      )
      target_compile_definitions(lavik_sentinel_e2e_test PRIVATE
        LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>)
      target_link_libraries(lavik_sentinel_e2e_test PRIVATE
        lavik_test_support
      )
      add_test(
        NAME lavik_sentinel_e2e
        COMMAND lavik_sentinel_e2e_test
                $<TARGET_FILE:${LAVIK_SENTINEL_SERVER_TARGET}>
                ${LAVIK_REDIS_SERVER_EXECUTABLE}
      )
      set_tests_properties(lavik_sentinel_e2e PROPERTIES
        LABELS cluster-integration
        TIMEOUT 180
        RUN_SERIAL TRUE)
    endif()

    add_executable(lavik_flushdb_reclaim_e2e_test
      tests/flushdb_reclaim_e2e_test.cpp
    )
    target_link_libraries(lavik_flushdb_reclaim_e2e_test
      PRIVATE lavik::module
    )
    add_test(
      NAME lavik_flushdb_reclaim_e2e
      COMMAND lavik_flushdb_reclaim_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_flushdb_reclaim_e2e PROPERTIES TIMEOUT 600)
    add_test(
      NAME lavik_paused_defrag_full_e2e
      COMMAND lavik_flushdb_reclaim_e2e_test $<TARGET_FILE:lavik>
              --paused-defrag-only
    )
    set_tests_properties(lavik_paused_defrag_full_e2e PROPERTIES TIMEOUT 120)
    add_test(
      NAME lavik_stale_block_recovery_e2e
      COMMAND lavik_flushdb_reclaim_e2e_test $<TARGET_FILE:lavik>
              --stale-header-only
    )
    set_tests_properties(lavik_stale_block_recovery_e2e PROPERTIES TIMEOUT 60)

    add_executable(lavik_tomb_raider_e2e_test
      tests/tomb_raider_e2e_test.cpp
    )
    target_link_libraries(lavik_tomb_raider_e2e_test
      PRIVATE lavik::module
    )
    add_test(
      NAME lavik_tomb_raider_e2e
      COMMAND lavik_tomb_raider_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_tomb_raider_e2e PROPERTIES TIMEOUT 600)

    add_executable(lavik_multikey_e2e_test
      tests/multikey_e2e_test.cpp
    )
    target_compile_definitions(lavik_multikey_e2e_test PRIVATE
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>)
    add_test(
      NAME lavik_multikey_e2e
      COMMAND lavik_multikey_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_multikey_e2e PROPERTIES TIMEOUT 300)

    add_executable(lavik_multi_exec_e2e_test
      tests/multi_exec_e2e_test.cpp
    )
    add_test(
      NAME lavik_multi_exec_e2e
      COMMAND lavik_multi_exec_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_multi_exec_e2e PROPERTIES TIMEOUT 180)

    add_executable(lavik_atomicity_stress_e2e_test
      tests/atomicity_stress_e2e_test.cpp
    )
    add_test(
      NAME lavik_atomicity_stress_e2e
      COMMAND lavik_atomicity_stress_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_atomicity_stress_e2e PROPERTIES TIMEOUT 150)

    add_executable(lavik_ttl_e2e_test
      tests/ttl_e2e_test.cpp
    )
    target_link_libraries(lavik_ttl_e2e_test
      PRIVATE lavik::module
    )
    add_test(
      NAME lavik_ttl_e2e
      COMMAND lavik_ttl_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_ttl_e2e PROPERTIES TIMEOUT 90)

    add_executable(lavik_metrics_e2e_test
      tests/metrics_e2e_test.cpp
    )
    target_link_libraries(lavik_metrics_e2e_test PRIVATE GTest::gtest)
    add_test(
      NAME lavik_metrics_e2e
      COMMAND lavik_metrics_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_metrics_e2e PROPERTIES TIMEOUT 120)

    add_executable(lavik_extent_recovery_e2e_test
      tests/extent_recovery_e2e_test.cpp
    )
    add_test(
      NAME lavik_extent_recovery_e2e
      COMMAND lavik_extent_recovery_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_extent_recovery_e2e PROPERTIES TIMEOUT 600)

    add_executable(lavik_grouped_recovery_e2e_test
      tests/grouped_recovery_e2e_test.cpp
    )
    target_link_libraries(lavik_grouped_recovery_e2e_test
      PRIVATE lavik::module GTest::gtest
    )
    target_compile_definitions(lavik_grouped_recovery_e2e_test PRIVATE
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME lavik_grouped_recovery_e2e
      COMMAND lavik_grouped_recovery_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_grouped_recovery_e2e PROPERTIES
      TIMEOUT 600 RUN_SERIAL TRUE
    )

    add_executable(lavik_grouped_hash_write_e2e_test
      tests/grouped_hash_write_e2e_test.cpp
    )
    target_link_libraries(lavik_grouped_hash_write_e2e_test
      PRIVATE lavik::module GTest::gtest
    )
    target_compile_definitions(lavik_grouped_hash_write_e2e_test PRIVATE
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME lavik_grouped_hash_write_e2e
      COMMAND lavik_grouped_hash_write_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_grouped_hash_write_e2e PROPERTIES
      TIMEOUT 600 RUN_SERIAL TRUE
    )

    add_executable(lavik_compact_collection_write_e2e_test
      tests/compact_collection_write_e2e_test.cpp
    )
    target_link_libraries(lavik_compact_collection_write_e2e_test
      PRIVATE lavik::module GTest::gtest
    )
    target_compile_definitions(lavik_compact_collection_write_e2e_test PRIVATE
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME lavik_compact_collection_write_e2e
      COMMAND lavik_compact_collection_write_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_compact_collection_write_e2e PROPERTIES
      TIMEOUT 600 RUN_SERIAL TRUE
    )

    add_executable(lavik_grouped_ordered_write_e2e_test
      tests/grouped_ordered_write_e2e_test.cpp
      tests/grouped_rdb_stream_e2e_test.cpp
      tests/grouped_zset_write_e2e_test.cpp
      tests/grouped_transfer_e2e_test.cpp
      tests/grouped_random_stream_e2e_test.cpp
    )
    target_link_libraries(lavik_grouped_ordered_write_e2e_test
      PRIVATE lavik::module GTest::gtest
    )
    target_compile_definitions(lavik_grouped_ordered_write_e2e_test PRIVATE
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME lavik_grouped_ordered_write_e2e
      COMMAND lavik_grouped_ordered_write_e2e_test $<TARGET_FILE:lavik>
    )
    set_tests_properties(lavik_grouped_ordered_write_e2e PROPERTIES
      TIMEOUT 1200 RUN_SERIAL TRUE
    )

    add_executable(lavik_list_e2e_test
      tests/list_e2e_test.cpp
    )
    target_link_libraries(lavik_list_e2e_test
      PRIVATE lavik::module GTest::gtest
    )
    target_compile_definitions(lavik_list_e2e_test PRIVATE
      LAVIK_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
      LAVIK_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${LAVIK_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME lavik_list_e2e
      COMMAND lavik_list_e2e_test $<TARGET_FILE:lavik>
              --gtest_filter=-LargeHashDurabilityE2eTest.*
    )
    add_test(
      NAME lavik_large_hash_durability_e2e
      COMMAND lavik_list_e2e_test $<TARGET_FILE:lavik>
              --gtest_filter=LargeHashDurabilityE2eTest.*
    )
    set_tests_properties(lavik_large_hash_durability_e2e PROPERTIES
      TIMEOUT 180 RUN_SERIAL TRUE)
    if (LAVIK_ASAN_BUILD)
      set_tests_properties(lavik_list_e2e PROPERTIES TIMEOUT 1800)
    else()
      set_tests_properties(lavik_list_e2e PROPERTIES TIMEOUT 1200)
    endif()

  endif()
endif()
