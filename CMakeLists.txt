cmake_minimum_required(VERSION 3.20)

project(keylane VERSION 0.1.0 LANGUAGES C CXX)

set(KEYLANE_VERSION_SUFFIX "-dev" CACHE STRING
    "Keylane prerelease suffix, such as -dev or -beta.1 (empty for a release)")
set(KEYLANE_VERSION "${PROJECT_VERSION}${KEYLANE_VERSION_SUFFIX}")
set(KEYLANE_GENERATED_INCLUDE_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${KEYLANE_GENERATED_INCLUDE_DIR}/keylane")
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/version.h.in"
  "${KEYLANE_GENERATED_INCLUDE_DIR}/keylane/version.h"
  @ONLY
)

option(BUILD_TESTING "Build keylane tests" OFF)
include(CTest)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KEYLANE_ENABLE_WERROR "Treat warnings as errors" OFF)
option(KEYLANE_ENABLE_OPT "Enable aggressive optimization" ON)
option(KEYLANE_STATIC_OPENSSL "Link OpenSSL statically" ON)
option(KEYLANE_STATIC_CXX_RUNTIME
  "Link the GNU C++ and compiler runtimes statically" ON)
set(KEYLANE_MARCH "native" CACHE STRING
  "Value passed to -march for optimized local builds (empty disables it)")
option(KEYLANE_ENABLE_READ_LATENCY_TRACE "Enable detailed GET latency tracing" OFF)
option(KEYLANE_ENABLE_SET_LATENCY_TRACE "Enable detailed SET latency tracing" OFF)
option(KEYLANE_ENABLE_CROSS_CORE_HOP_COUNT "Count cross-core SubmitTaskTo transfers (test builds only)" OFF)
option(KEYLANE_WITH_SPDK "Use the SPDK userspace NVMe storage backend" OFF)
option(KEYLANE_BUILD_FAULT_SERVER
  "Build the non-packageable server variant used by deterministic fault tests"
  OFF)

if (KEYLANE_BUILD_FAULT_SERVER AND NOT BUILD_TESTING)
  message(FATAL_ERROR
    "KEYLANE_BUILD_FAULT_SERVER requires BUILD_TESTING=ON")
endif()

if (KEYLANE_ENABLE_OPT)
  include(CheckIPOSupported)
  check_ipo_supported(
    RESULT KEYLANE_IPO_SUPPORTED
    OUTPUT KEYLANE_IPO_ERROR
    LANGUAGES C CXX
  )
  if (KEYLANE_IPO_SUPPORTED)
    # IPO properties are initialized when a target is created. Keep these
    # defaults ahead of every bundled target so Celer, Abseil, mimalloc, and
    # Lua participate in the same link-time optimization as the server.
    # Debug deliberately stays non-IPO so local builds do not pay an
    # optimization cost that is unrelated to correctness.
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO TRUE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_MINSIZEREL TRUE)
  else()
    message(WARNING
      "IPO is unavailable; continuing without LTO: ${KEYLANE_IPO_ERROR}")
  endif()
endif()

set(KEYLANE_ASAN_BUILD OFF)
string(CONCAT KEYLANE_COMPILER_AND_LINKER_FLAGS
  "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} ${CMAKE_EXE_LINKER_FLAGS} "
  "${CMAKE_C_FLAGS_DEBUG} ${CMAKE_CXX_FLAGS_DEBUG} "
  "${CMAKE_C_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_RELEASE} "
  "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO} "
  "${CMAKE_C_FLAGS_MINSIZEREL} ${CMAKE_CXX_FLAGS_MINSIZEREL}")
if (KEYLANE_COMPILER_AND_LINKER_FLAGS MATCHES "-fsanitize=[^ ]*address")
  set(KEYLANE_ASAN_BUILD ON)
endif()
if (KEYLANE_ASAN_BUILD AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR
    "AddressSanitizer builds require Clang. GCC does not preserve coroutine "
    "symmetric transfers under ASan and can overflow the native stack.")
endif()
set(KEYLANE_CLANG_FORMAT_REQUIRED_VERSION "23.1.0")
string(REPLACE "." "\\." KEYLANE_CLANG_FORMAT_VERSION_REGEX
  "${KEYLANE_CLANG_FORMAT_REQUIRED_VERSION}")
find_program(KEYLANE_CLANG_FORMAT NAMES clang-format-23 clang-format)
if (KEYLANE_CLANG_FORMAT)
  execute_process(
    COMMAND ${KEYLANE_CLANG_FORMAT} --version
    RESULT_VARIABLE KEYLANE_CLANG_FORMAT_VERSION_RESULT
    OUTPUT_VARIABLE KEYLANE_CLANG_FORMAT_VERSION_OUTPUT
    ERROR_VARIABLE KEYLANE_CLANG_FORMAT_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE
  )
  # Formatting is repository-wide, so silently accepting another formatter
  # version would create unrelated churn in otherwise valid changes.
  if (NOT KEYLANE_CLANG_FORMAT_VERSION_RESULT EQUAL 0 OR
      NOT KEYLANE_CLANG_FORMAT_VERSION_OUTPUT MATCHES
        "clang-format version ${KEYLANE_CLANG_FORMAT_VERSION_REGEX}([^0-9.]|$)")
    unset(KEYLANE_CLANG_FORMAT CACHE)
    set(KEYLANE_CLANG_FORMAT "")
  endif()
endif()
file(GLOB_RECURSE KEYLANE_FORMAT_FILES CONFIGURE_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/app/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/keylane/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp
)
# CLI11.hpp is an upstream-generated single header; rewriting it makes local
# formatting depend on the generated artifact instead of its upstream source.
list(FILTER KEYLANE_FORMAT_FILES EXCLUDE REGEX "/keylane/CLI11\\.hpp$")
if (KEYLANE_CLANG_FORMAT)
  add_custom_target(format
    COMMAND ${KEYLANE_CLANG_FORMAT} -i ${KEYLANE_FORMAT_FILES}
    COMMENT "Formatting Keylane sources"
    VERBATIM
  )
  add_custom_target(format-check
    COMMAND ${KEYLANE_CLANG_FORMAT} --dry-run --Werror ${KEYLANE_FORMAT_FILES}
    COMMENT "Checking Keylane source formatting"
    VERBATIM
  )
else()
  message(STATUS
    "clang-format ${KEYLANE_CLANG_FORMAT_REQUIRED_VERSION} not found; "
    "format targets are disabled")
endif()

set(CELER_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CELER_WITH_SPDK_STORAGE ${KEYLANE_WITH_SPDK} CACHE BOOL "" FORCE)
# This must be set before Celer's first find_package(OpenSSL). Static release
# artifacts can then run without libssl.so or libcrypto.so on the target.
set(OPENSSL_USE_STATIC_LIBS ${KEYLANE_STATIC_OPENSSL} CACHE BOOL
  "Use static OpenSSL libraries" FORCE)
foreach(open_ssl_cache_var IN ITEMS OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY)
  if (KEYLANE_STATIC_OPENSSL AND DEFINED ${open_ssl_cache_var} AND
      NOT "${${open_ssl_cache_var}}" MATCHES "\\${CMAKE_STATIC_LIBRARY_SUFFIX}$")
    unset(${open_ssl_cache_var} CACHE)
  elseif (NOT KEYLANE_STATIC_OPENSSL AND DEFINED ${open_ssl_cache_var} AND
          "${${open_ssl_cache_var}}" MATCHES "\\${CMAKE_STATIC_LIBRARY_SUFFIX}$")
    unset(${open_ssl_cache_var} CACHE)
  endif()
endforeach()
find_package(OpenSSL REQUIRED)

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/celer/CMakeLists.txt")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/celer" "${CMAKE_CURRENT_BINARY_DIR}/celer")
else()
  message(FATAL_ERROR "celer submodule not found; run git submodule update --init --recursive")
endif()
target_compile_definitions(celer PUBLIC
  CELER_ENABLE_SUBMIT_TASK_COUNT=$<BOOL:${KEYLANE_ENABLE_CROSS_CORE_HOP_COUNT}>
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
# Apply Keylane's explicit CPU target below. This prevents mimalloc from
# silently raising packaged arm64 builds to armv8.1-a or x86_64 to Haswell.
set(MI_NO_OPT_ARCH ON CACHE BOOL "" FORCE)
set(MI_TRACK_ASAN ${KEYLANE_ASAN_BUILD} CACHE BOOL "" FORCE)
set(MI_EXTRA_CPPDEFS "MI_DEFAULT_ARENA_EAGER_COMMIT=1" CACHE STRING "" FORCE)
add_subdirectory(third_party/mimalloc)

option(KEYLANE_BUILD_META
  "Build the keylane-meta Raft control plane (requires the recursive nuraft submodule)"
  ON)
if (KEYLANE_BUILD_META)
  if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nuraft/CMakeLists.txt)
    message(FATAL_ERROR
      "nuraft submodule not found; run git submodule update --init --recursive")
  endif()
  if (NOT EXISTS
      ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nuraft/asio/asio/include/asio.hpp)
    message(FATAL_ERROR
      "nuraft/asio submodule not found; run git submodule update --init --recursive")
  endif()
  # Apply the pinned Keylane patch set to an isolated build-tree copy. The
  # third-party checkout stays pristine, configure is repeatable, and a patch
  # that no longer applies fails before compilation instead of drifting.
  set(KEYLANE_NURAFT_SOURCE_DIR
      ${CMAKE_CURRENT_BINARY_DIR}/_keylane_nuraft_source)
  file(REMOVE_RECURSE ${KEYLANE_NURAFT_SOURCE_DIR})
  file(MAKE_DIRECTORY ${KEYLANE_NURAFT_SOURCE_DIR})
  file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/nuraft/
       DESTINATION ${KEYLANE_NURAFT_SOURCE_DIR}
       PATTERN ".git" EXCLUDE)
  find_program(KEYLANE_GIT_EXECUTABLE git REQUIRED)
  file(GLOB KEYLANE_NURAFT_PATCHES CONFIGURE_DEPENDS
       ${CMAKE_CURRENT_SOURCE_DIR}/third_party/patches/nuraft/*.patch)
  list(SORT KEYLANE_NURAFT_PATCHES)
  foreach(KEYLANE_NURAFT_PATCH IN LISTS KEYLANE_NURAFT_PATCHES)
    execute_process(
      # The isolated copy may live underneath Keylane's own worktree. Stop
      # repository discovery at the build root so git applies paths relative
      # to WORKING_DIRECTORY instead of silently skipping every path outside
      # the generated directory's prefix in the parent repository.
      COMMAND ${CMAKE_COMMAND} -E env
              GIT_CEILING_DIRECTORIES=${CMAKE_CURRENT_BINARY_DIR}
              ${KEYLANE_GIT_EXECUTABLE} apply --check ${KEYLANE_NURAFT_PATCH}
      WORKING_DIRECTORY ${KEYLANE_NURAFT_SOURCE_DIR}
      RESULT_VARIABLE KEYLANE_NURAFT_PATCH_CHECK)
    if (NOT KEYLANE_NURAFT_PATCH_CHECK EQUAL 0)
      message(FATAL_ERROR "NuRaft patch no longer applies: ${KEYLANE_NURAFT_PATCH}")
    endif()
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E env
              GIT_CEILING_DIRECTORIES=${CMAKE_CURRENT_BINARY_DIR}
              ${KEYLANE_GIT_EXECUTABLE} apply ${KEYLANE_NURAFT_PATCH}
      WORKING_DIRECTORY ${KEYLANE_NURAFT_SOURCE_DIR}
      COMMAND_ERROR_IS_FATAL ANY)
  endforeach()
  unset(KEYLANE_NURAFT_PATCH)
  unset(KEYLANE_NURAFT_PATCHES)
  set(DISABLE_ASIO OFF)
  set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(ASIO_INCLUDE_DIR
      ${KEYLANE_NURAFT_SOURCE_DIR}/asio/asio/include CACHE PATH "" FORCE)
  # NuRaft's tests subdirectory builds its asio-based harness whenever
  # BUILD_TESTING is on. Shadow keylane's global test enablement for this
  # subdirectory only, then restore it for keylane's own test targets.
  set(KEYLANE_SAVED_BUILD_TESTING ${BUILD_TESTING})
  set(BUILD_TESTING OFF)
  add_subdirectory(${KEYLANE_NURAFT_SOURCE_DIR}
                   ${CMAKE_CURRENT_BINARY_DIR}/_keylane_nuraft_build)
  set(BUILD_TESTING ${KEYLANE_SAVED_BUILD_TESTING})
  unset(KEYLANE_SAVED_BUILD_TESTING)
  # NuRaft's headers are not warning-clean under -Wpedantic/-Wunused-parameter;
  # consumers compile with -Wall -Wextra -Wpedantic, so treat them as SYSTEM.
  get_target_property(KEYLANE_NURAFT_IFACE_INCLUDES static_lib
    INTERFACE_INCLUDE_DIRECTORIES)
  if (KEYLANE_NURAFT_IFACE_INCLUDES)
    set_target_properties(static_lib PROPERTIES
      INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${KEYLANE_NURAFT_IFACE_INCLUDES}")
  endif()
  unset(KEYLANE_NURAFT_IFACE_INCLUDES)
endif()

set(KEYLANE_LUA_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/lua/src")
add_library(keylane_lua STATIC
  ${KEYLANE_LUA_SOURCE_DIR}/lapi.c
  ${KEYLANE_LUA_SOURCE_DIR}/lauxlib.c
  ${KEYLANE_LUA_SOURCE_DIR}/lbaselib.c
  ${KEYLANE_LUA_SOURCE_DIR}/lcode.c
  ${KEYLANE_LUA_SOURCE_DIR}/ldebug.c
  ${KEYLANE_LUA_SOURCE_DIR}/ldo.c
  ${KEYLANE_LUA_SOURCE_DIR}/ldump.c
  ${KEYLANE_LUA_SOURCE_DIR}/lfunc.c
  ${KEYLANE_LUA_SOURCE_DIR}/lgc.c
  ${KEYLANE_LUA_SOURCE_DIR}/llex.c
  ${KEYLANE_LUA_SOURCE_DIR}/lmathlib.c
  ${KEYLANE_LUA_SOURCE_DIR}/lmem.c
  ${KEYLANE_LUA_SOURCE_DIR}/lobject.c
  ${KEYLANE_LUA_SOURCE_DIR}/lopcodes.c
  ${KEYLANE_LUA_SOURCE_DIR}/lparser.c
  ${KEYLANE_LUA_SOURCE_DIR}/lstate.c
  ${KEYLANE_LUA_SOURCE_DIR}/lstring.c
  ${KEYLANE_LUA_SOURCE_DIR}/lstrlib.c
  ${KEYLANE_LUA_SOURCE_DIR}/ltable.c
  ${KEYLANE_LUA_SOURCE_DIR}/ltablib.c
  ${KEYLANE_LUA_SOURCE_DIR}/ltm.c
  ${KEYLANE_LUA_SOURCE_DIR}/lundump.c
  ${KEYLANE_LUA_SOURCE_DIR}/lvm.c
  ${KEYLANE_LUA_SOURCE_DIR}/lzio.c
  ${KEYLANE_LUA_SOURCE_DIR}/strbuf.c
  ${KEYLANE_LUA_SOURCE_DIR}/fpconv.c
  ${KEYLANE_LUA_SOURCE_DIR}/lua_cjson.c
  ${KEYLANE_LUA_SOURCE_DIR}/lua_struct.c
  ${KEYLANE_LUA_SOURCE_DIR}/lua_cmsgpack.c
  ${KEYLANE_LUA_SOURCE_DIR}/lua_bit.c
)
target_include_directories(keylane_lua PUBLIC ${KEYLANE_LUA_SOURCE_DIR})
target_compile_definitions(keylane_lua PRIVATE
  LUA_USE_POSIX
  # luaopen_cjson returns its module table but only publishes the Redis/Valkey
  # global when this compatibility define is enabled.
  ENABLE_CJSON_GLOBAL
)
target_link_libraries(keylane_lua PUBLIC m)

if (TARGET format AND TARGET celer-format)
  add_dependencies(format celer-format)
  add_dependencies(format-check celer-format-check)
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

add_library(keylane_lease_clock STATIC
  src/cluster/lease_clock.cpp
)
add_library(keylane::lease_clock ALIAS keylane_lease_clock)
target_include_directories(keylane_lease_clock PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(keylane_lease_clock PUBLIC cxx_std_23)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(keylane_lease_clock PRIVATE
    -Wall -Wextra -Wpedantic)
endif()

add_library(keylane_control_protocol STATIC
  src/cluster/control_protocol.cpp
)
add_library(keylane::control_protocol ALIAS keylane_control_protocol)
target_include_directories(keylane_control_protocol PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(keylane_control_protocol PUBLIC cxx_std_23)
target_link_libraries(keylane_control_protocol PUBLIC
  absl::status
  absl::statusor
)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(keylane_control_protocol PRIVATE
    -Wall -Wextra -Wpedantic)
endif()

add_library(keylane_control_transport STATIC
  src/cluster/control_transport.cpp
)
add_library(keylane::control_transport ALIAS keylane_control_transport)
target_include_directories(keylane_control_transport PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(keylane_control_transport PUBLIC cxx_std_23)
target_link_libraries(keylane_control_transport PUBLIC
  keylane::control_protocol
  celer::core
  absl::status
  absl::statusor
)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(keylane_control_transport PRIVATE
    -Wall -Wextra -Wpedantic)
endif()

add_library(keylane_module
  src/cluster/authority.cpp
  src/cluster/control_port.cpp
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

add_library(keylane::module ALIAS keylane_module)

target_include_directories(keylane_module
  PUBLIC
    ${KEYLANE_GENERATED_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_precompile_headers(keylane_module PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include/keylane/pch.h
)

target_compile_features(keylane_module PUBLIC cxx_std_23)
target_compile_definitions(keylane_module PUBLIC
  KEYLANE_ASAN_BUILD=$<BOOL:${KEYLANE_ASAN_BUILD}>
  KEYLANE_ENABLE_READ_LATENCY_TRACE=$<BOOL:${KEYLANE_ENABLE_READ_LATENCY_TRACE}>
  KEYLANE_ENABLE_SET_LATENCY_TRACE=$<BOOL:${KEYLANE_ENABLE_SET_LATENCY_TRACE}>
  KEYLANE_ENABLE_CROSS_CORE_HOP_COUNT=$<BOOL:${KEYLANE_ENABLE_CROSS_CORE_HOP_COUNT}>
  KEYLANE_ENABLE_TEST_FAULTS=$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>
)
target_link_libraries(keylane_module PUBLIC
  keylane::lease_clock
  keylane::control_transport
  keylane::control_protocol
  celer::core
  celer::rpc
  absl::crc32c
  absl::flat_hash_map
  absl::inlined_vector
  absl::status
  absl::statusor
  absl::strings
  mimalloc-static
  keylane_lua
  OpenSSL::Crypto
)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(keylane_module PRIVATE -Wall -Wextra -Wpedantic)
  if (KEYLANE_ENABLE_WERROR)
    target_compile_options(keylane_module PRIVATE -Werror)
  endif()
endif()

add_executable(keylane
  app/keylane.cpp
  app/mimalloc_new_delete.cpp
)
target_precompile_headers(keylane REUSE_FROM keylane_module)
target_link_libraries(keylane PRIVATE keylane::module)

if (KEYLANE_BUILD_FAULT_SERVER)
  # This binary intentionally shares the test-instrumented module with the
  # ordinary test server in this build tree. The option is rejected unless
  # BUILD_TESTING is enabled, so packaging cannot accidentally ship it.
  add_executable(keylane_fault_server
    app/keylane.cpp
    app/mimalloc_new_delete.cpp
  )
  target_precompile_headers(keylane_fault_server REUSE_FROM keylane_module)
  target_link_libraries(keylane_fault_server PRIVATE keylane::module)
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  if (KEYLANE_ENABLE_OPT)
    set(KEYLANE_OPT_COMPILE_OPTIONS -O3)
    if (NOT KEYLANE_MARCH STREQUAL "")
      list(APPEND KEYLANE_OPT_COMPILE_OPTIONS "-march=${KEYLANE_MARCH}")
    endif()
    # Celer is compiled as part of Keylane, so local native builds should tune
    # the transport and runtime too, not only the Redis/storage layer. Abseil's
    # accelerated CRC engines are compile-gated by architecture feature
    # macros; apply the explicit target to those translation units as well or
    # a native Keylane build silently uses the generic table-based checksum.
    set(KEYLANE_OPT_TARGETS
        absl_crc_internal
        absl_crc32c
        celer
        celer_rpc
        keylane_control_protocol
        keylane_control_transport
        keylane_lease_clock
        keylane_module
        keylane)
    if (TARGET keylane_fault_server)
      list(APPEND KEYLANE_OPT_TARGETS keylane_fault_server)
    endif()
    foreach(target IN LISTS KEYLANE_OPT_TARGETS)
      target_compile_options(${target} PRIVATE ${KEYLANE_OPT_COMPILE_OPTIONS})
    endforeach()
    if (NOT KEYLANE_MARCH STREQUAL "")
      target_compile_options(mimalloc-static PRIVATE "-march=${KEYLANE_MARCH}")
    endif()
  endif()
  if (KEYLANE_STATIC_CXX_RUNTIME)
    target_link_options(keylane PRIVATE -static-libstdc++ -static-libgcc)
    if (TARGET keylane_fault_server)
      target_link_options(keylane_fault_server PRIVATE
        -static-libstdc++ -static-libgcc)
    endif()
  endif()
endif()

if (UNIX)
  add_custom_target(valkey-tcl-tests
    COMMAND ${CMAKE_COMMAND} -E env
            KEYLANE_BIN=$<TARGET_FILE:keylane>
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/valkey/run-keylane
    DEPENDS keylane
    USES_TERMINAL
    COMMENT "Running vendored Valkey TCL data-structure tests"
  )
endif()

if (KEYLANE_BUILD_META)
  # Architecture layering: Raft is a meta-plane concern. NuRaft is linked
  # only into keylane-meta; keylane, keylane_module, and the test binaries
  # must stay Raft-free so the data plane never sees consensus code.
  #
  # keylane_meta_core carries durability and control logic; Celer owns
  # foreign-thread completion ingress while NuRaft's native Asio service owns
  # peer transport and timers.
  set(KEYLANE_META_CORE_SOURCES
    src/meta/audit_store.cpp
    src/meta/commands.cpp
    src/meta/control_projector.cpp
    src/meta/coordinator.cpp
    src/meta/ctl_server.cpp
    src/meta/data_control_server.cpp
    src/meta/encoding.cpp
    src/meta/grant_store.cpp
    src/meta/identity_verifier.cpp
    src/meta/identity_store.cpp
    src/meta/observation_store.cpp
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
  add_library(keylane_meta_core STATIC ${KEYLANE_META_CORE_SOURCES})
  target_include_directories(keylane_meta_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(keylane_meta_core PUBLIC
    keylane::lease_clock
    keylane::control_transport
    keylane::control_protocol
    NuRaft::static_lib
    celer::core
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(keylane_meta_core PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  add_executable(keylane-meta
    app/keylane_meta.cpp
  )
  target_include_directories(keylane-meta PRIVATE
    ${KEYLANE_GENERATED_INCLUDE_DIR}
  )
  target_compile_definitions(keylane-meta PRIVATE
    KEYLANE_NURAFT_PINNED_COMMIT=\"0b01b185b46b8e482f6c384b636c35605121a41e\"
  )
  target_link_libraries(keylane-meta PRIVATE
    keylane_meta_core
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(keylane-meta PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  # The operator client intentionally does not link keylane_meta_core (and
  # therefore NuRaft). It only needs OpenSSL for the optional remote mTLS
  # transport; local administration uses a Unix domain socket.
  add_executable(keylane-meta-ctl
    app/keylane_meta_ctl.cpp
  )
  target_include_directories(keylane-meta-ctl PRIVATE
    ${KEYLANE_GENERATED_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(keylane-meta-ctl PRIVATE
    OpenSSL::SSL
    OpenSSL::Crypto
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(keylane-meta-ctl PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  # Negative guard for the layering rule above: fail at configure time if
  # nuraft ever leaks into the data-plane module's link interface.
  get_target_property(KEYLANE_MODULE_IFACE_LIBS keylane_module
    INTERFACE_LINK_LIBRARIES)
  if (KEYLANE_MODULE_IFACE_LIBS MATCHES "nuraft|NuRaft")
    message(FATAL_ERROR
      "keylane_module must not link NuRaft; Raft is meta-plane only")
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

  include(FetchContent)

  add_test(NAME keylane_version COMMAND keylane --version)
  set_tests_properties(keylane_version PROPERTIES
    PASS_REGULAR_EXPRESSION "keylane ${KEYLANE_VERSION}")
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
      set(fault_target keylane_fault_injection_${fault_mode}_test)
      add_executable(${fault_target} tests/fault_injection_test.cpp)
      target_link_libraries(${fault_target} PRIVATE GTest::gtest_main)
      target_include_directories(${fault_target} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include)
      set_target_properties(${fault_target} PROPERTIES
        DISABLE_PRECOMPILE_HEADERS ON)
      if (fault_mode STREQUAL "debug")
        target_compile_options(${fault_target} PRIVATE -UNDEBUG)
        target_compile_definitions(${fault_target} PRIVATE
          KEYLANE_ENABLE_TEST_FAULTS=0 KEYLANE_FAULT_TEST_EXPECTED_ENABLED=1)
      elseif (fault_mode STREQUAL "release")
        target_compile_definitions(${fault_target} PRIVATE
          NDEBUG KEYLANE_ENABLE_TEST_FAULTS=0
          KEYLANE_FAULT_TEST_EXPECTED_ENABLED=0)
      else()
        target_compile_definitions(${fault_target} PRIVATE
          NDEBUG KEYLANE_ENABLE_TEST_FAULTS=1
          KEYLANE_FAULT_TEST_EXPECTED_ENABLED=1)
      endif()
      add_test(NAME keylane_fault_injection_${fault_mode}
        COMMAND ${fault_target})
      set_tests_properties(keylane_fault_injection_${fault_mode} PROPERTIES
        LABELS "unit;fault-policy" TIMEOUT 15)
    endforeach()
  endif()

  add_library(keylane_test_support STATIC
    tests/support/process.cpp
  )
  target_include_directories(keylane_test_support PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_compile_features(keylane_test_support PUBLIC cxx_std_23)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(keylane_test_support PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  add_library(keylane_cluster_fault_harness STATIC
    tests/cluster/cluster_invariants.cpp
    tests/cluster/fault_controls.cpp
    tests/cluster/fault_harness.cpp
    tests/cluster/fault_trace.cpp
    tests/cluster/reference_model.cpp
  )
  target_include_directories(keylane_cluster_fault_harness PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_compile_features(keylane_cluster_fault_harness PUBLIC cxx_std_23)
  target_link_libraries(keylane_cluster_fault_harness PUBLIC
    absl::status
    absl::statusor
    absl::strings
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(keylane_cluster_fault_harness PRIVATE
      -Wall -Wextra -Wpedantic)
  endif()

  add_executable(keylane_cluster_model_tests
    tests/cluster/fault_controls_test.cpp
    tests/cluster/fault_harness_test.cpp
  )
  target_link_libraries(keylane_cluster_model_tests PRIVATE
    keylane_cluster_fault_harness
    GTest::gtest_main
  )
  target_compile_definitions(keylane_cluster_model_tests PRIVATE
    KEYLANE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
  )
  gtest_discover_tests(keylane_cluster_model_tests
    TEST_PREFIX cluster_model.
    PROPERTIES
      LABELS cluster-model
      TIMEOUT 15
  )

  if (KEYLANE_BUILD_META)
    # Meta-plane tests. Links keylane_meta_core (NuRaft); the data-plane unit
    # test binary stays Raft-free, while this binary also links the data module
    # for explicit Meta-projection-to-Data-validation contract tests.
    add_executable(keylane_meta_tests
      tests/meta_coordinator_test.cpp
      tests/meta_control_projector_test.cpp
      tests/meta_data_control_server_test.cpp
      tests/meta_journal_test.cpp
      tests/meta_model_test.cpp
      tests/meta_observation_test.cpp
      tests/meta_security_test.cpp
      tests/meta_state_machine_test.cpp
      tests/meta_store_test.cpp
      tests/meta_stores_test.cpp
    )
    target_link_libraries(keylane_meta_tests PRIVATE
      keylane_meta_core
      keylane_module
      GTest::gtest_main
    )
    target_compile_definitions(keylane_meta_tests PRIVATE
      KEYLANE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
    )
    gtest_discover_tests(keylane_meta_tests
      TEST_PREFIX meta.
      PROPERTIES
        LABELS meta
        TIMEOUT 30
    )

    # 3-node end-to-end smoke for the meta control plane: membership changes,
    # replication, crash/restart catch-up, automatic and manual snapshots.
    find_program(KEYLANE_PYTHON3_EXECUTABLE NAMES python3)
    if (KEYLANE_PYTHON3_EXECUTABLE)
      add_test(
        NAME meta_integration.smoke_3node
        COMMAND ${KEYLANE_PYTHON3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/smoke_3node.py
                $<TARGET_FILE:keylane-meta>
      )
      set_tests_properties(meta_integration.smoke_3node PROPERTIES
        LABELS meta-integration
        TIMEOUT 120
        RUN_SERIAL TRUE
      )

      add_test(
        NAME meta_integration.ctl_client
        COMMAND ${KEYLANE_PYTHON3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/ctl_client.py
                $<TARGET_FILE:keylane-meta>
                $<TARGET_FILE:keylane-meta-ctl>
      )
      set_tests_properties(meta_integration.ctl_client PROPERTIES
        LABELS meta-integration
        TIMEOUT 60
        RUN_SERIAL TRUE
      )

      # Metadata process gates: leader-change safety, WAL/snapshot
      # recovery matrix, membership-change semantics, transport
      # connect/cancel/timeout, mTLS, the observation
      # stale-evidence/observation-lifecycle gate, and Meta partition safety.
      # The backpressure gate enforces a production RSS budget, so sanitizer
      # allocator quarantine would turn it into a measurement of ASan retention
      # instead of the bounded live Raft queues it is intended to cover.
      set(KEYLANE_META_INTEGRATION_GATES
        leader_change recovery membership transport mtls stale_evidence
        partition
      )
      if (NOT KEYLANE_ASAN_BUILD)
        list(APPEND KEYLANE_META_INTEGRATION_GATES backpressure)
      endif()

      # Each spins up its own
      # multi-node keylane-meta clusters; RUN_SERIAL keeps port allocation
      # and timing margins deterministic.
      foreach(gate IN LISTS KEYLANE_META_INTEGRATION_GATES)
        add_test(
          NAME meta_integration.gate_${gate}
          COMMAND ${KEYLANE_PYTHON3_EXECUTABLE}
                  ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_${gate}.py
                  $<TARGET_FILE:keylane-meta>
        )
        set_tests_properties(meta_integration.gate_${gate} PROPERTIES
          LABELS meta-integration
          TIMEOUT 180
          RUN_SERIAL TRUE
        )
      endforeach()

      # Real-process Meta <-> Data control gate.  It needs both executables,
      # unlike the Meta-only fault gates above.
      add_test(
        NAME meta_integration.gate_data_control
        COMMAND ${KEYLANE_PYTHON3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/meta_integration/gate_data_control.py
                $<TARGET_FILE:keylane-meta>
                $<TARGET_FILE:keylane>
      )
      set_tests_properties(meta_integration.gate_data_control PROPERTIES
        LABELS meta-integration
        TIMEOUT 180
        RUN_SERIAL TRUE
      )
    else()
      message(STATUS "python3 not found; meta integration tests are disabled")
    endif()
  endif()

  # celer's connect/timer verification harness, run under ctest. Keylane
  # forces CELER_BUILD_EXAMPLES=OFF, so the celer-side example target never
  # materializes in this build; compile the source directly here instead (it
  # only uses celer public headers). The suite deliberately sleeps past armed
  # deadlines to catch stale-timeout dispatches, hence the 90s budget.
  add_executable(celer_connect_timer_check
    celer/example/connect_timer_check.cpp
  )
  target_link_libraries(celer_connect_timer_check PRIVATE celer::core)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(celer_connect_timer_check PRIVATE
      -Wall -Wextra -Wpedantic)
  endif()
  add_test(
    NAME celer_connect_timer_check
    COMMAND celer_connect_timer_check
  )
  set_tests_properties(celer_connect_timer_check PROPERTIES
    LABELS celer
    TIMEOUT 90
  )

  add_executable(keylane_cluster_fault
    tests/cluster/fault_harness_main.cpp
  )
  target_link_libraries(keylane_cluster_fault PRIVATE
    keylane_cluster_fault_harness
  )
  add_test(
    NAME cluster_soak_reference_model
    COMMAND keylane_cluster_fault --soak-seconds 1 --seed 1
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
    add_executable(keylane_process_support_tests
      tests/support/process_test.cpp
    )
    target_link_libraries(keylane_process_support_tests PRIVATE
      keylane_test_support
      GTest::gtest_main
    )
    gtest_discover_tests(keylane_process_support_tests
      TEST_PREFIX cluster_integration.
      PROPERTIES
        LABELS cluster-integration
        TIMEOUT 15
    )

    add_executable(keylane_cluster_population_integration_test
      tests/cluster/population_integration_test.cpp
    )
    target_link_libraries(keylane_cluster_population_integration_test PRIVATE
      keylane_test_support
      GTest::gtest
    )
    add_test(
      NAME cluster_integration.population_fail_closed
      COMMAND keylane_cluster_population_integration_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(cluster_integration.population_fail_closed PROPERTIES
      LABELS cluster-integration
      TIMEOUT 30
      RUN_SERIAL TRUE
    )

    add_executable(keylane_cluster_replication_manager_integration_test
      tests/cluster/replication_manager_integration_test.cpp
    )
    target_link_libraries(keylane_cluster_replication_manager_integration_test PRIVATE
      keylane::module
      keylane_test_support
      GTest::gtest_main
    )
    add_test(
      NAME cluster_integration.replication_manager_control_api
      COMMAND keylane_cluster_replication_manager_integration_test
    )
    set_tests_properties(
      cluster_integration.replication_manager_control_api PROPERTIES
      LABELS cluster-integration
      TIMEOUT 30
      RUN_SERIAL TRUE
    )

    add_executable(keylane_cluster_serving_generation_integration_test
      tests/cluster/serving_generation_integration_test.cpp
    )
    target_link_libraries(keylane_cluster_serving_generation_integration_test PRIVATE
      keylane_test_support
      GTest::gtest
    )
    target_compile_definitions(keylane_cluster_serving_generation_integration_test PRIVATE
      KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>)
    add_dependencies(keylane_cluster_serving_generation_integration_test keylane)
    add_test(
      NAME cluster_integration.serving_generation_fence
      COMMAND keylane_cluster_serving_generation_integration_test
              $<TARGET_FILE:keylane>
    )
    set_tests_properties(cluster_integration.serving_generation_fence PROPERTIES
      LABELS cluster-integration
      TIMEOUT 45
      RUN_SERIAL TRUE
    )

    # The injection hook is compiled only into Debug or the explicitly
    # non-packageable fault server. Do not register a release test that would
    # silently run without its fault.
    if (CMAKE_BUILD_TYPE STREQUAL "Debug" OR TARGET keylane_fault_server)
      set(KEYLANE_REBUILD_FAILURE_SERVER_TARGET keylane)
      if (TARGET keylane_fault_server)
        set(KEYLANE_REBUILD_FAILURE_SERVER_TARGET keylane_fault_server)
      endif()
      add_executable(keylane_cluster_rebuild_failure_integration_test
        tests/cluster/rebuild_failure_integration_test.cpp
      )
      target_link_libraries(keylane_cluster_rebuild_failure_integration_test PRIVATE
        keylane_test_support
        GTest::gtest
      )
      add_dependencies(keylane_cluster_rebuild_failure_integration_test
        ${KEYLANE_REBUILD_FAILURE_SERVER_TARGET})
      add_test(
        NAME cluster_integration.rebuild_failure_stops
        COMMAND keylane_cluster_rebuild_failure_integration_test
                $<TARGET_FILE:${KEYLANE_REBUILD_FAILURE_SERVER_TARGET}>
      )
      set_tests_properties(cluster_integration.rebuild_failure_stops PROPERTIES
        LABELS cluster-integration
        TIMEOUT 45
        RUN_SERIAL TRUE
      )

      add_executable(keylane_cluster_rebuild_protocol_integration_test
        tests/cluster/rebuild_protocol_integration_test.cpp
      )
      target_compile_definitions(keylane_cluster_rebuild_protocol_integration_test PRIVATE
        KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>)
      target_link_libraries(keylane_cluster_rebuild_protocol_integration_test PRIVATE
        keylane_test_support
        GTest::gtest
      )
      add_dependencies(keylane_cluster_rebuild_protocol_integration_test
        ${KEYLANE_REBUILD_FAILURE_SERVER_TARGET})
      add_test(
        NAME cluster_integration.rebuild_protocol_guards
        COMMAND keylane_cluster_rebuild_protocol_integration_test
                $<TARGET_FILE:${KEYLANE_REBUILD_FAILURE_SERVER_TARGET}>
      )
      set_tests_properties(cluster_integration.rebuild_protocol_guards PROPERTIES
        LABELS cluster-integration
        TIMEOUT 120
        RUN_SERIAL TRUE
      )
    endif()
  endif()

  add_executable(keylane_unit_tests
    tests/buffer_pool_test.cpp
    tests/cluster_authority_test.cpp
    tests/cluster_command_test.cpp
    tests/cluster_control_port_test.cpp
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
    tests/ring_buffer_test.cpp
    tests/tx_lock_test.cpp
    tests/storage_format_test.cpp
    tests/storage_capacity_test.cpp
    tests/tx_cleaner_test.cpp
    tests/tcp_nodelay_accept_test.cpp
  )
  target_link_libraries(keylane_unit_tests
    PRIVATE keylane::module GTest::gtest_main
  )
  target_include_directories(keylane_unit_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/redis
  )
  gtest_discover_tests(keylane_unit_tests)

  if (UNIX)
    find_program(KEYLANE_REDIS_SERVER_EXECUTABLE redis-server)
    find_program(KEYLANE_REDIS_CLI_EXECUTABLE redis-cli)
    if (KEYLANE_REDIS_SERVER_EXECUTABLE AND KEYLANE_REDIS_CLI_EXECUTABLE)
      add_test(
        NAME keylane_redis_cluster_psync_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/redis_cluster_psync_e2e.sh
                $<TARGET_FILE:keylane>
                ${KEYLANE_REDIS_SERVER_EXECUTABLE}
                ${KEYLANE_REDIS_CLI_EXECUTABLE}
      )
      set_tests_properties(keylane_redis_cluster_psync_e2e
        PROPERTIES TIMEOUT 120)
      add_test(
        NAME keylane_redis_export_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/redis_export_e2e.sh
                $<TARGET_FILE:keylane>
                ${KEYLANE_REDIS_SERVER_EXECUTABLE}
                ${KEYLANE_REDIS_CLI_EXECUTABLE}
      )
      set_tests_properties(keylane_redis_export_e2e PROPERTIES
        TIMEOUT 120
        RUN_SERIAL TRUE)
      add_test(
        NAME keylane_redis_export_backpressure_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/redis_export_e2e.sh
                $<TARGET_FILE:keylane>
                ${KEYLANE_REDIS_SERVER_EXECUTABLE}
                ${KEYLANE_REDIS_CLI_EXECUTABLE}
                backpressure
      )
      set_tests_properties(keylane_redis_export_backpressure_e2e PROPERTIES
        TIMEOUT 120
        RUN_SERIAL TRUE)
      add_test(
        NAME keylane_rdb_backup_e2e
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/rdb_backup_e2e.sh
                $<TARGET_FILE:keylane>
                ${KEYLANE_REDIS_CLI_EXECUTABLE}
      )
      set_tests_properties(keylane_rdb_backup_e2e PROPERTIES
        TIMEOUT 180
        RUN_SERIAL TRUE)
    endif()

    add_executable(keylane_replication_log_e2e_test
      tests/replication_log_e2e_test.cpp
    )
    target_link_libraries(keylane_replication_log_e2e_test
      PRIVATE keylane::module
    )
    add_test(
      NAME keylane_replication_log_e2e
      COMMAND keylane_replication_log_e2e_test
    )
    set_tests_properties(keylane_replication_log_e2e PROPERTIES TIMEOUT 120)

    add_executable(keylane_replica_abort_reclaim_e2e_test
      tests/replica_abort_reclaim_e2e_test.cpp
    )
    target_link_libraries(keylane_replica_abort_reclaim_e2e_test
      PRIVATE keylane::module
    )
    add_test(
      NAME keylane_replica_abort_reclaim_e2e
      COMMAND keylane_replica_abort_reclaim_e2e_test
    )
    set_tests_properties(keylane_replica_abort_reclaim_e2e PROPERTIES
      TIMEOUT 180)

    add_executable(keylane_pubsub_e2e_test
      tests/pubsub_e2e_test.cpp
    )
    add_test(
      NAME keylane_pubsub_e2e
      COMMAND keylane_pubsub_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_pubsub_e2e PROPERTIES
      TIMEOUT 120
      RUN_SERIAL TRUE)

    find_program(KEYLANE_REDIS_SERVER_EXECUTABLE redis-server)
    if (KEYLANE_REDIS_SERVER_EXECUTABLE)
      set(KEYLANE_SENTINEL_SERVER_TARGET keylane)
      if (TARGET keylane_fault_server)
        set(KEYLANE_SENTINEL_SERVER_TARGET keylane_fault_server)
      endif()
      add_executable(keylane_sentinel_e2e_test
        tests/sentinel_e2e_test.cpp
      )
      target_compile_definitions(keylane_sentinel_e2e_test PRIVATE
        KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>)
      target_link_libraries(keylane_sentinel_e2e_test PRIVATE
        keylane_test_support
      )
      add_test(
        NAME keylane_sentinel_e2e
        COMMAND keylane_sentinel_e2e_test
                $<TARGET_FILE:${KEYLANE_SENTINEL_SERVER_TARGET}>
                ${KEYLANE_REDIS_SERVER_EXECUTABLE}
      )
      set_tests_properties(keylane_sentinel_e2e PROPERTIES
        LABELS cluster-integration
        TIMEOUT 180
        RUN_SERIAL TRUE)
    endif()

    add_executable(keylane_flushdb_reclaim_e2e_test
      tests/flushdb_reclaim_e2e_test.cpp
    )
    target_link_libraries(keylane_flushdb_reclaim_e2e_test
      PRIVATE keylane::module
    )
    add_test(
      NAME keylane_flushdb_reclaim_e2e
      COMMAND keylane_flushdb_reclaim_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_flushdb_reclaim_e2e PROPERTIES TIMEOUT 600)
    add_test(
      NAME keylane_paused_defrag_full_e2e
      COMMAND keylane_flushdb_reclaim_e2e_test $<TARGET_FILE:keylane>
              --paused-defrag-only
    )
    set_tests_properties(keylane_paused_defrag_full_e2e PROPERTIES TIMEOUT 120)
    add_test(
      NAME keylane_stale_block_recovery_e2e
      COMMAND keylane_flushdb_reclaim_e2e_test $<TARGET_FILE:keylane>
              --stale-header-only
    )
    set_tests_properties(keylane_stale_block_recovery_e2e PROPERTIES TIMEOUT 60)

    add_executable(keylane_tomb_raider_e2e_test
      tests/tomb_raider_e2e_test.cpp
    )
    target_link_libraries(keylane_tomb_raider_e2e_test
      PRIVATE keylane::module
    )
    add_test(
      NAME keylane_tomb_raider_e2e
      COMMAND keylane_tomb_raider_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_tomb_raider_e2e PROPERTIES TIMEOUT 120)

    add_executable(keylane_multikey_e2e_test
      tests/multikey_e2e_test.cpp
    )
    target_compile_definitions(keylane_multikey_e2e_test PRIVATE
      KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>)
    add_test(
      NAME keylane_multikey_e2e
      COMMAND keylane_multikey_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_multikey_e2e PROPERTIES TIMEOUT 300)

    add_executable(keylane_multi_exec_e2e_test
      tests/multi_exec_e2e_test.cpp
    )
    add_test(
      NAME keylane_multi_exec_e2e
      COMMAND keylane_multi_exec_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_multi_exec_e2e PROPERTIES TIMEOUT 180)

    add_executable(keylane_atomicity_stress_e2e_test
      tests/atomicity_stress_e2e_test.cpp
    )
    add_test(
      NAME keylane_atomicity_stress_e2e
      COMMAND keylane_atomicity_stress_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_atomicity_stress_e2e PROPERTIES TIMEOUT 150)

    add_executable(keylane_ttl_e2e_test
      tests/ttl_e2e_test.cpp
    )
    target_link_libraries(keylane_ttl_e2e_test
      PRIVATE keylane::module
    )
    add_test(
      NAME keylane_ttl_e2e
      COMMAND keylane_ttl_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_ttl_e2e PROPERTIES TIMEOUT 90)

    add_executable(keylane_metrics_e2e_test
      tests/metrics_e2e_test.cpp
    )
    target_link_libraries(keylane_metrics_e2e_test PRIVATE GTest::gtest)
    add_test(
      NAME keylane_metrics_e2e
      COMMAND keylane_metrics_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_metrics_e2e PROPERTIES TIMEOUT 120)

    add_executable(keylane_extent_recovery_e2e_test
      tests/extent_recovery_e2e_test.cpp
    )
    add_test(
      NAME keylane_extent_recovery_e2e
      COMMAND keylane_extent_recovery_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_extent_recovery_e2e PROPERTIES TIMEOUT 600)

    add_executable(keylane_grouped_recovery_e2e_test
      tests/grouped_recovery_e2e_test.cpp
    )
    target_link_libraries(keylane_grouped_recovery_e2e_test
      PRIVATE keylane::module GTest::gtest
    )
    target_compile_definitions(keylane_grouped_recovery_e2e_test PRIVATE
      KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME keylane_grouped_recovery_e2e
      COMMAND keylane_grouped_recovery_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_grouped_recovery_e2e PROPERTIES
      TIMEOUT 600 RUN_SERIAL TRUE
    )

    add_executable(keylane_grouped_hash_write_e2e_test
      tests/grouped_hash_write_e2e_test.cpp
    )
    target_link_libraries(keylane_grouped_hash_write_e2e_test
      PRIVATE keylane::module GTest::gtest
    )
    target_compile_definitions(keylane_grouped_hash_write_e2e_test PRIVATE
      KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME keylane_grouped_hash_write_e2e
      COMMAND keylane_grouped_hash_write_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_grouped_hash_write_e2e PROPERTIES
      TIMEOUT 240 RUN_SERIAL TRUE
    )

    add_executable(keylane_compact_collection_write_e2e_test
      tests/compact_collection_write_e2e_test.cpp
    )
    target_link_libraries(keylane_compact_collection_write_e2e_test
      PRIVATE keylane::module GTest::gtest
    )
    target_compile_definitions(keylane_compact_collection_write_e2e_test PRIVATE
      KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME keylane_compact_collection_write_e2e
      COMMAND keylane_compact_collection_write_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_compact_collection_write_e2e PROPERTIES
      TIMEOUT 240 RUN_SERIAL TRUE
    )

    add_executable(keylane_grouped_ordered_write_e2e_test
      tests/grouped_ordered_write_e2e_test.cpp
      tests/grouped_rdb_stream_e2e_test.cpp
      tests/grouped_zset_write_e2e_test.cpp
      tests/grouped_transfer_e2e_test.cpp
      tests/grouped_random_stream_e2e_test.cpp
    )
    target_link_libraries(keylane_grouped_ordered_write_e2e_test
      PRIVATE keylane::module GTest::gtest
    )
    target_compile_definitions(keylane_grouped_ordered_write_e2e_test PRIVATE
      KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME keylane_grouped_ordered_write_e2e
      COMMAND keylane_grouped_ordered_write_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_grouped_ordered_write_e2e PROPERTIES
      TIMEOUT 1200 RUN_SERIAL TRUE
    )

    add_executable(keylane_list_e2e_test
      tests/list_e2e_test.cpp
    )
    target_link_libraries(keylane_list_e2e_test
      PRIVATE keylane::module GTest::gtest
    )
    target_compile_definitions(keylane_list_e2e_test PRIVATE
      KEYLANE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
      KEYLANE_TEST_FAULTS_AVAILABLE=$<OR:$<CONFIG:Debug>,$<BOOL:${KEYLANE_BUILD_FAULT_SERVER}>>
    )
    add_test(
      NAME keylane_list_e2e
      COMMAND keylane_list_e2e_test $<TARGET_FILE:keylane>
              --gtest_filter=-LargeHashDurabilityE2eTest.*
    )
    add_test(
      NAME keylane_large_hash_durability_e2e
      COMMAND keylane_list_e2e_test $<TARGET_FILE:keylane>
              --gtest_filter=LargeHashDurabilityE2eTest.*
    )
    set_tests_properties(keylane_large_hash_durability_e2e PROPERTIES
      TIMEOUT 180 RUN_SERIAL TRUE)
    if (KEYLANE_ASAN_BUILD)
      set_tests_properties(keylane_list_e2e PROPERTIES TIMEOUT 1800)
    else()
      set_tests_properties(keylane_list_e2e PROPERTIES TIMEOUT 1200)
    endif()

    add_executable(keylane_cluster_e2e_test
      tests/cluster_e2e_test.cpp
    )
    target_link_libraries(keylane_cluster_e2e_test
      PRIVATE keylane::module GTest::gtest
    )
    # argv[2] carries the redis-cli path for the real-client MOVED-following
    # check; empty when the environment has no redis-cli, and the test skips
    # that check.
    add_test(
      NAME keylane_cluster_e2e
      COMMAND keylane_cluster_e2e_test $<TARGET_FILE:keylane>
              "${KEYLANE_REDIS_CLI_EXECUTABLE}"
    )
    set_tests_properties(keylane_cluster_e2e PROPERTIES
      TIMEOUT 300
      RUN_SERIAL TRUE)
  endif()
endif()
