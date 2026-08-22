cmake_minimum_required(VERSION 3.20)

project(keylane VERSION 0.1.0 LANGUAGES CXX)

option(BUILD_TESTING "Build keylane tests" OFF)
include(CTest)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KEYLANE_ENABLE_WERROR "Treat warnings as errors" OFF)
option(KEYLANE_ENABLE_OPT "Enable aggressive optimization" ON)
option(KEYLANE_STATIC_OPENSSL "Link OpenSSL statically" ON)
option(KEYLANE_STATIC_CXX_RUNTIME
  "Link the GNU C++ and compiler runtimes statically" OFF)
set(KEYLANE_MARCH "native" CACHE STRING
  "Value passed to -march for optimized local builds (empty disables it)")
option(KEYLANE_ENABLE_READ_LATENCY_TRACE "Enable detailed GET latency tracing" OFF)
option(KEYLANE_ENABLE_SET_LATENCY_TRACE "Enable detailed SET latency tracing" OFF)
option(KEYLANE_WITH_SPDK "Use the SPDK userspace NVMe storage backend" OFF)

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
find_program(KEYLANE_CLANG_FORMAT NAMES clang-format-18 clang-format)
file(GLOB_RECURSE KEYLANE_FORMAT_FILES CONFIGURE_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/app/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/keylane/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp
)
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
  message(STATUS "clang-format not found; format targets are disabled")
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

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/celer/CMakeLists.txt")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/celer" "${CMAKE_CURRENT_BINARY_DIR}/celer")
else()
  message(FATAL_ERROR "celer submodule not found; run git submodule update --init --recursive")
endif()

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

add_library(keylane_module
  src/config.cpp
  src/storage/buffer_pool.cpp
  src/storage/engine.cpp
  src/storage/engine/alloc.cpp
  src/storage/engine/backup.cpp
  src/storage/engine/compact_api.cpp
  src/storage/engine/defrag.cpp
  src/storage/engine/expire.cpp
  src/storage/engine/flush.cpp
  src/storage/engine/flush_db.cpp
  src/storage/engine/hash_api.cpp
  src/storage/engine/hash_codec.cpp
  src/storage/engine/hash_tree.cpp
  src/storage/engine/init.cpp
  src/storage/engine/list_api.cpp
  src/storage/engine/list_tree.cpp
  src/storage/engine/metrics.cpp
  src/storage/engine/read.cpp
  src/storage/engine/recovery.cpp
  src/storage/engine/replication.cpp
  src/storage/engine/replication_log.cpp
  src/storage/engine/tomb_raider.cpp
  src/storage/engine/tx_cleaner.cpp
  src/storage/engine/write.cpp
  src/storage/format.cpp
  src/memory.cpp
  src/metrics.cpp
  src/replication/command.cpp
  src/replication/replication.cpp
  src/redis/resp.cpp
  src/redis/rdb.cpp
  src/redis/backup.cpp
  src/redis/blocking_wait.cpp
  src/redis/command.cpp
  src/redis/hash_command.cpp
  src/redis/list_command.cpp
  src/redis/set_command.cpp
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
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_precompile_headers(keylane_module PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include/keylane/pch.h
)

target_compile_features(keylane_module PUBLIC cxx_std_23)
target_compile_definitions(keylane_module PUBLIC
  KEYLANE_ENABLE_READ_LATENCY_TRACE=$<BOOL:${KEYLANE_ENABLE_READ_LATENCY_TRACE}>
  KEYLANE_ENABLE_SET_LATENCY_TRACE=$<BOOL:${KEYLANE_ENABLE_SET_LATENCY_TRACE}>
)
target_link_libraries(keylane_module PUBLIC
  celer::core
  celer::rpc
  absl::crc32c
  absl::flat_hash_map
  absl::inlined_vector
  absl::status
  absl::statusor
  absl::strings
  mimalloc-static
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

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  if (KEYLANE_ENABLE_OPT)
    set(KEYLANE_OPT_COMPILE_OPTIONS -O3 -flto=auto)
    if (NOT KEYLANE_MARCH STREQUAL "")
      list(APPEND KEYLANE_OPT_COMPILE_OPTIONS "-march=${KEYLANE_MARCH}")
    endif()
    # Celer is compiled as part of Keylane, so local native builds should tune
    # the transport and runtime too, not only the Redis/storage layer.
    foreach(target IN ITEMS celer celer_rpc keylane_module keylane)
      target_compile_options(${target} PRIVATE ${KEYLANE_OPT_COMPILE_OPTIONS})
    endforeach()
    if (NOT KEYLANE_MARCH STREQUAL "")
      target_compile_options(mimalloc-static PRIVATE "-march=${KEYLANE_MARCH}")
    endif()
    target_link_options(keylane PRIVATE -flto=auto)
  endif()
  if (KEYLANE_STATIC_CXX_RUNTIME)
    target_link_options(keylane PRIVATE -static-libstdc++ -static-libgcc)
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

if (BUILD_TESTING)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG 52eb8108c5bdec04579160ae17225d66034bd723 # v1.17.0
  )
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  add_executable(keylane_unit_tests
    tests/buffer_pool_test.cpp
    tests/config_test.cpp
    tests/cross_core_active_sender_test.cpp
    tests/resp_test.cpp
    tests/scan_hash_map_test.cpp
    tests/command_table_test.cpp
    tests/random_sample_test.cpp
    tests/rdb_test.cpp
    tests/replication_command_test.cpp
    tests/ring_buffer_test.cpp
    tests/tx_lock_test.cpp
    tests/storage_format_test.cpp
    tests/storage_capacity_test.cpp
    tests/tx_cleaner_test.cpp
  )
  target_link_libraries(keylane_unit_tests
    PRIVATE keylane::module GTest::gtest_main
  )
  include(GoogleTest)
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
    set_tests_properties(keylane_multi_exec_e2e PROPERTIES TIMEOUT 120)

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

    add_executable(keylane_list_e2e_test
      tests/list_e2e_test.cpp
    )
    target_link_libraries(keylane_list_e2e_test
      PRIVATE keylane::module GTest::gtest
    )
    target_compile_definitions(keylane_list_e2e_test PRIVATE
      KEYLANE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
    )
    add_test(
      NAME keylane_list_e2e
      COMMAND keylane_list_e2e_test $<TARGET_FILE:keylane>
    )
    if (KEYLANE_ASAN_BUILD)
      set_tests_properties(keylane_list_e2e PROPERTIES TIMEOUT 900)
    else()
      set_tests_properties(keylane_list_e2e PROPERTIES TIMEOUT 300)
    endif()
  endif()
endif()
