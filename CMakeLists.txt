cmake_minimum_required(VERSION 3.20)

project(keylane VERSION 0.1.0 LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KEYLANE_BUILD_EXAMPLE "Build keylane example binary" ON)
option(KEYLANE_ENABLE_WERROR "Treat warnings as errors" OFF)
option(KEYLANE_ENABLE_OPT "Enable aggressive optimization" ON)
option(KEYLANE_ENABLE_READ_LATENCY_TRACE "Enable detailed GET latency tracing" OFF)

set(CELER_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/celer/CMakeLists.txt")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/celer" "${CMAKE_CURRENT_BINARY_DIR}/celer")
else()
  message(FATAL_ERROR "celer submodule not found; run git submodule update --init --recursive")
endif()

add_library(keylane_module
  src/storage/buffer_pool.cpp
  src/storage/engine.cpp
  src/storage/format.cpp
  src/replication/replication.cpp
  src/redis/resp.cpp
  src/redis/command.cpp
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
)
target_link_libraries(keylane_module PUBLIC
  celer::core
  celer::rpc
  absl::crc32c
  absl::flat_hash_map
  absl::inlined_vector
)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(keylane_module PRIVATE -Wall -Wextra -Wpedantic)
  if (KEYLANE_ENABLE_OPT)
    target_compile_options(keylane_module PRIVATE -O3 -march=native -flto=auto)
  endif()
  if (KEYLANE_ENABLE_WERROR)
    target_compile_options(keylane_module PRIVATE -Werror)
  endif()
endif()

if (KEYLANE_BUILD_EXAMPLE)
  add_executable(keylane
    app/keylane.cpp
  )
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU" AND KEYLANE_ENABLE_OPT)
    target_compile_options(keylane PRIVATE -O3 -march=native -flto=auto)
    target_link_options(keylane PRIVATE -flto=auto)
  endif()
  target_precompile_headers(keylane REUSE_FROM keylane_module)
  target_link_libraries(keylane PRIVATE keylane::module)
endif()

if (BUILD_TESTING)
  add_executable(keylane_scan_hash_map_test
    tests/scan_hash_map_test.cpp
  )
  target_link_libraries(keylane_scan_hash_map_test PRIVATE keylane::module)
  add_test(NAME keylane_scan_hash_map COMMAND keylane_scan_hash_map_test)

  add_executable(keylane_command_table_test
    tests/command_table_test.cpp
  )
  target_link_libraries(keylane_command_table_test PRIVATE keylane::module)
  add_test(NAME keylane_command_table COMMAND keylane_command_table_test)

  add_executable(keylane_tx_lock_test
    tests/tx_lock_test.cpp
  )
  target_link_libraries(keylane_tx_lock_test PRIVATE keylane::module)
  add_test(NAME keylane_tx_lock COMMAND keylane_tx_lock_test)

  add_executable(keylane_storage_format_test
    tests/storage_format_test.cpp
  )
  target_link_libraries(keylane_storage_format_test PRIVATE keylane::module)
  add_test(NAME keylane_storage_format COMMAND keylane_storage_format_test)

  add_executable(keylane_storage_capacity_test
    tests/storage_capacity_test.cpp
  )
  target_link_libraries(keylane_storage_capacity_test PRIVATE keylane::module)
  add_test(NAME keylane_storage_capacity COMMAND keylane_storage_capacity_test)

  if (KEYLANE_BUILD_EXAMPLE AND UNIX)
    add_executable(keylane_flushdb_reclaim_e2e_test
      tests/flushdb_reclaim_e2e_test.cpp
    )
    add_test(
      NAME keylane_flushdb_reclaim_e2e
      COMMAND keylane_flushdb_reclaim_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_flushdb_reclaim_e2e PROPERTIES TIMEOUT 150)

    add_executable(keylane_multikey_e2e_test
      tests/multikey_e2e_test.cpp
    )
    add_test(
      NAME keylane_multikey_e2e
      COMMAND keylane_multikey_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_multikey_e2e PROPERTIES TIMEOUT 120)

    add_executable(keylane_ttl_e2e_test
      tests/ttl_e2e_test.cpp
    )
    add_test(
      NAME keylane_ttl_e2e
      COMMAND keylane_ttl_e2e_test $<TARGET_FILE:keylane>
    )
    set_tests_properties(keylane_ttl_e2e PROPERTIES TIMEOUT 90)
  endif()
endif()
