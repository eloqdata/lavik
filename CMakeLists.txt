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

set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
set(ABSL_BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil"
  "${CMAKE_CURRENT_BINARY_DIR}/third_party/abseil"
  EXCLUDE_FROM_ALL
)

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/celer/CMakeLists.txt")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/celer" "${CMAKE_CURRENT_BINARY_DIR}/celer")
else()
  message(FATAL_ERROR "celer submodule not found; run git submodule update --init --recursive")
endif()

add_library(keylane_module
  src/storage/buffer_pool.cpp
  src/storage/engine.cpp
  src/storage/format.cpp
  src/redis/resp.cpp
  src/redis/command.cpp
  src/redis/server.cpp
)

add_library(keylane::module ALIAS keylane_module)

target_include_directories(keylane_module
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  SYSTEM PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil
)

target_precompile_headers(keylane_module PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include/keylane/pch.h
)

target_compile_features(keylane_module PUBLIC cxx_std_23)
target_compile_definitions(keylane_module PRIVATE
  KEYLANE_ENABLE_READ_LATENCY_TRACE=$<BOOL:${KEYLANE_ENABLE_READ_LATENCY_TRACE}>
)
target_link_libraries(keylane_module PUBLIC
  celer::core
  absl::crc32c
  absl::flat_hash_map
)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(keylane_module PRIVATE -Wall -Wextra -Wpedantic)
  if (KEYLANE_ENABLE_OPT)
    target_compile_options(keylane_module PRIVATE -O3 -march=native -flto)
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
    target_compile_options(keylane PRIVATE -O3 -march=native -flto)
    target_link_options(keylane PRIVATE -flto)
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
endif()
