cmake_minimum_required(VERSION 3.20)

project(celer_redis VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(CELER_REDIS_BUILD_EXAMPLE "Build celer_redis example binary" ON)
option(CELER_REDIS_ENABLE_WERROR "Treat warnings as errors" OFF)

set(CELER_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/celer/CMakeLists.txt")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/celer" "${CMAKE_CURRENT_BINARY_DIR}/celer")
else()
  message(FATAL_ERROR "celer submodule not found; run git submodule update --init --recursive")
endif()

add_library(celer_redis_module
  src/redis/resp.cpp
  src/redis/db.cpp
  src/redis/command.cpp
  src/redis/server.cpp
)

add_library(celer_redis::module ALIAS celer_redis_module)

target_include_directories(celer_redis_module
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(celer_redis_module PUBLIC cxx_std_23)
target_link_libraries(celer_redis_module PUBLIC celer::core)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(celer_redis_module PRIVATE -Wall -Wextra -Wpedantic)
  if (CELER_REDIS_ENABLE_WERROR)
    target_compile_options(celer_redis_module PRIVATE -Werror)
  endif()
endif()

if (CELER_REDIS_BUILD_EXAMPLE)
  add_executable(celer_redis
    app/celer_redis.cpp
  )
  target_link_libraries(celer_redis PRIVATE celer_redis::module)
endif()
