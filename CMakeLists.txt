cmake_minimum_required(VERSION 3.20)

project(keylane VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KEYLANE_BUILD_EXAMPLE "Build keylane example binary" ON)
option(KEYLANE_ENABLE_WERROR "Treat warnings as errors" OFF)

set(CELER_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/celer/CMakeLists.txt")
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/celer" "${CMAKE_CURRENT_BINARY_DIR}/celer")
else()
  message(FATAL_ERROR "celer submodule not found; run git submodule update --init --recursive")
endif()

add_library(keylane_module
  src/redis/resp.cpp
  src/redis/db.cpp
  src/redis/command.cpp
  src/redis/server.cpp
)

add_library(keylane::module ALIAS keylane_module)

target_include_directories(keylane_module
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(keylane_module PUBLIC cxx_std_23)
target_link_libraries(keylane_module PUBLIC celer::core)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(keylane_module PRIVATE -Wall -Wextra -Wpedantic)
  if (KEYLANE_ENABLE_WERROR)
    target_compile_options(keylane_module PRIVATE -Werror)
  endif()
endif()

if (KEYLANE_BUILD_EXAMPLE)
  add_executable(keylane
    app/keylane.cpp
  )
  target_link_libraries(keylane PRIVATE keylane::module)
endif()
