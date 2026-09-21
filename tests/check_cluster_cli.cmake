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

if(NOT DEFINED LAVIK_EXECUTABLE)
  message(FATAL_ERROR "LAVIK_EXECUTABLE is required")
endif()

execute_process(
  COMMAND "${LAVIK_EXECUTABLE}" --help
  RESULT_VARIABLE help_result
  OUTPUT_VARIABLE help_stdout
  ERROR_VARIABLE help_stderr
)
set(help_text "${help_stdout}${help_stderr}")
if(NOT help_result EQUAL 0)
  message(FATAL_ERROR "lavik --help failed: ${help_text}")
endif()
foreach(required IN ITEMS "--node-id" "--meta-seed")
  string(FIND "${help_text}" "${required}" found)
  if(found EQUAL -1)
    message(FATAL_ERROR "lavik --help omitted ${required}")
  endif()
endforeach()

foreach(removed IN ITEMS "--cluster-enabled" "--cluster-node-id" "--cluster-meta-seed"
                         "--cluster-announce-ip" "--cluster-announce-port"
                         "--cluster-announce-tls-port")
  execute_process(COMMAND "${LAVIK_EXECUTABLE}" "${removed}" yes
    RESULT_VARIABLE result OUTPUT_VARIABLE stdout ERROR_VARIABLE stderr)
  if(result EQUAL 0 OR NOT "${stdout}${stderr}" MATCHES "not expected|unknown|unrecognized")
    message(FATAL_ERROR "removed option was not rejected: ${removed}: ${stdout}${stderr}")
  endif()
endforeach()

foreach(removed IN ITEMS "--client-mode" "--meta-managed")
  execute_process(COMMAND "${LAVIK_EXECUTABLE}" "${removed}" yes
    RESULT_VARIABLE result OUTPUT_VARIABLE stdout ERROR_VARIABLE stderr)
  if(result EQUAL 0 OR NOT "${stdout}${stderr}" MATCHES "removed|Meta")
    message(FATAL_ERROR "missing mode migration hint: ${removed}: ${stdout}${stderr}")
  endif()
endforeach()
