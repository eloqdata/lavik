if(NOT DEFINED KEYLANE_EXECUTABLE)
  message(FATAL_ERROR "KEYLANE_EXECUTABLE is required")
endif()

execute_process(
  COMMAND "${KEYLANE_EXECUTABLE}" --help
  RESULT_VARIABLE help_result
  OUTPUT_VARIABLE help_stdout
  ERROR_VARIABLE help_stderr
)
set(help_text "${help_stdout}${help_stderr}")
if(NOT help_result EQUAL 0)
  message(FATAL_ERROR "keylane --help failed: ${help_text}")
endif()
foreach(required IN ITEMS "--cluster-node-id" "--cluster-meta-seed")
  string(FIND "${help_text}" "${required}" found)
  if(found EQUAL -1)
    message(FATAL_ERROR "keylane --help omitted ${required}")
  endif()
endforeach()
