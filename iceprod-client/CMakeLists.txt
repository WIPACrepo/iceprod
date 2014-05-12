#
# 
#
i3_project(iceprod-client
	DEPENDS iceprod-core
)

add_custom_target(iceprod-client)
add_dependencies(iceprod-client iceprod-core)

file(GLOB ipclient ${CMAKE_CURRENT_SOURCE_DIR}/lib/iceprod/client/*.py)
file(GLOB ipclientbin ${CMAKE_CURRENT_SOURCE_DIR}/bin/*.py ${CMAKE_CURRENT_SOURCE_DIR}/bin/*iceprod* )

install( FILES ${ipclient} DESTINATION lib/iceprod/client)
install( FILES ${ipclientbin} DESTINATION bin )

foreach(pyfile ${ipclient} )
  string(REGEX REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/lib" "${LIBRARY_OUTPUT_PATH}" PYTARGET "${pyfile}")
  string(REGEX REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/lib" "py" PYTEMPBUILDTARGET "${pyfile}")
  string(REGEX REPLACE "/" "_" PYBUILDTARGET "${PYTEMPBUILDTARGET}")
  add_custom_command(OUTPUT ${PYTARGET}
	DEPENDS ${pyfile}
    COMMAND mkdir -p `dirname ${PYTARGET}`
	COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/bin/pycompile.py -o `dirname ${PYTARGET}` -O -v ${pyfile}
  )
  add_custom_target(${PYBUILDTARGET}
    DEPENDS ${PYTARGET}
  )
  add_dependencies(iceprod-client ${PYBUILDTARGET})
endforeach(pyfile ${ipclient} )

foreach(pyfile ${ipclientbin})
  string(REGEX REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/bin" "${EXECUTABLE_OUTPUT_PATH}" PYTARGET "${pyfile}")
  string(REGEX REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/bin" "py" PYTEMPBUILDTARGET "${pyfile}")
  string(REGEX REPLACE "/" "_" PYBUILDTARGET "${PYTEMPBUILDTARGET}")
  add_custom_command(OUTPUT ${PYTARGET}
	DEPENDS ${pyfile}
	COMMAND cp ${pyfile} ${EXECUTABLE_OUTPUT_PATH}
  )
  add_custom_target(${PYBUILDTARGET}
    DEPENDS ${PYTARGET}
  )
  add_dependencies(iceprod-client ${PYBUILDTARGET})
endforeach(pyfile ${ipclientbin})

