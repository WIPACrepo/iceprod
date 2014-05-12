#
# 
#
i3_project(iceprod-modules)

add_custom_target(iceprod-modules
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} &&
        python setup.py install
            --install-lib ${CMAKE_BINARY_DIR}/lib 
            --install-scripts ${CMAKE_BINARY_DIR}/bin 
            --install-data ${CMAKE_BINARY_DIR} 
            -O2
)

file(GLOB ipmodules ${CMAKE_CURRENT_SOURCE_DIR}/lib/iceprod/modules/*.py)
install(FILES ${ipmodules} DESTINATION lib/iceprod/modules)
