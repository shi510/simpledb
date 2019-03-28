if(BUILD_TEST)
    set(catch_include_dirs ${PROJECT_SOURCE_DIR}/third_party/Catch2/single_include)
    list(APPEND simpledb_include_dirs ${catch_include_dirs})
endif()
