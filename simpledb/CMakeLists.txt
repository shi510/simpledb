set(the_library simpledb)
set(simpledb_source_files)
set(simpledb_header_files)

file(GLOB_RECURSE files "*.*")
group_files("${files}")

add_subdirectory(core)
add_subdirectory(util)

include_directories(${PROJECT_SOURCE_DIR})
add_library(${the_library}
            ${LIB_TYPE}
            ${simpledb_source_files}
            ${simpledb_header_files}
            )
target_link_libraries(${the_library} ${simpledb_library_deps})
set_target_properties(${the_library} PROPERTIES FOLDER "simpledb_library")
