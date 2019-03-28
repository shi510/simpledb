cmake_minimum_required(VERSION 2.8)

project(simpledb CXX)

set(CMAKE_CXX_STANDARD 11)
set(simpledb_include_dirs ${PROJECT_SOURCE_DIR})
set(simpledb_library_deps)
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

include(cmake/util.cmake)

if(UNIX)
  option(BUILD_SHARED_LIBS "Build Shared Libraries" OFF)
endif()

set(LIB_TYPE STATIC)
if(BUILD_SHARED_LIBS)
    set(LIB_TYPE SHARED)
endif()

add_subdirectory(simpledb)
