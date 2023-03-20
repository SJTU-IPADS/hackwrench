cmake_minimum_required(VERSION 3.2)
project(QueryPlaner)

## C++ flags
ADD_DEFINITIONS(-std=c++0x)
set(CMAKE_CXX_FLAGS "-O2 -g -Wno-deprecated-declarations -Wreturn-type")

## Source files
include_directories("./src/")
file(GLOB_RECURSE SRC "src/*.cc")

## Executable files
add_executable(analyze ${SRC} "src/main.cpp" )
target_compile_options(analyze PRIVATE "-DBENCH_TPCC")
