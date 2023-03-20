cmake_minimum_required(VERSION 3.2)
project(TransactionRepair)

if(DEFINED ENV{LOG_LEVEL})
    add_definitions(-DLOG_LEVEL=$ENV{LOG_LEVEL})
    message("setting LOG_LEVEL to $ENV{LOG_LEVEL}")
endif()

# set C++ flags
ADD_DEFINITIONS(-std=c++14)
set(CMAKE_CXX_FLAGS "-O2 -g -Wall -Wformat-truncation=0")
# following is used for AddressSanitizer
# set(CMAKE_CXX_FLAGS "-fsanitize=address -fno-omit-frame-pointer -fsanitize-recover=address -O2 -g -Wall -Wformat-truncation=0")

# set the executable binary to build directory
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/")

# link_libraries(profiler pthread rt)
link_libraries(pthread rt)

add_subdirectory(src) 
