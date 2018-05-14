cmake_minimum_required(VERSION 3.0)
project(ExternelSort)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -pthread -fopenmp")
set(SOURCE_FILES externalSort.cpp)
add_executable(ExternelSort ${SOURCE_FILES})
