cmake_minimum_required(VERSION 3.0)
project(gcl)
enable_testing()

include_directories(src)

find_package(Threads)

add_library(gcl src/gcl.h src/gcl.cpp)
set_target_properties(gcl PROPERTIES PUBLIC_HEADER src/gcl.h)
install(TARGETS gcl ARCHIVE DESTINATION lib LIBRARY DESTINATION lib PUBLIC_HEADER DESTINATION include)

add_executable(gcl_test src/gcl.h test/catch.hpp test/test.cpp)
target_link_libraries(gcl_test gcl)

add_test(gcl_test gcl_test --use-colour no)

if (MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /std:c++14 /W4 /bigobj /EHsc /wd4503 /wd4996 /wd4702 /wd4100")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -pedantic -Wall -Wextra -Wconversion")
    if (CMAKE_COMPILER_IS_GNUCC)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
    endif()
    target_link_libraries(gcl ${CMAKE_THREAD_LIBS_INIT})
    target_link_libraries(gcl_test ${CMAKE_THREAD_LIBS_INIT})
endif()
