cmake_minimum_required(VERSION 3.0)
project(gcl)

option(gcl_build_tests "Build gcl tests." OFF)

if (MSVC)
    add_compile_options(/std:c++14 /W4 /bigobj /EHsc /wd4503 /wd4996 /wd4702 /wd4100 /wd4706)
else()
    add_compile_options(-std=c++14 -pedantic -Wall -Wextra -Wconversion)
    if (CMAKE_COMPILER_IS_GNUCC)
        add_compile_options(-pthread)
    endif()
endif()

find_package(Threads)

add_library(gcl src/gcl.h src/gcl.cpp)
if (MSVC)
else()
    target_link_libraries(gcl ${CMAKE_THREAD_LIBS_INIT})
endif()

set_target_properties(gcl PROPERTIES PUBLIC_HEADER src/gcl.h)
install(TARGETS gcl ARCHIVE DESTINATION lib LIBRARY DESTINATION lib PUBLIC_HEADER DESTINATION include)

if (gcl_build_tests)
    enable_testing()
    include_directories(src)
    add_executable(gcl_test src/gcl.h test/catch.hpp test/test.cpp)
    target_link_libraries(gcl_test gcl)
    add_test(gcl_test gcl_test --use-colour no)
endif()
