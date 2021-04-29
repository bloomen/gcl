cmake_minimum_required(VERSION 3.0)
project(gcl)

option(gcl_build_tests "Build gcl tests." OFF)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_EXTENSIONS OFF)

if (MSVC)
    add_compile_options(/W4 /bigobj /EHsc /wd4503 /wd4996 /wd4702 /wd4100 /wd4706)
else()
    add_compile_options(-Wall -Wconversion -Wextra -Wpedantic)
    if (CMAKE_COMPILER_IS_GNUCC)
        add_compile_options(-pthread)
    endif()
endif()

find_package(Threads)

add_library(gcl include/gcl.h src/gcl.cpp)
target_include_directories(gcl PRIVATE include)
if (MSVC)
else()
    target_link_libraries(gcl ${CMAKE_THREAD_LIBS_INIT})
endif()

set_target_properties(gcl PROPERTIES PUBLIC_HEADER include/gcl.h)
install(TARGETS gcl ARCHIVE DESTINATION lib LIBRARY DESTINATION lib PUBLIC_HEADER DESTINATION include)

if (gcl_build_tests)
    enable_testing()
    add_executable(gcl_test include/gcl.h test/catch.hpp test/test.cpp)
    target_include_directories(gcl_test PRIVATE include)
    target_link_libraries(gcl_test gcl)
    add_test(gcl_test gcl_test --use-colour no)
endif()
