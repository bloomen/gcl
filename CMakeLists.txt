cmake_minimum_required(VERSION 3.0)
project(gcl)

option(gcl_build_tests "Build gcl tests." OFF)
option(gcl_enable_asan "Build gcl with address sanitizer." OFF)
option(gcl_enable_tsan "Build gcl with thread sanitizer." OFF)
option(gcl_enable_coverage "Build gcl with coverage reporting." OFF)

if(gcl_enable_asan AND gcl_enable_tsan)
    message(FATAL_ERROR "gcl_enable_asan and gcl_enable_tsan cannot both be ON")
endif()

function(gcl_add_flags target)
    set_property(TARGET ${target} PROPERTY CXX_STANDARD 14)
    set_property(TARGET ${target} PROPERTY CXX_EXTENSIONS OFF)
    target_include_directories(${target} PRIVATE include)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /bigobj /EHsc /wd4503 /wd4996 /wd4702 /wd4100 /wd4706)
        if(${MSVC_VERSION} GREATER_EQUAL 1929)
            if(gcl_enable_asan)
                target_compile_options(${target} PRIVATE /fsanitize=address)
            endif()
        endif()
    else()
        target_compile_options(${target} PRIVATE -Wall -Wconversion -Wextra -Wpedantic)
        target_link_libraries(${target} ${CMAKE_THREAD_LIBS_INIT})
        if(CMAKE_COMPILER_IS_GNUCC)
            target_compile_options(${target} PRIVATE -pthread)
        endif()
        if(gcl_enable_asan)
            if(APPLE)
                target_compile_options(${target} PRIVATE -fsanitize=address,undefined)
                set_target_properties(${target} PROPERTIES LINK_FLAGS "-fsanitize=address,undefined")
            else()
                target_compile_options(${target} PRIVATE -fsanitize=address,leak,undefined)
                set_target_properties(${target} PROPERTIES LINK_FLAGS "-fsanitize=address,leak,undefined")
            endif()
        endif()
        if(gcl_enable_tsan)
            target_compile_options(${target} PRIVATE -fsanitize=thread)
            set_target_properties(${target} PROPERTIES LINK_FLAGS "-fsanitize=thread")
        endif()
        if(gcl_enable_coverage)
            target_compile_options(${target} PRIVATE --coverage)
            set_target_properties(${target} PROPERTIES LINK_FLAGS "--coverage")
        endif()
    endif()
endfunction()

find_package(Threads)

add_library(gcl include/gcl.h src/gcl.cpp)
gcl_add_flags(gcl)

set_target_properties(gcl PROPERTIES PUBLIC_HEADER include/gcl.h)
install(TARGETS gcl ARCHIVE DESTINATION lib LIBRARY DESTINATION lib PUBLIC_HEADER DESTINATION include)

if(gcl_build_tests)
    enable_testing()
    add_executable(gcl_test include/gcl.h test/catch_amalgamated.hpp test/catch_amalgamated.cpp test/test.cpp)
    gcl_add_flags(gcl_test)
    target_link_libraries(gcl_test gcl)
    add_executable(gcl_example include/gcl.h test/example.cpp)
    gcl_add_flags(gcl_example)
    target_link_libraries(gcl_example gcl)
    add_test(gcl_test gcl_test)
endif()

set(gcl_source_files
    ${PROJECT_SOURCE_DIR}/include/gcl.h
    ${PROJECT_SOURCE_DIR}/src/gcl.cpp
    ${PROJECT_SOURCE_DIR}/test/test.cpp)

add_custom_target(
    gcl_format
    COMMAND clang-format
    -style=file
    -i
    ${gcl_source_files}
)
