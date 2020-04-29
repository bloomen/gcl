cmake_minimum_required(VERSION 2.8)
project(gcl)
enable_testing()

include_directories(examples)

find_package(Threads)

set(SOURCES
	src/gcl.h
	src/test.h
	src/test.cpp
	src/test_Task.cpp
	)

add_executable(gcl_test ${SOURCES})

add_test(gcl_test transwarp_test --use-colour no)

if (MSVC)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /std:c++14 /W4 /bigobj /EHsc /wd4503 /wd4996 /wd4702 /wd4100")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -pedantic -Wall -Wextra -Wconversion")
	if (CMAKE_COMPILER_IS_GNUCC)
		set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
	endif()
	target_link_libraries(gcl_test ${CMAKE_THREAD_LIBS_INIT})
endif()
