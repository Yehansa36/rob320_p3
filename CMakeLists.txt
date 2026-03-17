cmake_minimum_required(VERSION 3.11 FATAL_ERROR)
set(CMAKE_CXX_STANDARD 20)

project(pub-sub)

# Used to find GTest on CAEN
list(APPEND CMAKE_PREFIX_PATH "~/.local")

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

# Used to compile on Autograder (possibly unnecessary ...)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT DEFINED ARCH)
    # If we do not pre-define our architecture, run this command to infer it
    EXECUTE_PROCESS(COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCH)
    # Normalize name
    if(ARCH STREQUAL "arm64")
        set(ARCH "aarch64")
    endif()
endif()

if(NOT DEFINED OS)
    # If we do not pre-define our operating system, infer it
    if(APPLE)
        set(OS "macos")
    elseif(UNIX)
        set(OS "linux")
    else()
        message(FATAL_ERROR "Unsupported OS.")
    endif()
endif()

message(STATUS "Architecture: ${ARCH}")
message(STATUS "Operating System: ${OS}")

# Compile the Project 3 library
add_library(project3 
    src/rix/core/node.cpp
    src/rix/core/publisher.cpp
    src/rix/core/subscriber.cpp
    src/rix/core/timer.cpp
    src/rix/core/mediator.cpp
)
target_include_directories(project3 PRIVATE include/)
target_link_libraries(project3 PUBLIC Threads::Threads)

# Link against the proper Project 2 library
target_link_libraries(project3 PUBLIC ${CMAKE_SOURCE_DIR}/lib/${OS}-${ARCH}/libproject2.a) 
target_link_libraries(project3 PUBLIC ${CMAKE_SOURCE_DIR}/lib/${OS}-${ARCH}/libproject1.a)

# Compile rixhub
add_executable(rixhub src/rixhub.cpp)
target_link_libraries(rixhub PRIVATE project3)
target_include_directories(rixhub PRIVATE include/)

# Compile simple_publisher
add_executable(simple_publisher src/simple_publisher.cpp)
target_link_libraries(simple_publisher PRIVATE project3)
target_include_directories(simple_publisher PRIVATE include/)

# Compile simple_subscriber
add_executable(simple_subscriber src/simple_subscriber.cpp)
target_link_libraries(simple_subscriber PRIVATE project3)
target_include_directories(simple_subscriber PRIVATE include/)

# Compile subscriber test
add_executable(rix_test tests/rix_test.cpp)
target_link_libraries(rix_test PRIVATE project3 GTest::gtest_main GTest::gmock)
target_include_directories(rix_test PRIVATE include/)
