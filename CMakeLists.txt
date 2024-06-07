cmake_minimum_required(VERSION 3.10)
project(IndexProject)
set(CMAKE_CXX_STANDARD 20)

# # 添加 GoogleTest 和 GoogleMock
# include(FetchContent)
# FetchContent_Declare(
#   googletest
#   URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
# )
# # For Windows: Prevent overriding the parent project's compiler/linker settings
# set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
# FetchContent_MakeAvailable(googletest)
add_subdirectory(lib/googletest)

# 直接在根目录下添加可执行文件和库
set(BINARY ${CMAKE_PROJECT_NAME})
file(GLOB_RECURSE SOURCES LIST_DIRECTORIES true "${CMAKE_SOURCE_DIR}/*.h" "${CMAKE_SOURCE_DIR}/*.cpp" "${CMAKE_SOURCE_DIR}/*.cc")
# Exclude the googletest directory
list(FILTER SRC_FILES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/googletest/*")
list(FILTER SOURCES EXCLUDE REGEX ".*CMakeCXXCompilerId.cpp$")
list(FILTER SOURCES EXCLUDE REGEX ".*tst/main.cpp$")  # 排除测试的main.cpp

# 打印找到的源文件列表
# message(STATUS "Source files: ${SOURCES}")

set(SOURCES ${SOURCES})

# 设置目标属性以确保CMake知道链接语言
add_executable(${BINARY}_run ${SOURCES})
set_target_properties(${BINARY}_run PROPERTIES LINKER_LANGUAGE CXX)

add_library(${BINARY}_lib STATIC ${SOURCES})
set_target_properties(${BINARY}_lib PROPERTIES LINKER_LANGUAGE CXX)

# 添加测试子目录
add_subdirectory(tst)
