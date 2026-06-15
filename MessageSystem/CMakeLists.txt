cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 17)

project(MessageSystem)


add_library(message_system
    src/message_system.cpp
)

target_include_directories(message_system
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
