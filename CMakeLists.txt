cmake_minimum_required(VERSION 3.20)

project(Unicom LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(ThirdParty)
add_subdirectory(MessageSystem)
add_subdirectory(Tests)