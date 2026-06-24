# utils/CMakeLists.txt

add_library(utils SHARED
    hash.cpp
    rng.cpp
)

target_include_directories(utils
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)
