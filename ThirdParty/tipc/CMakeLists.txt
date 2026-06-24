add_library(tipc SHARED
    libtipc.c
)


target_include_directories(tipc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)
