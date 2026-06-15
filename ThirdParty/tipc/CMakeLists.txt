add_library(tipc
    libtipc.c
)


target_include_directories(tipc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)
