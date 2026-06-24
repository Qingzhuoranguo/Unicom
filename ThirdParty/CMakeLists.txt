# ThirdParty/CMakeLists.txt
add_subdirectory(utils)
add_subdirectory(tipc)
add_subdirectory(nlohmann)
add_subdirectory(threadpool)


add_library(ThirdParty INTERFACE)

target_include_directories(ThirdParty
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(ThirdParty
    INTERFACE
        utils
        tipc
        nlohmann
        threadpool
)