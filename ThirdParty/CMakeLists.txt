# ThirdParty/CMakeLists.txt

add_subdirectory(utils)
add_subdirectory(tipc)

target_include_directories(utils
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}   
)

target_include_directories(tipc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}   
)
