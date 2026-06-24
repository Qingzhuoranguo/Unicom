add_library(Component SHARED
    src/component.cpp
)

target_include_directories(Component
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(Component
    PUBLIC
        MessageSystem
        ThirdParty
)