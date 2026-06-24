# ~/MessageSystem/CMakeLists.txt

add_library(MessageSystem SHARED
    src/message_system.cpp
)
target_include_directories(MessageSystem
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(MessageSystem
    PUBLIC ThirdParty
)