# ~/Unicom/Tests/CMakeLists.txt

add_executable(publisher
    publisher.cpp
)

target_link_libraries(publisher
    PRIVATE
        MessageSystem
)




add_executable(subscriber
    subscriber.cpp
)

target_link_libraries(subscriber
    PRIVATE
        MessageSystem
)





add_executable(module
    module.cpp
)

target_link_libraries(module
    PRIVATE
        Component
        MessageSystem
)



add_executable(threadpooltest
    threadpooltest.cpp
)

target_link_libraries(threadpooltest
    PUBLIC
        ThirdParty
)



add_executable(cockpit_gateway
    cockpit_gateway.cpp
)

target_link_libraries(cockpit_gateway
    PRIVATE
        MessageSystem
        nlohmann
)


add_executable(cockpit_gateway_sender
    cockpit_gateway_sender.cpp
)

target_link_libraries(cockpit_gateway_sender
    PRIVATE
        ThirdParty
)


add_executable(cockpit_client_reciever
    cockpit_client_reciver.cpp
)

target_link_libraries(cockpit_client_reciever
    PRIVATE
        MessageSystem
)
