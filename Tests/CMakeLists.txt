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