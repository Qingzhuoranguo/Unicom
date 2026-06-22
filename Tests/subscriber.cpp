#include <iostream>

#include "message_system.h"

int main () {
    MessageSystem msgSys;

    msgSys.subscribe("test_topic");

    while (true) {
        MessagePtr msg;
        if (msgSys.receive(msg)) {
            std::string received_msg(msg->payload, msg->size);
            std::cout << "topic: " << msg->topic 
                    << "\nmessage: " << received_msg
                    << "\npriority: " << static_cast<int>(msg->priority) 
                    << std::endl;
        }
    }

    return 0;
}