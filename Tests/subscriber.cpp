#include <iostream>

#include "message_system.h"

int main () {
    MessageSystem msgSys;

    msgSys.subscribe("test_topic");

    while (true) {
        MessagePtr msg;
        if (msgSys.receive(msg)) {
            std::string received_msg(msg->payload, msg->size);
            std::cout << "Received message on topic " << msg->topic << ": " << received_msg << std::endl;
        }
    }

    return 0;
}