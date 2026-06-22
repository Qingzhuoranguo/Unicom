#include <iostream>

#include "message_system.h"



int main () {
    MessageSystem msgSys;

    msgSys.subscribe("test_topic");

    while (true) {
        Message msg;
        if (msgSys.receive(msg)) {
            std::string received_msg(msg.data.begin(), msg.data.end());
            std::cout << "Received message on topic " << msg.topic << ": " << received_msg << std::endl;
        }
    }


    return 0;
}