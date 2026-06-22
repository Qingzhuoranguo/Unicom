#include <iostream>
#include <unistd.h>

#include "message_system.h"

#define NUM_MESSAGES 100

int main () {
    MessageSystem msgSys;


    for ( int i = 0; i < NUM_MESSAGES; ++i ) {
        std::string message = "this is the message number " + std::to_string(i);

        msgSys.publish("test_topic", message.c_str(), message.size());
        sleep(1); // Sleep for a bit to allow the subscriber to process messages
    }

    return 0;
}