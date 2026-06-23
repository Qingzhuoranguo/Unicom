#include "message_system.h"

#include <iostream>
#include <string>
#include <thread>
#include <signal.h>

MessageSystem *msgSys = nullptr;

void sigint_handler (int signum)
{
    if ( msgSys) delete msgSys;
}

int main()
{
    signal(SIGINT, sigint_handler);

    msgSys = new MessageSystem();

    TopicID id = msgSys->subscribe("test_topic", ChannelType::ReliableFast);
    std::cout << "subscribed, TopicID=" << id << std::endl;
    if (id == 0) {
        std::cerr << "subscribe failed!" << std::endl;
        return 1;
    }
    
    std::thread worker ( [&](){
        while (msgSys->isRunning()) {
            MessagePtr msg;
            if (msgSys->receive(msg, 2000)) {   // 2s 超时，方便看到没消息的情况
                std::string received_msg(msg->payload, msg->size);
                std::cout << "topic: "    << msg->topic
                          << "\nmessage: " << received_msg
                          << "\npriority: "<< static_cast<int>(msg->priority)
                          << std::endl;
            } else {
                std::cout << "[2s timeout, no message]" << std::endl;
            }
        }
    } );



    worker.join();

    return 0;
}