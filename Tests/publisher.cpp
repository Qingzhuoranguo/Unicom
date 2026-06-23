#include "message_system.h"

#include <iostream>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <random>

#define NUM_MESSAGES 10000

int main(int argc, char* argv[])
{
    // 1) 读取命令行参数
    std::string topic = "test_topic";
    if (argc > 1) {
        topic = argv[1];
    }

    MessageSystem msgSys;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(
        0, static_cast<int>(MessagePriority::Count) - 1
    );

    int i = 0;
    while (true) {
        ++i;
        std::string message = "test msg: " + std::to_string(i);

        MessagePriority pr = static_cast<MessagePriority>(dist(gen));

        bool ok = msgSys.publish(topic,
                                 message.c_str(),
                                 message.size(),
                                 pr,
                                 ChannelType::ReliableFast);

        std::cout << "publish[" << i << "]"
                  << " topic=" << topic
                  << " priority=" << static_cast<int>(pr)
                  << " => " << (ok ? "ok" : "FAILED")
                  << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    return 0;
}
