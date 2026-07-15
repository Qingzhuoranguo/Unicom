#include <iostream>
#include <cstring>
#include <chrono>
#include <thread>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <cstdlib>

int main() {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        std::cerr << "socket() failed\n";
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(40001);
    // addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_addr.s_addr = inet_addr("192.168.0.150");

    uint8_t buf[13];

    // ======== 1. 你自己填 CAN ID 列表 ========
    std::vector<uint32_t> ids = {
        0x181, 0x281, 0x381, 0x481,
        0x191, 0x291, 0x391
    };

    while (true) {
        for (uint32_t can_id : ids) {

            uint8_t dlc = 8;

            // ======== 2. 随机数据 ========
            uint8_t data[8];
            for (int i = 0; i < 8; i++)
                data[i] = rand() % 256;

            buf[0] = dlc;

            // CAN ID 小端序
            buf[1] = (can_id & 0xFF);
            buf[2] = (can_id >> 8) & 0xFF;
            buf[3] = (can_id >> 16) & 0xFF;
            buf[4] = (can_id >> 24) & 0xFF;

            memcpy(buf + 5, data, 8);

            sendto(fd, buf, sizeof(buf), 0,
                   (struct sockaddr*)&addr, sizeof(addr));

            std::cout << "Sent CAN frame: ID=0x"
                      << std::hex << can_id << std::dec
                      << " DATA=[";
            for (int i = 0; i < 8; i++) {
                std::cout << (int)data[i];
                if (i < 7) std::cout << ",";
            }
            std::cout << "]\n";

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    close(fd);
    return 0;
}
