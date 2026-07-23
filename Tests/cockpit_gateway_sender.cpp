#include <iostream>
#include <cstring>
#include <chrono>
#include <thread>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <cstdlib>
#include <ctime>

int main(int argc, char* argv[]) {
    // ======== 命令行参数解析 ========
    // 用法: ./can_sender <ip>  [interval_seconds]
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <ip_address> [byte6_interval_seconds]\n";
        return -1;
    }
    const char* ip = argv[1];

    // N：byte6 最小变化间隔（秒）；-1 表示不限制
    int byte6_interval = -1;
    if (argc >= 3) {
        byte6_interval = std::atoi(argv[2]);
        if (byte6_interval < 0) {
            std::cerr << "interval_seconds must be >= 0\n";
            return -1;
        }
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        std::cerr << "socket() failed\n";
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(40001);
    addr.sin_addr.s_addr = inet_addr(ip);

    uint8_t buf[13];  // 1 + 4 + 8 = 13 bytes

    std::vector<uint32_t> ids = {
        0x181, 0x281, 0x381, 0x481,
        0x191, 0x291, 0x391
    };

    srand((unsigned)time(nullptr));

    // ======== 0x181 byte6 的"锁定"状态 ========
    uint8_t locked_byte6      = rand() % 256;
    auto    last_byte6_change = std::chrono::steady_clock::now();

    while (true) {
        for (uint32_t can_id : ids) {

            uint8_t dlc = 8;
            uint8_t data[8];
            for (int i = 0; i < 8; i++)
                data[i] = rand() % 256;

            // ======== 特殊处理 0x181 的 byte6 ========
            if (can_id == 0x181) {
                bool allow_change = false;

                if (byte6_interval < 0) {
                    // 没有传 N：每次都随机，不限制
                    allow_change = true;
                } else {
                    // 传了 N：距上次变化须满 N 秒
                    auto now     = std::chrono::steady_clock::now();
                    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                       now - last_byte6_change).count();
                    if (elapsed >= byte6_interval) {
                        allow_change      = true;
                        last_byte6_change = now;
                    }
                }

                if (allow_change) {
                    uint8_t new_val = rand() % 256;
                    while (new_val == locked_byte6)
                        new_val = rand() % 256;
                    locked_byte6 = new_val;
                }

                data[6] = locked_byte6;
            }

            // ======== 组帧 ========
            uint8_t ff  = 0;
            uint8_t rtr = 0;
            buf[0] = (ff << 7) | (rtr << 6) | (dlc & 0x0F);

            buf[1] = (can_id >> 24) & 0xFF;
            buf[2] = (can_id >> 16) & 0xFF;
            buf[3] = (can_id >>  8) & 0xFF;
            buf[4] =  can_id        & 0xFF;

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
            if (can_id == 0x181)
                std::cout << "]  <byte6(locked)=" << (int)locked_byte6 << ">";
            else
                std::cout << "]";
            std::cout << "\n";

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    close(fd);
    return 0;
}