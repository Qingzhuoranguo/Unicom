#include "message_system.h"
#include "nlohmann/json.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <string>
#include <string_view>
#include <thread>
#include <chrono>
#include <map>
#include <unordered_set>
#include <cstring>
#include <cstdlib>

#include <stdint.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

using json = nlohmann::json;

struct can_msg {
    uint32_t id;        // CAN ID
    uint8_t data[8];    // Data bytes
    uint8_t len;        // Length of data
    uint8_t flags;      // Flags for the message
    uint64_t timestamp; // Message timestamp
};

static int udp_init(const char *ip, uint16_t port);
static inline bool parse_udp_can_msg(const uint8_t *buf, size_t n, can_msg *m);

void print_can_msg_live(const can_msg& msg);
std::string format_timestamp(uint64_t ts_ms);


int main(int argc, char* argv[]) {
    std::cout << "[cockpit_gateway] Starting..." << std::endl;

    // load configs
    std::string config_path;
    if (argc < 2) {
        config_path = "/home/cockpit/configs/cockpit_gateway.config";
    } else {
        config_path = argv[1];
    }

    std::ifstream f(config_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open config file: " << config_path << std::endl;
        return -1;
    }
    json configs = json::parse(f);

    /* ==========================================
     *             setup parameters
     * ========================================== */
    std::string LISTEN_IP;
    uint16_t LISTEN_PORT;
    bool DEBUG_FLAG;
    std::string CAN_DATA_SAHRE_TOPIC;
    std::string HEARTBEAT_TOPIC;
    std::unordered_set<uint32_t> ALLOWED_IDS;

    try {
        LISTEN_IP   = configs.at("service").at("wan").at("listen_ip").get<std::string>();
        LISTEN_PORT = configs.at("service").at("wan").at("listen_port").get<uint16_t>();
        DEBUG_FLAG  = configs.at("service").at("debug_flag").get<bool>();

        const auto& topics = configs.at("service").at("lan").at("message_system_topics");
        if (topics.size() < 2) {
            std::cerr << "config error: message_system_topics must contain at least 2 entries" << std::endl;
            return -1;
        }
        CAN_DATA_SAHRE_TOPIC = topics.at(0).get<std::string>();
        HEARTBEAT_TOPIC      = topics.at(1).get<std::string>();

        const auto& ids = configs.at("service").at("allowed_ids");
        for (const auto& s : ids) {
            uint32_t id = std::stoul(s.get<std::string>(), nullptr, 16);
            ALLOWED_IDS.insert(id);
        }

    } catch (const std::exception& e) {
        std::cerr << "Failed to parse config: " << e.what() << std::endl;
        return -1;
    }

    

    // setup Message system
    auto msgSys = new MessageSystem();

    // setup UDP interface
    int udp_fd = udp_init(LISTEN_IP.c_str(), LISTEN_PORT);
    if (udp_fd < 0) {
        std::cerr << "Failed to initialize UDP socket" << std::endl;
        return -1;
    }

    uint8_t buf[1024];
    can_msg msg_rc;

    // start service: rcv from UDP interface, then redistribute to MessageSystem
    while (true) {
        ssize_t n = recv(udp_fd, buf, sizeof(buf), 0);

        if (n < 0) {
            std::cerr << "recv() error: " << strerror(errno) << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        if (n == 0) continue;

        if (parse_udp_can_msg(buf, static_cast<size_t>(n), &msg_rc)) {
            // process the received CAN message
            if (DEBUG_FLAG) {
                print_can_msg_live(msg_rc);
            }

            // filtering: 0x000 = allow all, otherwise normal filtering
            if (!ALLOWED_IDS.count(0x000) && !ALLOWED_IDS.count(msg_rc.id)) continue;

            // publish CAN message directly, no need to manually build a Message packet
            msgSys->publish(CAN_DATA_SAHRE_TOPIC, &msg_rc, sizeof(can_msg), MessagePriority::Normal);
        }
    }

    return 0;
}


static int udp_init(const char *ip, uint16_t port) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);

    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        std::cerr << "Invalid IP address: " << ip << std::endl;
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static inline bool parse_udp_can_msg(const uint8_t *buf, size_t n, can_msg *m) {
    if (n < 13) return false;

    // 1. DLC
    m->len = buf[0];
    if (m->len > 8) {
        // DLC 超出合法范围，丢弃该帧
        return false;
    }

    // 2. CAN ID（小端）
    m->id =  (uint32_t)buf[1]
           | ((uint32_t)buf[2] << 8)
           | ((uint32_t)buf[3] << 16)
           | ((uint32_t)buf[4] << 24);

    // 3. DATA（固定 8 字节）
    memcpy(m->data, buf + 5, 8);

    m->flags = 0;

    // 使用实际接收时间作为时间戳（毫秒）
    auto now = std::chrono::system_clock::now();
    m->timestamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count());

    return true;
}


void print_can_msg_live(const can_msg& msg){
    static std::map<uint32_t, can_msg> table;

    table[msg.id] = msg;

    std::cout << "\033[2J\033[H";
    std::cout << "CAN msg received:\n";

    for (const auto& [id, m] : table) {

        std::cout << "  ID  : 0x"
                  << std::uppercase << std::setfill('0') << std::setw(8)
                  << std::hex << id << std::dec << "\t";

        std::cout << "  DLC : " << static_cast<int>(m.len) << "\t";
        std::cout << "  TS  : " << format_timestamp(m.timestamp) << "\n";

        std::cout << "  DATA: ";
        for (int i = 0; i < 8; ++i) {
            std::cout << "0x"
                      << std::uppercase << std::setw(2) << std::setfill('0')
                      << std::hex << static_cast<int>(m.data[i]) << std::dec;

            if (i + 1 < 8)
                std::cout << " ";
        }

        std::cout << "\n";
    }

    std::cout.flush();
}


std::string format_timestamp(uint64_t ts_ms) {
    std::time_t t = static_cast<std::time_t>(ts_ms / 1000);

    uint16_t ms = static_cast<uint16_t>(ts_ms % 1000);
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif

    std::ostringstream oss;
    oss << std::setfill('0')
        << std::setw(2) << (tm.tm_year % 100) << "-"   // yy
        << std::setw(2) << (tm.tm_mon + 1) << "-"      // mm
        << std::setw(2) << tm.tm_mday << "-"           // dd
        << std::setw(2) << tm.tm_hour << "-"           // hh
        << std::setw(2) << tm.tm_min << "-"            // mm
        << std::setw(2) << tm.tm_sec << "-"            // ss
        << std::setw(3) << ms;                         // ms

    return oss.str();
}