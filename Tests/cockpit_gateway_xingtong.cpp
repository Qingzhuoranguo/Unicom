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

struct rtk_msg {
    int status;
    int satellite_count;
    double hdop;        // Horizontal Dilution of Precision
    double MSL;    // sea level
    double GS;     // ground level  

    double latitude;   // Latitude in degrees
    double longitude;  // Longitude in degrees
    double velocity;    // Velocity in km/h
    double course;      // Course in degrees, North = 0, clockwise
    double heading;      // Heading in degrees， true North = 0, clockwise

    uint64_t timestamp;  // msg generation timestamp
};

void printAlive();

static int udp_init(const char *ip, uint16_t port);
static inline bool parse_udp_can_msg(const uint8_t *buf, size_t n, can_msg *m);

void printRTK ( rtk_msg const& rtk_msg_rc );

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
    std::string RTK_DATA_TOPIC;
    std::string CAN_DATA_SAHRE_TOPIC;
    std::string HEARTBEAT_TOPIC;
    std::unordered_set<uint32_t> ALLOWED_IDS;

    try {
        LISTEN_IP   = configs.at("service").at("wan").at("listen_ip").get<std::string>();
        LISTEN_PORT = configs.at("service").at("wan").at("listen_port").get<uint16_t>();
        DEBUG_FLAG  = configs.at("service").at("debug_flag").get<bool>();

        const auto& topics = configs.at("service").at("lan").at("message_system_topics");
        if (topics.size() < 3) {
            std::cerr << "config error: message_system_topics must contain at least 3 entries" << std::endl;
            return -1;
        }
        RTK_DATA_TOPIC       = topics.at(0).get<std::string>();
        CAN_DATA_SAHRE_TOPIC = topics.at(1).get<std::string>();
        HEARTBEAT_TOPIC      = topics.at(2).get<std::string>();

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
    memset(buf, 0, sizeof(buf));

    can_msg msg_rc;
    rtk_msg rtk_msg_rc;

    bool gga_ready = false, rmc_ready = false, hdt_ready = false;

    // start service
    while (true) {
        ssize_t n = recv(udp_fd, buf, sizeof(buf), 0);
        if (n < 0) {
            std::cerr << "recv() error: " << strerror(errno) << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        if (n == 0) continue;

        if ( buf[0] == '$' ) {
            static auto parse_rtk_msg = [](const char *buf, size_t n) -> std::vector<std::string> {
                std::vector<std::string> fields;

                std::stringstream ss(std::string(buf, n));
                std::string field;

                while (std::getline(ss, field, ',')) fields.push_back(field);

                return fields;
            };

            static auto nmeaChecksum = [](const uint8_t *buf, size_t n) -> bool {
                if (buf == nullptr || n < 4) return false; // 至少 "$*HH"

                // 找 '$'
                size_t start = 0;
                while (start < n && buf[start] != static_cast<uint8_t>('$')) ++start;
                if (start >= n) return false;

                // 找 '*'
                size_t star = start + 1;
                while (star < n && buf[star] != static_cast<uint8_t>('*')) ++star;
                if (star >= n) return false;

                // '*' 后至少两位十六进制
                if (star + 2 >= n) return false;

                auto hexVal = [](uint8_t c) -> int {
                    if (c >= static_cast<uint8_t>('0') && c <= static_cast<uint8_t>('9')) return c - static_cast<uint8_t>('0');
                    if (c >= static_cast<uint8_t>('A') && c <= static_cast<uint8_t>('F')) return c - static_cast<uint8_t>('A') + 10;
                    if (c >= static_cast<uint8_t>('a') && c <= static_cast<uint8_t>('f')) return c - static_cast<uint8_t>('a') + 10;
                    return -1;
                };

                int hi = hexVal(buf[star + 1]);
                int lo = hexVal(buf[star + 2]);
                if (hi < 0 || lo < 0) return false;

                uint8_t expected = static_cast<uint8_t>((hi << 4) | lo);

                // 计算 '$' 与 '*' 之间异或
                uint8_t calc = 0;
                for (size_t i = start + 1; i < star; ++i) {
                    calc ^= buf[i];
                }

                return calc == expected;
            };

            if ( !nmeaChecksum(buf, n) ) {
                std::cerr << "NMEA checksum failed" << std::endl;
                continue;
            }

            auto fields = parse_rtk_msg(reinterpret_cast<const char*>(buf), static_cast<size_t>(n));

            if (fields.empty()) continue;
            
            if (fields[0] == "$GPGGA") {
                rtk_msg_rc.status = std::stoi(fields[6]);
                rtk_msg_rc.satellite_count = std::stoi(fields[7]);
                rtk_msg_rc.hdop = std::stod(fields[8]);
                rtk_msg_rc.MSL = std::stod(fields[9]);
                rtk_msg_rc.GS = std::stod(fields[11]);

                gga_ready = true;
            }else if (fields[0] == "$GPRMC") {
                static auto nmea_to_degree = [](const std::string &nmea){
                    double v = std::stod(nmea);
                    
                    int deg = static_cast<int>(v / 100.0);
                    double min = v - deg * 100.0;
                    
                    return deg + min / 60.0;
                };

                rtk_msg_rc.latitude = nmea_to_degree(fields[3]);
                rtk_msg_rc.longitude = nmea_to_degree(fields[5]);
                if (fields[4] == "S") rtk_msg_rc.latitude = -rtk_msg_rc.latitude;
                if (fields[6] == "W") rtk_msg_rc.longitude = -rtk_msg_rc.longitude;

                rtk_msg_rc.velocity =  std::stod(fields[7]) * 1.852;   // knot -> km/h
                rtk_msg_rc.course = std::stod(fields[8]);

                rmc_ready = true;
            }else if ( fields[0] == "$GNHDT") {
                rtk_msg_rc.heading = std::stod(fields[1]);
                hdt_ready = true;

            }else {
                // unknown RTK message type
            }   
            
            if (gga_ready && rmc_ready && hdt_ready) {
                rtk_msg_rc.timestamp = static_cast<uint64_t>(std::time(nullptr));

                msgSys->publish(RTK_DATA_TOPIC, &rtk_msg_rc, sizeof(rtk_msg), MessagePriority::Normal);
                gga_ready = false;
                rmc_ready = false;
                hdt_ready = false;

                if ( DEBUG_FLAG ){
                    printRTK( rtk_msg_rc );
                }
            }    


        } else {  // process CAN msg 
            if (parse_udp_can_msg(buf, static_cast<size_t>(n), &msg_rc)) {
                // process the received CAN message
                if (DEBUG_FLAG) {
                    print_can_msg_live(msg_rc);
                    std::cout.flush();
                }

                // filtering: 0x000 = allow all, otherwise normal filtering
                if (!ALLOWED_IDS.count(0x000) && !ALLOWED_IDS.count(msg_rc.id)) continue;

                // publish CAN message directly, no need to manually build a Message packet
                msgSys->publish(CAN_DATA_SAHRE_TOPIC, &msg_rc, sizeof(can_msg), MessagePriority::Normal);
            }
        }
        memset(buf, 0, sizeof(buf));     
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
    // 最小长度：1字节 header + 4字节 ID = 5字节
    if (n < 5) return false;

    // 1. 解析 header byte
    uint8_t header = buf[0];
    uint8_t ff  = (header >> 7) & 0x01;
    uint8_t rtr = (header >> 6) & 0x01;
    uint8_t dlc = header & 0x0F;

    if (dlc > 8) return false;  // DLC 非法，丢弃

    m->len = dlc;

    // 2. CAN ID（大端序）
    m->id =  ((uint32_t)buf[1] << 24)
           | ((uint32_t)buf[2] << 16)
           | ((uint32_t)buf[3] <<  8)
           |  (uint32_t)buf[4];

    // 3. DATA：清零后按实际可用长度拷贝
    memset(m->data, 0, 8);

    size_t available = (n > 5) ? (n - 5) : 0;
    size_t copy_len  = (dlc < available) ? dlc : available;
    if (copy_len > 8) copy_len = 8;

    memcpy(m->data, buf + 5, copy_len);

    // 4. flags：存 ff / rtr
    m->flags = 0;
    if (ff)  m->flags |= 0x01;
    if (rtr) m->flags |= 0x02;

    // 5. 时间戳
    auto now = std::chrono::system_clock::now();
    m->timestamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count());

    return true;
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

void printRTK ( rtk_msg const& rtk_msg_rc ) { 

    std::cout << "========== RTK ==========" << std::endl;

    std::cout << "status          : " << rtk_msg_rc.status << std::endl;
    std::cout << "satellite_count : " << rtk_msg_rc.satellite_count << std::endl;

    std::cout << "hdop            : "
            << std::fixed << std::setprecision(2)
            << rtk_msg_rc.hdop << std::endl;

    std::cout << "MSL             : "
            << std::fixed << std::setprecision(4)
            << rtk_msg_rc.MSL << " m" << std::endl;
    std::cout << "GS              : "
            << std::fixed << std::setprecision(4)
            << rtk_msg_rc.GS << " m" << std::endl;  

    std::cout << "latitude        : "
            << std::fixed << std::setprecision(8)
            << rtk_msg_rc.latitude << std::endl;

    std::cout << "longitude       : "
            << std::fixed << std::setprecision(8)
            << rtk_msg_rc.longitude << std::endl;

    std::cout << "velocity        : "
            << std::fixed << std::setprecision(3)
            << rtk_msg_rc.velocity << " km/h" << std::endl;

    std::cout << "course          : "
            << std::fixed << std::setprecision(1)
            << rtk_msg_rc.course << " deg" << std::endl;

    std::cout << "heading         : "
            << std::fixed << std::setprecision(1)
            << rtk_msg_rc.heading << " deg" << std::endl;

    std::time_t ts = static_cast<std::time_t>(rtk_msg_rc.timestamp);
    std::tm tm_local{};
    localtime_r(&ts, &tm_local); 

    std::cout << "timestamp       : "
            << std::put_time(&tm_local, "%Y-%m-%d %H:%M:%S")
            << std::endl;


}
