#include "message_system.h"

#include <iostream>
#include <iomanip>
#include <map>
#include <sstream>
#include <ctime>
#include <cstring>

struct can_msg {
    uint32_t id;        // CAN ID
    uint8_t data[8];    // Data bytes
    uint8_t len;        // Length of data
    uint8_t flags;      // Flags for the message
    uint64_t timestamp; // Message timestamp
};

void print_can_msg_live(const can_msg& msg);


std::string format_timestamp(uint64_t ts_ms) ;

int main () {
    std::cout << "Starting cockpit client receiver..." << std::endl;
    
    MessageSystem msgSys;
    TopicID id = msgSys.subscribe( "cockpit_gateway.CAN_PROXY");
    if (id == 0) {
        std::cerr << "subscribe failed!" << std::endl;
        return 1;
    }

    while (true) {
        MessagePtr msg;
        if (msgSys.receive(msg, 2000)) {   // 2s timeout
            can_msg m;
            std::memcpy(&m, msg->payload, sizeof(can_msg));
            
            print_can_msg_live(m);

            // pass to handler
        } else {
            std::cout << "[2s timeout, no message]" << std::endl;
        }

    }



    return 0;
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