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

void print_can_msg_live(const can_msg& msg);
void printRTK ( rtk_msg const& rtk_msg_rc );

std::string format_timestamp(uint64_t ts_ms) ;

int main ( int argc, char* argv[]) {
    std::cout << "Starting cockpit client receiver..." << std::endl;
    
    bool flag = true;
    if (argc == 2) {
        if ( argv[1] == std::string("CAN")) {
            flag = true;
        } else if ( argv[1] == std::string("RTK")) {
            flag = false;
        }else {

        }
    }else {
        std::cout << "Usage: " << argv[0] << " <CAN|RTK>" << std::endl;
    }

    MessageSystem msgSys;
    TopicID id = msgSys.subscribe( "cockpit_gateway.CAN_PROXY");
    if (id == 0) {
        std::cerr << "subscribe failed!" << std::endl;
        return 1;
    }

    TopicID rtk_id = msgSys.subscribe( "cockpit_gateway.RTK_DATA");
    if (rtk_id == 0) {
        std::cerr << "subscribe failed!" << std::endl;
        return 1;
    }

    while (true) {
        MessagePtr msg;
        if (msgSys.receive(msg, 2000)) {   // 2s timeout
            if (msg->topic == id) {
                can_msg m;
                std::memcpy(&m, msg->payload, sizeof(can_msg));
                
                if (flag) print_can_msg_live(m);
            }

            // pass to handler
            if (msg->topic == rtk_id) {
                rtk_msg rtk;
                std::memcpy(&rtk, msg->payload, sizeof(rtk_msg));
                if (!flag) printRTK(rtk);
            }
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
