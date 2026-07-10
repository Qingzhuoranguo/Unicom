#include "message_system.h"
#include "nlomann/json.hpp"

#include <iostream>
#include <string>
#include <thread>
#include <signal.h>

constexpr std::string CAN_DATA_SAHRE_TOPIC = "cockpit_gateway.CAN_PROXY";
constexpr std::string HEARTBEAT_TOPIC = "cockpit_gateway.HEARTBEAT";


struct can_msg {
    uint32_t id;        // CAN ID
    uint8_t data[8];    // Data bytes
    uint8_t len;        // Length of data
    uint8_t flags;      // Flags for the message
    uint64_t timestamp; // Message timestamp    
};

int main(int argc, char* argv[]) {
    std::cout << "[cockpit_gateway] Starting..." << std::endl;

    // load configs
    std::string config_path;
    if (argc < 2) {
        config_path = "cockpit_gateway.config";
    } else {
        config_path = argv[1];
    }

    std::ifstream f(config_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open config file: " << config_path << std::endl;
        return -1;
    }
    json configs = json::parse(f);

    // setup parameters, getting ready
    std::string listen_ip;
    int listen_port;
    std::vector<uint32_t>  allowed_ids;
    
    // setup Message system
    auto msgSys = new Messagesystem();

    can_msg msg;

    msgSys->publish(
        CAN_DATA_SAHRE_TOPIC,
        &msg,
        sizeof(can_msg)
    );



    msgSys = new MessageSystem();


    return 0;
}