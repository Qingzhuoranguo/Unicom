#pragma once

#include <string_view>
#include <vector>
#include <cstdint>

using TopicID = uint64_t;

struct Message{
    TopicID topic;
    std::vector<uint8_t> data;
};

class MessageSystem{
public:
    MessageSystem();
    ~MessageSystem();
    
    bool publish( std::string_view topic, const void* data, size_t size);

    bool subscribe( std::string_view topic);

    bool receive( Message& msg, int timeout_ms = -1);

private:
    
    int m_socket;
    
};