#pragma once

#include <string_view>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <mutex>

using TopicID = uint64_t;

struct Message{
    TopicID topic = 0;
    std::vector<uint8_t> data;
};

class MessageSystem{
public:
    MessageSystem();
    ~MessageSystem();

    bool publish(std::string_view topic, const void* data, size_t size);
    TopicID subscribe(std::string_view topic);
    bool unsubscribe(TopicID topic);     
    bool receive(Message& msg);      

private:
    static constexpr size_t MAX_MSG_SIZE = 4096;

    int m_pubSocket{-1};
    std::unordered_map<TopicID, int> m_topic_to_socket;

    mutable std::mutex m_mutex;          
};