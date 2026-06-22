#include "message_system.h"

#include "utils/hash.h"
#include "tipc/tipcc.h"

#include <linux/tipc.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <string>
#include <cstring>

#define TIPC_SOCKET_TYPE SOCK_DGRAM

struct TIPCAddr{
    uint32_t type;
    uint32_t instance;

    explicit TIPCAddr(uint64_t topic_id = 0)
        : type(static_cast<uint32_t>(topic_id >> 32)),
          instance(static_cast<uint32_t>(topic_id & 0xFFFFFFFF)) {}
};

MessageSystem::MessageSystem() {
    m_pubSocket = tipc_socket(TIPC_SOCKET_TYPE);
}

MessageSystem::~MessageSystem() {
    std::lock_guard<std::mutex> lk(m_mutex);

    if (m_pubSocket >= 0) {
        tipc_close(m_pubSocket);
        m_pubSocket = -1;
    }

    for (auto& kv : m_topic_to_socket) {
        if (kv.second >= 0) tipc_close(kv.second);
    }
    m_topic_to_socket.clear();
}

bool MessageSystem::publish(std::string_view topic, const void* data, size_t size)
{
    if (m_pubSocket < 0 || data == nullptr || size == 0 || size > MAX_MSG_SIZE)
        return false;

    const uint64_t topic_id = hash::hash64(std::string(topic));
    const uint32_t type = static_cast<uint32_t>(topic_id >> 32);
    const uint32_t instance = static_cast<uint32_t>(topic_id & 0xFFFFFFFFu);

    sockaddr_tipc dst{};
    dst.family = AF_TIPC;
    dst.addrtype = TIPC_ADDR_MCAST;            
    dst.scope = TIPC_CLUSTER_SCOPE;
    dst.addr.nameseq.type = type;
    dst.addr.nameseq.lower = instance;
    dst.addr.nameseq.upper = instance;   

    const ssize_t n = ::sendto(
        m_pubSocket,
        data,
        size,
        0,
        reinterpret_cast<sockaddr*>(&dst),
        sizeof(dst)
    );

    return n == static_cast<ssize_t>(size);
}

TopicID MessageSystem::subscribe(std::string_view topic) {
    const uint64_t topic_id = hash::hash64(std::string(topic));
    TIPCAddr addr(topic_id);

    std::lock_guard<std::mutex> lk(m_mutex);

    // 防重复订阅
    if (auto it = m_topic_to_socket.find(topic_id); it != m_topic_to_socket.end()) {
        return topic_id;
    }

    int subSocket = tipc_socket(TIPC_SOCKET_TYPE);
    if (subSocket < 0) return 0;

    tipc_sock_non_block(subSocket);

    if (tipc_bind(subSocket, addr.type, addr.instance, addr.instance, TIPC_CLUSTER_SCOPE) < 0) {
        tipc_close(subSocket);
        return 0;
    }

    m_topic_to_socket[topic_id] = subSocket;
    return topic_id;
}

bool MessageSystem::unsubscribe(TopicID topic) {
    std::lock_guard<std::mutex> lk(m_mutex);
    auto it = m_topic_to_socket.find(topic);
    if (it == m_topic_to_socket.end()) return false;

    if (it->second >= 0) tipc_close(it->second);
    m_topic_to_socket.erase(it);
    return true;
}

bool MessageSystem::receive(MessagePtr& msg) {
    std::lock_guard<std::mutex> lk(m_mutex);

    std::array<uint8_t, MAX_MSG_SIZE> buf{};

    for (const auto& [topic_id, socket] : m_topic_to_socket) {
        struct tipc_addr src{};
        int err = 0;

        ssize_t recv_size = tipc_recvfrom(
            socket,
            buf.data(),
            buf.size(),
            &src,
            nullptr,
            &err
        );

        if (recv_size > 0) {
            // 分配 Message 内存
            Message* raw_msg = static_cast<Message*>(std::malloc(sizeof(Message) + recv_size));
            if (!raw_msg) return false;
            
            raw_msg->topic = topic_id;
            raw_msg->size = static_cast<int>(recv_size);
            std::memcpy(raw_msg->payload, buf.data(), recv_size);
            
            // 使用智能指针管理
            msg.reset(raw_msg);
            return true;
        }
    }

    return false;
}