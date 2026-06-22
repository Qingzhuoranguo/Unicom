#include "message_system.h"

#include "utils/hash.h"
#include "tipc/tipcc.h"

#include <linux/tipc.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <unistd.h>

#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <chrono>
#include <iostream>
#include <vector>

// ═════════════════════════════════════════════════════════════
//  内部工具
// ═════════════════════════════════════════════════════════════

static constexpr size_t MAX_MSG_SIZE = 65536;

struct TIPCAddr {
    uint32_t type;
    uint32_t instance;

    explicit TIPCAddr(TopicID topic_id)
        : type    (static_cast<uint32_t>(topic_id >> 32))
        , instance(static_cast<uint32_t>(topic_id & 0xFFFFFFFFu))
    {}
};

static MessagePtr allocMessage(TopicID topic, MessagePriority priority,
                               const void* data, size_t size)
{
    void* raw = std::malloc(sizeof(Message) + size);
    if (!raw) return nullptr;
    auto* msg     = static_cast<Message*>(raw);
    msg->topic    = topic;
    msg->priority = priority;
    msg->size     = static_cast<int>(size);
    std::memcpy(msg->payload, data, size);
    return MessagePtr(msg);
}

// ═════════════════════════════════════════════════════════════
//  PriorityMailbox
// ═════════════════════════════════════════════════════════════

PriorityMailbox::PriorityMailbox(PriorityMailbox&& other) noexcept
    : m_queue       (std::move(other.m_queue))
    , m_maxSize     (other.m_maxSize)
    , m_policy      (other.m_policy)
    , m_droppedCount(other.m_droppedCount)
{}

PriorityMailbox& PriorityMailbox::operator=(PriorityMailbox&& other) noexcept
{
    if (this != &other) {
        m_queue        = std::move(other.m_queue);
        m_maxSize      = other.m_maxSize;
        m_policy       = other.m_policy;
        m_droppedCount = other.m_droppedCount;
    }
    return *this;
}

PriorityMailbox::PriorityMailbox(size_t maxSize, OverflowPolicy policy)
    : m_maxSize(maxSize)
    , m_policy(policy)
{}

bool PriorityMailbox::push(MessagePtr msg)
{
    // 唯一 producer（recvLoop），不加锁
    if (m_queue.size() < m_maxSize) {
        m_queue.push_back(std::move(msg));
        return true;
    }

    switch (m_policy) {
        case OverflowPolicy::DropNewest:
            ++m_droppedCount;
            return false;

        case OverflowPolicy::DropOldest:
            m_queue.pop_front();
            ++m_droppedCount;
            m_queue.push_back(std::move(msg));
            return true;

        case OverflowPolicy::Fatal:
            std::terminate();
    }
    return false;
}

MessagePtr PriorityMailbox::tryPop()
{
    std::lock_guard lk(m_mutex);
    if (m_queue.empty()) return nullptr;
    auto msg = std::move(m_queue.front());
    m_queue.pop_front();
    return msg;
}

size_t PriorityMailbox::size() const
{
    std::lock_guard lk(m_mutex);
    return m_queue.size();
}

uint64_t PriorityMailbox::droppedCount() const
{
    return m_droppedCount;
}

// ═════════════════════════════════════════════════════════════
//  MessageSystem — 生命周期
// ═════════════════════════════════════════════════════════════

MessageSystem::MessageSystem()
{
    m_mailboxes[static_cast<size_t>(MessagePriority::Critical)] =
        PriorityMailbox{ 256,  OverflowPolicy::Fatal      };
    m_mailboxes[static_cast<size_t>(MessagePriority::High)] =
        PriorityMailbox{ 512,  OverflowPolicy::DropOldest };
    m_mailboxes[static_cast<size_t>(MessagePriority::Normal)] =
        PriorityMailbox{ 1024, OverflowPolicy::DropOldest };
    m_mailboxes[static_cast<size_t>(MessagePriority::Low)] =
        PriorityMailbox{ 1024, OverflowPolicy::DropNewest };
    m_mailboxes[static_cast<size_t>(MessagePriority::Debug)] =
        PriorityMailbox{ 256,  OverflowPolicy::DropNewest };

    m_pubSocket = tipc_socket(SOCK_DGRAM);
    if (m_pubSocket < 0)
        throw std::runtime_error("Failed to create TIPC publish socket");

    m_recvThread = std::thread(&MessageSystem::recvLoop, this);
}

MessageSystem::~MessageSystem()
{
    m_running.store(false);
    m_cv.notify_all();
    if (m_recvThread.joinable())
        m_recvThread.join();

    if (m_pubSocket >= 0) {
        tipc_close(m_pubSocket);
        m_pubSocket = -1;
    }

    for (auto& [id, fd] : m_topicSockets)
        if (fd >= 0) tipc_close(fd);
}

// ═════════════════════════════════════════════════════════════
//  MessageSystem — 公开接口
// ═════════════════════════════════════════════════════════════

bool MessageSystem::publish(std::string_view topic, const void* data,
                            size_t size, MessagePriority priority)
{
    if (m_pubSocket < 0 || !data || size == 0 || size > MAX_MSG_SIZE)
        return false;

    TopicID  id   = hash::hash64(std::string(topic));
    TIPCAddr addr(id);

    size_t totalSize = sizeof(Message) + size;
    std::vector<uint8_t> buf(totalSize);       // thread independent
    auto* msg     = reinterpret_cast<Message*>(buf.data());
    msg->topic    = id;
    msg->priority = priority;
    msg->size     = static_cast<int>(size);
    std::memcpy(msg->payload, data, size);

    sockaddr_tipc dst{};
    dst.family               = AF_TIPC;
    dst.addrtype             = TIPC_ADDR_MCAST;
    dst.scope                = TIPC_CLUSTER_SCOPE;
    dst.addr.nameseq.type    = addr.type;
    dst.addr.nameseq.lower   = addr.instance;
    dst.addr.nameseq.upper   = addr.instance;

    // 只锁 sendto，buf 构造在锁外，减少临界区
    std::lock_guard lk(m_pubMutex);
    ssize_t n = ::sendto(m_pubSocket, buf.data(), totalSize, 0,
                         reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
    return n == static_cast<ssize_t>(totalSize);
}

TopicID MessageSystem::subscribe(std::string_view topic)
{
    TopicID  id   = hash::hash64(std::string(topic));
    TIPCAddr addr(id);

    {
        std::lock_guard lk(m_cvMutex);
        if (m_topicSockets.count(id)) return id;  // 已订阅
    }

    int fd = tipc_socket(SOCK_DGRAM);
    if (fd < 0) return 0;

    tipc_sock_non_block(fd);

    if (tipc_bind(fd, addr.type, addr.instance, addr.instance,
                  TIPC_CLUSTER_SCOPE) < 0) {
        tipc_close(fd);
        return 0;
    }

    {
        std::lock_guard lk(m_cvMutex);
        m_topicSockets[id] = fd;
    }

    m_cv.notify_all();  // 唤醒 recvLoop 重建 fd_set
    return id;
}

bool MessageSystem::unsubscribe(TopicID id)
{
    std::lock_guard lk(m_cvMutex);
    auto it = m_topicSockets.find(id);
    if (it == m_topicSockets.end()) return false;

    if (it->second >= 0) tipc_close(it->second);
    m_topicSockets.erase(it);
    return true;
}

bool MessageSystem::receive(MessagePtr& out, int timeout_ms)
{
    auto tryDequeue = [&]() -> bool {
        for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
            if (auto msg = m_mailboxes[i].tryPop()) {
                m_totalCount.fetch_sub(1, std::memory_order_relaxed);
                out = std::move(msg);
                return true;
            }
        }
        return false;
    };

    if (timeout_ms == 0)
        return tryDequeue();

    std::unique_lock cvLk(m_cvMutex);

    auto pred = [&] {
        return m_totalCount.load(std::memory_order_acquire) > 0
               || !m_running.load(std::memory_order_relaxed);
    };

    if (timeout_ms < 0)
        m_cv.wait(cvLk, pred);
    else
        m_cv.wait_for(cvLk, std::chrono::milliseconds(timeout_ms), pred);

    cvLk.unlock();  // 先释放 cv 锁，再抢队列锁，固定锁序
    return tryDequeue();
}

void MessageSystem::stats() const
{
    static constexpr const char* names[] = {
        "Critical", "High", "Normal", "Low", "Debug"
    };

    uint64_t totalDropped = 0;
    std::cout << "=== MessageSystem stats ===\n";
    for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
        uint64_t d = m_mailboxes[i].droppedCount();
        totalDropped += d;
        std::cout << "  [" << names[i] << "]"
                  << "  depth="   << m_mailboxes[i].size()
                  << "  dropped=" << d << '\n';
    }
    std::cout << "  total received=" << m_receivedCount.load()
              << "  total dropped="  << totalDropped
              << "  maxDepth="       << m_maxDepth.load()
              << '\n';
}

// ═════════════════════════════════════════════════════════════
//  MessageSystem — 私有
// ═════════════════════════════════════════════════════════════

bool MessageSystem::enqueue(MessagePtr msg)
{
    size_t idx = static_cast<size_t>(msg->priority);
    bool   ok  = m_mailboxes[idx].push(std::move(msg));

    if (ok) {
        size_t total = m_totalCount.fetch_add(1, std::memory_order_release) + 1;

        // 更新历史最大深度
        size_t prev = m_maxDepth.load(std::memory_order_relaxed);
        while (total > prev &&
               !m_maxDepth.compare_exchange_weak(prev, total,
                   std::memory_order_relaxed))
        {}

        m_cv.notify_one();
    }
    return ok;
}

void MessageSystem::recvLoop()
{
    std::array<uint8_t, MAX_MSG_SIZE> buf;

    while (m_running.load(std::memory_order_relaxed)) {

        // 每次循环重建 fd_set（订阅表可能运行中变化）
        fd_set readfds;
        int    maxfd = -1;
        std::vector<int> fds;

        {
            std::lock_guard lk(m_cvMutex);
            FD_ZERO(&readfds);
            for (auto& [id, fd] : m_topicSockets) {
                FD_SET(fd, &readfds);
                if (fd > maxfd) maxfd = fd;
                fds.push_back(fd);
            }
        }

        if (maxfd < 0) {
            // 还没有任何订阅，等待 subscribe() 唤醒
            std::unique_lock lk(m_cvMutex);
            m_cv.wait_for(lk, std::chrono::milliseconds(10),
                          [&] { return !m_topicSockets.empty()
                                       || !m_running.load(); });
            continue;
        }

        // 10ms 超时，保证能定期检查 m_running
        timeval tv{ 0, 10'000 };
        int ready = ::select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
        if (ready <= 0) continue;

        for (int fd : fds) {
            if (!FD_ISSET(fd, &readfds)) continue;

            struct tipc_addr src{};
            int     err = 0;
            ssize_t n   = tipc_recvfrom(fd, buf.data(), buf.size(),
                                        &src, nullptr, &err);

            // 至少要有完整的 Message header
            if (n < static_cast<ssize_t>(sizeof(Message))) continue;

            const auto* hdr = reinterpret_cast<const Message*>(buf.data());

            // 校验 priority 合法性
            MessagePriority priority = hdr->priority;
            if (static_cast<size_t>(priority) >= PRIORITY_COUNT)
                priority = MessagePriority::Normal;

            size_t payloadSize = static_cast<size_t>(n) - sizeof(Message);
            auto   msg = allocMessage(hdr->topic, priority,
                                      hdr->payload, payloadSize);
            if (!msg) continue;

            ++m_receivedCount;
            enqueue(std::move(msg));
        }
    }
}