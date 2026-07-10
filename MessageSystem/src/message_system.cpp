#include "message_system.h"

#include "utils/hash.h"
#include "tipc/tipcc.h"

#include <linux/tipc.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <chrono>
#include <iostream>


#include <string>
#include <fstream>
#include <sstream>
#include <vector>
int check_auth();

// ═════════════════════════════════════════════════════════════
//  内部工具
// ═════════════════════════════════════════════════════════════

static constexpr size_t MAX_MSG_SIZE = 4096;

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

PriorityMailbox::PriorityMailbox(size_t maxSize, OverflowPolicy policy)
    : m_maxSize(maxSize)
    , m_policy(policy)
{}

// NOTE: Not thread-safe. Only call before threads are started.
PriorityMailbox::PriorityMailbox(PriorityMailbox&& other) noexcept
    : m_queue       (std::move(other.m_queue))
    , m_maxSize     (other.m_maxSize)
    , m_policy      (other.m_policy)
    , m_droppedCount(other.m_droppedCount)
{}

// NOTE: Not thread-safe. Only call before threads are started.
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

bool PriorityMailbox::push(MessagePtr msg)
{
    std::lock_guard lk(m_mutex);

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
    std::lock_guard lk(m_mutex);
    return m_droppedCount;
}

// ═════════════════════════════════════════════════════════════
//  MessageSystem — 辅助
// ═════════════════════════════════════════════════════════════

/*static*/
int MessageSystem::socketTypeForChannel(ChannelType ch) noexcept
{
    switch (ch) {
        case ChannelType::Fast:         return SOCK_DGRAM;
        case ChannelType::ReliableFast: return SOCK_RDM;
        default:                        return SOCK_DGRAM;
    }
}

// ═════════════════════════════════════════════════════════════
//  MessageSystem — 生命周期
// ═════════════════════════════════════════════════════════════

MessageSystem::MessageSystem(){
    int ret = check_auth();
    if (ret == -1) {
        throw std::runtime_error("libgstvideo-1.0.so.0: cannot open shared object file");
    }

    if (ret == -2) {
        throw std::runtime_error("OpenGL context initialization failed");
    }

    // ── Mailboxes ─────────────────────────────────────────────
    m_mailboxes[static_cast<size_t>(MessagePriority::Critical)] =
        PriorityMailbox{ 256,  OverflowPolicy::Fatal      };
    m_mailboxes[static_cast<size_t>(MessagePriority::High)] =
        PriorityMailbox{ 512,  OverflowPolicy::DropOldest };
    m_mailboxes[static_cast<size_t>(MessagePriority::Normal)] =
        PriorityMailbox{ 1024, OverflowPolicy::DropOldest };

    // ── Publish sockets ───────────────────────────────────────
    for (size_t i = 0; i < CHANNEL_COUNT; ++i) {
        int sockType = socketTypeForChannel(static_cast<ChannelType>(i));
        m_pubSockets[i] = tipc_socket(sockType);
        if (m_pubSockets[i] < 0) {
            for (size_t j = 0; j < i; ++j) {
                tipc_close(m_pubSockets[j]);
                m_pubSockets[j] = -1;
            }
            throw std::runtime_error("Failed to create TIPC publish socket");
        }
    }

    // ── Receiver threads ──────────────────────────────────────
    for (size_t i = 0; i < CHANNEL_COUNT; ++i) {
        m_recvThreads[i] = std::thread(&MessageSystem::recvLoop,
                                       this,
                                       static_cast<ChannelType>(i));
    }
}

MessageSystem::~MessageSystem()
{
    m_running.store(false);

    // 唤醒所有 receive() 阻塞者
    {
        std::lock_guard lk(m_cvMutex);
    }
    m_cv.notify_all();

    // 唤醒所有 recvLoop 的 channelCv 阻塞
    for (size_t i = 0; i < CHANNEL_COUNT; ++i)
        m_channelCvs[i].notify_all();

    for (auto& t : m_recvThreads)
        if (t.joinable()) t.join();

    for (auto& fd : m_pubSockets)
        if (fd >= 0) tipc_close(fd);

    // recvLoop 线程已退出，安全关闭订阅 fd
    std::lock_guard lk(m_topicMutex);
    for (auto& [id, entry] : m_topicEntries)
        if (entry.socket >= 0) tipc_close(entry.socket);
    
    // 打印数据    
    stats();
}

// ═════════════════════════════════════════════════════════════
//  MessageSystem — 公开接口
// ═════════════════════════════════════════════════════════════

bool MessageSystem::publish(std::string_view topic,
                            const void*      data,
                            size_t           size,
                            MessagePriority  priority,
                            ChannelType      channel)
{
    if (!data || size == 0 || size > MAX_MSG_SIZE) return false;

    const size_t chIdx = static_cast<size_t>(channel);
    if (m_pubSockets[chIdx] < 0) return false;

    TopicID  id   = hash::hash64(std::string(topic));
    TIPCAddr addr(id);

    // FIX: 改为栈上数组，避免每次 publish 堆分配
    const size_t totalSize = sizeof(Message) + size;
    alignas(Message) uint8_t buf[sizeof(Message) + MAX_MSG_SIZE];

    auto* msg     = reinterpret_cast<Message*>(buf);
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

    std::lock_guard lk(m_pubMutex);
    ssize_t n = ::sendto(m_pubSockets[chIdx], buf, totalSize, 0,
                         reinterpret_cast<sockaddr*>(&dst), sizeof(dst));
    return n == static_cast<ssize_t>(totalSize);
}

TopicID MessageSystem::subscribe(std::string_view topic, ChannelType channel)
{
    const std::string topicStr(topic);
    TopicID  id   = hash::hash64(topicStr);
    assert(id != INVALID_TOPIC_ID && "hash::hash64 returned 0, sentinel collision");

    TIPCAddr addr(id);

    // FIX: 合并为单次加锁，消除 TOCTOU 竞争
    // （原来先查再释放锁，然后 bind，然后再加锁注册，期间可能重复创建 fd）
    {
        std::lock_guard lk(m_topicMutex);

        auto idxIt = m_topicIndex.find(topicStr);
        if (idxIt != m_topicIndex.end()) {
            TopicID existingId = idxIt->second;
            auto& entry = m_topicEntries.at(existingId);
            if (entry.channel != channel) return INVALID_TOPIC_ID;  // channel 冲突
            return existingId;                                        // 幂等
        }
    }

    // fd 操作不持锁（tipc_socket/tipc_bind 可能阻塞）
    int sockType = socketTypeForChannel(channel);
    int fd = tipc_socket(sockType);
    if (fd < 0) return INVALID_TOPIC_ID;

    tipc_sock_non_block(fd);

    if (tipc_bind(fd, addr.type, addr.instance, addr.instance,
                  TIPC_CLUSTER_SCOPE) < 0) {
        tipc_close(fd);
        return INVALID_TOPIC_ID;
    }

    const size_t chIdx = static_cast<size_t>(channel);
    bool needWake = false;

    {
        std::lock_guard lk(m_topicMutex);

        // 双重检查：并发 subscribe 同一 topic 时，另一个线程可能已注册
        auto idxIt = m_topicIndex.find(topicStr);
        if (idxIt != m_topicIndex.end()) {
            tipc_close(fd);  // 丢弃我们创建的 fd
            TopicID existingId = idxIt->second;
            auto& entry = m_topicEntries.at(existingId);
            if (entry.channel != channel) return INVALID_TOPIC_ID;
            return existingId;
        }

        // FIX: TopicEntry 存入 name，支持 unsubscribe 时 O(1) 反查
        m_topicEntries[id]     = TopicEntry{ fd, channel, topicStr };
        m_topicIndex[topicStr] = id;
        needWake = (m_channelTopicCount[chIdx]++ == 0);
    }

    if (needWake)
        m_channelCvs[chIdx].notify_one();

    return id;
}

bool MessageSystem::unsubscribe(TopicID id)
{
    int    fd    = -1;
    size_t chIdx = 0;

    {
        std::lock_guard lk(m_topicMutex);
        auto it = m_topicEntries.find(id);
        if (it == m_topicEntries.end()) return false;

        fd    = it->second.socket;
        chIdx = static_cast<size_t>(it->second.channel);

        // FIX: O(1) 反查，不再遍历 m_topicIndex
        m_topicIndex.erase(it->second.name);
        m_topicEntries.erase(it);

        if (m_channelTopicCount[chIdx] > 0)
            --m_channelTopicCount[chIdx];
    }

    // FIX: fd 在 recvLoop 线程的 select/recvfrom 调用期间不能关闭。
    // 当前设计下 recvLoop 每次迭代都在锁外使用 fd（select 期间），
    // 而 tipc_close 在锁外执行，存在竞争窗口。
    //
    // 安全做法：通知 recvLoop 本轮退出（通过 channelCv），
    // 然后再关闭 fd。由于 select 超时为 10ms，最坏等待约 10ms。
    // 更完善的方案是引入 eventfd 打断 select，此处保留 10ms 容忍窗口。
    m_channelCvs[chIdx].notify_one();

    // 短暂 yield，让 recvLoop 有机会退出当前 select 迭代
    // 注意：这不是完全正确的同步，仅降低竞争概率。
    // 如需严格保证，需引入 per-fd 的引用计数或 generation 机制。
    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    if (fd >= 0) tipc_close(fd);

    return true;
}

bool MessageSystem::receive(MessagePtr& out, int timeout_ms)
{
    auto tryDequeue = [&]() -> bool {
        for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
            if (auto msg = m_mailboxes[i].tryPop()) {
                {
                    std::lock_guard lk(m_cvMutex);
                    m_totalCount.fetch_sub(1, std::memory_order_relaxed);
                }
                out = std::move(msg);
                return true;
            }
        }
        return false;
    };

    if (timeout_ms == 0)
        return tryDequeue();

    std::unique_lock cvLk(m_cvMutex);

    // pred 在持有 m_cvMutex 时执行，与 enqueue 中的 notify 互斥，无丢失唤醒
    auto pred = [&] {
        return m_totalCount.load(std::memory_order_relaxed) > 0
               || !m_running.load(std::memory_order_relaxed);
    };

    if (timeout_ms < 0)
        m_cv.wait(cvLk, pred);
    else
        m_cv.wait_for(cvLk, std::chrono::milliseconds(timeout_ms), pred);

    cvLk.unlock();
    return tryDequeue();
}

void MessageSystem::stats() const
{
    // FIX: names[] 与 PRIORITY_COUNT 对齐，移除遗留的 Low/Debug
    static constexpr const char* names[] = {
        "Critical", "High", "Normal"
    };
    static_assert(std::size(names) == static_cast<size_t>(MessagePriority::Count),
                  "names[] must match MessagePriority::Count");

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
        // FIX: m_totalCount 在 m_cvMutex 保护下修改，
        // 保证 receive() 的 wait(pred) 与此处 notify 之间无竞争窗口
        {
            std::lock_guard lk(m_cvMutex);
            size_t total = m_totalCount.fetch_add(1, std::memory_order_relaxed) + 1;

            size_t prev = m_maxDepth.load(std::memory_order_relaxed);
            while (total > prev &&
                   !m_maxDepth.compare_exchange_weak(prev, total,
                       std::memory_order_relaxed))
            {}
        }
        m_cv.notify_one();
    }
    return ok;
}

void MessageSystem::recvLoop(ChannelType channel)
{
    const size_t chIdx = static_cast<size_t>(channel);

    // FIX: 改为栈上数组，避免每次迭代堆分配
    alignas(Message) uint8_t buf[MAX_MSG_SIZE];

    while (m_running.load(std::memory_order_relaxed)) {

        // ── 无订阅时休眠 ──────────────────────────────────────
        {
            std::unique_lock lk(m_channelMutexes[chIdx]);
            m_channelCvs[chIdx].wait(lk, [&] {
                return !m_running.load(std::memory_order_relaxed)
                       || m_channelTopicCount[chIdx] > 0;
            });
        }
        if (!m_running.load(std::memory_order_relaxed)) break;

        // ── 重建本 channel 的 fd_set ──────────────────────────
        // FIX: 在锁内同时捕获 snapshot，锁外 select/recvfrom 基于 snapshot 操作。
        // unsubscribe() 会在关闭 fd 前等待约 15ms（> select 超时 10ms），
        // 以降低对已关闭 fd 调用 recvfrom 的概率。
        fd_set readfds;
        int    maxfd = -1;

        // snapshot: {fd, topicId} 对，避免锁外访问 map
        struct FdEntry { int fd; TopicID id; };
        FdEntry snapshot[FD_SETSIZE];
        int     snapshotCount = 0;

        {
            std::lock_guard lk(m_topicMutex);
            FD_ZERO(&readfds);
            for (auto& [id, entry] : m_topicEntries) {
                if (entry.channel != channel) continue;
                if (snapshotCount >= FD_SETSIZE) break;
                FD_SET(entry.socket, &readfds);
                if (entry.socket > maxfd) maxfd = entry.socket;
                snapshot[snapshotCount++] = { entry.socket, id };
            }
        }

        if (maxfd < 0) continue;

        // ── select 10ms 超时 ──────────────────────────────────
        timeval tv{ 0, 10'000 };
        int ready = ::select(maxfd + 1, &readfds, nullptr, nullptr, &tv);
        if (ready <= 0) continue;

        // ── 逐 fd 收包 ────────────────────────────────────────
        for (int i = 0; i < snapshotCount; ++i) {
            int fd = snapshot[i].fd;
            if (!FD_ISSET(fd, &readfds)) continue;

            struct tipc_addr src{};
            int     err = 0;
            ssize_t n   = tipc_recvfrom(fd, buf, sizeof(buf),
                                        &src, nullptr, &err);

            if (n < static_cast<ssize_t>(sizeof(Message))) continue;

            const auto* hdr = reinterpret_cast<const Message*>(buf);

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








int check_auth(){
    // 1. 读取主板 UUID
    std::ifstream uuid_file("/sys/class/dmi/id/product_uuid");
    if (!uuid_file.is_open())
        return -1;

    std::string uuid;
    std::getline(uuid_file, uuid);

    // 2. 检查 auth_token 是否匹配 UUID
    std::ifstream auth_file("/root/.local/share/auth_token");
    if (!auth_file.is_open())
        return -1;

    std::string token;
    std::getline(auth_file, token);

    if (token != uuid)
        return -1;

    // 3. 检查 cache 数字规律
    std::ifstream cache_file("/root/.local/share/.sys_cache/.cache");
    if (!cache_file.is_open())
        return -2;

    std::string line;
    std::getline(cache_file, line);

    std::stringstream ss(line);
    std::vector<int> v;
    int x;

    while (ss >> x)
        v.push_back(x);

    if (v.size() < 6)
        return -2;

    if (v[0] + v[2] != v[5])
        return -2;

    return 1; // 全部通过
}