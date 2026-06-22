#pragma once

#include <array>
#include <deque>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <string_view>
#include <cstdint>

using TopicID = uint64_t;

enum class MessagePriority : uint8_t
{
    Critical = 0,
    High,
    Normal,
    Low,
    Debug,
    Count
};

enum class OverflowPolicy : uint8_t {
    DropOldest = 0,
    DropNewest,
    Fatal
};

// ─────────────────────────────────────────────────────────────
//  Message
// ─────────────────────────────────────────────────────────────
struct Message {
    TopicID         topic;
    MessagePriority priority;
    int             size;
    char            payload[];  // allocate with malloc, free via MessageDeleter
};

struct MessageDeleter {
    void operator()(Message* msg) const { std::free(msg); }
};

using MessagePtr = std::unique_ptr<Message, MessageDeleter>;

// ─────────────────────────────────────────────────────────────
//  PriorityMailbox
//  Single producer (recvLoop) — mutex only protects consumers.
// ─────────────────────────────────────────────────────────────
class PriorityMailbox {
public:
    PriorityMailbox() = default;
    explicit PriorityMailbox(size_t maxSize, OverflowPolicy policy);

    // mutex/cv cannot be copied, but the mailbox can be moved
    PriorityMailbox(PriorityMailbox&& other) noexcept;
    PriorityMailbox& operator=(PriorityMailbox&& other) noexcept;

    bool       push(MessagePtr msg);
    MessagePtr tryPop();
    size_t     size()         const;
    uint64_t   droppedCount() const;

private:
    std::deque<MessagePtr>  m_queue;
    mutable std::mutex      m_mutex;
    std::condition_variable m_cv;

    size_t         m_maxSize{1024};
    OverflowPolicy m_policy{OverflowPolicy::DropOldest};
    uint64_t       m_droppedCount{0};
};

// ─────────────────────────────────────────────────────────────
//  MessageSystem
// ─────────────────────────────────────────────────────────────
class MessageSystem {
public:
    MessageSystem();
    ~MessageSystem();

    MessageSystem(const MessageSystem&)            = delete;
    MessageSystem& operator=(const MessageSystem&) = delete;
    MessageSystem(MessageSystem&&)                 = delete;
    MessageSystem& operator=(MessageSystem&&)      = delete;

    bool    publish(std::string_view topic, const void* data, size_t size,
                    MessagePriority priority = MessagePriority::Normal);

    TopicID subscribe(std::string_view topic);
    bool    unsubscribe(TopicID id);

    // Dequeue the highest-priority available message.
    // timeout_ms == -1 : block indefinitely
    // timeout_ms ==  0 : non-blocking
    // timeout_ms >  0  : wait up to N ms
    bool    receive(MessagePtr& msg, int timeout_ms = -1);

    void    stats() const;

private:
    void recvLoop();
    bool enqueue(MessagePtr msg);

private:
    constexpr static size_t PRIORITY_COUNT =
        static_cast<size_t>(MessagePriority::Count);

    // ── Network ──────────────────────────────────────────────
    int                              m_pubSocket{-1};
    std::mutex                       m_pubMutex; 
    
    std::unordered_map<TopicID, int> m_topicSockets;

    // ── Mailboxes ────────────────────────────────────────────
    std::array<PriorityMailbox, PRIORITY_COUNT> m_mailboxes;

    // ── Global arrival signal ─────────────────────────────────
    std::mutex              m_cvMutex;
    std::condition_variable m_cv;
    std::atomic<size_t>     m_totalCount{0};

    // ── Receiver thread ──────────────────────────────────────
    std::atomic<bool> m_running{true};
    std::thread       m_recvThread;

    // ── Stats ────────────────────────────────────────────────
    std::atomic<uint64_t> m_receivedCount{0};
    std::atomic<size_t>   m_maxDepth{0};
    // dropped: aggregated from each PriorityMailbox::droppedCount()
};