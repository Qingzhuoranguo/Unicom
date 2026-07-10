#pragma once

#include <array>
#include <deque>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <cstdint>

using TopicID = uint64_t;

static constexpr TopicID INVALID_TOPIC_ID = 0;

enum class MessagePriority : uint8_t {
    Critical = 0,
    High,
    Normal,
    Count
};


enum class ChannelType : uint8_t {
    Fast,           // Unreliable + Unordered
    ReliableFast,   // Reliable   + Unordered
    Count
};

// ─────────────────────────────────────────────────────────────
//  Message  (application-layer envelope, channel-agnostic)
// ─────────────────────────────────────────────────────────────
struct Message {
    TopicID         topic;
    MessagePriority priority;
    int             size;
    char            payload[];  
};

struct MessageDeleter {
    void operator()(Message* msg) const { std::free(msg); }
};

using MessagePtr = std::unique_ptr<Message, MessageDeleter>;

// ─────────────────────────────────────────────────────────────
//  NOTE: Move construction/assignment are NOT thread-safe.
// ─────────────────────────────────────────────────────────────
enum class OverflowPolicy : uint8_t {
    DropOldest = 0,
    DropNewest,
    Fatal
};
class PriorityMailbox {
public:
    PriorityMailbox() = default;
    explicit PriorityMailbox(size_t maxSize, OverflowPolicy policy);

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

    bool publish(   std::string_view topic,
                    const void*      data,
                    size_t           size,
                    MessagePriority  priority = MessagePriority::Normal,
                    ChannelType      channel  = ChannelType::Fast );

    // A topic can only be bound to one ChannelType.
    // Returns INVALID_TOPIC_ID (0) on failure or channel conflict.
    TopicID subscribe(  std::string_view topic,
                        ChannelType      channel = ChannelType::Fast );

    bool unsubscribe(TopicID id);

    // timeout_ms == -1 : block indefinitely
    // timeout_ms ==  0 : non-blocking
    // timeout_ms >  0  : wait up to N ms
    bool receive(MessagePtr& msg, int timeout_ms = -1);

    inline bool isRunning() const { return m_running.load(); }
    void stats() const;

private:
    void recvLoop(ChannelType channel);
    bool enqueue(MessagePtr msg);
    static int socketTypeForChannel(ChannelType ch) noexcept;

    
    MessageSystem( const MessageSystem& )            = delete;
    MessageSystem& operator = (const MessageSystem& ) = delete;
    MessageSystem( MessageSystem&& )                 = delete;
    MessageSystem& operator = ( MessageSystem&& )      = delete;

private:
    constexpr static size_t PRIORITY_COUNT =
        static_cast<size_t>(MessagePriority::Count);  // 3
    constexpr static size_t CHANNEL_COUNT  =
        static_cast<size_t>(ChannelType::Count);       // 2

    struct TopicEntry {
        int         socket {-1};
        ChannelType channel{ChannelType::Fast};
        std::string name;           // back-reference for O(1) unsubscribe
    };
    std::unordered_map<TopicID, TopicEntry>  m_topicEntries;
    std::unordered_map<std::string, TopicID> m_topicIndex;
    std::array<size_t, CHANNEL_COUNT>        m_channelTopicCount{};
    std::mutex                               m_topicMutex;

    std::array<int, CHANNEL_COUNT> m_pubSockets{-1, -1};
    mutable std::mutex             m_pubMutex;

    std::array<PriorityMailbox, PRIORITY_COUNT> m_mailboxes;

    std::mutex              m_cvMutex;
    std::condition_variable m_cv;
    std::atomic<size_t>     m_totalCount{0};

    std::array<std::mutex,              CHANNEL_COUNT> m_channelMutexes;
    std::array<std::condition_variable, CHANNEL_COUNT> m_channelCvs;

    std::atomic<bool>                      m_running{true};
    std::array<std::thread, CHANNEL_COUNT> m_recvThreads;

    std::atomic<uint64_t> m_receivedCount{0};
    std::atomic<size_t>   m_maxDepth{0};
};