#include "threadpool.h"

#include <array>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

// ─────────────────────────────────────────────────────────────
//  队列上限（内部配置，不对外暴露）
// ─────────────────────────────────────────────────────────────
static constexpr size_t QUEUE_MAX_CRITICAL =   64;
static constexpr size_t QUEUE_MAX_HIGH     =  256;
static constexpr size_t QUEUE_MAX_NORMAL   = 1024;

// ─────────────────────────────────────────────────────────────
//  Impl
// ─────────────────────────────────────────────────────────────
struct ThreadPool::Impl {

    static constexpr size_t PRIORITY_COUNT =
        static_cast<size_t>(TaskPriority::Count);  // 3

    struct PriorityQueue {
        std::deque<std::function<void()>> tasks;
        std::mutex                        mutex;
        size_t                            maxSize;
    };

    std::array<PriorityQueue, PRIORITY_COUNT> queues {{
        PriorityQueue{{}, {}, QUEUE_MAX_CRITICAL},
        PriorityQueue{{}, {}, QUEUE_MAX_HIGH    },
        PriorityQueue{{}, {}, QUEUE_MAX_NORMAL  },
    }};

    // ── 状态 ──────────────────────────────────────────────────
    enum class State { Idle, Running, Paused };
    std::atomic<State> state   {State::Idle};
    std::atomic<bool>  shutdown{false};

    // ── 待处理计数 ────────────────────────────────────────────
    std::atomic<size_t> totalPending{0};
    std::atomic<size_t> peakPending {0};  // 历史最大

    // ── 唤醒 workers ──────────────────────────────────────────
    std::mutex              cvMutex;
    std::condition_variable cv;

    // ── 线程 ──────────────────────────────────────────────────
    std::vector<std::thread> workers;
};

// ─────────────────────────────────────────────────────────────
//  生命周期
// ─────────────────────────────────────────────────────────────
ThreadPool::ThreadPool(size_t poolSize)
    : m_poolSize(
        [&]{
            if (poolSize > 0) return poolSize;
            long n = ::sysconf(_SC_NPROCESSORS_ONLN);
            return static_cast<size_t>(n > 0 ? n : 1);
        }())
    , m_impl(std::make_unique<Impl>())
{
    m_impl->workers.reserve(m_poolSize);
    for (size_t i = 0; i < m_poolSize; ++i)
        m_impl->workers.emplace_back(&ThreadPool::workerLoop, this, i);
}

ThreadPool::~ThreadPool()
{
    m_impl->shutdown.store(true);

    // 丢弃未分发任务
    for (auto& q : m_impl->queues) {
        std::lock_guard lk(q.mutex);
        m_impl->totalPending.fetch_sub(q.tasks.size(),
                                        std::memory_order_relaxed);
        q.tasks.clear();
    }

    m_impl->cv.notify_all();

    for (auto& t : m_impl->workers)
        if (t.joinable()) t.join();
}

// ─────────────────────────────────────────────────────────────
//  控制
// ─────────────────────────────────────────────────────────────
bool ThreadPool::start()
{
    auto expected = Impl::State::Idle;
    if (m_impl->state.compare_exchange_strong(expected, Impl::State::Running)) {
        m_impl->cv.notify_all();
        return true;
    }
    expected = Impl::State::Paused;
    if (m_impl->state.compare_exchange_strong(expected, Impl::State::Running)) {
        m_impl->cv.notify_all();
        return true;
    }
    return false;
}

bool ThreadPool::pause()
{
    auto expected = Impl::State::Running;
    return m_impl->state.compare_exchange_strong(expected, Impl::State::Paused);
}

// ─────────────────────────────────────────────────────────────
//  入队
// ─────────────────────────────────────────────────────────────
bool ThreadPool::enqueue(std::function<void()> task, TaskPriority priority)
{
    if (!task) return false;

    auto& q = m_impl->queues[static_cast<size_t>(priority)];
    {
        std::lock_guard lk(q.mutex);
        if (q.tasks.size() >= q.maxSize) return false;
        q.tasks.push_back(std::move(task));
    }

    size_t cur = m_impl->totalPending.fetch_add(1, std::memory_order_release) + 1;

    // 更新历史峰值
    size_t peak = m_impl->peakPending.load(std::memory_order_relaxed);
    while (cur > peak &&
           !m_impl->peakPending.compare_exchange_weak(peak, cur,
               std::memory_order_relaxed))
    {}

    m_impl->cv.notify_one();
    return true;
}

// ─────────────────────────────────────────────────────────────
//  查询
// ─────────────────────────────────────────────────────────────
bool ThreadPool::isRunning() const
{
    return m_impl->state.load() == Impl::State::Running;
}

size_t ThreadPool::poolSize() const
{
    return m_poolSize;
}

ThreadPoolStats ThreadPool::stats() const
{
    return {
        m_impl->totalPending.load(std::memory_order_relaxed),
        m_impl->peakPending .load(std::memory_order_relaxed),
    };
}

// ─────────────────────────────────────────────────────────────
//  Worker
// ─────────────────────────────────────────────────────────────
void ThreadPool::workerLoop(size_t workerId)
{
    std::deque<std::function<void()>> localCache;

    while (true) {

        // 1) 如果本地缓存有任务，直接执行（无锁）
        if (!localCache.empty()) {
            auto task = std::move(localCache.front());
            localCache.pop_front();
            task();
            continue;
        }

        // 2) 本地缓存空了 → 去全局队列批量取任务
        {
            std::unique_lock lk(m_impl->cvMutex);
            m_impl->cv.wait(lk, [&] {
                return m_impl->shutdown
                    || (m_impl->state == Impl::State::Running
                     && m_impl->totalPending > 0);
            });
        }

        if (m_impl->shutdown) break;

        // 3) 批量取任务
        constexpr size_t BATCH = 8;
        for (auto& q : m_impl->queues) {
            std::lock_guard qlk(q.mutex);
            while (!q.tasks.empty() && localCache.size() < BATCH) {
                localCache.push_back(std::move(q.tasks.front()));
                q.tasks.pop_front();
                m_impl->totalPending--;
            }
            if (!localCache.empty()) break;
        }

        // 4) 如果还是没任务，继续下一轮
        if (localCache.empty()) continue;
    }
}
