#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

enum class TaskPriority : uint8_t {
    Critical = 0,
    High,
    Normal,
    Count
};

struct ThreadPoolStats {
    size_t pendingNow;  
    size_t pendingMax;
};

class ThreadPool {
public:
    explicit ThreadPool(size_t poolSize = 0);
    ~ThreadPool();

    
    bool start();
    bool pause();
    
    bool enqueue(  std::function<void()> task,
                   TaskPriority priority = TaskPriority::Normal);
    
    bool            isRunning() const;
    size_t          poolSize()  const;
    ThreadPoolStats stats()     const;
    
private:
    void workerLoop(size_t workerId);
    
private:
    const size_t m_poolSize;
    
    struct Impl;
    std::unique_ptr<Impl> m_impl;

    ThreadPool(const ThreadPool&)            = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&)                 = delete;
    ThreadPool& operator=(ThreadPool&&)      = delete;
};