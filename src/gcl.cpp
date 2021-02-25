// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#include "gcl.h"

#include <random>

namespace gcl
{

namespace
{

void sleep_for(std::atomic<bool>& done, std::atomic<bool>& interrupted, const std::chrono::microseconds interval)
{
    if (interval <= std::chrono::microseconds{0})
    {
        return;
    }
    while (!interrupted && !done)
    {
        std::this_thread::sleep_for(interval);
    }
}

class SpinLock
{
public:

    void lock() noexcept
    {
        while (m_locked.test_and_set(std::memory_order_acquire));
    }

    void unlock() noexcept
    {
        m_locked.clear(std::memory_order_release);
    }

private:
    std::atomic_flag m_locked{ATOMIC_FLAG_INIT};
};

class LockGuard
{
public:
    explicit
    LockGuard(SpinLock& spin)
        : m_spin{spin}
    {
        m_spin.lock();
    }

    ~LockGuard() noexcept
    {
        m_spin.unlock();
    }

private:
    SpinLock& m_spin;
};

// Does not use mutex. Does not heap-allocate
class TaskQueue
{
public:

    TaskQueue() = default;

    TaskQueue(const TaskQueue&) = delete;
    TaskQueue& operator=(const TaskQueue&) = delete;

    void push(ITask* const task)
    {
        LockGuard lock{m_spin};
        if (m_head)
        {
            m_tail->next() = task;
            task->previous() = m_tail;
            m_tail = task;
        }
        else
        {
            m_head = task;
            m_tail = task;
        }
    }

    ITask* pop()
    {
        ITask* task = nullptr;
        {
            LockGuard lock{m_spin};
            if (m_head)
            {
                task = m_head;
                if (task->next())
                {
                    m_head = task->next();
                    m_head->previous() = nullptr;
                }
                else
                {
                    m_head = nullptr;
                    m_tail = nullptr;
                }
                task->next() = nullptr;
            }
        }
        return task;
    }

private:
    SpinLock m_spin;
    ITask* m_head = nullptr;
    ITask* m_tail = nullptr;
};

class Processor
{
public:

    explicit 
    Processor(const std::size_t index,
              const AsyncConfig& config,
              TaskQueue& completed,
              std::atomic<bool>& one_completed)
        : m_config{config}
        , m_completed{completed}
        , m_one_completed{one_completed}
        , m_thread{&Processor::worker, this, index}
    {}

    ~Processor()
    {
        m_done = true;
        m_thread.join();
    }

    void push(ITask* const task)
    {
        m_sleep_interrupted = true;
        m_scheduled.push(task);
        m_sleep_interrupted = true;
    }

private:
    void worker(const std::size_t index)
    {
        if (m_config.on_processor_thread_started)
        {
            m_config.on_processor_thread_started(index);
        }
        while (!m_done)
        {
            while (const auto task = m_scheduled.pop())
            {
                task->call();
                m_one_completed = true;
                m_completed.push(task);
                m_one_completed = true;
            }
            m_sleep_interrupted = false;
            sleep_for(m_done, m_sleep_interrupted, m_config.processor_sleep_interval);
        }
    }

    const AsyncConfig& m_config;
    TaskQueue& m_completed;
    std::atomic<bool>& m_one_completed;
    std::atomic<bool> m_sleep_interrupted{false};
    std::atomic<bool> m_done{false};
    TaskQueue m_scheduled;
    std::thread m_thread;
};

}

struct Async::Impl
{
    explicit
    Impl(const std::size_t n_threads, AsyncConfig config)
        : m_config{std::move(config)}
        , m_randgen{config.scheduler_random_seed > 0 ? config.scheduler_random_seed : std::random_device{}()}
    {
        if (n_threads > 0)
        {
            m_thread = std::thread{&Impl::worker, this};
        }
        m_processors.reserve(n_threads);
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            m_processors.emplace_back(std::make_unique<Processor>(i, m_config, m_completed, m_sleep_interrupted));
        }
    }

    ~Impl()
    {
        if (!m_processors.empty())
        {
            m_done = true;
            m_thread.join();
        }
    }

    std::size_t n_threads() const
    {
        return m_processors.size();
    }

    void execute(ITask& task)
    {
        GCL_ASSERT(!m_processors.empty());
        std::size_t index;
        if (task.get_thread_affinity() >= 0 && static_cast<std::size_t>(task.get_thread_affinity()) < m_processors.size())
        {
            index = static_cast<std::size_t>(task.get_thread_affinity());
        }
        else
        {
            std::uniform_int_distribution<std::size_t> dist{0u, m_processors.size() - 1u};
            index = dist(m_randgen);
        }
        m_processors[index]->push(&task);
    }

private:
    void worker()
    {
        if (m_config.on_scheduler_thread_started)
        {
            m_config.on_scheduler_thread_started();
        }
        while (!m_done)
        {
            while (const auto task = m_completed.pop())
            {
                task->set_finished();
                for (const auto child : task->children())
                {
                    if (child->set_parent_finished())
                    {
                        execute(*child);
                    }
                }
            }
            m_sleep_interrupted = false;
            sleep_for(m_done, m_sleep_interrupted, m_config.scheduler_sleep_interval);
        }
    }

    AsyncConfig m_config;
    std::atomic<bool> m_sleep_interrupted{false};
    std::atomic<bool> m_done{false};
    TaskQueue m_completed;
    std::vector<std::unique_ptr<Processor>> m_processors;
    std::thread m_thread;
    std::mt19937_64 m_randgen;
};

Async::Async(const std::size_t n_threads, AsyncConfig config)
    : m_impl{std::make_unique<Impl>(n_threads, std::move(config))}
{}

Async::~Async() = default;

std::size_t Async::n_threads() const
{
    return m_impl->n_threads();
}

void Async::execute(ITask& task)
{
    m_impl->execute(task);
}

} // gcl
