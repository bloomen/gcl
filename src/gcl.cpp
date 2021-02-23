// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#include "gcl.h"

#include <forward_list>
#include <random>

#include <concurrentqueue.h>
#include <readerwriterqueue.h>

namespace gcl
{

namespace
{

template<typename QueueImpl>
class LockFreeQueue
{
public:

    LockFreeQueue() = default;

    explicit
    LockFreeQueue(const std::size_t initial_size)
        : m_queue{initial_size}
    {}

    LockFreeQueue(const LockFreeQueue&) = delete;
    LockFreeQueue& operator=(const LockFreeQueue&) = delete;

    void push(ITask* const task)
    {
        m_queue.enqueue(task);
    }

    ITask* pop()
    {
        ITask* task = nullptr;
        m_queue.try_dequeue(task);
        return task;
    }

private:
    QueueImpl m_queue;
};

using CompletedQueue = LockFreeQueue<moodycamel::ConcurrentQueue<ITask*>>; // MpSc
using ScheduledQueue = LockFreeQueue<moodycamel::ReaderWriterQueue<ITask*>>; // SpSc

class Processor
{
public:

    explicit 
    Processor(const std::size_t index,
              const AsyncConfig& config,
              const std::atomic<bool>& active,
              CompletedQueue& completed)
        : m_config{config}
        , m_active{active}
        , m_completed{completed}
        , m_thread{&Processor::worker, this, index}
    {}

    ~Processor()
    {
        m_done = true;
        m_thread.join();
    }

    void push(ITask* const task)
    {
        m_scheduled.push(task);
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
                m_completed.push(task);
            }
            if (m_config.processor_yields)
            {
                std::this_thread::yield();
            }
            if (m_config.inactive_processor_sleep_interval > std::chrono::microseconds{0} && !m_active)
            {
                std::this_thread::sleep_for(m_config.inactive_processor_sleep_interval);
            }
        }
    }

    const AsyncConfig& m_config;
    const std::atomic<bool>& m_active;
    CompletedQueue& m_completed;
    std::atomic<bool> m_done{false};
    ScheduledQueue m_scheduled;
    std::thread m_thread;
};

}

struct Async::Impl
{
    explicit
    Impl(const std::size_t n_threads, AsyncConfig config)
        : m_config{std::move(config)}
        , m_active{m_config.active}
        , m_completed{n_threads > 0 ? m_config.initial_scheduler_queue_size : 0}
        , m_randgen{config.scheduler_random_seed > 0 ? config.scheduler_random_seed : std::random_device{}()}
    {
        if (n_threads > 0)
        {
            m_thread = std::thread{&Impl::worker, this};
        }
        m_processors.reserve(n_threads);
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            m_processors.emplace_back(std::make_unique<Processor>(i, m_config, m_active, m_completed));
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

    void set_active(const bool active)
    {
        m_active = active;
    }

    void execute(ITask& task)
    {
        if (m_processors.empty())
        {
            task.call();
            on_completed(task);
        }
        else
        {
            if (task.get_thread_affinity() >= 0 && static_cast<std::size_t>(task.get_thread_affinity()) < m_processors.size())
            {
                const auto index = static_cast<std::size_t>(task.get_thread_affinity());
                m_processors[index]->push(&task);
            }
            else
            {
                std::uniform_int_distribution<std::size_t> dist{0u, m_processors.size() - 1u};
                const auto index = dist(m_randgen);
                m_processors[index]->push(&task);
            }
        }
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
                on_completed(*task);
            }
            if (m_config.scheduler_yields)
            {
                std::this_thread::yield();
            }
            if (m_config.inactive_scheduler_sleep_interval > std::chrono::microseconds{0} && !m_active)
            {
                std::this_thread::sleep_for(m_config.inactive_scheduler_sleep_interval);
            }
        }
    }

    void on_completed(ITask& task)
    {
        for (const auto parent : task.parents())
        {
            if (parent->set_child_finished())
            {
                parent->auto_release();
            }
        }
        task.set_finished();
        for (const auto child : task.children())
        {
            if (child->set_parent_finished())
            {
                execute(*child);
            }
        }
    }

    AsyncConfig m_config;
    std::atomic<bool> m_done{false};
    std::atomic<bool> m_active;
    CompletedQueue m_completed;
    std::vector<std::unique_ptr<Processor>> m_processors;
    std::thread m_thread;
    std::mt19937 m_randgen;
};

Async::Async(const std::size_t n_threads, AsyncConfig config)
    : m_impl{std::make_unique<Impl>(n_threads, std::move(config))}
{}

Async::~Async() = default;

void Async::set_active(bool active)
{
    m_impl->set_active(active);
}

void Async::execute(ITask& task)
{
    m_impl->execute(task);
}

int detail::BaseImpl::get_thread_affinity() const
{
    return m_thread_affinity;
}

const std::vector<gcl::ITask*>& detail::BaseImpl::parents() const
{
    return m_parents;
}

const std::vector<gcl::ITask*>& detail::BaseImpl::children() const
{
    return m_children;
}

bool detail::BaseImpl::set_parent_finished()
{
    return ++m_parents_ready == m_parents.size();
}

bool detail::BaseImpl::set_child_finished()
{
    return ++m_children_ready == m_children.size();
}

void detail::BaseImpl::auto_release()
{
    if (m_auto_release)
    {
        release();
    }
}

void detail::BaseImpl::set_finished()
{
    m_finished = true;
}

void detail::BaseImpl::set_thread_affinity(const int affinity)
{
    m_thread_affinity = affinity;
}

void detail::BaseImpl::set_auto_release(const bool auto_release)
{
    m_auto_release = auto_release;
}

void detail::BaseImpl::add_parent(BaseImpl& impl)
{
    m_parents.emplace_back(&impl);
    impl.m_children.emplace_back(this);
}

TaskId detail::BaseImpl::id() const
{
    return std::hash<const BaseImpl*>{}(this);
}

std::vector<Edge> detail::BaseImpl::edges()
{
    std::vector<Edge> es;
    visit([&es](BaseImpl& i)
    {
        for (const auto p : i.m_parents)
        {
            es.push_back({static_cast<BaseImpl*>(p)->id(), i.id()});
        }
    });
    return es;
}

} // gcl
