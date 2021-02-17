// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#include "gcl.h"

#include <forward_list>
#include <queue>
#include <set>
#include <thread>

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

    std::size_t size() const
    {
        return m_queue.size_approx();
    }

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
              const Async::Config& config,
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

    std::size_t size() const
    {
        return m_scheduled.size();
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
            std::this_thread::yield();
            if (m_config.inactive_processor_sleep_interval > std::chrono::microseconds{0} && !m_active)
            {
                std::this_thread::sleep_for(m_config.inactive_processor_sleep_interval);
            }
        }
    }

    const Async::Config& m_config;
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
    Impl(const std::size_t n_threads, Config config)
        : m_config{std::move(config)}
        , m_active{m_config.intially_active}
        , m_completed{m_config.initial_scheduler_queue_size}
    {
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            m_processors.emplace_front(i, m_config, m_active, m_completed);
        }
    }

    ~Impl()
    {
        m_done = true;
        m_thread.join();
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
            // find the "least busy" processor
            auto proc = m_processors.begin();
            auto processor = &*proc;
            ++proc;
            auto size = processor->size();
            while (proc != m_processors.end())
            {
                const auto current_size = proc->size();
                if (current_size < size)
                {
                    size = current_size;
                    processor = &*proc;
                }
                ++proc;
            }
            processor->push(&task);
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
            std::this_thread::yield();
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
        for (const auto child : task.children())
        {
            if (child->set_parent_finished())
            {
                execute(*child);
            }
        }
    }

    Config m_config;
    std::atomic<bool> m_done{false};
    std::atomic<bool> m_active;
    CompletedQueue m_completed;
    std::forward_list<Processor> m_processors;
    std::thread m_thread{&Impl::worker, this};
};

Async::Async(const std::size_t n_threads, Config config)
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

void detail::BaseImpl::set_auto_release(const bool auto_release)
{
    m_auto_release = auto_release;
}

void detail::BaseImpl::unvisit(const bool perform_reset)
{
    if (!m_visited)
    {
        return;
    }
    m_visited = false;
    if (perform_reset)
    {
        reset();
    }
    for (const auto parent : m_parents)
    {
        static_cast<BaseImpl*>(parent)->unvisit(perform_reset);
    }
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
    unvisit();
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
