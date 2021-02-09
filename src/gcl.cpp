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
using ActiveQueue = LockFreeQueue<moodycamel::ReaderWriterQueue<ITask*>>; // SpSc

class Processor
{
public:

    explicit 
    Processor(CompletedQueue& completed, const std::size_t initial_processor_size)
        : m_completed{completed}, m_active{initial_processor_size}
    {}

    ~Processor()
    {
        m_done = true;
        m_thread.join();
    }

    std::size_t size() const
    {
        return m_active.size();
    }

    void push(ITask* const task)
    {
        m_active.push(task);
    }

private:
    void worker()
    {
        while (!m_done)
        {
            if (const auto task = m_active.pop())
            {
                task->call();
                m_completed.push(task);
            }
            std::this_thread::yield();
        }
    }

    std::atomic<bool> m_done{false};
    CompletedQueue& m_completed;
    ActiveQueue m_active;
    std::thread m_thread{&Processor::worker, this};
};

}

struct Async::Impl
{
    explicit
    Impl(const std::size_t n_threads, const std::size_t initial_processor_size)
    {
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            m_processors.emplace_front(m_completed, initial_processor_size);
        }
    }

    ~Impl()
    {
        m_done = true;
        m_thread.join();
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
        while (!m_done)
        {
            if (const auto task = m_completed.pop())
            {
                on_completed(*task);
            }
            std::this_thread::yield();
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

    std::atomic<bool> m_done{false};
    std::forward_list<Processor> m_processors;
    CompletedQueue m_completed;
    std::thread m_thread{&Impl::worker, this};
};

Async::Async(const std::size_t n_threads, const std::size_t initial_processor_size)
    : m_impl{std::make_unique<Impl>(n_threads, initial_processor_size)}
{}

Async::~Async() = default;

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
