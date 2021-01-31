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

class CompletedQueue // MpSc
{
public:

    CompletedQueue() = default;

    CompletedQueue(const CompletedQueue&) = delete;
    CompletedQueue& operator=(const CompletedQueue&) = delete;

    void push(Callable* const it) 
    {
        m_queue.enqueue(it);
    }

    Callable* pop()
    {
        Callable* callable = nullptr;
        m_queue.try_dequeue(callable);
        return callable;
    }

private:
    moodycamel::ConcurrentQueue<Callable*> m_queue;
};

class ActiveQueue // SpSc
{
public:

    explicit 
    ActiveQueue(const std::size_t initial_queue_size)
        : m_queue{initial_queue_size}
    {}

    ActiveQueue(const ActiveQueue&) = delete;
    ActiveQueue& operator=(const ActiveQueue&) = delete;

    std::size_t size() const
    {
        return m_queue.size_approx();
    }

    void push(Callable* const callable) 
    {
        m_queue.enqueue(callable);
    }

    Callable* pop()
    {
        Callable* callable = nullptr;
        m_queue.try_dequeue(callable);
        return callable;
    }

private:
    moodycamel::ReaderWriterQueue<Callable*> m_queue;
};

class Processor
{
public:

    explicit 
    Processor(CompletedQueue& completed, const std::size_t initial_queue_size)
        : m_completed{completed}, m_active{initial_queue_size}
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

    void push(Callable* const callable) 
    {
        m_active.push(callable);
    }

private:
    void worker()
    {
        while (!m_done)
        {
            if (auto callable = m_active.pop()) 
            {
                callable->call();
                m_completed.push(callable);
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
    Impl(const std::size_t n_threads, const std::size_t initial_queue_size)
    {
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            m_processors.emplace_front(m_completed, initial_queue_size);
        }
    }

    ~Impl()
    {
        m_running = false;
        m_thread.join();
    }

    void push(Callable& callable)
    {
        if (m_processors.empty())
        {
            callable.call();
        }
        else
        {
            m_callables.insert(&callable);
        }
    }

    void execute()
    {
        if (m_processors.empty()) 
        {
            return;
        }
        m_running = true;
    }

private:
    void worker()
    {
        while (!m_running)
        {
            std::this_thread::yield();
        }
        for (const auto& callable : m_callables)
        {
            if (callable->is_ready())
            {
                run(callable);
            }
        }
        while (m_running)
        {
            if (auto completed = m_completed.pop())
            {
                for (const auto child : completed->children())
                {
                    child->parent_finished();
                    if (child->is_ready())
                    {
                        run(child);
                    }
                }
                m_callables.erase(completed);
            }
            std::this_thread::yield();
        }
    }

    void run(Callable* const callable)
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
        processor->push(callable);
    }

    std::atomic<bool> m_running{false};
    std::set<Callable*> m_callables;
    std::forward_list<Processor> m_processors;
    CompletedQueue m_completed;
    std::thread m_thread{&Impl::worker, this};
};

Async::Async(const std::size_t n_threads, const std::size_t initial_queue_size)
    : m_impl{std::make_unique<Impl>(n_threads, initial_queue_size)}
{}

Async::~Async() = default;

void Async::push(Callable& callable)
{
    m_impl->push(callable);
}

void Async::execute()
{
    m_impl->execute();
}

void detail::BaseImpl::unflag()
{
    if (!m_flagged)
    {
        return;
    }
    for (BaseImpl* const p : m_parents)
    {
        p->unflag();
    }
    m_flagged = false;
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

std::vector<Edge> detail::BaseImpl::edges(gcl::Cache& cache)
{
    std::vector<Edge> es;
    visit(cache, [&es](BaseImpl& i)
    {
        for (const BaseImpl* const p : i.m_parents)
        {
            es.push_back({p->id(), i.id()});
        }
    });
    return es;
}

std::vector<detail::BaseImpl*> detail::BaseImpl::tasks_by_breadth()
{
    unflag();
    std::vector<BaseImpl*> tasks;
    std::queue<BaseImpl*> q;
    q.emplace(this);
    tasks.push_back(this);
    m_flagged = true;
    while (!q.empty())
    {
        const BaseImpl* const v = q.front();
        q.pop();
        for (BaseImpl* const w : v->m_parents)
        {
            if (!w->m_flagged)
            {
                q.emplace(w);
                tasks.emplace_back(w);
                w->m_flagged = true;
            }
        }
    }
    return tasks;
}

} // gcl
