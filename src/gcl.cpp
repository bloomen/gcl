// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#include "gcl.h"

#include <forward_list>
#include <queue>
#include <thread>

#include <readerwriterqueue.h>

namespace gcl
{

namespace
{

class SpScQ
{
public:

    explicit 
    SpScQ(const std::size_t initial_queue_size)
        : m_queue{initial_queue_size}
    {}

    std::size_t size() const
    {
        return m_queue.size_approx();
    }

    void push(std::unique_ptr<Callable> callable) 
    {
        m_queue.enqueue(std::move(callable));
    }

    std::unique_ptr<Callable> pop()
    {
        std::unique_ptr<Callable> callable;
        m_queue.try_dequeue(callable);
        return callable;
    }

private:
    moodycamel::ReaderWriterQueue<std::unique_ptr<Callable>> m_queue;
};

class Processor
{
public:

    explicit 
    Processor(const std::size_t initial_queue_size)
        : m_queue{initial_queue_size}
    {}

    std::size_t size() const
    {
        return m_queue.size();
    }

    void push(std::unique_ptr<Callable> callable) 
    {
        m_queue.push(std::move(callable));
    }

    ~Processor()
    {
        m_done = true;
        m_thread.join();
    }

private:
    void worker()
    {
        while (!m_done)
        {
            if (auto callable = m_queue.pop()) 
            {
                callable->call();
            }
            std::this_thread::yield();
        }
    }

    std::atomic<bool> m_done{false};
    SpScQ m_queue;
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
            m_processors.emplace_front(initial_queue_size);
        }
    }

    void execute(std::unique_ptr<Callable> callable)
    {
        if (m_processors.empty())
        {
            callable->call();
        }
        else
        {
            // use the "least busy" processor
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
            processor->push(std::move(callable));
        }
    }

private:
    std::forward_list<Processor> m_processors;
};

Async::Async(const std::size_t n_threads, const std::size_t initial_queue_size)
    : m_impl{std::make_unique<Impl>(n_threads, initial_queue_size)}
{}

Async::~Async() = default;

void Async::execute(std::unique_ptr<Callable> callable)
{
    m_impl->execute(std::move(callable));
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
