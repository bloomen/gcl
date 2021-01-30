// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#include "gcl.h"

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace gcl
{

namespace 
{

class SpScQ
{
public:

    std::size_t size() const
    {
        return m_queue.size();
    }

    void push(std::unique_ptr<Callable> callable) 
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        m_queue.push(std::move(callable));
    }

    std::unique_ptr<Callable> pop()
    {
        std::unique_ptr<Callable> callable;
        {
            std::lock_guard<std::mutex> lock{m_mutex};
            if (!m_queue.empty()) 
            {
                callable = std::move(m_queue.front());
                m_queue.pop();
            }
        }
        return callable;
    }

private:
    std::mutex m_mutex;
    std::queue<std::unique_ptr<Callable>> m_queue;
};

class Processor
{
public:

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
    Impl(const std::size_t n_threads)
        : m_processors(n_threads)
    {}

    void execute(std::unique_ptr<Callable> callable)
    {
        if (m_processors.empty())
        {
            callable->call();
        }
        else
        {
            std::size_t index = 0;
            std::size_t size = std::numeric_limits<std::size_t>::max();
            for (std::size_t i = 0; i < m_processors.size(); ++i) 
            {
                const auto current_size = m_processors[i].size();
                if (current_size < size) 
                {
                    size = current_size;
                    index = i;
                }
            }
            m_processors[index].push(std::move(callable));
        }
    }

private:
    std::vector<Processor> m_processors;
};

Async::Async(const std::size_t n_threads)
    : m_impl{std::make_unique<Impl>(n_threads)}
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
