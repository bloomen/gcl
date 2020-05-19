// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Contributors: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#include "gcl.h"

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace gcl
{

struct Async::Impl
{
    explicit
    Impl(const std::size_t n_threads)
    {
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            std::thread thread;
            try
            {
                thread = std::thread{&Impl::worker, this};
            }
            catch (...)
            {
                shutdown();
                throw;
            }
            try
            {
                m_threads.emplace_back(std::move(thread));
            }
            catch (...)
            {
                shutdown();
                thread.join();
                throw;
            }
        }
    }

    ~Impl()
    {
        shutdown();
    }

    void execute(std::function<void()> f)
    {
        if (m_threads.empty())
        {
            f();
        }
        else
        {
            {
                std::lock_guard<std::mutex> lock{m_mutex};
                m_functors.emplace(std::move(f));
            }
            m_cond_var.notify_one();
        }
    }

    void worker()
    {
        for (;;)
        {
            std::function<void()> f;
            {
                std::unique_lock<std::mutex> lock{m_mutex};
                m_cond_var.wait(lock, [this]
                {
                    return m_done || !m_functors.empty();
                });
                if (m_done && m_functors.empty())
                {
                    break;
                }
                f = std::move(m_functors.front());
                m_functors.pop();
            }
            f();
        }
    }

    void shutdown()
    {
        {
            std::lock_guard<std::mutex> lock{m_mutex};
            m_done = true;
        }
        m_cond_var.notify_all();
        for (std::thread& thread : m_threads)
        {
            thread.join();
        }
        m_threads.clear();
    }

private:
    bool m_done = false;
    std::vector<std::thread> m_threads;
    std::queue<std::function<void()>> m_functors;
    std::condition_variable m_cond_var;
    std::mutex m_mutex;
};

Async::Async(const std::size_t n_threads)
    : m_impl{std::make_unique<Impl>(n_threads)}
{}

Async::~Async() = default;

void Async::execute(std::function<void()> f)
{
    if (m_impl)
    {
        m_impl->execute(std::move(f));
    }
    else
    {
        f();
    }
}

void detail::BaseImpl::unvisit()
{
    if (!m_visited)
    {
        return;
    }
    for (BaseImpl* const p : m_parents)
    {
        p->unvisit();
    }
    m_visited = false;
}

void detail::BaseImpl::add_parent(BaseImpl& impl)
{
    m_parents.emplace_back(&impl);
}

TaskId detail::BaseImpl::id() const
{
    return std::hash<const BaseImpl*>{}(this);
}

std::vector<Edge> detail::BaseImpl::edges()
{
    unvisit();
    std::vector<Edge> es;
    visit_depth([&es](BaseImpl& i)
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
    std::vector<BaseImpl*> tasks;
    std::queue<BaseImpl*> q;
    q.emplace(this);
    tasks.push_back(this);
    m_visited = true;
    while (!q.empty())
    {
        const BaseImpl* const v = q.front();
        q.pop();
        for (BaseImpl* const w : v->m_parents)
        {
            if (!w->m_visited)
            {
                q.emplace(w);
                tasks.emplace_back(w);
                w->m_visited = true;
            }
        }
    }
    return tasks;
}

} // gcl
