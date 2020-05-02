#include "gcl.h"

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace gcl
{

struct Async::Impl
{
    Impl(Async& async, const std::size_t n_threads)
        : m_async{async}
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

    void execute(Callable* f)
    {
        if (m_threads.empty())
        {
            f->call();
            m_async.release(f);
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
            Callable* f;
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
                f = m_functors.front();
                m_functors.pop();
            }
            f->call();
            m_async.release(f);
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
    Async& m_async;
    bool m_done = false;
    std::vector<std::thread> m_threads;
    std::queue<Callable*> m_functors;
    std::condition_variable m_cond_var;
    std::mutex m_mutex;
};

Async::Async(const std::size_t n_threads)
    : m_impl{new Impl{*this, n_threads}}
{}

Async::~Async() = default;

void Async::execute(Callable* f)
{
    if (m_impl)
    {
        m_impl->execute(f);
    }
    else
    {
        f->call();
        release(f);
    }
}

void Async::release(Callable* f)
{
    delete f;
}

void detail::BaseImpl::schedule(Exec* e)
{
    m_schedule(e);
}

void detail::BaseImpl::visit(const std::function<void(BaseImpl&)>& f)
{
    if (m_visited)
    {
        return;
    }
    for (auto p : m_parents)
    {
        p->visit(f);
    }
    f(*this);
    m_visited = true;
}

void detail::BaseImpl::unvisit()
{
    if (!m_visited)
    {
        return;
    }
    for (auto p : m_parents)
    {
        p->unvisit();
    }
    m_visited = false;
}

void detail::BaseImpl::visit_breadth_first(const std::function<void(BaseImpl&)>& f)
{
    std::vector<BaseImpl*> tasks;
    std::queue<BaseImpl*> q;
    q.emplace(this);
    tasks.push_back(this);
    m_visited = true;
    while (!q.empty())
    {
        const auto v = q.front();
        q.pop();
        for (const auto w : v->m_parents)
        {
            if (!w->m_visited)
            {
                q.emplace(w);
                tasks.emplace_back(w);
                w->m_visited = true;
            }
        }
    }
    for (auto t = tasks.rbegin(); t != tasks.rend(); ++t)
    {
        f(**t);
    }
}

void Task<void>::get() const
{
    this->m_impl->m_future.get();
}

} // gcl
