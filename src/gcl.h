#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <type_traits>
#include <vector>

namespace gcl
{

class Executor
{
public:
    virtual ~Executor() = default;
    virtual void execute(const std::function<void()>& f) = 0;
};

class Sequential : public Executor
{
public:
    void execute(const std::function<void()>& f) override
    {
        f();
    }
};

class Parallel : public Executor
{
public:

    explicit
    Parallel(const std::size_t n_threads)
    {
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            std::thread thread;
            try
            {
                thread = std::thread{&Parallel::worker, this};
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

    ~Parallel()
    {
        shutdown();
    }

    void execute(const std::function<void()>& f) override
    {
        if (m_threads.empty())
        {
            f();
        }
        else
        {
            {
                std::lock_guard<std::mutex> lock{m_mutex};
                m_functors.push(f);
            }
            m_cond_var.notify_one();
        }
    }

private:

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

    bool m_done = false;
    std::vector<std::thread> m_threads;
    std::queue<std::function<void()>> m_functors;
    std::condition_variable m_cond_var;
    std::mutex m_mutex;
};

namespace detail
{

class iImpl;

template<typename Result>
class BaseTask;

template<typename Result>
iImpl* get_impl(const BaseTask<Result>& t);

template<typename Result>
class BaseTask
{
public:

    void schedule();
    void schedule(Executor& e);

    void release();
    void release(Executor& e);

    bool valid() const;

    void wait() const;

    template<typename Rep, typename Period>
    std::future_status wait_for(const std::chrono::duration<Rep, Period>& d) const;

    template<typename Clock, typename Duration>
    std::future_status wait_until(const std::chrono::time_point<Clock, Duration>& tp) const;

protected:
    template<typename R>
    friend iImpl* get_impl(const BaseTask<R>& t);

    struct Impl;
    std::shared_ptr<Impl> m_impl;
};

template<typename Result>
iImpl* get_impl(const BaseTask<Result>& t)
{
    return t.m_impl.get();
}

} // detail

template<typename Result>
class Task : public detail::BaseTask<Result>
{
public:

    template<typename Functor, typename... ParentResults>
    explicit
    Task(Functor&& functor, Task<ParentResults>... parents);

    const Result& get() const;
};

template<typename Result>
class Task<Result&> : public detail::BaseTask<Result&>
{
public:

    template<typename Functor, typename... ParentResults>
    explicit
    Task(Functor&& functor, Task<ParentResults>... parents);

    Result& get() const;
};

template<>
class Task<void> : public detail::BaseTask<void>
{
public:

    template<typename Functor, typename... ParentResults>
    explicit
    Task(Functor&& functor, Task<ParentResults>... parents);

    void get() const;
};

template<typename Functor, typename... ParentResults>
auto make_task(Functor&& functor, Task<ParentResults>... parents)
{
    return Task<decltype(functor(parents...))>{std::forward<Functor>(functor), std::move(parents)...};
}

namespace detail
{

class iImpl
{
public:
    virtual ~iImpl() = default;
    virtual void schedule(Executor* e = nullptr) = 0;
    virtual void release(Executor* e = nullptr) = 0;
    virtual const std::vector<iImpl*>& parents() const = 0;
    virtual void visit(const std::function<void(iImpl&)>& f) = 0;
    virtual void unvisit() = 0;
};

template<typename F>
void for_each_impl(const F&)
{
}

template<typename F, typename Arg, typename... Args>
void for_each_impl(const F& f, Arg&& arg, Args&&... args)
{
    f(std::forward<Arg>(arg));
    for_each_impl(f, std::forward<Args>(args)...);
}

template<typename F, typename... Args>
void for_each(const F& f, Args&&... args)
{
    for_each_impl(f, std::forward<Args>(args)...);
}

template<typename Result>
struct BaseTask<Result>::Impl : public iImpl
{
    template<typename Functor, typename... ParentResults>
    explicit
    Impl(Functor&& functor, Task<ParentResults>... parents)
    {
        for_each([this](const auto& p)
        {
            m_parents.emplace_back(detail::get_impl(p));
        }, parents...);
        auto pack_func = std::bind(std::forward<Functor>(functor), std::move(parents)...);
        m_schedule = [this, pack_func = std::move(pack_func)](Executor* e)
        {
            auto pack_task = std::make_shared<std::packaged_task<Result()>>(pack_func);
            m_future = pack_task->get_future();
            if (e)
            {
                e->execute([pack_task]{ (*pack_task)(); });
            }
            else
            {
                (*pack_task)();
            }
        };
    }

    void schedule(Executor* e = nullptr) override
    {
        m_schedule(e);
    }

    void release(Executor* e = nullptr) override
    {
        if (e)
        {
            e->execute([this]{ m_future = {}; });
        }
        else
        {
            m_future = {};
        }
    }

    const std::vector<iImpl*>& parents() const override
    {
        return m_parents;
    }

    void visit(const std::function<void(iImpl&)>& f) override
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
    }

    void unvisit() override
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

    bool m_visited = false;
    std::vector<iImpl*> m_parents;
    std::function<void(Executor*)> m_schedule;
    std::shared_future<Result> m_future;
};

template<typename Result>
void BaseTask<Result>::schedule()
{
    m_impl->unvisit();
    m_impl->visit([](iImpl& i){ i.schedule(); });
}

template<typename Result>
void BaseTask<Result>::schedule(Executor& e)
{
    m_impl->unvisit();
    m_impl->visit([&e](iImpl& i){ i.schedule(&e); });
}

template<typename Result>
void BaseTask<Result>::release()
{
    m_impl->unvisit();
    m_impl->visit([](iImpl& i){ i.release(); });
}

template<typename Result>
void BaseTask<Result>::release(Executor& e)
{
    m_impl->unvisit();
    m_impl->visit([&e](iImpl& i){ i.release(&e); });
}

template<typename Result>
void BaseTask<Result>::wait() const
{
    m_impl->m_future.wait();
}

template<typename Result>
bool BaseTask<Result>::valid() const
{
    return m_impl->m_future.valid();
}

template<typename Result>
template<typename Rep, typename Period>
std::future_status BaseTask<Result>::wait_for(const std::chrono::duration<Rep, Period>& d) const
{
    return m_impl->m_future.wait_for(d);
}

template<typename Result>
template<typename Clock, typename Duration>
std::future_status BaseTask<Result>::wait_until(const std::chrono::time_point<Clock, Duration>& tp) const
{
    return m_impl->m_future.wait_until(tp);
}

} // detail

template<typename Result>
template<typename Functor, typename... ParentResults>
Task<Result>::Task(Functor&& functor, Task<ParentResults>... parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<Result>::Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

template<typename Result>
const Result& Task<Result>::get() const
{
    return this->m_impl->m_future.get();
}

template<typename Result>
template<typename Functor, typename... ParentResults>
Task<Result&>::Task(Functor&& functor, Task<ParentResults>... parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<Result&>::Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

template<typename Result>
Result& Task<Result&>::get() const
{
    return this->m_impl->m_future.get();
}

template<typename Functor, typename... ParentResults>
Task<void>::Task(Functor&& functor, Task<ParentResults>... parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<void>::Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

inline
void Task<void>::get() const
{
    m_impl->m_future.get();
}

template<typename... Results>
void wait(const Task<Results>&... tasks)
{
    detail::for_each([](const auto& t){ t.wait(); }, tasks...);
}

} // gcl
