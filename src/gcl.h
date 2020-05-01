#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <tuple>
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
    void execute(const std::function<void()>& f) override;
};

class Parallel : public Executor
{
public:

    explicit
    Parallel(const std::size_t n_threads);
    ~Parallel();

    void execute(const std::function<void()>& f) override;

private:
    void worker();
    void shutdown();

    bool m_done = false;
    std::vector<std::thread> m_threads;
    std::queue<std::function<void()>> m_functors;
    std::condition_variable m_cond_var;
    std::mutex m_mutex;
};

namespace detail
{

class BaseImpl;

template<typename Result>
class BaseTask;

template<typename Result>
BaseImpl* get_impl(const BaseTask<Result>& t);

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
    BaseTask() = default;

    template<typename R>
    friend BaseImpl* get_impl(const BaseTask<R>& t);

    struct Impl;
    std::shared_ptr<Impl> m_impl;

private:
    ~BaseTask() = default;
};

template<typename Result>
BaseImpl* get_impl(const BaseTask<Result>& t)
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

    template<typename Functor, typename ParentResult>
    explicit
    Task(Functor&& functor, std::vector<Task<ParentResult>> parents);

    const Result& get() const;
};

template<typename Result>
class Task<Result&> : public detail::BaseTask<Result&>
{
public:

    template<typename Functor, typename... ParentResults>
    explicit
    Task(Functor&& functor, Task<ParentResults>... parents);

    template<typename Functor, typename ParentResult>
    explicit
    Task(Functor&& functor, std::vector<Task<ParentResult>> parents);

    Result& get() const;
};

template<>
class Task<void> : public detail::BaseTask<void>
{
public:

    template<typename Functor, typename... ParentResults>
    explicit
    Task(Functor&& functor, Task<ParentResults>... parents);

    template<typename Functor, typename ParentResult>
    explicit
    Task(Functor&& functor, std::vector<Task<ParentResult>> parents);

    void get() const;
};

template<typename Result>
using Vec = std::vector<Task<Result>>;

template<typename Functor, typename... ParentResults>
auto task(Functor&& functor, Task<ParentResults>... parents)
{
    return Task<decltype(functor(parents...))>{std::forward<Functor>(functor), std::move(parents)...};
}

template<typename Functor, typename ParentResult>
auto task(Functor&& functor, Vec<ParentResult> parents)
{
    return Task<decltype(functor(parents))>{std::forward<Functor>(functor), std::move(parents)};
}

template<typename... Result>
auto vec(Task<Result>... tasks)
{
    using ResultType = std::tuple_element_t<0, std::tuple<Result...>>;
    return Vec<ResultType>{std::move(tasks)...};
}

namespace detail
{

class BaseImpl
{
protected:
    BaseImpl() = default;
    virtual ~BaseImpl() = default;

    virtual void release(Executor* e = nullptr) = 0;

    void schedule(Executor* e = nullptr);
    void visit(const std::function<void(BaseImpl&)>& f);
    void unvisit();
    void visit_breadth_first(const std::function<void(BaseImpl&)>& f);

    bool m_visited = false;
    std::function<void(Executor*)> m_schedule;
    std::vector<BaseImpl*> m_parents;
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

template<typename F, typename... Results>
void for_each(const F& f, const Task<Results>&... tasks)
{
    for_each_impl(f, tasks...);
}

template<typename F, typename Result>
void for_each(const F& f, const Vec<Result>& tasks)
{
    for (const auto& t : tasks)
    {
        f(t);
    }
}

template<typename Result>
struct BaseTask<Result>::Impl : public BaseImpl
{
    template<typename Functor, typename... Parents>
    explicit
    Impl(Functor&& functor, Parents... parents)
    {
        for_each([this](const auto& p)
        {
            m_parents.emplace_back(detail::get_impl(p));
        }, parents...);
        auto pack_func = std::bind(std::forward<Functor>(functor), std::move(parents)...);
        m_schedule = [this, pack_func = std::move(pack_func)](Executor* const e)
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

    void release(Executor* const e = nullptr) override
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

    std::shared_future<Result> m_future;
};

template<typename Result>
void BaseTask<Result>::schedule()
{
    m_impl->unvisit();
    m_impl->visit_breadth_first([](BaseImpl& i){ i.schedule(); });
}

template<typename Result>
void BaseTask<Result>::schedule(Executor& e)
{
    m_impl->unvisit();
    m_impl->visit_breadth_first([&e](BaseImpl& i){ i.schedule(&e); });
}

template<typename Result>
void BaseTask<Result>::release()
{
    m_impl->unvisit();
    m_impl->visit_breadth_first([](BaseImpl& i){ i.release(); });
}

template<typename Result>
void BaseTask<Result>::release(Executor& e)
{
    m_impl->unvisit();
    m_impl->visit_breadth_first([&e](BaseImpl& i){ i.release(&e); });
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
template<typename Functor, typename ParentResult>
Task<Result>::Task(Functor&& functor, Vec<ParentResult> parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<Result>::Impl>(std::forward<Functor>(functor), std::move(parents));
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
template<typename Functor, typename ParentResult>
Task<Result&>::Task(Functor&& functor, Vec<ParentResult> parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<Result&>::Impl>(std::forward<Functor>(functor), std::move(parents));
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

template<typename Functor, typename ParentResult>
Task<void>::Task(Functor&& functor, Vec<ParentResult> parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<void>::Impl>(std::forward<Functor>(functor), std::move(parents));
}

template<typename... Results>
void wait(const Task<Results>&... tasks)
{
    detail::for_each([](const auto& t){ t.wait(); }, tasks...);
}

template<typename Result>
void wait(const Vec<Result>& tasks)
{
    detail::for_each([](const auto& t){ t.wait(); }, tasks);
}

template<typename... Results>
Task<void> schedule(Task<Results>... tasks)
{
    auto t = task([](const Task<Results>&... ts){ wait(ts...); }, std::move(tasks)...);
    t.schedule();
    return t;
}

template<typename Result>
Task<void> schedule(Vec<Result> tasks)
{
    auto t = task([](const Vec<Result>& ts){ wait(ts); }, std::move(tasks));
    t.schedule();
    return t;
}

template<typename... Results>
Task<void> schedule(Executor& exec, Task<Results>... tasks)
{
    auto t = task([](const Task<Results>&... ts){ wait(ts...); }, std::move(tasks)...);
    t.schedule(exec);
    return t;
}

template<typename Result>
Task<void> schedule(Executor& exec, Vec<Result> tasks)
{
    auto t = task([](const Vec<Result>& ts){ wait(ts); }, std::move(tasks));
    t.schedule(exec);
    return t;
}

} // gcl
