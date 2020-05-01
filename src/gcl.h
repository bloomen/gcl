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

class Exec
{
public:
    virtual ~Exec() = default;
    virtual void execute(const std::function<void()>& f) = 0;
};

class Async : public Exec
{
public:

    explicit
    Async(const std::size_t n_threads);
    ~Async();

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
    void schedule(Exec& e);

    void release();
    void release(Exec& e);

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
    void init(Functor&& functor, Task<ParentResults>... parents);

    template<typename Functor, typename ParentResult>
    void init(Functor&& functor, std::vector<Task<ParentResult>> parents);

    const Result& get() const;
};

template<typename Result>
class Task<Result&> : public detail::BaseTask<Result&>
{
public:

    template<typename Functor, typename... ParentResults>
    void init(Functor&& functor, Task<ParentResults>... parents);

    template<typename Functor, typename ParentResult>
    void init(Functor&& functor, std::vector<Task<ParentResult>> parents);

    Result& get() const;
};

template<>
class Task<void> : public detail::BaseTask<void>
{
public:

    template<typename Functor, typename... ParentResults>
    void init(Functor&& functor, Task<ParentResults>... parents);

    template<typename Functor, typename ParentResult>
    void init(Functor&& functor, std::vector<Task<ParentResult>> parents);

    void get() const;
};

template<typename Result>
using Vec = std::vector<Task<Result>>;

template<typename Functor, typename... ParentResults>
auto task(Functor&& functor, Task<ParentResults>... parents)
{
    Task<decltype(functor(parents...))> t;
    t.init(std::forward<Functor>(functor), std::move(parents)...);
    return t;
}

template<typename Functor, typename ParentResult>
auto task(Functor&& functor, Vec<ParentResult> parents)
{
    Task<decltype(functor(parents))> t;
    t.init(std::forward<Functor>(functor), std::move(parents));
    return t;
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
public:
    virtual ~BaseImpl() = default;

    virtual void release(Exec* e = nullptr) = 0;

    void schedule(Exec* e = nullptr);
    void visit(const std::function<void(BaseImpl&)>& f);
    void unvisit();
    void visit_breadth_first(const std::function<void(BaseImpl&)>& f);

protected:
    BaseImpl() = default;

    bool m_visited = false;
    std::function<void(Exec*)> m_schedule;
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
        m_schedule = [this, pack_func = std::move(pack_func)](Exec* const e)
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

    void release(Exec* const e = nullptr) override
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
void BaseTask<Result>::schedule(Exec& e)
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
void BaseTask<Result>::release(Exec& e)
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
void Task<Result>::init(Functor&& functor, Task<ParentResults>... parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<Result>::Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

template<typename Result>
template<typename Functor, typename ParentResult>
void Task<Result>::init(Functor&& functor, Vec<ParentResult> parents)
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
void Task<Result&>::init(Functor&& functor, Task<ParentResults>... parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<Result&>::Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

template<typename Result>
template<typename Functor, typename ParentResult>
void Task<Result&>::init(Functor&& functor, Vec<ParentResult> parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<Result&>::Impl>(std::forward<Functor>(functor), std::move(parents));
}

template<typename Result>
Result& Task<Result&>::get() const
{
    return this->m_impl->m_future.get();
}

template<typename Functor, typename... ParentResults>
void Task<void>::init(Functor&& functor, Task<ParentResults>... parents)
{
    this->m_impl = std::make_shared<typename detail::BaseTask<void>::Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

template<typename Functor, typename ParentResult>
void Task<void>::init(Functor&& functor, Vec<ParentResult> parents)
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
Task<void> schedule(Exec& exec, Task<Results>... tasks)
{
    auto t = task([](const Task<Results>&... ts){ wait(ts...); }, std::move(tasks)...);
    t.schedule(exec);
    return t;
}

template<typename Result>
Task<void> schedule(Exec& exec, Vec<Result> tasks)
{
    auto t = task([](const Vec<Result>& ts){ wait(ts); }, std::move(tasks));
    t.schedule(exec);
    return t;
}

} // gcl
