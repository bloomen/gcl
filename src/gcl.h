#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <tuple>
#include <type_traits>
#include <vector>

namespace gcl
{

class Exec
{
public:
    virtual ~Exec() = default;
    virtual void execute(std::function<void()> f) = 0;
};

class Async : public Exec
{
public:
    Async() = default;
    explicit
    Async(std::size_t n_threads);
    ~Async();
    void execute(std::function<void()> f) override;
private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
};

namespace detail
{

template<typename Result>
class BaseTask
{
protected:
    struct Impl;

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

    Impl& get_impl() const;

protected:
    BaseTask() = default;
    std::shared_ptr<Impl> m_impl;
};

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
auto task(Functor&& functor, Task<ParentResults>... parents) -> Task<decltype(functor(parents...))>
{
    Task<decltype(functor(parents...))> t;
    t.init(std::forward<Functor>(functor), std::move(parents)...);
    return t;
}

template<typename Functor, typename ParentResult>
auto task(Functor&& functor, Vec<ParentResult> parents) -> Task<decltype(functor(parents))>
{
    Task<decltype(functor(parents))> t;
    t.init(std::forward<Functor>(functor), std::move(parents));
    return t;
}

template<typename... Result>
auto vec(Task<Result>... tasks) -> Vec<typename std::tuple_element<0, std::tuple<Result...>>::type>
{
    using ResultType = typename std::tuple_element<0, std::tuple<Result...>>::type;
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
    void add_parent(BaseImpl& impl);

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

struct CollectParents
{
    BaseImpl* impl;
    template<typename P>
    void operator()(const P& p) const
    {
        impl->add_parent(p.get_impl());
    }
};

struct Waiter
{
    template<typename T>
    void operator()(const T& t) const
    {
        t.wait();
    }
};

template<typename Result>
struct BaseTask<Result>::Impl : public BaseImpl
{
    template<typename Functor, typename... Parents>
    explicit
    Impl(Functor&& functor, Parents... parents)
    {
        for_each(CollectParents{this}, parents...);
        std::function<Result()> func = std::bind(std::forward<Functor>(functor), std::move(parents)...);
        m_schedule = std::bind([this](std::function<Result()> func, Exec* const e)
        {
            auto package = std::make_shared<std::packaged_task<Result()>>(func);
            m_future = package->get_future();
            if (e)
            {
                e->execute([package]{ (*package)(); });
            }
            else
            {
                (*package)();
            }
        }, std::move(func), std::placeholders::_1);
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

template<typename Result>
typename BaseTask<Result>::Impl& BaseTask<Result>::get_impl() const
{
    return *m_impl;
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
    detail::for_each(detail::Waiter{}, tasks...);
}

template<typename Result>
void wait(const Vec<Result>& tasks)
{
    detail::for_each(detail::Waiter{}, tasks);
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
