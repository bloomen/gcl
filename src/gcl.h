#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <vector>

namespace gcl
{

// Executor interface for running functions
class Exec
{
public:
    virtual ~Exec() = default;
    virtual void execute(std::function<void()> f) = 0;
};

// Async executor for parallel execution
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

// The unique id of a task
using TaskId = std::size_t;

// An edge between tasks
struct Edge
{
    TaskId parent;
    TaskId child;
};

namespace detail
{

struct CollectParents;

template<typename Result>
class BaseTask
{
public:

    template<typename Functor, typename... Parents>
    void init(Functor&& functor, Parents... parents);

    // Schedules this task and its parents for execution
    void schedule();
    void schedule(Exec& e);

    // Release this task's result and its parents' results
    void release();

    // Returns true if this task contains a valid shared state
    bool valid() const;

    // Waits for the task to finish
    void wait() const;

    // Waits for a given duration `d` for the task to finish
    template<typename Rep, typename Period>
    std::future_status wait_for(const std::chrono::duration<Rep, Period>& d) const;

    // Waits until time `tp` for the task to finish
    template<typename Clock, typename Duration>
    std::future_status wait_until(const std::chrono::time_point<Clock, Duration>& tp) const;

    // Returns the id of the task (unique but changes between runs)
    TaskId id() const;

    // Returns the edges between tasks
    std::vector<Edge> edges() const;

protected:
    BaseTask() = default;
    friend struct CollectParents;
    struct Impl;
    std::shared_ptr<Impl> m_impl;
};

} // detail

// The task type for general result types
template<typename Result>
class Task : public detail::BaseTask<Result>
{
public:
    // Returns the task's result. May throw
    const Result& get() const;
};

// The task type for reference result types
template<typename Result>
class Task<Result&> : public detail::BaseTask<Result&>
{
public:
    // Returns the task's result. May throw
    Result& get() const;
};

// The task type for void result
template<>
class Task<void> : public detail::BaseTask<void>
{
public:
    // Returns the task's result. May throw
    void get() const;
};

// Vector to hold multiple tasks of the same type
template<typename Result>
using Vec = std::vector<Task<Result>>;

// Creates a new task where parents can be Task and/or Vec
template<typename Functor, typename... Parents>
auto task(Functor&& functor, Parents... parents)
{
    Task<decltype(functor(parents...))> t;
    t.init(std::forward<Functor>(functor), std::move(parents)...);
    return t;
}

// Creates a new vector of tasks of the same type
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

    virtual void release() = 0;

    void schedule(Exec* e = nullptr);
    void visit_breadth(const std::function<void(BaseImpl&)>& f);
    void visit_depth(const std::function<void(BaseImpl&)>& f);
    void unvisit();
    void add_parent(BaseImpl& impl);
    std::size_t id() const;
    std::vector<Edge> edges();

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

template<typename F, typename Result, typename... TaskTypes>
void for_each_impl(const F& f, const Task<Result>& t, TaskTypes&&... tasks)
{
    f(t);
    for_each_impl(f, std::forward<TaskTypes>(tasks)...);
}

template<typename F, typename Result, typename... TaskTypes>
void for_each_impl(const F& f, Task<Result>&& t, TaskTypes&&... tasks)
{
    f(std::move(t));
    for_each_impl(f, std::forward<TaskTypes>(tasks)...);
}

template<typename F, typename Result, typename... TaskTypes>
void for_each_impl(const F& f, const Vec<Result>& ts, TaskTypes&&... tasks)
{
    for (const Task<Result>& t : ts) f(t);
    for_each_impl(f, std::forward<TaskTypes>(tasks)...);
}

template<typename F, typename Result, typename... TaskTypes>
void for_each_impl(const F& f, Vec<Result>&& ts, TaskTypes&&... tasks)
{
    for (Task<Result>&& t : ts) f(std::move(t));
    for_each_impl(f, std::forward<TaskTypes>(tasks)...);
}

template<typename F, typename... TaskTypes>
void for_each(const F& f, TaskTypes&&... tasks)
{
    for_each_impl(f, std::forward<TaskTypes>(tasks)...);
}

struct CollectParents
{
    BaseImpl* impl;
    template<typename P>
    void operator()(const P& p) const
    {
        impl->add_parent(*p.m_impl);
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
        auto func = std::bind(std::forward<Functor>(functor), std::move(parents)...);
        m_schedule = [this, func = std::move(func)](Exec* const e)
        {
            if (e)
            {
                auto pkg = std::make_shared<std::packaged_task<Result()>>(func);
                m_future = pkg->get_future();
                e->execute([pkg = std::move(pkg)]{ (*pkg)(); });
            }
            else
            {
                std::packaged_task<Result()> pkg{func};
                m_future = pkg.get_future();
                pkg();
            }
        };
    }

    void release() override
    {
        m_future = {};
    }

    std::shared_future<Result> m_future;
};

template<typename Result>
template<typename Functor, typename... Parents>
void BaseTask<Result>::init(Functor&& functor, Parents... parents)
{
    m_impl = std::make_shared<Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

template<typename Result>
void BaseTask<Result>::schedule()
{
    m_impl->unvisit();
    m_impl->visit_breadth([](BaseImpl& i){ i.schedule(); });
}

template<typename Result>
void BaseTask<Result>::schedule(Exec& e)
{
    m_impl->unvisit();
    m_impl->visit_breadth([&e](BaseImpl& i){ i.schedule(&e); });
}

template<typename Result>
void BaseTask<Result>::release()
{
    m_impl->unvisit();
    m_impl->visit_breadth([](BaseImpl& i){ i.release(); });
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
std::size_t BaseTask<Result>::id() const
{
    return m_impl->id();
}

template<typename Result>
std::vector<Edge> BaseTask<Result>::edges() const
{
    return m_impl->edges();
}

} // detail

template<typename Result>
const Result& Task<Result>::get() const
{
    return this->m_impl->m_future.get();
}

template<typename Result>
Result& Task<Result&>::get() const
{
    return this->m_impl->m_future.get();
}

inline
void Task<void>::get() const
{
    this->m_impl->m_future.get();
}

// Returns a task for releasing the task's result and its parents' results
template<typename Result>
Task<void> release(Task<Result> task)
{
    return task([](Task<Result> t){ t.release(); }, std::move(task));
}

// Thrown to cancel an operation
class Canceled : public std::exception
{
};

// Used to cancel operations
class CancelToken
{
public:
    void cancel()
    {
        m_canceled = true;
    }
    void uncancel()
    {
        m_canceled = false;
    }
    bool is_canceled() const
    {
        return m_canceled.load();
    }
private:
    std::atomic<bool> m_canceled{false};
};

// Used to cancel operations. Throws Canceled when token is canceled
inline
void cancel_point(const CancelToken& ct)
{
    if (ct.is_canceled())
    {
        throw gcl::Canceled{};
    }
}

// Waits for all tasks to finish
template<typename... TaskTypes>
void wait(const TaskTypes&... tasks)
{
    detail::for_each([](const auto& t){ t.wait(); }, tasks...);
}

// Waits for all tasks to finish (cancelable)
// Throws `Canceled` if waiting was canceled
template<typename... TaskTypes>
void wait_cl(const CancelToken& ct, const TaskTypes&... tasks)
{
    bool done = false;
    while (!done)
    {
        done = true;
        cancel_point(ct);
        detail::for_each([&done](const auto& t)
        {
            if (t.wait_for(std::chrono::microseconds{0}) != std::future_status::ready &&
                t.wait_for(std::chrono::microseconds{1}) != std::future_status::ready)
            {
                done = false;
            }
        },
        tasks...);
    }
}

// Waits for the first task to finish; only works for `Vec`
template<typename Result>
Task<Result> wait_any(const Vec<Result>& tasks)
{
    Task<Result>* result = nullptr;
    while (result == nullptr)
    {
        detail::for_each([&result,&tasks](const auto& t)
        {
            if (result == nullptr)
            {
                if (t.wait_for(std::chrono::microseconds{0}) == std::future_status::ready)
                {
                    result = &t;
                }
                else if (t.wait_for(std::chrono::microseconds{1}) == std::future_status::ready)
                {
                    result = &t;
                }
            }
        },
        tasks);
    }
    return *result;
}

// Waits for the first task to finish; only works for `Vec` (cancelable)
// Throws `Canceled` if waiting was canceled
template<typename Result>
Task<Result> wait_any_cl(const CancelToken& ct, const Vec<Result>& tasks)
{
    Task<Result>* result = nullptr;
    while (result == nullptr)
    {
        cancel_point(ct);
        detail::for_each([&result,&tasks](const auto& t)
        {
            if (result == nullptr)
            {
                if (t.wait_for(std::chrono::microseconds{0}) == std::future_status::ready)
                {
                    result = &t;
                }
                else if (t.wait_for(std::chrono::microseconds{1}) == std::future_status::ready)
                {
                    result = &t;
                }
            }
        },
        tasks);
    }
    return *result;
}

// Joins all tasks into a single waiting task
template<typename... TaskTypes>
Task<void> join(TaskTypes... tasks)
{
    return task([](const TaskTypes&... ts){ wait(ts...); }, std::move(tasks)...);
}

// Joins all tasks into a single waiting task (cancelable)
// Task's exception is `Canceled` if waiting was canceled
template<typename... TaskTypes>
Task<void> join_cl(const CancelToken& ct, TaskTypes... tasks)
{
    return task([&ct](const TaskTypes&... ts){ wait_cl(ct, ts...); }, std::move(tasks)...);
}

// Joins all tasks into a single waiting task
// Waits for the first task to finish; only works for `Vec`
template<typename Result>
Task<Result> join_any(Vec<Result> tasks)
{
    return task([](const Vec<Result>& ts){ return wait_any(ts).get(); }, std::move(tasks));
}

// Joins all tasks into a single waiting task (cancelable)
// Waits for the first task to finish; only works for `Vec`
// Task's exception is `Canceled` if waiting was canceled
template<typename Result>
Task<Result> join_any_cl(const CancelToken& ct, Vec<Result> tasks)
{
    return task([&ct](const Vec<Result>& ts){ return wait_any_cl(ct, ts).get(); }, std::move(tasks));
}

template<typename InputIt, typename UnaryOperation>
Task<void> for_each(InputIt first, InputIt last, UnaryOperation unary_op)
{
    const auto distance = std::distance(first, last);
    if (distance <= 0)
    {
        return task([]{ throw std::logic_error{"gcl::for_each: distance <= 0"}; });
    }
    Vec<void> tasks;
    tasks.reserve(static_cast<std::size_t>(distance));
    for (; first != last; ++first)
    {
        tasks.emplace_back([unary_op, value = *first]{ unary_op(value); });
    }
    return join(tasks);
}

template<typename InputIt, typename UnaryOperation>
Task<void> for_each_cl(const CancelToken& ct, InputIt first, InputIt last, UnaryOperation unary_op)
{
    const auto distance = std::distance(first, last);
    if (distance <= 0)
    {
        return task([]{ throw std::logic_error{"gcl::for_each_cl: distance <= 0"}; });
    }
    Vec<void> tasks;
    tasks.reserve(static_cast<std::size_t>(distance));
    for (; first != last; ++first)
    {
        tasks.emplace_back([unary_op, value = *first]{ unary_op(value); });
    }
    return join_cl(ct, tasks);
}

} // gcl
