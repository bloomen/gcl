#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <memory>
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

// Async executor for asynchronous execution
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

    // Creates a continuation to this task
    template<typename Functor>
    auto then(Functor&& functor);

    // Schedules this task and its parents for execution
    void schedule();
    void schedule(Exec& e);

    // Releases this task's result and its parents' results
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

// Creates a new task where parents can be `Task` and/or `Vec`
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

} // detail

// Applies functor `f` to each task in `tasks` which can be of type `Task` and/or `Vec`
template<typename F, typename... TaskTypes>
void for_each(const F& f, TaskTypes&&... tasks)
{
    detail::for_each_impl(f, std::forward<TaskTypes>(tasks)...);
}

namespace detail
{

class BaseImpl
{
public:
    virtual ~BaseImpl() = default;

    virtual void schedule(Exec* e = nullptr) = 0;
    virtual void release() = 0;

    void visit_breadth(const std::function<void(BaseImpl&)>& f);
    void visit_depth(const std::function<void(BaseImpl&)>& f);
    void unvisit();
    void add_parent(BaseImpl& impl);
    std::size_t id() const;
    std::vector<Edge> edges();

protected:
    BaseImpl() = default;
    bool m_visited = false;
    std::vector<BaseImpl*> m_parents;
};

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
        m_functor = std::bind(std::forward<Functor>(functor), std::move(parents)...);
    }

    void schedule(Exec* const e) override
    {
        if (e)
        {
            auto pkg = std::make_shared<std::packaged_task<Result()>>(m_functor);
            m_future = pkg->get_future();
            e->execute([pkg = std::move(pkg)]{ (*pkg)(); });
        }
        else
        {
            std::packaged_task<Result()> pkg{m_functor};
            m_future = pkg.get_future();
            pkg();
        }
    }

    void release() override
    {
        m_future = {};
    }

    std::function<Result()> m_functor;
    std::shared_future<Result> m_future;
};

template<typename Result>
template<typename Functor, typename... Parents>
void BaseTask<Result>::init(Functor&& functor, Parents... parents)
{
    m_impl = std::make_shared<Impl>(std::forward<Functor>(functor), std::move(parents)...);
}

template<typename Result>
template<typename Functor>
auto BaseTask<Result>::then(Functor&& functor)
{
    return task(std::forward<Functor>(functor), static_cast<const Task<Result>&>(*this));
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

// Waits for all tasks to finish which can be of type `Task` and/or `Vec`
template<typename... TaskTypes>
void wait(const TaskTypes&... tasks)
{
    for_each([](const auto& t){ t.wait(); }, tasks...);
}

// Waits for all tasks to finish which can be of type `Task` and/or `Vec`
// Propagates the first exception thrown by `tasks` if any
template<typename... TaskTypes>
void get(const TaskTypes&... tasks)
{
    wait(tasks...);
    for_each([](const auto& t){ t.get(); }, tasks...);
}

// Creates a task that waits for all tasks to finish where `tasks` can be of type `Task` and/or `Vec`
// Contains the first exception thrown by `tasks` if any
template<typename... TaskTypes>
Task<void> when(TaskTypes... tasks)
{
    return task([](const TaskTypes&... ts){ get(ts...); }, std::move(tasks)...);
}

} // gcl
