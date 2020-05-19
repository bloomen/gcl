// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Contributors: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#pragma once

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
class Async : public gcl::Exec
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
    gcl::TaskId parent;
    gcl::TaskId child;
};

namespace detail
{

struct CollectParents;

template<typename Result>
class BaseTask
{
public:

    // Creates a child to this task (continuation)
    template<typename Functor>
    auto then(Functor&& functor) const;

    // Schedules this task and its parents for execution
    void schedule(); // runs functors on the current thread
    void schedule(gcl::Exec& e); // hands functors to the executor

    // Releases this task's result and its parents' results
    void release();

    // Returns true if this task contains a valid shared state
    bool valid() const;

    // Waits for the task to finish
    void wait() const;

    // Returns the id of the task (unique but changes between runs)
    gcl::TaskId id() const;

    // Returns the edges between tasks
    std::vector<gcl::Edge> edges() const;

protected:
    friend struct CollectParents;

    BaseTask() = default;

    template<typename Functor, typename... Parents>
    void init(Functor&& functor, Parents... parents);

    struct Impl;
    std::shared_ptr<Impl> m_impl;
};

} // detail

// The task type for general result types
template<typename Result>
class Task : public gcl::detail::BaseTask<Result>
{
public:
    // Returns the task's result. May throw
    const Result& get() const;

    template<typename Functor, typename... Parents>
    static Task create(Functor&& functor, Parents... parents)
    {
        Task<Result> t;
        t.init(std::forward<Functor>(functor), std::move(parents)...);
        return t;
    }

private:
    Task() = default;
};

// The task type for reference result types
template<typename Result>
class Task<Result&> : public gcl::detail::BaseTask<Result&>
{
public:
    // Returns the task's result. May throw
    Result& get() const;

    template<typename Functor, typename... Parents>
    static Task create(Functor&& functor, Parents... parents)
    {
        Task<Result&> t;
        t.init(std::forward<Functor>(functor), std::move(parents)...);
        return t;
    }

private:
    Task() = default;
};

// The task type for void result
template<>
class Task<void> : public gcl::detail::BaseTask<void>
{
public:
    // Returns the task's result. May throw
    void get() const;

    template<typename Functor, typename... Parents>
    static Task create(Functor&& functor, Parents... parents)
    {
        Task<void> t;
        t.init(std::forward<Functor>(functor), std::move(parents)...);
        return t;
    }

private:
    Task() = default;
};

// Vector to hold multiple tasks of the same type
template<typename Result>
using Vec = std::vector<gcl::Task<Result>>;

// Creates a new task
template<typename Functor>
auto task(Functor&& functor)
{
    return gcl::Task<decltype(functor())>::create(std::forward<Functor>(functor));
}

// Creates a new vector of tasks of the same type
template<typename... Result>
auto vec(gcl::Task<Result>... tasks)
{
    using ResultType = std::tuple_element_t<0, std::tuple<Result...>>;
    return gcl::Vec<ResultType>{std::move(tasks)...};
}

namespace detail
{

template<typename F>
void for_each_impl(const F&)
{}

template<typename F, typename Result, typename... Tasks>
void for_each_impl(const F& f, const gcl::Task<Result>& t, Tasks&&... tasks)
{
    f(t);
    for_each_impl(f, std::forward<Tasks>(tasks)...);
}

template<typename F, typename Result, typename... Tasks>
void for_each_impl(const F& f, gcl::Task<Result>&& t, Tasks&&... tasks)
{
    f(std::move(t));
    for_each_impl(f, std::forward<Tasks>(tasks)...);
}

template<typename F, typename Result, typename... Tasks>
void for_each_impl(const F& f, const gcl::Vec<Result>& ts, Tasks&&... tasks)
{
    for (const gcl::Task<Result>& t : ts) f(t);
    for_each_impl(f, std::forward<Tasks>(tasks)...);
}

template<typename F, typename Result, typename... Tasks>
void for_each_impl(const F& f, gcl::Vec<Result>&& ts, Tasks&&... tasks)
{
    for (gcl::Task<Result>&& t : ts) f(std::move(t));
    for_each_impl(f, std::forward<Tasks>(tasks)...);
}

} // detail

// Applies functor `f` to each task in `tasks` which can be of type `Task` and/or `Vec`
template<typename F, typename... Tasks>
void for_each(const F& f, Tasks&&... tasks)
{
    gcl::detail::for_each_impl(f, std::forward<Tasks>(tasks)...);
}

namespace detail
{

class BaseImpl
{
public:
    virtual ~BaseImpl() = default;

    virtual void schedule(Exec* e = nullptr) = 0;
    virtual void release() = 0;

    template<typename Functor>
    void visit_breadth(const Functor& f)
    {
        const std::vector<BaseImpl*> tasks = tasks_by_breadth();
        for (auto t = tasks.rbegin(); t != tasks.rend(); ++t)
        {
            f(**t);
        }
    }

    template<typename Functor>
    void visit_depth(const Functor& f)
    {
        if (m_visited)
        {
            return;
        }
        for (BaseImpl* const p : m_parents)
        {
            p->visit_depth(f);
        }
        f(*this);
        m_visited = true;
    }

    void unvisit();
    void add_parent(BaseImpl& impl);
    gcl::TaskId id() const;
    std::vector<gcl::Edge> edges();

protected:
    BaseImpl() = default;

    std::vector<BaseImpl*> tasks_by_breadth();

    bool m_visited = false;
    std::vector<BaseImpl*> m_parents;
};

struct CollectParents
{
    gcl::detail::BaseImpl* impl;
    template<typename P>
    void operator()(const P& p) const
    {
        impl->add_parent(*p.m_impl);
    }
};

template<typename Result>
struct BaseTask<Result>::Impl : BaseImpl
{
    template<typename Functor, typename... Parents>
    explicit
    Impl(Functor&& functor, Parents... parents)
    {
        gcl::for_each(gcl::detail::CollectParents{this}, parents...);
        m_functor = std::bind([f = std::forward<Functor>(functor)](Parents... ts) -> Result
                              {
                                  gcl::for_each([](const auto& t){ t.wait(); }, ts...);
                                  return f(std::move(ts)...);
                              }, std::move(parents)...);
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
auto BaseTask<Result>::then(Functor&& functor) const
{
    return gcl::Task<decltype(functor(static_cast<const gcl::Task<Result>&>(*this)))>::create(std::forward<Functor>(functor), static_cast<const gcl::Task<Result>&>(*this));
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
gcl::TaskId BaseTask<Result>::id() const
{
    return m_impl->id();
}

template<typename Result>
std::vector<gcl::Edge> BaseTask<Result>::edges() const
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

// Ties tasks together which can be of type `Task` and/or `Vec`
template<typename... Tasks>
class Tie
{
public:
    explicit
    Tie(Tasks... tasks)
        : m_tasks{std::move(tasks)...}
    {}

    // Creates a child to all tied tasks (continuation)
    template<typename Functor>
    auto then(Functor&& functor) const
    {
        return then_impl(std::forward<Functor>(functor), std::index_sequence_for<Tasks...>{});
    }

    const std::tuple<Tasks...>& tasks() const
    {
        return m_tasks;
    }

private:
    template<typename Functor, std::size_t... Is>
    auto then_impl(Functor&& functor, std::index_sequence<Is...>) const
    {
        return gcl::Task<decltype(functor(std::get<Is>(m_tasks)...))>::create(std::forward<Functor>(functor), std::get<Is>(m_tasks)...);
    }
    std::tuple<Tasks...> m_tasks;
};

// Ties tasks together where `tasks` can be of type `Task` and/or `Vec`
template<typename... Tasks>
auto tie(Tasks... tasks)
{
    return gcl::Tie<Tasks...>{std::move(tasks)...};
}

// Creates a child that waits for all tasks to finish where `tasks` can be of type `Task` and/or `Vec`
template<typename... Tasks>
gcl::Task<void> when(Tasks... tasks)
{
    return gcl::tie(std::move(tasks)...).then([](Tasks... ts){ gcl::for_each([](const auto& t){ t.get(); }, ts...); });
}

// Creates a child that waits for all tasks to finish that are part of `tie`
template<typename... Tasks>
gcl::Task<void> when(const gcl::Tie<Tasks...>& tie)
{
    return tie.then([](Tasks... ts){ gcl::for_each([](const auto& t){ t.get(); }, ts...); });
}

} // gcl
