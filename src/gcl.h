// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#pragma once
#include <cassert>
#include <future>
#include <memory>
#include <tuple>
#include <vector>

namespace gcl
{

// Callable interface which is created when scheduling and owned by executors
class Callable
{
public:
    virtual ~Callable() = default;
    virtual void call() = 0;
    //virtual void set_iterator(std::list<std::unique_ptr<Callable>>::iterator* it) = 0;
    //virtual std::list<std::unique_ptr<Callable>>::iterator* iterator() const = 0;
    //virtual void set_running() = 0;
    //virtual bool is_running() const = 0;
    //virtual void set_finished() = 0; // after this notify scheduling thread which can check if any children are ready
    virtual const std::vector<Callable*>& children() const = 0;
    virtual void parent_finished() = 0; // called within set_finished(); increments a counter
    virtual bool is_ready() const = 0; // returns true when all parents finished
};

// Executor interface for calling objects of Callable
class Exec
{
public:
    virtual ~Exec() = default;
    virtual void push(std::unique_ptr<Callable> callable) = 0;
    virtual void execute() = 0;
};

// Async executor for asynchronous execution
class Async : public gcl::Exec
{
public:
    Async() = default;
    explicit
    Async(std::size_t n_threads, std::size_t initial_queue_size = 32);
    ~Async();
    void push(std::unique_ptr<Callable> callable) override;
    void execute() override;
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
class BaseImpl;
}

using Cache = std::vector<gcl::detail::BaseImpl*>;

namespace detail
{

struct CollectParents;

template<typename Result>
class BaseTask
{
public:

    // Creates a child to this task (continuation)
    template<typename Functor>
    auto then(Functor&& functor) const &;

    // Creates a child to this task (continuation)
    template<typename Functor>
    auto then(Functor&& functor) &&;

    // Schedules this task and its parents for execution
    void schedule(gcl::Cache& cache); // runs functors on the current thread
    void schedule(gcl::Cache& cache, gcl::Exec& e); // hands functors to the executor

    // Releases this task's result and its parents' results
    void release(gcl::Cache& cache);

    // Returns true if this task contains a valid shared state
    bool valid() const;

    // Waits for the task to finish
    void wait() const;

    // Returns the id of the task (unique but changes between runs)
    gcl::TaskId id() const;

    // Returns the edges between tasks
    std::vector<gcl::Edge> edges(gcl::Cache& cache) const;

protected:
    friend struct CollectParents;

    BaseTask() = default;

    template<typename Functor, typename... Parents>
    void init(Functor&& functor, Parents&&... parents);

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
    static Task create(Functor&& functor, Parents&&... parents)
    {
        Task<Result> task;
        task.init(std::forward<Functor>(functor), std::forward<Parents>(parents)...);
        return task;
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
    static Task create(Functor&& functor, Parents&&... parents)
    {
        Task<Result&> task;
        task.init(std::forward<Functor>(functor), std::forward<Parents>(parents)...);
        return task;
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
    static Task create(Functor&& functor, Parents&&... parents)
    {
        Task<void> task;
        task.init(std::forward<Functor>(functor), std::forward<Parents>(parents)...);
        return task;
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
auto vec(const gcl::Task<Result>&... tasks)
{
    using ResultType = std::tuple_element_t<0, std::tuple<Result...>>;
    return gcl::Vec<ResultType>{tasks...};
}

// Creates a new vector of tasks of the same type
template<typename... Result>
auto vec(gcl::Task<Result>&&... tasks)
{
    using ResultType = std::tuple_element_t<0, std::tuple<Result...>>;
    return gcl::Vec<ResultType>{std::move(tasks)...};
}

namespace detail
{

template<typename Functor, typename Tuple, std::size_t... Is>
decltype(auto) call_impl(Functor&& f, Tuple&& t, std::index_sequence<Is...>)
{
    return std::forward<Functor>(f)(std::get<Is>(std::forward<Tuple>(t))...);
}

template<typename Functor, typename Tuple>
decltype(auto) call(Functor&& f, Tuple&& t)
{
    return call_impl(std::forward<Functor>(f), std::forward<Tuple>(t),
                     std::make_index_sequence<std::tuple_size<std::remove_reference_t<Tuple>>::value>{});
}

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
template<typename Functor, typename... Tasks>
void for_each(const Functor& f, Tasks&&... tasks)
{
    gcl::detail::for_each_impl(f, std::forward<Tasks>(tasks)...);
}

namespace detail
{

class BaseImpl
{
public:
    virtual ~BaseImpl() = default;

    virtual void schedule(Exec* exec = nullptr) = 0;
    virtual void release() = 0;

    template<typename Visitor>
    void visit(gcl::Cache& cache, const Visitor& visitor)
    {
        if (cache.empty() || cache.front() != this)
        {
            cache = tasks_by_breadth();
        }
        for (auto task = cache.begin(); task != cache.end(); ++task)
        {
            visitor(**task);
        }
    }

    void unflag();
    void add_parent(BaseImpl& impl);
    gcl::TaskId id() const;
    std::vector<gcl::Edge> edges(gcl::Cache& cache);
    Callable* callable() const;

protected:
    BaseImpl() = default;

    std::vector<BaseImpl*> tasks_by_breadth();

    bool m_flagged = false;
    std::vector<BaseImpl*> m_parents;
    std::vector<BaseImpl*> m_children;
    Callable* m_callable = nullptr;
};

struct CollectParents
{
    gcl::detail::BaseImpl* impl;
    template<typename Parent>
    void operator()(const Parent& parent) const
    {
        impl->add_parent(*parent.m_impl);
    }
};

template<typename Result>
class Binding
{
public:
    virtual ~Binding() = default;
    virtual Result evaluate() = 0;
};

template<typename Result, typename Functor, typename... Parents>
class BindingImpl : public gcl::detail::Binding<Result>
{
public:
    template<typename F, typename... P>
    explicit
    BindingImpl(F&& functor, P&&... parents)
        : m_functor{std::forward<F>(functor)}
        , m_parents{std::make_tuple(std::forward<P>(parents)...)}
    {}

    BindingImpl(const BindingImpl&) = delete;
    BindingImpl& operator=(const BindingImpl&) = delete;

    Result evaluate() override
    {
        return gcl::detail::call([this](auto&&... p) -> Result
        {
            gcl::for_each([](const auto& t){ t.wait(); }, p...);
            return m_functor(std::forward<decltype(p)>(p)...);
        }, m_parents);
    }

private:
    std::remove_reference_t<Functor> m_functor;
    std::tuple<std::remove_reference_t<Parents>...> m_parents;
};

template<typename Result>
struct Evaluate
{
    void operator()(std::promise<Result>& promise, gcl::detail::Binding<Result>& binding) const
    {
        try
        {
            promise.set_value(binding.evaluate());
        }
        catch (...)
        {
            promise.set_exception(std::current_exception());
        }
    }
};

template<>
struct Evaluate<void>
{
    void operator()(std::promise<void>& promise, gcl::detail::Binding<void>& binding) const
    {
        try
        {
            binding.evaluate();
            promise.set_value();
        }
        catch (...)
        {
            promise.set_exception(std::current_exception());
        }
    }
};

template<typename Result>
struct BaseTask<Result>::Impl : BaseImpl
{
    class CallableImpl : public gcl::Callable
    {
    public:
        CallableImpl(gcl::detail::Binding<Result>& binding, std::vector<Callable*> children, const std::size_t parents_count)
            : m_binding{binding}
            , m_children{std::move(children)}
            , m_parents_count{parents_count}
        {}

        std::future<Result> get_future()
        {
            return m_promise.get_future();
        }

        void call() override
        {
            gcl::detail::Evaluate<Result>{}(m_promise, m_binding);
        }

        const std::vector<Callable*>& children() const override
        {
            return m_children;
        }

        void parent_finished() override
        {
            ++m_parents_ready;
        }

        bool is_ready() const override
        {
            return m_parents_ready == m_parents_count;
        }

    private:
        gcl::detail::Binding<Result>& m_binding;
        std::promise<Result> m_promise;
        std::vector<Callable*> m_children;
        std::size_t m_parents_ready = 0;
        std::size_t m_parents_count = 0;
    };

    template<typename Functor, typename... Parents>
    explicit
    Impl(Functor&& functor, Parents&&... parents)
    {
        gcl::for_each(gcl::detail::CollectParents{this}, parents...);
        m_binding = std::make_unique<gcl::detail::BindingImpl<Result, Functor, Parents...>>(std::forward<Functor>(functor), std::forward<Parents>(parents)...);
    }

    void schedule(Exec* const exec) override
    {
        if (exec)
        {
            std::vector<Callable*> children;
            for (const auto child : m_children)
            {
                assert(child->callable());
                children.emplace_back(child->callable());
            }
            auto callable = std::make_unique<CallableImpl>(*m_binding, std::move(children), m_parents.size());
            m_future = callable->get_future();
            m_callable = callable.get();
            exec->push(std::move(callable));
        }
        else
        {
            std::promise<Result> promise;
            m_future = promise.get_future();
            gcl::detail::Evaluate<Result>{}(promise, *m_binding);
        }
    }

    void release() override
    {
        m_future = {};
    }

    std::unique_ptr<gcl::detail::Binding<Result>> m_binding;
    std::shared_future<Result> m_future;
};

template<typename Result>
template<typename Functor>
auto BaseTask<Result>::then(Functor&& functor) const &
{
    return gcl::Task<decltype(functor(static_cast<const gcl::Task<Result>&>(*this)))>::create(std::forward<Functor>(functor), static_cast<const gcl::Task<Result>&>(*this));
}

template<typename Result>
template<typename Functor>
auto BaseTask<Result>::then(Functor&& functor) &&
{
    return gcl::Task<decltype(functor(static_cast<gcl::Task<Result>&&>(*this)))>::create(std::forward<Functor>(functor), static_cast<gcl::Task<Result>&&>(*this));
}

template<typename Result>
template<typename Functor, typename... Parents>
void BaseTask<Result>::init(Functor&& functor, Parents&&... parents)
{
    m_impl = std::make_shared<Impl>(std::forward<Functor>(functor), std::forward<Parents>(parents)...);
}

template<typename Result>
void BaseTask<Result>::schedule(gcl::Cache& cache)
{
    m_impl->visit(cache, [](BaseImpl& i){ i.schedule(); });
}

template<typename Result>
void BaseTask<Result>::schedule(gcl::Cache& cache, Exec& exec)
{
    m_impl->visit(cache, [&exec](BaseImpl& i){ i.schedule(&exec); });
    exec.execute(); // TODO: move to Scheduler class
}

template<typename Result>
void BaseTask<Result>::release(gcl::Cache& cache)
{
    m_impl->visit(cache, [](BaseImpl& i){ i.release(); });
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
std::vector<gcl::Edge> BaseTask<Result>::edges(gcl::Cache& cache) const
{
    return m_impl->edges(cache);
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
    Tie(const Tasks&... tasks)
        : m_tasks{std::make_tuple(tasks...)}
    {}

    explicit
    Tie(Tasks&&... tasks)
        : m_tasks{std::make_tuple(std::move(tasks)...)}
    {}

    // Creates a child to all tied tasks (continuation)
    template<typename Functor>
    auto then(Functor&& functor) const &
    {
        return then_impl(std::forward<Functor>(functor), std::index_sequence_for<Tasks...>{});
    }

    // Creates a child to all tied tasks (continuation)
    template<typename Functor>
    auto then(Functor&& functor) &&
    {
        return then_impl(std::forward<Functor>(functor), std::index_sequence_for<Tasks...>{});
    }

    const std::tuple<Tasks...>& tasks() const
    {
        return m_tasks;
    }

private:

    template<typename Functor, std::size_t... Is>
    auto then_impl(Functor&& functor, std::index_sequence<Is...>) const &
    {
        return gcl::Task<decltype(functor(std::get<Is>(m_tasks)...))>::create(std::forward<Functor>(functor), std::get<Is>(m_tasks)...);
    }

    template<typename Functor, std::size_t... Is>
    auto then_impl(Functor&& functor, std::index_sequence<Is...>) &&
    {
        return gcl::Task<decltype(functor(std::get<Is>(m_tasks)...))>::create(std::forward<Functor>(functor), std::get<Is>(std::move(m_tasks))...);
    }

    std::tuple<Tasks...> m_tasks;
};

// Ties tasks together where `tasks` can be of type `Task` and/or `Vec`
template<typename... Tasks>
auto tie(Tasks&&... tasks)
{
    return gcl::Tie<std::remove_reference_t<Tasks>...>{std::forward<Tasks>(tasks)...};
}

// Creates a child that waits for all tasks to finish where `tasks` can be of type `Task` and/or `Vec`
template<typename... Tasks>
gcl::Task<void> when(Tasks... tasks)
{
    return gcl::tie(std::move(tasks)...).then([](auto&&... ts){ gcl::for_each([](const auto& t){ t.get(); }, std::forward<decltype(ts)>(ts)...); });
}

// Creates a child that waits for all tasks to finish that are part of `tie`
template<typename... Tasks>
gcl::Task<void> when(const gcl::Tie<Tasks...>& tie)
{
    return tie.then([](auto&&... ts){ gcl::for_each([](const auto& t){ t.get(); }, std::forward<decltype(ts)>(ts)...); });
}

} // gcl
