// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <tuple>
#include <vector>

#ifndef GCL_ASSERT
#ifdef NDEBUG
#define GCL_ASSERT(x)
#else
#include <cassert>
#define GCL_ASSERT(x) assert(x)
#endif
#endif

namespace gcl
{

// Task interface used for scheduling
class ITask
{
public:
    virtual ~ITask() = default;
    virtual void call() = 0;
    virtual const std::vector<ITask*>& parents() const = 0;
    virtual const std::vector<ITask*>& children() const = 0;
    virtual bool set_parent_finished() = 0;
    virtual bool set_child_finished() = 0;
    virtual void auto_release() = 0;
};

// Executor interface for running tasks
class Exec
{
public:
    virtual ~Exec() = default;
    virtual void set_active(bool active) = 0;
    virtual void execute(ITask& task) = 0;
};

// Config struct for the Async class
struct AsyncConfig
{
    bool active = true;
    bool processor_yields = true;
    bool scheduler_yields = true;
    std::chrono::microseconds inactive_processor_sleep_interval = std::chrono::microseconds{1000};
    std::chrono::microseconds inactive_scheduler_sleep_interval = std::chrono::microseconds{1000};
    std::size_t initial_processor_queue_size = 8;
    std::size_t initial_scheduler_queue_size = 32;
    std::function<void(std::size_t thread_index)> on_processor_thread_started;
    std::function<void()> on_scheduler_thread_started;
};

// Async executor for asynchronous execution
class Async : public gcl::Exec
{
public:
    explicit
    Async(std::size_t n_threads = 0, gcl::AsyncConfig config = {});
    ~Async();

    void set_active(bool active) override;
    void execute(ITask& task) override;
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
    auto then(Functor&& functor) const &;

    // Creates a child to this task (continuation)
    template<typename Functor>
    auto then(Functor&& functor) &&;

    // Schedules this task and its parents for execution. Returns true if successfully scheduled and false
    // if already scheduled and not finished. The result of this function call is valid == true.
    bool schedule(gcl::Exec& e);

    // Returns true if this task was scheduled at least once and not released
    bool valid() const;

    // Returns true if this task has a result
    bool has_result() const;

    // Auto-release means automatic result clean-up once a parent's result was fully consumed
    void set_auto_release(bool auto_release);

    // Releases this task's result and its parents' results. Returns true if successfully scheduled and false
    // if already scheduled and not finished. The result of this function call is valid == false.
    bool release();

    // Returns the id of the task (unique but changes between runs)
    gcl::TaskId id() const;

    // Returns the edges between tasks
    std::vector<gcl::Edge> edges() const;

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

    // Returns the task's result. Returns null if no result available. May throw
    const Result* get() const;

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

    // Returns the task's result. Returns null if no result available. May throw
    Result* get() const;

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

    // Returns true if the task finished, false otherwise. May throw
    bool get() const;

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
    static_assert(sizeof...(tasks) > 0, "Need to provide at least one task");
    using ResultType = std::tuple_element_t<0, std::tuple<Result...>>;
    return gcl::Vec<ResultType>{tasks...};
}

// Creates a new vector of tasks of the same type
template<typename... Result>
auto vec(gcl::Task<Result>&&... tasks)
{
    static_assert(sizeof...(tasks) > 0, "Need to provide at least one task");
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

// Applies functor `f` to each task in `tasks` which can be of type `Task` and/or `Vec`
template<typename Functor, typename... Tasks>
void for_each(const Functor& f, Tasks&&... tasks)
{
    gcl::detail::for_each_impl(f, std::forward<Tasks>(tasks)...);
}

class BaseImpl : public gcl::ITask
{
public:
    virtual ~BaseImpl() = default;

    const std::vector<gcl::ITask*>& parents() const override;
    const std::vector<gcl::ITask*>& children() const override;
    bool set_parent_finished() override;
    bool set_child_finished() override;
    void auto_release() override;

    virtual void reset() = 0;
    virtual void schedule(Exec& exec) = 0;
    virtual void release() = 0;

    void set_auto_release(bool auto_release);

    template<typename Visitor>
    void visit(const Visitor& visitor)
    {
        if (m_visited)
        {
            return;
        }
        m_visited = true;
        for (const auto& parent : m_parents)
        {
            static_cast<BaseImpl*>(parent)->visit(visitor);
        }
        visitor(*this);
    }

    void unvisit(bool perform_reset = false);

    void add_parent(BaseImpl& impl);
    gcl::TaskId id() const;
    std::vector<gcl::Edge> edges();

protected:
    BaseImpl() = default;

    std::atomic<bool> m_auto_release{false};
    bool m_visited = true;
    std::vector<gcl::ITask*> m_parents;
    std::vector<gcl::ITask*> m_children;
    std::uint32_t m_parents_ready = 0;
    std::uint32_t m_children_ready = 0;
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
class ChannelElement
{
public:
    ChannelElement(const Result& value)
        : m_value{value}
        , m_has_value{true}
    {}
    ChannelElement(Result&& value)
        : m_value{std::move(value)}
        , m_has_value{true}
    {}
    ChannelElement(const std::exception_ptr& exception)
        : m_exception{exception}
    {}
    ChannelElement(std::exception_ptr&& exception)
        : m_exception{std::move(exception)}
    {}

    ChannelElement(const ChannelElement&) = delete;
    ChannelElement& operator=(const ChannelElement&) = delete;

    ChannelElement(ChannelElement&& other) noexcept
    {
        m_has_value = other.m_has_value;
        if (m_has_value)
        {
            new (&m_value) Result{std::move(other.m_value)};
        }
        else
        {
            new (&m_exception) std::exception_ptr{std::move(other.m_exception)};
        }
    }

    ChannelElement& operator=(ChannelElement&&) = delete;

    bool has_value() const
    {
        return m_has_value;
    }

    const Result* value() const
    {
        return &m_value;
    }

    const std::exception_ptr& exception() const
    {
        return m_exception;
    }

    ~ChannelElement()
    {
        if (m_has_value)
        {
            m_value.~Result();
        }
        else
        {
            m_exception.~exception_ptr();
        }
    }

private:
    union 
    {
        Result m_value;
        std::exception_ptr m_exception;
    };
    bool m_has_value = false;
};

template<typename Result>
class ChannelElement<Result&>
{
public:
    ChannelElement(Result& value)
        : m_value{&value}
        , m_has_value{true}
    {}
    ChannelElement(const std::exception_ptr& exception)
        : m_exception{exception}
    {}
    ChannelElement(std::exception_ptr&& exception)
        : m_exception{std::move(exception)}
    {}

    ChannelElement(const ChannelElement&) = delete;
    ChannelElement& operator=(const ChannelElement&) = delete;

    ChannelElement(ChannelElement&& other) noexcept
    {
        m_has_value = other.m_has_value;
        if (m_has_value)
        {
            new (&m_value) Result*{std::move(other.m_value)};
        }
        else
        {
            new (&m_exception) std::exception_ptr{std::move(other.m_exception)};
        }
    }

    ChannelElement& operator=(ChannelElement&&) = delete;

    bool has_value() const
    {
        return m_has_value;
    }

    Result* value()
    {
        return m_value;
    }

    const std::exception_ptr& exception() const
    {
        return m_exception;
    }

    ~ChannelElement()
    {
        if (!m_has_value)
        {
            m_exception.~exception_ptr();
        }
    }

private:
    union 
    {
        Result* m_value;
        std::exception_ptr m_exception;
    };
    bool m_has_value = false;
};

template<>
class ChannelElement<void>
{
public:
    ChannelElement()
    {}
    ChannelElement(const std::exception_ptr& exception)
        : m_exception{exception}
    {}
    ChannelElement(std::exception_ptr&& exception)
        : m_exception{std::move(exception)}
    {}

    ChannelElement(const ChannelElement&) = delete;
    ChannelElement& operator=(const ChannelElement&) = delete;
    
    ChannelElement(ChannelElement&& other) = default;
    ChannelElement& operator=(ChannelElement&&) = delete;

    bool has_value() const
    {
        return !m_exception;
    }

    const std::exception_ptr& exception() const
    {
        return m_exception;
    }

private:
    std::exception_ptr m_exception;
};

template<typename Result>
class Channel
{
public:
    Channel() = default;

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    // producer
    void set(gcl::detail::ChannelElement<Result>&& element)
    {
        new (m_storage) gcl::detail::ChannelElement<Result>{std::move(element)};
        m_element.store(reinterpret_cast<gcl::detail::ChannelElement<Result>*>(m_storage));
    }

    // consumer
    gcl::detail::ChannelElement<Result>* get() const
    {
        return m_element.load();
    }

private:
    char m_storage[sizeof(gcl::detail::ChannelElement<Result>)];
    std::atomic<gcl::detail::ChannelElement<Result>*> m_element{nullptr};
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
    void operator()(gcl::detail::Channel<Result>& channel, gcl::detail::Binding<Result>& binding) const
    {
        try
        {
            channel.set(binding.evaluate());
        }
        catch (...)
        {
            channel.set(std::current_exception());
        }
    }
};

template<>
struct Evaluate<void>
{
    void operator()(gcl::detail::Channel<void>& channel, gcl::detail::Binding<void>& binding) const
    {
        try
        {
            binding.evaluate();
            channel.set({});
        }
        catch (...)
        {
            channel.set(std::current_exception());
        }
    }
};

template<typename Result>
struct BaseTask<Result>::Impl : BaseImpl
{
    template<typename Functor, typename... Parents>
    explicit
    Impl(Functor&& functor, Parents&&... parents)
    {
        gcl::detail::for_each(gcl::detail::CollectParents{this}, parents...);
        m_binding = std::make_unique<gcl::detail::BindingImpl<Result, Functor, Parents...>>(std::forward<Functor>(functor), std::forward<Parents>(parents)...);
    }

    void reset() override
    {
        m_parents_ready = 0;
        m_children_ready = 0;
        m_channel.reset();
        m_channel = std::make_unique<Channel<Result>>();
    }

    void schedule(Exec& exec) override
    {
        if (m_parents.empty())
        {
            exec.execute(*this);
        }
    }

    void call() override
    {
        gcl::detail::Evaluate<Result>{}(*m_channel, *m_binding);
    }

    void release() override
    {
        m_channel.reset();
    }

    std::unique_ptr<gcl::detail::Binding<Result>> m_binding;
    std::unique_ptr<Channel<Result>> m_channel;
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
bool BaseTask<Result>::schedule(Exec& exec)
{
    if (valid() && !has_result())
    {
        return false;
    }
    m_impl->unvisit(true);
    m_impl->visit([&exec](BaseImpl& i){ i.schedule(exec); });
    return true;
}

template<typename Result>
bool BaseTask<Result>::valid() const
{
    return m_impl->m_channel != nullptr;
}

template<typename Result>
bool BaseTask<Result>::has_result() const
{
    return m_impl->m_channel && m_impl->m_channel->get();
}

template<typename Result>
void BaseTask<Result>::set_auto_release(const bool auto_release)
{
    m_impl->unvisit();
    m_impl->visit([auto_release](BaseImpl& i){ i.set_auto_release(auto_release); });
}

template<typename Result>
bool BaseTask<Result>::release()
{
    if (valid() && !has_result())
    {
        return false;
    }
    m_impl->unvisit();
    m_impl->visit([](BaseImpl& i){ i.release(); });
    return true;
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
const Result* Task<Result>::get() const
{
    if (this->m_impl->m_channel)
    {
        if (const auto element = this->m_impl->m_channel->get())
        {
            if (element->has_value())
            {
                return element->value();
            }
            else
            {
                std::rethrow_exception(element->exception());
            }
        }
    }
    return nullptr;
}

template<typename Result>
Result* Task<Result&>::get() const
{
    if (this->m_impl->m_channel)
    {
        if (const auto element = this->m_impl->m_channel->get())
        {
            if (element->has_value())
            {
                return element->value();
            }
            else
            {
                std::rethrow_exception(element->exception());
            }
        }
    }
    return nullptr;
}

inline
bool Task<void>::get() const
{
    if (this->m_impl->m_channel)
    {
        if (const auto element = this->m_impl->m_channel->get())
        {
            if (!element->has_value())
            {
                std::rethrow_exception(element->exception());
            }
            return true;
        }
    }
    return false;
}

// Ties tasks together which can be of type `Task` and/or `Vec`
template<typename... Tasks>
class Tie
{
public:
    static_assert(sizeof...(Tasks) > 0, "Need to provide at least one task");

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
    return gcl::tie(std::move(tasks)...).then([](auto&&... ts){ gcl::detail::for_each([](const auto& t){ t.get(); }, std::forward<decltype(ts)>(ts)...); });
}

// Creates a child that waits for all tasks to finish that are part of `tie`
template<typename... Tasks>
gcl::Task<void> when(const gcl::Tie<Tasks...>& tie)
{
    return tie.then([](auto&&... ts){ gcl::detail::for_each([](const auto& t){ t.get(); }, std::forward<decltype(ts)>(ts)...); });
}

// Can be used to facilitate task canceling
class CancelToken
{
public:
    // Called from outside the task
    void set_canceled(const bool canceled = true)
    {
        m_token = canceled;
    }

    // Checked from within a task's functor
    bool is_canceled() const
    {
        return m_token.load();
    }
private:
    std::atomic<bool> m_token{false};
};

// A function similar to std::for_each but returning a task for deferred,
// possibly asynchronous execution. This function creates a graph
// with std::distance(first, last) + 1 tasks. UB if first >= last.
// Note that `unary_op` takes an iterator.
template<typename InputIt, typename UnaryOperation>
gcl::Task<void> for_each(InputIt first, InputIt last, UnaryOperation unary_op)
{
    const auto distance = std::distance(first, last);
    GCL_ASSERT(distance > 0);
    gcl::Vec<void> tasks;
    tasks.reserve(static_cast<std::size_t>(distance));
    for (; first != last; ++first)
    {
        tasks.push_back(gcl::task([unary_op, first]{ unary_op(first); }));
    }
    return gcl::when(tasks);
}

} // gcl
