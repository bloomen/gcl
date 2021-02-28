// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <thread>
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
    virtual int thread_affinity() const = 0;
    virtual const std::vector<ITask*>& children() const = 0;
    virtual bool set_parent_finished() = 0;
    virtual void set_finished() = 0;
    virtual ITask*& next() = 0;
    virtual ITask*& previous() = 0;
};

// Config struct for the Async class
struct AsyncConfig
{
    std::uint_fast64_t scheduler_random_seed = 0; // to select a random processor for a new task to run
    std::function<void(std::size_t thread_index)> on_processor_thread_started;
    std::function<void()> on_scheduler_thread_started;

    enum class QueueType
    {
        Mutex, // Use std::mutex for work queue synchronization + std::condition_variable for wait/notify
        Spin   // Use a spin lock for work queue synchronization + busy wait with interval sleep
    };

    QueueType queue_type = QueueType::Mutex;

    // Below are only used for QueueType::Spin
    bool active = false; // Whether we're in active mode which skips interval sleeping, i.e. full busy waits
    std::chrono::microseconds processor_sleep_interval = std::chrono::microseconds{100};
    std::chrono::microseconds scheduler_sleep_interval = std::chrono::microseconds{100};
};

// Async executor for asynchronous execution.
// It works such that it runs a task as soon as the task's parents finish.
// Async consists of a scheduler thread which manages when a task should run
// and `n_threads` processor threads which actually run the tasks
class Async
{
public:
    // Spawns a scheduler thread and `n_threads` processor threads.
    // Will run tasks on the current thread if `n_threads` == 0
    explicit
    Async(std::size_t n_threads = 0, gcl::AsyncConfig config = {});
    ~Async();

    void set_active(bool active); // Only relevant for QueueType::Spin
    std::size_t n_threads() const;
    void execute(ITask& task);
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

    // Specifies a particular thread the task should run on.
    // If not specified then it'll run on a randomly selected thread
    void set_thread_affinity(std::size_t thread_index);

    // Schedules this task and its parents for execution. Returns true if successfully scheduled
    // and false if already scheduled and not finished
    bool schedule_all(gcl::Async& async);

    // Runs this task and its parents synchronously on the current thread. Returns true if successfully scheduled
    // and false if already scheduled and not finished
    bool schedule_all();

    // Returns true if this task is currently being scheduled
    bool is_scheduled() const;

    // Returns true if this task has a result
    bool has_result() const;

    // Waits for this task to finish
    void wait() const;

    // Sets whether this task's parents' results should be automatically released
    void set_auto_release_parents(bool auto_release);

    // Sets whether this task's result should be automatically released
    void set_auto_release(bool auto_release);

    // Releases this task's parents' results. Returns true if successfully released
    // and false if currently scheduled and not finished
    bool release_parents();

    // Releases this task's result. Returns true if successfully released
    // and false if currently scheduled and not finished
    bool release();

    // Returns the unique id of the task
    gcl::TaskId id() const;

    // Returns the edges between this task and all its parent tasks
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

    // Returns the task's result if the task finished (has_result() == true), null otherwise.
    // Re-throws any exception thrown from running this task or any of its parents.
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

    // Returns the task's result if the task finished (has_result() == true), null otherwise.
    // Re-throws any exception thrown from running this task or any of its parents.
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

    // Returns true if the task finished (has_result() == true), false otherwise.
    // Re-throws any exception thrown from running this task or any of its parents.
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

// Unless otherwise mentioned a given method is called from the control thread
class BaseImpl : public gcl::ITask
{
public:
    virtual ~BaseImpl() = default;

    // Called from scheduler thread
    int thread_affinity() const override
    {
        return m_thread_affinity;
    }

    // Called from scheduler thread
    const std::vector<gcl::ITask*>& children() const override
    {
        return m_children;
    }

    // Called from scheduler thread
    bool set_parent_finished() override
    {
        return ++m_parents_ready == m_parents.size();
    }

    // Called from scheduler thread
    void set_finished() override
    {
        for (const auto parent : parents())
        {
            if (parent->set_child_finished())
            {
                parent->auto_release_if();
            }
        }
        if (m_children.empty())
        {
            auto_release_if();
        }
        m_promise.set_value();
        m_scheduled = false;
    }

    // Called from scheduler and processor threads but never simultaneously
    gcl::ITask*& next() override
    {
        return m_next;
    }

    // Called from scheduler and processor threads but never simultaneously
    gcl::ITask*& previous() override
    {
        return m_previous;
    }

    virtual void prepare() = 0;
    virtual void release() = 0;

    void set_thread_affinity(const int affinity)
    {
        m_thread_affinity = affinity;
    }

    void set_auto_release(const bool auto_release)
    {
        m_auto_release = auto_release;
    }

    template<typename Visitor>
    void visit(const Visitor& visitor)
    {
        if (!m_task_cache)
        {
            m_task_cache = std::make_unique<std::vector<BaseImpl*>>();
            m_task_cache->emplace_back(this);
            std::queue<BaseImpl*> q;
            q.emplace(this);
            while (!q.empty())
            {
                const BaseImpl* const v = q.front();
                q.pop();
                for (gcl::ITask* const p : v->m_parents)
                {
                    const auto w = static_cast<BaseImpl*>(p);
                    if (!w->m_visited)
                    {
                        q.emplace(w);
                        w->m_visited = true;
                        m_task_cache->emplace_back(w);
                    }
                }
            }
            for (const auto task : *m_task_cache)
            {
                task->m_visited = false;
            }
        }
        for (auto task = m_task_cache->rbegin(); task != m_task_cache->rend(); ++task)
        {
            visitor(**task);
        }
    }

    void wait() const
    {
        m_future.wait();
        while (is_scheduled());
    }

    bool is_scheduled() const
    {
        return m_scheduled;
    }

    void add_child(BaseImpl& child)
    {
        m_children.emplace_back(&child);
        m_task_cache.reset();
    }

    void add_parent(BaseImpl& parent)
    {
        m_parents.emplace_back(&parent);
        parent.add_child(*this);
    }

    // Called from scheduler thread
    const std::vector<BaseImpl*>& parents() const
    {
        return m_parents;
    }

    // Called from scheduler thread
    bool set_child_finished()
    {
        return ++m_children_ready == m_children.size();
    }

    // Called from scheduler thread
    void auto_release_if()
    {
        if (m_auto_release)
        {
            release();
        }
    }

    gcl::TaskId id() const
    {
        return std::hash<const BaseImpl*>{}(this);
    }

    std::vector<gcl::Edge> edges()
    {
        std::vector<Edge> es;
        visit([&es](BaseImpl& i)
        {
            for (const auto p : i.m_parents)
            {
                es.push_back({p->id(), i.id()});
            }
        });
        return es;
    }

protected:
    BaseImpl() = default;

    std::promise<void> m_promise;
    std::future<void> m_future;
    int m_thread_affinity = -1;
    std::atomic<bool> m_auto_release{false};
    std::atomic<bool> m_scheduled{false};
    bool m_visited = false;
    std::unique_ptr<std::vector<BaseImpl*>> m_task_cache;
    std::vector<BaseImpl*> m_parents;
    std::vector<gcl::ITask*> m_children;
    std::uint32_t m_parents_ready = 0;
    std::uint32_t m_children_ready = 0;
    gcl::ITask* m_next = nullptr;
    gcl::ITask* m_previous = nullptr;
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

    Result* value() const
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

// Unless otherwise mentioned a given method is called from the control thread
template<typename Result>
class Channel
{
public:
    Channel() = default;

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    ~Channel()
    {
        reset();
    }

    void set_future(const std::future<void>& future)
    {
        GCL_ASSERT(future.valid());
        m_future = &future;
    }

    // Called from a processor thread
    void set(gcl::detail::ChannelElement<Result>&& element)
    {
        new (m_storage) gcl::detail::ChannelElement<Result>{std::move(element)};
    }

    const gcl::detail::ChannelElement<Result>* get() const
    {
        if (!m_future || m_future.load()->wait_for(std::chrono::seconds{0}) != std::future_status::ready)
        {
            return nullptr;
        }
        return reinterpret_cast<const gcl::detail::ChannelElement<Result>*>(m_storage);
    }

    // Called from either control or scheduler thread
    void reset()
    {
        if (const auto element = get())
        {
            element->~ChannelElement();
        }
        m_future = nullptr;
    }

private:
    std::atomic<const std::future<void>*> m_future{nullptr};
    char m_storage[sizeof(gcl::detail::ChannelElement<Result>)];
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
#ifndef NDEBUG
                                     gcl::detail::for_each([](const auto& p){ GCL_ASSERT(p.has_result()); }, p...);
#endif
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

// Unless otherwise mentioned a given method is called from the control thread
template<typename Result>
struct BaseTask<Result>::Impl : BaseImpl
{
    template<typename Functor, typename... Parents>
    explicit
    Impl(Functor&& functor, Parents&&... parents)
    {
        gcl::detail::for_each(gcl::detail::CollectParents{this}, parents...);
        m_binding = std::make_unique<gcl::detail::BindingImpl<Result, Functor, Parents...>>(std::forward<Functor>(functor), std::forward<Parents>(parents)...);
        m_future = m_promise.get_future();
    }

    void prepare() override
    {
        m_parents_ready = 0;
        m_children_ready = 0;
        m_promise = {};
        m_future = m_promise.get_future();
        m_scheduled = true;
        m_channel.reset();
        m_channel.set_future(m_future);
    }

    // Called from a processor thread
    void call() override
    {
        gcl::detail::Evaluate<Result>{}(m_channel, *m_binding);
    }

    // Called from either control or scheduler thread
    void release() override
    {
        m_channel.reset();
    }

    const ChannelElement<Result>* channel_element() const
    {
        if (m_scheduled)
        {
            return nullptr;
        }
        return m_channel.get();
    }

private:
    std::unique_ptr<gcl::detail::Binding<Result>> m_binding;
    Channel<Result> m_channel;
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
void BaseTask<Result>::set_thread_affinity(const std::size_t thread_index)
{
    m_impl->set_thread_affinity(static_cast<int>(thread_index));
}

template<typename Result>
bool BaseTask<Result>::schedule_all(gcl::Async& async)
{
    if (is_scheduled())
    {
        return false;
    }
    if (async.n_threads() == 0)
    {
        return schedule_all();
    }
    std::vector<gcl::ITask*> roots;
    m_impl->visit([&roots](BaseImpl& i)
    {
        i.prepare();
        if (i.parents().empty())
        {
            roots.emplace_back(&i);
        }
    });
    for (const auto root : roots)
    {
        async.execute(*root);
    }
    return true;
}

template<typename Result> 
bool BaseTask<Result>::schedule_all()
{
    if (is_scheduled())
    {
        return false;
    }
    m_impl->visit([](BaseImpl& i)
    {
        i.prepare();
        i.call();
        i.set_finished();
    });
    return true;
}

template<typename Result> 
bool BaseTask<Result>::is_scheduled() const
{
    return m_impl->is_scheduled();
}

template<typename Result>
bool BaseTask<Result>::has_result() const
{
    return m_impl->channel_element();
}

template<typename Result>
void BaseTask<Result>::wait() const
{
    m_impl->wait();
}

template<typename Result>
void BaseTask<Result>::set_auto_release_parents(const bool auto_release)
{
    const auto final = m_impl.get();
    m_impl->visit([auto_release, final](BaseImpl& i){ if (&i != final) { i.set_auto_release(auto_release); } });
}

template<typename Result>
void BaseTask<Result>::set_auto_release(const bool auto_release)
{
    m_impl->set_auto_release(auto_release);
}

template<typename Result>
bool BaseTask<Result>::release_parents()
{
    if (is_scheduled())
    {
        return false;
    }
    const auto final = m_impl.get();
    m_impl->visit([final](BaseImpl& i){ if (&i != final) { i.release(); } });
    return true;
}

template<typename Result>
bool BaseTask<Result>::release()
{
    if (is_scheduled())
    {
        return false;
    }
    m_impl->release();
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
    if (const auto element = this->m_impl->channel_element())
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
    return nullptr;
}

template<typename Result>
Result* Task<Result&>::get() const
{
    if (const auto element = this->m_impl->channel_element())
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
    return nullptr;
}

inline
bool Task<void>::get() const
{
    if (const auto element = this->m_impl->channel_element())
    {
        if (!element->has_value())
        {
            std::rethrow_exception(element->exception());
        }
        return true;
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

namespace detail
{

template<bool is_arithmetic>
struct Distance;

template<>
struct Distance<true>
{
    template<typename Number>
    auto operator()(const Number first, const Number last) const
    {
        GCL_ASSERT(last > first);
        return last - first;
    }
};

template<>
struct Distance<false>
{
    template<typename Iterator>
    auto operator()(const Iterator first, const Iterator last) const
    {
        return std::distance(first, last);
    }
};

}

// A function similar to std::for_each but returning a task for deferred,
// possibly asynchronous execution. This function creates a graph
// with distance(first, last) + 1 tasks. UB if first >= last.
// Note that `unary_op` takes an object of type T.
template<typename T, typename UnaryOperation>
gcl::Task<void> for_each(T first, const T last, UnaryOperation unary_op)
{
    const auto distance = gcl::detail::Distance<std::is_arithmetic<T>::value>{}(first, last);
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
