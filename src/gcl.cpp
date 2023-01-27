// gcl is a tiny graph concurrent library for C++
// Repo: https://github.com/bloomen/gcl
// Author: Christian Blume
// License: MIT http://www.opensource.org/licenses/mit-license.php

#include <gcl.h>

#include <condition_variable>
#include <mutex>
#include <random>

namespace gcl
{

namespace
{

class SpinLock
{
public:
    explicit SpinLock(const bool yields)
        : m_yields{yields}
    {
    }

    void
    lock() noexcept
    {
        while (m_locked.test_and_set(std::memory_order_acquire))
        {
            if (m_yields)
            {
                std::this_thread::yield();
            }
        }
    }

    void
    unlock() noexcept
    {
        m_locked.clear(std::memory_order_release);
    }

private:
    const bool m_yields;
    std::atomic_flag m_locked = ATOMIC_FLAG_INIT;
};

class TaskQueue
{
public:
    TaskQueue() = default;

    TaskQueue(const TaskQueue&) = delete;
    TaskQueue&
    operator=(const TaskQueue&) = delete;

    virtual ~TaskQueue() = default;

    virtual void
    shutdown()
    {
    }

    virtual void
    yield() const
    {
    }

    virtual std::size_t
    size() const
    {
        return m_size;
    }

    virtual void
    push(ITask* const task)
    {
        if (m_head)
        {
            m_tail->next() = task;
            task->previous() = m_tail;
            m_tail = task;
        }
        else
        {
            m_head = task;
            m_tail = task;
        }
        ++m_size;
    }

    virtual ITask*
    pop()
    {
        ITask* task = nullptr;
        if (m_head)
        {
            task = m_head;
            if (task->next())
            {
                m_head = task->next();
                m_head->previous() = nullptr;
            }
            else
            {
                m_head = nullptr;
                m_tail = nullptr;
            }
            task->next() = nullptr;
            --m_size;
        }
        return task;
    }

private:
    std::size_t m_size = 0;
    ITask* m_head = nullptr;
    ITask* m_tail = nullptr;
};

// Mp-Sc queue
class TaskQueueMutex : public TaskQueue
{
public:
    explicit TaskQueueMutex(const std::atomic<bool>& done)
        : m_done{done}
    {
    }

    void
    shutdown() override
    {
        m_cv.notify_one();
    }

    std::size_t
    size() const override
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        return TaskQueue::size();
    }

    // multiple producer
    void
    push(ITask* const task) override
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        TaskQueue::push(task);
        m_cv.notify_one();
    }

    // single consumer
    ITask*
    pop() override
    {
        std::unique_lock<std::mutex> lock{m_mutex};
        m_cv.wait(lock, [this] { return TaskQueue::size() > 0 || m_done; });
        return TaskQueue::pop();
    }

private:
    const std::atomic<bool>& m_done;
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
};

// Mp-Sc queue
class TaskQueueSpin : public TaskQueue
{
public:
    explicit TaskQueueSpin(const bool yields,
                           const std::atomic<bool>& active,
                           const std::chrono::microseconds sleep_interval)
        : m_spin{yields}
        , m_active{active}
        , m_sleep_interval{sleep_interval}
    {
    }

    void
    yield() const override
    {
        if (m_sleep_interval <= std::chrono::microseconds{0})
        {
            return;
        }
        while (!m_active && size() == 0)
        {
            std::this_thread::sleep_for(m_sleep_interval);
        }
    }

    std::size_t
    size() const override
    {
        std::lock_guard<SpinLock> lock{m_spin};
        return TaskQueue::size();
    }

    // multiple producer
    void
    push(ITask* const task) override
    {
        std::lock_guard<SpinLock> lock{m_spin};
        TaskQueue::push(task);
    }

    // single consumer
    ITask*
    pop() override
    {
        std::lock_guard<SpinLock> lock{m_spin};
        return TaskQueue::pop();
    }

private:
    mutable SpinLock m_spin;
    const std::atomic<bool>& m_active;
    const std::chrono::microseconds m_sleep_interval;
};

std::unique_ptr<TaskQueue>
make_task_queue(const AsyncConfig::QueueType queue_type,
                std::atomic<bool>& done,
                const bool spin_lock_yields,
                const std::atomic<bool>& active,
                const std::chrono::microseconds sleep_interval)
{
    switch (queue_type)
    {
    case AsyncConfig::QueueType::Mutex:
        return std::make_unique<TaskQueueMutex>(done);
    case AsyncConfig::QueueType::Spin:
        return std::make_unique<TaskQueueSpin>(
            spin_lock_yields, active, sleep_interval);
    }
    GCL_ASSERT(false);
    return nullptr;
}

class Worker
{
public:
    virtual ~Worker() = default;
    virtual void
    run() = 0;
    virtual void
    shutdown() = 0;
};

class Thread
{
public:
    void
    start(Worker& worker)
    {
        GCL_ASSERT(!m_worker);
        m_thread = std::thread{[&worker] { worker.run(); }};
        m_worker = &worker;
    }

    ~Thread() noexcept
    {
        if (m_worker)
        {
            m_worker->shutdown();
            m_thread.join();
        }
    }

private:
    Worker* m_worker = nullptr;
    std::thread m_thread;
};

class Processor : public Worker
{
public:
    explicit Processor(const std::size_t index,
                       const AsyncConfig& config,
                       TaskQueue& completed,
                       const std::atomic<bool>& active)
        : m_config{config}
        , m_completed{completed}
        , m_active{active}
        , m_index{index}
        , m_scheduled{
              make_task_queue(m_config.queue_type,
                              m_done,
                              m_config.spin_config.spin_lock_yields,
                              m_active,
                              m_config.spin_config.processor_sleep_interval)}
    {
        m_thread.start(*this);
    }

    void
    push(ITask* const task)
    {
        m_scheduled->push(task);
    }

private:
    void
    run() override
    {
        if (m_config.on_processor_thread_started)
        {
            m_config.on_processor_thread_started(m_index);
        }
        while (!m_done)
        {
            while (const auto task = m_scheduled->pop())
            {
                task->call();
                m_completed.push(task);
            }
            m_scheduled->yield();
        }
    }

    void
    shutdown() override
    {
        m_done = true;
        m_scheduled->shutdown();
    }

    const AsyncConfig& m_config;
    TaskQueue& m_completed;
    const std::atomic<bool>& m_active;
    std::size_t m_index;
    std::atomic<bool> m_done{false};
    std::unique_ptr<TaskQueue> m_scheduled;
    Thread m_thread;
};

} // namespace

struct Async::Impl : public Worker
{
    explicit Impl(const std::size_t n_threads, AsyncConfig config)
        : m_config{std::move(config)}
        , m_active{m_config.spin_config.active}
        , m_completed{make_task_queue(
              m_config.queue_type,
              m_done,
              m_config.spin_config.spin_lock_yields,
              m_active,
              m_config.spin_config.scheduler_sleep_interval)}
        , m_randgen{m_config.scheduler_random_seed > 0
                        ? m_config.scheduler_random_seed
                        : std::random_device{}()}
    {
        if (n_threads == 0)
        {
            return;
        }
        m_thread.start(*this);
        m_processors.reserve(n_threads);
        for (std::size_t i = 0; i < n_threads; ++i)
        {
            m_processors.emplace_back(std::make_unique<Processor>(
                i, m_config, *m_completed, m_active));
        }
    }

    void
    set_active(const bool active)
    {
        m_active = active;
    }

    std::size_t
    n_threads() const
    {
        return m_processors.size();
    }

    void
    execute(ITask& root)
    {
        GCL_ASSERT(n_threads() > 0);
        std::size_t index;
        if (root.thread_affinity() >= 0 &&
            static_cast<std::size_t>(root.thread_affinity()) <
                m_processors.size())
        {
            index = static_cast<std::size_t>(root.thread_affinity());
        }
        else
        {
            std::uniform_int_distribution<std::size_t> dist{
                0u, m_processors.size() - 1u};
            index = dist(m_randgen);
        }
        m_processors[index]->push(&root);
    }

private:
    void
    run() override
    {
        if (m_config.on_scheduler_thread_started)
        {
            m_config.on_scheduler_thread_started();
        }
        while (!m_done)
        {
            while (const auto task = m_completed->pop())
            {
                if (task->set_finished())
                {
                    for (const auto child : task->children())
                    {
                        if (child->set_parent_finished())
                        {
                            execute(*child);
                        }
                    }
                }
            }
            m_completed->yield();
        }
    }

    void
    shutdown() override
    {
        m_active = true;
        m_done = true;
        m_completed->shutdown();
    }

    AsyncConfig m_config;
    std::atomic<bool> m_active;
    std::atomic<bool> m_done{false};
    std::unique_ptr<TaskQueue> m_completed;
    std::vector<std::unique_ptr<Processor>> m_processors;
    std::mt19937_64 m_randgen;
    Thread m_thread;
};

Async::Async(const std::size_t n_threads, AsyncConfig config)
    : m_impl{std::make_unique<Impl>(n_threads, std::move(config))}
{
}

Async::~Async() = default;

void
Async::set_active(const bool active)
{
    m_impl->set_active(active);
}

std::size_t
Async::n_threads() const
{
    return m_impl->n_threads();
}

void
Async::execute(ITask& root)
{
    m_impl->execute(root);
}

std::string
to_dot(const std::vector<Edge>& edges, const Meta::Map& meta_map)
{
    auto str = [&meta_map](const auto& id) {
        std::string task = "\"" + std::to_string(id);
        auto meta = meta_map.find(id);
        if (meta != meta_map.end())
        {
            task += "\n" + meta->second.name;
        }
        task += "\"";
        return task;
    };
    std::string dot = "digraph {\n";
    for (const auto& edge : edges)
    {
        dot += str(edge.parent) + " -> " + str(edge.child) + "\n";
    }
    dot += "}";
    return dot;
}

} // namespace gcl
