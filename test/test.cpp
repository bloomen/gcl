#define CATCH_CONFIG_MAIN
#if __clang__ || __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif
#include "catch_amalgamated.hpp"
#if __clang__ || __GNUC__
#pragma GCC diagnostic pop
#endif
#include "gcl.h"

#include <mutex>

namespace gcl
{

bool
operator==(const Edge& lhs, const Edge& rhs)
{
    return lhs.parent == rhs.parent && lhs.child == rhs.child;
}

} // namespace gcl

namespace
{

bool
contains(const std::string& str, const char* token)
{
    return str.find(token) != std::string::npos;
}

template <typename T>
bool
contains(const std::string& str, const T& token)
{
    return str.find(std::to_string(token)) != std::string::npos;
}

} // namespace

void
test_schedule(const std::size_t n_threads)
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    gcl::Async async{n_threads};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(55 == *t.get());
}

TEST_CASE("schedule")
{
    test_schedule(0);
}

TEST_CASE("schedule_with_1_thread")
{
    test_schedule(1);
}

TEST_CASE("schedule_with_4_threads")
{
    test_schedule(4);
}

void
test_schedule_and_cancel(const std::size_t n_threads)
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    gcl::CancelToken ct;
    auto t = gcl::tie(p1, p2).then([&ct](auto p1, auto p2) {
        if (ct.is_canceled())
        {
            return -1;
        }
        return *p1.get() + *p2.get();
    });
    ct.set_canceled();
    gcl::Async async{n_threads};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(-1 == *t.get());
}

TEST_CASE("schedule_and_cancel")
{
    test_schedule_and_cancel(0);
}

TEST_CASE("schedule_and_cancel_with_1_thread")
{
    test_schedule_and_cancel(1);
}

TEST_CASE("schedule_and_cancel_with_4_threads")
{
    test_schedule_and_cancel(4);
}

void
test_schedule_and_release(const std::size_t n_threads)
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    gcl::Async async{n_threads};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(p1.has_result());
    REQUIRE(p2.has_result());
    REQUIRE(t.has_result());
    t.release_parents();
    REQUIRE(!p1.has_result());
    REQUIRE(!p2.has_result());
    REQUIRE(t.has_result());
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(p1.has_result());
    REQUIRE(p2.has_result());
    REQUIRE(t.has_result());
    p2.release();
    REQUIRE(p1.has_result());
    REQUIRE(!p2.has_result());
    REQUIRE(t.has_result());
}

TEST_CASE("schedule_and_release")
{
    test_schedule_and_release(0);
}

TEST_CASE("schedule_and_release_with_1_thread")
{
    test_schedule_and_release(1);
}

TEST_CASE("schedule_and_release_with_4_threads")
{
    test_schedule_and_release(4);
}

void
test_schedule_and_auto_release(const std::size_t n_threads)
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    t.set_auto_release_parents(true);
    gcl::Async async{n_threads};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(!p1.has_result());
    REQUIRE(!p2.has_result());
    REQUIRE(t.has_result());
    REQUIRE(55 == *t.get());
    p1.set_auto_release(false);
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(p1.has_result());
    REQUIRE(!p2.has_result());
    REQUIRE(t.has_result());
    REQUIRE(55 == *t.get());
    t.set_auto_release_parents(true);
    t.set_auto_release(true);
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(!p1.has_result());
    REQUIRE(!p2.has_result());
    REQUIRE(!t.has_result());
}

TEST_CASE("schedule_and_auto_release")
{
    test_schedule_and_auto_release(0);
}

TEST_CASE("schedule_and_auto_release_with_1_thread")
{
    test_schedule_and_auto_release(1);
}

TEST_CASE("schedule_and_auto_release_with_4_threads")
{
    test_schedule_and_auto_release(4);
}

TEST_CASE("schedule_with_vec_parents")
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(gcl::vec(p1, p2)).then([](gcl::Vec<int> p) {
        return *p[0].get() + *p[1].get();
    });
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(55 == *t.get());
}

TEST_CASE("schedule_using_reference_type")
{
    int x = 42;
    auto p = gcl::task([&x]() -> int& { return x; });
    auto t = p.then([](auto p) -> int& { return *p.get(); });
    gcl::Async async{4};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(42 == *p.get());
    REQUIRE(&x == p.get());
}

void
test_schedule_a_wide_graph(const std::size_t n_threads,
                           const gcl::AsyncConfig::QueueType queue_type,
                           const bool use_schedule_overload)
{
    std::atomic<int> x{0};
    auto top = gcl::task([&x] { return x++; });
    gcl::Vec<int> tasks;
    for (int i = 0; i < 10; ++i)
    {
        auto functor = [&x](auto t) {
            x++;
            return *t.get();
        };
        auto t1 = top.then(functor);
        auto t2 = t1.then(functor);
        tasks.push_back(t2);
    }
    auto t = gcl::tie(tasks).then([&x](gcl::Vec<int>) { x++; });
    if (use_schedule_overload)
    {
        t.schedule_all();
    }
    else
    {
        gcl::AsyncConfig config;
        config.queue_type = queue_type;
        gcl::Async async{n_threads, config};
        REQUIRE(t.schedule_all(async));
        t.wait();
    }
    REQUIRE(22 == x);
}

TEST_CASE("schedule_a_wide_graph")
{
    test_schedule_a_wide_graph(0, gcl::AsyncConfig::QueueType::Spin, false);
    test_schedule_a_wide_graph(0, gcl::AsyncConfig::QueueType::Mutex, true);
    test_schedule_a_wide_graph(0, gcl::AsyncConfig::QueueType::Mutex, false);
    test_schedule_a_wide_graph(0, gcl::AsyncConfig::QueueType::Spin, true);
}

TEST_CASE("schedule_a_wide_graph_with_1_thread")
{
    test_schedule_a_wide_graph(1, gcl::AsyncConfig::QueueType::Spin, false);
    test_schedule_a_wide_graph(1, gcl::AsyncConfig::QueueType::Mutex, true);
    test_schedule_a_wide_graph(1, gcl::AsyncConfig::QueueType::Mutex, false);
    test_schedule_a_wide_graph(1, gcl::AsyncConfig::QueueType::Spin, true);
}

TEST_CASE("schedule_a_wide_graph_with_4_threads")
{
    test_schedule_a_wide_graph(4, gcl::AsyncConfig::QueueType::Spin, false);
    test_schedule_a_wide_graph(4, gcl::AsyncConfig::QueueType::Mutex, true);
    test_schedule_a_wide_graph(4, gcl::AsyncConfig::QueueType::Mutex, false);
    test_schedule_a_wide_graph(4, gcl::AsyncConfig::QueueType::Spin, true);
}

TEST_CASE("schedule_with_mixed_parents")
{
    int x = 0;
    auto p1 = gcl::task([&x] {
        x++;
        return 42.0;
    });
    auto p2 = gcl::task([&x] {
        x++;
        return 13;
    });
    auto p3 = gcl::task([&x] {
        x++;
        return 20;
    });
    auto p4 = gcl::task([&x] {
        x++;
        return 21;
    });
    auto p6 = gcl::task([&x] {
        x++;
        return std::string{"guan"};
    });
    auto t = gcl::when(p1, p2, gcl::vec(p3, p4), p6);
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(5 == x);
}

TEST_CASE("schedule_with_bind_and_when")
{
    int x = 0;
    auto p1 = gcl::task([&x] { x++; });
    auto p2 = gcl::task([&x] { x++; });
    auto t = gcl::when(gcl::tie(p1, p2));
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(2 == x);
}

TEST_CASE("edges")
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::when(p1, p2);
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    const std::vector<gcl::Edge> exp_edges = {{p1.id(), t.id()},
                                              {p2.id(), t.id()}};
    REQUIRE(exp_edges == t.edges());
}

void
test_schedule_twice(const std::size_t n_threads)
{
    std::atomic<int> x{0};
    auto p1 = gcl::task([&x] { x++; });
    auto p2 = gcl::task([&x] { x++; });
    auto t = gcl::when(p1, p2);
    gcl::Async async{n_threads};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(2 == x);
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(4 == x);
}

TEST_CASE("schedule_twice")
{
    test_schedule_twice(0);
}

TEST_CASE("schedule_twice_with_1_thread")
{
    test_schedule_twice(1);
}

TEST_CASE("schedule_twice_with_4_thread")
{
    test_schedule_twice(4);
}

namespace
{

struct CopyOnly
{
    CopyOnly() = default;
    CopyOnly(const CopyOnly&) = default;
    CopyOnly&
    operator=(const CopyOnly&) = default;
    int
    operator()() const
    {
        return 42;
    }
};

} // namespace

TEST_CASE("functor_only_copyable_as_rvalue")
{
    auto t = gcl::task(CopyOnly{});
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(42 == *t.get());
}

TEST_CASE("functor_only_copyable_as_lvalue")
{
    CopyOnly functor;
    auto t = gcl::task(functor);
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(42 == *t.get());
}

TEST_CASE("functor_only_copyable_as_const_lvalue")
{
    const CopyOnly functor;
    auto t = gcl::task(functor);
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(42 == *t.get());
}

namespace
{

struct MoveOnly
{
    MoveOnly() = default;
    MoveOnly(MoveOnly&&) = default;
    MoveOnly&
    operator=(MoveOnly&&) = default;
    int
    operator()() const
    {
        return 42;
    }
};

} // namespace

TEST_CASE("functor_only_movable")
{
    auto t = gcl::task(MoveOnly{});
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(42 == *t.get());
}

TEST_CASE("task_chaining_with_int")
{
    int x = 0;
    auto f = [&x](auto) {
        x++;
        return 0;
    };
    auto t = gcl::task([&x] {
                 x++;
                 return 0;
             })
                 .then(f)
                 .then(f)
                 .then(f)
                 .then(f);
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(5 == x);
}

TEST_CASE("task_chaining_with_void")
{
    int x = 0;
    auto f = [&x](gcl::Task<void>) { x++; };
    auto t = gcl::task([&x] { x++; }).then(f).then(f).then(f).then(f);
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    REQUIRE(5 == x);
}

void
test_for_each(const std::size_t n_threads)
{
    std::vector<double> data{1, 2, 3, 4, 5};
    auto t = gcl::for_each(data.begin(), data.end(), [](auto it) { *it *= 2; });
    gcl::Async async{n_threads};
    REQUIRE(t.schedule_all(async));
    t.wait();
    const std::vector<double> data_exp{2, 4, 6, 8, 10};
    REQUIRE(data_exp == data);
}

TEST_CASE("for_each")
{
    test_for_each(0);
}

TEST_CASE("for_each_with_1_thread")
{
    test_for_each(1);
}

TEST_CASE("for_each_with_4_threads")
{
    test_for_each(4);
}

TEST_CASE("for_each_with_counters")
{
    std::vector<double> data{1, 2, 3, 4, 5};
    auto t = gcl::for_each(0u, data.size(), [&data](auto i) { data[i] *= 2; });
    gcl::Async async;
    REQUIRE(t.schedule_all(async));
    const std::vector<double> data_exp{2, 4, 6, 8, 10};
    REQUIRE(data_exp == data);
}

TEST_CASE("for_each_with_empty_range")
{
    std::vector<double> data;
    auto t = gcl::for_each(data.begin(), data.end(), [](auto it) { *it *= 2; });
    gcl::Async async{2};
    REQUIRE(t.schedule_all(async));
    t.wait();
    const std::vector<double> data_exp;
    REQUIRE(data_exp == data);
}

TEST_CASE("for_each_with_empty_counter_range")
{
    std::vector<double> data;
    auto t = gcl::for_each(0u, data.size(), [](auto) {});
    gcl::Async async{2};
    REQUIRE(t.schedule_all(async));
    t.wait();
    const std::vector<double> data_exp;
    REQUIRE(data_exp == data);
}

TEST_CASE("schedule_with_thread_affinity")
{
    auto p1 = gcl::task([] { return 42; });
    p1.set_thread_affinity(0);
    auto p2 = gcl::task([] { return 13; });
    p1.set_thread_affinity(1);
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    t.set_thread_affinity(2);
    gcl::Async async{2};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(55 == *t.get());
}

TEST_CASE("to_dot_without_meta")
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    const auto dot = gcl::to_dot(t.edges());
    REQUIRE(contains(dot, p1.id()));
    REQUIRE(contains(dot, p2.id()));
    REQUIRE(contains(dot, t.id()));
}

TEST_CASE("to_dot_with_meta")
{
    gcl::MetaData meta;
    auto p1 = gcl::task([] { return 42; }).md(meta, "parent1");
    auto p2 = gcl::task([] { return 13; }).md(meta);
    auto t = gcl::tie(p1, p2)
                 .then([](auto p1, auto p2) { return *p1.get() + *p2.get(); })
                 .md(meta, "child");
    const auto dot = gcl::to_dot(t.edges(), meta);
    REQUIRE(contains(dot, p1.id()));
    REQUIRE(contains(dot, "parent1"));
    REQUIRE(contains(dot, p2.id()));
    REQUIRE(contains(dot, t.id()));
    REQUIRE(contains(dot, "child"));
}

TEST_CASE("async_with_spin_queue_and_no_interval_sleep")
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    gcl::AsyncConfig config;
    config.queue_type = gcl::AsyncConfig::QueueType::Spin;
    config.spin_config.processor_sleep_interval = std::chrono::microseconds{0};
    gcl::Async async{4, config};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(55 == *t.get());
}

TEST_CASE("async_with_spin_queue_and_active")
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    gcl::AsyncConfig config;
    config.queue_type = gcl::AsyncConfig::QueueType::Spin;
    gcl::Async async{4, config};
    async.set_active(true);
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(55 == *t.get());
}

TEST_CASE("async_with_spin_queue_and_yields")
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    gcl::AsyncConfig config;
    config.queue_type = gcl::AsyncConfig::QueueType::Spin;
    config.spin_config.spin_lock_yields = true;
    gcl::Async async{4, config};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(55 == *t.get());
}

TEST_CASE("schedule_with_thread_callbacks")
{
    auto p1 = gcl::task([] { return 42; });
    auto p2 = gcl::task([] { return 13; });
    auto t = gcl::tie(p1, p2).then(
        [](auto p1, auto p2) { return *p1.get() + *p2.get(); });
    gcl::AsyncConfig config;
    std::atomic<int> scheduler{0};
    std::atomic<int> processor{0};
    config.on_scheduler_thread_started = [&scheduler] { ++scheduler; };
    config.on_processor_thread_started = [&processor](const std::size_t index) {
        ++processor;
        if (index >= 4)
        {
            throw std::runtime_error{std::to_string(index)};
        }
    };
    gcl::Async async{4, config};
    REQUIRE(t.schedule_all(async));
    t.wait();
    REQUIRE(1 == scheduler.load());
    while (processor.load() < 4)
    {
        std::this_thread::yield();
    }
    REQUIRE(55 == *t.get());
    REQUIRE(4 == processor.load());
}

TEST_CASE("schedule_with_exception_with_int_return")
{
    auto t = gcl::task([]() -> int { throw std::runtime_error{"booh"}; });
    REQUIRE(!t.get());
    REQUIRE(t.schedule_all());
    REQUIRE_THROWS_AS(*t.get(), std::runtime_error);
}

TEST_CASE("schedule_with_exception_with_int&_return")
{
    auto t = gcl::task([]() -> int& { throw std::runtime_error{"booh"}; });
    REQUIRE(!t.get());
    REQUIRE(t.schedule_all());
    REQUIRE_THROWS_AS(*t.get(), std::runtime_error);
}

TEST_CASE("schedule_with_exception_with_void_return")
{
    auto t = gcl::task([]() { throw std::runtime_error{"booh"}; });
    REQUIRE(!t.get());
    REQUIRE(t.schedule_all());
    REQUIRE_THROWS_AS(t.get(), std::runtime_error);
}
