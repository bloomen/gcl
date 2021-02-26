#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "gcl.h"

#include <mutex>

namespace gcl
{

bool operator==(const Edge& lhs, const Edge& rhs)
{
    return lhs.parent == rhs.parent && lhs.child == rhs.child;
}

}

void test_schedule(const std::size_t n_threads)
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::tie(p1, p2).then([](auto p1, auto p2){
        REQUIRE(p1.has_result());
        REQUIRE(p2.has_result());
        return *p1.get() + *p2.get();
    });
    gcl::Async async{n_threads};
    REQUIRE(!t.wait());
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
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

void test_schedule_and_cancel(const std::size_t n_threads)
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    gcl::CancelToken ct;
    auto t = gcl::tie(p1, p2).then([&ct](auto p1, auto p2){
        if (ct.is_canceled())
        {
            return -1;
        }
        return *p1.get() + *p2.get();
    });
    ct.set_canceled();
    gcl::Async async{n_threads};
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
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

void test_schedule_and_auto_release(const std::size_t n_threads)
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::tie(p1, p2).then([](auto p1, auto p2){
        return *p1.get() + *p2.get();
    });
    t.set_auto_release(true);
    gcl::Async async{n_threads};
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
    REQUIRE(!p1.has_result());
    REQUIRE(!p2.has_result());
    REQUIRE(t.has_result());
    REQUIRE(55 == *t.get());
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
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::tie(gcl::vec(p1, p2)).then([](gcl::Vec<int> p){ return *p[0].get() + *p[1].get(); });
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(55 == *t.get());
}

TEST_CASE("schedule_using_reference_type")
{
    int x = 42;
    auto p = gcl::task([&x]() -> int& { return x; });
    auto t = p.then([](auto p) -> int& { return *p.get(); });
    gcl::Async async{4};
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
    REQUIRE(42 == *p.get());
    REQUIRE(&x == p.get());
}

TEST_CASE("schedule_and_release")
{
    auto t = gcl::task([]{ return 42; });
    REQUIRE(!t.has_result());
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(t.has_result());
    REQUIRE(42 == *t.get());
    REQUIRE(t.release());
    REQUIRE(!t.has_result());
}

void test_schedule_a_wide_graph(const std::size_t n_threads,
                                const bool use_conditon_variable,
                                const bool use_schedule_overload)
{
    std::atomic<int> x{0};
    std::mutex m;
    auto top = gcl::task([&x]{ return x++; });
    gcl::Vec<int> tasks;
    for (int i = 0; i < 10; ++i)
    {
        auto functor = [&x, &m](auto t)
        {
            const bool has_result = t.has_result();
            {
                std::lock_guard<std::mutex> lock{m};
                REQUIRE(has_result); // catch's assertion counter is not thread-safe
            }
            x++;
            return *t.get();
        };
        auto t1 = top.then(functor);
        auto t2 = t1.then(functor);
        tasks.push_back(t2);
    }
    auto bottom = gcl::tie(tasks).then([&x](gcl::Vec<int>) { x++; });
    if (use_schedule_overload)
    {
        bottom.schedule();
    }
    else
    {
        gcl::AsyncConfig config;
        config.use_condition_variable = use_conditon_variable;
        gcl::Async async{n_threads, config};
        REQUIRE(bottom.schedule(async));
        REQUIRE(bottom.wait());
    }
    REQUIRE(22 == x);
}

TEST_CASE("schedule_a_wide_graph")
{
    test_schedule_a_wide_graph(0, false, false);
    test_schedule_a_wide_graph(0, true, true);
    test_schedule_a_wide_graph(0, true, false);
    test_schedule_a_wide_graph(0, false, true);
}

TEST_CASE("schedule_a_wide_graph_with_1_thread")
{
    test_schedule_a_wide_graph(1, false, false);
    test_schedule_a_wide_graph(1, true, true);
    test_schedule_a_wide_graph(1, true, false);
    test_schedule_a_wide_graph(1, false, true);
}

TEST_CASE("schedule_a_wide_graph_with_4_threads")
{
    test_schedule_a_wide_graph(4, false, false);
    test_schedule_a_wide_graph(4, true, true);
    test_schedule_a_wide_graph(4, true, false);
    test_schedule_a_wide_graph(4, false, true);
}

TEST_CASE("schedule_with_mixed_parents")
{
    int x = 0;
    auto p1 = gcl::task([&x]{ x++; return 42.0; });
    auto p2 = gcl::task([&x]{ x++; return 13; });
    auto p3 = gcl::task([&x]{ x++; return 20; });
    auto p4 = gcl::task([&x]{ x++; return 21; });
    auto p6 = gcl::task([&x]{ x++; return std::string{"guan"}; });
    auto t = gcl::when(p1, p2, gcl::vec(p3, p4), p6);
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(5 == x);
}

TEST_CASE("schedule_with_bind_and_when")
{
    int x = 0;
    auto p1 = gcl::task([&x]{ x++; });
    auto p2 = gcl::task([&x]{ x++; });
    auto t = gcl::when(gcl::tie(p1, p2));
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(2 == x);
}

TEST_CASE("edges")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::when(p1, p2);
    gcl::Async async;
    REQUIRE(t.schedule(async));
    const std::vector<gcl::Edge> exp_edges = {
        {p1.id(), t.id()},
        {p2.id(), t.id()}
    };
    REQUIRE(exp_edges == t.edges());
}

void test_schedule_twice(const std::size_t n_threads)
{
    std::atomic<int> x{0};
    auto p1 = gcl::task([&x]{ x++; });
    auto p2 = gcl::task([&x]{ x++; });
    auto t = gcl::when(p1, p2);
    gcl::Async async{n_threads};
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
    REQUIRE(2 == x);
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
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
    CopyOnly& operator=(const CopyOnly&) = default;
    int operator()() const
    {
        return 42;
    }
};

}

TEST_CASE("functor_only_copyable_as_rvalue")
{
    auto t = gcl::task(CopyOnly{});
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(42 == *t.get());
}

TEST_CASE("functor_only_copyable_as_lvalue")
{
    CopyOnly functor;
    auto t = gcl::task(functor);
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(42 == *t.get());
}

TEST_CASE("functor_only_copyable_as_const_lvalue")
{
    const CopyOnly functor;
    auto t = gcl::task(functor);
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(42 == *t.get());
}

namespace
{

struct MoveOnly
{
    MoveOnly() = default;
    MoveOnly(MoveOnly&&) = default;
    MoveOnly& operator=(MoveOnly&&) = default;
    int operator()() const
    {
        return 42;
    }
};

}

TEST_CASE("functor_only_movable")
{
    auto t = gcl::task(MoveOnly{});
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(42 == *t.get());
}

TEST_CASE("task_chaining_with_int")
{
    int x = 0;
    auto f = [&x](auto){ x++; return 0; };
    auto t = gcl::task([&x]{ x++; return 0; }).then(f).then(f).then(f).then(f);
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(5 == x);
}

TEST_CASE("task_chaining_with_void")
{
    int x = 0;
    auto f = [&x](gcl::Task<void> t){ REQUIRE(t.has_result()); x++; };
    auto t = gcl::task([&x]{ x++; }).then(f).then(f).then(f).then(f);
    gcl::Async async;
    REQUIRE(t.schedule(async));
    REQUIRE(5 == x);
}

void test_for_each(const std::size_t n_threads)
{
    std::vector<double> data{1, 2, 3, 4, 5};
    auto t = gcl::for_each(data.begin(), data.end(), [](auto it){ *it *= 2; });
    gcl::Async async{n_threads};
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
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
    auto t = gcl::for_each(size_t{0}, data.size(), [&data](auto i){ data[i] *= 2; });
    gcl::Async async;
    REQUIRE(t.schedule(async));
    const std::vector<double> data_exp{2, 4, 6, 8, 10};
    REQUIRE(data_exp == data);
}

TEST_CASE("schedule_with_thread_affinity")
{
    auto p1 = gcl::task([]{ return 42; });
    p1.set_thread_affinity(0);
    auto p2 = gcl::task([]{ return 13; });
    p1.set_thread_affinity(1);
    auto t = gcl::tie(p1, p2).then([](auto p1, auto p2){
        REQUIRE(p1.has_result());
        REQUIRE(p2.has_result());
        return *p1.get() + *p2.get();
    });
    t.set_thread_affinity(2);
    gcl::Async async{2};
    REQUIRE(t.schedule(async));
    REQUIRE(t.wait());
    REQUIRE(55 == *t.get());
}
