#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "gcl.h"

TEST_CASE("schedule")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    t.schedule();
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Vec<int> p){ return p[0].get() + p[1].get(); }, gcl::vec(p1, p2));
    t.schedule();
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_Async")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    gcl::Async async{4};
    t.schedule(async);
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_Async_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    gcl::Async async{4};
    t.schedule(async);
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_join")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::join(p1, p2);
    t.schedule();
    t.wait();
    REQUIRE(42 == p1.get());
    REQUIRE(13 == p2.get());
}

TEST_CASE("schedule_using_join_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::join(gcl::vec(p1, p2));
    t.schedule();
    t.wait();
    REQUIRE(42 == p1.get());
    REQUIRE(13 == p2.get());
}

TEST_CASE("schedule_using_reference_type")
{
    int x = 42;
    auto p = gcl::task([&x]() -> int& { return x; });
    auto t = gcl::task([](auto p) -> int& { return p.get(); }, p);
    gcl::Async async{4};
    t.schedule(async);
    t.wait();
    REQUIRE(42 == p.get());
    REQUIRE(&x == &p.get());
}

TEST_CASE("schedule_and_release")
{
    auto t = gcl::task([]{ return 42; });
    REQUIRE(!t.valid());
    t.schedule();
    REQUIRE(t.valid());
    REQUIRE(42 == t.get());
    t.release();
    REQUIRE(!t.valid());
}

TEST_CASE("schedule_a_wide_graph")
{
    int x = 0;
    auto top = gcl::task([&x]{ return x++; });
    gcl::Vec<int> tasks;
    for (int i = 0; i < 10; ++i)
    {
        auto t1 = gcl::task([&x](auto t){ x++; return t.get(); }, top);
        auto t2 = gcl::task([&x](auto t){ x++; return t.get(); }, t1);
        tasks.push_back(t2);
    }
    auto bottom = gcl::task([&x](gcl::Vec<int> ts) { x++; gcl::wait(ts); }, tasks);
    bottom.schedule();
    REQUIRE(22 == x);
}

TEST_CASE("schedule_with_mixed_parents")
{
    int x = 0;
    auto p1 = gcl::task([&x]{ x++; return 42.0; });
    auto p2 = gcl::task([&x]{ x++; return 13; });
    auto p3 = gcl::task([&x]{ x++; return 20; });
    auto p4 = gcl::task([&x]{ x++; return 21; });
    auto p6 = gcl::task([&x]{ x++; return std::string{"guan"}; });
    auto t = gcl::join(p1, p2, gcl::vec(p3, p4), p6);
    t.schedule();
    t.wait();
    REQUIRE(5 == x);
}
