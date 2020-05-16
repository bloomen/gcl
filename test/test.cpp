#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "gcl.h"

namespace gcl
{

bool operator==(const Edge& lhs, const Edge& rhs)
{
    return lhs.parent == rhs.parent && lhs.child == rhs.child;
}

}

TEST_CASE("schedule")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::tie(p1, p2).then([](auto p1, auto p2){ return p1.get() + p2.get(); });
    t.schedule();
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::tie(gcl::vec(p1, p2)).then([](gcl::Vec<int> p){ return p[0].get() + p[1].get(); });
    t.schedule();
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_async")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::tie(p1, p2).then([](auto p1, auto p2){ return p1.get() + p2.get(); });
    gcl::Async async{4};
    t.schedule(async);
    t.wait();
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_with_vec_parents_using_async")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::tie(gcl::vec(p1, p2)).then([](gcl::Vec<int> p){ return p[0].get() + p[1].get(); });
    gcl::Async async{4};
    t.schedule(async);
    t.wait();
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_reference_type")
{
    int x = 42;
    auto p = gcl::task([&x]() -> int& { return x; });
    auto t = p.then([](auto p) -> int& { return p.get(); });
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
        auto t1 = top.then([&x](auto t){ x++; return t.get(); });
        auto t2 = t1.then([&x](auto t){ x++; return t.get(); });
        tasks.push_back(t2);
    }
    auto bottom = gcl::tie(tasks).then([&x](gcl::Vec<int>) { x++; });
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
    auto t = gcl::when(p1, p2, gcl::vec(p3, p4), p6);
    t.schedule();
    t.wait();
    REQUIRE(5 == x);
}

TEST_CASE("schedule_with_bind_and_when")
{
    int x = 0;
    auto p1 = gcl::task([&x]{ x++; });
    auto p2 = gcl::task([&x]{ x++; });
    auto t = gcl::when(gcl::tie(p1, p2));
    t.schedule();
    REQUIRE(2 == x);
}

TEST_CASE("edges")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::when(p1, p2);
    t.schedule();
    const std::vector<gcl::Edge> exp_edges = {
        {p1.id(), t.id()},
        {p2.id(), t.id()}
    };
    REQUIRE(exp_edges == t.edges());
}
