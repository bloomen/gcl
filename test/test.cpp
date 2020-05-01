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

TEST_CASE("schedule_using_Sequential")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    gcl::Sequential exec;
    t.schedule(exec);
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_Sequential_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Vec<int> p){ return p[0].get() + p[1].get(); }, gcl::vec(p1, p2));
    gcl::Sequential exec;
    t.schedule(exec);
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_Parallel")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    gcl::Parallel exec{4};
    t.schedule(exec);
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_Parallel_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    auto t = gcl::task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    gcl::Parallel exec{4};
    t.schedule(exec);
    REQUIRE(55 == t.get());
}

TEST_CASE("schedule_using_free_schedule")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    gcl::schedule(p1, p2).wait();
    REQUIRE(42 == p1.get());
    REQUIRE(13 == p2.get());
}

TEST_CASE("schedule_using_free_schedule_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    gcl::schedule(gcl::vec(p1, p2)).wait();
    REQUIRE(42 == p1.get());
    REQUIRE(13 == p2.get());
}

TEST_CASE("schedule_using_free_schedule_with_executor")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    gcl::Parallel exec{4};
    gcl::schedule(exec, p1, p2).wait();
    REQUIRE(42 == p1.get());
    REQUIRE(13 == p2.get());
}

TEST_CASE("schedule_using_free_schedule_with_executor_with_vec_parents")
{
    auto p1 = gcl::task([]{ return 42; });
    auto p2 = gcl::task([]{ return 13; });
    gcl::Parallel exec{4};
    gcl::schedule(exec, gcl::vec(p1, p2)).wait();
    REQUIRE(42 == p1.get());
    REQUIRE(13 == p2.get());
}
