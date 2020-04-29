#include "test.h"

TEST_CASE("Task_schedule")
{
    auto p1 = gcl::make_task([]{ return 42; });
    auto p2 = gcl::make_task([]{ return 13; });
    auto t = gcl::make_task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    t.schedule();
    REQUIRE(55 == t.get());
}

TEST_CASE("Task_schedule_using_Sequential")
{
    auto p1 = gcl::make_task([]{ return 42; });
    auto p2 = gcl::make_task([]{ return 13; });
    auto t = gcl::make_task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    gcl::Sequential exec;
    t.schedule(exec);
    REQUIRE(55 == t.get());
}

TEST_CASE("Task_schedule_using_Parallel")
{
    auto p1 = gcl::make_task([]{ return 42; });
    auto p2 = gcl::make_task([]{ return 13; });
    auto t = gcl::make_task([](gcl::Task<int> p1, gcl::Task<int> p2){ return p1.get() + p2.get(); }, p1, p2);
    gcl::Parallel exec{4};
    t.schedule(exec);
    REQUIRE(55 == t.get());
}

TEST_CASE("Task_schedule_using_wait")
{
    int x = 0;
    int y = 0;
    auto p1 = gcl::make_task([&x]{ x = 42; return x; });
    auto p2 = gcl::make_task([&y]{ y = 13; return y; });
    auto t = gcl::make_task(gcl::wait<int, int>, p1, p2);
    gcl::Parallel exec{4};
    t.schedule(exec);
    t.wait();
    REQUIRE(42 == x);
    REQUIRE(13 == y);
}
