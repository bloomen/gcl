# gcl

[work in progress]

[![Travis](https://travis-ci.org/bloomen/gcl.svg?branch=master)](https://travis-ci.org/bloomen/gcl/branches) [![Appveyor](https://ci.appveyor.com/api/projects/status/memx407sve38sbj0?svg=true)](https://ci.appveyor.com/project/bloomen/gcl?branch=master)

A tiny graph concurrent library for C++ that allows you to build a computational
graph with a minimal interface and a small memory footprint.
Requires a C++14 compliant compiler. Tested with Clang, GCC, and Visual Studio.

Sample usage:
```cpp
auto t1 = gcl::task([]{ return 42; });
auto t2 = gcl::task([]{ return 13.3; });
auto t3 = gcl::tie(t1, t2).then([](auto t1, auto t2){ return t1.get() + t2.get(); });
gcl::Cache cache;
gcl::Async async{4};
t3.schedule(cache, async);
std::cout << t3.get() << std::endl; // 55.3
```
