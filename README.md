# gcl

[![Actions](https://github.com/bloomen/gcl/actions/workflows/gcl-tests.yml/badge.svg?branch=master)](https://github.com/bloomen/gcl/actions/workflows/gcl-tests.yml?query=branch%3Amaster)

A **g**raph **c**oncurrent **l**ibrary for C++ that allows you to build a
computational graph with an efficient interface and a small memory footprint.
Requires a C++14 compliant compiler. Tested with Clang, GCC, and Visual Studio.

Sample usage:
```cpp
auto t1 = gcl::task([]{ return 42; });
auto t2 = gcl::task([]{ return 13.3; });
auto t3 = gcl::tie(t1, t2).then([](auto t1, auto t2){ return *t1.get() + *t2.get(); });
gcl::Async async{4};
t3.schedule_all(async);
t3.wait();
std::cout << *t3.get() << std::endl; // 55.3
```
