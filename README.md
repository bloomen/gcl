# gcl

[![Actions](https://github.com/bloomen/gcl/actions/workflows/gcl-tests.yml/badge.svg?branch=master)](https://github.com/bloomen/gcl/actions/workflows/gcl-tests.yml?query=branch%3Amaster)

A **g**raph **c**oncurrent **l**ibrary for C++ that allows you to build a
computational graph with an efficient interface and a small memory footprint.
Requires a C++14 compliant compiler. Tested with Clang, GCC, and Visual Studio.

Sample usage:
```cpp
#include <iostream>
#include <fstream>
#include <gcl.h>

int main() {
    gcl::MetaData meta; // To collect graph metadata. Completely optional.
    auto t1 = gcl::task([]{ return 42.0; }).md(meta, "parent1");
    auto t2 = gcl::task([]{ return 13.3; }).md(meta, "parent2");
    auto t3 = gcl::tie(t1, t2).then([](auto t1, auto t2){
                                    return *t1.get() + *t2.get(); }).md(meta, "child");
    gcl::Async async{4};
    t3.schedule_all(async); // Running the tasks using 4 threads
    t3.wait();
    std::cout << *t3.get() << std::endl; // 55.3
    std::ofstream{"example.dot"} << gcl::to_dot(t3.edges(), meta); // output in dot notation
}
```

The resulting graph looks like this:

![graph](https://raw.githubusercontent.com/bloomen/gcl/master/test/example.png)

A bubble represents a task and each arrow is an edge between two tasks.
The first line within a bubble is the unique task ID, followed by the task name,
and finally the task instance number.
