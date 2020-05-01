# gcl

[![Travis](https://travis-ci.org/bloomen/gcl.svg?branch=master)](https://travis-ci.org/bloomen/gcl/branches) [![Appveyor](https://ci.appveyor.com/api/projects/status/memx407sve38sbj0?svg=true)](https://ci.appveyor.com/project/bloomen/gcl?branch=master)

A tiny graph concurrent library written in C++14.

Sample usage:
```cpp
auto t1 = gcl::task([]{ return 42; });
auto t2 = gcl::task([]{ return 13.3; });
gcl::Async async{4};
gcl::schedule(async, t1, t2).wait();
```
