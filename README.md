# gcl

A tiny graph concurrent library written in C++14.

Sample usage:
```cpp
auto t1 = gcl::task([]{ return 42; });
auto t2 = gcl::task([]{ return 13.3; });
gcl::Par par{4};
gcl::schedule(par, t1, t2).wait();
```
