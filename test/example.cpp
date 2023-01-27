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
