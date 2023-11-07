#include <seastar/core/future.hh>
#include <iostream>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/reactor.hh>

seastar::future<> run(const std::string &s) {
    std::cerr << "running on " << seastar::this_shard_id() <<
        ", str = " << s << "\n";
    return seastar::make_ready_future<>();
}
 

seastar::future<> f() {
    return seastar::smp::invoke_on_others(0,
        seastar::smp_submit_to_options(),
        []{
            return run("hello");
        });
}
