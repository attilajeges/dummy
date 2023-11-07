#include <seastar/core/future.hh>
#include <iostream>
#include <seastar/core/sharded.hh>
#include <seastar/core/reactor.hh>

class my_service {
public:
    std::string _str;
    my_service(const std::string& str) : _str(str) { }
    seastar::future<> run() {
        // std::cerr << "running on " << seastar::engine().cpu_id() <<
        std::cerr << "running on " << seastar::this_shard_id() <<
            ", _str = " << _str << "\n";
        return seastar::make_ready_future<>();
    }
    seastar::future<> stop() {
        return seastar::make_ready_future<>();
    }
};


seastar::sharded<my_service> s;
seastar::sharded<my_service> s2;

seastar::future<> f() {
    return s.start(std::string("hello")).then([] {
        return s.invoke_on_all([] (my_service& local_service) {
            return local_service.run();
        });
    }).then([] {
        return s.stop();
    }).then([] {
      return  s2.start(std::string("hello2")).then([] {
           return s2.invoke_on_all([] (my_service& local_service) {
               return local_service.run();
           });
       }).then([] {
           return s2.stop();
       });
    });
}
