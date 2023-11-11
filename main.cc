#include <seastar/core/app-template.hh>
#include <seastar/util/log.hh>
#include <iostream>
#include <stdexcept>

extern seastar::future<> run_extern_sort();

int main(int argc, char** argv) {
    seastar::app_template app;
    try {
        app.run(argc, argv, run_extern_sort);
    } catch(...) {
        std::cerr << "Couldn't start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}
