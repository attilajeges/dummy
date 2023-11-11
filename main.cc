#include <seastar/core/app-template.hh>
#include <seastar/util/log.hh>
#include <iostream>
#include <stdexcept>

namespace po = boost::program_options;

extern seastar::future<> extern_sort(seastar::sstring filename, size_t max_sort_buf_size, bool clean);

int main(int argc, char** argv) {
    seastar::app_template app;

    app.add_options()
        ("clean", "remove intermediate files when done")
        ;
    app.add_positional_options({
       {"filename", po::value<seastar::sstring>()->required(),
        "input data file containing 4096 byte ascii records", 1},
       {"max_sort_buf_size", po::value<size_t>()->required(),
        "maximum buffer size (in bytes) used for sorting per shard (must be multiple of 4096)", 1}
    });
    try {
        app.run(argc, argv, [&app] {
            auto &args = app.configuration();
            return extern_sort(
                 args["filename"].as<seastar::sstring>(),
                 args["max_sort_buf_size"].as<size_t>(),
                 args.count("clean") != 0);
        });
    } catch(...) {
        std::cerr << "Couldn't start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}
