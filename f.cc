#include <seastar/core/future.hh>
#include <iostream>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/core/memory.hh>

using namespace seastar;

constexpr size_t aligned_size = 4096;

future<> read_file(sstring filename, temporary_buffer<char>& rbuf) {
    return with_file(open_file_dma(filename, open_flags::ro), [&rbuf] (file& f) {
        return f.dma_read(0, rbuf.get_write(), aligned_size).then([&rbuf] (size_t count) {
            fmt::print("    read count {}\n", count);
            assert(count == aligned_size);
            return make_ready_future<>();
        });
    });
}

future<> demo_with_file(sstring filename) {
    fmt::print("Demonstrating with_file():\n");

    // Thread approach:
    // return async([filename] () {
    //     fmt::print("    retrieving data from {}\n", filename);
    //     fmt::print("    free memory {}\n", memory::stats().free_memory());
    //     auto rbuf = temporary_buffer<char>::aligned(aligned_size, 1000000000);
    //     read_file(filename, rbuf).get();
    // });

    auto rbuf = temporary_buffer<char>::aligned(aligned_size, 1000000000);
    return do_with(std::move(rbuf), [filename](auto &rbuf) {
            return read_file(filename, rbuf);
        });
}

future<> f() {
    return smp::invoke_on_all([] {
            return demo_with_file("/home/attilaj/work/data.txt");
        });
}
