#include <seastar/core/future.hh>
#include <iostream>
#include <vector>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/when_all.hh>

using namespace seastar;

constexpr size_t aligned_size = 4096;
constexpr size_t record_size = 4096;

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
    fmt::print(": smp::count {}\n", smp::count);
    fmt::print(": smp::this_shard_id() {}\n", this_shard_id());

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

future<> external_merge(sstring filename, uint64_t size, uint64_t max_buffer_size) {
    fmt::print("filename {} size {} max_buffer_size {}\n", filename, size, max_buffer_size);

    uint64_t record_count = size / record_size; 
    uint64_t max_record_count_per_shard = record_count / smp::count + (record_count % smp::count ? 1 : 0);
    if (max_record_count_per_shard * record_size <= max_buffer_size) {
        std::vector<future<>> futures;
        futures.reserve(smp::count);
        for (uint64_t shard_id = 0; shard_id < smp::count; ++shard_id) {
            futures.push_back(
                smp::submit_to(0, smp_submit_to_options(), [] {
                        return demo_with_file("/home/attilaj/dummy/data.txt");
                    }));
        }
        return when_all_succeed(std::move(futures));
    }
    return make_ready_future<>();
}

future<> f() {
    // return smp::invoke_on_all([] {
    //         return demo_with_file("/home/attilaj/dummy/data.txt");
    //     });
    //
    //
    // return smp::submit_to(0,
    //     smp_submit_to_options(),
    //     [] {
    //         return demo_with_file("/home/attilaj/dummy/data.txt");
    //     });
 
    sstring filename = "/home/attilaj/dummy/data.txt";
    uint64_t max_buffer_size = 1024UL * 1024UL * 1024UL;

    return when_all_succeed(
            file_size(filename),
            file_accessible(filename, access_flags::exists | access_flags::read)
        ).then_unpack([filename, max_buffer_size] (uint64_t size, bool accessible) {
            if (!accessible)
                throw std::runtime_error(filename + " is not accessible");
            return external_merge(filename, size, max_buffer_size);        
        }).handle_exception([] (std::exception_ptr e) {
            std::cout << "Exception: " << e << "\n";
        });
}

