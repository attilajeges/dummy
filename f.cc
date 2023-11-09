#include <seastar/core/future.hh>
#include <iostream>
#include <vector>
#include <algorithm>
#include <sstream>
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


struct record_type { 
    char _buf[record_size];

    record_type(const record_type &other) {
      strncpy(_buf, other._buf, record_size);
    }
    record_type &operator=(const record_type &other) {
      strncpy(_buf, other._buf, record_size);
      return *this;
    }
    bool operator<(const record_type &other) const {
      return strncmp(_buf, other._buf, record_size) < 0;
    }
};

future<> read_file(sstring filename, temporary_buffer<char>& rbuf, uint64_t offset, uint64_t chunk_size) {
    return with_file(open_file_dma(filename, open_flags::ro), [&rbuf, offset, chunk_size] (file& f) {
        return f.dma_read(offset, rbuf.get_write(), chunk_size).then([&rbuf, chunk_size] (size_t count) {
            fmt::print("    read count {} chunk_size {}\n", count, chunk_size);
            assert(count == chunk_size);
            return make_ready_future<>();
        });
    });
}

future<> write_file(sstring filename, temporary_buffer<char>& rbuf, uint64_t size) {
    open_flags wflags = open_flags::wo | open_flags::truncate | open_flags::create;
    return with_file(open_file_dma(filename, wflags), [&rbuf, size] (file& f) {
        return f.dma_write(0, rbuf.get(), size).then([&rbuf, size] (size_t count) {
            fmt::print("    write count {}\n", count);
            assert(count == size);
            return make_ready_future<>();
        });
    });
}

future<sstring> read_and_sort(sstring filename, uint64_t offset, uint64_t chunk_size) {
    fmt::print("read_and_sort");
    fmt::print("-------- smp::count {}\n", smp::count);
    fmt::print("-------- this_shard_id() {}\n", this_shard_id());
    fmt::print("-------- filename {} offset {} chunk_size {}\n", filename, offset, chunk_size);

    // Thread approach:
    // return async([filename] () {
    //     fmt::print("    retrieving data from {}\n", filename);
    //     fmt::print("    free memory {}\n", memory::stats().free_memory());
    //     auto rbuf = temporary_buffer<char>::aligned(aligned_size, 1000000000);
    //     read_file(filename, rbuf).get();
    // });

    auto rbuf = temporary_buffer<char>::aligned(aligned_size, chunk_size);
    return do_with(std::move(rbuf), [filename, offset, chunk_size](auto &rbuf) {
            return read_file(filename, rbuf, offset, chunk_size).then([&rbuf, chunk_size] {
                    return async([&rbuf, chunk_size] {
                            // sorting will stall the event log
                            // this is ok as there are no i/o events here to take care of.
                            record_type *record_buffer = reinterpret_cast<record_type*>(rbuf.get_write());
                            std::sort(record_buffer,
                                record_buffer + chunk_size / record_size);
                        });
                }).then([filename, offset, chunk_size, &rbuf] {
                    std::ostringstream os;
                    os << filename << "-" << offset << "-" << this_shard_id();
                    sstring out_filename{os.str()};
                    fmt::print("out_filename {} \n", out_filename);
                    return write_file(os.str(), rbuf, chunk_size).then([out_filename] {
                            return out_filename;
                        });
                });
        });
}

future<std::vector<std::vector<sstring>>> futures_for_initial_sort(sstring filename, uint64_t size, uint64_t buffer_size) {
    std::vector<future<std::vector<sstring>>> futures;
    uint64_t offset = 0;
    while (offset < size) {
        std::vector<future<sstring>> futures_in_iteration;
        for (uint64_t shard_id = 0; shard_id < smp::count && offset < size; ++shard_id) {
            uint64_t chunk_size = 0;
            if (offset + buffer_size <= size) {
                chunk_size = buffer_size;
            } else {
                chunk_size = size - offset;
                chunk_size = chunk_size / record_size * record_size;
                if (chunk_size == 0) {
                    // Incomplete record at the end of the file, ignore it.
                    offset = size;
                    break;
                }
            }

            futures_in_iteration.push_back(
                smp::submit_to(shard_id, smp_submit_to_options(), [filename, offset, chunk_size] {
                        return read_and_sort(filename, offset, chunk_size);
                    }));
            offset += chunk_size;
        }
        if (!futures_in_iteration.empty())
            futures.push_back(when_all_succeed(std::move(futures_in_iteration)));
    }
    return when_all_succeed(std::move(futures));
}

future<std::vector<std::vector<sstring>>> external_merge_sort(sstring filename, uint64_t size, uint64_t max_buffer_size, uint64_t min_buffer_size) {
    fmt::print("filename {} size {} max_buffer_size {} min_buffer_size {}\n", filename, size, max_buffer_size, min_buffer_size);

    uint64_t record_count = size / record_size; 
    uint64_t record_count_per_shard = record_count / smp::count + (record_count % smp::count ? 1 : 0);
    uint64_t byte_count_per_shard = std::max(record_count_per_shard * record_size, min_buffer_size); 
    if (byte_count_per_shard <= max_buffer_size) {
        // One run on the avaialble shards will suffice
        return futures_for_initial_sort(filename, size, byte_count_per_shard);
    } else {
        // Multiple runs on the available nodes are needed
        return futures_for_initial_sort(filename, size, max_buffer_size);
    }
}

future<> check_params(uint64_t max_buffer_size, uint64_t min_buffer_size) {
    if (max_buffer_size < min_buffer_size) {
       std::ostringstream os;
       os << "Max buffer size (" << max_buffer_size << ") should be greater than or equal to min buffer size (" << min_buffer_size << ")";
       throw std::runtime_error(os.str());
    }

    if (max_buffer_size % record_size != 0 || min_buffer_size % record_size != 0) {
       std::ostringstream os;
       os << "Max buffer size (" << max_buffer_size << ") and min buffer size (" << min_buffer_size << ") should both be multiplies of record size (" << record_size << ")";
       throw std::runtime_error(os.str());
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
 
    // sstring filename = "/home/attilaj/dummy/data.txt";
    sstring filename = "/home/attilaj/dummy/data-big.txt";

    // TODO: uncomment these
    uint64_t max_buffer_size = 512 * (1UL << 20); // 512M
    uint64_t min_buffer_size = 1UL << 20; // 1M
    // uint64_t max_buffer_size = 4096UL;
    // uint64_t min_buffer_size = 4096UL;

    return when_all_succeed(
            file_size(filename),
            file_accessible(filename, access_flags::exists | access_flags::read),
            check_params(max_buffer_size, min_buffer_size)
        ).then_unpack([filename, max_buffer_size, min_buffer_size] (uint64_t size, bool accessible) {
            if (!accessible)
                throw std::runtime_error(filename + " file is not accessible");
            if (size < record_size)
                throw std::runtime_error(filename + " file doesn't contain a full record");
            return external_merge_sort(filename, size, max_buffer_size, min_buffer_size);
        }).then([](std::vector<std::vector<sstring>> fn_vectors){
            for (const auto& fn_vec: fn_vectors)
                for (const auto& fn: fn_vec)
                    fmt::print("Created filename: {}\n", fn);
        }).handle_exception([] (std::exception_ptr e) {
            std::cout << "Exception: " << e << "\n";
        });
}

