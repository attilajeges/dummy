#include <algorithm>
#include <chrono>
#include <compare>
#include <cstdio>
#include <deque>
#include <vector>
#include <sstream>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/util/file.hh>
#include <seastar/core/when_all.hh>


using namespace seastar;

constexpr size_t aligned_size = 4096;
constexpr size_t record_size = 4096;

const open_flags wflags = open_flags::wo | open_flags::truncate | open_flags::create;

static logger LOG("external_merge_sort");

class Record {
public:
    Record(const Record &other) {
      std::copy(other._buf, other._buf + record_size, _buf);
    }

    Record &operator=(const Record &other) {
      std::copy(other._buf, other._buf + record_size, _buf);
      return *this;
    }

    std::strong_ordering operator<=>(const Record& other) const {
        return std::lexicographical_compare_three_way(
            _buf, _buf + record_size,
            other._buf, other._buf + record_size);
    }

    const char *get() const { return _buf; }
    char *getw() { return _buf; }

    static Record *cast(temporary_buffer<char> &buf) {
        return reinterpret_cast<Record*>(buf.get_write());
    }

private:
    char _buf[record_size];
};

sstring get_filename(std::string_view prefix) {
    const auto p1 = std::chrono::system_clock::now();
    std::ostringstream os;
    os << prefix << "-"
       << std::chrono::duration_cast<std::chrono::nanoseconds>(p1.time_since_epoch()).count() << "-"
       << this_shard_id();
    return os.str();
}

future<> read_chunk(sstring filename, temporary_buffer<char>& rbuf, size_t offset, size_t chunk_size) {
    return with_file(open_file_dma(filename, open_flags::ro), [&rbuf, offset, chunk_size] (file& f) {
        return f.dma_read(offset, rbuf.get_write(), chunk_size).then([&rbuf, chunk_size] (size_t count) {
            assert(count == chunk_size);
        });
    });
}

future<output_stream<char>> make_output_stream(std::string_view filename) {
    return with_file_close_on_failure(open_file_dma(filename, wflags), [] (file f) {
        return make_file_output_stream(std::move(f), aligned_size);
    });
}

future<> write_records_to_stream(output_stream<char>& o, const std::vector<const Record*>& records) {
    return seastar::do_for_each(records.begin(), records.end(), [&o] (const Record *r) {
        return o.write(r->get(), record_size);
    }).finally([&o] {
        return o.close();
    });
}

std::vector<const Record*> make_records(temporary_buffer<char> &buf) {
    const Record *begin = Record::cast(buf);
    const Record *end = begin + buf.size() / record_size;

    std::vector<const Record*> records;
    records.reserve(end - begin);
    for (const Record *r = begin; r < end; ++r) {
        records.emplace_back(r);
    }
    return records;
}
 
future<sstring> sort_chunk(sstring filename, size_t offset, size_t chunk_size) {
		sstring out_fn = get_filename("sort");
    LOG.info("Sorting {} range [{}, {}) -> {}", filename, offset, offset + chunk_size, out_fn);

    auto rwbuf = temporary_buffer<char>::aligned(aligned_size, chunk_size);
    return do_with(std::move(rwbuf), [filename, offset, chunk_size, out_fn](auto &rwbuf) {
        return read_chunk(filename, rwbuf, offset, chunk_size).then([&rwbuf] {
            return async([&rwbuf] {
                std::vector<const Record*> records = make_records(rwbuf);
                std::sort(records.begin(), records.end(), [](const Record *p1, const Record *p2) {
                    return *p1 < *p2;
                });
                return records;
            });
        }).then([&rwbuf, out_fn](std::vector<const Record*> records) {
            return async([records=std::move(records), out_fn] {
                output_stream<char> o = make_output_stream(out_fn).get0();
                write_records_to_stream(o, records).get();
                return out_fn;
            });
        });
    });
}

future<sstring> merge_sorted_chunks(sstring filename1, sstring filename2) {
    sstring out_fn = get_filename("merge");
    LOG.info("Merging {} {} -> {}", filename1, filename2, out_fn);

    return util::with_file_input_stream(filename1.data(), [filename2, out_fn](input_stream<char> &i1) {
        return util::with_file_input_stream(filename2.data(), [&i1, out_fn](input_stream<char> &i2) {
            return async([&i1, &i2, out_fn] {
                output_stream<char> o = make_output_stream(out_fn).get0();

                auto [rbuf1, rbuf2] = when_all_succeed(
                      i1.read_exactly(record_size),
                      i2.read_exactly(record_size)).get();

                while (rbuf1.size() == record_size && rbuf2.size() == record_size) {
                    const Record *rec1 = Record::cast(rbuf1);
                    const Record *rec2 = Record::cast(rbuf2);

                    auto cmp = *rec1 <=> *rec2;

                    if (cmp < 0) {
                        auto [next_buf] = when_all_succeed(
                              o.write(rec1->get(), record_size),
                              i1.read_exactly(record_size)).get();
                        rbuf1 = std::move(next_buf);
                    } else if (cmp > 0) {
                        auto [next_buf] = when_all_succeed(
                              o.write(rec2->get(), record_size),
                              i2.read_exactly(record_size)).get();
                        rbuf2 = std::move(next_buf);
                    } else {
                        Record recs[] = {*rec1, *rec2};
                        auto [next_buf1, next_buf2] = when_all_succeed(
                              o.write(recs[0].get(), 2 * record_size),
                              i1.read_exactly(record_size),
                              i2.read_exactly(record_size)).get();
                        rbuf1 = std::move(next_buf1);
                        rbuf2 = std::move(next_buf2);
                    }
                }

                while (rbuf1.size() == record_size) {
                    const Record *rec1 = Record::cast(rbuf1);
                    auto [next_buf] = when_all_succeed(
                          o.write(rec1->get(), record_size),
                          i1.read_exactly(record_size)).get();
                    rbuf1 = std::move(next_buf);
                }
                while (rbuf2.size() == record_size) {
                    const Record *rec2 = Record::cast(rbuf2);
                    auto [next_buf] = when_all_succeed(
                          o.write(rec2->get(), record_size),
                          i2.read_exactly(record_size)).get();
                    rbuf2 = std::move(next_buf);
                }

                o.close().get();
                return out_fn;
            });
        });
    });
}

struct SortTask {
    SortTask(size_t off, size_t chunk_size)
        : _off(off), _chunk_size(chunk_size) {
    }

    size_t _off;
    size_t _chunk_size;
};

std::vector<SortTask> get_sort_tasks(size_t size, size_t buffer_size) {
    std::vector<SortTask> sort_tasks;
    size_t offset = 0;
    while (offset < size) {
        size_t chunk_size = 0;
        if (offset + buffer_size <= size) {
            chunk_size = buffer_size;
        } else {
            chunk_size = size - offset;
            chunk_size = chunk_size / record_size * record_size;
            if (chunk_size == 0) {
                // Incomplete record at the end of the file, ignore it.
                break;
            }
        }

        sort_tasks.emplace_back(offset, chunk_size);
        offset += chunk_size;
    }
    return sort_tasks;
}

future<sstring> external_merge_sort(sstring filename, size_t size, size_t buffer_size) {
    std::vector<SortTask> sort_tasks = get_sort_tasks(size, buffer_size);

    return async([sort_tasks=std::move(sort_tasks), fn=std::move(filename)] {
        std::deque<sstring> merge_queue;
        std::vector<future<sstring>> futures;
        size_t shard_id = 0;
        size_t i = 0;

        auto resolve_futures = [&futures, &merge_queue, &shard_id]() mutable {
            if (!futures.empty()) {
                std::vector<sstring> fnames = when_all_succeed(std::move(futures)).get();
                shard_id = 0;
                futures.clear();
                for (const sstring &fn: fnames) {
                    merge_queue.emplace_back(fn);
                }
            }
        };

        auto resolve_filled_up_futures = [&futures, &resolve_futures]() {
            if (futures.size() == smp::count) {
                return resolve_futures();
            }
        };

        while (i < sort_tasks.size()) {
            const SortTask &task = sort_tasks[i++];
            futures.push_back(
                smp::submit_to(shard_id++, smp_submit_to_options(), [fn, task] {
                        return sort_chunk(fn, task._off, task._chunk_size);
                    }));
            resolve_filled_up_futures();
        }

        while (1) {
            if (merge_queue.size() <= 1)
                resolve_futures();

            if (merge_queue.size() == 1)
              return merge_queue.front();
    
            while (merge_queue.size() > 1) {
                sstring fn1 = merge_queue.front();
                merge_queue.pop_front();
    
                sstring fn2 = merge_queue.front();
                merge_queue.pop_front();
    
                futures.push_back(
                    smp::submit_to(shard_id++, smp_submit_to_options(), [fn1=std::move(fn1), fn2=std::move(fn2)] {
                            return merge_sorted_chunks(fn1, fn2);
                        }));
    
                resolve_filled_up_futures();
            }
        }
        throw std::logic_error("Unexpected failure");
    });
}

future<> check_params(size_t max_buffer_size, size_t min_buffer_size) {
    if (max_buffer_size < min_buffer_size) {
       std::ostringstream os;
       os << "Max buffer size (" << max_buffer_size
          << ") should be greater than or equal to min buffer size (" << min_buffer_size << ")";
       throw std::invalid_argument(os.str());
    }

    if (max_buffer_size % record_size != 0 || min_buffer_size % record_size != 0) {
       std::ostringstream os;
       os << "Max buffer size (" << max_buffer_size
          << ") and min buffer size (" << min_buffer_size
          << ") should both be multiplies of record size (" << record_size << ")";
       throw std::invalid_argument(os.str());
    }
    return make_ready_future<>();
}

future<> f() {
    sstring filename = "data.txt";

    // TODO: remove this
    fmt::print("    free memory {}\n", memory::stats().free_memory()); 
    // return seastar::make_ready_future<>();

    /// size_t max_buffer_size = memory::stats().free_memory() / 3 / 4096 * 4096;
    /// size_t min_buffer_size = std::min(1UL << 20, max_buffer_size);
    /// LOG.info("max_buffer_size={} min_buffer_size={}", max_buffer_size, min_buffer_size);


    // TODO: uncomment these
    size_t max_buffer_size = 1UL << 30; // 1G
    size_t min_buffer_size = 1UL << 20; // 1M
    // size_t max_buffer_size = 1UL << 20;
    // size_t min_buffer_size = 1UL << 20;
    // size_t max_buffer_size = 4096UL;
    // size_t min_buffer_size = 4096UL;

    return when_all_succeed(
            file_size(filename),
            file_accessible(filename, access_flags::exists | access_flags::read),
            check_params(max_buffer_size, min_buffer_size)
        ).then_unpack([filename, max_buffer_size, min_buffer_size] (size_t size, bool accessible) {
            if (!accessible)
                throw std::runtime_error(filename + " file is not accessible");
            if (size < record_size)
                throw std::runtime_error(filename + " file doesn't contain a full record");

            size_t rec_cnt = size / record_size;
            size_t rec_cnt_per_shard = rec_cnt / smp::count + (rec_cnt % smp::count ? 1 : 0);
            size_t bytes_per_shard = std::max(rec_cnt_per_shard * record_size, min_buffer_size);
            size_t buffer_size = std::min(bytes_per_shard, max_buffer_size);

            return external_merge_sort(filename, size, buffer_size);
        }).then([filename](sstring fn){
            sstring sorted_fn = filename + ".sorted";
            std::rename(fn.data(), sorted_fn.data());
            LOG.info("Created sorted file: {}", sorted_fn);
        }).handle_exception([] (std::exception_ptr e) {
            LOG.error("Exception: {}", e);
        });
}


