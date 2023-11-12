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
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/file.hh>
#include <seastar/util/log.hh>
#include <seastar/util/tmp_file.hh>


using namespace seastar;

constexpr size_t aligned_size = 4096;
constexpr size_t record_size = 4096;

const open_flags wflags = open_flags::wo | open_flags::truncate | open_flags::create;

static logger LOG("extern_sort");

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

future<> read_chunk(sstring fn, temporary_buffer<char>& rbuf, size_t offset, size_t chunk_size) {
    return with_file(open_file_dma(fn, open_flags::ro), [&rbuf, offset, chunk_size] (file& f) {
        return f.dma_read(offset, rbuf.get_write(), chunk_size).then([&rbuf, chunk_size] (size_t count) {
            assert(count == chunk_size);
        });
    });
}

future<> write_chunk(sstring fn, temporary_buffer<char>& wbuf) {
    return with_file(open_file_dma(fn, wflags), [&wbuf] (file& f) {
        return f.dma_write(0, wbuf.get(), wbuf.size()).then([&wbuf] (size_t count) {
            assert(count == wbuf.size());
        });
    });
}

future<sstring> sort_chunk(sstring fn, size_t offset, size_t chunk_size) {
		sstring fn_out = get_filename("sort");
    LOG.info("Sorting {} range [{}, {}) -> {}", fn, offset, offset + chunk_size, fn_out);

    auto buf = temporary_buffer<char>::aligned(aligned_size, chunk_size);
    return do_with(std::move(buf), [fn, offset, chunk_size, fn_out](auto &buf) {
        return read_chunk(fn, buf, offset, chunk_size).then([&buf] {
            return async([&buf] {
                Record *records = Record::cast(buf);
                Record *records_end = records + buf.size() / record_size;
                std::sort(records, records_end);
            });
        }).then([fn_out, &buf]() {
            return write_chunk(fn_out, buf).then([fn_out] {
                return fn_out;
            });
        });
    });
}

future<output_stream<char>> make_output_stream(std::string_view fn) {
    return with_file_close_on_failure(open_file_dma(fn, wflags), [] (file f) {
        return make_file_output_stream(std::move(f), aligned_size);
    });
}

future<sstring> merge_sorted_chunks(sstring fn1, sstring fn2) {
    sstring fn_out = get_filename("merge");
    LOG.info("Merging {} {} -> {}", fn1, fn2, fn_out);

    return util::with_file_input_stream(fn1.data(), [fn2, fn_out](input_stream<char> &i1) {
        return util::with_file_input_stream(fn2.data(), [&i1, fn_out](input_stream<char> &i2) {
            return async([&i1, &i2, fn_out] {
                output_stream<char> o = make_output_stream(fn_out).get0();

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
                return fn_out;
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

std::vector<SortTask> get_sort_tasks(size_t fsize, size_t sort_buf_size) {
    std::vector<SortTask> sort_tasks;
    size_t offset = 0;
    while (offset < fsize) {
        size_t chunk_size = 0;
        if (offset + sort_buf_size <= fsize) {
            chunk_size = sort_buf_size;
        } else {
            chunk_size = fsize - offset;
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

future<sstring> concurrent_extern_sort(sstring fn, size_t fsize, size_t sort_buf_size) {
    std::vector<SortTask> sort_tasks = get_sort_tasks(fsize, sort_buf_size);

    return async([sort_tasks=std::move(sort_tasks), fn=std::move(fn)] {
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

future<> check_params(size_t max_sort_buf_size) {
    if (max_sort_buf_size % record_size != 0) {
       std::ostringstream os;
       os << "max_sort_buf_size (" << max_sort_buf_size
          << ") must be multiple of record size (" << record_size << ")";
       throw std::invalid_argument(os.str());
    }
    return make_ready_future<>();
}

size_t calc_sort_buf_size(size_t fsize, size_t max_sort_buf_size) {
    // Adjust sort_buf_size to evenly spread the load among the required shards 
    size_t shard_cnt = fsize / max_sort_buf_size + (fsize % max_sort_buf_size ? 1 : 0);
    size_t sort_buf_size = fsize / shard_cnt + (fsize % shard_cnt ? 1 : 0);
    sort_buf_size = (sort_buf_size / record_size + (sort_buf_size % record_size ? 1 : 0)) * record_size;
    sort_buf_size = std::min(sort_buf_size, max_sort_buf_size);
    return sort_buf_size;
}

future<> extern_sort(sstring fn, size_t max_sort_buf_size, bool clean) {
    return when_all_succeed(
        file_size(fn),
        file_accessible(fn, access_flags::exists | access_flags::read),
        check_params(max_sort_buf_size)
    ).then_unpack([fn, max_sort_buf_size] (size_t fsize, bool accessible) {
        if (!accessible)
            throw std::runtime_error(fn + " file is not accessible");
        if (fsize < record_size)
            throw std::runtime_error(fn + " file doesn't contain a full record");

        size_t sort_buf_size = calc_sort_buf_size(fsize, max_sort_buf_size);
        LOG.info("Using sort_buf_size={}", sort_buf_size);
        return concurrent_extern_sort(fn, fsize, sort_buf_size);
    }).then([fn](sstring fn_merged) {
        sstring fn_sorted = fn + ".sorted";
        std::rename(fn_merged.data(), fn_sorted.data());
        LOG.info("Created sorted file: {}", fn_sorted);
    }).handle_exception([] (std::exception_ptr e) {
        LOG.error("Exception: {}", e);
    });
}


