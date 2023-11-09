#include <seastar/core/future.hh>
#include <iostream>
#include <vector>
#include <algorithm>
#include <deque>
#include <compare>
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


class Record {
public:
    Record(const Record &other) {
      std::copy(other._buf, other._buf + record_size, _buf);
    }

    Record &operator=(const Record &other) {
      std::copy(other._buf, other._buf + record_size, _buf);
      return *this;
    }

    bool operator<(const Record& other) const {
        return cmp(other) < 0;
    }

    std::strong_ordering cmp(const Record& other) const {
        return std::lexicographical_compare_three_way(
            _buf, _buf + record_size,
            other._buf, other._buf + record_size);
    }

    char *get() {
        return _buf;
    }

    static Record *ptr_to_temp_buffer(temporary_buffer<char> &buf) {
        return reinterpret_cast<Record*>(buf.get_write());
    }

private:
    char _buf[record_size];
};

future<> read_chunk(sstring filename, temporary_buffer<char>& rbuf, uint64_t offset, uint64_t chunk_size) {
    return with_file(open_file_dma(filename, open_flags::ro), [&rbuf, offset, chunk_size] (file& f) {
        return f.dma_read(offset, rbuf.get_write(), chunk_size).then([&rbuf, chunk_size] (size_t count) {
            assert(count == chunk_size);
        });
    });
}

future<> write_file(sstring filename, temporary_buffer<char>& rbuf, uint64_t size) {
    open_flags wflags = open_flags::wo | open_flags::truncate | open_flags::create;
    return with_file(open_file_dma(filename, wflags), [&rbuf, size] (file& f) {
        return f.dma_write(0, rbuf.get(), size).then([&rbuf, size] (size_t count) {
            assert(count == size);
        });
    });
}

future<sstring> read_sort_write(sstring filename, uint64_t offset, uint64_t chunk_size) {
    auto rbuf = temporary_buffer<char>::aligned(aligned_size, chunk_size);
    return do_with(std::move(rbuf), [filename, offset, chunk_size](auto &rbuf) {
            return read_chunk(filename, rbuf, offset, chunk_size).then([&rbuf, chunk_size] {
                    return async([&rbuf, chunk_size] {
                            // TODO: fixme
                            // sorting will stall the event log
                            // this is ok as there are no i/o events here to take care of.
                            Record *record_buffer = Record::ptr_to_temp_buffer(rbuf);
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



future<> merge_and_write(sstring filename1, sstring filename2, sstring out_filename) {
    return with_file(open_file_dma(filename1, open_flags::ro), [filename2, out_filename] (file& f1) {
            return with_file(open_file_dma(filename2, open_flags::ro), [&f1, out_filename] (file& f2) {
                open_flags wflags = open_flags::wo | open_flags::truncate | open_flags::create;
                return with_file(open_file_dma(out_filename, wflags), [&f1, &f2] (file& fout) {
                        return async([&f1, &f2, &fout] {
                                auto rbuf1 = temporary_buffer<char>::aligned(aligned_size, record_size);
                                auto rbuf2 = temporary_buffer<char>::aligned(aligned_size, record_size);
                                size_t off1 = 0;
                                size_t off2 = 0;
                                size_t out_off = 0;
                                future<size_t> read1 = f1.dma_read(off1, rbuf1.get_write(), record_size);
                                future<size_t> read2 = f2.dma_read(off2, rbuf2.get_write(), record_size);
                                auto [cnt1, cnt2] = when_all_succeed(std::move(read1), std::move(read2)).get();

                                Record *rec1 = Record::ptr_to_temp_buffer(rbuf1);
                                Record *rec2 = Record::ptr_to_temp_buffer(rbuf2);

                                while (cnt1 == record_size && cnt2 == record_size) {
                                    auto cmp = rec1->cmp(*rec2);

                                    if (cmp < 0) {
                                        Record rec1_cp{*rec1};
                                        off1 += record_size;
                                        future<size_t> read1 = f1.dma_read(off1, rec1->get(), record_size);
                                        future<size_t> write1 = fout.dma_write(out_off, rec1_cp.get(), record_size);
                                        auto [wcnt1, rcnt1] = when_all_succeed(std::move(write1), std::move(read1)).get();
                                        assert(wcnt1 == record_size);
                                        out_off += wcnt1;
                                        cnt1 = rcnt1;
                                    } else if (cmp > 0) {
                                        Record rec2_cp{*rec2};
                                        off2 += record_size;
                                        future<size_t> read2 = f2.dma_read(off2, rec2->get(), record_size);
                                        future<size_t> write2 = fout.dma_write(out_off, rec2_cp.get(), record_size);
                                        auto [wcnt2, rcnt2] = when_all_succeed(std::move(write2), std::move(read2)).get();
                                        assert(wcnt2 == record_size);
                                        out_off += wcnt2;
                                        cnt2 = rcnt2;
                                    } else {
                                        Record rec1_cp{*rec1};
                                        off1 += record_size;
                                        future<size_t> read1 = f1.dma_read(off1, rec1->get(), record_size);
                                        off2 += record_size;
                                        future<size_t> read2 = f2.dma_read(off2, rec2->get(), record_size);
                                        future<size_t> write1 = fout.dma_write(out_off, rec1_cp.get(), record_size);
                                        future<size_t> write2 = fout.dma_write(out_off, rec1_cp.get(), record_size);
                                        auto [wcnt1, wcnt2, rcnt1, rcnt2] = when_all_succeed(std::move(write1), std::move(write2), std::move(read1), std::move(read2)).get();
                                        assert(wcnt1 == record_size && wcnt2 == record_size);
                                        out_off += wcnt1 + wcnt2;
                                        cnt1 = rcnt1;
                                        cnt2 = rcnt2;
                                    }
                                }

                                while (cnt1 == record_size) {
                                    Record rec1_cp{*rec1};
                                    off1 += record_size;
                                    future<size_t> read1 = f1.dma_read(off1, rec1->get(), record_size);
                                    future<size_t> write1 = fout.dma_write(out_off, rec1_cp.get(), record_size);
                                    auto [wcnt1, rcnt1] = when_all_succeed(std::move(write1), std::move(read1)).get();
                                    assert(wcnt1 == record_size);
                                    out_off += wcnt1;
                                    cnt1 = rcnt1;
                                }


                               while (cnt2 == record_size) {
                                    Record rec2_cp{*rec2};
                                    off2 += record_size;
                                    future<size_t> read2 = f2.dma_read(off2, rec2->get(), record_size);
                                    future<size_t> write2 = fout.dma_write(out_off, rec2_cp.get(), record_size);
                                    auto [wcnt2, rcnt2] = when_all_succeed(std::move(write2), std::move(read2)).get();
                                    assert(wcnt2 == record_size);
                                    out_off += wcnt2;
                                    cnt2 = rcnt2;
                                }
                            });
                    });
                });
        });
}

struct SortTask {
    SortTask(sstring fn, size_t off, size_t chunk_size)
        : _fn(std::move(fn)), _off(off), _chunk_size(chunk_size) {
    }

    sstring _fn;
    size_t _off;
    size_t _chunk_size;
};

std::vector<SortTask> get_sort_tasks(sstring filename, uint64_t size, uint64_t buffer_size) {
    std::vector<SortTask> sort_tasks;
    uint64_t offset = 0;
    while (offset < size) {
        uint64_t chunk_size = 0;
        if (offset + buffer_size <= size) {
            chunk_size = buffer_size;
        } else {
            chunk_size = size - offset;
            chunk_size = chunk_size / record_size * record_size;
            if (chunk_size == 0) {
                // Incomplete record at the end of the file, skip it and jump to the end.
                offset = size;
                break;
            }
        }

        sort_tasks.emplace_back(filename, offset, chunk_size);
        offset += chunk_size;
    }
    return sort_tasks;
}

future<sstring> external_merge_sort(sstring filename, uint64_t size, uint64_t buffer_size) {
    std::vector<SortTask> sort_tasks = get_sort_tasks(filename, size, buffer_size);

    return async([sort_tasks=std::move(sort_tasks)] {
        std::deque<sstring> merge_queue;
        std::vector<future<sstring>> futures;
        size_t shard_id = 0;
        size_t i = 0;
        while (i < sort_tasks.size()) {
            if (shard_id == smp::count) {
                std::vector<sstring> fnames = when_all_succeed(std::move(futures)).get();
                shard_id = 0;
                futures.clear();
                for (const sstring &fn: fnames) {
                    merge_queue.emplace_back(fn);
                }
            }

            const SortTask &task = sort_tasks[i++];
            futures.push_back(
                smp::submit_to(shard_id++, smp_submit_to_options(), [task] {
                        return read_sort_write(task._fn, task._off, task._chunk_size);
                    }));
        }

        if (shard_id == smp::count) {
            std::vector<sstring> fnames = when_all_succeed(std::move(futures)).get();
            shard_id = 0;
            futures.clear();
            for (const sstring &fn: fnames) {
                merge_queue.emplace_back(fn);
            }
        }

        if (!merge_queue.empty()) {
            while (1) {
                if (shard_id == smp::count) {
                    std::vector<sstring> fnames = when_all_succeed(std::move(futures)).get();
                    shard_id = 0;
                    futures.clear();
                    for (const sstring &fn: fnames) {
                        merge_queue.emplace_back(fn);
                    }
                }

                if (merge_queue.size() == 1 && futures.empty()) {
                    return merge_queue.front();
                }

                if (merge_queue.size() > 1 && shard_id < smp::count) {
                    sstring fn1 = merge_queue.front();
                    merge_queue.pop_front();

                    sstring fn2 = merge_queue.front();
                    merge_queue.pop_front();

                    std::ostringstream os;
                    os << fn1 << "-" << fn2 << "-" << shard_id;
                    sstring out_fn = os.str();

                    futures.push_back(
                        smp::submit_to(shard_id++, smp_submit_to_options(), [fn1=std::move(fn1), fn2=std::move(fn2), out_fn=std::move(out_fn)] {
                                return merge_and_write(fn1, fn2, out_fn).then([out_fn] {
                                        return out_fn;
                                    });
                            }));
                }
            }
        }

        throw std::runtime_error("Unexpected failure");
    });
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
    sstring filename = "data-big.txt";

    // TODO: uncomment these
    uint64_t max_buffer_size = 1UL << 30; // 1G
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

            uint64_t rec_cnt = size / record_size;
            uint64_t rec_cnt_per_shard = rec_cnt / smp::count + (rec_cnt % smp::count ? 1 : 0);
            uint64_t bytes_per_shard = std::max(rec_cnt_per_shard * record_size, min_buffer_size);
            size_t buffer_size = std::min(bytes_per_shard, max_buffer_size);

            return external_merge_sort(filename, size, buffer_size);
        }).then([](sstring fn){
            fmt::print("Created filename: {}\n", fn);
        }).handle_exception([] (std::exception_ptr e) {
            std::cout << "Exception: " << e << "\n";
        });
}

