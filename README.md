# Sorting a large file with Seastar
## Task
You are given a large file (much larger than memory), a multi-core
machine with lots of RAM, and a good disk subsystem.

The file is composed of records of 4K bytes. The goal is to create a
sorted version of the file, with the records sorted lexicographically,
using Seastar.
You can assume that the file is of size N * 4K, where N is an integer,
you don't have to deal with incomplete chunks. You can assume that the
chunks are made up of ASCII characters.
You can use as many intermediary files as you want.

## Approach
1. Split the input file to large chunks. Sort them in memory and write the results to intermediary files.
   - Sorting file chunks can be done in parallel on avaialble shards.
   - Resulting files are queued for later processing
2. Merge pair of intermediary files until there's only one file left
   - Merging intermediary files can be done in parallel with other sorting / merging tasks
   - Resulting files are queued for further processing.

## Building
1. Clone and build Seastar: https://github.com/scylladb/seastar
2. Build `extern_sort` executable using Seastar from its build directory:
   ```
   $ export SEASTAR_DIR='/path/to/seastar'
   $ g++ -Wall -std=c++20 -I "$SEASTAR_DIR/include" main.cc extern_sort.cc $(pkg-config --libs --cflags --static "$SEASTAR_DIR/build/release/seastar.pc") -o extern_sort
   ```
## Running
1. Create a test input file with random data of a given size
   ```
   $ ./gen-test-input.sh "$(( 32 * 1024**3 ))" data.txt   ## Creates an 32GB test input file: data.txt
   ```
2. Run `extern_sort`. It has two required positional arguments:
   - filename: input data file
   - max_sort_buf_size: maximum size of buffer in bytes used for initial sorting. Must be a multiple of 4096.
   ```
   $ ## Runs external sort with 8 threads on data.txt input file. Initial sorting buffer cannot be bigger than 2GB.
   $ ## The resulting sorted file will be named data.txt.sorted
   $ ./extern_sort -c8 data.txt "$((2 * 1024**3))"
   ```
3. A python script (requires python 3.11) is included to do a simple verification on the resulting file
   ```
   $ ./check-sorted-file.py  data.txt.sorted
   File data.txt.sorted is sorted.
   Found 0 duplicate records.
   ```
## Issues
While doing the initial sort on larger file chunks, seastar will complain about stalling the event loop. This is expected and normally should be avoided.
In this particular case, stalling the event loop doesn't cause too much trouble. There are no concurrent events that the event loop should handle in the meantime anyway.

I tried to solve the issue by calling `thread::yield()` every couple iterations during sorting but it didn't help. I didn't have time to further investigate this.
