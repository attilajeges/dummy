#!/usr/bin/python

import argparse

RECORD_SIZE = 4096

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            prog='check-sorted-file.py',
            description='Checks if the given file containing 4K ascii records is sorted')

    parser.add_argument('filename', type=str)
    args = parser.parse_args()

    with open(args.filename, 'r') as f:
        prev_record = None
        dup_record_count = 0;
        while True:
            record = f.read(RECORD_SIZE)
            if not record:
                break;

            if len(record) != RECORD_SIZE:
                print(f'File {args.filename} ends with an incomplete record')
                exit(1);
            if prev_record is not None and record < prev_record:
                print(f'File {args.filename} is NOT sorted')
                exit(1);
            if record == prev_record:
                dup_record_count += 1
            prev_record = record

    print(f'File {args.filename} is sorted.\nFound {dup_record_count} duplicate records.')

