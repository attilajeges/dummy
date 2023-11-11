#!/bin/bash

# Generates test input file of a given size
# Prerequisite packages: coreutils

set -eu -o pipefail

if [[ $# -ne 2 || ! "$1" =~ ^[0-9]+$ ]]; then
    echo "usage: $0 bytecount filename"
    exit 1
fi

base64 -w0 /dev/urandom | head -c "$1" > "$2"
