#!/bin/bash

set -e

# Get the GCC version
gcc_version=$(gcc -dumpversion | cut -f1 -d.)

# Check if the version is greater than 11
if [ "$gcc_version" -ge 11 ]; then
        echo "GCC version is greater than 11 (version $gcc_version)."
else
        echo "GCC version is not greater than 11 (version $gcc_version)."
        exit 1
fi

make -j
#sudo make install
