#!/bin/bash

set -e
set -x

##Assumes Centos 8
## You will find the tools in /usr/share/bcc/tools/

# Ensure your system is up to date
sudo dnf update -y

# Install Python 3 (while keeping Python 2 as default)
sudo dnf install -y python3 python3-setuptools python3-pip expect

# Optional: Ensure pip for Python 3 is installed and updated
sudo python3 -m ensurepip --upgrade
sudo python3 -m pip install --upgrade pip setuptools

# Install build tools and dependencies
sudo dnf install -y \
  zip bison cmake gcc gcc-c++ make git flex \
  llvm llvm-devel \
  clang clang-devel \
  zlib-devel elfutils-libelf-devel libffi-devel

# Enable EPEL and PowerTools repositories
sudo dnf install -y epel-release

# Install ncurses and terminfo Development Libraries
# Can check if the required library is present by running "ls /usr/lib64/libtinfo.so"
sudo dnf install -y ncurses-devel

# Clone the BCC repository
git clone https://github.com/iovisor/bcc.git
mkdir -p bcc/build
pushd bcc/build || exit

# Configure CMake for building BCC
# Using DENABLE_LLVM_SHARED flag because of https://github.com/bpftrace/bpftrace/issues/1855
cmake .. -DCMAKE_INSTALL_PREFIX=/usr \
         -DCMAKE_BUILD_TYPE=Release \
         -DBUILD_SHARED_LIBS=ON \
         -DPYTHON_CMD=python3 \
         -DLLVM_DIR=$(llvm-config --prefix)/lib/cmake/llvm \
         -DENABLE_LLVM_SHARED=1

# Build and install BCC
make -j$(nproc)
sudo make install

# Return to the original directory
popd || exit

# The shebangs on the tools' files have /usr/bin/env python written on them, they seem to be picking up the
# system python, which is python2.
# Modify the shebang lines in the BCC tools to explicitly use Python 3.
# This replaces /usr/bin/env python with /usr/bin/env python3 in all tool scripts.
sudo find /usr/share/bcc/tools -type f -exec sed -i '1s|^#!/usr/bin/env python|#!/usr/bin/env python3|' {} +

echo "Installation completed successfully! You will find the tools in /usr/share/bcc/tools/"
