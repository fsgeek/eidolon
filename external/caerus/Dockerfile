# Build of the public Caerus artifact (Hildred et al., PVLDB 17(3) 2023) as released.
# Mirrors the repo's install-ext, with these departures:
#   - repo pinned to aaf0eb6 (last commit, 2023-12-21)
#   - libzmq pinned to v4.3.5 (install-ext clones HEAD)
#   - libzmq `make install` failure tolerated: the in-tree prefix copies headers
#     onto themselves. install-ext has no `set -e`, so it was silent there too;
#     the library itself is installed first and is checked for explicitly.
FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git wget unzip g++ cmake make autoconf automake libtool pkg-config \
    libboost-all-dev libevent-dev libdouble-conversion-dev libgoogle-glog-dev libgflags-dev \
    libiberty-dev libreadline-dev liblz4-dev liblzma-dev libsnappy-dev zlib1g-dev binutils-dev \
    libjemalloc-dev libssl-dev libunwind-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /caerus
RUN git clone https://github.com/josh-hildred/Caerus.git . && git checkout aaf0eb64ddbc89f9c45593fd60d0dba9686be1d5
WORKDIR /caerus/ext
RUN wget -q https://github.com/google/protobuf/releases/download/v3.2.0/protobuf-cpp-3.2.0.tar.gz \
 && tar -xzf protobuf-cpp-3.2.0.tar.gz && mv protobuf-3.2.0 protobuf && rm protobuf-cpp-3.2.0.tar.gz \
 && cd protobuf && ./autogen.sh && ./configure --prefix=$PWD && make -j16 && make install
RUN wget -q https://github.com/google/glog/archive/v0.3.4.tar.gz && tar -xzf v0.3.4.tar.gz \
 && mv glog-0.3.4 glog && rm v0.3.4.tar.gz && cd glog && ./configure --prefix=$PWD && make -j16 && make install
RUN git clone -b v4.3.5 --depth 1 https://github.com/zeromq/libzmq.git zeromq \
 && cd zeromq && ./autogen.sh && ./configure --prefix=$PWD && make -j16 \
 && (make install || true) && test -f lib/libzmq.so
RUN wget -q https://github.com/gflags/gflags/archive/v2.0.tar.gz && tar -xzf v2.0.tar.gz \
 && rm v2.0.tar.gz && mv gflags-2.0 gflags && cd gflags && ./configure --prefix=$PWD && make -j16 && make install
RUN git clone -b v1.13.0 --depth 1 https://github.com/google/googletest.git \
 && cd googletest && mkdir build && cd build && cmake .. && make -j16 && make install
WORKDIR /caerus
ENV LD_LIBRARY_PATH=/caerus/ext/zeromq/lib:/caerus/ext/protobuf/lib:/caerus/ext/gflags/.libs:/caerus/ext/glog/.libs
RUN bash -o pipefail -c 'cd src && make -j32 2>&1 | tee /caerus/build.log'
