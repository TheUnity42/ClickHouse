FROM ubuntu:22.04

RUN apt-get update
RUN apt-get install -y wget git cmake ccache python3 ninja-build yasm gawk lsb-release software-properties-common gnupg build-essential

RUN bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"

RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test

ENV CC=clang-15
ENV CXX=clang++-15

#RUN cd /host/ClickHouse
#RUN bash build.sh

ENTRYPOINT /bin/bash
