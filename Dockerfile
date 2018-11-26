FROM ubuntu:14.04
MAINTAINER Jyothish Soman <jyothish.soman@gmail.com>

# get dependencies
RUN apt-get update
RUN apt-get install -y build-essential git-core cmake 
RUN apt-get clean
RUN apt-get install -y curl
RUN curl https://sh.rustup.rs -sSf > rustup-init.sh
RUN sh rustup-init.sh -y --default-toolchain nightly
WORKDIR /usr/local/src
RUN git clone https://github.com/jyosoman/libpvm-rs.git
WORKDIR /usr/local/src/libpvm-rs
RUN git submodule update --init
WORKDIR build
RUN cmake ..
RUN echo $HOME
RUN /bin/bash -c "source $HOME/.cargo/env; make"
