#!/usr/bin/env bash

PL_PATH=`readlink -f ./build/plugins`

if [[ ! -d ${PL_PATH} ]]
then
    mkdir -p ${PL_PATH}
fi

cd ./plugins/

for path in *;
do
    cd ${path}
    cargo build
    cp ./target/debug/*.so ${PL_PATH}
    cd ..
done
