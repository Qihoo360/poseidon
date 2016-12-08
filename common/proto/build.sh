#!/bin/bash

if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi

ROOT_DIR=`pwd`
PROTOC_BIN=protoc

$PROTOC_BIN --proto_path=. --go_out=../../service/searcher/proto/ poseidon_if.proto
$PROTOC_BIN --proto_path=. --java_out=../../builder/index/src/main/java/ poseidon_if.proto

