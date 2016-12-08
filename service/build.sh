#!/bin/bash

if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi

mkdir -p ../dist
cd ./hdfsreader && sh ./build.sh && cd -
cd ./idgenerator && sh ./build.sh && cd -
cd ./proxy && sh ./build.sh && cd -
cd ./searcher && sh ./build.sh && cd -
cd ./meta && sh ./build.sh && cd -
cd ./allinone && sh ./build.sh && cd -

