#!/bin/bash

if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi


mkdir -p dist

cd builder
sh build.sh
if [ $? -ne 0 ]; then exit -1; fi;
cd ..

cd service
sh build.sh
if [ $? -ne 0 ]; then exit -1; fi;
cd ..

