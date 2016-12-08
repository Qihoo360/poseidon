#!/bin/bash
if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi

ROOT_DIR=`pwd`
APP="meta"
VER="0.1"

mkdir -p bin logs
mkdir meta

go build -o bin/$APP

cp -r bin/ meta/
cp -r conf/ meta/
cp serverctl meta/
cp -r logs meta/

tar -zcvf $ROOT_DIR/../../dist/$APP-$VER.tar.gz meta/

rm -rf ./meta
