#!/bin/bash
if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi

ROOT_DIR=`pwd`
APP="proxy"
VER="0.1"

mkdir -p bin logs
mkdir proxy

go build -o bin/$APP

cp -r bin/ proxy/
cp -r conf/ proxy/
cp serverctl proxy/
cp -r logs proxy/

tar -zcvf $ROOT_DIR/../../dist/$APP-$VER.tar.gz proxy/

rm -rf ./proxy
