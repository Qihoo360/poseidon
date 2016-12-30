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

rsync -r bin proxy/
rsync -r conf proxy/
rsync serverctl proxy/
rsync -r logs proxy/

tar -zcvf $ROOT_DIR/../../dist/$APP-$VER.tar.gz proxy/

rm -rf ./proxy
