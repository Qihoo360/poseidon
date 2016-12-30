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

rsync -r bin meta/
rsync -r conf meta/
rsync serverctl meta/
rsync -r logs meta/

tar -zcvf $ROOT_DIR/../../dist/$APP-$VER.tar.gz meta/

rm -rf ./meta
