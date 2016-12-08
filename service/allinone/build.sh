#!/bin/bash
if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi

ROOT_DIR=`pwd`
APP="allinone"
VER="0.1"

mkdir -p logs bin
mkdir allinone

go build -o bin/$APP

cp -r bin/ allinone/
cp -r conf/ allinone/
cp serverctl allinone/
cp -r logs allinone/

tar -zcvf $ROOT_DIR/../../dist/$APP-$VER.tar.gz allinone/

rm -rf ./allinone
