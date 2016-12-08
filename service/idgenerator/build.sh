#!/bin/bash
if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi

ROOT_DIR=`pwd`
APP=`basename $ROOT_DIR`
VER="0.1"


mkdir -p bin logs
mkdir idgenerator

go build -o bin/$APP

cp -r bin/ idgenerator/
cp -r conf/ idgenerator/
cp serverctl idgenerator/
cp -r logs idgenerator/


tar -zcvf $ROOT_DIR/../../dist/idgenerator-$VER.tar.gz idgenerator/

rm -rf ./idgenerator
