#!/bin/bash

dir=$(dirname $0)

if [ ! -f build.sh ]; then
    echo 'build.sh must be run within its container folder' 1>&2
    exit 1
fi

(cd $dir/index; sh ./build.sh)
(cd $dir/docformat; sh ./build.sh)
