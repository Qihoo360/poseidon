#!/bin/sh

which gradle
if [ $? -eq 0 ];
then
    gradle distTar
else
    ./gradlew distTar
fi

if [ $? -ne 0 ];
then
    echo pkg hdfsreader failed
else
    echo "Pack Success"
fi

cp -r build/distributions/hdfsreader*.tar ../../dist/
