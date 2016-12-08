#!/bin/bash

which gradle
if [ $? -eq 0 ];
then
    gradle distTar
else
    ./gradlew distTar
fi

if [ $? -ne 0 ];
then
    echo pkg index failed
fi

cp -r build/distributions/index*.tar ../../dist/
