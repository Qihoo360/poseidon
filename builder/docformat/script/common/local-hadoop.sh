#!/bin/bash
# exec 1>>logs/local-hadoop.log 2>&1
echo "$(date "+%Y/%m/%d %H:%M:%S") this a local hadoop client for demo"
echo "params is: $*"
# local-hadoop.sh fs -mkdir -p /home/poseidon/src/test
if [ $1 = "fs" ]; then
    echo "this is fs"
    if [ $2 = "-mkdir" ]; then
        echo "this is mkdir"
        if [ $3 == "-p" ]; then 
            echo "mkdir -p $4"
            mkdir -p $4
            exit $?
        else
            echo "mkdir $3"
            mkdir $3
            exit $?
        fi
    elif [ $2 = "-put" -o $2 = "-copyFromLocal" ]; then
        echo "this is put"
        if [ $3 == "-f" ]; then 
            echo "cp -f $4 $5"
            cp -f $4 $5
            exit $?
        else
            echo "cp $3 $4"
            cp $3 $4
            exit $?
        fi
    fi
fi
echo "not processed"
exit 1
