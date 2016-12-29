#!/bin/bash

if [ $# -lt 1 ];
then
    echo $0 logfile
    echo usage: $0 ./weibo.txt
    exit
fi

pwd=$(pwd)

day=`date -d "24 hours ago" +"%Y-%m-%d"`
hour=`date -d "24 hours ago" +"%H"`
timestamp=`date -d "24 hours ago" +"%Y%m%d%H%M%S"`

cp -rf $1 /home/poseidon/data/log${timestamp}_$day-$hour.txt

echo wait for file in /home/poseidon/src/test/$day
echo and then you should run:
echo /bin/bash  $pwd/bin/mock_start.sh $day

