#!/bin/bash

# 用于将文件按照指定的文件名放入文件夹
# 文件名约定
# 必须包含日期例如：2016-10-10-08[-20]
# 必须以-分割，至少精确到小时，也可以精确到分钟
# 新文件必须按照字典序最大

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

# 这里的timestamp仅仅是为了使最新的文件按照字典序最大

cp -rf $1 /home/poseidon/data/log${timestamp}_$day-$hour.txt

echo file has copyed to /home/poseidon/data
echo wait for file in /home/poseidon/src/test/$day
echo detail log see logs/docformat.log

# echo and then you should go to run:
# echo /bin/bash  $pwd/bin/mock_start.sh $day
