#!/bin/sh 
set -i
#######################################
#查询测试脚本，由于生成索引分词算法和策略，并不能保证每个测试key都能找出数据，可以从原始日志weibo_data.tar.gz找关键字查询测试，如果关键字是汉字需要urlencode
######################################


if [ $# -lt 1 ];
then
    echo usage: $0 token
    echo eg   : $0  good
    exit
fi

D=`date -d  '-1 day' +%Y-%m-%d`

host=127.0.0.1:39460
curl -XPOST "http://$host/service/proxy/mdsearch" -d "{\"query\":{\"page_size\":100,\"page_number\":0,\"days\":[\"$D\"],\"business\":\"test\",\"options\":{\"pv_only\":0,\"filter\":\"\"},\"keywords\":{\"text\":\"$1\"}}}"
