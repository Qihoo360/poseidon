#!/bin/bash
# exec 1>>logs/real-hadoop.log 2>&1
echo "$(date "+%Y/%m/%d %H:%M:%S") this a proxy of hadoop client for demo"
echo "params is: $*"
/usr/local/hadoop/bin/hadoop $*
