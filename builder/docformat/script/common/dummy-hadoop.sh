#!/bin/bash
exec 1>>logs/dummy-hadoop.log 2>&1
echo "$(date "+%Y/%m/%d %H:%M:%S") this a fake hadoop client for demo"
echo "params is: $*"
echo "now sleep for a while"
sleep 5
echo "now exit"
exit 0
