#!/bin/bash

if [[ -z "$1" || ("$1" != "start" && "$1" != "stop") ]]; then
    echo "Usage: $0 <start|stop>"
    echo "       $0 start"
    exit
fi

BUSI="BUSI_DEFAULT"

start_cmd="bin/docformat -c etc/${BUSI}.json"
pid=$(ps axu | grep "${start_cmd}$" | awk '{print $2}')
if [[ -z "$pid" && "$1" == "stop" ]]; then
    echo "service has NOT been started"
    exit
elif [[ -n "$pid" && "$1" == "start" ]]; then
    echo "service has been started"
    ps axu | grep "${start_cmd}$"
    exit
fi

current=$(cd $(dirname $0) && pwd -P)
current=$(dirname $current)
echo $current

if [[ "$1" == "stop" || (-n "$pid" && "$1" == "restart") ]]; then
    kill $pid
    echo "service has start to stop, see ${current}/logs/docformat.log"
    sleep 0.1
    ps axu | grep "${start_cmd}$"
    if [[ "$1" == "stop" ]]; then
        exit
    fi
fi

rm -rf $current/logs
mkdir $current/logs

log_postfix=$(date "+%Y%m%d-%H%M%S")
if [[ "$1" == "start" ]]; then
    cd $current
    nohup $start_cmd &> logs/docformat.out.${log_postfix} &
    echo "service started"
    sleep 0.1
    ps axu | grep "${start_cmd}$"
fi
