#!/bin/bash

current=$(cd $(dirname $0) && pwd -P)
echo $current
root=$(cd ${current}/.. && pwd)
echo $root

CLASSPATH=""
export CLASSPATH="$root/etc":"$root/lib/*":${CLASSPATH}

if [[ -z "$1" || ("$1" != "start" && "$1" != "stop" && "$1" != "restart") ]]; then
    echo "Usage: $0 <start|stop|restart>"
    echo "       $0 start"
    exit
fi

port=39997
start_cmd="java -Xmx16000m Reader $port"
pid=$(ps axu | grep "Reader $port$" | awk '{print $2}')
if [[ -z "$pid" && "$1" == "stop" ]]; then
    echo "service has NOT been started"
    exit
elif [[ -n "$pid" && "$1" == "start" ]]; then
    echo "service has been started"
    ps axu | grep "Reader $port$"
    exit
fi

if [[ "$1" == "stop" || (-n "$pid" && "$1" == "restart") ]]; then
    kill -9 $pid
    echo "service has been killed"
    sleep 0.1
    ps axu | grep "Reader $port$"
    if [[ "$1" == "stop" ]]; then
        exit
    fi
fi

mkdir -p $root/logs

log_postfix=$(date "+%Y%m%d-%H%M%S")
if [[ "$1" == "start" || "$1" == "restart" ]]; then
    cd $root
    nohup $start_cmd &> logs/reader.out.${log_postfix} &
    echo "service started"
    sleep 0.1
    ps axu | grep "Reader $port$"
fi
exit
