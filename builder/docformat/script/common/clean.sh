#!/bin/bash
current=$(cd $(dirname $0) && pwd -P)
current=$(dirname $current)

cd $current && sh bin/install.sh stop

rm -fv /home/poseidon/data/*
rm -fv $current/logs/last*
