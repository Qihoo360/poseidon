#!/bin/bash
count=$(ps axu | grep "java .* Reader [0-9]*$" | wc -l)
if [[ "$count" -ge 1 ]]; then
    exit
fi

echo "service has NOT been started"

sh hdfsReader.sh start
exit
