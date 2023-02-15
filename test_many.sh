#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT
runs=$1

for i in $(seq 1 $runs); do
    res=`make project1`
    echo '**************************************'
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
    echo '***' PASSED TESTS IN TRIAL $i
done
echo '***' PASSED ALL $i TESTING TRIALS