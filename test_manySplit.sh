#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT
runs=$1

echo `date`

for i in $(seq 1 $runs); do
    echo '**************************************'
    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    else
        echo '***' PASSED TESTS IN TRIAL $i
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS