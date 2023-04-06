#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT
runs=$1

for i in $(seq 1 $runs); do
    echo '**************************************'
    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestBasicConfChange3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeRecover3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeRecoverManyClients3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeUnreliable3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecover3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    res=`go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ || true`
    echo $res
    if [[ $res == *"FAIL"* ]]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
    echo '***' PASSED TESTS IN TRIAL $i
done
echo '***' PASSED ALL $i TESTING TRIALS