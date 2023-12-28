#!/usr/bin/env bash

set -e

query=$1
sink=$2
data_path=hdfs:///user/hive/warehouse/benchmark_test

hdfs dfs -rm -r -f -skipTrash ${data_path}
hdfs dfs -mkdir -p ${data_path}

~/paimon-benchmark/bin/benchmark_restart.sh

sleep 10s

echo "start run test..."

~/paimon-benchmark/bin/run_benchmark.sh ${query} ${sink} &> ~/paimon-benchmark/log/${sink}.log

echo "finish run test..."
