#!/usr/bin/env bash

echo "stop..."
stop-cluster.sh
~/paimon-benchmark/bin/shutdown_cluster.sh

echo "clear remained TM..."
ssh core-1-1 "pids=\$(jps | grep TaskManagerRunner | awk '{print \$1}'); for pid in \$pids; do kill -9 \$pid; done"
ssh core-1-2 "pids=\$(jps | grep TaskManagerRunner | awk '{print \$1}'); for pid in \$pids; do kill -9 \$pid; done"
ssh core-1-3 "pids=\$(jps | grep TaskManagerRunner | awk '{print \$1}'); for pid in \$pids; do kill -9 \$pid; done"
ssh core-1-4 "pids=\$(jps | grep TaskManagerRunner | awk '{print \$1}'); for pid in \$pids; do kill -9 \$pid; done"

echo "restart..."
start-cluster.sh
~/paimon-benchmark/bin/setup_cluster.sh
