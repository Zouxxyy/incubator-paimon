# Steps to test

1. compile paimon-cluster-benchmark, and copy to all nodes

```shell
# cd paimon
mvn clean package -DskipTests -Drat.skip -Dcheckstyle.skip -pl paimon-benchmark/paimon-cluster-benchmark/ -am
scp -r paimon-benchmark/paimon-cluster-benchmark/target/paimon-benchmark-bin/paimon-benchmark/ root@master-1-1:/home/emr-user

# ssh master-1-1
# this benchmark need hadoop denpendency, here just borrow from EMR
find / -type f -name "flink-shaded-hadoop-2-uber-3.1.3-10.0.jar" -exec cp {} ~/paimon-benchmark/lib/ \;
sudo chown -R emr-user /home/emr-user/paimon-benchmark

# copy paimon-benchmark to all nodes
```

2. compile paimon and hudi, and copy to all nodes

```shell
# cd paimon
mvn clean package -DskipTests -Drat.skip -Dcheckstyle.skip -pl paimon-flink/paimon-flink-1.15 -am
scp paimon-flink/paimon-flink-1.15/target/paimon-flink-1.15-0.6-SNAPSHOT.jar root@master-1-1:/opt/apps/FLINK/flink-current/lib/

# cd hudi
mvn clean package -DskipTests -Dflink1.15
scp packaging/hudi-flink-bundle/target/hudi-flink1.15-bundle-0.14.0.jar root@master-1-1:/opt/apps/FLINK/flink-current/lib/

# copy package to all nodes
```

3. modify flink conf, and copy to all nodes

```shell
# ssh master-1-1
sudo vim ${FLINK_CONF_DIR}/masters
master-1-1:8081

sudo vim ${FLINK_CONF_DIR}/workers
core-1-1
core-1-1
core-1-1
core-1-1
core-1-2
core-1-2
core-1-2
core-1-2
core-1-3
core-1-3
core-1-3
core-1-3
core-1-4
core-1-4
core-1-4
core-1-4

sudo vim ${FLINK_CONF_DIR}/flink-conf.yaml
jobmanager.rpc.address: master-1-1
# other test configs, e.g.
parallelism.default: 16
jobmanager.memory.process.size: 4g
taskmanager.numberOfTaskSlots: 1
taskmanager.memory.process.size: 8g
execution.checkpointing.interval: 2min
execution.checkpointing.max-concurrent-checkpoints: 3
taskmanager.memory.managed.size: 1m
state.backend: rocksdb
state.backend.incremental: true
table.exec.sink.upsert-materialize: NONE

# copy config to all nodes

sudo chmod -R 777 /var/log/taihao-apps/flink/
```

4. start run test

```shell
# ssh master-1-1
su emr-user
# Flink web UI http://master-1-1:8081/#/overview

# run test, e.g.
cd ~/paimon-benchmark/bin
sh benchmark_test.sh upsert upsert_paimon
sh benchmark_test.sh upsert upsert_hudi
```
