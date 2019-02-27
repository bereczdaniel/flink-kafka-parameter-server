#!/bin/bash
export KAFKA_HOME=/home/flink/kafka
pdsh -R ssh -w k8s[01-04] $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
sleep 5
pdsh -R ssh -w k8s[01-04] $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
