#!/bin/bash
export REDIS_HOME=/home/flink/redis
pdsh -R ssh -w k8s[01-04] $REDIS_HOME/src/redis-server $REDIS_HOME/redis.conf
