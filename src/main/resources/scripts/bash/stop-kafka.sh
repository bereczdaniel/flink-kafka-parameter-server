#!/bin/bash
pdsh -R ssh -w k8s[02-04] ./kill-local-kafka-server.sh
sleep 5
pdsh -R ssh -w k8s[02-04] ./kill-local-zookeeper.sh
./kill-local-kafka-server.sh
sleep 5
./kill-local-zookeeper.sh

