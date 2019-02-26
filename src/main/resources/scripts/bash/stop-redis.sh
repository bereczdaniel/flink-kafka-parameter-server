#!/bin/bash
pdsh -R ssh -w k8s[02-04] ./kill-local-redis-server.sh
./kill-local-redis-server.sh
