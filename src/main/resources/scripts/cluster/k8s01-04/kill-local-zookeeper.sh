#!/bin/bash
kill `ps aux | grep zookeeper\.properties | awk '{print $2}' | head -n 1`
