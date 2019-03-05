#!/bin/bash
kill `ps aux | grep redis-server | head -n 1 | awk '{print $2}'`
