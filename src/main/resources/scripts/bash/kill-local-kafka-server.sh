#!/bin/bash
kill `ps aux | grep kafka/config/server\.properties | awk '{print $2}' | head -n 1`
