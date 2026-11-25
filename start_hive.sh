#!/bin/bash
# Starts HiveServer2 (needs ./start_cluster.sh first). Beeline / scripts connect to jdbc:hive2://localhost:10000
cd "$(dirname "$0")" && source env.sh
if nc -z localhost 10000 2>/dev/null; then echo ">> HiveServer2 already running"; exit 0; fi
mkdir -p .hive && cd .hive
HADOOP_HEAPSIZE=1024 nohup hiveserver2 > hiveserver2.log 2>&1 &
echo $! > hiveserver2.pid
echo -n ">> Starting HiveServer2 "
for i in $(seq 1 60); do nc -z localhost 10000 2>/dev/null && { echo " ready on port 10000 (UI http://localhost:10002)"; exit 0; }; echo -n "."; sleep 3; done
echo " not up after 3 min - see .hive/hiveserver2.log"; exit 1
