#!/bin/bash
# Starts a single-node Hadoop cluster (HDFS + YARN) without needing SSH.
cd "$(dirname "$0")" && source env.sh
if [ ! -d .hdfs/name/current ]; then
  echo ">> First run: formatting the NameNode"
  hdfs namenode -format -force -nonInteractive >/dev/null 2>&1
fi
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
sleep 8
hdfs dfsadmin -safemode leave >/dev/null 2>&1
echo ">> Running Java daemons:"; jps | grep -v Jps
echo ">> HDFS UI: http://localhost:9870   YARN UI: http://localhost:8088"
