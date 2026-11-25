#!/bin/bash
cd "$(dirname "$0")" && source env.sh
yarn --daemon stop nodemanager; yarn --daemon stop resourcemanager
hdfs --daemon stop datanode;   hdfs --daemon stop namenode
echo ">> Cluster stopped"
