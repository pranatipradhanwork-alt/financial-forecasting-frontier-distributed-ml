#!/bin/bash
# Part 1b - Hive. Needs ./start_cluster.sh and ./start_hive.sh first.
cd "$(dirname "$0")/.." && source env.sh
# Hive gets its own copy of the CSV without the header line
# (LOAD DATA INPATH moves the file into the Hive warehouse)
hdfs dfs -mkdir -p /user/$USER/banking/staging 2>/dev/null
hdfs dfs -cat /user/$USER/banking/input/bank.csv 2>/dev/null | tail -n +2 \
  | hdfs dfs -put -f - /user/$USER/banking/staging/bank.csv 2>/dev/null
python 01_hadoop_hive/run_hive.py
