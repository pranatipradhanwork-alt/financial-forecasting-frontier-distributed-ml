#!/bin/bash
# Part 1 - Hadoop: HDFS ingestion + 5 MapReduce jobs (Hadoop Streaming, Python)
# Run from anywhere after ./start_cluster.sh
cd "$(dirname "$0")" && source ../env.sh
PY=$(command -v python3)
JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)
MR=mapreduce

echo "================ DATA INGESTION ================"
hdfs dfs -mkdir -p /user/$USER/banking/input
hdfs dfs -D dfs.blocksize=131072 -put -f ../data/bank.csv /user/$USER/banking/input/
hdfs dfs -ls -h /user/$USER/banking/input
echo "-- blocks of bank.csv (128 KB blocks, so the file is split across blocks like real big data):"
hdfs fsck /user/$USER/banking/input/bank.csv -files -blocks 2>/dev/null | grep -E "bank.csv|blk_" | grep -v "^$" | head -6

run_job () {   # $1=name  $2=mapper  $3=reducer  $4=title
  echo; echo "================ $4 ================"
  hdfs dfs -rm -r -f -skipTrash /user/$USER/banking/output/$1 >/dev/null 2>&1
  hadoop jar $JAR \
    -D mapreduce.job.name="$4" \
    -D mapreduce.job.reduces=1 \
    -files $MR/$2,$MR/$3,$MR/common.py \
    -mapper "$PY $2" -reducer "$PY $3" \
    -input /user/$USER/banking/input/bank.csv \
    -output /user/$USER/banking/output/$1 2>&1 | grep -E "Running job|completed successfully|Launched map tasks|Map input records|Reduce output records|ERROR"
  echo "-- result: hdfs dfs -cat /user/$USER/banking/output/$1/part-00000"
  hdfs dfs -cat /user/$USER/banking/output/$1/part-00000
}

run_job job1_avg_balance_by_job   job1_mapper.py job1_reducer.py  "Job1: average balance per job  (job | clients | avg_balance)"
run_job job2_housing_by_education job2_mapper.py count_reducer.py "Job2: housing loan per education  (education | housing | clients)"
run_job job3_month_subscription   job3_mapper.py count_reducer.py "Job3: contacts per month and y  (month | y | clients)"
run_job job4_duration_by_poutcome job4_mapper.py job1_reducer.py  "Job4: avg contact duration per poutcome  (poutcome | clients | avg_seconds)"
run_job job5_age_vs_balance       job5_mapper.py job5_reducer.py  "Job5: age band vs balance  (band | clients | min | avg | median | max)"
