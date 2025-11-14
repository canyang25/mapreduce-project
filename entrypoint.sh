#!/bin/bash
set -e

# Build Hadoop classpath dynamically at runtime
export CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath --glob)"
echo "[INIT] CLASSPATH set."

# Launch HDFS DataNode and worker
hdfs datanode -D dfs.datanode.data.dir=/var/datanode -fs hdfs://boss:9000 &
sleep 3

exec python3 -u /app/worker.py
