#!/usr/bin/env bash
set -euo pipefail
: "${NODE2_HOST:?Set NODE2_HOST}"; : "${NODE3_HOST:?Set NODE3_HOST}";
echo '=== HDFS files / replication ==='
hdfs dfsadmin -report | head -n 60
hdfs getconf -confKey dfs.replication
hdfs dfs -ls /search || true
echo '=== Node 2 daemons ==='
ssh "$NODE2_HOST" jps
echo '=== Node 3 daemons ==='
ssh "$NODE3_HOST" jps
echo '=== Spark master ==='
curl -fsS "http://${NODE3_HOST}:8080/" >/dev/null && echo 'Spark master UI reachable'
