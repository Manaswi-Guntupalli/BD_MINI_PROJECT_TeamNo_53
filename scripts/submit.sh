#!/usr/bin/env bash
# Use on Node 1 for ingestion or Node 3 for indexing/export.
set -euo pipefail
if [ "$#" -lt 1 ]; then echo 'usage: scripts/submit.sh jobs/ingest.py [args]' >&2; exit 2; fi
: "${SPARK_MASTER:?Set SPARK_MASTER=spark://NODE3_IP:7077}"
: "${HDFS_URI:?Set HDFS_URI=hdfs://NODE2_IP:9000}"
export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-${HDFS_URI}/search/warehouse}"
export ICEBERG_CATALOG="${ICEBERG_CATALOG:-lake}"
export ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-search}"
ICEBERG_RUNTIME="${ICEBERG_RUNTIME:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1}"
KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1}"
# Spark 3.5 / Scala 2.12 jars; ensure these align with installed Spark.
# Dependencies must also be reachable by Spark workers or downloaded to their caches.
EXTRA_ARGS=()
if [ -n "${SPARK_DRIVER_HOST:-}" ]; then
  EXTRA_ARGS+=(--conf "spark.driver.host=$SPARK_DRIVER_HOST" --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.port=34000")
fi
exec spark-submit \
  --master "$SPARK_MASTER" --deploy-mode client "${EXTRA_ARGS[@]}" \
  --packages "${ICEBERG_RUNTIME},${KAFKA_PACKAGE}" \
  --conf "spark.executorEnv.ICEBERG_WAREHOUSE=$ICEBERG_WAREHOUSE" \
  --conf "spark.executorEnv.ICEBERG_CATALOG=$ICEBERG_CATALOG" \
  --conf "spark.executorEnv.ICEBERG_NAMESPACE=$ICEBERG_NAMESPACE" \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.task.maxFailures=4 \
  --py-files "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/search_engine.zip" \
  "$@"
