#!/usr/bin/env bash
# Use on Node 1 for ingestion or Node 3 for indexing/export.
set -euo pipefail
if [ "$#" -lt 1 ]; then echo 'usage: scripts/submit.sh jobs/ingest.py [args]' >&2; exit 2; fi
: "${SPARK_MASTER:?Set SPARK_MASTER=spark://NODE3_IP:7077}"
: "${HDFS_URI:?Set HDFS_URI=hdfs://NODE2_IP:9000}"
export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-${HDFS_URI}/search/warehouse}"
export ICEBERG_CATALOG="${ICEBERG_CATALOG:-lake}"
export ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-search}"
ICEBERG_RUNTIME="${ICEBERG_RUNTIME:-org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1}"
KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4}"
# Spark 3.3.4 uses Python 3.10 (VMs may have Ubuntu 24.04's default Python 3.12).
# Configure the same interpreter on all Spark worker VMs, or override both variables.
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/usr/bin/python3.10}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-/usr/bin/python3.10}"
if [[ ! -x "$PYSPARK_PYTHON" || ! -x "$PYSPARK_DRIVER_PYTHON" ]]; then
  echo "Spark 3.3.4 requires working Python 3.10 executables on driver/workers; check PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON" >&2
  exit 1
fi
# Kafka is optional. Do NOT force Kafka's dependency resolution for mandatory
# phase ingestion, indexing, and exports; this removes an unnecessary failure mode.
PACKAGES="$ICEBERG_RUNTIME"
if [[ "$(basename "$1")" == "stream_ingest.py" ]]; then
  PACKAGES="${ICEBERG_RUNTIME},${KAFKA_PACKAGE}"
fi
# Jars must be resolvable by Spark, including remote worker processes.
EXTRA_ARGS=()
if [ -n "${SPARK_DRIVER_HOST:-}" ]; then
  EXTRA_ARGS+=(--conf "spark.driver.host=$SPARK_DRIVER_HOST" --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.port=34000")
fi
exec spark-submit \
  --master "$SPARK_MASTER" --deploy-mode client "${EXTRA_ARGS[@]}" \
  --packages "$PACKAGES" \
  --conf "spark.pyspark.python=$PYSPARK_PYTHON" \
  --conf "spark.pyspark.driver.python=$PYSPARK_DRIVER_PYTHON" \
  --conf "spark.executorEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON" \
  --conf "spark.executorEnv.ICEBERG_WAREHOUSE=$ICEBERG_WAREHOUSE" \
  --conf "spark.executorEnv.ICEBERG_CATALOG=$ICEBERG_CATALOG" \
  --conf "spark.executorEnv.ICEBERG_NAMESPACE=$ICEBERG_NAMESPACE" \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.task.maxFailures=4 \
  --py-files "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/search_engine.zip" \
  "$@"
