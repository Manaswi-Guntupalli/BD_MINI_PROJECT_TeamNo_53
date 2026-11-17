#!/usr/bin/env bash
# VM3: run an Iceberg SQL query with this repository's actual Hadoop-catalog settings.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -ne 1 ]]; then echo 'Usage: bash scripts/ta_sql_node3.sh "SELECT ..."' >&2; exit 2; fi
: "${SPARK_MASTER:?Source .env and set SPARK_MASTER}"
: "${ICEBERG_WAREHOUSE:?Source .env and set ICEBERG_WAREHOUSE}"
: "${NODE3_HOST:?Source .env and set NODE3_HOST}"
CAT="${ICEBERG_CATALOG:-lake}"
RUNTIME="${ICEBERG_RUNTIME:-org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/usr/bin/python3.10}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-/usr/bin/python3.10}"
exec spark-sql --master "$SPARK_MASTER" \
  --packages "$RUNTIME" \
  --conf "spark.driver.host=$NODE3_HOST" \
  --conf 'spark.driver.bindAddress=0.0.0.0' \
  --conf 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
  --conf "spark.sql.catalog.${CAT}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${CAT}.type=hadoop" \
  --conf "spark.sql.catalog.${CAT}.warehouse=$ICEBERG_WAREHOUSE" \
  -e "$1"
