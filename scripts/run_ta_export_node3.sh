#!/usr/bin/env bash
# VM3 only. Publish literal TA-required /serving/vN/{docs.parquet,index.parquet,_READY}.
# Does NOT replace the original /search/exports/vN used by Node4.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -ne 1 || ! "$1" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo 'Usage: bash scripts/run_ta_export_node3.sh v0|v1' >&2
  exit 2
fi
: "${HDFS_URI:?Source .env on VM3 first}"
export SPARK_DRIVER_HOST="${NODE3_HOST:?Set NODE3_HOST}"
bash scripts/build_pyfiles.sh
bash scripts/submit.sh jobs/ta_export_compat.py \
  --version "$1" --export-root "${HDFS_URI}/search/exports" \
  --serving-root "${HDFS_URI}/serving"
hdfs dfs -ls "/serving/$1/"
hdfs dfs -test -e "/serving/$1/_READY" && echo "$1 TA serving export ready"
