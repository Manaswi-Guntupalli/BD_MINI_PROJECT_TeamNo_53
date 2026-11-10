#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_DRIVER_HOST="${NODE1_HOST:?Set NODE1_HOST}"
: "${KAFKA_BOOTSTRAP:?Source .env}"
: "${HDFS_URI:?Source .env}"
bash scripts/build_pyfiles.sh
bash scripts/submit.sh jobs/stream_ingest.py --bootstrap "$KAFKA_BOOTSTRAP" \
  --checkpoint "${HDFS_URI}/search/checkpoints/kafka-documents-v1"
