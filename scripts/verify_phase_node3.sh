#!/usr/bin/env bash
# Dataset-specific Iceberg count and referential integrity verification after Phase 0 or 1.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -ne 1 ] || [[ ! "$1" =~ ^[01]$ ]]; then
  echo 'usage: bash scripts/verify_phase_node3.sh 0|1' >&2
  exit 2
fi
export SPARK_DRIVER_HOST="${NODE3_HOST:?Set NODE3_HOST}"
: "${HDFS_URI:?Source .env}"
bash scripts/build_pyfiles.sh
bash scripts/submit.sh jobs/verify_phase.py --phase "$1"
