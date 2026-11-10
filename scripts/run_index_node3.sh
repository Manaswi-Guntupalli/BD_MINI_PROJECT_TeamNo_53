#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -ne 1 ]; then echo 'usage: scripts/run_index_node3.sh v1' >&2; exit 2; fi
VERSION="$1"
[[ "$VERSION" =~ ^[A-Za-z0-9_-]+$ ]] || { echo 'Invalid version name' >&2; exit 2; }
export SPARK_DRIVER_HOST="${NODE3_HOST:?Set NODE3_HOST}"
: "${HDFS_URI:?Source .env}"
bash scripts/build_pyfiles.sh
bash scripts/submit.sh jobs/build_index.py --iterations 10 --damping 0.85
bash scripts/submit.sh jobs/export.py --version "$VERSION" --iterations 10 \
  --root "${HDFS_URI}/search/exports"
echo "Export written to ${HDFS_URI}/search/exports/${VERSION}"
