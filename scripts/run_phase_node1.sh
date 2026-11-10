#!/usr/bin/env bash
# Use one phase at a time. Node3 indexing and Node4 deployment intentionally separate.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -ne 2 ]; then echo 'usage: scripts/run_phase_node1.sh 0 /path/pages.jsonl (or 1 changes.json)' >&2; exit 2; fi
PHASE="$1"; INPUT="$2"
[[ "$PHASE" == 0 || "$PHASE" == 1 ]] || { echo 'Phase must be 0 or 1' >&2; exit 2; }
[ -f "$INPUT" ] || { echo "Missing local file: $INPUT" >&2; exit 1; }
export SPARK_DRIVER_HOST="${NODE1_HOST:?Set NODE1_HOST}"
: "${HDFS_URI:?Source .env on this node}"
NAME="$(basename "$INPUT")"
hdfs dfs -mkdir -p /search/input
hdfs dfs -put -f "$INPUT" "/search/input/$NAME"
hdfs dfs -ls "/search/input/$NAME"
bash scripts/build_pyfiles.sh
bash scripts/submit.sh jobs/ingest.py --input "${HDFS_URI}/search/input/${NAME}" --phase "$PHASE" \
  --rejects "${HDFS_URI}/search/rejects/phase${PHASE}"
printf '\nPhase %s ingested. Now run Node 3 indexing/export and then Node 4 deploy.\n' "$PHASE"
