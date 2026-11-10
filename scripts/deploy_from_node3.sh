#!/usr/bin/env bash
# Run on Node 4. Requires SSH/rsync access to Node 3 and Python venv with pyarrow.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -ne 1 ]; then echo 'usage: scripts/deploy_from_node3.sh v1' >&2; exit 2; fi
VERSION="$1"
[[ "$VERSION" =~ ^[A-Za-z0-9_-]+$ ]] || { echo 'Invalid version name' >&2; exit 2; }
: "${NODE3_HOST:?Set NODE3_HOST}"; : "${HDFS_URI:?Set HDFS_URI}";
ROOT="${SERVING_ROOT:-/srv/distributed-search}"
mkdir -p "$ROOT/incoming"
[ ! -e "$ROOT/incoming/$VERSION" ] || { echo 'Incoming version already exists' >&2; exit 1; }
# Copy from HDFS to a temporary directory on Node 3 (no HDFS daemon on Node 4).
ssh "$NODE3_HOST" "mkdir -p /tmp/search-exports && rm -rf /tmp/search-exports/$VERSION && hdfs dfs -test -e /search/exports/$VERSION/manifest/_SUCCESS && hdfs dfs -get -f /search/exports/$VERSION /tmp/search-exports/"
mkdir -p "$ROOT/incoming/$VERSION"
rsync -a "${NODE3_HOST}:/tmp/search-exports/${VERSION}/" "$ROOT/incoming/$VERSION/"
cat "$ROOT/incoming/$VERSION/manifest"/part-* > "$ROOT/incoming/$VERSION/manifest.json"
python3 -m search_engine.deploy --root "$ROOT" --version "$VERSION"
curl -fsS http://127.0.0.1:8000/api/health || true
echo "Search release $VERSION deployed. Old versions retained for rollback."
