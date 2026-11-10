#!/usr/bin/env bash
# Deliberate manual rollback to an already validated version.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="${SERVING_ROOT:-/srv/distributed-search}"
VERSION="${1:?usage: scripts/rollback_node4.sh v1}"
[[ "$VERSION" =~ ^[A-Za-z0-9_-]+$ ]] || exit 2
[ -f "$ROOT/versions/$VERSION/manifest.json" ] || { echo 'Version not found' >&2; exit 1; }
python3 - "$ROOT" "$VERSION" <<'PY'
import os, sys, uuid
from pathlib import Path
from search_engine.serving import SearchIndex
root, version = Path(sys.argv[1]), sys.argv[2]
SearchIndex.read_release(root / 'versions' / version)
tmp = root / ('.rollback-'+uuid.uuid4().hex)
try:
 os.symlink(str(Path('versions')/version), tmp)
 os.replace(tmp, root/'current')
finally:
 tmp.unlink(missing_ok=True)
print('Rolled back to', version)
PY
