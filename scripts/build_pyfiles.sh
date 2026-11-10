#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
from zipfile import ZipFile,ZIP_DEFLATED
from pathlib import Path
with ZipFile('search_engine.zip', 'w', ZIP_DEFLATED) as z:
    for p in Path('search_engine').rglob('*.py'):
        z.write(p)
print('Built search_engine.zip for Spark worker distribution')
PY
