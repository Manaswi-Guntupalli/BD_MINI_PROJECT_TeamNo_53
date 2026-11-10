#!/usr/bin/env bash
# Run locally ONLY after creating an EMPTY GitHub repository in the browser.
set -euo pipefail
cd "$(dirname "$0")/.."
REMOTE="https://github.com/Manaswi-Guntupalli/distributed-search-engine-kafka.git"
if ! git config user.name >/dev/null || ! git config user.email >/dev/null; then
  echo 'First configure YOUR git user.name and user.email (see docs/GITHUB_PUBLISH.md).' >&2
  exit 1
fi
if [ ! -d .git ]; then git init; fi
git add .
if ! git diff --cached --quiet; then git commit -m "Initial distributed search engine with Kafka integration"; fi
git branch -M main
if ! git remote get-url origin >/dev/null 2>&1; then git remote add origin "$REMOTE"; fi
[ "$(git remote get-url origin)" = "$REMOTE" ] || { echo 'Existing origin points elsewhere; refusing to push' >&2; exit 1; }
git push -u origin main
