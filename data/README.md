# Official Project 1 datasets

The official files used by this repository are:

- `official/pages.jsonl` — Phase 0 input; 1,040 lines, including 20 intentionally malformed lines. The ingestion job expects 1,000 distinct active pages after validation and deduplication.
- `official/changes.json` — Phase 1 input; 100 newline-delimited JSON events (50 DELETE, 50 UPSERT). Despite the `.json` extension, this is **JSON Lines**, not a single JSON array.

The repository is configured to track these exact two file paths. The files were supplied separately and **still need to be committed from a computer with the original dataset files** (for example VM1). Do not claim they are published until both show up on the GitHub main branch.

On VM1, after updating the repository:

```bash
cd ~/BD_MINI_PROJECT_TeamNo_53
git pull --ff-only origin main
ls -lh data/official/pages.jsonl data/official/changes.json
git add data/official/pages.jsonl data/official/changes.json
git diff --cached --stat
git commit -m "Add official Project 1 Phase 0 and Phase 1 datasets"
git push origin main
```

Before committing, confirm the course allows redistribution of these datasets in the repository, especially if public. Never commit `.env`, credentials, logs, or generated `data/expected_results.json`.

Check data inputs locally:

```bash
PYTHONPATH=. python3 scripts/check_input.py data/official/pages.jsonl --phase 0
PYTHONPATH=. python3 scripts/check_input.py data/official/changes.json --phase 1
```

Generate a private reference summary:

```bash
PYTHONPATH=. python3 scripts/verify_official_dataset.py \
  data/official/pages.jsonl data/official/changes.json --output data/expected_results.json
```

`data/expected_results.json` stays Git-ignored.
