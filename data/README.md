# Official dataset files (bundled locally, not automatically published)

The user provided two original project datasets, now included in the **downloadable archive**:

- `official/pages.jsonl` (Phase 0, 9.4 MB): **1,040** lines with **20 malformed JSON** lines; 1,020 valid records and 20 repeated documents → **1,000** distinct documents
- `official/changes.json` (Phase 1, 406 KB): **100** newline-delimited JSON events despite the `.json` filename; 50 DELETE and 50 UPSERT; upserts consist of 25 existing-document updates and 25 new documents

These files are ignored by Git by default to avoid automatically publishing a course-provided dataset and third-party Wikipedia article content to a public repository. The files are **present** when you extract the downloadable ZIP/TAR.GZ. Copy the same files to Node 1 for running phases. If your course explicitly allows redistribution, you can choose to track them in Git deliberately (`git add -f data/official/...`).

The `expected_results.json` file is also generated locally and gitignored. Run:

```bash
PYTHONPATH=. python scripts/verify_official_dataset.py \
  data/official/pages.jsonl data/official/changes.json --output data/expected_results.json
```

Do **not** use `json.load()` on the entire `changes.json` file; every non-empty line is its own JSON object.
