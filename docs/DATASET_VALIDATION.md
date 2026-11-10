# Dataset validation and acceptance ground truth

**Source:** The two actual user-uploaded Phase 0 and Phase 1 files, analyzed locally using the source parser and an offline reference merge. These are not results of a running Spark/HDFS cluster.

| Measurement | Phase 0 | Phase 1 delta / post-state |
|---|---:|---:|
| Non-empty lines | 1,040 | 100 |
| Valid JSON lines | 1,020 | 100 |
| Malformed JSON lines to reject | 20 | 0 |
| Duplicate content records | 20 | 0 |
| Unique base document IDs | 1,000 | — |
| UPSERT | Base document load | 50 (25 existing, 25 new) |
| DELETE | — | 50 (all existing IDs) |
| Live documents | **1,000** | **975** |
| Tombstone rows | 0 | **50** |
| Total Iceberg `pages` rows | 1,000 | **1,025** |
| Live-page raw link references | 499 | 474 |
| Valid source-to-existing-target edges | **499** | **467** |
| Extra state changes on replay | **0** | **0** |

Notes:

- Both files are **JSON Lines**. Phase 1 is named `changes.json` but is **not** one JSON array. Do not parse it using ordinary whole-file JSON loading.
- Phase 0 rows contain `doc_id`, `url`, `title`, `text`, optional `links`. 40 valid Phase 0 records omit `links`; the parser defaults those to an empty list.
- Phase 1 DELETE records contain `doc_id` and `op` only; deletion tombstones must not be rejected for missing text/title.
- Phase 0 has 20 duplicate *identical* documents. Highest-version-wins deduplication also handles duplicate revisions in general.
- Both phases have **no explicit `version` field**, so the project assigns fallback `version=0` for Phase 0 and `version=1` for Phase 1 (the specified phase sequence is mandatory).
- Phase 1 updates are **50 deletions, 25 updates, 25 inserts**; hence 1,000 - 50 + 25 = 975 live docs.
- Links to deleted or missing docs must not remain in the resolved `links` Iceberg table. After Phase 1, 474 raw links from active docs produce 467 valid distinct resolved edges.
- If Phase 0 is rerun **after** Phase 1, the lower fallback version must **not** overwrite the newer state. Repeated Phase 1 merges should likewise make no changes.

## SHA-256 of the user-provided original files

```
pages.jsonl  245b1fdebac792d607578196c9eed86cf91ad4642832be623a12c0f19115bde9
changes.json 75c840fa497aaf14b7b7350071edcffd66cc6b2b9d1a31b12da8d60a943e66e5
```

## Verification commands

```bash
# Local / Node 1: verify content and produce expected report (no Spark required)
PYTHONPATH=. python scripts/verify_official_dataset.py \
  data/official/pages.jsonl data/official/changes.json --output data/expected_results.json

# Node 1, after cluster setup: mandatory Phase 0
bash scripts/run_phase_node1.sh 0 data/official/pages.jsonl
# Node 3: inspect actual Iceberg contents before indexing
bash scripts/verify_phase_node3.sh 0
bash scripts/run_index_node3.sh v1
# Node 4: deploy and inspect Phase 0 search results
bash scripts/deploy_from_node3.sh v1

# Node 1: mandatory Phase 1
bash scripts/run_phase_node1.sh 1 data/official/changes.json
# Node 3: inspect actual Iceberg contents before indexing
bash scripts/verify_phase_node3.sh 1
bash scripts/run_index_node3.sh v2
# Node 4: deploy and compare to v1
bash scripts/deploy_from_node3.sh v2
```

`verify_phase_node3.sh` checks the real Spark/Iceberg tables and exits non-zero unless the expected phase-specific live, tombstone, row and link counts are correct, with no duplicate IDs or orphan links. This still needs to be executed on the actual four VMs.

### Spark driver expected summaries

- First Phase 0 ingestion: 1,020 successfully parsed entries; 20 rejected; 20 duplicates collapsed; **1,000 changed documents**, 499 validated links.
- First Phase 1 ingestion: 100 valid; 0 rejected; **100 changed IDs**; 467 validated links.
- Replaying identical Phase 1 should result in **0 changed documents**; replaying Phase 0 after Phase 1 should not downgrade document versions.

### Known limitations

This is offline schema and state validation, not an integration test of Spark/Iceberg jars, HDFS replication, Kafka streaming, or Node 4 Parquet deployment. The course demo must measure and independently verify each of those on the VMs.
