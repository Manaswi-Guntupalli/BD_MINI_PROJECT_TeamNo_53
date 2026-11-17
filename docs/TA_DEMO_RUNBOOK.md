# TA Project 1 Verification — exact commands for Team 53

**Source:** TA's Project 1 Verification Runbook supplied in chat, adapted to the code in this repository. **This is a verification PLAN, not evidence that a live cluster passed.**

## 0. Before starting: confirm versions, nodes and shell settings

The team uses Java 11, Hadoop 3.3.6, **Spark 3.3.4 / Scala 2.12**, Python 3.10 for Spark jobs, and Apache Iceberg runtime `org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1`. Node 1's `.env` is a LOCAL file, not in Git. Confirm all 4 VM IPs are actual ZeroTier host addresses. **192.168.196.255 is a /24 broadcast address, not a valid VM4 address** if you are still using the previously shown /24 network.

On VMs 1 and 3:

```bash
cd ~/BD_MINI_PROJECT_TeamNo_53
set -a; source .env; set +a
export PYSPARK_PYTHON=/usr/bin/python3.10
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
```

Ensure Spark executors on VMs 2 and 3 use a Python 3.10 interpreter at the indicated path. Do not run Hadoop/Spark cluster daemons on VM1. Ensure ZeroTier IPs for HDFS NameNode (VM2) and Spark master (VM3) are reachable. Get the course-provided official files separately onto VM1 at `data/official/pages.jsonl` and `data/official/changes.json`.

**Important naming differences from TA example:**

| TA examples | This repo |
|---|---|
| `iceberg_catalog.lake.pages` | `lake.search.pages` |
| `iceberg_catalog.lake.inverted_index` | `lake.search.inverted_index` |
| `iceberg_catalog.lake.pagerank` | `lake.search.pagerank` |
| PageRank `rank` | PageRank `score` |
| Phase 0 `/serving/v0/`, Phase 1 `/serving/v1/` | Run `scripts/run_ta_export_node3.sh v0` and `v1` after the primary exports; these produce the exact TA locations |
| `/serving/vN/docs.parquet`, `index.parquet` | Provided by the additive TA compatibility export; the main serving pipeline still uses `docs/part-*.parquet` and `postings/part-*.parquet` under `/search/exports/vN` |
| `/serving/v1/_READY` | Written by the additional TA compatibility export only after both files exist; the primary export also has `/search/exports/v1/manifest/_SUCCESS` |
| `/search` and `/health` | Both aliases added; existing `/api/search` and `/api/health` continue to work |

**The added compatibility job creates exact single-file names and the `_READY` marker without changing the real Node 4 deployment flow. It has not yet been executed on the team VMs and must be tested before demo.**

The official TA example selects `committed_at` from `pages.history`, but Iceberg `.history` has `made_current_at`. The `committed_at` column exists in `.snapshots`. Use the corrected SQL below.


### IMPORTANT VM address correction (must fix before any serving demo)

- VM1: `192.168.196.151` (confirmed on ZeroTier)
- VM2: `192.168.196.156` (reported; verify on VM2)
- VM3: `192.168.196.7` (reported; verify on VM3)
- VM4: **`192.168.196.255` is invalid for your previously shown `/24` subnet** — it is the broadcast address. VM4 must read its real address using `sudo zerotier-cli listnetworks` and `ip -4 addr` and replace `NODE4_HOST` in `.env` on every VM.

Run `bash scripts/ta_preflight_node1.sh` on VM1 before submitting the first job. It checks the subnet/broadcast problem and local inputs without formatting/altering the cluster.

For the Node4 export hand-off, VM4 should set `NODE3_SSH_TARGET=USER_ON_VM3@192.168.196.7` if the VM3 Linux username differs from the one on VM4. This avoids SSH defaulting to VM4's local username.

The script `scripts/submit.sh` now pins Spark's Python interpreter to Python 3.10 for the driver and remote executors, and includes the Kafka connector only for the optional streaming ingestion job. This reduces failure modes during the mandatory demonstration.

## 1. Mandatory Phase 0: deploy `v0`

VM1:

```bash
# Show both raw files, validate, and ingest once
PYTHONPATH=. python3 scripts/check_input.py data/official/pages.jsonl --phase 0
bash scripts/run_phase_node1.sh 0 data/official/pages.jsonl
hdfs dfs -ls /search/input/pages.jsonl
```

VM2 (ask VM2 member to record evidence):

```bash
hdfs dfsadmin -report           # 2 live DataNodes (VM2 and VM3)
hdfs getconf -confKey dfs.replication  # must be 2
hdfs fsck /search/input/pages.jsonl -files -blocks -locations
```

NameNode UI: `http://VM2_ZEROTIER_IP:9870`, Utilities → Browse the filesystem → `/search/input/pages.jsonl`; verify a positive file size and HEALTHY with block locations. A healthy file alone does not prove **every** block has 2 replicas; inspect the fsck output and replication counters.

VM3:

```bash
set -a; source .env; set +a
bash scripts/verify_phase_node3.sh 0
bash scripts/run_index_node3.sh v0
bash scripts/run_ta_export_node3.sh v0    # additional strict TA layout
bash scripts/ta_sql_node3.sh "SELECT doc_id, title FROM lake.search.pages WHERE doc_id = '94'"
bash scripts/ta_sql_node3.sh "SELECT doc_id, score AS rank FROM lake.search.pagerank WHERE doc_id = '146'"
bash scripts/ta_sql_node3.sh "SELECT COUNT(*) AS docs FROM lake.search.pages WHERE NOT is_deleted"
bash scripts/ta_sql_node3.sh "SELECT snapshot_id, committed_at FROM lake.search.pages.snapshots ORDER BY committed_at"
hdfs dfs -ls /search/exports/v0/docs
hdfs dfs -ls /search/exports/v0/postings
hdfs dfs -test -e /search/exports/v0/manifest/_SUCCESS && echo 'v0 export ready'
hdfs dfs -ls /serving/v0/docs.parquet /serving/v0/index.parquet
hdfs dfs -test -e /serving/v0/_READY && echo 'v0 TA-ready'
```

VM4 (once v0 is exported and local SERVING_ROOT is writable):

```bash
set -a; source .env; set +a
# Start the API in a separate terminal (venv activated; dependencies installed)
uvicorn search_engine.api:app --host 0.0.0.0 --port 8000
# In another terminal:
bash scripts/deploy_from_node3.sh v0
curl -fsS 'http://localhost:8000/health'
curl -fsS 'http://localhost:8000/search?q=beard&limit=5' | python3 -m json.tool
```

Expect title `Beard` for `doc_id=94`; note doc 146's PageRank value and a baseline search result. The `/search` results are filtered to query matches; if doc 94 is not in the first 5 results, query the Iceberg table directly. Keep v0 running and *do not* re-ingest Phase 0 concurrently with Phase 1.

## 2. Phase 1 live demo: ingest, verify, index, export v1

VM1:

```bash
bash scripts/run_phase_node1.sh 1 data/official/changes.json
```

VM3 (wait until VM1 ingestion succeeds):

```bash
bash scripts/verify_phase_node3.sh 1    # 1025 total rows, 975 live, 50 deleted, 467 links
bash scripts/ta_sql_node3.sh "SELECT snapshot_id, committed_at FROM lake.search.pages.snapshots ORDER BY committed_at"
bash scripts/ta_sql_node3.sh "SELECT doc_id, title FROM lake.search.pages WHERE doc_id='94'"
bash scripts/run_index_node3.sh v1
bash scripts/run_ta_export_node3.sh v1    # additional strict TA layout
bash scripts/ta_sql_node3.sh "SELECT term, doc_id FROM lake.search.inverted_index WHERE doc_id = 'phase1-new-001' LIMIT 5"
bash scripts/ta_sql_node3.sh "SELECT COUNT(*) AS deleted_doc_ranks FROM lake.search.pagerank WHERE doc_id = '1'"
bash scripts/ta_sql_node3.sh "SELECT doc_id, score AS rank FROM lake.search.pagerank WHERE doc_id = '146'"
hdfs dfs -ls /search/exports/v0/docs /search/exports/v1/docs
hdfs dfs -ls /search/exports/v0/postings /search/exports/v1/postings
hdfs dfs -test -e /search/exports/v1/manifest/_SUCCESS && echo 'v1 export ready'
hdfs dfs -ls /serving/v0/docs.parquet /serving/v0/index.parquet
hdfs dfs -ls /serving/v1/docs.parquet /serving/v1/index.parquet
hdfs dfs -test -e /serving/v1/_READY && echo 'v1 TA-ready'
```

VM4 (API already running, no restart required):

```bash
bash scripts/deploy_from_node3.sh v1
curl -fsS 'http://localhost:8000/health'    # expected status ok, version 1
curl -fsS 'http://localhost:8000/search?q=beard&limit=5' | python3 -m json.tool
curl -fsS 'http://localhost:8000/search?q=cooking&limit=5' | python3 -m json.tool
curl -fsS 'http://localhost:8000/search?q=plant+growth&limit=5' | python3 -m json.tool
```

Verify `doc_id 94` has title **`Beard (Phase 1)`**, `phase1-new-001` has indexed terms, and `doc_id 1` has **zero** PageRank rows. The app's result payload includes a `results` array and `index_version`; the compatibility `/health` route adds the TA's `status` and numeric `version`.

### TA PageRank discrepancy — doc 146

**Do not falsify the rank.** The supplied data really adds incoming links to 146 from doc `98` and `phase1-new-011`. But the repo's 10-iteration, 0.85-damping, dangling-node-corrected PageRank calculation, verified independently against the raw source graph, produces these *offline reference predictions*:

- Phase 0: **0.0017517249423**
- Phase 1: **0.0013945573064**
- The result also remains lower after 100 iterations.

Adding incoming links does not mathematically guarantee a higher PageRank when the whole graph has changed. Run the real Spark job and record its actual scores; if they differ, inspect the graph construction and algorithm. Show the TA both raw links and scores and explain the discrepancy politely. **This is not proof of a failure in the implementation by itself.**

## 3. Idempotency (run on correct phase)

VM3 (baseline):

```bash
bash scripts/ta_sql_node3.sh "SELECT COUNT(*) AS total_rows FROM lake.search.pages"
bash scripts/ta_sql_node3.sh "SELECT snapshot_id, committed_at FROM lake.search.pages.snapshots ORDER BY committed_at DESC LIMIT 1"
```

VM1: rerun Phase 1 **with the same file**:

```bash
bash scripts/run_phase_node1.sh 1 data/official/changes.json
```

VM3: repeat both queries. Expect unchanged row count **1025**, unchanged live count **975**, and an unchanged **pages snapshot ID** if replay generated no changes. Our merge code suppresses identical versions and hashes, rather than performing unnecessary writes. Also check its `INGEST_SUMMARY` log reports `changed: 0`. If an exception occurs, this remains unverified until fixed.

## 4. Failure / snapshot / Kafka evidence still required

- Actual HDFS report and fsck output on VMs 2/3; Spark UI two workers; Spark worker crash/restart recovery.
- Iceberg snapshot history/time travel to Phase 0 before applying Phase 1; if rolling back pages, rebuild sanitized links and index before publishing a new release.
- A partial failed export must **not** activate a new Node 4 release; the old `current` should still point to `v0`.
- Kafka is optional and should be demonstrated only after mandatory Phase 0/1 passes. Node 1 broker in KRaft mode; stream checkpoints in HDFS; replay duplicate, stale version, and recover from the same checkpoint.
- Save timestamps, CLI outputs, Spark UI and HDFS UI screenshots, BM25/PageRank scores, and results before/after Phase 1.

## Known deployment blockers to check (not yet tested here)

1. **Python:** Spark 3.3.4 must run with Python 3.10 on drivers and workers, not the Python 3.12 unit-test venv.
2. **Firewall / network:** VM1 Spark driver `34000` and Spark executor↔driver networking must be reachable via ZeroTier. NameNode `9000`, HDFS `9870`, Spark master `7077`, Spark UI `8080`, and SSH must be reachable as relevant.
3. **Configuration:** All nodes must use compatible Iceberg/Hadoop settings and the same warehouse path. Use existing repository scripts on the assigned VMs only.
4. **VM4's ZeroTier IP:** A previous screenshot showed `192.168.196.255`, which is the broadcast address of a `/24` network. Replace with VM4's assigned host address.
5. **No live verification:** These commands are adapted from source inspection, not validated by running against your team VMs.
