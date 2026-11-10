# Final acceptance checklist — course deliverables

## Installation / topology

- [ ] Ubuntu 24.04 / Java 11 / Hadoop 3.3.6 installed using course guides
- [ ] Four peers reachable over ZeroTier, SSH access verified
- [ ] Node 1 runs only client ingestion Spark driver, no Spark/HDFS cluster daemon
- [ ] Node 2 NameNode + DataNode + Spark worker
- [ ] Node 3 Spark master + worker + HDFS DataNode
- [ ] HDFS config replication factor **2**, two live DataNodes verified
- [ ] Spark UI shows two registered workers
- [ ] Node 4 has FastAPI + UI; no HDFS/Spark daemons

## Correctness

- [x] Uploaded official `pages.jsonl` and `changes.json` schema inspected and mapped offline (see DATASET_VALIDATION.md)
- [ ] Both files uploaded to HDFS from Node 1, separately in order (actual dataset included in archive)
- [ ] On VMs verify **20 malformed Phase 0** records rejected and **20 duplicate Phase 0** records removed; accept Phase 1 tombstones
- [ ] Highest-version deduplication tested (including repeated Phase 1)
- [ ] On VMs: Phase 0 **1000 live / 499 links**, Phase 1 **975 live / 50 deleted / 467 links**; no orphan edges
- [ ] Iceberg `pages` and `links` snapshots inspected
- [ ] Iceberg time travel and recovery demonstrated
- [ ] Distributed index contains correct term frequencies and document frequencies
- [ ] PageRank completed **10 iterations**, damping **0.85**; scores sum ≈ 1
- [ ] Search BM25 uses k1 **1.2**, b **0.75**
- [ ] Combined ranking uses **0.7 × normalized BM25 + 0.3 × normalized PageRank**
- [ ] Versioned Parquet exported from Node 3 and copied to Node 4
- [ ] Node 4 returns doc ID/URL, meaningful ranking and version
- [ ] V1 results before changes and V2 results after changes recorded
- [ ] Bad/incomplete export does not replace last-good index
- [ ] Actual Spark fault experiment performed and logs/screenshots saved

## Kafka enhancement (optional)

- [ ] Single-node Kafka KRaft broker on Node 1 created and secured on ZeroTier
- [ ] `document_updates` topic created
- [ ] Producer, consumer and HDFS checkpoint validated
- [ ] Duplicate and out-of-order versions tested against Iceberg
- [ ] Stream stopped before reindex/export; v3 deployed
- [ ] Kafka is not presented as a mandatory course feature or as broker-level HA

## Presentation / team deliverables

- [ ] README includes actual VM IP setup without committing private secrets
- [ ] Final course corpus (not toy fixture) used for assessed demonstration
- [ ] Screenshots of HDFS/Spark/Iceberg/jobs/API
- [ ] Documented experiment evidence with measured time/query performance
- [ ] Each of 4 team members can explain their assigned components and dependencies
