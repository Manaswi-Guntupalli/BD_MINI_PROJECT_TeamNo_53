# Failure and correctness experiments — must run on the actual four VMs

**Do not report experiments as passed until performed and logged.** Record date, command, Spark UI screenshots, before/after metrics and whether results matched expectations.

## 1. Baseline Phase 0 / Phase 1

1. Ingest official Phase 0; record `pages.count`, valid edges, PageRank score sum, number of inverted-index postings.
2. Deploy `v1`; run queries in `/api/search`. Record doc IDs and BM25/PageRank scores.
3. Apply Phase 1; deploy `v2`. Compare changed documents / deleted documents / results.
4. Keep v1 deployment available. Run `scripts/rollback_node4.sh v1`, query again, then switch back to v2.
5. Re-ingest Phase 1; verify zero changes and the `pages` snapshot ID unchanged.

## 2. HDFS replication factor 2

```bash
hdfs getconf -confKey dfs.replication
hdfs dfsadmin -report
hdfs fsck /search -files -blocks -locations
```

The report must show **two live DataNodes** on Nodes 2 and 3 and (for files with sufficient replication) two replica locations. If Node 2 NameNode goes down, HDFS metadata service becomes unavailable; this setup has **no NameNode HA**. Do not claim otherwise.

## 3. Spark worker fault tolerance

1. Run the Spark indexing job while monitoring Node 3 Spark master UI and stage progress.
2. On Node 2, intentionally stop **only the Spark worker** (`$SPARK_HOME/sbin/stop-worker.sh`, or identify and kill a task executor). Avoid stopping NameNode/DataNode.
3. Confirm Spark master shows the worker lost; Node 3 worker remains running.
4. Restart Node 2 worker (`start-worker.sh spark://NODE3_IP:7077`).
5. Record whether the job recovers, retries, completes, or needs resubmission; verify final index counts and PageRank sum.

No unconditional guarantee of recovery: actual behavior depends on executor-loss timing and your task retry configuration.

## 4. Export safety and atomic release switch

1. With Node 4 serving v1, intentionally **do not** finish writing the new v2 export (e.g. kill the export Spark job before manifest creation).
2. Verify `/api/health` remains at v1; `deploy_from_node3.sh v2` must refuse the incomplete export.
3. Rerun as a **new version identifier**, e.g. `v2_retry`, rather than overwriting a partly written export.
4. After success, deploy v2_retry. Confirm `current` points to a new version and old version directory is preserved.

## 5. Iceberg snapshots and rollback

```sql
SELECT snapshot_id, committed_at, operation FROM lake.search.pages.snapshots ORDER BY committed_at DESC;
SELECT COUNT(*) FROM lake.search.pages VERSION AS OF <REAL_OLD_SNAPSHOT_ID>;
CALL lake.system.rollback_to_snapshot('search.pages', <REAL_OLD_SNAPSHOT_ID>);
```

After intentionally rolling back `pages`, rebuild `links`, then index/export/deploy a **new** search version. Record before/after results. Do not use the same number in the example verbatim: replace it with a real snapshot ID.

## 6. Kafka replay and update-order experiment (optional)

1. Start the Kafka broker and Spark Structured Streaming consumer.
2. Produce an event `doc_id=abc,version=5`. Produce it again. Verify only one final Iceberg document row with version 5.
3. Produce a stale event `doc_id=abc,version=4`; verify version 5 wins.
4. Stop the consumer after checkpointed processing; restart from **the same HDFS checkpoint**. Verify no duplicate current pages.
5. Stop the stream; rerun index/Parquet export and deploy a new version. Verify a query for the updated document.
6. Note that a single broker itself has no Kafka broker fault tolerance.

## Evidence table

| Experiment | Expected | Actual result | Proof files/screenshots |
|---|---|---|---|
| Baseline / Phase 0 | v1 valid and queryable | TODO | TODO |
| Phase 1 changes | v2 reflects updates | TODO | TODO |
| HDFS replication | 2 DataNodes, RF=2 | TODO | TODO |
| Spark worker restart | Job retries/completes or explains failure | TODO | TODO |
| Aborted export | Last-good version remains active | TODO | TODO |
| Iceberg rollback | Previous state visible | TODO | TODO |
| Kafka duplicate and stale versions | Same final state | TODO | TODO |
