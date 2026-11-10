# Four-person division (4–5 weeks, overlapping tasks)

| Person | Primary ownership | Shared integration responsibilities |
|---|---|---|
| Member 1 — infra | ZeroTier, Java/Hadoop/Spark setup, HDFS RF2, permissions, fault demos | Spark driver reachability, Spark UI and HDFS logs |
| Member 2 — ingestion | Official corpus schema, validation, Iceberg tables and snapshots, Kafka producer/consumer | Idempotence, updates, link integrity |
| Member 3 — search computation | Spark inverted index, PageRank, export manifest | Numeric checks, sources/snapshots, runtime |
| Member 4 — serving | BM25, FastAPI, UI, pulling/deploying/rolling back versioned exports | Query baselines, search UX, acceptance evidence |

**Order:** First shared VM setup and official schema inspection, then parallel development (2/3/4), mandatory Phase 0/1 end-to-end, failures, optional Kafka, final screenshots and report. Set a code freeze before the demo. Never call an untested live behavior complete.
