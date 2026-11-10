"""Validated versioned Parquet export for Node 4, submitted on Node 3.

No serving version is marked ready until all data writes and consistency
checks succeed. A unique version identifier must be used for every export.
"""
import argparse
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from search_engine.spark_common import create_spark, table_names


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--version", required=True, help="e.g. v1, v2, v3 (unique)")
    p.add_argument("--root", default="hdfs:///search/exports")
    p.add_argument("--iterations", type=int, default=10)
    a = p.parse_args()
    if not a.version.replace("-", "").replace("_", "").isalnum():
        p.error("version must contain only letters, numbers, underscore or hyphen")
    spark = create_spark(f"search-export-{a.version}")
    try:
        pages_name, _ = table_names()
        namespace = pages_name.rsplit(".", 1)[0]
        source = spark.sql(
            f"SELECT snapshot_id FROM {pages_name}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        if not source:
            raise RuntimeError("No source snapshot")
        source_snapshot = str(source.snapshot_id)
        index = spark.table(f"{namespace}.inverted_index").cache()
        ranks = spark.table(f"{namespace}.pagerank").cache()
        idx_sources = [x[0] for x in index.select("source_snapshot_id").distinct().collect()]
        rank_sources = [x[0] for x in ranks.select("source_snapshot_id").distinct().collect()]
        if idx_sources != [source_snapshot] or rank_sources != [source_snapshot]:
            raise RuntimeError("Index/PageRank is stale or inconsistent with current pages snapshot")
        pages = spark.table(pages_name).where(~F.col("is_deleted"))
        documents = (pages.select("doc_id", "title", "text", "url", "version")
                     .join(ranks.select("doc_id", F.col("score").alias("pagerank")), "doc_id"))
        counts = index.select("doc_id", "doc_len").distinct()
        documents = documents.join(counts, "doc_id")
        n = pages.count()
        doc_count = documents.count()
        if n == 0 or doc_count != n or ranks.count() != n:
            raise RuntimeError(f"Expected {n} documents, exported {doc_count}")
        rank_sum = ranks.agg(F.sum("score").alias("total")).first().total
        if abs(rank_sum - 1.0) > 1e-4:
            raise RuntimeError("PageRank scores do not sum to 1")
        avgdl = documents.agg(F.avg("doc_len").alias("avgdl")).first().avgdl
        posting_count = index.count()
        if posting_count == 0 or not avgdl or avgdl <= 0:
            raise RuntimeError("Empty index or invalid average document length")
        orphan_count = (index.select("doc_id").distinct()
                        .join(documents.select("doc_id"), "doc_id", "left_anti").count())
        if orphan_count:
            raise RuntimeError("Index contains postings for missing documents")
        dest = f"{a.root.rstrip('/')}/{a.version}"
        # Error on existing version, never clobber a published release.
        documents.write.mode("errorifexists").parquet(f"{dest}/docs")
        index.select("term", "doc_id", "tf", "df").write.mode("errorifexists").parquet(f"{dest}/postings")
        manifest = {
            "format_version": 1, "version": a.version,
            "source_snapshot_id": source_snapshot,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "documents": n, "postings": posting_count,
            "avgdl": float(avgdl), "pagerank_sum": float(rank_sum),
            "bm25_k1": 1.2, "bm25_b": 0.75,
            "pagerank_iterations": a.iterations, "pagerank_damping": 0.85,
            "bm25_weight": 0.7, "pagerank_weight": 0.3
        }
        # Last step: Spark saveAsTextFile writes marker only after jobs succeeded.
        # Node 4 materializes manifest/part-... as manifest.json.
        spark.sparkContext.parallelize([json.dumps(manifest, sort_keys=True)], 1).saveAsTextFile(
            f"{dest}/manifest")
        print("EXPORT_READY=" + json.dumps({"path": dest, **manifest}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
