"""Node 3 distributed inverted-index and PageRank jobs (Spark workers Nodes 2+3)."""
import argparse
import json
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from search_engine.spark_common import create_spark, table_names


def pagerank(nodes: DataFrame, edges: DataFrame, iterations: int, damping: float) -> DataFrame:
    """PageRank with dangling-node mass redistribution (sum should remain 1)."""
    spark = nodes.sparkSession
    ids = nodes.select("doc_id").distinct().cache()
    n = ids.count()
    if not n:
        raise ValueError("No active documents to rank")
    valid_edges = (edges.select(F.col("source_id"), F.col("target_id"))
                   .join(ids.select(F.col("doc_id").alias("source_id")), "source_id")
                   .join(ids.select(F.col("doc_id").alias("target_id")), "target_id").distinct().cache())
    degrees = valid_edges.groupBy("source_id").agg(F.count("*").alias("degree")).cache()
    ranks = ids.withColumn("score", F.lit(1.0 / n)).cache()
    ranks.count()
    for iteration in range(iterations):
        old = ranks
        with_degrees = (old.join(degrees, old.doc_id == degrees.source_id, "left")
                        .select("doc_id", "score", F.coalesce("degree", F.lit(0)).alias("degree")))
        dangling = (with_degrees.where(F.col("degree") == 0)
                    .agg(F.coalesce(F.sum("score"), F.lit(0.)).alias("total")).first()["total"])
        contributions = (valid_edges.join(
            with_degrees.select(F.col("doc_id").alias("source_id"), "score", "degree"), "source_id")
            .select(F.col("target_id").alias("doc_id"),
                    (F.col("score") / F.col("degree")).alias("contribution"))
            .groupBy("doc_id").agg(F.sum("contribution").alias("inbound")))
        ranks = (ids.join(contributions, "doc_id", "left")
                 .select("doc_id", (F.lit((1.0 - damping) / n + damping * dangling / n)
                                    + F.lit(damping) * F.coalesce("inbound", F.lit(0.))).alias("score"))
                 .cache())
        ranks.count()
        old.unpersist()
        print(f"PageRank iteration {iteration + 1}/{iterations} complete")
    return ranks


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--iterations", type=int, default=10)
    p.add_argument("--damping", type=float, default=0.85)
    a = p.parse_args()
    if a.iterations < 10:
        p.error("Course project requires at least 10 PageRank iterations")
    if not 0 < a.damping < 1:
        p.error("damping must be between 0 and 1")
    spark = create_spark("search-index-and-pagerank")
    try:
        pages_name, links_name = table_names()
        namespace = pages_name.rsplit(".", 1)[0]
        snap = (spark.sql(f"SELECT snapshot_id FROM {pages_name}.snapshots ORDER BY committed_at DESC LIMIT 1")
                .first())
        if not snap:
            raise ValueError("Ingest Phase 0 first: pages table has no Iceberg snapshot")
        source_snapshot = str(snap.snapshot_id)
        pages = spark.table(pages_name).where(~F.col("is_deleted")).cache()
        # Title plus body, ASCII tokenizer shared with FastAPI.
        normalized = (pages.withColumn(
            "tokens", F.split(F.trim(F.regexp_replace(
                F.lower(F.concat_ws(" ", "title", "text")), "[^a-z0-9]+", " ")), " +"))
            .withColumn("doc_len", F.size("tokens")).cache())
        counts = (normalized.select("doc_id", "doc_len", F.explode("tokens").alias("term"))
                  .where(F.length("term") > 0)
                  .groupBy("doc_id", "doc_len", "term").agg(F.count("*").alias("tf")))
        df = counts.groupBy("term").agg(F.countDistinct("doc_id").alias("df"))
        postings = (counts.join(df, "term").select("term", "doc_id", "tf", "df", "doc_len")
                    .withColumn("source_snapshot_id", F.lit(source_snapshot)))
        nodes = pages.select("doc_id")
        ranks = pagerank(nodes, spark.table(links_name), a.iterations, a.damping)
        ranks = ranks.withColumn("source_snapshot_id", F.lit(source_snapshot))
        idx = f"{namespace}.inverted_index"
        pr = f"{namespace}.pagerank"
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {idx} (
            term STRING, doc_id STRING, tf BIGINT, df BIGINT,
            doc_len INT, source_snapshot_id STRING
        ) USING iceberg TBLPROPERTIES ('format-version'='2')""")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {pr} (
            doc_id STRING, score DOUBLE, source_snapshot_id STRING
        ) USING iceberg TBLPROPERTIES ('format-version'='2')""")
        postings.writeTo(idx).overwrite(F.lit(True))
        ranks.writeTo(pr).overwrite(F.lit(True))
        rank_total = ranks.agg(F.sum("score").alias("total")).first().total
        print("INDEX_SUMMARY=" + json.dumps({
            "source_snapshot_id": source_snapshot, "documents": pages.count(),
            "postings": postings.count(), "pagerank_sum": rank_total,
            "pagerank_iterations": a.iterations, "damping": a.damping}))
        if abs(rank_total - 1.0) > 1e-4:
            raise RuntimeError("PageRank mass conservation failed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
