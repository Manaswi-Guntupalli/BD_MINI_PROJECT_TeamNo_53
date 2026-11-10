"""Mandatory HDFS phase ingestion job, submitted from Node 1."""
import argparse
import json
from pyspark.sql import functions as F
from search_engine.spark_common import (create_spark, initialize_tables, parse_batch,
                                        merge_documents)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="hdfs:///search/input/pages.jsonl etc.")
    p.add_argument("--phase", type=int, required=True, help="0 or 1; fallback revision for records missing version")
    p.add_argument("--rejects", default="hdfs:///search/rejects")
    a = p.parse_args()
    spark = create_spark(f"search-ingest-phase-{a.phase}")
    try:
        initialize_tables(spark)
        raw = spark.read.text(a.input).select(F.col("value"))
        good, bad = parse_batch(spark, raw, a.phase)
        invalid_count = bad.count()
        if invalid_count:
            bad.withColumn("phase", F.lit(a.phase)).write.mode("append").json(a.rejects)
        stats = merge_documents(spark, good)
        print("INGEST_SUMMARY=" + json.dumps({"phase": a.phase, "rejected": invalid_count, **stats}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
