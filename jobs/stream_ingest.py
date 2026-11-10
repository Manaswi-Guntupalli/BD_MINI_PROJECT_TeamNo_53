"""Optional Kafka -> Spark Structured Streaming -> Iceberg event ingestion.

Run on Node 1. Checkpoint directory MUST be on HDFS. This is separate from
mandatory Phase 0/1 ingestion and must not run concurrently with those writes.
"""
import argparse
import json
from pyspark.sql import functions as F
from search_engine.spark_common import create_spark, initialize_tables, parse_batch, merge_documents


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic", default="document_updates")
    p.add_argument("--checkpoint", default="hdfs:///search/checkpoints/kafka-documents-v1")
    p.add_argument("--trigger-seconds", type=int, default=30)
    a = p.parse_args()
    if not a.checkpoint.startswith("hdfs://"):
        p.error("Kafka checkpoint location must be on HDFS")
    spark = create_spark("search-kafka-stream-ingest")
    initialize_tables(spark)

    def process_batch(df, batch_id):
        if df.isEmpty():
            return
        good, bad = parse_batch(spark, df.select("value"), phase=2)
        rejected = bad.count()
        if rejected:
            bad.withColumn("batch_id", F.lit(batch_id)).write.mode("append").json(
                "hdfs:///search/rejects/kafka")
        result = merge_documents(spark, good)
        print("KAFKA_BATCH=" + json.dumps({"batch_id": batch_id, "rejected": rejected, **result}))
        # Consumers should periodically run index/export after a committed batch.
        # Do not invoke nested Spark jobs simultaneously from a streaming callback.

    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", a.bootstrap)
              .option("subscribe", a.topic)
              .option("startingOffsets", "earliest")
              .load().selectExpr("CAST(value AS STRING) AS value"))
    query = (events.writeStream.foreachBatch(process_batch)
             .option("checkpointLocation", a.checkpoint)
             .trigger(processingTime=f"{a.trigger_seconds} seconds").start())
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
