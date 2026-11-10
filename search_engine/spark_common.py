"""Shared Spark/Iceberg functions. Requires a configured Spark 3.5 cluster."""
from __future__ import annotations

import os
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from search_engine.records import parse_line

DOCUMENT_SCHEMA = T.StructType([
    T.StructField("doc_id", T.StringType(), False),
    T.StructField("version", T.LongType(), False),
    T.StructField("title", T.StringType(), False),
    T.StructField("text", T.StringType(), False),
    T.StructField("url", T.StringType(), False),
    T.StructField("raw_links", T.ArrayType(T.StringType()), False),
    T.StructField("is_deleted", T.BooleanType(), False),
    T.StructField("checksum", T.StringType(), False),
])
ERROR_SCHEMA = T.StructType([
    T.StructField("raw_line", T.StringType(), True),
    T.StructField("reason", T.StringType(), False),
])


def create_spark(name: str) -> SparkSession:
    """JARs must be supplied by spark-submit --packages (see scripts/submit.sh)."""
    catalog = os.getenv("ICEBERG_CATALOG", "lake")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "hdfs:///search/warehouse")
    return (SparkSession.builder.appName(name)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def table_names() -> tuple[str, str]:
    cat = os.getenv("ICEBERG_CATALOG", "lake")
    namespace = os.getenv("ICEBERG_NAMESPACE", "search")
    return f"{cat}.{namespace}.pages", f"{cat}.{namespace}.links"


def initialize_tables(spark: SparkSession) -> None:
    cat = os.getenv("ICEBERG_CATALOG", "lake")
    ns = os.getenv("ICEBERG_NAMESPACE", "search")
    pages, links = table_names()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{ns}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {pages} (
        doc_id STRING, version BIGINT, title STRING, text STRING, url STRING,
        raw_links ARRAY<STRING>, is_deleted BOOLEAN, checksum STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {links} (
        source_id STRING, target_id STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')""")


def parse_batch(spark: SparkSession, lines: DataFrame, phase: int) -> tuple[DataFrame, DataFrame]:
    parsed = lines.select("value").rdd.map(lambda x: parse_line(x.value, phase))
    good = spark.createDataFrame(parsed.filter(lambda x: x[0]).map(lambda x: x[1]), DOCUMENT_SCHEMA)
    bad = spark.createDataFrame(parsed.filter(lambda x: not x[0]).map(lambda x: x[1]), ERROR_SCHEMA)
    return good, bad


def deduplicate(documents: DataFrame) -> DataFrame:
    # Deterministic tie breaker; the same set of events always yields same winner.
    w = Window.partitionBy("doc_id").orderBy(F.desc("version"), F.desc("checksum"))
    return (documents.withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1).drop("_rn"))


def rebuild_valid_links(spark: SparkSession) -> int:
    """Atomically overwrite the derived Iceberg links table, retaining snapshots.

    Resolve link references via document IDs or page URLs; drop dangling edges.
    """
    pages, links = table_names()
    docs = spark.table(pages).where(~F.col("is_deleted"))
    refs = (docs.select(F.col("doc_id").alias("reference"), F.col("doc_id").alias("target_id"))
            .unionByName(docs.where(F.length("url") > 0).select(
                F.col("url").alias("reference"), F.col("doc_id").alias("target_id")))
            .groupBy("reference").agg(F.min("target_id").alias("target_id")))
    edges = (docs.select(F.col("doc_id").alias("source_id"), F.explode("raw_links").alias("reference"))
             .join(refs, "reference", "inner")
             .select("source_id", "target_id").distinct())
    count = edges.count()
    edges.writeTo(links).overwrite(F.lit(True))
    return count


def merge_documents(spark: SparkSession, changes: DataFrame) -> dict[str, int]:
    """Merge only real changes. Version is monotonic; checksum resolves ties.

    A failed duplicate/replay must not create new Iceberg table snapshots.
    """
    pages, _ = table_names()
    unique = deduplicate(changes).cache()
    before = spark.table(pages).select("doc_id", F.col("version").alias("old_version"),
                                       F.col("checksum").alias("old_checksum"))
    to_apply = (unique.join(before, "doc_id", "left")
                .where(F.col("old_version").isNull() |
                       (F.col("version") > F.col("old_version")) |
                       ((F.col("version") == F.col("old_version")) &
                        (F.col("checksum") > F.col("old_checksum"))))
                .select(*DOCUMENT_SCHEMA.fieldNames()).cache())
    count = to_apply.count()
    skipped = unique.count() - count
    if count:
        # temp view required by MERGE INTO; Spark extensions required.
        view = "_incoming_documents"
        to_apply.createOrReplaceTempView(view)
        spark.sql(f"""MERGE INTO {pages} target USING {view} source
          ON target.doc_id = source.doc_id
          WHEN MATCHED THEN UPDATE SET
            target.version = source.version, target.title = source.title,
            target.text = source.text, target.url = source.url,
            target.raw_links = source.raw_links, target.is_deleted = source.is_deleted,
            target.checksum = source.checksum
          WHEN NOT MATCHED THEN INSERT (
            doc_id,version,title,text,url,raw_links,is_deleted,checksum
          ) VALUES (
            source.doc_id,source.version,source.title,source.text,
            source.url,source.raw_links,source.is_deleted,source.checksum
          )""")
        edges = rebuild_valid_links(spark)
    else:
        edges = spark.table(table_names()[1]).count()
    to_apply.unpersist()
    unique.unpersist()
    return {"changed": count, "skipped": skipped, "valid_links": edges}
