"""Rebuild sanitized Iceberg link table after explicitly rolling back pages."""
from search_engine.spark_common import create_spark, initialize_tables, rebuild_valid_links


def main():
    spark = create_spark("search-rebuild-links")
    try:
        initialize_tables(spark)
        print("Valid link edges:", rebuild_valid_links(spark))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
