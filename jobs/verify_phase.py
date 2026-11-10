"""Dataset-specific Spark/Iceberg acceptance checks for the uploaded corpus.

Run on Node 3 AFTER the relevant mandatory ingestion phase, before indexing.
Counts were derived by scripts/verify_official_dataset.py against the actual
Phase 0 and Phase 1 files; not from a live cluster run.
"""
import argparse
import json
from pyspark.sql import functions as F
from search_engine.spark_common import create_spark, table_names

EXPECTED = {
    0: {'rows': 1000, 'live': 1000, 'deleted': 0, 'links': 499},
    1: {'rows': 1025, 'live': 975, 'deleted': 50, 'links': 467},
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phase', type=int, choices=(0, 1), required=True)
    a = parser.parse_args()
    spark = create_spark('search-verify-phase-%s' % a.phase)
    try:
        pages_name, links_name = table_names()
        pages = spark.table(pages_name)
        links = spark.table(links_name)
        live = pages.where(~F.col('is_deleted'))
        expected = EXPECTED[a.phase]
        rows = pages.count()
        live_count = live.count()
        deleted = pages.where(F.col('is_deleted')).count()
        link_count = links.count()
        duplicate_ids = pages.groupBy('doc_id').count().where(F.col('count') > 1).count()
        ids = live.select('doc_id')
        missing_sources = links.join(ids.select(F.col('doc_id').alias('source_id')), 'source_id', 'left_anti').count()
        missing_targets = links.join(ids.select(F.col('doc_id').alias('target_id')), 'target_id', 'left_anti').count()
        actual = {'rows': rows, 'live': live_count, 'deleted': deleted, 'links': link_count,
                  'duplicate_ids': duplicate_ids, 'missing_link_sources': missing_sources,
                  'missing_link_targets': missing_targets}
        errors = []
        for key, want in expected.items():
            if actual[key] != want:
                errors.append(f'{key}: got {actual[key]}, expected {want}')
        for key in ('duplicate_ids', 'missing_link_sources', 'missing_link_targets'):
            if actual[key] != 0:
                errors.append(f'{key}: expected 0, got {actual[key]}')
        print('DATASET_PHASE_VERIFICATION=' + json.dumps({
            'phase': a.phase, 'expected': expected, 'actual': actual,
            'passed': not errors, 'errors': errors}, sort_keys=True))
        if errors:
            raise RuntimeError('Dataset-specific phase verification FAILED: ' + '; '.join(errors))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
