"""VM3: generate the TA runbook's literal /serving/vN/{docs.parquet,index.parquet,_READY}.

This is an ADDITIONAL read-only copy of the already-validated main Parquet
release under /search/exports/vN. Node4 still deploys the original release.
Never publish a READY marker for incomplete output.
"""
import argparse
import json
import uuid
from pyspark.sql import SparkSession
from search_engine.spark_common import create_spark


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--version', required=True)
    p.add_argument('--export-root', required=True)
    p.add_argument('--serving-root', required=True)
    a = p.parse_args()
    if not a.version or not a.version.replace('_', '').replace('-', '').isalnum():
        p.error('Version may contain only letters, digits, underscore and hyphen')
    spark = create_spark('ta-runbook-parquet-compat-' + a.version)
    try:
        jvm = spark._jvm
        Path = jvm.org.apache.hadoop.fs.Path
        fs = Path(a.serving_root).getFileSystem(spark._jsc.hadoopConfiguration())
        source = a.export_root.rstrip('/') + '/' + a.version
        root = a.serving_root.rstrip('/')
        dest = root + '/' + a.version
        stage = root + '/.staging-' + a.version + '-' + uuid.uuid4().hex
        if not fs.exists(Path(source + '/manifest/_SUCCESS')):
            raise RuntimeError('Primary validated release is not ready: ' + source)
        if fs.exists(Path(dest)):
            raise RuntimeError('Refusing to overwrite existing TA release: ' + dest)
        docs = spark.read.parquet(source + '/docs')
        postings = spark.read.parquet(source + '/postings')
        docs_count = docs.count()
        postings_count = postings.count()
        if docs_count <= 0 or postings_count <= 0:
            raise RuntimeError('Cannot publish empty document/index export')
        fs.mkdirs(Path(root))
        fs.mkdirs(Path(stage))
        try:
            for name, frame in (('docs.parquet', docs), ('index.parquet', postings)):
                tmp = stage + '/.tmp_' + name
                frame.coalesce(1).write.mode('errorifexists').parquet(tmp)
                parts = [status.getPath() for status in fs.listStatus(Path(tmp))
                         if status.isFile() and status.getPath().getName().endswith('.parquet')]
                if len(parts) != 1:
                    raise RuntimeError('Expected exactly one Parquet part for ' + name)
                dest_file = Path(stage + '/' + name)
                if not fs.rename(parts[0], dest_file):
                    raise RuntimeError('Failed to rename Parquet part to ' + name)
                fs.delete(Path(tmp), True)
                if fs.getFileStatus(dest_file).getLen() <= 0:
                    raise RuntimeError('Empty Parquet output: ' + name)
            # Marker only after both files have been fully written and validated.
            fs.create(Path(stage + '/_READY'), False).close()
            if not fs.rename(Path(stage), Path(dest)):
                raise RuntimeError('Failed to publish the completed serving directory')
        finally:
            if fs.exists(Path(stage)):
                fs.delete(Path(stage), True)
        print('TA_COMPAT_EXPORT=' + json.dumps({
            'version': a.version, 'path': dest, 'documents': docs_count,
            'index_postings': postings_count, 'ready': True}, sort_keys=True))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
