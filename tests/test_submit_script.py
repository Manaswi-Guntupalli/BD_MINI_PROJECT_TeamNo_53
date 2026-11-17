"""Submit script must keep optional Kafka deps out of mandatory jobs."""
from pathlib import Path
import os
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]

def test_packages_are_selected_by_job(tmp_path):
    fake=tmp_path/'spark-submit'
    fake.write_text('#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n')
    fake.chmod(0o755)
    env={**os.environ,'PATH':str(tmp_path)+os.pathsep+os.environ['PATH'],
         'SPARK_MASTER':'spark://10.0.0.3:7077','HDFS_URI':'hdfs://10.0.0.2:9000',
         'ICEBERG_RUNTIME':'example:iceberg:1','SPARK_KAFKA_PACKAGE':'example:kafka:1',
         'PYSPARK_PYTHON':sys.executable,'PYSPARK_DRIVER_PYTHON':sys.executable}
    for job,expected in [('jobs/ingest.py','example:iceberg:1'),('jobs/build_index.py','example:iceberg:1'),('jobs/ta_export_compat.py','example:iceberg:1'),('jobs/stream_ingest.py','example:iceberg:1,example:kafka:1')]:
        p=subprocess.run(['bash',str(ROOT/'scripts/submit.sh'),job],cwd=ROOT,env=env,capture_output=True,text=True,check=True)
        import json
        argv=json.loads(p.stdout)
        assert argv[argv.index('--packages')+1]==expected
        assert f'spark.executorEnv.PYSPARK_PYTHON={sys.executable}' in argv
