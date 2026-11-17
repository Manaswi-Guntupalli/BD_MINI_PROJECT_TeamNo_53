#!/usr/bin/env bash
# Safe VM1 preflight: reads configs/datasets only; does not start, format or modify HDFS.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Missing .env: cp .env.example .env then edit ZeroTier addresses' >&2; exit 2; }
set -a; source .env; set +a
python3 - <<'PYCODE'
import ipaddress, os, sys
ip_names=('NODE1_HOST', 'NODE2_HOST', 'NODE3_HOST', 'NODE4_HOST')
errors=[]
for key in ip_names:
    value=os.environ.get(key, '')
    try:
        ip=ipaddress.ip_address(value)
        if ip.version!=4: errors.append(f'{key}: expected IPv4, got {value!r}')
    except ValueError: errors.append(f'{key}: invalid/unconfigured IPv4 address: {value!r}')
network=ipaddress.ip_network(os.environ.get('NODE1_HOST', '0.0.0.0')+'/24', strict=False)
for key in ip_names:
    try:
        ip=ipaddress.ip_address(os.environ[key])
        if ip in (network.network_address, network.broadcast_address):
            errors.append(f'{key}={ip}: network/broadcast address in {network}; ask owner for actual ZeroTier host IP')
    except (ValueError, KeyError): pass
if len(set(os.environ.get(k) for k in ip_names)) != len(ip_names):
    errors.append('One or more NODE IPs are identical; verify all four VMs')
if os.environ.get('SPARK_MASTER') != f"spark://{os.environ.get('NODE3_HOST')}:7077":
    errors.append('SPARK_MASTER must use Node 3 ZeroTier IP port 7077')
expected_hdfs=f"hdfs://{os.environ.get('NODE2_HOST')}:9000"
if os.environ.get('HDFS_URI') != expected_hdfs:
    errors.append('HDFS_URI must use Node 2 ZeroTier IP port 9000')
if not os.environ.get('ICEBERG_WAREHOUSE','').startswith(expected_hdfs+'/'):
    errors.append('ICEBERG_WAREHOUSE must point into Node 2 HDFS')
for env in ('PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON'):
    exe=os.environ.get(env,'/usr/bin/python3.10')
    if not os.path.exists(exe): errors.append(env+' executable not found: '+exe)
if '3.3_2.12' not in os.environ.get('ICEBERG_RUNTIME',''):
    errors.append('ICEBERG_RUNTIME is not for Spark 3.3 / Scala 2.12')
if '3.3.4' not in os.environ.get('SPARK_KAFKA_PACKAGE',''):
    errors.append('SPARK_KAFKA_PACKAGE is not for Spark 3.3.4')
if errors:
    for error in errors:print('FAIL:',error,file=sys.stderr)
    raise SystemExit(2)
print('PASS: environment IP addresses, Spark versions, and driver Python path configured')
PYCODE
PYTHONPATH=. python3 scripts/check_input.py data/official/pages.jsonl --phase 0
PYTHONPATH=. python3 scripts/check_input.py data/official/changes.json --phase 1
printf '\nPASS: VM1 preflight passed. Next check VM2 HDFS, VM3 Spark, then run Phase 0.\n'
