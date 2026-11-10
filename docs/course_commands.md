# Course-specific setup guides and commands

The relevant official tutorial pages were checked when preparing this project. These links are the authoritative installation references; the commands below adapt **single-machine lab examples to the mandatory four-VM project layout**. Different Spark or Kafka versions require compatible dependency coordinates.

## Course links (direct)

| Course resource | Where it's used |
|---|---|
| [VM setup](https://big-data-2026-supersite.super.site/get-your-vm-setup-1) | Ubuntu 24.04 VM provisioning, recommended resources |
| [Prerequisites](https://big-data-2026-supersite.super.site/prerequisites) | Java **11**, SSH, firewall |
| [Hadoop installation](https://big-data-2026-supersite.super.site/hadoop-installation-guide-1) | Hadoop **3.3.6**, JAVA_HOME, daemon setup |
| [Hadoop verification](https://big-data-2026-supersite.super.site/hadoop-installation-verification) | HDFS verification |
| [Spark installation](https://big-data-2026-supersite.super.site/spark-install) | Spark commands and env setup |
| [SparkSQL hands-on](https://big-data-2026-supersite.super.site/sparksql-hands-on) | Spark SQL and table operations |
| [ZeroTier setup](https://big-data-2026-supersite.super.site/zerotier-setup) | Peer connectivity across four VMs |
| [Iceberg hands-on](https://big-data-2026-supersite.super.site/apache-iceberg-hands-on) | Spark + Iceberg catalog, SQL, snapshots |
| [Kafka — KRaft](https://big-data-2026-supersite.super.site/kafka-kraft) | Optional broker/producer configuration |
| [Spark Structured Streaming starter](https://big-data-2026-supersite.super.site/spark-structured-streaming-a-quick-starter) | Optional Kafka streaming jobs |
| [Original project manual](https://big-data-2026-supersite.super.site/projects-1/distributed-search-engine) | **Mandatory deliverables**, node roles, BM25/PageRank weights |

## Relevant baseline commands

```bash
# Update Ubuntu 24.04
sudo apt update && sudo apt -y upgrade
# Prerequisites guide requires Java 11
sudo apt -y install openjdk-11-jdk openssh-server pdsh rsync curl git python3-venv
java -version
sudo systemctl enable --now ssh
ssh localhost "echo SSH_OK"

# HDFS install guide specifies Hadoop 3.3.6 (install using official guide)
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

# After configuring the DISTRIBUTED HDFS config across Nodes 1-3:
hdfs dfsadmin -report
hdfs getconf -confKey dfs.replication
hdfs dfs -ls /
hdfs fsck /search -files -blocks -locations

# Spark master/worker services (after official Spark installation)
# Node 3: start-master.sh; Node 2 and Node 3: start-worker.sh
$SPARK_HOME/sbin/start-master.sh --host "$NODE3_HOST"
$SPARK_HOME/sbin/start-worker.sh "spark://${NODE3_HOST}:7077"

# Spark version and configured packages
spark-submit --version

# Iceberg version history: after running ingestion in an Iceberg-enabled Spark shell
spark-sql --packages "$ICEBERG_RUNTIME" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse="$ICEBERG_WAREHOUSE"
```

Inside the Iceberg-enabled Spark shell:

```sql
SHOW NAMESPACES IN lake;
SHOW TABLES IN lake.search;
SELECT COUNT(*) FROM lake.search.pages WHERE NOT is_deleted;
SELECT COUNT(*) FROM lake.search.links;
SELECT snapshot_id, committed_at FROM lake.search.pages.snapshots ORDER BY committed_at DESC;
SELECT * FROM lake.search.inverted_index LIMIT 10;
SELECT SUM(score) FROM lake.search.pagerank;
```

## Differences from the introductory labs

The introductory Hadoop tutorial is **pseudo-distributed single-node** and uses `fs.defaultFS=hdfs://localhost:9000` plus `dfs.replication=1`. These values **must not be copied unchanged** into this project's four-machine architecture. The course project requires `fs.defaultFS=hdfs://NODE2_ZT_IP:9000`, Node 2 NameNode + DataNode, Node 3 DataNode, and `dfs.replication=2`. The Spark master runs on Node 3; two Spark workers execute on Nodes 2 and 3. The Node 1 Spark driver is a client process and must be network-reachable over ZeroTier by the remote workers.

Java 11 and Hadoop 3.3.6 are explicitly named in the course guides. The compatible Spark/Iceberg/Kafka JAR versions in `.env.example` are **defaults to validate against your actual installation**, not a claim that the course guides require precisely those artifacts.

## Spark/Iceberg command troubleshooting

- `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog`: missing/mismatched Iceberg runtime JAR; fix `ICEBERG_RUNTIME` to match Spark major/minor and Scala binary.
- `Failed to find data source: kafka`: add `SPARK_KAFKA_PACKAGE` to `spark-submit --packages`; match the Spark/Scala release.
- `Connection refused` to HDFS: check NameNode IP, network and firewall; NEVER use `localhost` for cross-machine HDFS clients.
- Spark stuck at zero registered workers: check Node 3 master UI, workers' `SPARK_MASTER`, Java/Scala compatibility, and inbound routes.
- Remote executors cannot call driver: use `spark.driver.host` bound to the correct Node 1/Node 3 ZeroTier IP and allow Spark driver port in private firewall rules.
- `AnalysisException` for Iceberg MERGE: confirm extensions and Iceberg v2 table creation.
- `hdfs namenode -format`: destroys existing namespace metadata; only use during **initial setup** on an empty cluster.
- Spark 4 binary with Spark 3.5 Iceberg runtime does not work: use corresponding Iceberg runtime packages and test compatibility.
