## Livy Setup
* conf/livy-env.sh
```
LIVY_SERVER_JAVA_OPTS="\
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED"
```

* conf/livy-conf
```
livy.spark.master = local[*]
```

* cmd
```
export SPARK_HOME=/home/ec2-user/spark/spark-3.5.2-bin-hadoop3

./bin/livy-server start

./bin/livy-server stop
```

## Run
* Spark on OpenSearch Snapshot
```
python3.11 livy_benchmark.py --queries-file tracks/iceberg/queries.json --output-csv tracks/iceberg/benchmark_results.csv --livy-conf-file tracks/iceberg/config.json
```
* Spark Iceberg
```
python3.11 livy_benchmark.py --queries-file tracks/snapshot/queries.json --output-csv tracks/snapshot/benchmark_results.csv --livy-conf-file tracks/snapshot/config.json
```