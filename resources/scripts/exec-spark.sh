#!/usr/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

master="$(docker ps --format '{{.Names}}' | grep spark352_master)"
docker cp $SCRIPT_DIR/../target/odibel-1.0-SNAPSHOT-jar-with-dependencies.jar $master:/opt/spark/work-dir/job.jar
docker exec -u root $master chown spark:spark /opt/spark/work-dir/job.jar

docker exec $master \
bash -c "HADOOP_USER_NAME=hadena \
/opt/spark/bin/spark-submit \
  --master spark://athena1:37077 \
  --name "ODIBEL" \
  --driver-memory 2g \
  --executor-memory 256g \
  --executor-cores 64  \
  --conf spark.hadoop.fs.defaultFS=hdfs://athena1.informatik.intern.uni-leipzig.de:9000 \
  --class ai.scads.odibel.main.Main \
  job.jar $*"
