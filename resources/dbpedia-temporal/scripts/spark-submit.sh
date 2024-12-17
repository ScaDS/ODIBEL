

SPARK_OPS+=" --master spark://athena1:37077"
SPARK_OPS+=" --driver-memory 2g"
SPARK_OPS+=" --executor-memory 128g"
SPARK_OPS+=" --executor-cores 32"
SPARK_OPS+=" --conf spark.ui.port=34040"
SPARK_OPS+=" --properties-file /conf/spark-defaults.conf"

JAR_PATH="/opt/spark/work-dir/job.jar"
JAR_CLASS="ai.scads.odibel.datasets.wikitext.DiefPoolTest"
JAR_ARGS="/user/hofer/wikidumps/en/2024-06-01/bz2_lines/enwiki-20240601-pages-meta-history1.* /user/hofer/wikidumps/en/hist_pars/1"
#JAR_ARGS="spark-dummy"

# scp -P 52222 target/odibel-1.0-SNAPSHOT-jar-with-dependencies.jar spark@127.0.0.1:/opt/spark/work-dir/job.jar
ssh -p 52222 spark@127.0.0.1 "JAVA_HOME=/opt/java/openjdk/ HADOOP_USER_NAME=hadena /opt/spark/bin/spark-submit $SPARK_OPS --class $JAR_CLASS $JAR_PATH $JAR_ARGS"

exit 0
SPARK_OPS+=" --deploy-mode cluster"
