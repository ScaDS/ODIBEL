mvn package

scp -P 52222 target/odibel-1.0-SNAPSHOT-jar-with-dependencies.jar spark@127.0.0.1:/opt/spark/work-dir/job.jar