java -cp target/odibel-1.0-SNAPSHOT-jar-with-dependencies.jar ai.scads.odibel.main.Main dbpedia-tkg extract -i hdfs://172.26.54.1:9000/user/hofer/wikidumps/en/2024-06-01/bz2_lines_hist/$1/ -o hdfs://172.26.54.1:9000/user/hofer/wikidumps/en/2024-06-01/bz2_lines_hist.out/$1/ -e http://172.26.54.2:59001-59016/server/extraction/en,http://172.26.54.3:59001-59016/server/extraction/en,http://172.26.54.4:59001-59016/server/extraction/en,http://172.26.54.5:59001-59016/server/extraction/en,http://172.26.54.6:59001-59016/server/extraction/en,http://172.26.54.7:59001-59016/server/extraction/en,http://172.26.54.8:59001-59016/server/extraction/en,http://172.26.54.9:59001-59016/server/extraction/en 2>&1 > logs/extract_$1_$stamp.log

