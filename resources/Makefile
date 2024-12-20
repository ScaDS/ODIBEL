# .PHONY: all

convertTKGALL2parquet:
	bash sparkJsonToParquet.sh /user/hofer/dbpedia-tkg/tkg_all_20240601.json.bz2 /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet 2048

convertHeadMetadata2parquet:
	bash sparkJsonToParquet.sh /user/hofer/dbpedia-tkg/head_metadata.json/all2 /user/hofer/dbpedia-tkg/tmp/head_metadata2.parquet 2048

evalGenerateSubgraphs:
	bash exec-spark.sh eval snapshot -i /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet -o /user/hofer/dbpedia-tkg/tmp/subgraphs -f genWPLsubgraph,genCATsubgraph,genDBOsubgraph

evalGenerateSubgraphDBOnoWPL:
	bash exec-spark.sh eval snapshot -i /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet -o /user/hofer/dbpedia-tkg/tmp/subgraphs -f genDBOsubgraphNoWPL

evalGenerateSnapshots:
	bash exec-spark.sh eval snapshot -i /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet -o /user/hofer/dbpedia-tkg/tmp/snapshots -f genYearlySnapshots


evalSnapshots:
	bash exec-spark.sh eval snapshot -i /user/hofer/dbpedia-tkg/tmp/snapshots/ -o /user/hofer/dbpedia-tkg/stats -f yearlyTripleDiffStats,yearlyOutDegreeDistribution,yearlyOutDegreeDistributionOnlyObjects


evalHeadMeta:
	bash exec-spark.sh eval input -i /user/hofer/dbpedia-tkg/tmp/head_metadata2.parquet -o /user/hofer/dbpedia-tkg/stats -f namespacePageCount,namespaceRevisionCount,revisionsPerYear,pagesPerYear

evalOutputTKGALL_summary:
	bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet -o /user/hofer/dbpedia-tkg/stats/FULL -f summary

evalOutputTKGCAT_summary:
	bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/subgraphs/CAT -o /user/hofer/dbpedia-tkg/stats/CAT -f summary

evalOutputTKGDBO_summary:
	bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/subgraphs/DBO -o /user/hofer/dbpedia-tkg/stats/DBO -f summary

evalOutputTKGDBOnoWPL_summary:
	bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/subgraphs/DBOnoWPL -o /user/hofer/dbpedia-tkg/stats/DBOnoWPL -f summary

evalOutputTKGWPL_summary:
	bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/subgraphs/WPL -o /user/hofer/dbpedia-tkg/stats/WPL -f summary

evalOutputTKGALL_hourWindowdist:
	bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet -o /user/hofer/dbpedia-tkg/stats/FULL -f hourWindowDistribution

evalOutputTKGALL_startEndTriples:
	bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet -o /user/hofer/dbpedia-tkg/stats/FULL -f countStartTriplesOverTime,countEndTriplesOverTime

writeCSV:
	bash sparkFinalCSV.sh /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.csv 2048

writeNQ:
	bash sparkFinalNQ.sh /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet /user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.nq 2048

test:
	seq 2000 2025 | while read -r year; do \
	echo $$year; \
	hdfs dfs -mkdir -p /user/hofer/dbpedia-tkg/stats/FULL/SNAP/$$year-06-01; \
        bash exec-spark.sh eval output -i /user/hofer/dbpedia-tkg/tmp/snapshots/$$year-06-01 -o /user/hofer/dbpedia-tkg/stats/FULL/SNAP/$$year-06-01 -f summary; \
	done
