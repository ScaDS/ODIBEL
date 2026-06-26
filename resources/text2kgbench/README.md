# Building Multi Source Format KG Integration Benchmark Datasets using DBpedia and Ontologies of Text2KGBench

## Ontologies

1_university_ontology.json
2_musicalwork_ontology.json
3_airport_ontology.json
4_building_ontology.json
5_athlete_ontology.json
6_politician_ontology.json
7_company_ontology.json
8_celestialbody_ontology.json
9_astronaut_ontology.json
10_comicscharacter_ontology.json
11_meanoftransportation_ontology.json
12_monument_ontology.json
13_food_ontology.json
14_writtenwork_ontology.json
15_sportsteam_ontology.json
16_city_ontology.json
17_artist_ontology.json
18_scientist_ontology.json
19_film_ontology.json


## Issues of Text2KGBench

- Ontologies are not well-defined
  - instead of xsd:... uses this:number
  - only defined ObjectProperties even if this is clearly a DatatypeProperty


## Example Generation

```
sdk use java 17.0.16-tem

uv run generate_refac.py file://./data/dbpedia-multi-source-kg-data/selected.nt.bz2 text2kgbench/subgraphs data/text2kgbench/data/kgpipe-ontologies/dbpedia_webnlg/13_food_ontology.ttl
```

## On Spark Cluster

Run from the repository root. The default Spark Docker image ships **Python 3.10**, so build
wheels and dependencies for 3.10 even if local development uses 3.12.

```
SPARK_PYTHON=3.10

# 1. Build the project wheel
uv build --python $SPARK_PYTHON

# 2. Install runtime dependencies into dist/deps/ (exclude pyspark and pyodibel itself)
uv export --format requirements.txt --no-dev --no-hashes \
  --prune pyspark --no-emit-package pyodibel --python $SPARK_PYTHON \
  -o /tmp/spark-deps.txt
uv pip install --python $SPARK_PYTHON --target dist/deps --python-version 3.10 \
  -r /tmp/spark-deps.txt
cd dist/deps && zip -r ../deps.zip . && cd ../..
```

```
spark-submit \
  --master yarn \
  --py-files dist/pyodibel-0.1.0-py3-none-any.whl,dist/deps.zip \
  --driver-memory 8g \
  --executor-memory 16g \
  resources/text2kgbench/generate_refac.py \
  hdfs:///path/to/selected.nt.bz2 \
  hdfs:///path/to/subgraphs \
  hdfs:///path/to/13_food_ontology.ttl
```