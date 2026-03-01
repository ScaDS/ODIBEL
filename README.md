# PyODIBEL

**Open Data Integration Benchmark Evaluation Lab**

PyODIBEL is a Python library for generating and evaluating benchmark datasets for data integration tasks—such as entity matching and entity linking—across different domains. These benchmarks are built from Linked Open Data (LOD) sources, large public dumps such as Wikipedia and Wikidata, or extending benchmark datasets.

## Features

- **Multi-Source Data Ingestion**: Support for SPARQL endpoints, REST APIs, web pages, and data dumps
- **Flexible Storage**: File-based, database, or S3-compatible storage backends
- **Format Support**: Work with CSV, JSON, RDF, and other formats seamlessly
- **Benchmark Construction**: Create reproducible datasets with splits and ground-truth annotations
- **Benchmark Evaluation** 

## Structure

```
api/
    benchmark.py # interface to define and describe benchmark data
    entity.py # main class to define an wrapp other entities
    evaluation.py # evaluation suite interface for a specific dataset or benchmark data to derive chraracterisitcs (e.g. size, missing values...)
    operations.py # interface to define operations on enities, e.g., and impl would be a join of rdf data represented in an Spark dataframe
    souce.py # interface to read in and process data in speciifc structure in odibel providing a typed interface to access it
benchmark # definition of interfaces and evaluation functions for 
    entity_resolution
        data.py # describes the data artifacts of the entity resoltion benchmark task
        eval.py # enables evaluation of benchmark data artifacts for the entity resolution tasks, creating metrics about the data not about the task results
    schema_matching
        data.py
        eval.py
    entity_fusion
        data.py
        eval.py
cli
    main.py
    ...
management # management of internal information for using odibel
    systemkg.py # later connection to represent odibel actions and implementations as knowledge graph
    clusters.py # manage same as clusters of entities
    ...
operations # collection of named operations often in use by the framework on specific data structures
    base/
        entity_ops.py #
        entity_spark_ops.py
    rdf/
        rdf_spark_ops.py # rdf operations for RDF data (triples, or  quads) represented in a spark dataframe
source
    gradoop # reading in gradoop datastructure
    wikidata # reading in wikidata json
```