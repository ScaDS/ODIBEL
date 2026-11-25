# Open Data Integration Benchmark and Evaluation Lab (ODIBEL)

A collection of benchmark dataset generation and evaluation code for data integration problems.

Version 0.1.0 is an alpha. The [inventory](inventory.md) is the map of what is supported, what is experimental, and which branches are still open.

## Supported now

- RDF processing with [rDF2](rdf2.md): parse N-Triples into Spark, filter, sample, write, and build a schema graph
- [KGI-Bench domain data](kgi_data.md) from a DBpedia dump and Text2KGBench ontologies (`resources/text2kgbench/`)
- Entity-resolution benchmark records and dataset evaluation under `pyodibel.benchmark.entity_resolution`
- CLI: `pyodibel rdf run`, `pyodibel rdf schema-graph`

## Experimental

- Abstract interfaces in `pyodibel.api`, plus schema matching and entity fusion
- Movie multi-source KG (`resources/movie-multi-source-kg/`), tracked separately in issues #11–#14
- DBpedia multi-source class selection, KGFlex fetch scripts, and the Scala DBpedia temporal KG pipeline
- Gradoop/FAMER import (`GradoopSource`)

## Layout

```text
src/pyodibel/     Python library
src/main/scala/   Scala/Spark jobs
resources/        generation workflows
docs/             this site
```

Library entry points and extension rules are on the [API](api.md) page.
