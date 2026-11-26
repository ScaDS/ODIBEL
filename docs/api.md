# PyODIBEL API

The supported calls are the ones below. Everything else in the package is experimental: it may be a stub, an untested task package or an adapter that does not exist yet. The full module table is the [inventory](inventory.md).

## Supported

### rDF2

```python
from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2

spark = get_spark_session("RDF2")
rdf = rDF2.parse(spark, "/path/to/input.nt")
persons = rdf.filter_triples_by_s_type("<http://dbpedia.org/ontology/Person>")
persons.write_nt("/path/to/persons.nt")
```

`get_spark_session` currently ignores `master`. The selection logic is commented out in `spark_mgr.py`. Pass Spark configuration through the environment or `spark-submit` until that is restored.

Further operations (reachable subgraphs, sampling, schema graph, domain/range cleaning) are in the [rDF2 guide](rdf2.md).

N-Triples helpers that do not need Spark live in `pyodibel.operations.rdf.nt_io` and `pyodibel.operations.rdf.reachability`. Shading and namespace replacement live in `pyodibel.operations.rdf.replacer`.

### Sampling and profiling

```python
from pyodibel.operations.sample import (
    sample_representative_subgraph,
    stratified_sample_triples,
    class_distribution,
    property_distribution,
    degree_distribution,
)
```

`sample_representative_subgraph` and the stratified RDF sampler use an `rDF2` instance. `stratified_sample_triples` and the quota helpers work on in-memory triples. Distribution helpers summarize classes, properties, and degree.

### Entity operations

`pyodibel.operations.base` provides in-memory `FilterOperation`, `ProjectOperation`, and `JoinOperation` on `SimpleEntity`. The Spark subclasses in `entity_spark_ops` and `rdf_spark_ops` raise `NotImplementedError`.

### Cache

```python
from pyodibel.cache import HttpFetchCache, RocksDbCache
```

`RocksDbCache` uses `rocksdict`.

### Entity resolution benchmark

```python
from pyodibel.benchmark.entity_resolution.data import (
    EntityPair,
    EntityResolutionData,
    EntityResolutionBenchmark,
)
from pyodibel.benchmark.entity_resolution.eval import EntityResolutionEvaluator
```

The package `__init__.py` does not re-export these names. Schema matching and entity fusion mirror this shape and have no tests.

### Overlap

`pyodibel.datasets.mp_mf.overlap_util` builds overlapping entity subsets for multi-source splits.

### CLI

```bash
pyodibel rdf run --input INPUT --output OUTPUT
pyodibel rdf schema-graph --input INPUT --output OUTPUT
```

`rdf run` chains subject-type filters, class filters, or entity sampling, then writes N-Triples. `rdf schema-graph` writes a source-type / relation / target-type CSV.

### Gradoop import (experimental)

```python
from pyodibel.api.source import SourceConfig
from pyodibel.source.gradoop.source import GradoopSource
```

Older scripts that import `DataSourceConfig` or `GradoopDataSource` do not match this API. See `resources/dexter-kg/generate.py`.

## Extension conventions

- A source adapter implements `pyodibel.api.source.Source` and lives in `src/pyodibel/source/<name>/`.
- A reusable operation lives in `src/pyodibel/operations/<family>/`.
- A dataset generator stays in `resources/<workflow>/` until a second workflow needs the same helper.
- Task-specific benchmark records subclass `pyodibel.api.benchmark.Benchmark` and live in `src/pyodibel/benchmark/<task>/`.

## Unimplemented design sketch

The notes below were the previous contents of this page. They are not implemented. There is no `CoverageSampling` type, no `sp-construct`, and no `sparql_store` / `urihash_store` / `object_store` / `postgres_store` backend. `SQLAlchemy` was declared and unused and has been removed.

```text
sample
  CoverageSampling — requires avg relation coverage
  ISampling

mapping
  replace
    Shade — ignore namespace/property
    UriHashShading
    NumericShading

construct
  sp-construct

select
  Class Selection — remove untyped

retrieve
acquire/populate

inference
  add type based on relation

store
  sparql_store
  urihash_store
  object_store
  postgres_store
```

Related pieces that do exist: shading in `replacer.shade_reference_graph`, class-rooted subgraphs in `reachability`, and URI hashing in `replacer.hashed_resource_uri`.
