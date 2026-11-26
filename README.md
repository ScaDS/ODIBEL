# PyODIBEL

**Open Data Integration Benchmark Evaluation Lab**

PyODIBEL generates and evaluates benchmark datasets for data integration tasks such as entity resolution, schema matching, and entity fusion. The datasets are built from Linked Open Data, public dumps (DBpedia, Wikipedia, Wikidata), and existing benchmark exports.

Version 0.1.0 is the alpha consolidation (OD-M0). The supported surface is the Python library under `src/pyodibel` and the workflows marked supported in the [inventory](docs/inventory.md). Abstract API types, the Scala temporal-KG jobs, and most scripts under `resources/` are experimental.

## Install

Python `>=3.10,<3.13`. Spark commands need Java 17 (the Dockerfile and `pom.xml` use 17; a Java 8 default will fail).

```bash
uv sync --extra test
```

The console script is `pyodibel`. The Scala jobs are a separate Maven build: `mvn package` produces the jar used by the Dockerfile (`ai.scads.odibel.main.Main`).

## Supported entry points

RDF on Spark, documented in [docs/rdf2.md](docs/rdf2.md):

```python
from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2

spark = get_spark_session("example")
rdf = rDF2.parse(spark, "/path/to/input.nt")
```

CLI:

```bash
pyodibel rdf run --input /path/to/input.nt --output /path/to/output.nt --help
pyodibel rdf schema-graph --input /path/to/input.nt --output /path/to/schema.csv --help
```

Also supported, with tests: N-Triples I/O and reachable subgraphs, shading and match clusters, stratified and representative sampling, in-memory entity filter/project/join, RocksDB and HTTP caches, entity-resolution benchmark data and evaluation, and overlapping subset construction (`pyodibel.datasets.mp_mf`).

KGI-Bench domain datasets from DBpedia and Text2KGBench ontologies are documented in [docs/kgi_data.md](docs/kgi_data.md). The scripts live in `resources/text2kgbench/`.

## Experimental

These exist in the tree and are not a stable API:

- `pyodibel.api` interfaces (`Entity`, `Source`, `Operation`, `Benchmark`, `Evaluator`) and the schema-matching and entity-fusion packages
- `GradoopSource` for FAMER/Gradoop exports
- Spark operation stubs that raise `NotImplementedError`
- `pyodibel.management.systemkg` (imports without `kgcore`; `require_kgcore()` raises a clear `ImportError` until the external package is installed)
- Not implemented and no longer exported from `pyodibel`: `WikidataDumpSource`, `LinkedDataSource`, `StorageBackend`, `PartitionStrategy`
- `import pyodibel` does not configure logging or write `pyodibel.log`; call `pyodibel.setup_logging()` to opt in
- Dataset scripts under `resources/` other than the text2kgbench path above, including MovieKG (tracked in issues #11–#14)
- The Scala DBpedia temporal KG pipeline under `src/main/scala/`

## Layout

```text
src/pyodibel/     installed library (api, operations, benchmark, cache, cli, source)
src/main/scala/   Spark/Scala jobs (temporal KG)
resources/        dataset generation workflows, not installed
docs/             MkDocs site, including the OD-01 inventory
pom.xml           Scala build
pyproject.toml    Python build
```

New reusable code belongs in `src/pyodibel/`. A one-off generator stays in `resources/<workflow>/` until a second workflow needs it. Details, branch dispositions, and follow-up issues are in [docs/inventory.md](docs/inventory.md).
