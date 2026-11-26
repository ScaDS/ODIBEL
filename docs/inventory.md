# OD-01 inventory

Audit for [OD-01](https://github.com/ScaDS/ODIBEL/issues/7), recorded on branch `od-01-inventory` from `main` at `3cb3c0b` (2026-11-25).

This is an inventory and a documentation alignment. It does not redesign the benchmark API, does not merge the open feature branches, and does not change blank-node identifier generation ([#5](https://github.com/ScaDS/ODIBEL/issues/5)). MovieKG reproduction stays on [#11](https://github.com/ScaDS/ODIBEL/issues/11), [#12](https://github.com/ScaDS/ODIBEL/issues/12), [#13](https://github.com/ScaDS/ODIBEL/issues/13), and [#14](https://github.com/ScaDS/ODIBEL/issues/14).

Status labels used below:

| Label | Meaning |
| --- | --- |
| supported | Implemented, covered by tests or an operator doc, and safe to call from new workflows |
| experimental | Present and sometimes useful, with stubs, missing tests, or a drifting caller |
| redundant | Overlaps another module; keep until a caller is updated |
| deprecated | Empty or superseded placeholder; no behavior to preserve |

## Canonical layout

This audit treats `main` as the canonical tree, for review on this branch. The open branches add files under that tree. They do not introduce a second repository root. Historical layout work already landed in PRs #2 and #6 (`cleanup`).

```text
pyproject.toml          Python package (setuptools, src layout), version 0.1.0
uv.lock                 Locked Python environment
pom.xml                 Scala 2.13 / Spark 3.5.1 / Java 17 Maven build
Dockerfile              Maven package only; entrypoint ai.scads.odibel.main.Main
mkdocs.yml              GitHub Pages site (docs job on push to main)
src/pyodibel/           Installed library
src/main/scala/         DBpedia temporal KG and Wikidata-dump jobs
src/test/scala/         ScalaTest / sample suites (not run in CI)
resources/<workflow>/   Dataset generation scripts; not installed as a package
data/<dataset_id>/      Input and run datasets; only .odibel/ metadata is versioned
docs/                   Operator and API docs, including this inventory
studies/experiments/    Local experiments (rdf-part); outside the product surface
```

Extension conventions for new work:

- Reusable library code goes in `src/pyodibel/`. Source adapters go in `source/<name>/` and implement `pyodibel.api.source.Source`. Operations go in `operations/<family>/`.
- A dataset generator that is still a script stays in `resources/<workflow>/` and imports the library. Promote a helper into `src/pyodibel` only when a second workflow calls it.
- Do not add a second top-level Python package. `resources/kg-corruption` is the exception already on its branch: it has its own `pyproject.toml` and stays outside the `pyodibel` import surface until the benchmark API work (OD-08) asks for it.
- Public imports that new code should prefer are listed under [Entry points](#entry-points). `pyodibel/__init__.py` re-exports only the experimental `pyodibel.api` interfaces; import everything else from its own module.
- A dataset that a workflow reads or produces gets a directory `data/<dataset_id>/` with a `.odibel/` folder (see [Datasets](datasets.md)). Payloads stay out of Git.

## Entry points

Supported:

| Entry | Role |
| --- | --- |
| `pyodibel.operations.rdf.rdf2.rDF2` | Spark DataFrame of N-Triples columns `s`, `p`, `o`, `isLiteral`. Parse, filter, sample, schema graph, write. Documented in [rDF2](rdf2.md). |
| `pyodibel.management.spark_mgr.get_spark_session` | Shared SparkSession builder. This branch applies an explicit master only when no `SparkContext` exists. |
| `pyodibel.operations.rdf` helpers | `nt_io`, `reachability`, `schema_graph`, `uris`, `replacer` (shade / namespace replace), `cluster.MatchCluster` |
| `pyodibel.operations.sample` | Stratified and representative sampling, plus class, property, and degree distribution reports |
| `pyodibel.operations.base` | In-memory `FilterOperation`, `ProjectOperation`, `JoinOperation` |
| `pyodibel.cache` | `RocksDbCache` (`rocksdict`) and `HttpFetchCache` |
| `pyodibel.benchmark.entity_resolution` | `EntityResolutionData`, `EntityResolutionBenchmark`, `EntityResolutionEvaluator` |
| `pyodibel.management.clusters` | `InMemoryClusterManager` |
| `pyodibel.datasets.mp_mf.overlap_util` | Overlapping subset construction used by KGI splits |
| CLI `pyodibel rdf run` | Filter or sample an NT input and write NT |
| CLI `pyodibel rdf schema-graph` | Write a schema-level CSV |

Experimental:

| Entry | Role |
| --- | --- |
| `pyodibel.api` | Abstract `Entity`, `Source`, `Operation`, `Benchmark`, `Evaluator`. Several methods are empty. `Benchmark` task modules subclass these. |
| `pyodibel.benchmark.schema_matching` | Data classes and evaluator. No tests. Builder methods are empty. |
| `pyodibel.benchmark.entity_fusion` | Same shape as schema matching. No tests. |
| `pyodibel.source.gradoop.GradoopSource` | Gradoop/FAMER CSV and JSON reader. `resources/examples/dexter_kg_mapping.py` matches the current class names. |
| `pyodibel.operations.gradoop` | Legacy reader, cluster, and NetworkX graph helpers used by `GradoopSource` |
| `pyodibel.operations.base.entity_spark_ops` | Spark operation subclasses that raise `NotImplementedError` |
| `pyodibel.operations.rdf.rdf_spark_ops` | Same for RDF Spark operations |
| `pyodibel.management.systemkg` | Imports without `kgcore` since [#16](https://github.com/ScaDS/ODIBEL/issues/16); `require_kgcore()` raises a clear `ImportError`. `kgcore` is not a project dependency, so the module is unusable until it is installed. |
| `pyodibel.datasets.manifest`, `pyodibel.datasets.versions` | `.odibel` dataset metadata: manifest validation, payload checksums, and payload versions with a stat cache. Tested (`test_dataset_manifest.py`, `test_dataset_versions.py`); the metadata format is a prototype (version `0.1`). Documented in [Datasets](datasets.md). |
| CLI `pyodibel data hash`, `verify` | Write `.odibel/files.sha256`; check manifest keys and payload checksums (`--metadata-only` skips the payload) |
| CLI `pyodibel data status`, `commit`, `log` | Payload changes since the latest version, record a new version with optional labels, list versions |

Since [#16](https://github.com/ScaDS/ODIBEL/issues/16), `WikidataDumpSource`, `LinkedDataSource`, `StorageBackend`, and `PartitionStrategy` are no longer exported from `pyodibel`; their modules were never in the tree. `import pyodibel` no longer configures logging or writes `pyodibel.log`; `pyodibel.setup_logging()` opts in. `test_imports.py` covers both.

## Library modules

| Module | Status | Tests on `main` | Notes |
| --- | --- | --- | --- |
| `api/entity.py`, `api/source.py`, `api/operations.py`, `api/benchmark.py`, `api/evaluation.py` | experimental | `test_api_entity.py` covers `SimpleEntity` only | Contract for OD-08. The storage, partition, and backend names are no longer exported ([#16](https://github.com/ScaDS/ODIBEL/issues/16)). |
| `operations/rdf/rdf2.py` | supported | `test_rdf2_ontology_ops.py`, reachability and sample tests | Domain/range cleaning, type filters, sampling, schema graph. |
| `operations/rdf/nt_io.py` | supported | `test_nt_io.py` plus reachability tests | This branch reads a file or a Spark `write.text` directory. |
| `operations/rdf/reachability.py`, `uris.py` | supported | `test_reachable_subgraph.py` | In-memory and `rDF2` reachable subgraphs. |
| `operations/rdf/replacer.py`, `cluster.py` | supported | `test_shade.py` | Shading and match clusters for multi-source KGs. |
| `operations/rdf/schema_graph.py` | supported | covered by the schema-graph CLI path and rDF2 | Operator doc is the rDF2 guide. |
| `operations/sample/` | supported | `test_sample.py` (core, stratified triples, representative subgraph) | Distribution report helpers are implemented and have no direct test. |
| `operations/base/entity_ops.py` | supported | `test_operations_base.py` | In-memory entity filter, project, join. |
| `operations/base/entity_spark_ops.py`, `operations/rdf/rdf_spark_ops.py` | experimental | none | Raise `NotImplementedError`. |
| `operations/gradoop/` | experimental | none | Legacy FAMER reader. Two entity types: `operations/gradoop/entity.py` and `source/gradoop/entity.py`. |
| `source/gradoop/` | experimental | none | Current adapter name is `GradoopSource` with `SourceConfig`. |
| `cache/` | supported | `test_http_cache.py`, crawl tests | Requires `rocksdict`. |
| `management/spark_mgr.py` | supported | `test_spark_mgr.py` | An explicit master is applied only when no `SparkContext` exists. An existing context is left alone. |
| `management/clusters.py` | supported | `test_management_clusters.py` | In-memory manager. Abstract methods on the base class are empty. |
| `management/systemkg.py` | experimental | `test_imports.py` (import without `kgcore`) | Importable; functions need the external `kgcore` and call `require_kgcore()`. |
| `benchmark/entity_resolution/` | supported | `test_benchmark_entity_resolution.py`, `test_benchmark_evaluation.py` | Package `__init__.py` does not re-export the classes. |
| `benchmark/schema_matching/`, `benchmark/entity_fusion/` | experimental | none | Parallel to entity resolution, unfinished builders. |
| `datasets/mp_mf/overlap_util.py` | supported | `test_overlap_util.py` | Used by text2kgbench splits. |
| `datasets/manifest.py`, `datasets/versions.py` | experimental | `test_dataset_manifest.py`, `test_dataset_versions.py` (12 tests, including the `data` CLI) | `.odibel` metadata version `0.1`. Versions record file lists only; older payloads are not stored. |
| `cli/` | supported (`rdf`), experimental (`data`) | `data` commands through the dataset tests; `rdf` none | `rdf run`, `rdf schema-graph`, `data hash`, `verify`, `status`, `commit`, `log`. No benchmark or evaluation commands. |
| `src/main/scala/.../wikitext` | experimental | Scala tests exist; not run in this audit | DBpedia temporal extraction, CSV/named-graph/reification transforms. |
| `src/main/scala/.../sample`, `SparkDummy` | deprecated | sample tests only | Scratch jobs (`AnotherOne`, `SparkDummy`, `WildScalaTest`). |

Profiling in the library is `operations/sample/distributions.py` (class, property, and degree distributions). The benchmark evaluators describe a dataset (pair counts, coverage-style metrics), not a model score.

## Dataset workflows (`resources/`)

| Path | Status | What it does |
| --- | --- | --- |
| `text2kgbench/` | supported | KGI-Bench domain sets from a DBpedia NT dump and a Text2KGBench ontology: `generate_refac.py`, `subgraph.py`, `splits.py`, `sources.py`, plus `crawl.py` and `sampling.py`. Operator doc: [KGI-Bench data](kgi_data.md). Tests call `crawl.py`. `align_ontologies.py` links Text2KGBench concepts and relations to `dbo:` by name (`text2kgbench-v1-dbo`). `validate_run.py` recomputes each step of a run ([#21](https://github.com/ScaDS/ODIBEL/issues/21)). |
| `dbpedia-slice/` | experimental | `slice.sh`: seeded, entity-closed N-Triples slice of a DBpedia dump with an optional literal language filter. Writes the `.odibel/` folder of the minidump it produces. |
| `movie-multi-source-kg/` | experimental | Movie KG notes, `generate.py`, `evaluate.py`, `movie.conf`. Reproduction and reference checks are [#11](https://github.com/ScaDS/ODIBEL/issues/11)–[#14](https://github.com/ScaDS/ODIBEL/issues/14). |
| `dbpedia-multi-source-kg/` | experimental | DBpedia ontology class-to-subdomain classifier (`src/multi_source_kg/`). Own README. LLM mode needs an OpenAI-compatible key. `generate.py` passes `ont_path` (`ONTOLOGY_PATH`, otherwise the old DBpedia snapshot URL). It ran end to end on the English minidump ([#20](https://github.com/ScaDS/ODIBEL/issues/20)); `validate_run.py` checks a run. This script is the multi-source subgraph generator, not the DBpedia temporal Scala pipeline. |
| `dbpedia-temporal/` | experimental | Temporal KG model note and Python plot scripts. Generation itself is the Scala pipeline under `src/main/scala/.../wikitext`. `python/dbpedia-tkg/plots/plot_snap_overtime.py` is the plot for the `SnapshotEval` `yearlyTripleDiffStats` output, together with the per-snapshot summaries. Input paths are hard-coded. |
| `kgflex-eval/` | experimental | DBpedia fetch scripts for KGFlex-style film/person/company extracts. README still has open TODOs. |
| `dexter-kg/generate.py` | experimental | Uses `SourceConfig` and `GradoopSource`. |
| `examples/dexter_kg_mapping.py` | experimental | Uses the current Gradoop adapter. Hard-coded paths. |
| `examples/entity_resolution_benchset.py` | experimental | Standalone benchset sketch. Needs the `examples` extra (pandas). |
| `em-data-eval/abt-buy.py` | experimental | Subclasses `Source`. Pair labels still belong in `EntityResolutionData`. |
| `wikidata_externalids.py`, `uris.txt`, `predicates.txt` | experimental | Loose helpers and URI lists at the resources root. |
| `studies/experiments/rdf-part/` | experimental | Partitioning notes and a notebook. Outside the supported layout. |

## Datasets (`data/`)

Inputs and validated run outputs are registered under `data/<dataset_id>/`. Git tracks only `.odibel/` (`.gitignore` excludes payloads and `.odibel/cache/`). Each `.odibel/` has a `dataset.yaml` (identity, source or `derived_from` lineage, recipe, contract, limitations) and a `files.sha256`, and optionally `stats.json`, a run `report.md`, and the version files written by `pyodibel data commit` (`versions.yaml`, `versions/<n>.sha256`). Format and commands are documented in [Datasets](datasets.md). The format is a prototype (metadata version `0.1`). Generated KGI-Bench bundle cards stay on `kgi-bench-dbp-subonto`.

| Dataset | Kind | Role |
| --- | --- | --- |
| `dbpedia-2022.12.01-selected` | rdf-ntriples | Merged DBpedia 2022.12.01 selection; parent of the minidump |
| `dbpedia-2022.12.01-mini-food-celestial-politics-en-s42` | rdf-ntriples | English minidump (Food, CelestialBody, politics; seed 42) from `resources/dbpedia-slice/slice.sh` |
| `dbpedia-ontology-2022.12.05` | ontology-turtle | DBpedia ontology snapshot used for domain/range cleaning |
| `text2kgbench-v1` | benchmark-release | Upstream Text2KGBench release |
| `text2kgbench-v1-dbo` | ontology-turtle | Text2KGBench ontologies linked to `dbo:` by `align_ontologies.py` |
| `dbpedia-2022.12.01-multisource-politics-mini-en-s42` | run-output | [#20](https://github.com/ScaDS/ODIBEL/issues/20) validation run, with `report.md` |
| `text2kgbench-ont13-food-mini-en-s42` | run-output | [#21](https://github.com/ScaDS/ODIBEL/issues/21) validation run, with `report.md` |

All entries are `status: draft`. Versions cover payload file lists only; older payloads are not stored. `pyodibel data verify --metadata-only data/*/.odibel/..` checks every registered manifest without the payloads; all seven pass. The glob skips unregistered local folders such as `data/dbpedia/`.

## Open branches

The useful pieces of `origin/abs-cache` and `origin/kg-corruption` are commits on this branch. Those remote tips were not updated. `origin/kgi-bench-dbp-subonto` stays as it is; only its shared Spark and N-Triples fixes were copied here.

| Branch | Base | Commits ahead | Merge into `main` | Disposition |
| --- | --- | --- | --- | --- |
| `origin/abs-cache` (`65ea07b`, 2026-04-21) | `bd11a68`, 24 commits behind `main` | 7, all additive under `resources/abs-cache/` | clean | Copied onto this branch. Direct mode uses `rocksdict`. README says Python `>=3.10,<3.13`. Remote tip unchanged. |
| `origin/kg-corruption` (`d6364a7`, 2026-09-17) | `main` (`3cb3c0b`) | 1, additive `resources/kg-corruption/` | clean | Brought onto this branch, including the example graph. Stays outside the `pyodibel` import surface. The remote tip is unchanged. |
| `origin/kgi-bench-dbp-subonto` (`4982fe6`, 2026-09-17) | `main` (`3cb3c0b`) | 1, mixed: provenance package, domain runner, gap docs, plus edits to `spark_mgr.py` and `nt_io.py` | clean | Provenance package and domain runner stay on that branch. Its `spark_mgr` and `nt_io` fixes are already on this branch. Gap list: `resources/text2kgbench/docs/DATASET_GENERATION_GAPS.md` on that tip. |

Merged history that already settled structure: #1 TKG evaluation, #2 and #6 cleanup, #3 and #4 RDF/Gradoop, #8 and #9 schema graphs and the text2kgbench download note.

`get_spark_session` on this branch sets `master` only when there is no active `SparkContext`, uses `local[*]` outside `spark-submit`, and sets `spark.local.dir`. `nt_io` resolves a file or a Spark `write.text` directory, skipping `_*` and `.*` part names.

## Tests, packaging, and docs

Python tests live in `src/pyodibel/test/` (17 modules plus `conftest.py`). `pytest` is an optional extra (`test`), together with `pytest-mock`. Markers are registered in `pytest.ini` with `--strict-markers`: `network` (opt-in) and `spark` (starts a local JVM, needs Java 17). `conftest.py` rejects a test that uses the `spark` fixture without the `spark` marker. Spark fixtures fail instead of skipping when Spark cannot start ([#18](https://github.com/ScaDS/ODIBEL/issues/18)).

Packaging:

- `requires-python` is `>=3.10,<3.13`. Console script: `pyodibel = pyodibel.cli.main:cli`.
- Runtime dependencies that the tree imports: `pydantic`, `requests`, `rdflib`, `click`, `tqdm`, `pyspark`, `networkx`, `pyyaml`, `rocksdict`. `python-dotenv` is imported by the generators under `resources/`.
- `SQLAlchemy` was declared and unused; removed in [#17](https://github.com/ScaDS/ODIBEL/issues/17).
- `pandas` is the `examples` extra, used only by `resources/examples/entity_resolution_benchset.py`.
- `uv.lock` installs PySpark 4.1.2. `pom.xml` pins Spark 3.5.1 for the Scala jobs. They stay separate runtimes: the Python code starts its own session and does not load the Scala jar. The pin `pyspark>=3.5.7` is unchanged; the non-network suite passes on PySpark 3.5.7 and 4.1.2 with Java 17 (see [Final test results](#final-test-results)). PySpark 4 needs Java 17 or newer.
- `pyodibel.test` is excluded from the wheel ([#17](https://github.com/ScaDS/ODIBEL/issues/17)). Tests run from the checkout.
- CI: `.github/workflows/docs.yml` builds MkDocs on `main`. `.github/workflows/tests.yml` runs on pushes to `main` and on pull requests, with two jobs: a smoke job without Java (`-m "not network and not spark"`) and a Java 17 job (`-m "not network"`). Both use `uv sync --locked --extra test`.
- Scala `mvn test` is not covered by CI; its coverage is a separate decision. The Dockerfile runs `mvn package` on Java 17.

Docs:

| Doc | Status |
| --- | --- |
| `README.md` | Aligned in this branch with the layout and the supported entry points. |
| `docs/kgi_data.md` | Supported operator doc for the text2kgbench RDF path. |
| `docs/rdf2.md` | Supported `rDF2` guide. |
| `docs/datasets.md` | `.odibel` metadata format, `pyodibel data` commands, versions, and minidumps. Added in this branch. |
| `docs/api.md` | Aligned in this branch. The previous page was an unimplemented design sketch; that sketch is kept at the bottom. |
| `docs/index.md` | Points here. |
| `resources/README.md` | Index of workflows, added in this branch. |

## Identifier stability

[#5](https://github.com/ScaDS/ODIBEL/issues/5) is unchanged. Unstable blank nodes are created in `src/main/scala/ai/scads/odibel/datasets/wikitext/transform/CSVToRDFReification.scala` with `_:b${System.nanoTime()}`. A commented copy of the same pattern is in `TKGModelMgr.scala`. `nt_io._term_as_nt` only renders blank nodes it reads back through rdflib. Fixing the generator belongs on a branch for #5.

## Smoke test

Representative check on this machine, Java 17 (`17.0.15-tem`), CPython from `uv` (`3.12.11`), `uv sync --extra test`. The default SDKMAN Java is 8, which is the wrong runtime for this stack; the commands below set `JAVA_HOME` explicitly.

```bash
export JAVA_HOME="$HOME/.sdkman/candidates/java/17.0.15-tem"
export PATH="$JAVA_HOME/bin:$PATH"
uv sync --extra test
uv run pytest src/pyodibel/test -m "not network" -q
uv run pyodibel --help
uv run pyodibel rdf --help
```

Recorded on 2026-11-25 with `uv.lock` PySpark 4.1.2:

| Command | Result |
| --- | --- |
| `uv sync --extra test` | Installed `pyodibel` 0.1.0 and the test extra into `.venv` (CPython 3.12.11). |
| `uv run pytest src/pyodibel/test -m "not network" -q` | First run: 107 passed, 11 failed, 1 deselected (`test_wikipedia_live_fetch`). All 11 failures were `TestCleanDomainRangeViolations`: the tests called `clean_domain_range_violations()` with no argument, and the method requires `ont_path`. |
| `uv run pytest src/pyodibel/test/test_rdf2_ontology_ops.py -q` after the test update | 11 passed. PySpark emitted 11 warnings: `Cannot infer the eval type from type hints` inside `clean_domain_range_violations`. |
| `uv run pyodibel --help` | Lists the `rdf` command. |
| `uv run pyodibel rdf --help` | Lists `run` and `schema-graph`. |
| `uv run mkdocs build` | Built `site/` with existing warnings in `kgi_data.md` for links that point at `resources/` (outside `docs/`). |

The test update passes the URL the existing rdflib patch already intercepts (`http://example.org/mini-ontology.ttl`). Production behavior of `clean_domain_range_violations` is unchanged. Combined with the untouched modules, the non-network suite is 118 passed and 1 deselected.

End-to-end generator runs were not part of this smoke test. They were run afterwards on the English minidump, with `validate_run.py` recomputing each step:

- `resources/dbpedia-multi-source-kg/generate.py` (the multi-source subgraph script, not the DBpedia temporal jobs under `src/main/scala/.../wikitext`): [#20](https://github.com/ScaDS/ODIBEL/issues/20), report in `data/dbpedia-2022.12.01-multisource-politics-mini-en-s42/.odibel/report.md`. Runs end to end and reruns give the same triples. Domain/range cleaning removes mostly valid triples ([#22](https://github.com/ScaDS/ODIBEL/issues/22)).
- `resources/text2kgbench` RDF path, ontology 13 (Food): [#21](https://github.com/ScaDS/ODIBEL/issues/21), report in `data/text2kgbench-ont13-food-mini-en-s42/.odibel/report.md`. All four steps match an independent recomputation. Shaded IRIs keep the DBpedia identity ([#23](https://github.com/ScaDS/ODIBEL/issues/23)), and entity list CSVs do not quote IRIs with commas ([#24](https://github.com/ScaDS/ODIBEL/issues/24)).

Both runs cover one domain at minidump scale and make no claim about full-dump counts or runtime. The findings are reported and not fixed on this branch. `resources/movie-multi-source-kg` was not run; MovieKG reproduction is [#12](https://github.com/ScaDS/ODIBEL/issues/12).

Scala `mvn test` was not run. The default `JAVA_HOME` on this machine points at Java 8; the commands above override it to 17.0.15-tem.

## Final test results

Recorded on 2026-11-26 at `43662ae`, with Java 17.0.15-tem and CPython 3.12.11. The suite includes the 12 dataset manifest and versioning tests (`test_dataset_manifest.py`, `test_dataset_versions.py`).

| Run | Command | Result |
| --- | --- | --- |
| Local, PySpark 4.1.2 (`uv.lock`) | `uv run pytest src/pyodibel/test -m "not network"` | 151 passed, 1 deselected (`test_wikipedia_live_fetch`), 11 known `Cannot infer the eval type` warnings |
| Local, PySpark 3.5.7 | `uv run --with pyspark==3.5.7 pytest src/pyodibel/test -m "not network"` | 151 passed, 1 deselected |
| Local, dataset metadata | `uv run pyodibel data verify --metadata-only data/*/.odibel/..` | 7 of 7 ok |

## Follow-ups

Items 1–4, 6–10, 12 and 14 are commits on this branch, and item 13 is run and reported here. Items 5 and 11 stay open.

1. **Spark session inheritance.** Done here. An explicit master is applied only when no `SparkContext` exists.
2. **Spark NT directories in `nt_io`.** Done here, with `test_nt_io.py`.
3. **Wikipedia abstract cache.** Done here under `resources/abs-cache`. Direct mode uses `rocksdict`. The README says Python `>=3.10,<3.13`.
4. **KG corruption package.** Done here under `resources/kg-corruption`, outside the `pyodibel` import surface. Includes the example graph.
5. **Leave KGI provenance on `kgi-bench-dbp-subonto`.** The gap checklist on that branch is the working list for multi-domain generation. MovieKG inputs, reproduction, reference audit, and entailment checks stay on #11–#14.
6. **Quarantine stale callers.** Done here for Dexter (`GradoopSource`), Abt-Buy (`Source`), and `resources/dbpedia-multi-source-kg/generate.py` (`ont_path`). The pandas import in `resources/examples/entity_resolution_benchset.py` is now the `examples` extra ([#17](https://github.com/ScaDS/ODIBEL/issues/17)). Running that DBpedia generator is follow-up 13, not this item.
7. **Make `systemkg` importable.** Done in [#16](https://github.com/ScaDS/ODIBEL/issues/16): the `kgcore` import is guarded and `require_kgcore()` raises a clear error. Original item: move the `kgcore` import behind an optional extra, or drop the module until the dependency exists.
8. **Trim unused dependencies and package tests.** Done in [#17](https://github.com/ScaDS/ODIBEL/issues/17): SQLAlchemy removed, tests excluded from the wheel, Spark runtimes kept separate and documented above. Original item: `SQLAlchemy` has no caller. Exclude `pyodibel.test` from the installed distribution. Decide whether the Python lock should stay on PySpark 4.1.2 while the Scala build stays on Spark 3.5.1.
9. **Add a Python test workflow.** Done in [#18](https://github.com/ScaDS/ODIBEL/issues/18): `tests.yml` with smoke and Spark jobs, `spark` marker registered. Scala `mvn test` stays out of CI. Original item: Run `pytest -m "not network"` on Java 17. Mark Spark tests so a non-Spark smoke path is explicit.
10. **Stop advertising missing adapters.** Done in [#16](https://github.com/ScaDS/ODIBEL/issues/16): the symbols are removed from `pyodibel/__init__.py` and logging is opt-in via `setup_logging()`. Original item: `WikidataDumpSource`, `LinkedDataSource`, storage, and partition symbols in `pyodibel/__init__.py` do not exist. `setup_logging()` on import writes `pyodibel.log` as a side effect.
11. **Blank-node IDs.** Track on #5 only.
12. **Remove or fill empty placeholders.** Done in [#19](https://github.com/ScaDS/ODIBEL/issues/19): the empty product README, the `docs/Generate.md` stub and both yearly-diff placeholders are removed; generation is documented in `resources/README.md` and [KGI-Bench data](kgi_data.md). Original item: `resources/product-multi-source-kg/`, `docs/Generate.md`, and the duplicate yearly-diff plot scripts under `resources/dbpedia-temporal`.
13. **Run the dataset generators as their own issue.** Tracked in [#20](https://github.com/ScaDS/ODIBEL/issues/20) (DBpedia multi-source generator) and [#21](https://github.com/ScaDS/ODIBEL/issues/21) (text2kgbench RDF path). Both ran end to end on a registered minidump; reports and validation scripts are on this branch. Their findings are separate issues: [#22](https://github.com/ScaDS/ODIBEL/issues/22), [#23](https://github.com/ScaDS/ODIBEL/issues/23), [#24](https://github.com/ScaDS/ODIBEL/issues/24). MovieKG reproduction stays on [#12](https://github.com/ScaDS/ODIBEL/issues/12). The DBpedia temporal Scala pipeline is separate and was not part of this check.
14. **Register datasets.** Done here: `.odibel` metadata, `pyodibel data hash|verify|status|commit|log`, and seven registered datasets under `data/`. Bundle cards and the provenance registry stay on `kgi-bench-dbp-subonto`.
