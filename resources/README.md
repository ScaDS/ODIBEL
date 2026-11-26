# Dataset workflows

Scripts in this directory are not part of the installed `pyodibel` package. Run them from the repository root with `uv run`. Status and follow-ups are in [docs/inventory.md](../docs/inventory.md).

| Directory | Status | Contents |
| --- | --- | --- |
| `text2kgbench/` | supported | DBpedia + Text2KGBench ontology to KGI-Bench bundles. Operator doc: `docs/kgi_data.md`. |
| `movie-multi-source-kg/` | experimental | Movie KG generator and notes. Reproduction is tracked in GitHub issues #11–#14. |
| `dbpedia-multi-source-kg/` | experimental | DBpedia ontology class-to-subdomain classifier. See its own README. |
| `dbpedia-temporal/` | experimental | Temporal KG model note and plot scripts. Extraction runs are the Scala jobs under `src/main/scala/`. Yearly triple diffs (`SnapshotEval` `yearlyTripleDiffStats`) are plotted by `python/dbpedia-tkg/plots/plot_snap_overtime.py`; input paths are hard-coded. |
| `kgflex-eval/` | experimental | DBpedia fetch helpers for film, person, and company extracts. |
| `dexter-kg/` | experimental | Gradoop-to-RDF script. Imports removed API names (`GradoopDataSource`). |
| `examples/` | experimental | `dexter_kg_mapping.py` uses the current `GradoopSource`. `entity_resolution_benchset.py` needs pandas: `uv sync --extra examples`. |
| `em-data-eval/` | experimental | Abt-Buy sketch. Imports classes that are not in the library. |

Loose files at this level (`wikidata_externalids.py`, `uris.txt`, `predicates.txt`, `execute_post_fetch.sh`) are helpers for the workflows above.
