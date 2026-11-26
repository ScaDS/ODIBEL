# Run report: Text2KGBench RDF generation, ontology 13 (Food), on the English minidump

Issue: [#21](https://github.com/ScaDS/ODIBEL/issues/21). Recorded 2026-11-26 at commit `6f549a9`.

**Result:** the documented RDF-only flow (`generate_refac.py`, `subgraph.py`, `splits.py`, `sources.py`) runs end to end for the Food domain on a pinned minidump. Every step matches an independent recomputation, seed overlap is exactly as configured, and the shaded sources contain no DBpedia IRIs. Two findings are reported and left unfixed: shaded IRIs keep the md5 of the DBpedia IRI, so entities can be matched across sources by local name, and the single-column `entities.csv` files do not quote IRIs that contain commas.

## Inputs

| Dataset | Payload | sha256 |
| --- | --- | --- |
| `dbpedia-2022.12.01-mini-food-celestial-politics-en-s42` | `data.nt.bz2`, 99,358 triples | `ddf6c18e…a9bfb` |
| `text2kgbench-v1-dbo` | `ontologies/13_food_ontology.ttl`: 8 dbo classes, 22 dbo properties | `797b07c6…fb574` |

`text2kgbench-v1-dbo` links the upstream Text2KGBench ontologies to the DBpedia ontology by name (`resources/text2kgbench/align_ontologies.py`); the original kgpipe ontologies were not available. Its lineage is in its `dataset.yaml`.

## Run

Commands are in `dataset.yaml` (`recipe.commands`). Parameters: main class `dbo:Food`, 4 splits of 100 seeds, overlap ratio 0.04, seed 42, unlimited forward hops, `main_class_scope=reachable`, memory engine for steps 2 and 3. CPython 3.12.11, PySpark 4.1.2 (step 1), OpenJDK 17.0.15. Wall time 19 s, all exit codes 0. The `@en` filter of step 1b is a no-op because the input is English-only.

Validation (`stats.json`):

```bash
uv run resources/text2kgbench/validate_run.py data/text2kgbench-ont13-food-mini-en-s42 \
  data/dbpedia-2022.12.01-mini-food-celestial-politics-en-s42/data.nt.bz2 \
  data/text2kgbench-v1-dbo/ontologies/13_food_ontology.ttl \
  --main-class dbo:Food --subset-size 100
```

## Step results

| Step | Output | Check | Result |
| --- | --- | --- | --- |
| 1 `generate_refac.py` | 8,545 triples (789 Food, 1,469 Place, 364 Person, 140 Country, 116 City, 21 EthnicGroup, 1 Language) | Equals a recount: ontology properties plus `rdf:type`/`rdfs:label`, subjects and IRI objects in the ontology classes | pass |
| 2 `subgraph.py` | 5,013 triples, 1,096 entities | Equals a forward reachability recount from all `dbo:Food` entities | pass |
| 3 `splits.py` | 789 master entities; 4 splits | Master list equals the Food entities; seeds are Food entities; each reference equals reachability from its seeds; `data_agg.nt` equals the union of splits 0..N | pass |
| 4 `sources.py` | seed and RDF source per split | Equal to an independent shading of the reference (md5 resource IRIs, `kg.org` ontology namespace, `skos:altLabel` copy of each label); no `dbpedia.org` IRIs | pass |

| Split | Seeds | Reference entities | Food entities | Reference triples | Seed / RDF source triples |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 100 | 276 | 130 | 1,389 | 1,760 |
| 1 | 100 | 285 | 140 | 1,418 | 1,800 |
| 2 | 100 | 265 | 131 | 1,387 | 1,750 |
| 3 | 100 | 255 | 128 | 1,273 | 1,634 |

Sources have more triples than the reference because each `rdfs:label` is copied to `skos:altLabel`.

## Overlap

| Measure | Configured | Measured (pairwise, 6 pairs) |
| --- | --- | --- |
| Shared seeds | 0.04 × 100 = 4 | 4 for every pair |
| Shared Food entities in reference graphs | not controlled | 14.8 % to 22.1 % of the smaller split |
| Shared entities in reference graphs | not controlled | 43.5 % to 49.8 % of the smaller split |

`overlap_ratio` only controls the seeds. With `main_class_scope=reachable`, Food entities reached through `dbo:ingredient` and the shared places and countries raise the actual overlap well above 4 %. `--main-class-scope seeds` limits Food entities to the seeds; it was not run here.

## Findings

### Shaded IRIs keep the DBpedia identity

Both sources hash the original DBpedia IRI: the seed KG uses `http://kg.org/resource/{md5}` and the RDF source of split N uses `http://kg.org/rdf/N/resource/{md5}`. The local name is the same md5 in both:

| Measure | Value |
| --- | --- |
| Seed vs RDF source local names, same split | 100 % shared in all 4 splits |
| RDF sources across splits | 115 to 132 shared local names per pair (all shared entities) |

An integration system can align entities across sources by comparing local names, without using the data. Whether this is intended (as in the MovieKG post-processing that `shade_reference_graph` mirrors) or should use per-source salts is open.

### Entity lists are not valid CSV for IRIs with commas

`splits/entities/master_entities.csv` and `split_N/index/entities.csv` are written as a header plus one raw IRI per line. Two Food IRIs contain commas (`dbr:Kadyos,_baboy,_kag_langka`, `dbr:Kadyos,_manok,_kag_ubad`), so a CSV reader splits them. `verified_entities.csv` is tab-separated and unaffected.

## Determinism

A second run of steps 2 to 4 from the same `property_filter.nt` produced the same content in every file. Line order differs in `entities.csv`, `data_agg.nt`, and the shaded sources (set iteration order). `stats.json` records an order-independent `reference_content_sha256` per split.

## Relation to the generation gaps checklist

From [DATASET_GENERATION_GAPS.md](https://github.com/ScaDS/ODIBEL/blob/kgi-bench-dbp-subonto/resources/text2kgbench/docs/DATASET_GENERATION_GAPS.md) (branch `kgi-bench-dbp-subonto`), this run covers the RDF path only:

- Dump version and checksum (gap 3.1): recorded through the dataset lineage, with the 2022.12.01 release inferred from the source directory.
- Measured overlap written to disk: `stats.json`.
- Not covered: JSON and text sources, gold beyond `verified_entities.csv`, packaging, provenance package and domain runner (kept on that branch).

## Limitations

- Minidump scale and one domain only; no claim about full-dump counts or runtime.
- Ontology alignment is a name-matching reconstruction (4 of 12 Food classes and 2 of 24 relations unlinked; see `text2kgbench-v1-dbo/.odibel/stats.json`).
- The Spark engine of steps 2 and 3 was not exercised.
