# Run report: DBpedia multi-source generator on the English minidump

Issue: [#20](https://github.com/ScaDS/ODIBEL/issues/20). Recorded 2026-11-26 at commit `6f549a9`.

**Result:** `generate.py` runs end to end on a pinned minidump. Selection, deduplication, type cleaning, object-subject filtering, and the schema graphs behave as specified, and a rerun gives the same triples. `clean_domain_range_violations` removes mostly valid triples (4,713 of 5,186 removals are false positives). That is reported here and left unfixed.

## Inputs

| Dataset | Payload | sha256 |
| --- | --- | --- |
| `dbpedia-2022.12.01-mini-food-celestial-politics-en-s42` | `data.nt.bz2`, 99,358 triples, 4,662 entities | `ddf6c18e…a9bfb` |
| `dbpedia-ontology-2022.12.05` | `dbo-snapshots.ttl` | `8bc79b50…04596` |

The minidump is an entity-closed slice of `dbpedia-2022.12.01-selected`: 500 seeds per group (Food, CelestialBody, the 12 politics classes of `generate_politics.py`), seed 42, one hop, only `@en` and untagged literals. Full hashes and lineage are in `dataset.yaml` of each dataset.

## Run

```bash
export JAVA_HOME=<Java 17>
cd resources/dbpedia-multi-source-kg
INPUT_PATH=../../data/dbpedia-2022.12.01-mini-food-celestial-politics-en-s42/data.nt.bz2 \
OUTPUT_PATH=../../data/dbpedia-2022.12.01-multisource-politics-mini-en-s42 \
ONTOLOGY_PATH=../../data/dbpedia-ontology-2022.12.05/dbo-snapshots.ttl \
uv run python generate_politics.py
```

CPython 3.12.11, PySpark 4.1.2, OpenJDK 17.0.15, `local[*]`. Wall time 17 s, exit code 0, no exceptions in the log.

Validation (`stats.json`):

```bash
uv run resources/dbpedia-multi-source-kg/validate_run.py \
  data/dbpedia-2022.12.01-multisource-politics-mini-en-s42 \
  data/dbpedia-2022.12.01-mini-food-celestial-politics-en-s42/data.nt.bz2 \
  data/dbpedia-ontology-2022.12.05/dbo-snapshots.ttl \
  resources/dbpedia-multi-source-kg/generate_politics.py
```

## Step results

| Step | Triples | Check | Result |
| --- | --- | --- | --- |
| `selected` | 20,822 | Equals an independent recount (subjects typed with a politics class) | pass |
| `distinct` | 20,822 | No duplicates | pass |
| `consistent` | 15,636 | Subset of `distinct`; removals classified below | see finding |
| `cleaned_types` | 7,464 | Non-type triples unchanged; only the 12 politics classes as `rdf:type` objects | pass |
| `cleaned_subgraph` | 6,441 | No IRI object outside the subject set, except `rdf:type` | pass |
| schema graphs | 114 / 78 / 16 rows | Written | pass |

Final graph types: Election 539, PoliticalParty 328, GovernmentAgency 89, LegalCase 52, Legislature 43, SupremeCourtOfTheUnitedStatesCase 22, Ideology 3.

The `_en` variant is identical to the unfiltered one because the input is English-only. A second run into `/tmp` produced the same `content_sha256` for every step and variant. `files.sha256` differs between runs because Spark part-file names are random.

## Finding: domain/range cleaning removes valid triples

`rDF2.clean_domain_range_violations` treats an object without an `rdf:type` in its input as an instance of a class named after its own IRI, so any declared range fails. In `generate.py` the cleaning runs after `filter_triples_by_s_types`, which has already dropped the type triples of objects outside the domain (countries, persons, images). This affects full-dump runs in the same way.

| Removed | Reason | Example | Verdict |
| ---: | --- | --- | --- |
| 2,338 | Object type dropped by the earlier subject filter | `dbo:country dbr:United_States` (range `dbo:Country`) | false positive |
| 2,133 | Object untyped in DBpedia | `dbo:thumbnail <commons…/Flag_of_….svg>` (range `dbo:Image`) | false positive |
| 242 | Untagged literal vs `xsd:string` range | `dbr:Civic_Platform dbo:logo "http://commons…"` | false positive (RDF 1.1: untagged literal is `xsd:string`) |
| 410 | Subject type not in `rdfs:domain` | `dbr:New_Democratic_Party dbo:member "https://wikidata.dbpedia.org/resource/Q159454"` (domain `dbo:Person` in this snapshot) | violation per ontology |
| 48 | Object typed outside the range | `dbr:2004_United_States_Senate_elections dbo:country dbr:2004_United_States_Senate_election_in_Alabama` (an Election) | real violation |
| 15 | DBpedia unit datatype vs XSD range | `dbo:budget "2.9E9"^^dbt:poundSterling` (range `xsd:double`) | debatable |

Most affected predicates: `dbo:country` 1,168, `dbo:ideology` 987, `dbo:thumbnail` 857, `dbo:headquarter` 393. Every `dbo:country` edge of the domain disappears.

Possible fixes, not applied: take object types from the unfiltered input, treat untyped objects as unknown instead of violating (open world), and accept untagged literals for `xsd:string`.

## Rules the pipeline does not check

The ontology declares 55 `owl:disjointWith` and 31 `owl:FunctionalProperty` axioms; the cleaning step ignores both.

| Axiom | `distinct` | `cleaned_subgraph` |
| --- | --- | --- |
| Entities with two disjoint classes | 1 (`dbr:South_Korea`: `dbo:Agent` and `dbo:Place`) | 0 |
| Functional properties with more than one value | 0 | 0 |

`cleaned_subgraph` keeps only politics classes as types, so disjointness can no longer be observed there.

## Limitations

- Minidump scale only; no claim about full-dump counts or runtime.
- The language filter (`_en` variant) is not exercised.
- The 2022.12.05 ontology snapshot contains 781 spurious Urdu-named classes (see its `dataset.yaml`); they match no data.
- Output folders are named `*.nt.bz2` but are Spark part directories with plain text.
