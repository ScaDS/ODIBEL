# Domain selection for additional KGI-Bench datasets

MovieKG (film) is a **creative-work hub**: films linked to many persons/companies via ambiguous roles, with narrative abstracts and mixed literal formats (runtime, budget).

We add two Text2KGBench DBpedia-WebNLG domains that stress different integration characteristics:

| Domain | Ontology | Root class | Why it differs from MovieKG |
|--------|----------|------------|-----------------------------|
| **Airport** | `3_airport_ontology.ttl` | `dbo:Airport` | Infrastructure / geo: admin place hierarchy (city, country, county), facility attributes, identifier-like runway/surface relations — less person-role fusion, more place linking and structured facility facts |
| **CelestialBody** | `8_celestialbody_ontology.ttl` | `dbo:CelestialBody` | Scientific / numeric-heavy: magnitudes, apoapsis, average speed, etc. (often mistyped as object props to `number` in Text2KGBench) — stresses literal/datatype handling more than social ER |

Sizes (first pass): **100 root entities** per domain (`*_100`), same split recipe as MovieKG small (`num_subsets=4`, `overlap_ratio=0.04`, `subset_size=25`), RDF path only (seed + shaded RDF). JSON/text come later.

Outputs under:

```
/local/d1/datasets/kgi-bench-text2kgbench/
  registry.json
  subgraphs/ont_3_subgraph/ ...
  subgraphs/ont_8_subgraph/ ...
  datasets/<dataset_id>/          # e.g. airport_n100_ov04_<hash>/
    generation_config.yaml
    dataset_card.json
    split_*/...
```

See [`PROVENANCE.md`](PROVENANCE.md) for dataset identity and cards.
