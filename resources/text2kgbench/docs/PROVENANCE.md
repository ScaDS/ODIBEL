# KGI-Bench Dataset Provenance and Parameterized Cards

This document specifies how generated multi-domain KGI-Bench bundles are identified,
recorded, and linked to integration experiments.

Schemas:

- [`schemas/generation_config.schema.json`](schemas/generation_config.schema.json)
- [`schemas/dataset_card.schema.json`](schemas/dataset_card.schema.json)
- [`schemas/experiment.schema.json`](schemas/experiment.schema.json)

Python API: `pyodibel.datasets.kgi_provenance`.

## Identity model

Three layers:

1. **`generation_config.yaml`** — declarative recipe (inputs + recipe knobs + software).
2. **`dataset_id`** — hybrid name: `{domain}_n{budget}_ov{XX}_{hash6}`.
3. **`dataset_card.json`** — human/machine summary including measured outcomes.

Identity hash covers **inputs + recipe only** (canonical JSON, sorted keys). Paths are
stored as **content SHA-256** checksums, not absolute paths. Software provenance and
measured stats are recorded but do not change the id.

```text
canonicalize(inputs + recipe) → sha256 → first 6 hex chars
dataset_id = f"{domain}_n{target_root_budget}_ov{int(round(overlap*100)):02d}_{hash}"
```

Display projection (tables/papers):

```text
Airport | n≈100 | ov=0.04 | hops=∞ | RDF-only
```

## Layout

```text
$KGI_DATA_ROOT/kgi-bench-text2kgbench/
  registry.json
  datasets/
    airport_n100_ov04_a3f91c/
      generation_config.yaml
      dataset_card.json
      quality_report.json          # optional; folded into card.measured
      ontology.ttl                 # optional copy
      split_0/ ...
  subgraphs/                       # shared intermediates (own ids later)
```

`run_domain.py` writes the final bundle under `datasets/<dataset_id>/` and upserts
`registry.json`.

## Quality gate status

Cards use `status`:

| Status | Meaning |
|--------|---------|
| `draft` | Generated; gates missing or failed; OK for debugging |
| `published` | Gates passed; suitable for integration studies |
| `deprecated` | Superseded by another `dataset_id` |

Gate failures do **not** block writing a card; they keep `status=draft` and record
details under `measured.quality_gate`.

## Variant comparison matrix (first wave)

Vary **one primary axis** at a time. Baseline recipe for new domains:

- `n=100` (`subset_size=25`, `num_subsets=4`, `overlap_ratio=0.04`, `seed=42`)
- `max_hops=null` (unbounded forward)
- formats: RDF-only until JSON/text bundlers land
- language: `@en` when cleaning is applied

| Axis | Baseline | Variants | Expected integration effect |
|------|----------|----------|-----------------------------|
| Domain | film (MovieKG legacy) | airport, celestialbody | schema/role vs geo/infra vs numeric/literal hardness |
| Overlap | `ov=0.04` | `0.10`, `0.20` | ER difficulty / duplicate pressure |
| Hops | unbounded | `1`, `2` | neighborhood size; fusion ambiguity |
| Size | `n=100` | `1000` | scalability / LLM cost |
| Formats | RDF-only | +JSON, +text, RDF+JSON+text | KE/mapping load; MSP vs SSP |
| Cleaning | `@en` | multilingual literals | literal matching / coverage |

Suggested first study pairs (same dump + ontology family):

1. `airport_n100_ov04_*` vs `airport_n100_ov10_*` (overlap only)
2. `celestialbody_n100_ov04_*` hops=∞ vs hops=1
3. Cross-domain at fixed recipe: airport vs celestialbody vs film_100 (legacy card)

## Experiment join

Every KGpipe / integration run should emit `experiment.json` (see schema) with:

- `dataset_id` — FK into `registry.json`
- `pipeline.pipeline_id` / setting / source order
- `metrics` — KGI coverage, correctness, consistency, auxiliaries

Analysis joins `experiment.dataset_id` → `dataset_card` so score deltas can be attributed
to domain semantics vs structural recipe (overlap, hops, missing text, …).

Example:

```json
{
  "schema_version": "1.0.0",
  "experiment_id": "exp_rdfbase_airport_001",
  "dataset_id": "airport_n100_ov04_a3f91c",
  "pipeline": {
    "pipeline_id": "RDFbase",
    "setting": "SSP",
    "source_order": ["rdf"],
    "tool": "KGpipe"
  },
  "metrics": {
    "Cov_E": 0.81,
    "Cov_T": 0.74,
    "Crct_E": 0.88,
    "Crct_T": 0.79
  },
  "created_at": "2026-08-11T12:00:00+00:00"
}
```

## MovieKG legacy

Released MovieKG (`film_100` / `film_1k` / `film_10k`) can be registered with
`legacy: true` and partial provenance until a full regen under this scheme exists.
