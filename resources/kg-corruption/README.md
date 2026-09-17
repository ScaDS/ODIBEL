# Controlled KG corruption benchmark

Omission-only generator for **source-aligned root reference** knowledge graphs.
It produces corrupted sub-reference graphs with known coverage loss so a
source-grounded coverage estimator can be evaluated later. This package does
**not** run SGE, does not inject false triples, and does not invent provenance.

Specification: [`IDEA_controlled_kg_corruption_benchmark.md`](IDEA_controlled_kg_corruption_benchmark.md)

## Layout

```text
resources/kg-corruption/
├── configs/corruption.example.yaml
├── data/example/
│   ├── ontology.ttl          # complete OWL class and property vocabulary
│   ├── root_reference.ttl    # TBox + instance graph
│   ├── provenance.jsonl
│   ├── movie1.txt            # Wikipedia-style abstract for One / Movie 1
│   └── movie2.txt            # Wikipedia-style abstract for Two / Movie 2
├── src/kg_corrupt/
│   └── corruptions/          # one module per family
└── tests/
```

## Setup

Use the repository-root virtualenv:

```bash
# from the ODIBEL repo root
.venv/bin/pip install -e resources/kg-corruption
.venv/bin/python -m pytest resources/kg-corruption/tests
```

Or from this directory:

```bash
../../.venv/bin/pip install -e .
../../.venv/bin/python -m pytest
```

## CLI

```bash
kg-corrupt generate --config configs/corruption.example.yaml
kg-corrupt generate --config configs/corruption.example.yaml --family entity
kg-corrupt generate-one \
  --root-reference data/example/root_reference.ttl \
  --family long_tail \
  --remove-fraction 0.30 \
  --seed 42 \
  --output /tmp/out.nt
kg-corrupt stats --root-reference data/example/root_reference.ttl
kg-corrupt validate --manifest outputs/example-corruptions/benchmark_manifest.csv
kg-corrupt summarize --manifest outputs/example-corruptions/benchmark_manifest.csv
```

## Defaults

| Key | Default |
| --- | --- |
| `benchmark.nested` | `false` (independent samples per fraction) |
| `benchmark.seeds` | `1..10` |
| `benchmark.removal_fractions` | `0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70` |
| `eligibility.protected_predicates` | `rdfs:label` |
| `eligibility.protect_schema_triples` | `true` (OWL predicates; RDF/RDFS structural predicates; `rdf:type` of RDF/RDFS/OWL classes) |
| `eligibility.protect_blank_node_scaffolding` | `false` |
| `eligibility.allow_blank_seed_entities` | `false` |
| `families.entity.mode` | `incident` |
| `families.predicate.mode` | `whole_predicates` |
| `families.long_tail.basis` | `predicate_frequency` |
| `families.long_tail.alpha` | `1.0` |
| `families.source.enabled` | `false` until a provenance file or named graphs are supplied |
| `families.source.multi_source_policy` | `remove_if_all_selected` |
| `families.subgraph.traversal` | `undirected` |
| `families.subgraph.max_hops` | `2` |

Protected predicates and namespaces are configurable. Structural RDF lists and
reification are **not** auto-repaired; exclude them with `protected_predicates`
or `protected_namespaces` when needed.

## Source provenance

The source/document family requires an explicit `triple → source_unit_id`
mapping. It is **not** inferred from URIs.

JSONL sidecar (N-Triples-like terms):

```json
{"s": "http://example.org/alice", "p": "http://example.org/knows", "o": "http://example.org/bob", "source_id": "doc_001"}
```

If the root graph is TriG or N-Quads, named graph URIs can be used as source
ids (`families.source.named_graph_as_source: true`). Dataset-specific provenance
extraction is out of scope for v1: prepare a sidecar first.

If the family is enabled and no mapping covers any eligible triple, generation
fails with a clear error.

## Output

Each run writes:

- `root_stats.json` — root graph statistics
- `config.yaml` / `reproducibility.json` — exact config and code/data hashes
- `benchmark_manifest.csv` and `.jsonl`
- `benchmark_summary.json`
- `{family}/remove_{pct}/seed_{seed}.nt` plus `.meta.json` and `.removed.nt`

`reference_coverage` is computed over eligible evaluation triples only.
Downstream evaluation must use the **realized** coverage, not the target.

## Non-goals (v1)

False-triple injection, predicate replacement, object swapping, SGE execution,
LLM calls, graph repair, and ontology reasoning.
