# IDEA.md — Controlled Corruption Benchmark for Source-Grounded KG Coverage

## 1. Goal

Implement a benchmark generator that takes a **source-aligned root reference knowledge graph** and produces many corrupted **sub-reference graphs** with known, controlled information loss.

The generator is intended to evaluate whether a source-grounded coverage estimator preserves the ordering and magnitude of KG coverage loss without relying on real pipeline outputs.

The benchmark must support six corruption families:

1. Random fact removal
2. Entity-centric removal
3. Predicate-centric removal
4. Long-tail / frequency-biased removal
5. Source/document-centric removal
6. Cluster/subgraph removal

The implementation must be deterministic, reproducible, configurable, and produce detailed metadata for every generated graph.

---

## 2. Core assumption

The input `root_reference` is already restricted to facts that are supported by the source material.

Therefore, this implementation **does not perform source verification** and does not attempt to determine whether a root-reference fact is source-supported.

Formally:

```text
K_root = source-aligned root reference KG
K_corrupt ⊆ K_root
```

For the main benchmark, corruption is omission-only:

```text
K_corrupt = K_root \ D
```

where `D` is the set of removed triples.

No false triples are added. Consequently, all retained triples remain valid root-reference facts, and only coverage is varied.

---

## 3. Research objective represented by the benchmark

For each generated graph `K_i`, we know the exact retained reference coverage:

```text
reference_coverage(K_i) = |K_i ∩ K_root| / |K_root|
                        = |K_i| / |K_root|
```

when all root triples are eligible for corruption.

If protected/non-evaluation triples exist, compute coverage only over the eligible evaluation triple set:

```text
T_eval = eligible evaluation triples in K_root

reference_coverage(K_i)
    = |T_eval ∩ K_i| / |T_eval|
```

The benchmark will later be evaluated independently using SGE:

```text
x_i = reference_coverage(K_i)
y_i = sge_coverage(K_i, Sources)
```

The benchmark generator itself must **not** contain SGE logic.

---

# 4. Design principles

## 4.1 Do not generate one simple corruption chain only

Avoid generating only:

```text
100% → 90% → 80% → 70% → ...
```

from one fixed sequence of deleted triples.

For every:

- corruption family,
- target corruption level,
- random seed,

generate an independent corruption instance unless explicitly configured otherwise.

Example:

```text
6 families
× 8 corruption levels
× 20 seeds
= 960 corrupted graphs
```

This enables distributions, confidence intervals, and robust ranking analyses.

---

## 4.2 Preserve the root KG

Never modify the input graph in place.

Every generated graph must be derived from an immutable root graph.

---

## 4.3 Record both target and realized corruption

Some structured corruption units may contain multiple triples, so an exact requested deletion percentage may not always be achievable.

For every graph record:

```text
target_removed_fraction
realized_removed_fraction
target_removed_triples
realized_removed_triples
```

The realized value is the authoritative ground truth.

---

## 4.4 Protect non-evaluation triples

The implementation must support a configurable set of triples/predicates that are never removed.

Examples may include:

- `rdfs:label`
- ontology/schema declarations
- provenance metadata
- helper triples required for serialization or matching

Do **not** hard-code the protected set. Provide sensible defaults but allow configuration.

The generator should distinguish:

```text
T_all       = all triples in root graph
T_protected = triples excluded from corruption
T_eval      = T_all \ T_protected
```

Only `T_eval` contributes to reference coverage.

---

# 5. Suggested repository structure

```text
corruption_benchmark/
├── IDEA.md
├── pyproject.toml
├── README.md
├── configs/
│   └── corruption.example.yaml
├── src/
│   └── kg_corrupt/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── io.py
│       ├── model.py
│       ├── eligibility.py
│       ├── generator.py
│       ├── manifest.py
│       ├── statistics.py
│       └── corruptions/
│           ├── __init__.py
│           ├── base.py
│           ├── random_fact.py
│           ├── entity.py
│           ├── predicate.py
│           ├── long_tail.py
│           ├── source.py
│           └── subgraph.py
└── tests/
    ├── test_random_fact.py
    ├── test_entity.py
    ├── test_predicate.py
    ├── test_long_tail.py
    ├── test_source.py
    ├── test_subgraph.py
    ├── test_reproducibility.py
    └── test_manifest.py
```

Use Python and `rdflib` unless the existing project already has a preferred RDF stack.

---

# 6. Input specification

Minimum required input:

```text
--root-reference path/to/root_reference.ttl
```

Supported RDF serializations should include at least:

- Turtle
- N-Triples
- RDF/XML
- TriG / N-Quads if provenance uses named graphs

Optional inputs:

```text
--config configs/corruption.yaml
--provenance path/to/provenance.jsonl
```

The root graph should be loaded once and reused across all corruption runs.

---

# 7. Configuration

Use a YAML configuration file.

Example:

```yaml
input:
  root_reference: data/root_reference.ttl
  format: turtle

output:
  directory: outputs/corruptions
  serialization: nt
  write_removed_triples: true
  write_retained_triples: false

benchmark:
  seeds: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  removal_fractions: [0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70]

eligibility:
  protected_predicates:
    - "http://www.w3.org/2000/01/rdf-schema#label"
  protect_schema_triples: true
  protect_blank_node_scaffolding: false

families:
  random_fact:
    enabled: true

  entity:
    enabled: true
    mode: incident
    entity_sampling: uniform

  predicate:
    enabled: true
    mode: whole_predicates

  long_tail:
    enabled: true
    basis: predicate_frequency
    alpha: 1.0

  source:
    enabled: true
    provenance_file: data/triple_provenance.jsonl

  subgraph:
    enabled: true
    traversal: undirected
    max_hops: 2
    seed_entity_sampling: uniform
```

All defaults must be documented.

---

# 8. Common corruption API

Define a common interface for all corruption families.

Pseudo-interface:

```python
class CorruptionStrategy(Protocol):
    name: str

    def corrupt(
        self,
        graph: Graph,
        eligible_triples: Sequence[Triple],
        target_removed_count: int,
        rng: random.Random,
        context: CorruptionContext,
    ) -> CorruptionResult:
        ...
```

Return:

```python
@dataclass
class CorruptionResult:
    retained_graph: Graph
    removed_triples: set[Triple]
    metadata: dict
```

Every strategy must receive its random generator explicitly.

Never use uncontrolled global randomness.

---

# 9. Family 1 — Random fact removal

## Purpose

Baseline corruption that simulates independent, uniformly distributed omissions.

## Algorithm

Given:

```text
N = |T_eval|
m = round(removal_fraction × N)
```

sample exactly `m` eligible triples uniformly without replacement.

```python
removed = rng.sample(eligible_triples, m)
```

Then:

```text
K_corrupt = K_root \ removed
```

## Required metadata

```json
{
  "family": "random_fact",
  "sampling": "uniform_without_replacement",
  "target_removed_count": 1000,
  "realized_removed_count": 1000
}
```

## Important

Each `(fraction, seed)` must be independently sampled by default.

Optionally support a `nested: true` mode for controlled monotonic experiments, but it must **not** be the default.

---

# 10. Family 2 — Entity-centric removal

## Purpose

Simulate failures where a construction system fails to extract/link/integrate particular entities and therefore loses groups of facts together.

## Entity universe

Derive candidate entities from the evaluation triples.

By default, resources appearing as subject or non-literal object are entities.

Do not treat literal objects as entities.

## Removal modes

Support at least:

### `incident`

Selecting entity `e` removes every eligible triple where:

```text
subject == e OR object == e
```

This models an entity being absent from the integrated KG.

### `subject`

Selecting entity `e` removes eligible triples where:

```text
subject == e
```

This models failure to process an entity-centered source record.

Default should be `incident`.

## Algorithm

Randomly order candidate entities using `rng`.

Add entities one by one and collect their associated triples until the target removed count is reached or would be substantially exceeded.

Because entities remove groups of triples, exact target percentages may not be possible.

Implement a simple boundary heuristic:

1. greedily add entity units while below target;
2. for the next candidate entity, compare:
   - absolute error if included;
   - absolute error if excluded;
3. choose whichever yields a realized count closer to the requested count;
4. stop.

Do not partially remove an entity unit in the default mode.

## Metadata

Record:

```text
selected_entities
selected_entity_count
entity_mode
entity_degrees
realized_removed_count
overshoot_or_undershoot
```

For large graphs, store long entity lists in a separate `.jsonl` file and reference it from the manifest.

---

# 11. Family 3 — Predicate-centric removal

## Purpose

Simulate systematic relation extraction, relation linking, schema alignment, or mapping failures.

## Two supported modes

### `whole_predicates`

Choose predicates and remove all eligible triples using those predicates.

This is the preferred default because it creates clear systematic blind spots.

### `within_predicates`

Choose one or more predicates and remove only a target fraction of their triples.

Useful for less extreme relation-specific degradation.

## Predicate selection

Default: uniformly sample predicates.

Also allow optional weighting:

```yaml
predicate_sampling: uniform
# or
predicate_sampling: frequency
# or
predicate_sampling: inverse_frequency
```

## Target handling

For `whole_predicates`, exact target deletion size is generally impossible.

Use the same nearest-target unit-selection heuristic as entity corruption.

## Metadata

Record:

```text
selected_predicates
predicate_frequencies_before
predicate_mode
realized_removed_count
```

---

# 12. Family 4 — Long-tail / frequency-biased removal

## Purpose

Simulate pipelines that perform well on common relations/entities but miss rare information.

This family must be explicitly different from predicate-centric removal.

Predicate-centric corruption removes selected relation types as units.

Long-tail corruption removes **individual triples with a probability biased toward rarity**.

## Default rarity definition

Use predicate frequency:

```text
freq(p) = number of eligible triples with predicate p
```

Define a rarity weight for triple `t=(s,p,o)`:

```text
w(t) = 1 / freq(p)^alpha
```

where:

```text
alpha > 0
```

controls the strength of long-tail bias.

Default:

```text
alpha = 1.0
```

Then sample triples **without replacement** using weighted sampling until exactly `m` triples have been selected.

## Optional rarity bases

Design the code so the following can be added/configured:

```text
predicate_frequency
subject_frequency
entity_frequency
combined
```

For `combined`, a possible future formulation is:

```text
w(t) =
    predicate_rarity(t)
    × subject_rarity(t)
    × object_rarity(t)
```

Do not overcomplicate the first implementation. Predicate-frequency rarity is enough for v1.

## Validation requirement

The removed set should demonstrably be rarer than a uniform random sample.

Record:

```text
mean_predicate_frequency_removed
median_predicate_frequency_removed
mean_predicate_frequency_retained
median_predicate_frequency_retained
```

---

# 13. Family 5 — Source/document-centric removal

## Purpose

Simulate failures affecting complete input sources, documents, records, or source units.

Examples:

- a Wikipedia document fails extraction;
- an RDF source entity is not processed;
- one input file fails;
- an integration source is unavailable.

## Critical requirement

This family requires an explicit mapping:

```text
triple → source_unit_id
```

Do not infer source membership from arbitrary URIs unless the dataset explicitly defines such semantics.

Supported provenance representations should include at least one easy sidecar format.

Recommended JSONL:

```json
{"s": "...", "p": "...", "o": "...", "source_id": "doc_001"}
{"s": "...", "p": "...", "o": "...", "source_id": "doc_001"}
{"s": "...", "p": "...", "o": "...", "source_id": "doc_002"}
```

If the root graph is provided as named graphs, optionally allow:

```text
named graph URI = source_unit_id
```

## Algorithm

Group eligible triples by source unit.

Randomly choose whole source units and remove all triples attributed to them until the closest achievable target count is reached.

Do not partially remove a source unit in the default mode.

## Many-to-many provenance

A triple may be supported by multiple source units.

Provide an explicit policy:

```yaml
source:
  multi_source_policy: remove_if_all_selected
```

Recommended semantics:

A triple is removed only when **all** source units supporting it are selected for corruption.

This models loss of source evidence more realistically than deleting a fact that remains available from another source.

Also allow:

```text
remove_if_any_selected
```

for controlled ablations.

## Failure behavior

If source-centric corruption is enabled but no provenance mapping exists, fail with a clear error.

Do not silently approximate.

---

# 14. Family 6 — Cluster/subgraph removal

## Purpose

Simulate locally correlated failures that remove connected regions of the graph rather than independent facts.

Examples:

- failure around one movie and its related people/companies;
- an integration issue affecting a connected entity cluster;
- loss of a bounded neighborhood.

## Graph view

Construct an entity adjacency graph from eligible triples.

Default:

- nodes = URI/BNode entities;
- an RDF triple `(s,p,o)` with resource-valued `o` induces an undirected edge `s—o`;
- literal-valued triples remain attached to their subject but do not create literal traversal nodes.

## Default algorithm: neighborhood growth

1. Randomly select a seed entity.
2. Perform BFS up to `max_hops`.
3. Collect eligible triples incident to visited entities.
4. If the target count is not yet reached, sample another seed outside the already selected region.
5. Continue until the nearest achievable target is obtained.

Config:

```yaml
subgraph:
  traversal: undirected
  max_hops: 2
  include_literal_properties: true
  seed_entity_sampling: uniform
```

## Avoid one giant high-degree seed dominating the corruption

Support an optional maximum unit size:

```yaml
max_cluster_fraction: 0.10
```

If a single seed neighborhood would remove more than this fraction of `T_eval`, reject that seed and sample another one.

## Metadata

Record:

```text
seed_entities
visited_entity_count
component_count
max_hops
removed_connected_components
realized_removed_count
```

---

# 15. Generation procedure

For every enabled family:

```text
for family in families:
    for removal_fraction in removal_fractions:
        for seed in seeds:
            generate corruption independently
            validate result
            serialize graph
            serialize metadata
```

Pseudocode:

```python
root = load_graph(config.input.root_reference)

protected, eligible = build_eligibility_sets(root, config.eligibility)

for family in enabled_families:
    strategy = create_strategy(family)

    for fraction in config.benchmark.removal_fractions:
        target = round(len(eligible) * fraction)

        for seed in config.benchmark.seeds:
            rng = random.Random(stable_seed(family, fraction, seed))

            result = strategy.corrupt(
                graph=root,
                eligible_triples=eligible,
                target_removed_count=target,
                rng=rng,
                context=context,
            )

            validate_corruption(root, eligible, protected, result)

            write_graph(result)
            write_manifest_entry(result)
```

Use a stable seed derivation; do not rely on Python's process-randomized `hash()`.

Example:

```python
stable_seed = sha256(
    f"{global_seed}|{family}|{fraction}|{seed}".encode()
).digest()
```

Convert part of the digest to an integer.

---

# 16. Output layout

Recommended:

```text
outputs/corruptions/
├── benchmark_manifest.csv
├── benchmark_manifest.jsonl
├── root_stats.json
├── random_fact/
│   ├── remove_005/
│   │   ├── seed_001.nt
│   │   ├── seed_001.meta.json
│   │   ├── seed_001.removed.nt
│   │   └── ...
│   └── ...
├── entity/
├── predicate/
├── long_tail/
├── source/
└── subgraph/
```

Use zero-padded percentage names only for readability. Store the exact floating-point fraction in metadata.

---

# 17. Manifest schema

Each generated KG must have one manifest row.

Minimum fields:

```text
graph_id
family
seed

root_graph_path
root_triple_count
eligible_triple_count
protected_triple_count

target_removed_fraction
target_removed_count

realized_removed_fraction
realized_removed_count
retained_eval_count
reference_coverage

output_graph_path
removed_triples_path
metadata_path

strategy_version
config_hash
root_graph_hash
created_at
```

Family-specific metadata may live in `metadata_path`.

Example:

```json
{
  "graph_id": "predicate_remove_020_seed_004",
  "family": "predicate",
  "seed": 4,
  "eligible_triple_count": 100000,
  "target_removed_fraction": 0.2,
  "target_removed_count": 20000,
  "realized_removed_count": 19782,
  "realized_removed_fraction": 0.19782,
  "reference_coverage": 0.80218,
  "selected_predicates": [
    "http://example.org/ontology/director",
    "http://example.org/ontology/producer"
  ]
}
```

---

# 18. Validation invariants

Run validation for every generated graph.

## Hard invariants

### Subset invariant

```text
K_corrupt ⊆ K_root
```

There must be no newly introduced triples.

### Protected-triple invariant

```text
T_protected ⊆ K_corrupt
```

All protected triples must remain present.

### Removal accounting

```text
removed = T_eval \ K_corrupt
```

and:

```text
|removed| = realized_removed_count
```

### Coverage accounting

```text
reference_coverage
    = 1 - realized_removed_count / |T_eval|
```

within floating-point tolerance.

### Determinism

Running the same configuration twice must produce byte-equivalent canonical triple sets, even if serialization ordering differs.

Compare parsed triple sets or canonical N-Triples, not raw Turtle bytes.

---

# 19. Tests

Implement unit tests using a small synthetic KG with:

- common and rare predicates;
- entities of different degrees;
- multiple connected components;
- literal properties;
- protected labels;
- explicit source provenance;
- one triple supported by multiple sources.

Required tests:

### Random fact
- removes exact requested number;
- only eligible triples removed;
- deterministic for seed.

### Entity
- all incident triples for selected entities are removed in `incident` mode;
- unrelated triples remain.

### Predicate
- `whole_predicates` removes complete predicate groups;
- no triple with a selected predicate remains.

### Long-tail
- exact requested number removed;
- deterministic;
- low-frequency predicates are selected more often across many seeds than high-frequency predicates.

### Source
- whole source units are removed;
- multi-source policy works correctly.

### Subgraph
- removed entities/triples form the intended local neighborhoods;
- hop limit is respected.

### Global invariants
- root graph never changes;
- corrupted graph is always a subset;
- protected triples remain;
- manifest values reproduce measured graph statistics.

---

# 20. CLI

Provide a simple CLI.

Generate benchmark:

```bash
kg-corrupt generate \
  --config configs/corruption.yaml
```

Generate one family only:

```bash
kg-corrupt generate \
  --config configs/corruption.yaml \
  --family entity
```

Generate one condition:

```bash
kg-corrupt generate-one \
  --root-reference data/root_reference.ttl \
  --family long_tail \
  --remove-fraction 0.30 \
  --seed 42 \
  --output out.nt
```

Inspect root statistics:

```bash
kg-corrupt stats \
  --root-reference data/root_reference.ttl
```

Validate generated benchmark:

```bash
kg-corrupt validate \
  --manifest outputs/corruptions/benchmark_manifest.csv
```

---

# 21. Root statistics

Before generating corruptions, output:

```text
total triples
eligible triples
protected triples
unique subjects
unique resource objects
unique entities
unique predicates
predicate frequency distribution
entity degree distribution
connected component sizes
source-unit count, if provenance exists
source-unit size distribution
```

Write this to:

```text
root_stats.json
```

These statistics are useful for explaining the benchmark in the paper and for interpreting why some corruption families cannot hit requested percentages exactly.

---

# 22. Reproducibility requirements

For every benchmark run, persist:

```text
full configuration
Git commit, if available
Python version
rdflib version
strategy implementation version
root graph content hash
provenance content hash, if used
timestamp
```

A generated benchmark must be reproducible solely from:

```text
root reference
provenance mapping, if needed
configuration
code version
```

---

# 23. Important implementation choices

## Independent vs nested corruptions

Default:

```yaml
nested: false
```

Each corruption level is independently sampled.

Optional experimental mode:

```yaml
nested: true
```

may create a monotonic corruption path, but this should be considered a separate benchmark variant.

---

## Blank nodes

Blank nodes complicate graph equality and serialization.

For v1:

- preserve them;
- canonicalize the graph for hashing/comparison if needed;
- avoid selecting blank nodes as corruption seed entities by default;
- triples containing blank nodes may still be eligible unless protected.

Make this configurable.

---

## RDF lists / structural scaffolding

If the graph contains RDF collections or reified structures, naive triple deletion can create malformed partial structures.

Do not attempt automatic semantic repair.

Instead provide an exclusion mechanism:

```yaml
eligibility:
  protected_namespaces: []
  protected_predicates: []
```

and document that benchmark creators should protect structural predicates where necessary.

---

# 24. Optional analysis helper

It is useful, but not required for the generator, to provide a small script that summarizes the generated benchmark.

For each family and corruption level calculate:

```text
number of instances
mean realized corruption
std realized corruption
min/max realized corruption
mean retained triple count
```

Later, after SGE results are available, a separate analysis module can join results by `graph_id` and calculate:

```text
Spearman rho
Kendall tau-b
pairwise ordering accuracy
MAE
RMSE
coverage-loss sensitivity
```

Do **not** couple SGE execution into the corruption generator.

---

# 25. Acceptance criteria

The implementation is complete when:

- [ ] A root RDF graph can be loaded from disk.
- [ ] Evaluation and protected triples are separated deterministically.
- [ ] All six corruption families are implemented.
- [ ] Multiple corruption levels and seeds can be generated in one run.
- [ ] Every output KG is a strict subset/equal subset of the root graph.
- [ ] No corruption family adds false facts.
- [ ] The realized reference coverage is computed exactly.
- [ ] Every output has machine-readable metadata.
- [ ] A global manifest is created.
- [ ] Re-running the same seed/config produces the same triple set.
- [ ] Unit tests cover every corruption family.
- [ ] Source-centric corruption fails explicitly if provenance is unavailable.
- [ ] Protected triples are never removed.
- [ ] The root graph itself is never modified.
- [ ] The benchmark can be regenerated from config + root + code version.

---

# 26. Suggested implementation order

Implement in this order:

```text
1. RDF I/O + eligibility filtering
2. common corruption interface
3. random fact removal
4. manifest + validation
5. entity-centric removal
6. predicate-centric removal
7. long-tail removal
8. source/document-centric removal
9. cluster/subgraph removal
10. CLI
11. tests
12. benchmark summary statistics
```

Get random corruption + manifests + invariants completely correct before implementing structured families.

---

# 27. Non-goals for v1

Do not implement these unless explicitly requested later:

- false-triple injection;
- predicate replacement;
- object swapping;
- contradictory-value injection;
- automatic source-grounding verification;
- SGE evaluation itself;
- LLM calls;
- pipeline execution;
- graph repair after corruption;
- ontology reasoning.

The v1 benchmark is intentionally an **omission-only coverage benchmark**.

---

# 28. Example expected experiment

With:

```yaml
seeds: 20
removal_fractions:
  - 0.05
  - 0.10
  - 0.20
  - 0.30
  - 0.40
  - 0.50
  - 0.60
  - 0.70
```

and all six corruption families:

```text
6 × 8 × 20 = 960 corrupted KGs
```

Each graph obtains a known `reference_coverage`.

For example:

```text
graph_id:
  long_tail_remove_030_seed_007

target corruption:
  0.30

realized corruption:
  0.30

reference coverage:
  0.70
```

For group-based corruption, the realized value may differ slightly:

```text
graph_id:
  entity_remove_030_seed_007

target corruption:
  0.30

realized corruption:
  0.2947

reference coverage:
  0.7053
```

Always use the **realized** coverage in downstream evaluation.

---

# 29. Cursor implementation instruction

Use this `IDEA.md` as the implementation specification.

Implement the benchmark generator incrementally and keep the code modular. Do not collapse all corruption logic into one script. Each corruption family must implement a shared strategy interface and be independently testable.

Before writing code:

1. inspect the existing repository;
2. reuse its package structure, RDF utilities, logging, and configuration conventions where sensible;
3. identify how the current reference KG is represented;
4. identify whether triple-to-source provenance already exists;
5. propose the exact files to add or modify.

Then implement the work in small stages, running tests after every stage.

Do not invent provenance for the source-centric family. If no provenance representation exists in the repository, implement the generic sidecar/named-graph interface and clearly mark dataset-specific provenance preparation as required input.

Favor transparent, inspectable algorithms over clever heuristics. Every deleted triple must be traceable to a corruption family, seed, corruption level, and selected corruption unit.
