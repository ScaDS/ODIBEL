# rDF2 Usage Guide

`rDF2` is a small Spark-first wrapper around RDF triples in N-Triples-like format.
It provides chainable operations for:

- parsing RDF triples into a typed DataFrame (`s`, `p`, `o`, `isLiteral`)
- filtering entity-centric subgraphs
- cleaning and sampling RDF data
- building schema-level edge summaries
- writing results back to `.nt` text output

---

## 1) Data Model

Every `rDF2` instance wraps a Spark DataFrame with exactly these columns:

- `s`: subject (string)
- `p`: predicate (string)
- `o`: object (string)
- `isLiteral`: whether `o` is a literal (`true` if object starts with `"`)

If the schema differs, `rDF2` raises a validation error.

---

## 2) Basic Setup

```python
from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.management.spark_mgr import get_spark_session

spark = get_spark_session("RDF2Guide")
```

Parse RDF triples from a file/folder:

```python
rdf = rDF2.parse(spark, "/path/to/input.nt")
```

Write back to N-Triples lines:

```python
rdf.write_nt("/path/to/output.nt")
```

`write_nt` fails if output path already exists.

---

## 3) Core Operations

### Filter by subject type

Keep all triples whose **subject** has `rdf:type == target_class`.

```python
persons = rdf.filter_triples_by_s_type("<http://dbpedia.org/ontology/Person>")
```

Multiple classes:

```python
classes = [
    "<http://dbpedia.org/ontology/PoliticalParty>",
    "<http://dbpedia.org/ontology/Election>",
]
politics = rdf.filter_triples_by_s_types(classes)
```

### Clean `rdf:type` values

For `rdf:type` triples, keep only allowed type objects.
All non-`rdf:type` triples are preserved.

```python
cleaned = politics.clean_rdf_types(classes)
```

### Keep only connected object resources

Drops non-literal edges whose object never appears as a subject.
Keeps:

- literal-object triples
- `rdf:type` / `a` triples
- resource-object triples where `o` exists as some `s`

```python
connected = cleaned.keep_triples_with_object_subject()
```

### Class-scoped entity subgraph

`filter_subgraph_by_entity_classes(classes)` keeps triples where subject is an entity typed in `classes`, and then enforces object constraints for literal/object-entity/type triples.

```python
subgraph = rdf.filter_subgraph_by_entity_classes(classes)
```

---

## 4) Sampling APIs

All sampling methods are entity-centric: they select subjects (entities), optionally expand by related entities, then keep triples with selected subjects.

### Global sampling

```python
sampled = rdf.sample_entities_global(
    sample_size=10000,
    related_per_seed=5,
    seed=13,
)
```

### Per-target-type sampling

```python
targets = {
    "<http://dbpedia.org/ontology/PoliticalParty>": 3000,
    "<http://dbpedia.org/ontology/Election>": 1000,
}

sampled = rdf.sample_entities_by_type_targets(
    type_targets=targets,
    related_per_seed=5,
    seed=13,
)
```

### Sample all discovered types

```python
sampled = rdf.sample_entities_all_types(
    target_per_type=500,
    related_per_seed=5,
    seed=13,
)
```

---

## 5) Schema Graph Generation

`build_schema_graph_df()` aggregates triple-level edges to schema-level counts:

- `SourceType`
- `Relation`
- `TargetType`
- `Count`

Default predicate filter includes:

- `rdfs:label`
- predicates with prefix `<http://dbpedia.org/ontology/`

```python
schema_df = rdf.build_schema_graph_df()
schema_df.show(20, truncate=False)
```

Optional custom predicate filters:

- exact match: full predicate IRI token
- prefix match: end with `*`

```python
schema_df = rdf.build_schema_graph_df(property_filters=[
    "<http://dbpedia.org/ontology/*",
    "<http://www.w3.org/2000/01/rdf-schema#label>",
])
```

Write directly as CSV:

```python
rdf.write_schema_graph_csv(
    output_path="/path/to/schema_graph.csv",
    property_filters=None,
)
```

---

## 6) End-to-End Pipeline Example

This mirrors the workflow style used in `resources/dbpedia-multi-source-kg/generate_politics.py`:

```python
import os
from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.management.spark_mgr import get_spark_session

spark = get_spark_session("PoliticsSubgraph")

classes = [
    "dbo:Election",
    "dbo:PoliticalParty",
    "dbo:GovernmentAgency",
]
classes = [f"<http://dbpedia.org/ontology/{c.replace('dbo:', '')}>" for c in classes]

input_path = "/data/datasets/dbpedia_20221201/selected.nt.bz2"
selected_path = "/data/datasets/dbpedia_politics_subgraph/selected.nt.bz2"
cleaned_types_path = "/data/datasets/dbpedia_politics_subgraph/cleaned_types.nt.bz2"
connected_path = "/data/datasets/dbpedia_politics_subgraph/connected.nt.bz2"
schema_path = "/data/datasets/dbpedia_politics_subgraph/schema_graph.csv"

if not os.path.exists(selected_path):
    rDF2.parse(spark, input_path).filter_triples_by_s_types(classes).write_nt(selected_path)

if not os.path.exists(cleaned_types_path):
    rDF2.parse(spark, selected_path).clean_rdf_types(classes).write_nt(cleaned_types_path)

if not os.path.exists(connected_path):
    rDF2.parse(spark, cleaned_types_path).keep_triples_with_object_subject().write_nt(connected_path)

if not os.path.exists(schema_path):
    rDF2.parse(spark, connected_path).write_schema_graph_csv(schema_path)

spark.stop()
```

---

## 7) Notes and Caveats

- Input parsing expects one triple per line ending in `.`.
- Comment lines starting with `#` and empty lines are ignored.
- `rdf:type` detection supports both full predicate IRI and Turtle shortcut `a`.
- `filter_triples_by_p_type` is currently not implemented (`pass`).
- `write_nt` requires a non-existing output path.

