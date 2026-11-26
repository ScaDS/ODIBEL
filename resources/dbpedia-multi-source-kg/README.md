# DBpedia Multi-Source KG Resources

This folder contains a DBpedia ontology snapshot and a reproducible class-selection workflow for subdomain-based graph extraction.

## Implemented class-selection workflow

The workflow from `CLASS_SELECTION_WORKFLOW.md` is implemented in:

- `src/multi_source_kg/main.py`

It does the following:

- Loads `dbo-snapshots.ttl` with `rdflib`
- Extracts DBpedia ontology classes and subclass hierarchy
- Applies fixed subdomain anchors and hierarchy-first assignment
- Applies semantic override heuristics when hierarchy is weak
- Adds optional secondary tags
- Assigns confidence (`high`/`medium`/`low`) and `manual_review` for low confidence
- Writes structured YAML output

The implementation is refactored into components:

- `constants.py` (subdomains, anchors, semantic hints)
- `ontology.py` (RDF loading and hierarchy extraction)
- `rule_classifier.py` (deterministic hierarchy-first classifier)
- `llm_classifier.py` (OpenAI-compatible structured classifier)
- `pipeline.py` (orchestration and YAML rendering)
- `main.py` (CLI entrypoint)

## Run

From the repository root:

`uv run python resources/dbpedia-multi-source-kg/src/multi_source_kg/main.py`

Optional flags:

- `--input <path-to-dbo-snapshots.ttl>`
- `--output <path-to-output-yaml>`
- `--method rules|hybrid|llm`
- `--max-classes <int>`
- `--export-hierarchy-tree`
- `--tree-output <path-to-tree-txt>`

LLM methods (`hybrid` / `llm`) require:

- `OPENAI_API_KEY`
- optional `OPENAI_MODEL` (default: `gpt-4.1-mini`)
- optional `OPENAI_BASE_URL` (default: `https://api.openai.com/v1`)
- optional `OPENAI_TIMEOUT_SECONDS` (default: `60`)

Example (hybrid, first 25 classes):

`OPENAI_API_KEY=... uv run python resources/dbpedia-multi-source-kg/src/multi_source_kg/main.py --method hybrid --max-classes 25`

Example (export hierarchy tree from existing YAML):

`uv run python resources/dbpedia-multi-source-kg/src/multi_source_kg/main.py --export-hierarchy-tree --output resources/dbpedia-multi-source-kg/dbpedia_class_subdomains.yaml --tree-output resources/dbpedia-multi-source-kg/dbpedia_class_hierarchy_by_subdomain.txt`

## Output

The generated dataset is:

- `dbpedia_class_subdomains.yaml`

Each entry now includes:

- final decision fields (`primary_subdomain`, `secondary_tags`, `confidence`, `rationale`)
- `decision_source` (`rules` or `llm`)
- optional `rule_based` / `llm_based` summaries when LLM methods are used

Current snapshot generation produced mappings for 790 ontology classes.

## Schema Graph

Provides tools to generate a schema graph and compute statistics based on it.

### Requirements

* Python 3.11
* Java 17
* built pyodibel

```bash
pip install -r requirements.txt
```

 Prepare the environment file:

   * Edit the provided environment file
   * Rename it to `.env`

### Generate Schema Graph

Run the corresponding script to generate a schema graph for a specific domain:

```bash
python generate_<domain>.py
```

`INPUT_PATH`, `OUTPUT_PATH` and `ONTOLOGY_PATH` can also be set in the environment. For a small run, use the minidump `data/dbpedia-2022.12.01-mini-food-celestial-politics-en-s42/data.nt.bz2` and the ontology `data/dbpedia-ontology-2022.12.05/dbo-snapshots.ttl` (see [docs/datasets.md](../../docs/datasets.md)).

### Calculate Statistics

Compute statistics based on a generated N-Triples file:

```bash
python statistics.py <input_path>
```

* `<input_path>`: Path to a `.nt` file generated in the previous step

### Validate a run

`validate_run.py` checks each step of a small run against an independent recount, classifies the triples removed by domain/range cleaning, and counts `owl:disjointWith` and `owl:FunctionalProperty` violations. It prints JSON:

```bash
uv run resources/dbpedia-multi-source-kg/validate_run.py \
  <OUTPUT_PATH> <INPUT_PATH> <ONTOLOGY_PATH> resources/dbpedia-multi-source-kg/generate_<domain>.py
```

The validation run for issue #20 is `data/dbpedia-2022.12.01-multisource-politics-mini-en-s42/.odibel/report.md`.
