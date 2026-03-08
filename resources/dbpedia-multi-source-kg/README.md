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

