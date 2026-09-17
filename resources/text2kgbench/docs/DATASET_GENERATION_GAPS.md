# Benchmark Dataset Generation — Clarity Gaps & Checklist

Purpose: make multi-format / multi-source KG integration benchmark construction reproducible and paper-ready.  
Compared against: KGI-Bench preprint (esp. §5), `resources/movie-multi-source-kg`, shipped MovieKG under `data/kgpipe-moviekg/datasets`, the generalization WIP in this directory, and the operator doc [`docs/kgi_data.md`](../../docs/kgi_data.md).

Legend:

- **Paper**: what §5 (and related) currently states or omits  
- **Movie code / data**: what the film pipeline actually did  
- **text2kgbench**: what the generalized scripts cover today  
- **kgi_data.md**: what the repo operator doc already explains  
- Status: `missing` | `partial` | `done-in-code-undocumented` | `paper-code-mismatch` | `documented-in-kgi_data`

---

## Relationship to `docs/kgi_data.md`

[`docs/kgi_data.md`](../../docs/kgi_data.md) is the **best existing runbook for the generalized RDF path** (steps 1–4). It already covers:

| Covered well | Still open relative to full domain KGI-Bench bundles |
|--------------|--------------------------------------------------------|
| Mermaid flow: dump → filter → reachable → splits → seed/RDF shade | No JSON / text / multi-source acquisition stages |
| CLI usage, examples (food / written work), Spark cluster notes | No MovieKG regeneration recipe (legacy `generate.py`) |
| Overlap params (`0.04`, subset size, seed) | No measured-overlap report written to disk |
| Output layout for reference + seed + RDF | No gold beyond `verified_entities.csv` (matches, links, relation maps) |
| `@en` literal filter tip | Dump version / checksum / selection criteria still unspecified |
| `sampling.py` / `crawl.py` mentioned as related | Not wired into the flowchart or end-to-end example |
| Text2KGBench ontology ID table + main-class choice | Ontology quality fixes / OBJECT vs LITERAL mapping policy absent |
| Env var table | No root `provenance.json` / generation config artifact |
| — | **No dataset-quality evaluation CLI** (source↔reference coverage, etc.) |

**Current focus (code, not paper):** finish multi-domain generation (especially non-RDF sources + packaging) and add **dataset evaluation** so new domains can be accepted/rejected with measurable quality gates. Paper §5 alignment is deferred while the work is under review.

**Implication:** extend `kgi_data.md` as features land; keep paper rewrite secondary.

---

## 0. Target process (intended end-to-end)

A clear methods write-up should name these stages in order, with inputs/outputs and parameters for each:

```text
[A] Domain ontology (+ property equivalences / target namespace)
        ↓
[B] Authoritative dump / subgraph (DBpedia and/or Wikidata)
        ↓
[C] Root-entity selection (main class, size, filters, seed)
        ↓
[D] Reference KG construction (reachable expansion + mapping + cleaning)
        ↓
[E] Overlapping splits of root entities → per-split reference (+ optional aggregate)
        ↓
[F] Derive seed KG + shaded RDF sources
        ↓
[G] Acquire / synthesize JSON + text (+ other) sources via links / external IDs
        ↓
[H] Emit evaluation gold (verified_entities, verified_matches, verified_links, relation maps, provenance)
        ↓
[I] Package (layout, ontology.ttl, sizes, checksums, dump versions)
```

**Gap:** `docs/kgi_data.md` already walks **A→F for the RDF-only path** (filter → reachable → splits → shade). Still missing as one recipe: MovieKG-specific construction, stages **G–I** (JSON/text/external sources, full gold, packaging), and provenance/dump versioning. Movie code remains ad-hoc with absolute paths; `crawl.py` / `sampling.py` are only side notes in `kgi_data.md`.

---

## 1. End-to-end method narrative (paper + docs)

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 1.1 | Explicit pipeline diagram / stage list for dataset *construction* (distinct from integration pipelines in §6) | `partial` | `kgi_data.md` has RDF construction flowchart; paper §5 still vague; multi-format stages absent from both |
| 1.2 | One-page “how to regenerate MovieKG / a new domain” runbook | `partial` | New domains: **documented in `kgi_data.md`**. MovieKG legacy regen: still `missing` |
| 1.3 | Map each artifact in the release layout to the stage that produces it | `partial` | `kgi_data.md` covers reference/seed/RDF; MovieKG extras (`data_agg_eval*`, relation matches, JSON/text meta) unexplained |
| 1.4 | Align paper wording with *actual* parameters used for released datasets | `paper-code-mismatch` | See §8; `kgi_data.md` correctly documents `overlap-ratio 0.04` |
| 1.5 | Keep `kgi_data.md` and paper §5 in sync (single source of truth) | `missing` | Risk of drift once methods are expanded |

**Checklist**

- [ ] Extend `docs/kgi_data.md` flowchart to A→I (or mark G–I as “planned”) — do not duplicate into a second PIPELINE.md  
- [ ] Add a short “MovieKG (paper artifact) vs text2kgbench (generalized)” divergence subsection to `kgi_data.md`  
- [ ] Table: artifact path → producer script → used for (eval / packaging) — merge with existing layout sections  
- [ ] When rewriting paper §5, cite/condense `kgi_data.md` for the RDF path rather than reinventing it

---

## 2. Ontology & schema contract

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 2.1 | How the target ontology was curated (manual rules, which equivalences kept) | `partial` | Paper: 3 classes, 25 properties, disjointness, cardinality; no curation protocol |
| 2.2 | Exact property set used for **reference expansion** vs full ontology | `paper-code-mismatch` | Paper: “eight ontology relations”; code uses a larger `DBOnto_DIRECT_MAPPINGS` dict (object+literal) |
| 2.3 | Literal vs object treatment per property (and effect on JSON nesting) | `done-in-code-undocumented` | `DirectMappingType.LITERAL` vs `OBJECT`; JSON forces many keys to literals |
| 2.4 | Role of Wikidata / `owl:equivalentProperty` in generation vs evaluation only | `missing` | `movie.conf` lists `wdt:` equals; film generation appears DBpedia-centric |
| 2.5 | Text2KGBench ontology quality fixes (xsd types, datatype vs object properties) | `partial` | README lists issues; `kgi_data.md` assumes ontologies are usable as-is |
| 2.6 | Target namespace shading rules (`http://kg.org/ontology/` vs per-split RDF ontology NS) | `documented-in-kgi_data` | Step 4 table in `kgi_data.md`; hashing details still thin for the paper |

**Checklist**

- [ ] Publish the **exact** property whitelist used for reference construction (and mark which are expansion edges)  
- [ ] Document OBJECT vs LITERAL mapping table (and JSON key naming)  
- [ ] State whether Wikidata equivalences are (a) used at generation, (b) only for SA gold, or (c) unused in MovieKG v1  
- [ ] For generalization: ontology-driven mapping config (TTL/`movie.conf`-like) as first-class input

---

## 3. Authoritative source & dump provenance

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 3.1 | DBpedia dump version / date / language edition | `missing` | Paper and `kgi_data.md` both omit snapshot ID; only mention `selected.nt.bz2` as a convenient input |
| 3.2 | Whether data comes from local NT dump, live SPARQL, or pre-acquired FileHashStore | `partial` | text2kgbench path: **documented in `kgi_data.md`**. Movie FileHashStore acquisition: still undocumented |
| 3.3 | Wikidata as second authoritative source (when / how) | `missing` | Stated intent for generalization; not in Movie §5 |
| 3.4 | Link types used to join sources: sameAs, interlanguage, external IDs (IMDb, Freebase, …) | `missing` | Critical for multi-source story; not specified |
| 3.5 | Handling of entities without Wikidata / without abstract / incomplete subgraph | `partial` | Movie skips missing/empty text & JSON; paper only mentions “cleaning” |

**Checklist**

- [ ] Record dump URI, date, checksum, and language filters in a `provenance.json` at dataset root  
- [ ] Define join policy: DBpedia↔Wikidata↔Wikipedia↔external IDs  
- [ ] Define drop/keep rules when a linked source is missing  
- [ ] For multi-source: which source is ground truth when facts conflict (MovieKG assumes DBpedia reference)

---

## 4. Root-entity selection (main class sampling)

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 4.1 | How 10k films were chosen from ~150k | `missing` | Random? degree-stratified? completeness filters? |
| 4.2 | Filters: must have abstract? must have ≥k object properties? type = dbo:Film only? | `missing` | README backlog mentions Wikidata match gaps (954/…) |
| 4.3 | Random seed and selection script for 100 / 1k / 10k variants | `missing` | Paths like `selection_*_dbpedia_film` are external |
| 4.4 | text2kgbench sampling (`sampling.py`) vs Movie selection relationship | `partial` | Stratified/subgraph sampling exists; not wired as “the” selection method for Movie |

**Checklist**

- [ ] Document selection algorithm + seed + exclusion criteria  
- [ ] Ship or regenerate `master_entities.csv` with selection metadata  
- [ ] Decide: selection before expansion (Movie) vs sample after subgraph (text2kgbench) — pick one for the paper

---

## 5. Reference KG construction

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 5.1 | Recursion / hop policy (1-hop along object properties? unbounded?) | `documented-in-kgi_data` | `kgi_data.md` Step 2: `--max-hops`, `--direction`; paper still vague; Movie hop semantics may differ |
| 5.2 | Which related types are retained (Person, Company only vs any reachable) | `paper-code-mismatch` | Paper emphasizes Film/Person/Company; reachability may pull more types unless filtered |
| 5.3 | URI rewriting for reference (MD5 hash under `kg.org/resource/`) | `done-in-code-undocumented` | Essential for ER challenge; paper only mentions shading for *source* RDF |
| 5.4 | Type enrichment from ontology | `done-in-code-undocumented` | `enrich_type_information` |
| 5.5 | Label handling (`rdfs:label`, `skos:altLabel` copy) | `done-in-code-undocumented` | Affects lexical ER / evaluation |
| 5.6 | Cleaning steps that shrink “final size a bit lower” | `missing` | What was removed? empty graphs? entities without labels? |
| 5.7 | Meaning of `data.nt` vs `data_agg.nt` vs `data_agg_eval*.nt` / `*_noo*` | `missing` | Present in released MovieKG; not in paper |

**Checklist**

- [ ] Specify hop direction (`forward`/`backward`/`both`) and max hops  
- [ ] Specify type allow-list / pruning after expansion  
- [ ] Document IRI hashing function and namespaces for reference vs seed vs RDF source  
- [ ] Document every reference file variant and which one is the eval gold (`KG_R`, `KG_R(n)`)  
- [ ] List cleaning filters with counts (before/after)

---

## 6. Overlapping splits

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 6.1 | Exact overlap construction algorithm | `partial` | Params in `kgi_data.md`; algorithm details (`build_exact_subsets` pairwise blocks) still not explained for the paper |
| 6.2 | Reported overlap ~5% films / ~30% all entities | `paper-code-mismatch` | `kgi_data.md` / code use `0.04`; paper says ~5%; 30% all-entity overlap is emergent |
| 6.3 | Which split is seed vs sources | `partial` | `kgi_data.md` shades every split; paper protocol (seed = split_0, integrate others) still needs an explicit eval subsection |
| 6.4 | Overlap gold for non-root entities (persons/companies shared because of graph structure) | `partial` | Paper mentions ~30%; how verified_matches treat them is unclear in methods |
| 6.5 | Random seed for shuffle before subsetting | `documented-in-kgi_data` | `--seed 42` in `kgi_data.md` / `splits.py`; Movie path less clear |

**Checklist**

- [ ] Formalize overlap design (controlled on root class only; secondary overlap is a measured outcome)  
- [ ] Report measured pairwise overlaps for films and for all entities on the released sets  
- [ ] Clarify experimental protocol: integrating `split_1..3` sources into `split_0` seed (or other)  
- [ ] Emit `overlap_report.json` from `validate_overlaps` / `pairwise_entity_overlaps`

---

## 7. Format-specific source generation

### 7.1 RDF seed & shaded sources

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 7.1.1 | Shading procedure | `documented-in-kgi_data` | Step 4 in `kgi_data.md` (namespaces); paper still needs IRI examples |
| 7.1.2 | Whether seed is a shaded reference or a separately reconstructed subgraph | `paper-code-mismatch` | Paper: “seed = first split of reference”; code also re-runs mapping/shading |
| 7.1.3 | Ontology NS renaming per RDF source split | `done-in-code-undocumented` | Forces SA/OM |

**Checklist**

- [ ] One paragraph + IRI examples for shading (paper §5)  
- [ ] Confirm seed ≡ shaded(reference of split_0) or document differences

### 7.2 JSON

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 7.2.1 | Algorithm (`build_recursive_json`) and nesting rules | `done-in-code-undocumented` | Not in paper beyond an example document |
| 7.2.2 | Key naming (ontology local names) and intentional ambiguities (“date”, roles) | `partial` | Paper discusses ambiguity qualitatively; no generation rule that *introduces* it |
| 7.2.3 | Provenance (`.prov` / `provenance.json`) | `partial` | Shipped for MovieKG JSON; generation not generalized |
| 7.2.4 | Relation gold (`verified_relation_matches.json`) | `done-in-code-undocumented` | Needed for RRL metric; not described in §5 |
| 7.2.5 | text2kgbench JSON synthesizer | `missing` | No script yet |

**Checklist**

- [ ] Spec: JSON schema derivation from ontology mappings  
- [ ] Spec: which object properties become nested objects vs label strings  
- [ ] Document provenance fields and relation-match gold  
- [ ] Implement `json_sources.py` (or equivalent) in text2kgbench

### 7.3 Text

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 7.3.1 | Exact text origin | `partial` | Paper: “DBpedia abstracts”; not SPARQL query / property / language / truncation |
| 7.3.2 | File naming = MD5(DBpedia URI) | `done-in-code-undocumented` | Links via `verified_links.csv` |
| 7.3.3 | Coverage when abstract missing | `partial` | Skipped with counts printed; not in paper |
| 7.3.4 | Generalization via Wikipedia / interlanguage / crawl | `partial` | `crawl.py` exists; not connected to splits or link resolution |
| 7.3.5 | Mention gold beyond root film (persons/companies in text) | `missing` | Only root Film links seem recorded |

**Checklist**

- [ ] Document abstract property, language (`en`), and fetch method  
- [ ] Define multi-source text policy (enwiki vs other languages via sitelinks)  
- [ ] Decide gold: root-only links vs full mention annotation  
- [ ] Wire crawl/export into the split bundle layout

### 7.4 Other / multi-source formats (generalization)

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 7.4.1 | How external IDs select alternate sources (Wikidata JSON, HTML infobox, …) | `missing` | Core of stated generalization |
| 7.4.2 | Conflict / complementarity policy across sources for the **same** entity | `missing` | Reference still needs a single gold |
| 7.4.3 | Licensing / ToS constraints for crawled sources | `missing` | Needed for redistribution |

---

## 8. Paper ↔ released MovieKG parameter audit

Reconcile before rewriting §5:

| Claim / parameter | Paper | Code / data | Action |
|-------------------|-------|-------------|--------|
| Film count | 10 000 (slightly lower after cleaning) | `film_10k` (+ 100 / 1k) | Publish exact final root counts per split |
| Pairwise film overlap | “around 5%” | `overlap_ratio=0.04` | Pick one number; report measured |
| All-entity overlap | “30%” | Emergent | Report measured; don’t imply it is a design knob |
| Expansion relations | “eight ontology relations” | Larger mapping dict | Count and list predicates actually used |
| Seed definition | First split of reference | Each `split_i` has `kg/seed` | Clarify eval protocol vs directory layout |
| Text | DBpedia abstracts | Pre-acquired `text/*.txt` | Document acquisition |
| Wikidata | Equivalences in ontology figure/conf | Little/no use in generate | Clarify |

**Checklist**

- [ ] Recompute and freeze statistics table for film_100 / film_1k / film_10k  
- [ ] Update §5 numbers to match frozen stats  
- [ ] Add appendix: full property mapping table

---

## 9. Evaluation gold / supplementary metadata

Paper §5 “Supplementary Data” mentions expected matches and verified entities. Released MovieKG has more; text2kgbench only partially emits them.

| Artifact | MovieKG | text2kgbench today | Gap |
|----------|---------|--------------------|-----|
| `entities/master_entities.csv` | yes | yes (`splits.py`) | Document selection provenance |
| `split_N/index/entities.csv` | yes | yes | OK |
| `kg/*/meta/verified_entities.csv` | yes | reference + seed/rdf | JSON/text entities incomplete in generalization |
| `verified_matches.csv` (seed↔seed, rdf↔seed) | yes | `missing` | Match generation still commented / unfinished in Movie `generate.py` |
| `verified_links.csv` (doc→entity) | yes | `missing` | Need after JSON/text bundling |
| `verified_relation_matches.json` | yes | `missing` | Schema-mapping gold |
| `provenance.json` (JSON) | yes | `missing` | |
| Aggregate / eval-restricted NT variants | yes | `data_agg` only | Document `*_eval*` / `*_noo*` or stop shipping unexplained files |

**Checklist**

- [ ] Spec schemas for all meta CSVs/JSON (columns, ID namespaces)  
- [ ] Implement match emission for shaded IRIs (hash-stable IDs make this feasible)  
- [ ] Define which gold files are required for KGI-Bench metrics vs optional diagnostics  
- [ ] Ensure `dataset` column values are real paths (Movie data often uses placeholder `"dataset"`)

---

## 10. Package layout & reproducibility

| ID | Gap | Status | Notes |
|----|-----|--------|-------|
| 10.1 | Canonical layout (STRUCTURE.md vs actual MovieKG vs text2kgbench outputs) | `partial` | Names differ (`source_json` vs `sources/json`) |
| 10.2 | Root `provenance.json` / `generation_config.yaml` | `missing` | Params, seeds, dump IDs, script versions |
| 10.3 | Absolute paths / env-specific `DATASET_SELECT` in Movie generator | `done-in-code-undocumented` | Blocks regeneration |
| 10.4 | Checksums / Zenodo packaging recipe | `missing` | Paper cites DOI; regen instructions absent here |
| 10.5 | Single orchestrator CLI (`make dataset DOMAIN=food SIZE=1k`) | `missing` | Stages are separate scripts |

**Checklist**

- [ ] Freeze one directory contract and generate against it  
- [ ] Emit config+provenance beside `ontology.ttl`  
- [ ] Replace hard-coded absolute paths with CLI/env relative to repo  
- [ ] Add smoke test: regenerate film_100 (or food_100) and compare overlap stats / triple counts

---

## 11. text2kgbench implementation gaps (code vs intended generalization)

Ordered by dependency (evaluation early so domains can be gated):

1. **Dataset evaluator** — text/JSON/RDF coverage vs reference; overlap; missing sources (`evaluate_dataset.py`)  
2. **Extend `docs/kgi_data.md`** — document eval gates + planned G–I as they land  
3. **Config object** — ontology path, main class, mappings, overlap, sizes, dump URI, RNG seed  
4. **Text bundler** — connect `crawl.py` / abstracts to split entity lists  
5. **JSON synthesizer** — port `build_recursive_json` path  
6. **Gold writers** — matches, links, relation maps, provenance  
7. **Acquisition / join layer** — external IDs & sitelinks (after single-source multi-format works)  
8. **Reference builder alignment** — Movie construct vs `subgraph.py` semantics if they diverge  
9. **Orchestration + tests** — small fixture domain end-to-end through generate + evaluate

---

## 12. Paper methods outline (deferred)

Paper is under review — keep this as a backlog for later. Fill when generator + evaluator are stable. For RDF bullets, start from [`docs/kgi_data.md`](../../docs/kgi_data.md).

1. Ontology / dump / selection / reference / splits / seed+RDF / JSON / text / multi-source links / gold / sizes / reproduction.

---

## 13. Priority order (code-first)

Paper under review → **defer methods rewriting**. Prioritize multi-domain generation and **dataset evaluation**.

### P0 — Dataset quality evaluation (gate for new domains)

Movie `evaluate.py` only has skipped stubs (`test_text_coverage_simple`, LLM variant, RDF consistency/coverage). Nothing under `text2kgbench/`.

Add `resources/text2kgbench/evaluate_dataset.py` (+ helpers in `pyodibel`) that scores a finished splits directory **without** running integration pipelines.

| Check | Question | Cheap signal | Stronger signal |
|-------|----------|--------------|-----------------|
| **Text ↔ reference coverage** | How much reference info is supportable from text? | Label/alias hits in abstracts; root `verified_links` completeness | Mention detection; LLM/NLI fact support |
| **JSON ↔ reference coverage** | Are JSON records complete vs reference? | Key presence vs ontology properties | Reconstruct triples from JSON vs reference |
| **RDF source ↔ reference** | Does shaded RDF preserve the split reference? | Count parity after unshading | Isomorphism under IRI map |
| **Overlap design** | Hit target pairwise root overlap? | Persist `validate_overlaps` JSON | Root-only vs all-entity overlap |
| **Source availability** | Missing abstracts / empty JSON / crawl failures? | Missing-file rates per split | Stratify by class / degree |
| **Ontology fitness** | Types/properties match ontology? | Histograms; orphan predicates | SHACL / disjointness |
| **Gold integrity** | Meta files consistent? | Links→docs; match IDs in both graphs | — |

**Minimum viable text-coverage metric (implement first):**

```text
For each root entity e in split with text doc T(e):
  R(e) = reference entities in the 1-hop (or reachable) subgraph of e
  hit_label(e) = |{ n in R(e) : label(n) appears in T(e) }| / |labeled nodes in R(e)|
  doc_recall = fraction of root entities with a non-empty text file

Report: mean/median hit_label, doc_recall, stratified by degree.
```

**Checklist**

- [ ] CLI: `evaluate_dataset.py <splits_dir> [--split N] [--source text|json|rdf|all]`
- [ ] Write `quality_report.json` per split + domain aggregate
- [ ] Text: doc availability + label-mention recall vs reference
- [ ] JSON: property-key recall vs reference predicates
- [ ] RDF: shade-inverse compare to reference
- [ ] Overlap + missing-source rates
- [ ] Optional later: LLM/NLI (port movie stubs)
- [ ] Quality gates documented in `kgi_data.md`

### P1 — Finish domain generation beyond RDF

From Text2KGBench ontology + DBpedia dump → full multi-format bundle.

1. Config object (ontology, main class, sizes, overlap, dump URI, seed, source flags)
2. JSON synthesizer (port `build_recursive_json`; ontology-driven keys)
3. Text bundler wired to splits (`crawl.py` / abstracts + `verified_links`)
4. Gold writers: matches, links, relation maps, provenance
5. Persist overlap report from `splits.py`
6. Smoke-test one non-film domain through evaluate gates

### P2 — Multi-source acquisition via links

External IDs / sitelinks / Wikidata once single-authority multi-format works.

### P3 — Paper alignment (deferred)

§5 mismatches, MovieKG provenance, Zenodo regen — after generator + evaluator are solid.

---

## 14. Near-term implementation slices

| Slice | Deliverable | Unblocks |
|-------|-------------|----------|
| A | `evaluate_dataset.py` structural + text label-coverage | Decide if abstracts are good enough per domain |
| B | Persist overlap/stats JSON from splits/sources | Comparable domain reports |
| C | Text bundler → `sources/text/` + links CSV | Real text for new domains |
| D | JSON synthesizer → `sources/json/` | Format parity with MovieKG |
| E | Match gold for shaded RDF | ER diagnostics |
| F | Domain config + one-command generate | Scale across Text2KGBench ontologies |

**Do next:** validate Slice A on MovieKG `film_100` (already has text), then Slice C so new domains can be scored the same way.

---

## Quick reference: what exists where

| Stage | Movie (`movie-multi-source-kg`) | text2kgbench | Documented in `kgi_data.md`? |
|-------|----------------------------------|--------------|------------------------------|
| Ontology-driven property filter | hardcoded mappings | `generate_refac.py` | yes (Step 1) |
| Sampling | external entity lists | `sampling.py` | related only |
| Reachable subgraph | `construct_graph_from_root_uris` | `subgraph.py` | yes (Step 2) |
| Overlapping splits + reference | `generate.py` + overlap util | `splits.py` | yes (Step 3) |
| Seed + RDF shade | `generate_rdf` / bundle_* | `sources.py` | yes (Step 4) |
| JSON | `test_convert_to_json` + bundle | — | no |
| Text | copy from ACQ `text/` | `crawl.py` (not wired) | related only |
| Matches / links gold | partial / commented | verified_entities only | partial |
| **Dataset quality eval** | stubs only (all skipped) | — | no |
| Orchestration / provenance | env paths | env vars table | partial |

## Active domain generation (2026-08-11)

Selected **Airport** + **CelestialBody** (see `DOMAIN_SELECTION.md`). RDF-path `*_100` bundles are being generated under `/local/d1/datasets/kgi-bench-text2kgbench/`. Runner: `run_domain.py` / `continue_domains.sh`.
