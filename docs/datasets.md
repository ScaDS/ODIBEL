# Datasets

Input datasets for the workflows under `resources/` live in `data/<dataset_id>/`. Each dataset directory has a `.odibel/` folder with its metadata. Only `.odibel/` is versioned in Git; payloads stay local (copies or symlinks to shared storage) and are identified by their checksums.

This is a prototype (metadata version `0.1`). Generated KGI-Bench bundles have their own cards and registry on the `kgi-bench-dbp-subonto` branch (`pyodibel.datasets.kgi_provenance`); an input dataset is referenced there by `id` and `sha256`.

## Layout

```text
data/
  dbpedia-2022.12.01-selected/
    selected.nt.bz2 -> /data/datasets/dbpedia/dbpedia_20221201/selected.nt.bz2
    .odibel/
      dataset.yaml
      files.sha256
      stats.json
  dbpedia-2022.12.01-mini-food-celestial-politics-en-s42/
    data.nt.bz2
    entities.txt
    seeds/
    .odibel/
      ...
```

| File | Content |
| --- | --- |
| `dataset.yaml` | Identity, source or lineage, recipe, contract, limitations |
| `files.sha256` | `sha256sum` output for every payload file, paths relative to the dataset directory |
| `stats.json` | Optional measured statistics (counts, top predicates and types, languages) |

## `dataset.yaml`

Required keys: `odibel` (metadata version), `id` (equals the directory name), `title`, `status` (`draft`, `published` or `deprecated`), `kind`, `license` (SPDX id).

Optional keys:

| Key | Content |
| --- | --- |
| `source` | Upstream name, release, URL, local origin; for datasets taken as they are |
| `derived_from` | List of `{id, sha256}` of parents; for derived datasets such as minidumps. With `file`, `sha256` is that payload file's checksum; without it, it is the sha256 of the parent's `files.sha256` |
| `recipe` | Script, git commit, dirty flag, arguments, seed, tool versions |
| `files` | Short description per payload file or directory |
| `contract` | What consumers can rely on: format, namespaces, type closure, languages |
| `limitations` | Known gaps, for example an unrecorded build recipe |
| `used_by` | Workflows that read the dataset |

## Commands

```bash
uv run pyodibel data hash data/<dataset_id>               # write .odibel/files.sha256
uv run pyodibel data verify data/<dataset_id> ...         # manifest keys and payload checksums
uv run pyodibel data verify --metadata-only data/*/       # without the payloads
```

`files.sha256` also works with `sha256sum -c` from inside the dataset directory.

## Minidumps

A minidump is a small derived dataset that is enough to run a workflow end to end. DBpedia minidumps are made with [`resources/dbpedia-slice/slice.sh`](https://github.com/ScaDS/ODIBEL/tree/main/resources/dbpedia-slice), which writes the `.odibel/` folder itself.
