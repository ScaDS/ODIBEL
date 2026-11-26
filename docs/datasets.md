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
| `versions.yaml` | Optional version history and labels, written by `pyodibel data commit` |
| `versions/<n>.sha256` | File list of version `n`, same format as `files.sha256` |
| `cache/stat.json` | Local stat cache for `pyodibel data status`; not versioned |

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

## Versions

```bash
uv run pyodibel data status data/<dataset_id> ...         # added, removed, modified files
uv run pyodibel data commit data/<dataset_id> -m "..." [--label stable]
uv run pyodibel data log data/<dataset_id>
```

`status` compares the payload against the latest version, or against `files.sha256` if there is no version yet. It re-hashes only files whose size, modification time or inode differ from `.odibel/cache/stat.json`, and files modified within two seconds before the cache was written. `--checksum` re-hashes every file; `verify` always does.

`commit` writes `versions/<n>.sha256`, updates `files.sha256` and appends an entry to `versions.yaml`:

```yaml
labels:
  stable: 1
versions:
- version: 2
  parent: 1
  created: '2026-11-26T19:41:28+01:00'
  message: test edit
  sha256: 2ace9f79...   # sha256 of versions/2.sha256
  files: 9
  bytes: 8569797
  changes: {added: 0, removed: 1, modified: 1}
```

The first commit creates version 1. Labels point at a version and can be moved by a later `commit --label`; `latest` is reserved for the newest version. Versions cover the payload only; changes to `dataset.yaml` or `stats.json` are tracked by Git. Payloads of older versions are not stored yet; only their file lists are.

## Minidumps

A minidump is a small derived dataset that is enough to run a workflow end to end. DBpedia minidumps are made with [`resources/dbpedia-slice/slice.sh`](https://github.com/ScaDS/ODIBEL/tree/main/resources/dbpedia-slice), which writes the `.odibel/` folder itself.
