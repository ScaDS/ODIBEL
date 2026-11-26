# DBpedia slice

`slice.sh` cuts a small, entity-closed N-Triples input out of a full DBpedia dump, for end-to-end runs of `text2kgbench/` and `dbpedia-multi-source-kg/` without the full dump.

1. Sample seed subjects per group from `rdf:type` triples (seeded `shuf`).
2. Add the `http://dbpedia.org/resource/` objects of the entity set's edges, `-H` times (default 1).
3. Keep every triple whose subject is in the entity set.

A plain `head` of the dump does not work as input: the generators drop edges whose object is not a subject, and the class filters need the object types.

Each step is one streaming pass over the input. On `selected.nt.bz2` (734M triples) a pass takes about three minutes with `lbzip2` and `mawk`.

## Usage

```bash
resources/dbpedia-slice/slice.sh \
  -i data/dbpedia-2022.12.01-selected/selected.nt.bz2 \
  -o data/dbpedia-2022.12.01-mini-food-s42 \
  -s 42 \
  -L en \
  500:dbo:Food \
  500:dbo:Election,dbo:PoliticalParty,dbo:Treaty
```

A group is `N:CLASS[,CLASS...]`; `N` is a seed count or `all`. Classes are `dbo:Name` or full IRIs. `-L en` drops literals tagged with any other language tag (including `en-GB`); untagged and typed literals stay. The output directory must not exist; its name becomes the dataset id. The parent id is read from the input's `.odibel/dataset.yaml`, or set with `-p`.

Requires bash, `sort`, `shuf` (GNU coreutils), `openssl` (seeded random source), and `bzip2`. `lbzip2` and `mawk` are used when present.

## Output

The output is a dataset directory as described in [docs/datasets.md](../../docs/datasets.md).

| Path | Content |
| --- | --- |
| `data.nt.bz2` | Sorted, distinct N-Triples |
| `entities.txt` | Final entity set |
| `seeds/group_N.txt`, `seeds/seeds.txt` | Sampled seeds per group, and their union |
| `seeds/candidates_N.txt` | All subjects typed with a group's classes |
| `.odibel/dataset.yaml` | Id, parent id and sha256, recipe (seed, hops, groups, git commit, tool versions), contract |
| `.odibel/stats.json` | Triple, subject, predicate, seed and entity counts; top predicates, types and literal languages |
| `.odibel/files.sha256` | Checksums of the payload files |

The input type triples must include superclasses (DBpedia's transitive instance types). Otherwise a group only matches subjects whose most specific type is listed.
