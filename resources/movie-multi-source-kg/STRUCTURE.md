
I have created a benchmark dataset with multiple sources, a seed, and a reference dataset

Everything is derived from a list of 10k entities.
Derviving the entity data in different formats

I have created splits 1,2,3,4 all sharing a 10% entity overlap

What would be a good directory structure:

I have come up with something like

```
data/
├── split_1
│   ├── kg_reference
│   ├── kg_seed
│   ├── source_json
│   ├── source_rdf
│   │   ├── data
│   │   │   ├── 1.nt
│   │   │   └── 2.nt
│   │   └── meta
│   │       ├── verified_entities.csv
│   │       └── verified_matches.csv
│   └── source_text
│       ├── data
│       │   ├── 1.txt
│       │   └── 2.txt
│       └── meta
│           ├── verfied_entities.csv
│           └── verfied_links.csv
├── d2
├── d3
├── d4
├── ontology.ttl
├── provenance
└── provenance.json
```


We have 5 types

```
data
    1
    2
    3
    4
        json_source
            data
            meta
                verified_entities.csv (in each data file)
        text_source
            data
            meta
        kg_seed
            data
            meta
        rdf_source
            data
            meta
                verified_matches.csv (rdf)
                verified_links.csv (text)
        kg_reference
            data
            meta
    ontology.ttl
    provenance.json
```

Verfied Matches are hard to calculate maybe

verified_matches.csv
```
datasetA,datasetB,idA,idB,id_type
d1/kg_seed,d2/source_rdf,1,2,e
d1/kg_seed,d2/source_rdf,1,2,v
```

verified_entities.csv
```
datasets,e_id,e_type
[d1/kg_seed,d2/kg_seed],1,c1
```

verified_links.csv
```
doc_id,e_id,e_type,datasets
1,1,c1,d1/kg_seed
```