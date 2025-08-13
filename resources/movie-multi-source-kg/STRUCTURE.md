

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

```
datasetA,datasetB,idA,idB,id_type
d1/kg_seed,d2/source_rdf,1,2,e
d1/kg_seed,d2/source_rdf,1,2,v
```

Verfied entites

```
datasets,e_id,e_type
[d1/kg_seed,d2/kg_seed],1,c1
```

Verfied links

```
doc_id,e_id,e_type,datasets
1,1,c1,d1/kg_seed
```