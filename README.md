# Open Data Benchmark Evaluation Lab

- DBpedia Temporal
- Inc Movie KG

## Generating a Mulit-Source Knowledge Graph Integration Tests

The goal is to generate a benchmark dataset for a specific domain of this form.
- A seed KG
- An RDF source
- A JSON source
- A Text source

## Idea is to derive the data from Wikidata, DBpedia and Wikipedia

Input: Ontology and equivalentClass equivalentProperty definitons, see odibel/ontology.ttl

Given a list of URIs from a main entity type (e.g., Film)
we fetch the RDF data for these URIs filter the 

## Splits

The sources and the seed share a data for sepcific sets of entities, for providing a good foundation for evaluatinf different aspects.

Based on 5 splits we have the following data composition
- seed:        s1
- rdf_source:  s1+s2+s5
- json_source: s1+s3+s5
- text_source: s1+s4+s5
- reference:   s1+s2+s3+s4+s5

## Hierachical KG

We start with a main entity and the object data for the given 

output_dir
  hash
    data.nt
    hop_1/
      data.nt

## Construct/Clean Data


## What we have 
- dbaccess.py


1. Fetch 
2. Filter
3. Fetch Connected
4. Filter
5. Splits (on main entity)

