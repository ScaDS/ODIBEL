
# A multi source benchmark dataset for a movie KG

# Sources

* JSON
  origin


# Conf

The conf file defines all properties and how they should be mapped, maybe as a group?





# Steps

Derive DBpedia

Derive Wikidata

# Version 1 

# Version 2

# Backlog

- Overlap as parameter
- [x] Ontology!
  - how to: title, name, label
  - issue is name can not be domain of all three
- MatchCluster impl and tests (benchutils)
  - write a test for
    - [x] insert
    - [x] get
    - [x] isMatch
    - [x] getMatchToNs
- load.py with /home/marvin/project/data/acquisiton/final_dbp_1k.txt
- get the dbpedia uris with not match to wikidata as matches are only 954
