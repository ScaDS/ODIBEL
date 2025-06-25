# Scripts to Generate KGFlex Eval/Experiment Data

# Run

pip install -r requirements.txt

get sameas and raw files #TODO

```
data
|- dbpedia
|  |- raw
|     |- film
|     |  |- $URI_LAST_SEGMENT
|     |- person
|     |- company
|- wikdiata
   |- raw
      |- film
      |- person
      |- company
```


```
bash execute_post_fetch.sh 
```

# Backlog

- remove empty entities in clean step
- splits have to be also company person film
- encoding issue when filtering with rdf lib


# DBpediaFetcherFilm
Fetches Films, Persons and Organisations from DBpedia

## Run
### Install Requirements
```
pip install -r requirements.txt
```

### Change SPARQL_ENDPOINT in dbpedia_fetcher.py line 8 to wanted endpoint
```
SPARQL_ENDPOINT = "http://localhost:8890/sparql"
```

### Change BASE_DIRS in dbpedia_fetcher.py starting at line 14 to wanted output folders
```
BASE_DIRS = {
    "raw": "/data/benchmark/5000/dbpedia/raw",
    "dbo": "/data/benchmark/5000/dbpedia/dbo",
    "dbp": "/data/benchmark/5000/dbpedia/dbp",
    "abstracts": "/data/benchmark/5000/dbpedia//film_abstracts"
}
```
### Run Python Script
```
python dbpedia_fetcher.py film_uris.txt
```

```
# Important:
# DBpedia URI must be at first position
# see film_uris.txt
```

# WikidataFetcherFilm
Fetches Films, Persons and Organisations from Wikidata

## Run
### Install Requirements
```
pip install -r requirements.txt
```

### Change WIKIDATA_DIR in wikidata_fetcher.py line 8 to wanted output folder
```
WIKIDATA_DIR = "/data/benchmark/3500/wikidata"
```

### Run Python Script
```
python wikidata_fetcher.py film_uris.txt
```

```
# Important:
# Wikidata URI must be at second position
# see film_uris.txt
```

