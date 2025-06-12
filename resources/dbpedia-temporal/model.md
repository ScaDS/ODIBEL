# model.md

## 1. CSV
```
head,       rel,                tail,                   tStart,      tEnd,     rStart, rEnd
dbo:Leipzig,dbo:populationTotal,"510043"^<xsd:integer>, 2019-08-13, 2019-10-28, 389,   647
dbo:Leipzig,dbo:populationTotal,"605407"^<xsd:integer>, 2019-10-28, 2021-04-11, 647,   250
...
```


## 2. Named Graph

```turtle
@prefix dbo: <http://dbpedia.org/ontology/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix graph: <http://example.org/graph/> .
@prefix rel: <http://example.org/relation/> .

graph:population1 {
    dbo:Leipzig dbo:populationTotal "510043"^^xsd:integer;
        rel:tStart "2019-08-13"^^xsd:date;
        rel:tEnd "2019-10-28"^^xsd:date;
        rel:rStart 389;
        rel:rEnd 647.
}

graph:population2 {
    dbo:Leipzig dbo:populationTotal "605407"^^xsd:integer;
        rel:tStart "2019-10-28"^^xsd:date;
        rel:tEnd "2021-04-11"^^xsd:date;
        rel:rStart 647;
        rel:rEnd 250.
}
```


## 3. Reification

```turtle
@prefix dbo: <http://dbpedia.org/ontology/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rel: <http://example.org/relation/> .

_:b1 a rdf:Statement ;
    rdf:subject dbo:Leipzig ;
    rdf:predicate dbo:populationTotal ;
    rdf:object "510043"^^xsd:integer ;
    rel:tStart "2019-08-13"^^xsd:date ;
    rel:tEnd "2019-10-28"^^xsd:date ;
    rel:rStart 389 ;
    rel:rEnd 647 .

_:b2 a rdf:Statement ;
    rdf:subject dbo:Leipzig ;
    rdf:predicate dbo:populationTotal ;
    rdf:object "605407"^^xsd:integer ;
    rel:tStart "2019-10-28"^^xsd:date ;
    rel:tEnd "2021-04-11"^^xsd:date ;
    rel:rStart 647 ;
    rel:rEnd 250 .
```


## 4. RDF-Star

```turtle
@prefix dbo: <http://dbpedia.org/ontology/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rel: <http://example.org/relation/> .

<<dbo:Leipzig dbo:populationTotal "510043"^^xsd:integer>> 
    rel:tStart "2019-08-13"^^xsd:date ;
    rel:tEnd "2019-10-28"^^xsd:date ;
    rel:rStart 389 ;
    rel:rEnd 647 .

<<dbo:Leipzig dbo:populationTotal "605407"^^xsd:integer>> 
    rel:tStart "2019-10-28"^^xsd:date ;
    rel:tEnd "2021-04-11"^^xsd:date ;
    rel:rStart 647 ;
    rel:rEnd 250 .
```
