package ai.scads.odibel.datasets.wikitext.transform

/**
 *
 * Example Output:
 *
 * \@prefix dbr: <http://dbpedia.org/resource/> .
 * \@prefix dbo: <http://dbpedia.org/ontology/> .
 * \@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
 * \@prefix ex:  <http://example.org/properties/> .
 *
 *         dbr:Leipzig
 *         a dbo:City ;
 *         ex:revision [
 *         dbo:populationTotal "510043"^^xsd:integer ;
 *         ex:start "2019-08-13T12:02:50"^^xsd:dateTime ;
 *         ex:end   "2019-10-28T19:35:51"^^xsd:dateTime
 *         ] ,
 *         [
 *         dbo:populationTotal "600000"^^xsd:integer ;
 *         ex:start "2020-01-01T00:00:00"^^xsd:dateTime ;
 *         ex:end   "2020-12-31T23:59:59"^^xsd:dateTime
 *         ] .
 */
class ToRDFPropertyTKG {
  // TODO implement for reviewer 1
}
