
WIKIDATA_PROPS=[
    "http://www.w3.org/2000/01/rdf-schema#label",

    "http://www.wikidata.org/prop/direct/P136",
    "http://www.wikidata.org/prop/direct/P58",
    "http://www.wikidata.org/prop/direct/P161",
    "http://www.wikidata.org/prop/direct/P272",
    "http://www.wikidata.org/prop/direct/P57",
    "http://www.wikidata.org/prop/direct/P750",
    "http://www.wikidata.org/prop/direct/P2047",
    "http://www.wikidata.org/prop/direct/P1431",
    "http://www.wikidata.org/prop/direct/P2130",
    "http://www.wikidata.org/prop/direct/P2142",
    "http://www.wikidata.org/prop/direct/P344",
    "http://www.wikidata.org/prop/direct/P86",
    "http://www.wikidata.org/prop/direct/P1476",

    "http://www.wikidata.org/prop/direct/P569",
    "http://www.wikidata.org/prop/direct/P570",
    "http://www.wikidata.org/prop/direct/P19",
    "http://www.wikidata.org/prop/direct/P20",
    "http://www.wikidata.org/prop/direct/P106",
    "http://www.wikidata.org/prop/direct/P27",
    "http://www.wikidata.org/prop/direct/P26",
    "http://www.wikidata.org/prop/direct/P40",
    "http://www.wikidata.org/prop/direct/P166",

    "http://www.wikidata.org/prop/direct/P1448",
    "http://www.wikidata.org/prop/direct/P571",
    "http://www.wikidata.org/prop/direct/P1056",
    "http://www.wikidata.org/prop/direct/P452",
    "http://www.wikidata.org/prop/direct/P2139",
    "http://www.wikidata.org/prop/direct/P1128",
    "http://www.wikidata.org/prop/direct/P159",
]

DBPEDIA_GENERIC_PROPS=[
    "http://www.w3.org/2000/01/rdf-schema#label",


    "http://dbpedia.org/property/genre",
    "http://dbpedia.org/property/writer",
    "http://dbpedia.org/property/starring",
    "http://dbpedia.org/property/director",
    "http://dbpedia.org/property/distributor",
    "http://dbpedia.org/property/runtime",
    "http://dbpedia.org/property/producer",
    "http://dbpedia.org/property/budget",
    "http://dbpedia.org/property/gross",
    "http://dbpedia.org/property/cinematography",
    "http://dbpedia.org/property/music",
    "http://dbpedia.org/property/title",

    "http://dbpedia.org/property/birthDate",
    "http://dbpedia.org/property/deathDate",
    "http://dbpedia.org/property/birthPlace",
    "http://dbpedia.org/property/deathPlace",
    "http://dbpedia.org/property/occupation",
    "http://dbpedia.org/property/nationality",
    "http://dbpedia.org/property/spouse",
    "http://dbpedia.org/property/child",
    "http://dbpedia.org/property/award",

    "http://dbpedia.org/property/name",
    "http://dbpedia.org/property/founded",
    "http://dbpedia.org/property/products",
    "http://dbpedia.org/property/industry",
    "http://dbpedia.org/property/revenue",
    "http://dbpedia.org/property/numEmployees",
    "http://dbpedia.org/property/headquarter"
]

DBPEDIA_ONTOLOGY_PROPS=[
    "http://www.w3.org/2000/01/rdf-schema#label",

    "http://dbpedia.org/ontology/writer",
    "http://dbpedia.org/ontology/starring",
    "http://dbpedia.org/ontology/productionCompany",
    "http://dbpedia.org/ontology/director",
    "http://dbpedia.org/ontology/distributor",
    "http://dbpedia.org/ontology/runtime",
    "http://dbpedia.org/ontology/producer",
    "http://dbpedia.org/ontology/budget",
    "http://dbpedia.org/ontology/cinematography",
    "http://dbpedia.org/ontology/musicComposer",
    "http://dbpedia.org/ontology/title",

    "http://dbpedia.org/ontology/birthDate",
    "http://dbpedia.org/ontology/deathDate",
    "http://dbpedia.org/ontology/birthPlace",
    "http://dbpedia.org/ontology/deathPlace",
    "http://dbpedia.org/ontology/occupation",
    "http://dbpedia.org/ontology/nationality",
    "http://dbpedia.org/ontology/spouse",
    "http://dbpedia.org/ontology/child",
    "http://dbpedia.org/ontology/award",

    "http://dbpedia.org/ontology/foundingYear",
    "http://dbpedia.org/ontology/foundingDate",
    "http://dbpedia.org/ontology/industry",
    "http://dbpedia.org/ontology/revenue",
    "http://dbpedia.org/ontology/numberOfEmployees",
    "http://dbpedia.org/ontology/headquarter",
]

DBPEDIA_SPECIAL_PROPS=[
    #foafname
    "http://xmlns.com/foaf/0.1/name",
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://foaf.org/0.1/name",
    "http://dbpedia.org/ontology/abstract",
#    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
]

DBPEDIA_PROPS=DBPEDIA_GENERIC_PROPS+DBPEDIA_ONTOLOGY_PROPS+DBPEDIA_SPECIAL_PROPS

TYPE_MAP = {
    'film': "http://dbpedia.org/ontology/Movie",
    'person': "http://dbpedia.org/ontology/Person",
    'company': "http://dbpedia.org/ontology/Company",
}