import os
from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.management.spark_mgr import get_spark_session

spark = get_spark_session("CrossMultiSourceKGGenerator")

CLASSES = [
    "dbo:Election",
    "dbo:GovernmentAgency",
    "dbo:GovernmentCabinet",
    "dbo:GovernmentType",
    "dbo:Ideology",
    "dbo:LegalCase",
    "dbo:SupremeCourtOfTheUnitedStatesCase",
    "dbo:Legislature",
    "dbo:PoliticalConcept",
    "dbo:PoliticalParty",
    "dbo:SystemOfLaw",
    "dbo:Treaty"
]

CLASSES = [ "<http://dbpedia.org/ontology/" + c.replace("dbo:", "") + ">" for c in CLASSES]

# PATH = "/data/datasets/dbpedia_20221201/some_types.nt"
PATH = "/data/datasets/dbpedia_20221201/selected.nt.bz2"
SELECTED_PATH = "/data/datasets/dbpedia_politics_subgraph/selected_politics.nt.bz2"
CLEANED_TYPES_PATH = "/data/datasets/dbpedia_politics_subgraph/cleaned_types.nt.bz2"
SCHEMA_GRAPH_PATH = "/data/datasets/dbpedia_politics_subgraph/schema_graph_all.csv"
CLEANED_SUBGRAPH_PATH = "/data/datasets/dbpedia_politics_subgraph/cleaned_subgraph.nt.bz2"
FINAL_SCHEMA_GRAPH_PATH = "/data/datasets/dbpedia_politics_subgraph/schema_graph_final.csv"

# Generate selected subgraph
if not os.path.exists(SELECTED_PATH):
    (
        rDF2.parse(spark, PATH)
        .filter_triples_by_s_types(CLASSES)
        .write_nt(SELECTED_PATH)
    )

# Clean rdf:type triples
if not os.path.exists(CLEANED_TYPES_PATH):
    (
        rDF2.parse(spark, SELECTED_PATH)
        .clean_rdf_types(CLASSES)
        .write_nt(CLEANED_TYPES_PATH)
    )

# Generate schema graph for all triples
if not os.path.exists(SCHEMA_GRAPH_PATH):
    (
        rDF2.parse(spark, CLEANED_TYPES_PATH)
        .build_schema_graph_df()
        .coalesce(1)
        .write.csv(SCHEMA_GRAPH_PATH, header=True)
    )

# Only keep triples where the object is found as subject
if not os.path.exists(CLEANED_SUBGRAPH_PATH):
    (
        rDF2.parse(spark, CLEANED_TYPES_PATH)
        .keep_triples_with_object_subject()
        .write_nt(CLEANED_SUBGRAPH_PATH)
    )

# Generate schema graph for final subgraph
if not os.path.exists(FINAL_SCHEMA_GRAPH_PATH):
    (
        rDF2.parse(spark, CLEANED_SUBGRAPH_PATH)
        .build_schema_graph_df()
        .coalesce(1)
        .write.csv(FINAL_SCHEMA_GRAPH_PATH, header=True)
    )

spark.stop()
