import os

from dotenv import load_dotenv
from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2


def generate(classes: list, name:str="", input_path: str=None, output_path: str=None):
        
    spark = get_spark_session("CrossMultiSourceKGGenerator")
    
    classes = [ "<http://dbpedia.org/ontology/" + c.replace("dbo:", "") + ">" for c in classes]
    
    if not input_path or not output_path:
        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))
    
    if not input_path:
        input_path = os.getenv("INPUT_PATH")
        if not input_path:
            raise ValueError("INPUT_PATH not set in .env file")
        
    if not output_path:
        output_path = os.getenv("OUTPUT_PATH")
        if not output_path:
            raise ValueError("OUTPUT_PATH not set in .env file")
    
    if name != "":
        name = "_" + name
        
    selected_path = os.path.join(output_path, "/dbpedia" + name + "_subgraph/selected.nt.bz2")
    distinct_path = os.path.join(output_path, "/dbpedia" + name + "_subgraph/distinct.nt.bz2")
    cleaned_types_path = os.path.join(output_path, "/dbpedia" + name + "_subgraph/cleaned_types.nt.bz2")
    schema_graph_path = os.path.join(output_path, "/dbpedia" + name + "_subgraph/schema_graph_all.csv")
    cleaned_subgraph_path = os.path.join(output_path, "/dbpedia" + name + "_subgraph/cleaned_subgraph.nt.bz2")
    final_schema_graph_path = os.path.join(output_path, "/dbpedia" + name + "_subgraph/schema_graph_final.csv")
    
    # Generate selected subgraph
    if not os.path.exists(selected_path):
        (
            rDF2.parse(spark, input_path)
            .filter_triples_by_s_types(classes)
            .write_nt(selected_path)
        )

    # Remove duplicate triples
    if not os.path.exists(distinct_path):
        (
            rDF2.parse(spark, selected_path)
            .remove_duplicate_triples(classes)
            .write_nt(cleaned_types_path)
        )
            
    # Clean rdf:type triples
    if not os.path.exists(cleaned_types_path):
        (
            rDF2.parse(spark, selected_path)
            .clean_rdf_types(classes)
            .write_nt(cleaned_types_path)
        )
    
    # Generate schema graph for all triples
    if not os.path.exists(schema_graph_path):
        (
            rDF2.parse(spark, cleaned_types_path)
            .build_schema_graph_df()
            .coalesce(1)
            .write.csv(schema_graph_path, header=True)
        )
    
    # Only keep triples where the object is found as subject
    if not os.path.exists(cleaned_subgraph_path):
        (
            rDF2.parse(spark, cleaned_types_path)
            .keep_triples_with_object_subject()
            .write_nt(cleaned_subgraph_path)
        )
    
    # Generate schema graph for final subgraph
    if not os.path.exists(final_schema_graph_path):
        (
            rDF2.parse(spark, cleaned_subgraph_path)
            .build_schema_graph_df()
            .coalesce(1)
            .write.csv(final_schema_graph_path, header=True)
        )
    
    spark.stop()
