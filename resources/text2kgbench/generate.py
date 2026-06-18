import os

from dotenv import load_dotenv
from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2


def generate(classes: list, properties:list, name:str="", input_path: str=None, output_path: str=None, ont_path=None):
        
    spark = get_spark_session("CrossMultiSourceKGGenerator")
    
    classes = [ "<http://dbpedia.org/ontology/" + c.replace("dbo:", "") + ">" for c in classes]

    properties = [ "<http://dbpedia.org/ontology/" + p.replace("dbo:", "") + ">" for p in properties]
    properties.append("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")
    properties.append("<http://www.w3.org/2000/01/rdf-schema#label>")
    
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

    selected_path = os.path.join(output_path, name + "_subgraph/selected.nt")
    property_filter_path =  os.path.join(output_path, name + "_subgraph/property_filter.nt")
    final_schema_graph_path = os.path.join(output_path, name + "_subgraph/schema_graph_final.csv")

    # Generate selected subgraph
    if not os.path.exists(selected_path):
        (
            rDF2.parse(spark, input_path)
            .filter_triples_by_s_types(classes, False)
            .write_nt(selected_path)
        )

    # Property Filter
    if not os.path.exists(property_filter_path):
        (
            rDF2.parse(spark, selected_path)
            .property_filter(properties)
            .write_nt(property_filter_path)
        )
        
    # Generate schema graph for final subgraph
    if not os.path.exists(final_schema_graph_path):
        (
            rDF2.parse(spark, selected_path)
            .build_schema_graph_df(properties)
            .coalesce(1)
            .write.csv(final_schema_graph_path, header=True)
        )

    spark.stop()

