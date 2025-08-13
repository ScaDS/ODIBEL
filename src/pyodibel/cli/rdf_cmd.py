import click
from typing import Optional

from pyodibel.rdf_ops.extract import extract_subgraph_recursive

import rdflib

# @click.argument("pipeline_file", type=click.Path(exists=True))
# @click.option(
#     "--output", 
#     "-o", 
#     type=click.Path(), 
#     help="Output directory for results"
# )
# @click.option(
#     "--temp-dir", 
#     "-t", 
#     type=click.Path(), 
#     help="Temporary directory for intermediate files"
# )
# @click.option(
#     "--dry-run", 
#     is_flag=True, 
#     help="Show execution plan without running"
# )
@click.group()
@click.pass_context
def rdf_cmd(ctx: click.Context):
    pass

@click.command("extract-subgraph")
@click.option("--root-entity", type=str)
@click.argument("graph_path", type=click.Path(exists=True))
@click.pass_context
def rdf_extract_subgraph(ctx: click.Context, root_entity: str, graph_path: str):
    click.echo(f"RDF Extract Subgraph Command")
    rdf_graph = rdflib.Graph()
    rdf_graph.parse(graph_path)
    sub_graph = extract_subgraph_recursive(root_entity, rdf_graph)
    print(sub_graph.serialize(format="turtle"))

rdf_cmd.add_command(rdf_extract_subgraph)