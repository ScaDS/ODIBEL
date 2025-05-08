import os
import definition

def filter_ntriples_file_by_properties(input_file: str, output_file: str, properties: list):
    """
    Reads an N-Triples file, filters triples by given predicate properties, and writes to a new file.

    Args:
        input_file (str): Path to the input N-Triples file.
        output_file (str): Path to the output file to write filtered triples.
        properties (list): List of predicate URIs (without angle brackets).
    """
    # Normalize to <angle-bracketed> form and store in a set for efficient lookup
    prop_set = {f"<{prop}>" if not (prop.startswith("<") and prop.endswith(">")) else prop for prop in properties}

    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        for line in infile:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                _, predicate, _ = line.split(maxsplit=2)
                if predicate in prop_set:
                    outfile.write(line + '\n')
            except ValueError:
                continue  # skip malformed lines


def filter_ntriples_dir_tree(input_root: str, output_root: str, properties: list):
    properties_set = {f"<{prop}>" if not (prop.startswith("<") and prop.endswith(">")) else prop
                      for prop in properties}

    for dirpath, _, filenames in os.walk(input_root):
        for filename in filenames:
            input_file = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(input_file, input_root)
            output_file = os.path.join(output_root, relative_path[0:-3])

            # Ensure target directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Filter and write the file
            filter_ntriples_file_by_properties(input_file, output_file, properties_set)

filter_ntriples_dir_tree(
    input_root="data/dbpedia/raw",
    output_root="data/dbpedia/filter",
    properties=definition.DBPEDIA_PROPS
)

filter_ntriples_dir_tree(
    input_root="data/wikidata/raw",
    output_root="data/wikidata/filter",
    properties=definition.WIKIDATA_PROPS
)

#######################################

# from rdflib import Graph, FOAF, RDF, RDFS, Literal
# from collections import defaultdict
# from typing import Dict, List



# Input RDF file (in N-Triples format)
# IN = "/home/marvin/papers/tmp/kg-testdata/bench1/samples/wikidata_Film.100.nt"
# OUT = "/data/pipelines_by_source/csv/wikidata_film.nt"
#IN="/data/pipelines_by_source/csv/dbpedia_film_dbp.nt"
#OUT="/data/pipelines_by_source/csv/dbpedia_film_dbp_filtered.nt"

# Load the RDF data
# g = Graph()
# g.parse(IN, format="nt")

# FILTER_TYPES: set[str] = set([])

# FILTER_PREDICATES_NS = set(["http://www.w3.org/2002/07/owl#", str(RDF), str(RDFS)])

# def filter_by_prop_list():

#     # Step 1: Find all entities (subjects) of the desired RDF types
#     entities_by_type = set()
#     for s, p, o in g.triples((None, RDF.type, None)):
#         if str(o) in FILTER_TYPES:
#             entities_by_type.add(s)

#     if FILTER_TYPES == set():
#         for s, _, _ in g.triples((None, None, None)):
#             entities_by_type.add(s)

#     # Step 2: Collect filtered properties for those entities
#     data: dict[str,dict]  = defaultdict(dict)
#     filtered_graph = Graph()

#     for s in entities_by_type:
#         # Always include the type triple
#         for o in g.objects(s, RDF.type):
#             if str(o) in FILTER_TYPES:
#                 filtered_graph.add((s, RDF.type, o))

#     for s, p, o in g:
#         if s not in entities_by_type:
#             continue

#         pred_str = str(p)

#         # Filter by predicate list or namespace
#         if FILTER_PREDICATES and pred_str not in FILTER_PREDICATES:
#             # If specific predicates are given, skip others
#             continue
#         elif not FILTER_PREDICATES and FILTER_PREDICATES_NS:
#             if not any(pred_str.startswith(ns) for ns in FILTER_PREDICATES_NS):
#                 continue

#         # Add triple to the filtered graph
#         filtered_graph.add((s, p, o))

#         # Store value, distinguish by whether it's a literal or URI
#         key = f"{pred_str}_literal" if isinstance(o, Literal) else f"{pred_str}_uri"
        
#         if key in data[s]:
#             if isinstance(data[s][key], list):
#                 data[s][key].append(str(o))
#             else:
#                 data[s][key] = [data[s][key], str(o)]
#         else:
#             data[s][key] = str(o)

#     with open(OUT, "w") as f:
#         f.write(filtered_graph.serialize(format="ntriples"))

#     print(f"{IN} Filterd TO {OUT}")
# # (Optional) print or further process the `data` dict
# # for subject, props in data.items():
# #     print(f"Entity: {subject}")
# #     for k, v in props.items():
# #         print(f"  {k}: {v}")
