import os
import rdflib
from typing import Callable
from filter import filter_ntriples_file_by_properties
from definition import *

def extract_abstracts(input_file: str, output_file: str):
    with open(input_file, 'r', encoding='utf-8') as infile, \
        open(output_file, 'w', encoding='utf-8') as outfile:
        for line in infile:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                _, predicate, obj = line.split(maxsplit=2)
                if predicate == "<http://dbpedia.org/ontology/abstract>" and obj.endswith("@en ."):
                    g = rdflib.Graph()
                    g.parse(data=line, format="ntriples")
                    for s, p, o in g:
                        if isinstance(o, rdflib.Literal):
                            outfile.write(str(o) + '\n')
            except ValueError:
                continue  # skip malformed lines

def process_dir_tree(input_root: str, output_root: str, func):
    print(f"Processing {input_root}")
    for dirpath, _, filenames in os.walk(input_root):
        for filename in filenames:
            input_file = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(input_file, input_root)
            output_file = os.path.join(output_root, relative_path)

            # Ensure target directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Filter and write the file
            func(input_file, output_file)

def filter_dbp(input_file, output_file):
    filter_ntriples_file_by_properties(input_file, output_file, DBPEDIA_GENERIC_PROPS)

def filter_dbo(input_file, output_file):
    filter_ntriples_file_by_properties(input_file, output_file, DBPEDIA_ONTOLOGY_PROPS)

if __name__ == "__main__":
    root = "data/splits"
    output_root = "data/final"
    for i in range(1,6):
        process_dir_tree(os.path.join(root, f"split_{i}/dbpedia"), os.path.join(output_root, f"split_{i}/abstracts"), extract_abstracts)
        process_dir_tree(os.path.join(root, f"split_{i}/dbpedia"), os.path.join(output_root, f"split_{i}/dbp"), filter_dbp)
        process_dir_tree(os.path.join(root, f"split_{i}/dbpedia"), os.path.join(output_root, f"split_{i}/dbo"), filter_dbo)

    # process_dir_tree("data/dbpedia/raw", output_root, properties)