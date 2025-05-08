
import os
import shutil

#recursive subgraph expansion
# Constants
SAME_AS_LIST_FILE = "data/dbp_sameas_wd.csv"

FILTERED_DBPEDIA_DIR = "data/dbpedia/filter"
CLEANED_DBPEDIA_DIR = "data/dbpedia/clean"

FILTERED_WIKIDATA_DIR = "data/wikidata/filter"
CLEANED_WIKIDATA_DIR = "data/wikidata/clean"

# URI helpers
def dbr_uri_from_filename(file_path: str) -> str:
    return "http://dbpedia.org/resource/" + os.path.basename(file_path)

def wd_uri_from_filename(file_path: str) -> str:
    return "http://www.wikidata.org/entity/" + os.path.basename(file_path)

def is_uri(token: str) -> bool:
    return token.startswith("<") and token.endswith(">")

# Load sameAs links
def load_sameas_links(path: str):
    dbp_to_wd = {}
    wd_to_dbp = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split(" ")
            if len(parts) == 2:
                dbp_uri, wd_uri = parts
                dbp_to_wd[dbp_uri] = wd_uri
                wd_to_dbp[wd_uri] = dbp_uri
    return dbp_to_wd, wd_to_dbp

# Walk and collect URIs from files
def collect_entity_uris(root_dir: str, uri_fn) -> set:
    uris = set()
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            # if filename.endswith('.nt'):
            full_path = os.path.join(dirpath, filename)
            uris.add(uri_fn(full_path))
    return uris

# Copy matching entity files based on URIs
def filter_entity_files(input_root: str, output_root: str, keep_uris: set, uri_fn):
    for dirpath, _, filenames in os.walk(input_root):
        for filename in filenames:
            # if filename.endswith('.nt'):
            file_path = os.path.join(dirpath, filename)
            entity_uri = uri_fn(file_path)
            if entity_uri in keep_uris:
                rel_path = os.path.relpath(file_path, input_root)
                target_path = os.path.join(output_root, rel_path)
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.copy(file_path, target_path)

import os

def filter_triples_in_file(input_path: str, output_path: str, valid_entities: set) -> tuple[int, int]:
    kept = 0
    removed = 0
    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                subj, pred, obj = line.split(maxsplit=2)
                obj_token = obj.rsplit(" ", 1)[0].replace("<", "").replace(">", "")  # remove trailing dot
                # print(obj_token)
                if is_uri(obj_token) and obj_token not in valid_entities:
                    removed += 1
                    continue
                outfile.write(line + '\n')
                kept += 1
            except ValueError:
                removed += 1

    # Remove file if no valid triples
    if kept == 0:
        os.remove(output_path)

    return kept, removed

def collect_nonempty_entity_uris(root_dir: str, uri_fn) -> set:
    uris = set()
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.nt'):
                full_path = os.path.join(dirpath, filename)
                if os.path.getsize(full_path) > 0:
                    uris.add(uri_fn(full_path))
    return uris

# def filter_triples_in_file(input_path: str, output_path: str, valid_entities: set) -> tuple[int, int]:
#     kept = 0
#     removed = 0
#     with open(input_path, 'r', encoding='utf-8') as infile, \
#          open(output_path, 'w', encoding='utf-8') as outfile:
#         for line in infile:
#             line = line.strip()
#             if not line or line.startswith("#"):
#                 continue
#             try:
#                 subj, pred, obj = line.split(maxsplit=2)
#                 obj_token = obj.rsplit(" ", 1)[0]  # remove trailing dot
#                 if is_uri(obj_token) and obj_token not in valid_entities:
#                     removed += 1
#                     continue
#                 outfile.write(line + '\n')
#                 kept += 1
#             except ValueError:
#                 removed += 1  # count malformed or skipped lines
#     return kept, removed


def process_dataset(input_root: str, output_root: str, valid_entities: set, uri_fn) -> tuple[int, int]:
    total_kept = 0
    total_removed = 0
    for dirpath, _, filenames in os.walk(input_root):
        for filename in filenames:
            # if filename.endswith('.nt'):
            input_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(input_path, input_root)
            output_path = os.path.join(output_root, rel_path)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            kept, removed = filter_triples_in_file(input_path, output_path, valid_entities)
            total_kept += kept
            total_removed += removed
    return total_kept, total_removed
# --- Main workflow ---

# Load links
dbpedia_to_wikidata, wikidata_to_dbpedia = load_sameas_links(SAME_AS_LIST_FILE)

# Collect entities
dbpedia_entities = collect_entity_uris(FILTERED_DBPEDIA_DIR, dbr_uri_from_filename)
wikidata_entities = collect_entity_uris(FILTERED_WIKIDATA_DIR, wd_uri_from_filename)

print("< dbpedia: "+str(len(dbpedia_entities)))
print("< wikidata: "+str(len(wikidata_entities)))

# Filter by sameAs match
linked_wikidata_uris = {dbpedia_to_wikidata[dbp] for dbp in dbpedia_entities if dbp in dbpedia_to_wikidata}
filtered_wikidata_entities = {wd for wd in wikidata_entities if wd in linked_wikidata_uris}
filtered_dbpedia_entities = {wikidata_to_dbpedia[wd] for wd in filtered_wikidata_entities}

print("> overlap: "+str(len(filtered_dbpedia_entities)))

# Filter both subject and object links
all_valid_dbpedia_uris = filtered_dbpedia_entities.union(filtered_wikidata_entities)

dbp_kept, dbp_removed = process_dataset(FILTERED_DBPEDIA_DIR, CLEANED_DBPEDIA_DIR, all_valid_dbpedia_uris, dbr_uri_from_filename)
wd_kept, wd_removed = process_dataset(FILTERED_WIKIDATA_DIR, CLEANED_WIKIDATA_DIR, all_valid_dbpedia_uris, wd_uri_from_filename)

print(f"\nSummary:")
print(f"DBpedia triples kept:   {dbp_kept:,}")
print(f"DBpedia triples removed:{dbp_removed:,}")
print(f"Wikidata triples kept:  {wd_kept:,}")
print(f"Wikidata triples removed:{wd_removed:,}")
print(f"Total triples removed:  {dbp_removed + wd_removed:,}")

def refilter_for_consistency(input_root: str, valid_entity_uris: set):
    for dirpath, _, filenames in os.walk(input_root):
        for filename in filenames:
            input_path = os.path.join(dirpath, filename)
            temp_path = input_path + ".tmp"
            kept = 0

            with open(input_path, 'r', encoding='utf-8') as infile, \
                    open(temp_path, 'w', encoding='utf-8') as outfile:
                for line in infile:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    try:
                        subj, pred, obj = line.split(maxsplit=2)
                        # obj_token = obj.rsplit(" ", 1)[0]
                        obj_token = obj.rsplit(" ", 1)[0].replace("<", "").replace(">", "")  # remove trailing dot
                        if subj not in valid_entity_uris:
                            continue
                        if is_uri(obj_token) and obj_token not in valid_entity_uris:
                            continue
                        outfile.write(line + '\n')
                        kept += 1
                    except ValueError:
                        continue

            if kept == 0:
                os.remove(input_path)
                os.remove(temp_path)
            else:
                os.replace(temp_path, input_path)

# # Collect URIs that still exist after cleanup
# final_dbpedia_uris = collect_nonempty_entity_uris(CLEANED_DBPEDIA_DIR, dbr_uri_from_filename)
# final_wikidata_uris = collect_nonempty_entity_uris(CLEANED_WIKIDATA_DIR, wd_uri_from_filename)

# final_valid_uris = final_dbpedia_uris.union(final_wikidata_uris)

# # 2nd pass: ensure no links to non-existent entities
# refilter_for_consistency(CLEANED_DBPEDIA_DIR, final_valid_uris)
# refilter_for_consistency(CLEANED_WIKIDATA_DIR, final_valid_uris)