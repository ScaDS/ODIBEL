import os
import shutil
from collections import defaultdict
from typing import Dict, Set, List
import random
import csv

def get_film_dependencies(film_dir: str, namespace: str) -> Dict[str, Set[str]]:
    film_to_entities = {}
    for fname in os.listdir(film_dir):
        # if not fname.endswith('.nt'):
        #     continue
        film_path = os.path.join(film_dir, fname)
        film_uri = namespace + fname
        entities = set()

        with open(film_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(maxsplit=2)
                if len(parts) == 3:
                    obj = parts[2].rsplit(" ", 1)[0]
                    if obj.startswith("<"+namespace):
                        entities.add(obj)
        film_to_entities[film_uri] = entities
    return film_to_entities

def split_films_min_overlap(film_to_entities: Dict[str, Set[str]], num_splits: int) -> List[Set[str]]:
    splits = [set() for _ in range(num_splits)]
    used_entities = [set() for _ in range(num_splits)]

    films_sorted = sorted(film_to_entities.items(), key=lambda x: len(x[1]), reverse=True)

    for film, entities in films_sorted:
        best_split = min(
            range(num_splits),
            key=lambda i: len(entities & used_entities[i])
        )
        splits[best_split].add(film)
        used_entities[best_split].update(entities)

    return splits, used_entities

def write_film_splits(film_dir: str, splits: List[Set[str]], output_root: str):
    for i, split in enumerate(splits):
        split_dir = os.path.join(output_root, f"split_{i+1}")
        os.makedirs(split_dir, exist_ok=True)
        for film_uri in split:
            fname = film_uri.rsplit("/", 1)[-1]
            src = os.path.join(film_dir, fname)
            dst = os.path.join(split_dir, fname)
            shutil.copy(src, dst)

def print_split_stats(splits: List[Set[str]], used_entities: List[Set[str]]):
    print("\n--- Split Statistics ---")
    for i, (films, entities) in enumerate(zip(splits, used_entities)):
        print(f"Split {i+1}:")
        print(f"  # Films:   {len(films)}")
        print(f"  # Entities:{len(entities)}")
    # Calculate overlap
    all_entity_sets = used_entities
    total_overlap = 0
    for i in range(len(all_entity_sets)):
        for j in range(i + 1, len(all_entity_sets)):
            overlap = len(all_entity_sets[i] & all_entity_sets[j])
            total_overlap += overlap
    print(f"\nTotal overlapping entities across splits: {total_overlap}")

def split_films_balanced_min_overlap(film_to_entities: Dict[str, Set[str]], num_splits: int) -> tuple[List[Set[str]], List[Set[str]]]:
    splits = [set() for _ in range(num_splits)]
    used_entities = [set() for _ in range(num_splits)]

    films_sorted = sorted(film_to_entities.items(), key=lambda x: len(x[1]), reverse=True)

    for film, entities in films_sorted:
        # For each split, compute (overlap count, current size)
        scores = [
            (len(entities & used_entities[i]), len(splits[i]), i)
            for i in range(num_splits)
        ]
        # Sort by least overlap, then smallest group size
        _, _, best_index = min(scores)
        splits[best_index].add(film)
        used_entities[best_index].update(entities)

    return splits, used_entities


def split_films_random(film_to_entities: Dict[str, Set[str]], num_splits: int, seed: int = 42) -> tuple[List[Set[str]], List[Set[str]]]:
    films = list(film_to_entities.keys())
    random.Random(seed).shuffle(films)

    splits = [set() for _ in range(num_splits)]
    used_entities = [set() for _ in range(num_splits)]

    for i, film in enumerate(films):
        split_index = i % num_splits
        splits[split_index].add(film)
        used_entities[split_index].update(film_to_entities[film])

    return splits, used_entities


# import random
# import os

# def load_matched_entities(sameas_file: str) -> list[tuple[str, str]]:
#     pairs = []
#     with open(sameas_file, 'r', encoding='utf-8') as f:
#         for line in f:
#             parts = line.strip().split()
#             if len(parts) == 2:
#                 dbp, wd = parts
#                 pairs.append((dbp, wd))
#     return pairs

# def split_matched_entities(pairs: list[tuple[str, str]], num_splits: int, seed: int = 42) -> list[list[tuple[str, str]]]:
#     random.Random(seed).shuffle(pairs)
#     splits = [[] for _ in range(num_splits)]
#     for i, pair in enumerate(pairs):
#         splits[i % num_splits].append(pair)
#     return splits

# def write_split_lists(splits: list[list[tuple[str, str]]], output_dir: str):
#     os.makedirs(output_dir, exist_ok=True)
#     for i, split in enumerate(splits):
#         dbp_file = os.path.join(output_dir, f"split_{i+1}_dbpedia.txt")
#         wd_file = os.path.join(output_dir, f"split_{i+1}_wikidata.txt")
#         with open(dbp_file, 'w', encoding='utf-8') as f_dbp, \
#              open(wd_file, 'w', encoding='utf-8') as f_wd:
#             for dbp, wd in split:
#                 f_dbp.write(dbp + "\n")
#                 f_wd.write(wd + "\n")

# def write_film_and_wikidata_splits(
#     film_dir: str,
#     wikidata_dir: str,
#     splits: List[Set[str]],
#     sameas_map: Dict[str, str],
#     output_root: str
# ):
#     for i, split in enumerate(splits):
#         split_dbp_dir = os.path.join(output_root, f"split_{i+1}", "dbpedia")
#         split_wd_dir = os.path.join(output_root, f"split_{i+1}", "wikidata")
#         os.makedirs(split_dbp_dir, exist_ok=True)
#         os.makedirs(split_wd_dir, exist_ok=True)

#         for film_uri in split:
#             fname = film_uri.rsplit("/", 1)[-1] + ".nt"
#             dbp_src = os.path.join(film_dir, fname)
#             dbp_dst = os.path.join(split_dbp_dir, fname)
#             if os.path.exists(dbp_src):
#                 shutil.copy(dbp_src, dbp_dst)

#             # Copy matching wikidata entity file
#             if film_uri in sameas_map:
#                 wd_uri = sameas_map[film_uri]
#                 wd_fname = wd_uri.rsplit("/", 1)[-1] + ".nt"
#                 wd_src = os.path.join(wikidata_dir, wd_fname)
#                 wd_dst = os.path.join(split_wd_dir, wd_fname)
#                 if os.path.exists(wd_src):
#                     shutil.copy(wd_src, wd_dst)


# def write_full_subgraphs_from_used_entities(
#     film_splits: List[Set[str]],
#     used_entities: List[Set[str]],
#     film_dir_dbp: str,
#     entity_dirs_dbp: dict,
#     film_dir_wd: str,
#     entity_dirs_wd: dict,
#     sameas_map: Dict[str, str],
#     wd_entity_to_type: Dict[str, str],
#     output_root: str
# ):
#     for i, (film_uris, entity_uris) in enumerate(zip(film_splits, used_entities)):
#         split_root = os.path.join(output_root, f"split_{i+1}")
#         dbp_dirs = {
#             "film": os.path.join(split_root, "dbpedia", "film"),
#             "person": os.path.join(split_root, "dbpedia", "person"),
#             "company": os.path.join(split_root, "dbpedia", "company"),
#         }
#         wd_dirs = {
#             "film": os.path.join(split_root, "wikidata", "film"),
#             "person": os.path.join(split_root, "wikidata", "person"),
#             "company": os.path.join(split_root, "wikidata", "company"),
#         }

#         for path in list(dbp_dirs.values()) + list(wd_dirs.values()):
#             os.makedirs(path, exist_ok=True)

#         for film_uri in film_uris:
#             film_id = film_uri.rsplit("/", 1)[-1]

#             dbp_src = os.path.join(film_dir_dbp, film_id)
#             dbp_dst = os.path.join(dbp_dirs["film"], film_id)
#             if os.path.exists(dbp_src):
#                 shutil.copy(dbp_src, dbp_dst)

#             if film_uri in sameas_map:
#                 wd_uri = sameas_map[film_uri]
#                 wd_id = wd_uri.rsplit("/", 1)[-1]
#                 wd_src = os.path.join(film_dir_wd, wd_id)
#                 wd_dst = os.path.join(wd_dirs["film"], wd_id)
#                 if os.path.exists(wd_src):
#                     shutil.copy(wd_src, wd_dst)

#         for entity_uri in entity_uris:
#             entity_id = entity_uri.rsplit("/", 1)[-1]
#             # DBpedia
#             for category, src_dir in entity_dirs_dbp.items():
#                 src_path = os.path.join(src_dir, entity_id)
#                 dst_path = os.path.join(dbp_dirs[category], entity_id)
#                 if os.path.exists(src_path):
#                     shutil.copy(src_path, dst_path)
#                     break
#             # Wikidata
#             if entity_uri in sameas_map:
#                 wd_uri = sameas_map[entity_uri]
#                 wd_id = wd_uri.rsplit("/", 1)[-1]
#                 category = wd_entity_to_type.get(wd_uri)
#                 if category and category in entity_dirs_wd:
#                     wd_src_path = os.path.join(entity_dirs_wd[category], wd_id)
#                     wd_dst_path = os.path.join(wd_dirs[category], wd_id)
#                     if os.path.exists(wd_src_path):
#                         shutil.copy(wd_src_path, wd_dst_path)


# def build_wikidata_type_map(entity_dirs_wd: dict) -> dict:
#     uri_map = {}
#     for entity_type, dir_path in entity_dirs_wd.items():
#         for fname in os.listdir(dir_path):
#             if fname.endswith(".nt"):
#                 entity_id = fname[:-3]
#                 uri = f"http://www.wikidata.org/entity/{entity_id}"
#                 uri_map[uri] = entity_type
#     return uri_map

# def load_sameas_links(path: str) -> dict:
#     """
#     Loads a DBpedia-to-Wikidata sameAs mapping from a two-column space-separated file.

#     Returns:
#         dict[str, str]: DBpedia URI → Wikidata URI
#     """
#     sameas_map = {}
#     with open(path, 'r', encoding='utf-8') as f:
#         for line in f:
#             parts = line.strip().split()
#             if len(parts) == 2:
#                 dbp_uri, wd_uri = parts
#                 sameas_map[dbp_uri] = wd_uri
#     return sameas_map

def build_uri_file_map(base_dir: str, uri_fn) -> dict:
    uri_map = {}
    for root, _, files in os.walk(base_dir):
        for fname in files:
            # if fname.endswith(".nt"):
            fpath = os.path.join(root, fname)
            uri = uri_fn(fname)
            uri_map[uri] = fpath
    return uri_map

from collections import deque

def expand_subgraph(split_entities: Set[str], uri_file_map: dict, out_dir: str):
    parsed = set()
    queue = deque(split_entities)

    

    while queue:
        uri = queue.popleft()
        if uri in parsed or uri not in uri_file_map:
            continue

        parsed.add(uri)
        src_path = uri_file_map[uri]
        dst_path = os.path.join(out_dir, src_path.split("/")[-2] + "/" + src_path.split("/")[-1])
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        shutil.copy(src_path, dst_path)

        with open(src_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(maxsplit=2)
                if len(parts) == 3:
                    obj = parts[2].rsplit(" ", 1)[0]
                    if obj.startswith("<http://dbpedia.org/resource/") or obj.startswith("<http://www.wikidata.org/entity/"):
                        queue.append(obj.strip("<>"))
    return parsed

def dbr_uri_from_filename(file_name: str) -> str:
    return "http://dbpedia.org/resource/" + os.path.splitext(file_name)[0]

def wd_uri_from_filename(file_name: str) -> str:
    return "http://www.wikidata.org/entity/" + os.path.splitext(file_name)[0]

def load_sameas_links(path: str) -> dict:
    sameas_map = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                sameas_map[parts[0]] = parts[1]
    return sameas_map


def compute_overlap_stats(parsed_sets: List[Set[str]], label="DBpedia"):
    print(f"\n--- Overlap Statistics ({label}) ---")
    total_entities_per_split = [len(s) for s in parsed_sets]
    total_overlap = 0

    for i in range(len(parsed_sets)):
        for j in range(i + 1, len(parsed_sets)):
            overlap = parsed_sets[i] & parsed_sets[j]
            overlap_size = len(overlap)
            total_overlap += overlap_size

            # Calculate percentage relative to average size of the two splits
            avg_size = (total_entities_per_split[i] + total_entities_per_split[j]) / 2
            percent = (overlap_size / avg_size) * 100 if avg_size > 0 else 0

            print(f"Split {i+1} ∩ Split {j+1}: {overlap_size} entities ({percent:.2f}%)")

    print(f"Total overlap (all pairs): {total_overlap} entities")


def write_split_entity_matches(
    parsed_sets_dbp: List[Set[str]],
    parsed_sets_wd: List[Set[str]],
    sameas_map: Dict[str, str],
    output_root: str
):
    for i, (dbp_set, wd_set) in enumerate(zip(parsed_sets_dbp, parsed_sets_wd)):
        split_name = f"split_{i+1}"
        out_file = os.path.join(output_root, split_name + "_matches.csv")
        
        with open(out_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["dbpedia_uri", "wikidata_uri"])
            for dbp_uri in dbp_set:
                wd_uri = sameas_map.get(dbp_uri)
                # if wd_uri and wd_uri in wd_set:
                writer.writerow([dbp_uri, wd_uri])

def main():
    film_dir = "data/dbpedia/clean/film"
    num_splits = 5

    print("Building film dependency map...")
    film_to_entities = get_film_dependencies(film_dir, "http://dbpedia.org/resource/")
    # film_to_entities = get_film_dependencies(film_dir, "http://www.wikidata.org/entity/")

    print(f"Found {len(film_to_entities)} films.")

    print("Creating minimal-overlap splits...")
    # splits, used_entities = split_films_min_overlap(film_to_entities, num_splits)
    splits, used_entities = split_films_balanced_min_overlap(film_to_entities, num_splits)
    # splits, used_entities = split_films_random(film_to_entities, num_splits)

    print_split_stats(splits, used_entities)

    print("Loading sameAs links...")
    sameas_map = load_sameas_links("data/dbp_sameas_wd.csv")

    print("Building URI-file maps...")
    uri_file_map_dbp = build_uri_file_map("data/dbpedia/clean", dbr_uri_from_filename)
    uri_file_map_wd = build_uri_file_map("data/wikidata/clean", wd_uri_from_filename)

    print("Expanding subgraphs into split folders...")
    parsed_sets_dbp = []
    parsed_sets_wd = []
    for i in range(num_splits):
        split_name = f"split_{i+1}"
        out_dbp = os.path.join("data/splits", split_name, "dbpedia")
        out_wd = os.path.join("data/splits", split_name, "wikidata")

        # Start from films + linked entities
        starting_dbp = splits[i] | used_entities[i]
        starting_wd = {sameas_map[uri] for uri in splits[i] if uri in sameas_map}

        parsed_dbp = expand_subgraph(starting_dbp, uri_file_map_dbp, out_dbp)
        parsed_wd = expand_subgraph(starting_wd, uri_file_map_wd, out_wd)
        
        parsed_sets_dbp.append(parsed_dbp)
        parsed_sets_wd.append(parsed_wd)

    write_split_entity_matches(
        parsed_sets_dbp=parsed_sets_dbp,
        parsed_sets_wd=parsed_sets_wd,
        sameas_map=sameas_map,
        output_root="data/splits"
    )

    compute_overlap_stats(parsed_sets_dbp, "DBpedia")
    compute_overlap_stats(parsed_sets_wd, "Wikidata")

    combined_sets = [dbp | wd for dbp, wd in zip(parsed_sets_dbp, parsed_sets_wd)]
    compute_overlap_stats(combined_sets, "Combined")
    # print("Done.")

    # entity_dirs_dbp = {
    #     "person": "data/dbpedia/clean/person",
    #     "company": "data/dbpedia/clean/company"
    # }
    # entity_dirs_wd = {
    #     "person": "data/wikidata/clean/person",
    #     "company": "data/wikidata/clean/company"
    # }

    # wd_entity_to_type = build_wikidata_type_map(entity_dirs_wd)

    # write_full_subgraphs_from_used_entities(
    #     film_splits=splits,
    #     used_entities=used_entities,
    #     film_dir_dbp="data/dbpedia/clean/film",
    #     entity_dirs_dbp=entity_dirs_dbp,
    #     film_dir_wd="data/wikidata/clean/film",
    #     entity_dirs_wd=entity_dirs_wd,
    #     sameas_map=sameas_map,
    #     wd_entity_to_type=wd_entity_to_type,
    #     output_root="data/splits"
    # )



# def main2():
#     pairs = load_matched_entities("data/dbp_sameas_wd.csv")
#     splits = split_matched_entities(pairs, num_splits=5, seed=123)
#     write_split_lists(splits, "data/splits/matched_entities")


if __name__ == "__main__":
    main()
