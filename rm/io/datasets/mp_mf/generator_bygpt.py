
from __future__ import annotations

import csv
import json
import random
from pathlib import Path
from typing import Dict, List, Iterable, Tuple, Optional

# Optional: import the models if the user places dataset_model.py on PYTHONPATH
try:
    from dataset_model import load_dataset, Dataset
except Exception:
    load_dataset = None
    Dataset = object  # type: ignore

# -----------------------------
# Utilities
# -----------------------------

def read_master_entities(master_csv: Path, id_col: str = "entity_id") -> List[str]:
    if not master_csv.exists():
        raise FileNotFoundError(master_csv)
    ids: List[str] = []
    with master_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if id_col not in reader.fieldnames:
            raise ValueError(f"{master_csv} must have column '{id_col}', got {reader.fieldnames}")
        for row in reader:
            ids.append(row[id_col])
    return ids

def write_csv(path: Path, header: List[str], rows: Iterable[Iterable]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)

# -----------------------------
# Overlap generator (pairwise targets)
# -----------------------------

def _feasible_or_raise(split_sizes: Dict[str, int], overlap_pct: float):
    # Simple necessary condition (not sufficient in all general cases):
    # For each split i, sum_j target_ij <= size_i
    # Here target_ij = floor(p * min(size_i, size_j)); with equal sizes n: (S-1)*p <= 1
    split_ids = list(split_sizes.keys())
    for i in split_ids:
        si = split_sizes[i]
        s = 0
        for j in split_ids:
            if i == j: 
                continue
            sj = split_sizes[j]
            s += int(overlap_pct * min(si, sj))
        if s > si:
            raise ValueError(f"Infeasible overlap: for split {i}, required shared entities {s} exceed size {si}. "
                             f"Consider reducing overlap_pct or split sizes, or increase number of unique entities.")

def generate_split_entity_sets(
    all_entity_ids: List[str],
    split_sizes: Dict[str, int],
    overlap_pct: float,
    seed: Optional[int] = 1337
) -> Dict[str, List[str]]:
    """Greedy generator that satisfies exact pairwise overlap counts:
       target_ij = floor(overlap_pct * min(size_i, size_j)).
       It allocates disjoint fresh entities per pair to avoid over-constraining,
       then fills each split with unique singletons to reach its target size.
    """
    if seed is not None:
        random.seed(seed)

    _feasible_or_raise(split_sizes, overlap_pct)

    split_ids = list(split_sizes.keys())
    # Shuffle a working pool of entity IDs
    pool = all_entity_ids.copy()
    random.shuffle(pool)
    pool_i = 0

    def take_fresh(k: int) -> List[str]:
        nonlocal pool_i
        if pool_i + k > len(pool):
            raise RuntimeError("Not enough entities in master list to realize the requested sizes/overlaps.")
        chunk = pool[pool_i: pool_i + k]
        pool_i += k
        return chunk

    # Initialize empty sets
    S: Dict[str, set] = {sid: set() for sid in split_ids}

    # 1) Create disjoint per-pair overlaps
    targets: Dict[Tuple[str, str], int] = {}
    for i_idx in range(len(split_ids)):
        for j_idx in range(i_idx + 1, len(split_ids)):
            a, b = split_ids[i_idx], split_ids[j_idx]
            t = int(overlap_pct * min(split_sizes[a], split_sizes[b]))
            targets[(a, b)] = t
            if t > 0:
                shared = take_fresh(t)
                S[a].update(shared)
                S[b].update(shared)

    # 2) Fill remaining with unique singletons per split
    for sid in split_ids:
        need = split_sizes[sid] - len(S[sid])
        if need < 0:
            raise RuntimeError(f"Split {sid} overfilled during overlap allocation.")
        if need > 0:
            uniques = take_fresh(need)
            S[sid].update(uniques)

    # 3) Sanity check sizes and overlaps
    for sid in split_ids:
        assert len(S[sid]) == split_sizes[sid], f"Size mismatch for {sid}"
    # Check pairwise overlaps match targets exactly
    for (a, b), t in targets.items():
        got = len(S[a] & S[b])
        if got != t:
            raise RuntimeError(f"Pair ({a},{b}) overlap mismatch: got {got}, target {t}")

    # Return as ordered lists (deterministic order via sorted)
    return {sid: sorted(S[sid]) for sid in split_ids}

# -----------------------------
# Filesystem scaffolding
# -----------------------------

def scaffold_split(root: Path, split_id: str):
    base = root / "splits" / split_id
    # index
    (base / "index").mkdir(parents=True, exist_ok=True)
    # kg
    for kind in ["reference", "seed"]:
        (base / "kg" / kind / "data").mkdir(parents=True, exist_ok=True)
        (base / "kg" / kind / "meta").mkdir(parents=True, exist_ok=True)
    # sources
    for st in ["rdf", "json", "text"]:
        (base / "sources" / st / "data").mkdir(parents=True, exist_ok=True)
        (base / "sources" / st / "meta").mkdir(parents=True, exist_ok=True)
    return base

def write_index_entities(root: Path, split_id: str, entity_ids: List[str]):
    path = root / "splits" / split_id / "index" / "entities.csv"
    rows = ((eid,) for eid in entity_ids)
    write_csv(path, ["entity_id"], rows)
    return path

def write_placeholder_files(root: Path, split_id: str, n_docs: int = 5):
    base = root / "splits" / split_id
    # Minimal meta files with headers only
    write_csv(base / "kg" / "reference" / "meta" / "verified_matches.csv",
              ["left_dataset","right_dataset","left_id","right_id","match_type"], [])
    write_csv(base / "sources" / "rdf" / "meta" / "verified_entities.csv",
              ["dataset","entity_id","entity_type"], [])
    write_csv(base / "sources" / "json" / "meta" / "verified_entities.csv",
              ["dataset","entity_id","entity_type"], [])
    write_csv(base / "sources" / "text" / "meta" / "verified_links.csv",
              ["doc_id","entity_id","entity_type","dataset"], [])

    # Seed/reference placeholders
    (base / "kg" / "seed" / "data" / "seed.nt").write_text("", encoding="utf-8")
    (base / "kg" / "reference" / "data" / "part-00001.nt").write_text("", encoding="utf-8")

    # A few text docs so doc_id references have targets
    for i in range(1, n_docs + 1):
        (base / "sources" / "text" / "data" / f"doc-{i:05d}.txt").write_text(
            f"Placeholder document {i} for {split_id}.", encoding="utf-8"
        )

# -----------------------------
# Public API
# -----------------------------

def generate_dataset_splits(
    dataset_root: Path,
    master_entities_csv: Path,
    split_sizes: Dict[str, int],
    overlap_pct: float,
    seed: Optional[int] = 1337,
    create_placeholders: bool = True,
) -> Dict[str, List[str]]:
    """Generate index/entities.csv for each split under dataset_root/splits/<split_id>,
    guaranteeing exact pairwise overlaps and target sizes (if feasible).
    Optionally scaffold KG and source directories with minimal placeholder files.
    Returns the mapping split_id -> list(entity_id).
    """
    dataset_root = dataset_root.resolve()
    dataset_root.mkdir(parents=True, exist_ok=True)

    # Ensure directories exist
    for sid in split_sizes.keys():
        scaffold_split(dataset_root, sid)

    # Load master entity IDs
    ids = read_master_entities(master_entities_csv)

    # Generate sets
    split_to_ids = generate_split_entity_sets(ids, split_sizes, overlap_pct, seed=seed)

    # Write index files
    for sid, id_list in split_to_ids.items():
        write_index_entities(dataset_root, sid, id_list)
        if create_placeholders:
            write_placeholder_files(dataset_root, sid)

    # Write a minimal provenance plan for overlap (users can replace with full PROV-O)
    prov_dir = dataset_root / "provenance"
    prov_dir.mkdir(parents=True, exist_ok=True)
    plan = {
        "@id": "plan:overlap-constraints",
        "@type": ["prov:Plan", "x:OverlapPlan"],
        "x:overlapGlobalPct": overlap_pct,
        "x:tolerancePct": 0.001,
        "x:randomSeed": seed,
        "x:splitIds": list(split_sizes.keys())
    }
    (prov_dir / "prov.jsonld").write_text(json.dumps(plan, indent=2), encoding="utf-8")

    return split_to_ids

# -----------------------------
# CLI
# -----------------------------

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Generate dataset splits with pairwise overlap.")
    ap.add_argument("--root", required=True, help="Dataset root directory")
    ap.add_argument("--master", required=True, help="Path to entities/master_entities.csv")
    ap.add_argument("--splits", required=True, nargs="+", help="Split IDs, e.g., split_01 split_02 split_03 split_04")
    ap.add_argument("--size", required=True, type=int, help="Size per split (for equal sizes)")
    ap.add_argument("--overlap", required=True, type=float, help="Pairwise overlap percentage (e.g., 0.10 for 10%%)")
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--no-placeholders", action="store_true", help="Do not create placeholder KG/source files")
    args = ap.parse_args()

    split_sizes = {sid: args.size for sid in args.splits}
    split_to_ids = generate_dataset_splits(
        Path(args.root),
        Path(args.master),
        split_sizes,
        overlap_pct=args.overlap,
        seed=args.seed,
        create_placeholders=not args.no_placeholders
    )

    # Print realized overlaps
    from itertools import combinations
    for a, b in combinations(sorted(split_to_ids.keys()), 2):
        inter = len(set(split_to_ids[a]) & set(split_to_ids[b]))
        print(f"Overlap {a} ∩ {b} = {inter} ({inter/args.size:.2%} of each)")


if __name__ == "__main__":
    main()
