from itertools import combinations
from math import comb

def build_exact_subsets(ids, num_subsets, overlap_ratio, subset_size):
    """
    ids: list of IDs (hashable)
    X: number of subsets
    O: desired pairwise overlap ratio (e.g. 0.25)
    k: size of each subset (must make t = O*k an integer)
    """
    N = len(ids)
    t_float = overlap_ratio * subset_size
    if abs(round(t_float) - t_float) > 1e-9:
        raise ValueError("Choose k so that t = O*k is an integer.")
    t = int(round(t_float))
    if subset_size < t * (num_subsets - 1):
        raise ValueError(f"subset_size too small. Need subset_size ≥ t*(num_subsets-1) = {t*(num_subsets-1)}.")
    
    needed = t * comb(num_subsets, 2) + (subset_size - t*(num_subsets-1)) * num_subsets
    if N < needed:
        raise ValueError(f"Not enough IDs. Need at least {needed}, have {N}.")
    
    ids_iter = iter(ids)
    # prepare empty subsets
    subsets = [set() for _ in range(num_subsets)]
    
    # allocate pairwise-overlap buckets
    for i, j in combinations(range(num_subsets), 2):
        pair_items = {next(ids_iter) for _ in range(t)}
        subsets[i] |= pair_items
        subsets[j] |= pair_items
    
    # top up with private items
    for i in range(num_subsets):
        need = subset_size - len(subsets[i])
        private_items = {next(ids_iter) for _ in range(need)}
        subsets[i] |= private_items
    
    # return as lists (stable order)
    return [list(s) for s in subsets]

def validate_overlaps(subsets):
    """
    subsets: list of iterable IDs for each subset
    returns: dict { "Si-Sj": overlap_ratio }
    """
    # Ensure subsets are sets for fast intersection
    sets = [set(s) for s in subsets]
    k = len(sets[0])  # assuming all subsets have same length
    overlaps = {}
    
    for i, j in combinations(range(len(sets)), 2):
        inter_size = len(sets[i] & sets[j])
        ratio = inter_size / k
        print(inter_size, k, ratio)
        overlaps[f"S{i+1}-S{j+1}"] = ratio
    
    return overlaps

def test_build_subsets():
    # ids: list of IDs (hashable)
    # X: number of subsets
    # O: desired pairwise overlap ratio (e.g. 0.25)
    # k: size of each subset (must make t = O*k an integer)
    ids = range(1, 10000)
    X = 4
    O = 0.2
    k = 250
    subsets = build_exact_subsets(ids, X, O, k)
    print(subsets)
    print(validate_overlaps(subsets))

if __name__ == "__main__":
    test_build_subsets()