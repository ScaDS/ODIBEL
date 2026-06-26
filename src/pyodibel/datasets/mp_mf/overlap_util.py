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
        raise ValueError(
            f"subset_size too small. Need subset_size ≥ t*(num_subsets-1) = {t * (num_subsets - 1)}."
        )

    needed = t * comb(num_subsets, 2) + (subset_size - t * (num_subsets - 1)) * num_subsets
    if N < needed:
        raise ValueError(f"Not enough IDs. Need at least {needed}, have {N}.")

    ids_iter = iter(ids)
    subsets = [set() for _ in range(num_subsets)]

    for i, j in combinations(range(num_subsets), 2):
        pair_items = {next(ids_iter) for _ in range(t)}
        subsets[i] |= pair_items
        subsets[j] |= pair_items

    for i in range(num_subsets):
        need = subset_size - len(subsets[i])
        private_items = {next(ids_iter) for _ in range(need)}
        subsets[i] |= private_items

    return [list(s) for s in subsets]


def validate_overlaps(subsets):
    """
    subsets: list of iterable IDs for each subset
    returns: dict { "Si-Sj": overlap_ratio }
    """
    sets = [set(s) for s in subsets]
    k = len(sets[0])
    overlaps = {}

    for i, j in combinations(range(len(sets)), 2):
        inter_size = len(sets[i] & sets[j])
        ratio = inter_size / k
        overlaps[f"S{i + 1}-S{j + 1}"] = ratio

    return overlaps


def pairwise_entity_overlaps(entity_sets: list[set[str]]) -> dict[str, dict[str, int | float]]:
    """
    Compute pairwise overlap between entity sets.

    Returns a dict keyed by ``split_i-split_j`` with intersection count and
    overlap percentages relative to each split's entity count.
    """
    if not entity_sets:
        return {}

    overlaps: dict[str, dict[str, int | float]] = {}
    for i, j in combinations(range(len(entity_sets)), 2):
        left = entity_sets[i]
        right = entity_sets[j]
        inter = left & right
        overlaps[f"split_{i}-split_{j}"] = {
            "count": len(inter),
            "pct_left": len(inter) / len(left) if left else 0.0,
            "pct_right": len(inter) / len(right) if right else 0.0,
        }
    return overlaps
