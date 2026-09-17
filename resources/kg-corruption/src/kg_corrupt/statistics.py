"""Root-graph and benchmark-run statistics."""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from typing import Any, Iterable, Sequence

from rdflib import Graph
from rdflib.term import Node

from kg_corrupt.eligibility import EligibilitySets
from kg_corrupt.model import Triple
from kg_corrupt.provenance import ProvenanceIndex
from kg_corrupt.terms import is_resource, term_to_json


def _percentile(sorted_values: list[int], q: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = (len(sorted_values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_values[lo])
    return float(sorted_values[lo] * (hi - pos) + sorted_values[hi] * (pos - lo))


def _distribution_summary(values: list[int]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
        "median": statistics.median(ordered),
        "p90": _percentile(ordered, 0.90),
        "p99": _percentile(ordered, 0.99),
    }


def _connected_component_sizes(eligible: Sequence[Triple]) -> list[int]:
    adj: dict[Node, set[Node]] = defaultdict(set)
    nodes: set[Node] = set()
    for subject, _predicate, obj in eligible:
        nodes.add(subject)
        if is_resource(obj):
            nodes.add(obj)
            adj[subject].add(obj)
            adj[obj].add(subject)
    remaining = set(nodes)
    sizes: list[int] = []
    while remaining:
        start = remaining.pop()
        stack = [start]
        size = 1
        while stack:
            node = stack.pop()
            for neighbor in adj.get(node, ()):
                if neighbor in remaining:
                    remaining.remove(neighbor)
                    stack.append(neighbor)
                    size += 1
        sizes.append(size)
    return sizes


def compute_root_stats(
    graph: Graph,
    eligibility: EligibilitySets,
    provenance: ProvenanceIndex | None = None,
) -> dict[str, Any]:
    eligible = eligibility.eligible
    subjects = {triple[0] for triple in eligible}
    resource_objects = {triple[2] for triple in eligible if is_resource(triple[2])}
    entities = subjects | resource_objects
    predicates = {triple[1] for triple in eligible}

    pred_freq = Counter(term_to_json(triple[1]) for triple in eligible)
    degrees: dict[Node, int] = defaultdict(int)
    for subject, _predicate, obj in eligible:
        degrees[subject] += 1
        if is_resource(obj):
            degrees[obj] += 1

    stats: dict[str, Any] = {
        "total_triples": len(eligibility.all_triples),
        "eligible_triples": eligibility.eligible_count,
        "protected_triples": eligibility.protected_count,
        "unique_subjects": len(subjects),
        "unique_resource_objects": len(resource_objects),
        "unique_entities": len(entities),
        "unique_predicates": len(predicates),
        "predicate_frequency_distribution": _distribution_summary(list(pred_freq.values())),
        "predicate_frequencies": dict(sorted(pred_freq.items(), key=lambda kv: (-kv[1], kv[0]))),
        "entity_degree_distribution": _distribution_summary(list(degrees.values())),
        "connected_component_sizes": _distribution_summary(
            _connected_component_sizes(eligible)
        ),
    }
    if provenance is not None and provenance.source_to_triples:
        sizes = [len(triples) for triples in provenance.source_to_triples.values()]
        stats["source_unit_count"] = len(provenance.source_to_triples)
        stats["source_unit_size_distribution"] = _distribution_summary(sizes)
        stats["mapped_triple_count"] = provenance.mapped_triple_count()
        stats["unmatched_provenance_records"] = provenance.unmatched_records
    else:
        stats["source_unit_count"] = 0
    return stats


def summarize_manifest_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (str(row["family"]), float(row["target_removed_fraction"]))
        grouped[key].append(row)
    summaries = []
    for (family, fraction), items in sorted(grouped.items()):
        realized = [float(item["realized_removed_fraction"]) for item in items]
        retained = [int(item["retained_eval_count"]) for item in items]
        summaries.append(
            {
                "family": family,
                "target_removed_fraction": fraction,
                "n": len(items),
                "mean_realized_corruption": sum(realized) / len(realized),
                "std_realized_corruption": (
                    statistics.pstdev(realized) if len(realized) > 1 else 0.0
                ),
                "min_realized_corruption": min(realized),
                "max_realized_corruption": max(realized),
                "mean_retained_eval_count": sum(retained) / len(retained),
            }
        )
    return summaries
