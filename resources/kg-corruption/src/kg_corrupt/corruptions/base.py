"""Shared helpers for corruption strategies."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TypeVar

from rdflib import Graph
from rdflib.term import Node

from kg_corrupt.model import CorruptionResult, Triple
from kg_corrupt.terms import is_bnode, is_literal, is_resource, triple_sort_key

T = TypeVar("T")


def build_retained_graph(graph: Graph, removed: set[Triple]) -> Graph:
    """Return a new graph equal to ``graph \\ removed``. Does not mutate ``graph``."""
    retained = Graph()
    for triple in graph:
        if triple not in removed:
            retained.add(triple)
    return retained


def make_result(
    graph: Graph,
    removed: set[Triple],
    metadata: dict,
) -> CorruptionResult:
    return CorruptionResult(
        retained_graph=build_retained_graph(graph, removed),
        removed_triples=set(removed),
        metadata=metadata,
    )


def clamp_target(target: int, n_eligible: int) -> int:
    return max(0, min(int(target), n_eligible))


def sorted_triples(triples: Sequence[Triple]) -> list[Triple]:
    return sorted(triples, key=triple_sort_key)


def entity_from_triple(triple: Triple, *, allow_bnodes: bool) -> list[Node]:
    subject, _predicate, obj = triple
    entities: list[Node] = []
    if is_resource(subject) and (allow_bnodes or not is_bnode(subject)):
        entities.append(subject)
    if is_resource(obj) and (allow_bnodes or not is_bnode(obj)):
        entities.append(obj)
    return entities


def collect_entities(
    triples: Sequence[Triple],
    *,
    allow_bnodes: bool,
) -> list[Node]:
    seen: set[Node] = set()
    ordered: list[Node] = []
    for triple in sorted_triples(triples):
        for entity in entity_from_triple(triple, allow_bnodes=allow_bnodes):
            if entity not in seen:
                seen.add(entity)
                ordered.append(entity)
    return ordered


def is_literal_object(triple: Triple) -> bool:
    return is_literal(triple[2])


def select_units_nearest_target(
    units: Sequence[T],
    unit_triples: Callable[[T], Sequence[Triple] | set[Triple]],
    target: int,
) -> tuple[list[T], set[Triple]]:
    """Greedy unit selection with a one-step nearest-target boundary.

    1. Add units while the cumulative triple count stays at or below target.
    2. For the next unit that would exceed target, keep or skip it according
       to which choice has smaller absolute error.
    3. Stop. Units are never split.
    """
    selected: list[T] = []
    removed: set[Triple] = set()
    for unit in units:
        extra = set(unit_triples(unit)) - removed
        if not extra:
            continue
        current = len(removed)
        if current >= target:
            break
        new_count = current + len(extra)
        if new_count <= target:
            selected.append(unit)
            removed.update(extra)
            continue
        if abs(new_count - target) < abs(current - target):
            selected.append(unit)
            removed.update(extra)
        break
    return selected, removed


def weighted_sample_without_replacement(
    items: Sequence[T],
    weights: Sequence[float],
    k: int,
    rng,
) -> list[T]:
    """Efraimidis–Spirakis weighted sampling without replacement."""
    if k <= 0 or not items:
        return []
    keyed: list[tuple[float, int, T]] = []
    for index, (item, weight) in enumerate(zip(items, weights)):
        if weight <= 0:
            continue
        u = rng.random()
        if u <= 0.0:
            u = 1e-30
        keyed.append((u ** (1.0 / weight), index, item))
    keyed.sort(key=lambda row: (-row[0], row[1]))
    return [item for _key, _index, item in keyed[:k]]


def weighted_shuffle(items: Sequence[T], weights: Sequence[float], rng) -> list[T]:
    return weighted_sample_without_replacement(items, weights, len(items), rng)
