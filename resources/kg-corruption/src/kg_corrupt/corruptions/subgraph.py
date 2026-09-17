"""Family 6: cluster/subgraph neighborhood removal."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Sequence

from rdflib import Graph
from rdflib.term import Node

from kg_corrupt.corruptions.base import (
    clamp_target,
    collect_entities,
    make_result,
    sorted_triples,
)
from kg_corrupt.model import CorruptionContext, CorruptionResult, Triple
from kg_corrupt.terms import is_resource, term_to_json


class SubgraphCorruption:
    name = "subgraph"

    def corrupt(
        self,
        graph: Graph,
        eligible_triples: Sequence[Triple],
        target_removed_count: int,
        rng,
        context: CorruptionContext,
    ) -> CorruptionResult:
        eligible = sorted_triples(eligible_triples)
        target = clamp_target(target_removed_count, len(eligible))
        cfg = context.family_config
        max_hops = int(cfg.get("max_hops", 2))
        traversal = str(cfg.get("traversal", "undirected"))
        include_literals = bool(cfg.get("include_literal_properties", True))
        max_cluster_fraction = cfg.get("max_cluster_fraction")
        allow_bnodes = bool(context.allow_blank_seed_entities)
        n_eval = len(eligible)
        max_unit = None
        if max_cluster_fraction is not None:
            max_unit = max(1, int(float(max_cluster_fraction) * n_eval))

        adjacency = _adjacency(eligible, traversal=traversal)
        incident = _incident_index(eligible, include_literals=include_literals)
        seeds = collect_entities(eligible, allow_bnodes=allow_bnodes)
        rng.shuffle(seeds)

        selected_units: list[tuple[Node, frozenset[Node], frozenset[Triple]]] = []
        removed: set[Triple] = set()
        claimed: set[Node] = set()
        for seed in seeds:
            if seed in claimed:
                continue
            visited = _bfs(seed, adjacency, max_hops)
            triples = _triples_for_entities(visited, incident)
            if max_unit is not None and len(triples) > max_unit:
                continue
            extra = triples - removed
            if not extra:
                continue
            current = len(removed)
            if current >= target:
                break
            new_count = current + len(extra)
            unit = (seed, frozenset(visited), frozenset(triples))
            if new_count <= target:
                selected_units.append(unit)
                removed.update(extra)
                claimed.update(visited)
                continue
            if abs(new_count - target) < abs(current - target):
                selected_units.append(unit)
                removed.update(extra)
                claimed.update(visited)
                break
            # Neighborhood overshoots and is a worse fit: try another seed.

        visited_entities: set[Node] = set()
        for _seed, visited, _triples in selected_units:
            visited_entities.update(visited)

        realized = len(removed)
        return make_result(
            graph,
            removed,
            {
                "family": self.name,
                "traversal": traversal,
                "max_hops": max_hops,
                "include_literal_properties": include_literals,
                "seed_entity_sampling": cfg.get("seed_entity_sampling", "uniform"),
                "seed_entities": [term_to_json(unit[0]) for unit in selected_units],
                "visited_entity_count": len(visited_entities),
                "component_count": len(selected_units),
                "removed_connected_components": _component_count(
                    visited_entities, adjacency
                ),
                "target_removed_count": target_removed_count,
                "realized_removed_count": realized,
                "overshoot_or_undershoot": realized - target,
            },
        )


def _adjacency(
    eligible: Sequence[Triple],
    *,
    traversal: str,
) -> dict[Node, set[Node]]:
    adj: dict[Node, set[Node]] = defaultdict(set)
    for subject, _predicate, obj in eligible:
        if not is_resource(obj):
            continue
        adj[subject].add(obj)
        if traversal == "undirected":
            adj[obj].add(subject)
        elif traversal != "directed":
            raise ValueError(f"Unknown subgraph.traversal: {traversal}")
    return dict(adj)


def _incident_index(
    eligible: Sequence[Triple],
    *,
    include_literals: bool,
) -> dict[Node, set[Triple]]:
    index: dict[Node, set[Triple]] = defaultdict(set)
    for triple in eligible:
        subject, _predicate, obj = triple
        if is_resource(obj):
            index[subject].add(triple)
            index[obj].add(triple)
        elif include_literals:
            index[subject].add(triple)
    return dict(index)


def _bfs(seed: Node, adjacency: dict[Node, set[Node]], max_hops: int) -> set[Node]:
    visited = {seed}
    queue: deque[tuple[Node, int]] = deque([(seed, 0)])
    while queue:
        node, depth = queue.popleft()
        if depth >= max_hops:
            continue
        for neighbor in sorted(adjacency.get(node, ()), key=term_to_json):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, depth + 1))
    return visited


def _triples_for_entities(
    entities: set[Node],
    incident: dict[Node, set[Triple]],
) -> set[Triple]:
    collected: set[Triple] = set()
    for entity in entities:
        collected.update(incident.get(entity, ()))
    return collected


def _component_count(
    entities: set[Node],
    adjacency: dict[Node, set[Node]],
) -> int:
    remaining = set(entities)
    components = 0
    while remaining:
        start = remaining.pop()
        components += 1
        stack = [start]
        while stack:
            node = stack.pop()
            for neighbor in adjacency.get(node, ()):
                if neighbor in remaining:
                    remaining.remove(neighbor)
                    stack.append(neighbor)
    return components
