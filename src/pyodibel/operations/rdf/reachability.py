"""In-memory class-rooted subgraph reachability over N-Triples."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Literal, Sequence

from pyodibel.operations.rdf.nt_io import Triple, load_nt, write_nt
from pyodibel.operations.rdf.uris import is_rdf_type_predicate, normalize_class_uri

TraversalDirection = Literal["forward", "backward", "both"]
MainClassScope = Literal["reachable", "seeds"]


@dataclass
class NTripleGraph:
    triples: list[Triple] = field(default_factory=list)
    forward: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    backward: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    types_by_entity: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))

    @classmethod
    def from_triples(cls, triples: Iterable[Triple]) -> "NTripleGraph":
        graph = cls()
        graph.add_triples(triples)
        return graph

    def add_triples(self, triples: Iterable[Triple]) -> None:
        for subject, predicate, obj, is_literal in triples:
            self.triples.append((subject, predicate, obj, is_literal))
            if is_rdf_type_predicate(predicate):
                self.types_by_entity[subject].add(obj)
            if not is_literal and subject != obj:
                self.forward[subject].add(obj)
                self.backward[obj].add(subject)

    @property
    def entity_count(self) -> int:
        return len({subject for subject, _, _, _ in self.triples})


def reachable_entities(
    graph: NTripleGraph,
    root_classes: str | Sequence[str],
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
) -> set[str]:
    if isinstance(root_classes, str):
        normalized_classes = {normalize_class_uri(root_classes)}
    else:
        normalized_classes = {normalize_class_uri(value) for value in root_classes}
    if not normalized_classes:
        raise ValueError("root_classes must not be empty")
    if max_hops is not None and max_hops < 0:
        raise ValueError("max_hops must be >= 0")

    seeds = {
        entity
        for entity, types in graph.types_by_entity.items()
        if types & normalized_classes
    }
    if not seeds:
        return set()

    reachable = set(seeds)
    frontier = set(seeds)
    hop = 0

    while frontier:
        if max_hops is not None and hop >= max_hops:
            break

        next_frontier: set[str] = set()
        for entity in frontier:
            neighbors: set[str] = set()
            if direction in ("forward", "both"):
                neighbors |= graph.forward.get(entity, set())
            if direction in ("backward", "both"):
                neighbors |= graph.backward.get(entity, set())

            for neighbor in neighbors:
                if neighbor not in reachable:
                    reachable.add(neighbor)
                    next_frontier.add(neighbor)

        if not next_frontier:
            break

        frontier = next_frontier
        hop += 1

    return reachable


def reachable_entities_from_seeds(
    graph: NTripleGraph,
    seeds: str | Sequence[str],
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
) -> set[str]:
    if isinstance(seeds, str):
        seed_set = {seeds}
    else:
        seed_set = set(seeds)
    if not seed_set:
        return set()
    if max_hops is not None and max_hops < 0:
        raise ValueError("max_hops must be >= 0")

    reachable = set(seed_set)
    frontier = set(seed_set)
    hop = 0

    while frontier:
        if max_hops is not None and hop >= max_hops:
            break

        next_frontier: set[str] = set()
        for entity in frontier:
            neighbors: set[str] = set()
            if direction in ("forward", "both"):
                neighbors |= graph.forward.get(entity, set())
            if direction in ("backward", "both"):
                neighbors |= graph.backward.get(entity, set())

            for neighbor in neighbors:
                if neighbor not in reachable:
                    reachable.add(neighbor)
                    next_frontier.add(neighbor)

        if not next_frontier:
            break

        frontier = next_frontier
        hop += 1

    return reachable


def apply_main_class_scope(
    entities: set[str],
    graph: NTripleGraph,
    seeds: set[str],
    main_class: str,
    scope: MainClassScope,
) -> set[str]:
    """Drop reachable main-class entities that are not in the seed set."""
    if scope == "reachable":
        return entities
    normalized = normalize_class_uri(main_class)
    return {
        entity
        for entity in entities
        if entity in seeds or normalized not in graph.types_by_entity.get(entity, set())
    }


def entities_of_class(graph: NTripleGraph, main_class: str) -> list[str]:
    normalized = normalize_class_uri(main_class)
    return sorted(
        entity
        for entity, types in graph.types_by_entity.items()
        if normalized in types
    )


def induce_subgraph_triples(
    graph: NTripleGraph,
    entities: set[str],
    *,
    allowed_type_objects: set[str] | None = None,
) -> list[Triple]:
    if not entities:
        return []

    selected = entities
    seen: set[Triple] = set()
    induced: list[Triple] = []

    for triple in graph.triples:
        subject, predicate, obj, is_literal = triple
        if subject not in selected:
            continue

        if is_literal:
            if triple not in seen:
                seen.add(triple)
                induced.append(triple)
            continue

        if is_rdf_type_predicate(predicate):
            if allowed_type_objects is None or obj in allowed_type_objects:
                if triple not in seen:
                    seen.add(triple)
                    induced.append(triple)
            continue

        if obj in selected:
            if triple not in seen:
                seen.add(triple)
                induced.append(triple)

    return induced


def filter_reachable_from_classes(
    triples: Sequence[Triple],
    root_classes: str | Sequence[str],
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
) -> list[Triple]:
    graph = NTripleGraph.from_triples(triples)
    entities = reachable_entities(
        graph,
        root_classes,
        max_hops=max_hops,
        direction=direction,
    )
    return induce_subgraph_triples(graph, entities)


def filter_reachable_from_entities(
    triples: Sequence[Triple],
    seeds: str | Sequence[str],
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    main_class: str | None = None,
    main_class_scope: MainClassScope = "reachable",
) -> list[Triple]:
    if main_class_scope == "seeds" and main_class is None:
        raise ValueError("main_class is required when main_class_scope is 'seeds'")

    graph = NTripleGraph.from_triples(triples)
    seed_set = {seeds} if isinstance(seeds, str) else set(seeds)
    entities = reachable_entities_from_seeds(
        graph,
        seed_set,
        max_hops=max_hops,
        direction=direction,
    )
    entities = apply_main_class_scope(
        entities,
        graph,
        seed_set,
        main_class or "",
        main_class_scope,
    )
    return induce_subgraph_triples(graph, entities)


def filter_reachable_from_entities_nt_file(
    input_path: str,
    output_path: str,
    seeds: Sequence[str],
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    parser: Literal["stream", "rdflib"] = "stream",
) -> dict[str, int | str]:
    triples = load_nt(input_path, parser=parser)
    before_entities = len({subject for subject, _, _, _ in triples})
    before_triples = len(triples)

    filtered = filter_reachable_from_entities(
        triples,
        seeds,
        max_hops=max_hops,
        direction=direction,
    )
    write_nt(filtered, output_path)

    after_entities = len({subject for subject, _, _, _ in filtered})
    after_triples = len(filtered)

    return {
        "seed_count": len(set(seeds)),
        "direction": direction,
        "max_hops": max_hops if max_hops is not None else "unlimited",
        "before_entities": before_entities,
        "before_triples": before_triples,
        "after_entities": after_entities,
        "after_triples": after_triples,
    }


def filter_reachable_nt_file(
    input_path: str,
    output_path: str,
    root_class: str,
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    parser: Literal["stream", "rdflib"] = "stream",
) -> dict[str, int | str]:
    triples = load_nt(input_path, parser=parser)
    before_entities = len({subject for subject, _, _, _ in triples})
    before_triples = len(triples)

    filtered = filter_reachable_from_classes(
        triples,
        root_class,
        max_hops=max_hops,
        direction=direction,
    )
    write_nt(filtered, output_path)

    after_entities = len({subject for subject, _, _, _ in filtered})
    after_triples = len(filtered)

    return {
        "root_class": normalize_class_uri(root_class),
        "direction": direction,
        "max_hops": max_hops if max_hops is not None else "unlimited",
        "before_entities": before_entities,
        "before_triples": before_triples,
        "after_entities": after_entities,
        "after_triples": after_triples,
    }
