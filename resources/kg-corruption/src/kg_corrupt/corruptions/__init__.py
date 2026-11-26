"""Corruption strategy registry."""

from __future__ import annotations

from kg_corrupt.corruptions.entity import EntityCorruption
from kg_corrupt.corruptions.long_tail import LongTailCorruption
from kg_corrupt.corruptions.predicate import PredicateCorruption
from kg_corrupt.corruptions.random_fact import RandomFactCorruption
from kg_corrupt.corruptions.source import SourceCorruption
from kg_corrupt.corruptions.subgraph import SubgraphCorruption

STRATEGIES = {
    "random_fact": RandomFactCorruption,
    "entity": EntityCorruption,
    "predicate": PredicateCorruption,
    "long_tail": LongTailCorruption,
    "source": SourceCorruption,
    "subgraph": SubgraphCorruption,
}


def create_strategy(family: str):
    try:
        cls = STRATEGIES[family]
    except KeyError as exc:
        raise ValueError(f"Unknown corruption family: {family}") from exc
    return cls()
