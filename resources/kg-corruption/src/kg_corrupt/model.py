"""Shared types for corruption strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence
from rdflib import Graph
from rdflib.term import Node

STRATEGY_VERSION = "1.0.0"

Triple = tuple[Node, Node, Node]


@dataclass(frozen=True)
class CorruptionContext:
    """Read-only per-run context passed to every strategy."""

    protected_triples: frozenset[Triple]
    family_config: dict[str, Any] = field(default_factory=dict)
    provenance: Any | None = None
    nested: bool = False
    allow_blank_seed_entities: bool = False


@dataclass
class CorruptionResult:
    retained_graph: Graph
    removed_triples: set[Triple]
    metadata: dict[str, Any]


class CorruptionStrategy(Protocol):
    name: str

    def corrupt(
        self,
        graph: Graph,
        eligible_triples: Sequence[Triple],
        target_removed_count: int,
        rng: Any,
        context: CorruptionContext,
    ) -> CorruptionResult:
        ...
