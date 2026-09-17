"""Family 1: uniform random fact removal."""

from __future__ import annotations

from typing import Sequence

from rdflib import Graph

from kg_corrupt.corruptions.base import clamp_target, make_result, sorted_triples
from kg_corrupt.model import CorruptionContext, CorruptionResult, Triple


class RandomFactCorruption:
    name = "random_fact"

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
        order = list(eligible)
        rng.shuffle(order)
        removed = set(order[:target])
        return make_result(
            graph,
            removed,
            {
                "family": self.name,
                "sampling": "uniform_without_replacement",
                "target_removed_count": target_removed_count,
                "realized_removed_count": len(removed),
            },
        )
