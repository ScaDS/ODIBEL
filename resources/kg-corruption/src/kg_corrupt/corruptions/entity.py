"""Family 2: entity-centric removal of incident or subject triple groups."""

from __future__ import annotations

from collections import defaultdict
from typing import Sequence

from rdflib import Graph
from rdflib.term import Node

from kg_corrupt.corruptions.base import (
    clamp_target,
    collect_entities,
    make_result,
    select_units_nearest_target,
    sorted_triples,
)
from kg_corrupt.model import CorruptionContext, CorruptionResult, Triple
from kg_corrupt.terms import is_resource, term_to_json


class EntityCorruption:
    name = "entity"

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
        mode = str(context.family_config.get("mode", "incident"))
        if mode not in {"incident", "subject"}:
            raise ValueError(f"Unknown entity mode: {mode}")
        allow_bnodes = bool(context.allow_blank_seed_entities)
        index = _entity_index(eligible, mode=mode)
        candidates = collect_entities(eligible, allow_bnodes=allow_bnodes)
        if mode == "subject":
            candidates = [entity for entity in candidates if entity in index]
        rng.shuffle(candidates)
        selected, removed = select_units_nearest_target(
            candidates,
            lambda entity: index.get(entity, set()),
            target,
        )
        degrees = {
            term_to_json(entity): len(index.get(entity, ())) for entity in selected
        }
        realized = len(removed)
        return make_result(
            graph,
            removed,
            {
                "family": self.name,
                "entity_mode": mode,
                "entity_sampling": context.family_config.get(
                    "entity_sampling", "uniform"
                ),
                "selected_entities": [term_to_json(entity) for entity in selected],
                "selected_entity_count": len(selected),
                "entity_degrees": degrees,
                "target_removed_count": target_removed_count,
                "realized_removed_count": realized,
                "overshoot_or_undershoot": realized - target,
            },
        )


def _entity_index(
    eligible: Sequence[Triple],
    *,
    mode: str,
) -> dict[Node, set[Triple]]:
    index: dict[Node, set[Triple]] = defaultdict(set)
    for triple in eligible:
        subject, _predicate, obj = triple
        index[subject].add(triple)
        if mode == "incident":
            if is_resource(obj):
                index[obj].add(triple)
    return dict(index)
