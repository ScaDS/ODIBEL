"""Family 3: predicate-centric removal of relation groups."""

from __future__ import annotations

from collections import defaultdict
from typing import Sequence

from rdflib import Graph
from rdflib.term import Node

from kg_corrupt.corruptions.base import (
    clamp_target,
    make_result,
    select_units_nearest_target,
    sorted_triples,
    weighted_shuffle,
)
from kg_corrupt.model import CorruptionContext, CorruptionResult, Triple
from kg_corrupt.terms import term_to_json


class PredicateCorruption:
    name = "predicate"

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
        mode = str(context.family_config.get("mode", "whole_predicates"))
        sampling = str(context.family_config.get("predicate_sampling", "uniform"))
        grouped = _group_by_predicate(eligible)
        frequencies = {predicate: len(triples) for predicate, triples in grouped.items()}
        ordered = _order_predicates(list(grouped), frequencies, sampling, rng)

        if mode == "whole_predicates":
            selected, removed = select_units_nearest_target(
                ordered,
                lambda predicate: grouped[predicate],
                target,
            )
        elif mode == "within_predicates":
            selected, removed = _within_predicates(ordered, grouped, target, rng)
        else:
            raise ValueError(f"Unknown predicate mode: {mode}")

        realized = len(removed)
        return make_result(
            graph,
            removed,
            {
                "family": self.name,
                "predicate_mode": mode,
                "predicate_sampling": sampling,
                "selected_predicates": [term_to_json(pred) for pred in selected],
                "predicate_frequencies_before": {
                    term_to_json(pred): frequencies[pred] for pred in selected
                },
                "target_removed_count": target_removed_count,
                "realized_removed_count": realized,
                "overshoot_or_undershoot": realized - target,
            },
        )


def _group_by_predicate(eligible: Sequence[Triple]) -> dict[Node, list[Triple]]:
    grouped: dict[Node, list[Triple]] = defaultdict(list)
    for triple in eligible:
        grouped[triple[1]].append(triple)
    return dict(grouped)


def _order_predicates(
    predicates: list[Node],
    frequencies: dict[Node, int],
    sampling: str,
    rng,
) -> list[Node]:
    predicates = list(predicates)
    predicates.sort(key=term_to_json)
    if sampling == "uniform":
        rng.shuffle(predicates)
        return predicates
    if sampling == "frequency":
        weights = [float(frequencies[pred]) for pred in predicates]
        return weighted_shuffle(predicates, weights, rng)
    if sampling == "inverse_frequency":
        weights = [1.0 / frequencies[pred] for pred in predicates]
        return weighted_shuffle(predicates, weights, rng)
    raise ValueError(f"Unknown predicate_sampling: {sampling}")


def _within_predicates(
    ordered: Sequence[Node],
    grouped: dict[Node, list[Triple]],
    target: int,
    rng,
) -> tuple[list[Node], set[Triple]]:
    """Select predicates until their pool covers the target, then sample exactly m triples."""
    pool: list[Triple] = []
    selected: list[Node] = []
    for predicate in ordered:
        if len(pool) >= target:
            break
        selected.append(predicate)
        pool.extend(grouped[predicate])
    if target <= 0 or not pool:
        return selected, set()
    sample_size = min(target, len(pool))
    pool_sorted = sorted_triples(pool)
    rng.shuffle(pool_sorted)
    return selected, set(pool_sorted[:sample_size])
