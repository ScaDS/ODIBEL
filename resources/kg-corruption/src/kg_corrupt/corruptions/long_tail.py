"""Family 4: frequency-biased removal of individual triples."""

from __future__ import annotations

import statistics
from collections import Counter
from typing import Sequence

from rdflib import Graph
from rdflib.term import Node

from kg_corrupt.corruptions.base import (
    clamp_target,
    make_result,
    sorted_triples,
    weighted_sample_without_replacement,
)
from kg_corrupt.model import CorruptionContext, CorruptionResult, Triple
from kg_corrupt.terms import is_resource


class LongTailCorruption:
    name = "long_tail"

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
        basis = str(context.family_config.get("basis", "predicate_frequency"))
        alpha = float(context.family_config.get("alpha", 1.0))
        if alpha <= 0:
            raise ValueError("long_tail.alpha must be > 0")

        pred_freq = Counter(triple[1] for triple in eligible)
        subj_freq = Counter(triple[0] for triple in eligible)
        entity_freq = _entity_frequencies(eligible)
        weights = [
            _rarity_weight(triple, basis, alpha, pred_freq, subj_freq, entity_freq)
            for triple in eligible
        ]
        sampled = weighted_sample_without_replacement(eligible, weights, target, rng)
        removed = set(sampled)

        removed_pred_freqs = [pred_freq[triple[1]] for triple in removed]
        retained_eval = [triple for triple in eligible if triple not in removed]
        retained_pred_freqs = [pred_freq[triple[1]] for triple in retained_eval]

        return make_result(
            graph,
            removed,
            {
                "family": self.name,
                "sampling": "weighted_without_replacement",
                "basis": basis,
                "alpha": alpha,
                "target_removed_count": target_removed_count,
                "realized_removed_count": len(removed),
                "mean_predicate_frequency_removed": _mean(removed_pred_freqs),
                "median_predicate_frequency_removed": _median(removed_pred_freqs),
                "mean_predicate_frequency_retained": _mean(retained_pred_freqs),
                "median_predicate_frequency_retained": _median(retained_pred_freqs),
            },
        )


def _entity_frequencies(eligible: Sequence[Triple]) -> Counter:
    freq: Counter[Node] = Counter()
    for subject, _predicate, obj in eligible:
        freq[subject] += 1
        if is_resource(obj):
            freq[obj] += 1
    return freq


def _rarity_weight(
    triple: Triple,
    basis: str,
    alpha: float,
    pred_freq: Counter,
    subj_freq: Counter,
    entity_freq: Counter,
) -> float:
    subject, predicate, obj = triple
    pred_rarity = 1.0 / (pred_freq[predicate] ** alpha)
    subj_rarity = 1.0 / (subj_freq[subject] ** alpha)
    obj_rarity = (
        1.0 / (entity_freq[obj] ** alpha) if is_resource(obj) else 1.0
    )
    entity_rarity = 1.0 / (entity_freq[subject] ** alpha)
    if is_resource(obj):
        entity_rarity *= 1.0 / (entity_freq[obj] ** alpha)

    if basis == "predicate_frequency":
        return pred_rarity
    if basis == "subject_frequency":
        return subj_rarity
    if basis == "entity_frequency":
        return entity_rarity
    if basis == "combined":
        return pred_rarity * subj_rarity * obj_rarity
    raise ValueError(f"Unknown long_tail.basis: {basis}")


def _mean(values: list[int]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _median(values: list[int]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))
