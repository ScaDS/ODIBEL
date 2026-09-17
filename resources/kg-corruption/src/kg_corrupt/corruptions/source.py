"""Family 5: source/document-centric removal of provenance units."""

from __future__ import annotations

from typing import Sequence

from rdflib import Graph

from kg_corrupt.corruptions.base import clamp_target, make_result
from kg_corrupt.model import CorruptionContext, CorruptionResult, Triple
from kg_corrupt.provenance import ProvenanceError, ProvenanceIndex


class SourceCorruption:
    name = "source"

    def corrupt(
        self,
        graph: Graph,
        eligible_triples: Sequence[Triple],
        target_removed_count: int,
        rng,
        context: CorruptionContext,
    ) -> CorruptionResult:
        provenance = context.provenance
        if not isinstance(provenance, ProvenanceIndex):
            raise ProvenanceError(
                "source-centric corruption is enabled but no provenance mapping "
                "was provided. Supply JSONL via families.source.provenance_file "
                "or --provenance, or use named graphs. Source membership is not "
                "inferred from URIs."
            )
        eligible = tuple(eligible_triples)
        provenance.require_for_source_family(eligible)
        target = clamp_target(target_removed_count, len(eligible))
        policy = str(
            context.family_config.get("multi_source_policy", "remove_if_all_selected")
        )
        if policy not in {"remove_if_all_selected", "remove_if_any_selected"}:
            raise ValueError(f"Unknown multi_source_policy: {policy}")

        eligible_set = set(eligible)
        units = list(provenance.source_ids())
        rng.shuffle(units)

        selected: list[str] = []
        removed: set[Triple] = set()
        selected_set: set[str] = set()
        for source_id in units:
            trial = selected_set | {source_id}
            trial_removed = _triples_removed_by_policy(
                eligible_set, provenance, trial, policy
            )
            extra = trial_removed - removed
            if not extra:
                continue
            current = len(removed)
            if current >= target:
                break
            new_count = current + len(extra)
            if new_count <= target:
                selected.append(source_id)
                selected_set.add(source_id)
                removed = trial_removed
                continue
            if abs(new_count - target) < abs(current - target):
                selected.append(source_id)
                selected_set.add(source_id)
                removed = trial_removed
            break

        realized = len(removed)
        return make_result(
            graph,
            removed,
            {
                "family": self.name,
                "multi_source_policy": policy,
                "selected_sources": selected,
                "selected_source_count": len(selected),
                "source_unit_count": len(units),
                "target_removed_count": target_removed_count,
                "realized_removed_count": realized,
                "overshoot_or_undershoot": realized - target,
            },
        )


def _triples_removed_by_policy(
    eligible: set[Triple],
    provenance: ProvenanceIndex,
    selected_sources: set[str],
    policy: str,
) -> set[Triple]:
    removed: set[Triple] = set()
    for triple in eligible:
        sources = provenance.triple_to_sources.get(triple)
        if not sources:
            continue
        if policy == "remove_if_all_selected":
            if sources <= selected_sources:
                removed.add(triple)
        elif sources & selected_sources:
            removed.add(triple)
    return removed
