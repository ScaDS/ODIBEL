"""Hard invariants for every generated corruption."""

from __future__ import annotations

from rdflib import Graph

from kg_corrupt.eligibility import EligibilitySets
from kg_corrupt.hashing import canonical_nt
from kg_corrupt.model import CorruptionResult


class ValidationError(AssertionError):
    """A generated graph violated a benchmark invariant."""


def validate_corruption(
    root: Graph,
    eligibility: EligibilitySets,
    result: CorruptionResult,
    *,
    target_removed_count: int | None = None,
) -> None:
    root_set = set(root)
    retained = set(result.retained_graph)
    removed = set(result.removed_triples)
    eligible_set = set(eligibility.eligible)
    protected = eligibility.protected

    extra = retained - root_set
    if extra:
        raise ValidationError(
            f"subset invariant violated: {len(extra)} triples not in root"
        )

    missing_protected = protected - retained
    if missing_protected:
        raise ValidationError(
            f"protected-triple invariant violated: {len(missing_protected)} "
            "protected triples were removed"
        )

    expected_removed = eligible_set - retained
    if removed != expected_removed:
        raise ValidationError(
            "removal accounting mismatch: removed triples are not exactly "
            "T_eval \\ K_corrupt"
        )

    realized = result.metadata.get("realized_removed_count", len(removed))
    if realized != len(removed):
        raise ValidationError(
            f"realized_removed_count={realized} != |removed|={len(removed)}"
        )

    n_eval = len(eligibility.eligible)
    if n_eval:
        coverage = 1.0 - (len(removed) / n_eval)
    else:
        coverage = 1.0
    reported = result.metadata.get("reference_coverage")
    if reported is not None and abs(float(reported) - coverage) > 1e-12:
        raise ValidationError(
            f"coverage accounting mismatch: metadata={reported} measured={coverage}"
        )

    if removed - eligible_set:
        raise ValidationError("removed set contains non-eligible triples")


def assert_same_triple_set(left: Graph, right: Graph) -> None:
    if canonical_nt(left) != canonical_nt(right):
        raise ValidationError("triple sets differ")


def reference_coverage(removed_count: int, eligible_count: int) -> float:
    if eligible_count == 0:
        return 1.0
    return 1.0 - (removed_count / eligible_count)
