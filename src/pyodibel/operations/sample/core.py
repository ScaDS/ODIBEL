"""Shared sampling utilities independent of Spark."""

from __future__ import annotations

import random
from typing import Sequence, TypeVar

Triple = tuple[str, str, str]
T = TypeVar("T")


def validate_sample_size(*, fraction: float | None, n: int | None, total: int) -> int:
    if (fraction is None) == (n is None):
        raise ValueError("Provide exactly one of fraction or n")

    if fraction is not None:
        if not 0 < fraction <= 1:
            raise ValueError("fraction must be in (0, 1]")
        target_n = int(round(total * fraction))
    else:
        if n is None or n <= 0:
            raise ValueError("n must be > 0")
        if n > total:
            raise ValueError(f"n ({n}) cannot exceed population size ({total})")
        target_n = n

    if target_n <= 0:
        raise ValueError("target sample size must be > 0")
    return target_n


def allocate_quotas(counts: dict[str, int], target_n: int) -> dict[str, int]:
    """Proportional integer allocation using the largest-remainder method."""
    total = sum(counts.values())
    if target_n >= total:
        return dict(counts)

    raw = {label: (count / total) * target_n for label, count in counts.items()}
    quotas = {label: int(value) for label, value in raw.items()}
    assigned = sum(quotas.values())
    remainder = target_n - assigned

    if remainder > 0:
        order = sorted(
            counts,
            key=lambda label: (raw[label] - quotas[label], counts[label]),
            reverse=True,
        )
        for label in order[:remainder]:
            quotas[label] += 1

    return {label: min(quota, counts[label]) for label, quota in quotas.items()}


def sample_stratified_groups(
    groups: dict[str, list[T]],
    target_n: int,
    seed: int,
) -> list[T]:
    """Draw a stratified sample from pre-grouped items."""
    if target_n <= 0:
        return []

    counts = {label: len(items) for label, items in groups.items()}
    total = sum(counts.values())
    if total == 0:
        return []
    if target_n >= total:
        return [item for items in groups.values() for item in items]

    quotas = allocate_quotas(counts, target_n)
    rng = random.Random(seed)
    sampled: list[T] = []
    for label, quota in quotas.items():
        if quota <= 0:
            continue
        pool = groups[label]
        if quota >= len(pool):
            sampled.extend(pool)
        else:
            sampled.extend(rng.sample(pool, quota))

    rng.shuffle(sampled)
    return sampled


def relation_distribution(triples: Sequence[Triple]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for _, relation, _ in triples:
        counts[relation] = counts.get(relation, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))
