from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Classification:
    primary_subdomain: str
    secondary_tags: list[str]
    confidence: str
    rationale: str
    manual_review: bool
    decision_source: str = "rules"


@dataclass(frozen=True)
class ClassContext:
    class_name: str
    labels: list[str]
    parents: set[str]
    anchor_distances: dict[str, int]

