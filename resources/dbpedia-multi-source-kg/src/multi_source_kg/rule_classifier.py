from __future__ import annotations

import re
from typing import Iterable

from .constants import DOMAIN_PRIORITY, SEMANTIC_KEYWORDS
from .models import ClassContext, Classification


def split_tokens(text: str) -> set[str]:
    chunks = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    tokens = set(re.findall(r"[a-z]+", chunks.lower()))
    if "video" in tokens and "game" in tokens:
        tokens.add("videogame")
    if "organization" in tokens:
        tokens.add("organisation")
    if "medical" in tokens and "specialty" in tokens:
        tokens.add("medicalspecialty")
    return tokens


def semantic_scores(cls: str, labels: Iterable[str], parents: Iterable[str]) -> dict[str, int]:
    tokens = split_tokens(cls)
    for label in labels:
        tokens.update(split_tokens(label))
    for parent in parents:
        tokens.update(split_tokens(parent))

    scores: dict[str, int] = {}
    for domain, words in SEMANTIC_KEYWORDS.items():
        score = 0
        for token in tokens:
            if token in words:
                score += 2
            else:
                for keyword in words:
                    if len(keyword) > 5 and keyword in token:
                        score += 1
                        break
        if score:
            scores[domain] = score
    return scores


def short_rationale(text: str) -> str:
    words = text.split()
    if len(words) <= 20:
        return text
    return " ".join(words[:20])


def choose_secondary_tags(primary: str, candidates: list[str]) -> list[str]:
    unique: list[str] = []
    for domain in candidates:
        if domain != primary and domain not in unique:
            unique.append(domain)
    return unique[:3]


class RuleBasedClassifier:
    def classify(self, context: ClassContext) -> Classification:
        semantic = semantic_scores(context.class_name, context.labels, context.parents)
        hierarchy_domains = sorted(
            context.anchor_distances.items(), key=lambda item: (item[1], DOMAIN_PRIORITY[item[0]])
        )
        semantic_domains = sorted(
            semantic.items(), key=lambda item: (-item[1], DOMAIN_PRIORITY[item[0]])
        )

        if hierarchy_domains:
            hierarchy_primary, hierarchy_depth = hierarchy_domains[0]
            semantic_primary = semantic_domains[0][0] if semantic_domains else None
            semantic_best = semantic_domains[0][1] if semantic_domains else 0
            semantic_second = semantic_domains[1][1] if len(semantic_domains) > 1 else 0

            if (
                semantic_primary
                and semantic_primary != hierarchy_primary
                and hierarchy_depth >= 2
                and semantic_best >= 4
                and (semantic_best - semantic_second) >= 2
            ):
                primary = semantic_primary
                confidence = "medium"
                rationale = short_rationale(
                    f"Semantic meaning outweighs inherited anchor; class better fits {semantic_primary}."
                )
            else:
                primary = hierarchy_primary
                confidence = "high" if hierarchy_depth <= 2 else "medium"
                rationale = short_rationale(
                    f"Inherits from anchor class through ontology hierarchy; semantic cues support {primary}."
                )
        elif semantic_domains:
            primary = semantic_domains[0][0]
            confidence = "medium" if semantic_domains[0][1] >= 4 else "low"
            rationale = short_rationale(
                f"No clear anchor inheritance; label and parent semantics best match {primary}."
            )
        else:
            primary = "Organizations"
            confidence = "low"
            rationale = "Unclear hierarchy and semantics; assigned fallback domain for downstream extraction consistency."

        secondary_candidates: list[str] = []
        if hierarchy_domains:
            primary_hierarchy_depth = hierarchy_domains[0][1]
            secondary_candidates.extend(
                domain
                for domain, depth in hierarchy_domains[1:]
                if (depth - primary_hierarchy_depth) <= 1
            )
        elif semantic_domains:
            secondary_candidates.extend(
                domain for domain, score in semantic_domains if score >= 2 and domain != primary
            )

        if context.class_name == "Politician" and "Politics & government" not in secondary_candidates:
            secondary_candidates.insert(0, "Politics & government")
        if context.class_name == "SportsTeam":
            if primary != "Sports":
                primary = "Sports"
                confidence = "high"
                rationale = "SportsTeam is anchored in sports; organization aspect is secondary."
            if "Organizations" not in secondary_candidates:
                secondary_candidates.insert(0, "Organizations")

        secondary_tags = choose_secondary_tags(primary, secondary_candidates)
        return Classification(
            primary_subdomain=primary,
            secondary_tags=secondary_tags,
            confidence=confidence,
            rationale=short_rationale(rationale),
            manual_review=(confidence == "low"),
            decision_source="rules",
        )

