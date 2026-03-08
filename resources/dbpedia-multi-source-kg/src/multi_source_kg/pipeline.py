from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from tqdm import tqdm

from .llm_classifier import LLMClassifier
from .models import ClassContext, Classification
from .ontology import compute_anchor_distances, load_ontology
from .rule_classifier import RuleBasedClassifier


@dataclass(frozen=True)
class PipelineOptions:
    method: str  # rules | llm | hybrid
    max_classes: int | None = None


def build_yaml_entry(
    class_name: str,
    final_result: Classification,
    rules_result: Classification | None = None,
    llm_result: Classification | None = None,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "class": f"dbo:{class_name}",
        "primary_subdomain": final_result.primary_subdomain,
        "confidence": final_result.confidence,
        "rationale": final_result.rationale,
        "decision_source": final_result.decision_source,
    }
    if final_result.secondary_tags:
        entry["secondary_tags"] = final_result.secondary_tags
    if final_result.manual_review:
        entry["manual_review"] = True

    if rules_result and llm_result:
        entry["rule_based"] = {
            "primary_subdomain": rules_result.primary_subdomain,
            "confidence": rules_result.confidence,
        }
        entry["llm_based"] = {
            "primary_subdomain": llm_result.primary_subdomain,
            "confidence": llm_result.confidence,
        }
    return entry


def run_pipeline(
    input_ttl: Path,
    output_yaml: Path,
    options: PipelineOptions,
    llm_classifier: LLMClassifier | None = None,
) -> tuple[int, int, int, int]:
    classes, parents, labels = load_ontology(input_ttl)
    anchor_distances = compute_anchor_distances(classes, parents)
    rule_classifier = RuleBasedClassifier()

    all_classes = sorted(classes)
    if options.max_classes is not None:
        all_classes = all_classes[: options.max_classes]

    entries: list[dict[str, object]] = []
    llm_attempted = 0
    llm_succeeded = 0
    for cls in tqdm(all_classes, desc="Classifying DBpedia classes"):
        context = ClassContext(
            class_name=cls,
            labels=labels.get(cls, []),
            parents=parents.get(cls, set()),
            anchor_distances=anchor_distances.get(cls, {}),
        )
        rules_result = rule_classifier.classify(context)
        llm_result: Classification | None = None

        if options.method == "rules":
            final_result = rules_result
        else:
            if llm_classifier is None:
                raise ValueError("LLM classifier is required for llm/hybrid method.")
            llm_attempted += 1
            try:
                llm_result = llm_classifier.classify(context, rule_suggestion=rules_result)
                llm_succeeded += 1
            except Exception:
                # Keep pipeline robust: if LLM fails, fallback to rules.
                llm_result = None

            if options.method == "llm":
                final_result = llm_result or rules_result
            else:  # hybrid
                final_result = llm_result or rules_result

        entries.append(build_yaml_entry(cls, final_result, rules_result=rules_result, llm_result=llm_result))

    output_yaml.parent.mkdir(parents=True, exist_ok=True)
    output_yaml.write_text(
        yaml.safe_dump(entries, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    low_count = sum(1 for entry in entries if entry.get("manual_review"))
    return len(entries), low_count, llm_attempted, llm_succeeded

