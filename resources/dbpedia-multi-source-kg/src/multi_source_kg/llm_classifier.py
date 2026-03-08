from __future__ import annotations

import json
import os
from dataclasses import dataclass

import requests

from .constants import SUBDOMAINS
from .models import ClassContext, Classification
from .rule_classifier import short_rationale

CONFIDENCE_LEVELS = {"high", "medium", "low"}


@dataclass(frozen=True)
class LLMSettings:
    api_key: str
    model: str = "gpt-4.1-mini"
    base_url: str = "https://api.openai.com/v1"
    timeout_seconds: int = 60

    @staticmethod
    def from_env() -> "LLMSettings | None":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        return LLMSettings(
            api_key=api_key,
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            timeout_seconds=int(os.getenv("OPENAI_TIMEOUT_SECONDS", "60")),
        )


class LLMClassifier:
    def __init__(self, settings: LLMSettings) -> None:
        self.settings = settings

    def classify(self, context: ClassContext, rule_suggestion: Classification | None = None) -> Classification:
        prompt = self._build_prompt(context, rule_suggestion=rule_suggestion)
        payload = {
            "model": self.settings.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You classify DBpedia ontology classes. "
                        "Return only JSON with keys: primary_subdomain, secondary_tags, confidence, rationale."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.settings.api_key}",
            "Content-Type": "application/json",
        }
        response = requests.post(
            f"{self.settings.base_url.rstrip('/')}/chat/completions",
            headers=headers,
            json=payload,
            timeout=self.settings.timeout_seconds,
        )
        response.raise_for_status()
        body = response.json()
        raw = body["choices"][0]["message"]["content"]
        parsed = json.loads(raw)
        return self._validate_and_build(parsed)

    def _build_prompt(self, context: ClassContext, rule_suggestion: Classification | None) -> str:
        anchors = ", ".join(
            f"{domain}:{distance}" for domain, distance in sorted(context.anchor_distances.items(), key=lambda item: item[1])[:5]
        )
        labels = ", ".join(context.labels[:3]) if context.labels else "none"
        parents = ", ".join(sorted(context.parents)[:6]) if context.parents else "none"
        rule_block = ""
        if rule_suggestion:
            rule_block = (
                f"\nRule-based suggestion:\n"
                f"- primary_subdomain: {rule_suggestion.primary_subdomain}\n"
                f"- secondary_tags: {rule_suggestion.secondary_tags}\n"
                f"- confidence: {rule_suggestion.confidence}\n"
                f"- rationale: {rule_suggestion.rationale}\n"
            )

        subdomains = ", ".join(SUBDOMAINS)
        return (
            "Choose exactly one primary_subdomain and optional secondary_tags.\n"
            "Use only these subdomains:\n"
            f"{subdomains}\n\n"
            "Class context:\n"
            f"- class: dbo:{context.class_name}\n"
            f"- labels: {labels}\n"
            f"- parents: {parents}\n"
            f"- nearest_anchor_distances: {anchors if anchors else 'none'}\n"
            f"{rule_block}\n"
            "Rules:\n"
            "- Do not invent subdomains.\n"
            "- secondary_tags must be from allowed subdomains and exclude primary_subdomain.\n"
            "- confidence must be high, medium, or low.\n"
            "- rationale must be <= 20 words.\n"
            "- Respond as JSON only.\n"
        )

    def _validate_and_build(self, data: dict[str, object]) -> Classification:
        primary = str(data.get("primary_subdomain", "")).strip()
        if primary not in SUBDOMAINS:
            raise ValueError(f"Invalid primary_subdomain from LLM: {primary}")

        raw_secondary = data.get("secondary_tags", [])
        if raw_secondary is None:
            raw_secondary = []
        if not isinstance(raw_secondary, list):
            raise ValueError("secondary_tags must be a list")

        secondary: list[str] = []
        for value in raw_secondary:
            tag = str(value).strip()
            if tag in SUBDOMAINS and tag != primary and tag not in secondary:
                secondary.append(tag)

        confidence = str(data.get("confidence", "")).strip().lower()
        if confidence not in CONFIDENCE_LEVELS:
            confidence = "low"

        rationale = short_rationale(str(data.get("rationale", "")).strip() or "LLM classification with limited evidence.")
        return Classification(
            primary_subdomain=primary,
            secondary_tags=secondary[:3],
            confidence=confidence,
            rationale=rationale,
            manual_review=(confidence == "low"),
            decision_source="llm",
        )

