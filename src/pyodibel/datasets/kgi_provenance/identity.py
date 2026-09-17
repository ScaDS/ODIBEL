"""Hybrid dataset_id stem + hash helpers."""

from __future__ import annotations

from typing import Any, Mapping

from pyodibel.datasets.kgi_provenance.config import (
    GenerationConfig,
    RecipeKnobs,
    config_hash,
)


def overlap_code(overlap_ratio: float) -> str:
    """Encode overlap as two digits: 0.04 → ov04."""
    return f"ov{int(round(float(overlap_ratio) * 100)):02d}"


def stem_from_config(config: GenerationConfig | Mapping[str, Any]) -> str:
    """Readable stem: ``{domain}_n{budget}_{ovXX}``."""
    if isinstance(config, GenerationConfig):
        domain = config.inputs.domain
        recipe = config.recipe
    else:
        domain = str(config["inputs"]["domain"])
        recipe = RecipeKnobs.from_dict(config.get("recipe"))
    return f"{domain}_n{recipe.target_root_budget}_{overlap_code(recipe.overlap_ratio)}"


def build_dataset_id(
    config: GenerationConfig | Mapping[str, Any],
    *,
    hash_length: int = 6,
) -> str:
    """Hybrid id: stem + short config hash."""
    stem = stem_from_config(config)
    digest = config_hash(config, length=hash_length)
    return f"{stem}_{digest}"


def display_projection(config: GenerationConfig | Mapping[str, Any]) -> str:
    """Human table projection, e.g. ``Airport | n≈100 | ov=0.04 | hops=∞ | RDF-only``."""
    if isinstance(config, GenerationConfig):
        domain = config.inputs.domain
        recipe = config.recipe
    else:
        domain = str(config["inputs"]["domain"])
        recipe = RecipeKnobs.from_dict(config.get("recipe"))

    hops = "∞" if recipe.max_hops is None else str(recipe.max_hops)
    formats = []
    if recipe.formats.rdf:
        formats.append("RDF")
    if recipe.formats.json:
        formats.append("JSON")
    if recipe.formats.text:
        formats.append("text")
    format_label = "+".join(formats) if formats else "none"
    if formats == ["RDF"]:
        format_label = "RDF-only"

    title = domain.replace("_", " ").title().replace("Celestialbody", "CelestialBody")
    return (
        f"{title} | n≈{recipe.target_root_budget} | "
        f"ov={recipe.overlap_ratio:g} | hops={hops} | {format_label}"
    )
