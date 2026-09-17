"""Tests for KGI-Bench provenance config, identity, cards, and registry."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pyodibel.datasets.kgi_provenance import (
    GenerationConfig,
    InputsProvenance,
    RecipeKnobs,
    SourceFormats,
    build_dataset_card,
    build_dataset_id,
    canonicalize_for_hash,
    config_hash,
    content_sha256,
    content_sha256_cached,
    display_projection,
    load_dataset_card,
    load_generation_config,
    load_registry,
    register_dataset,
    save_dataset_card,
    save_generation_config,
    stem_from_config,
)
from pyodibel.datasets.kgi_provenance.build import default_rdf_only_recipe


def _sample_config(**recipe_overrides) -> GenerationConfig:
    recipe = default_rdf_only_recipe(
        target_root_budget=100,
        subset_size=25,
        overlap_ratio=0.04,
        seed=42,
    )
    if recipe_overrides:
        from dataclasses import replace

        recipe = replace(recipe, **recipe_overrides)
    return GenerationConfig(
        schema_version="1.0.0",
        inputs=InputsProvenance(
            domain="airport",
            ontology_id="3_airport_ontology",
            ontology_content_sha256="a" * 64,
            root_class="dbo:Airport",
            dump_content_sha256="b" * 64,
            dump_uri="file:///tmp/selected.nt.bz2",
            language_filter="en",
        ),
        recipe=recipe,
    )


class TestConfigHash(unittest.TestCase):
    def test_hash_stable_and_independent_of_software(self):
        from dataclasses import replace

        from pyodibel.datasets.kgi_provenance import SoftwareProvenance

        left = _sample_config()
        right = replace(
            _sample_config(),
            software=SoftwareProvenance(pyodibel_version="9.9.9"),
            notes="should not affect hash",
        )
        self.assertEqual(config_hash(left), config_hash(right))
        self.assertEqual(len(config_hash(left)), 6)

    def test_hash_changes_with_overlap(self):
        base = _sample_config()
        other = _sample_config(overlap_ratio=0.10)
        self.assertNotEqual(config_hash(base), config_hash(other))

    def test_canonicalize_excludes_measured_fields(self):
        config = _sample_config()
        payload = canonicalize_for_hash(config)
        self.assertEqual(set(payload.keys()), {"schema_version", "inputs", "recipe"})
        self.assertNotIn("software", payload)


class TestIdentity(unittest.TestCase):
    def test_stem_and_dataset_id(self):
        config = _sample_config()
        self.assertEqual(stem_from_config(config), "airport_n100_ov04")
        dataset_id = build_dataset_id(config)
        self.assertTrue(dataset_id.startswith("airport_n100_ov04_"))
        self.assertEqual(len(dataset_id.rsplit("_", 1)[-1]), 6)

    def test_display_projection_rdf_only(self):
        text = display_projection(_sample_config())
        self.assertIn("Airport", text)
        self.assertIn("ov=0.04", text)
        self.assertIn("RDF-only", text)
        self.assertIn("hops=∞", text)


class TestCardAndRegistry(unittest.TestCase):
    def test_roundtrip_config_card_registry(self):
        config = _sample_config()
        dataset_id = build_dataset_id(config)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle = root / "datasets" / dataset_id
            bundle.mkdir(parents=True)
            save_generation_config(config, bundle / "generation_config.yaml")
            loaded = load_generation_config(bundle / "generation_config.yaml")
            self.assertEqual(config_hash(config), config_hash(loaded))

            card = build_dataset_card(
                config,
                dataset_id=dataset_id,
                status="draft",
                artifacts={"bundle_root": str(bundle)},
            )
            save_dataset_card(card, bundle / "dataset_card.json")
            reloaded = load_dataset_card(bundle / "dataset_card.json")
            self.assertEqual(reloaded.dataset_id, dataset_id)
            self.assertTrue(any("JSON sources" in item for item in reloaded.limitations))

            registry_path = root / "registry.json"
            register_dataset(registry_path, card, dataset_path=bundle)
            registry = load_registry(registry_path)
            self.assertIn(dataset_id, registry["datasets"])
            self.assertEqual(registry["datasets"][dataset_id]["domain"], "airport")


class TestContentHashCache(unittest.TestCase):
    def test_sidecar_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dump.nt"
            path.write_text("abc\n", encoding="utf-8")
            first = content_sha256_cached(path)
            second = content_sha256(path)
            self.assertEqual(first, second)
            cache = Path(str(path) + ".sha256")
            self.assertTrue(cache.is_file())
            cached_again = content_sha256_cached(path)
            self.assertEqual(cached_again, first)


if __name__ == "__main__":
    unittest.main()
