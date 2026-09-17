"""KGI-Bench dataset provenance: config, identity, cards, and registry."""

from pyodibel.datasets.kgi_provenance.build import (
    build_generation_config,
    default_rdf_only_recipe,
    detect_git_commit,
    detect_pyodibel_version,
)
from pyodibel.datasets.kgi_provenance.card import (
    DatasetCard,
    MeasuredOutcomes,
    build_dataset_card,
)
from pyodibel.datasets.kgi_provenance.config import (
    SCHEMA_VERSION,
    GenerationConfig,
    InputsProvenance,
    RecipeKnobs,
    SoftwareProvenance,
    SourceFormats,
    canonicalize_for_hash,
    config_hash,
    content_sha256,
    content_sha256_cached,
)
from pyodibel.datasets.kgi_provenance.identity import (
    build_dataset_id,
    display_projection,
    stem_from_config,
)
from pyodibel.datasets.kgi_provenance.io import (
    load_dataset_card,
    load_generation_config,
    save_dataset_card,
    save_generation_config,
)
from pyodibel.datasets.kgi_provenance.registry import (
    RegistryEntry,
    list_datasets_by_domain,
    load_registry,
    register_dataset,
    save_registry,
)

__all__ = [
    "SCHEMA_VERSION",
    "DatasetCard",
    "GenerationConfig",
    "InputsProvenance",
    "MeasuredOutcomes",
    "RecipeKnobs",
    "RegistryEntry",
    "SoftwareProvenance",
    "SourceFormats",
    "build_dataset_card",
    "build_dataset_id",
    "build_generation_config",
    "canonicalize_for_hash",
    "config_hash",
    "content_sha256",
    "content_sha256_cached",
    "default_rdf_only_recipe",
    "detect_git_commit",
    "detect_pyodibel_version",
    "display_projection",
    "list_datasets_by_domain",
    "load_dataset_card",
    "load_generation_config",
    "load_registry",
    "register_dataset",
    "save_dataset_card",
    "save_generation_config",
    "save_registry",
    "stem_from_config",
]
