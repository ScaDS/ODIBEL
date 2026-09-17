"""Controlled omission-only KG corruption benchmark generator."""

from __future__ import annotations

from kg_corrupt.config import CorruptionConfig, load_config
from kg_corrupt.generator import generate_benchmark, generate_one
from kg_corrupt.model import STRATEGY_VERSION, CorruptionResult

__version__ = "1.0.0"
__all__ = [
    "STRATEGY_VERSION",
    "CorruptionConfig",
    "CorruptionResult",
    "generate_benchmark",
    "generate_one",
    "load_config",
    "__version__",
]
