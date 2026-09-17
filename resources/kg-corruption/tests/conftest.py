from __future__ import annotations

from pathlib import Path

import pytest
from rdflib import Graph

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.eligibility import build_eligibility_sets
from kg_corrupt.provenance import ProvenanceIndex, build_provenance_index

PKG_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_TTL = PKG_ROOT / "data" / "example" / "root_reference.ttl"
EXAMPLE_PROV = PKG_ROOT / "data" / "example" / "provenance.jsonl"


@pytest.fixture
def example_graph() -> Graph:
    graph = Graph()
    graph.parse(EXAMPLE_TTL.as_posix(), format="turtle")
    return graph


@pytest.fixture
def eligibility(example_graph):
    return build_eligibility_sets(example_graph, EligibilityConfig())


@pytest.fixture
def example_provenance(example_graph) -> ProvenanceIndex:
    return build_provenance_index(example_graph, provenance_file=EXAMPLE_PROV)
