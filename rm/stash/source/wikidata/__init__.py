"""
Wikidata data source implementations.

This module provides:
- WikidataDumpSource: For ingesting Wikidata JSON dumps
- WikidataEntity: Pydantic models for Wikidata entities
- Mapping utilities: Convert Wikidata entities to ODIBEL format
"""

from pyodibel.source.wikidata.dump import WikidataDumpSource
from pyodibel.source.wikidata.entity import WikidataEntity
from pyodibel.source.wikidata.mapping import map_wikidata_to_odibel

__all__ = [
    "WikidataDumpSource",
    "WikidataEntity",
    "map_wikidata_to_odibel",
]


