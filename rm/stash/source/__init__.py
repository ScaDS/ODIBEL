"""
Data source implementations.

This module provides implementations for connecting to various data sources:
- SPARQL endpoints
- REST APIs
- Web pages (HTML or RDF via content negotiation)
- Data dumps (JSON, XML, RDF, SQL)
- Linked Data (HTTP with content negotiation for RDF)

These sources provide data that can be consumed and operated on by the ODIBEL framework.
"""

# Import from submodules to maintain backward compatibility
from pyodibel.source.wikidata import WikidataDumpSource, WikidataEntity
from pyodibel.source.linked_data import LinkedDataSource

__all__ = [
    "WikidataDumpSource",
    "LinkedDataSource",
    "WikidataEntity",
]
