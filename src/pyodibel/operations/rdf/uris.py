"""RDF URI helpers without Spark dependencies."""

from __future__ import annotations

DBPEDIA_ONTOLOGY_PREFIX = "<http://dbpedia.org/ontology/"
RDF_TYPE_URI = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def normalize_class_uri(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("class URI must not be empty")
    if value.startswith("dbo:"):
        return f"{DBPEDIA_ONTOLOGY_PREFIX}{value[4:]}>"
    if value.startswith("<") and value.endswith(">"):
        return value
    return f"<{value}>"


def is_rdf_type_predicate(predicate: str) -> bool:
    return predicate == RDF_TYPE_URI or predicate == "a"
