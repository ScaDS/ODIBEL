"""Triple-to-source-unit mapping. Never inferred from arbitrary URIs."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from rdflib import Graph
from rdflib.namespace import NamespaceManager
from rdflib.term import BNode, Literal, URIRef
from rdflib.util import from_n3

from kg_corrupt.model import Triple

_NSM = NamespaceManager(Graph())


class ProvenanceError(ValueError):
    """Raised when source-centric corruption cannot be run honestly."""


def parse_term(raw: object) -> object:
    if isinstance(raw, dict):
        if "uri" in raw:
            return URIRef(str(raw["uri"]))
        if "literal" in raw:
            return Literal(
                raw["literal"],
                lang=raw.get("lang"),
                datatype=raw.get("datatype"),
            )
        if "bnode" in raw:
            return BNode(str(raw["bnode"]))
        raise ProvenanceError(f"Unsupported structured term: {raw!r}")
    text = str(raw).strip()
    if text.startswith("<") and text.endswith(">"):
        return URIRef(text[1:-1])
    if text.startswith("_:"):
        return BNode(text[2:])
    if text.startswith('"') or text.startswith("'"):
        return from_n3(text, nsm=_NSM)
    if text.startswith(("http://", "https://", "urn:")):
        return URIRef(text)
    return from_n3(text, nsm=_NSM)


@dataclass
class ProvenanceIndex:
    triple_to_sources: dict[Triple, frozenset[str]]
    source_to_triples: dict[str, set[Triple]]
    unmatched_records: int = 0
    source: str = "none"

    def source_ids(self) -> list[str]:
        return sorted(self.source_to_triples)

    def mapped_triple_count(self) -> int:
        return len(self.triple_to_sources)

    def require_for_source_family(self, eligible: tuple[Triple, ...]) -> None:
        mapped_eligible = [triple for triple in eligible if triple in self.triple_to_sources]
        if not mapped_eligible:
            raise ProvenanceError(
                "source-centric corruption is enabled but no provenance mapping "
                "covers any eligible triple. Provide a JSONL sidecar "
                "(s, p, o, source_id) via families.source.provenance_file / "
                "--provenance, or load TriG/N-Quads with named graphs. "
                "Source membership is not inferred from URIs."
            )


def _add_mapping(
    bucket: dict[Triple, set[str]],
    triple: Triple,
    source_id: str,
) -> None:
    bucket[triple].add(source_id)


def load_jsonl_provenance(path: Path, graph: Graph) -> tuple[dict[Triple, set[str]], int]:
    known = {triple for triple in graph}
    mapping: dict[Triple, set[str]] = defaultdict(set)
    unmatched = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            text = raw.strip()
            if not text or text.startswith("#"):
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ProvenanceError(
                    f"Invalid JSONL at {path}:{line_no}: {exc}"
                ) from exc
            try:
                triple = (
                    parse_term(record["s"]),
                    parse_term(record["p"]),
                    parse_term(record["o"]),
                )
                source_id = str(record["source_id"])
            except KeyError as exc:
                raise ProvenanceError(
                    f"Provenance record at {path}:{line_no} missing {exc.args[0]}"
                ) from exc
            if triple not in known:
                unmatched += 1
                continue
            _add_mapping(mapping, triple, source_id)
    return mapping, unmatched


def load_named_graph_provenance(graph: Graph) -> dict[Triple, set[str]]:
    if not hasattr(graph, "quads"):
        return {}
    mapping: dict[Triple, set[str]] = defaultdict(set)
    for subject, predicate, obj, context in graph.quads((None, None, None, None)):
        graph_id = context.identifier if hasattr(context, "identifier") else context
        if graph_id is None or str(graph_id) == "urn:x-rdflib:default":
            continue
        _add_mapping(mapping, (subject, predicate, obj), str(graph_id))
    return mapping


def merge_mappings(*maps: dict[Triple, set[str]]) -> dict[Triple, set[str]]:
    merged: dict[Triple, set[str]] = defaultdict(set)
    for mapping in maps:
        for triple, sources in mapping.items():
            merged[triple].update(sources)
    return merged


def build_provenance_index(
    graph: Graph,
    *,
    provenance_file: Path | None = None,
    named_graph_as_source: bool = True,
) -> ProvenanceIndex:
    parts: list[dict[Triple, set[str]]] = []
    unmatched = 0
    sources: list[str] = []
    if provenance_file is not None:
        if not provenance_file.is_file():
            raise ProvenanceError(f"provenance file not found: {provenance_file}")
        loaded, unmatched = load_jsonl_provenance(provenance_file, graph)
        parts.append(loaded)
        sources.append(f"jsonl:{provenance_file}")
    if named_graph_as_source:
        named = load_named_graph_provenance(graph)
        if named:
            parts.append(named)
            sources.append("named_graphs")
    merged = merge_mappings(*parts) if parts else {}
    triple_to_sources = {
        triple: frozenset(ids) for triple, ids in merged.items()
    }
    source_to_triples: dict[str, set[Triple]] = defaultdict(set)
    for triple, ids in triple_to_sources.items():
        for source_id in ids:
            source_to_triples[source_id].add(triple)
    return ProvenanceIndex(
        triple_to_sources=triple_to_sources,
        source_to_triples=dict(source_to_triples),
        unmatched_records=unmatched,
        source=",".join(sources) if sources else "none",
    )


def empty_provenance() -> ProvenanceIndex:
    return ProvenanceIndex(triple_to_sources={}, source_to_triples={})
