"""RDF graph loading and canonical N-Triples writing."""

from __future__ import annotations

from pathlib import Path

from rdflib import Dataset, Graph

from kg_corrupt.hashing import canonical_nt
from kg_corrupt.model import Triple

EXTENSION_FORMATS = {
    ".ttl": "turtle",
    ".turtle": "turtle",
    ".nt": "nt",
    ".n3": "n3",
    ".rdf": "xml",
    ".owl": "xml",
    ".xml": "xml",
    ".trig": "trig",
    ".nq": "nquads",
    ".nquads": "nquads",
}

QUAD_FORMATS = frozenset({"trig", "nquads"})


def infer_format(path: Path, explicit: str | None = None) -> str:
    if explicit:
        return explicit
    suffix = path.suffix.lower()
    if suffix in {".gz", ".bz2"}:
        suffix = Path(path.stem).suffix.lower()
    if suffix not in EXTENSION_FORMATS:
        raise ValueError(
            f"Cannot infer RDF format from {path.name}; pass input.format explicitly."
        )
    return EXTENSION_FORMATS[suffix]


def load_graph(path: str | Path, *, rdf_format: str | None = None) -> Graph:
    """Load an RDF graph. Quad formats become a Dataset so named graphs can be used as source ids."""
    path_obj = Path(path)
    if not path_obj.is_file():
        raise FileNotFoundError(f"root reference not found: {path_obj}")
    fmt = infer_format(path_obj, rdf_format)
    graph: Graph
    if fmt in QUAD_FORMATS:
        graph = Dataset()
    else:
        graph = Graph()
    graph.parse(path_obj.as_posix(), format=fmt)
    return graph


def to_plain_graph(graph: Graph) -> Graph:
    """Return a default Graph of unique triples without mutating ``graph``."""
    plain = Graph()
    if hasattr(graph, "quads"):
        for subject, predicate, obj, _context in graph.quads((None, None, None, None)):
            plain.add((subject, predicate, obj))
        return plain
    for triple in graph:
        plain.add(triple)
    return plain


def write_graph(
    graph: Graph,
    path: str | Path,
    *,
    serialization: str = "nt",
) -> None:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    fmt = serialization.lower()
    if fmt in {"nt", "ntriples", "n-triples"}:
        path_obj.write_text(canonical_nt(graph), encoding="utf-8")
        return
    graph.serialize(destination=path_obj.as_posix(), format=fmt)


def write_triples(triples: list[Triple] | set[Triple], path: str | Path) -> None:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    path_obj.write_text(canonical_nt(triples), encoding="utf-8")
