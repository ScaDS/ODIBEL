"""N-Triples loading and writing for in-memory RDF processing."""

from __future__ import annotations

import bz2
import gzip
import os
from pathlib import Path
from typing import Iterator, Literal

Triple = tuple[str, str, str, bool]
NtParser = Literal["stream", "rdflib"]


def open_nt_text(path: str | os.PathLike[str]):
    path_str = os.fspath(path)
    if path_str.endswith(".bz2"):
        return bz2.open(path_str, "rt", encoding="utf-8")
    if path_str.endswith(".gz"):
        return gzip.open(path_str, "rt", encoding="utf-8")
    return open(path_str, "r", encoding="utf-8")


def parse_nt_line(line: str) -> Triple | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if not line.endswith("."):
        return None

    parts = line.split(None, 2)
    if len(parts) < 3:
        return None

    subject, predicate, remainder = parts
    obj = remainder.rstrip()
    if obj.endswith("."):
        obj = obj[:-1].rstrip()
    if not obj:
        return None

    return subject, predicate, obj, obj.startswith('"')


def load_nt_stream(path: str | os.PathLike[str]) -> list[Triple]:
    triples: list[Triple] = []
    with open_nt_text(path) as handle:
        for line in handle:
            triple = parse_nt_line(line)
            if triple is not None:
                triples.append(triple)
    return triples


def _term_as_nt(term) -> str:
    from rdflib.term import BNode, Literal

    if isinstance(term, Literal):
        return term.n3()
    if isinstance(term, BNode):
        return f"_:{term}"
    return f"<{term}>"


def load_nt_rdflib(path: str | os.PathLike[str]) -> list[Triple]:
    from rdflib import Graph
    from rdflib.term import Literal

    graph = Graph()
    graph.parse(os.fspath(path), format="nt")
    triples: list[Triple] = []
    for subject, predicate, obj in graph:
        triples.append(
            (
                _term_as_nt(subject),
                _term_as_nt(predicate),
                _term_as_nt(obj),
                isinstance(obj, Literal),
            )
        )
    return triples


def load_nt(path: str | os.PathLike[str], *, parser: NtParser = "stream") -> list[Triple]:
    if parser == "stream":
        return load_nt_stream(path)
    if parser == "rdflib":
        return load_nt_rdflib(path)
    raise ValueError(f"Unknown NT parser: {parser}")


def iter_nt_lines(triples: Iterator[Triple]) -> Iterator[str]:
    for subject, predicate, obj, _ in triples:
        yield f"{subject} {predicate} {obj} .\n"


def write_nt(triples: list[Triple], path: str | os.PathLike[str]) -> None:
    path_obj = Path(path)
    if path_obj.exists():
        raise ValueError(f"output path already exists: {path}")

    path_obj.parent.mkdir(parents=True, exist_ok=True)
    with open(path_obj, "w", encoding="utf-8") as handle:
        handle.writelines(iter_nt_lines(iter(triples)))
