"""Content hashes for reproducibility records."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from rdflib import Graph

from kg_corrupt.model import Triple
from kg_corrupt.terms import triple_to_nt


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path | None, *, chunk_size: int = 1024 * 1024) -> str | None:
    if path is None or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def canonical_nt(triples: Iterable[Triple]) -> str:
    lines = sorted(triple_to_nt(triple) for triple in triples)
    return "\n".join(lines) + ("\n" if lines else "")


def graph_sha256(graph: Graph) -> str:
    return sha256_bytes(canonical_nt(graph).encode("utf-8"))


def json_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256_bytes(encoded.encode("utf-8"))
