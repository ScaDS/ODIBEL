from __future__ import annotations

from collections import defaultdict, deque
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from .constants import ANCHORS_BY_DOMAIN, DBO


def local_name(uri: URIRef) -> str:
    value = str(uri)
    if value.startswith(str(DBO)):
        return value.removeprefix(str(DBO))
    return value.rsplit("/", 1)[-1].rsplit("#", 1)[-1]


def load_ontology(path: Path) -> tuple[set[str], dict[str, set[str]], dict[str, list[str]]]:
    graph = Graph()
    graph.parse(path, format="turtle")

    classes: set[str] = set()
    parents: dict[str, set[str]] = defaultdict(set)
    labels: dict[str, list[str]] = defaultdict(list)

    for subject in graph.subjects(RDF.type, OWL.Class):
        if isinstance(subject, URIRef) and str(subject).startswith(str(DBO)):
            classes.add(local_name(subject))

    for cls in classes:
        cls_uri = DBO[cls]
        for parent in graph.objects(cls_uri, RDFS.subClassOf):
            if isinstance(parent, URIRef) and str(parent).startswith(str(DBO)):
                parents[cls].add(local_name(parent))

        for label in graph.objects(cls_uri, RDFS.label):
            if isinstance(label, Literal) and label.language in (None, "en"):
                labels[cls].append(str(label))

    for cls in classes:
        parents.setdefault(cls, set())
        labels.setdefault(cls, [])

    return classes, parents, labels


def compute_anchor_distances(
    classes: set[str], parents: dict[str, set[str]]
) -> dict[str, dict[str, int]]:
    distances: dict[str, dict[str, int]] = {}
    for cls in classes:
        domain_best: dict[str, int] = {}
        queue: deque[tuple[str, int]] = deque([(cls, 0)])
        seen: set[str] = set()

        while queue:
            current, depth = queue.popleft()
            if current in seen:
                continue
            seen.add(current)

            for domain, anchors in ANCHORS_BY_DOMAIN.items():
                if current in anchors:
                    previous = domain_best.get(domain)
                    if previous is None or depth < previous:
                        domain_best[domain] = depth

            for parent in parents.get(current, ()):
                queue.append((parent, depth + 1))

        distances[cls] = domain_best

    return distances

