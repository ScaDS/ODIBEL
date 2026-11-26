"""
Link Text2KGBench DBpedia-WebNLG ontologies to the DBpedia ontology by name.

Each concept ``X`` gets ``owl:equivalentClass dbo:X`` and each relation ``p`` gets
``owl:equivalentProperty dbo:p`` when that term exists in the given DBpedia ontology.
Compound relation names (``associatedBand/associatedMusicalArtist``) link every part.
Unmatched names are kept without a link and listed in ``.odibel/stats.json``.

Input is the upstream JSON (all 19 ontologies; the OWL folder lacks ont_1_university).
Output files are named ``<n>_<name>_ontology.ttl`` as expected by ``generate_refac.py``.

Usage:
    uv run resources/text2kgbench/align_ontologies.py \
        data/text2kgbench-v1 data/dbpedia-ontology-2022.12.05 data/text2kgbench-v1-dbo
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from pyodibel.datasets.manifest import (
    META_DIR,
    STATS_FILE,
    DatasetManifest,
    dataset_digest,
    sha256_file,
    write_checksums,
)

DBO = Namespace("http://dbpedia.org/ontology/")
T2KB = "https://cenguix.github.io/Text2KGBench"
ONTOLOGY_DIR = "release/dbpedia_webnlg/ontologies"
DBO_FILE = "dbo-snapshots.ttl"


def dbo_terms(path: Path) -> tuple[set[str], set[str]]:
    graph = Graph()
    graph.parse(path, format="turtle")
    classes = {str(s) for s in graph.subjects(RDF.type, OWL.Class) if str(s).startswith(str(DBO))}
    properties = {
        str(s)
        for kind in (OWL.ObjectProperty, OWL.DatatypeProperty, RDF.Property)
        for s in graph.subjects(RDF.type, kind)
        if str(s).startswith(str(DBO))
    }
    return classes, properties


def align(ontology: dict, dbo_classes: set[str], dbo_properties: set[str]) -> tuple[Graph, dict]:
    ont_id = ontology["id"]
    concepts_ns = Namespace(f"{T2KB}/{ont_id}/concepts#")
    relations_ns = Namespace(f"{T2KB}/{ont_id}/relations#")

    graph = Graph()
    graph.bind("owl", OWL)
    graph.bind("dbo", DBO)
    graph.bind("concept", concepts_ns)
    graph.bind("relation", relations_ns)
    header = URIRef(f"{T2KB}/{ont_id}")
    graph.add((header, RDF.type, OWL.Ontology))
    graph.add((header, RDFS.label, Literal(ontology["title"])))

    report = {"classes": 0, "properties": 0, "linked_classes": 0, "linked_properties": 0,
              "unlinked_classes": [], "unlinked_properties": []}

    for name in sorted({c["label"] for c in ontology["concepts"]}):
        subject = concepts_ns[name]
        graph.add((subject, RDF.type, OWL.Class))
        graph.add((subject, RDFS.label, Literal(name)))
        report["classes"] += 1
        if str(DBO[name]) in dbo_classes:
            graph.add((subject, OWL.equivalentClass, DBO[name]))
            report["linked_classes"] += 1
        else:
            report["unlinked_classes"].append(name)

    for relation in sorted(ontology["relations"], key=lambda r: r["label"]):
        name = relation["label"]
        subject = relations_ns[name]
        graph.add((subject, RDF.type, OWL.ObjectProperty))
        graph.add((subject, RDFS.label, Literal(name)))
        graph.add((subject, RDFS.domain, concepts_ns[relation["domain"]]))
        graph.add((subject, RDFS.range, concepts_ns[relation["range"]]))
        report["properties"] += 1
        linked = [part for part in name.split("/") if str(DBO[part]) in dbo_properties]
        for part in linked:
            graph.add((subject, OWL.equivalentProperty, DBO[part]))
        if linked:
            report["linked_properties"] += 1
        else:
            report["unlinked_properties"].append(name)

    return graph, report


def git_state(path: Path) -> tuple[str, bool]:
    def run(*args: str) -> str:
        return subprocess.run(["git", "-C", str(path), *args], capture_output=True, text=True).stdout.strip()

    commit = run("rev-parse", "HEAD") or "unknown"
    dirty = bool(run("status", "--porcelain", "--", "."))
    return commit, dirty


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("text2kgbench", type=Path, help="text2kgbench-v1 dataset directory")
    parser.add_argument("dbo", type=Path, help="DBpedia ontology dataset directory")
    parser.add_argument("output", type=Path, help="output dataset directory (must not exist)")
    args = parser.parse_args(argv)

    if args.output.exists():
        sys.exit(f"output exists: {args.output}")
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    parents = [DatasetManifest.load(args.text2kgbench), DatasetManifest.load(args.dbo)]
    dbo_classes, dbo_properties = dbo_terms(args.dbo / DBO_FILE)

    out_dir = args.output / "ontologies"
    out_dir.mkdir(parents=True)
    stats: dict[str, dict] = {}
    sources = sorted((args.text2kgbench / ONTOLOGY_DIR).glob("*_ontology.json"),
                     key=lambda p: int(p.name.split("_", 1)[0]))
    for source in sources:
        ontology = json.loads(source.read_text(encoding="utf-8"))
        graph, report = align(ontology, dbo_classes, dbo_properties)
        target = out_dir / source.with_suffix(".ttl").name
        graph.serialize(target, format="turtle")
        stats[target.name] = report
        print(f"{target.name:36} classes {report['linked_classes']:2}/{report['classes']:2}  "
              f"properties {report['linked_properties']:2}/{report['properties']:2}")

    write_checksums(args.output)
    (args.output / META_DIR / STATS_FILE).write_text(json.dumps(stats, indent=2) + "\n", encoding="utf-8")

    commit, dirty = git_state(Path(__file__).parent)
    DatasetManifest(args.output, {
        "odibel": "0.1",
        "id": args.output.name,
        "title": "Text2KGBench DBpedia-WebNLG ontologies linked to the DBpedia ontology",
        "status": "draft",
        "kind": "ontology-turtle",
        "license": "CC-BY-SA-4.0",
        "created_at": started,
        "derived_from": [
            {"id": parents[0].id, "sha256": dataset_digest(args.text2kgbench)},
            {"id": parents[1].id, "file": DBO_FILE, "sha256": sha256_file(args.dbo / DBO_FILE)},
        ],
        "recipe": {
            "script": "resources/text2kgbench/align_ontologies.py",
            "git_commit": commit,
            "git_dirty": dirty,
            "input": f"{ONTOLOGY_DIR}/*_ontology.json",
            "rule": "link X to dbo:X when it exists in the DBpedia ontology; split relation names on '/'",
        },
        "files": {"ontologies/": "<n>_<name>_ontology.ttl per Text2KGBench ontology (19)"},
        "contract": {
            "format": "turtle",
            "classes": f"{T2KB}/<ont_id>/concepts#<name>, owl:equivalentClass dbo:<name> when linked",
            "properties": f"{T2KB}/<ont_id>/relations#<name>, owl:ObjectProperty, "
                          "owl:equivalentProperty dbo:<name> when linked",
        },
        "limitations": [
            "Name matching only: no manual mapping of WebNLG wordings without a dbo term "
            "(for example elevationAboveTheSeaLevel); those stay unlinked, see stats.json.",
            "Reconstruction of the lost kgpipe-ontologies; may differ from the ontologies "
            "behind earlier KGI-Bench outputs.",
        ],
        "used_by": ["resources/text2kgbench/generate_refac.py"],
    }).write()
    print(f"done: {args.output}")


if __name__ == "__main__":
    main()
