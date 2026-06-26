#!/usr/bin/env python3
"""
Derive shaded seed and RDF source graphs from Text2KGBench reference bundles.

Reads ``split_N/kg/reference/data.nt`` produced by ``splits.py`` and writes:
  - ``split_N/kg/seed/data.nt`` with ``http://kg.org/resource/{md5}`` entities
  - ``split_N/sources/rdf/data.nt`` with split-scoped ``http://kg.org/rdf/N/...`` URIs

Shading follows ``resources/movie-multi-source-kg/generate.py``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from rdflib import Graph, OWL, RDF, RDFS

from pyodibel.operations.rdf.replacer import shade_reference_graph

KG_RESOURCE_NS = "http://kg.org/resource/"
KG_ONTOLOGY_NS = "http://kg.org/ontology/"


def _discover_splits(splits_dir: Path) -> list[tuple[int, Path]]:
    splits: list[tuple[int, Path]] = []
    for reference_path in sorted(splits_dir.glob("split_*/kg/reference/data.nt")):
        split_name = reference_path.parent.parent.parent.name
        if not split_name.startswith("split_"):
            continue
        split_idx = int(split_name.removeprefix("split_"))
        splits.append((split_idx, reference_path))
    if not splits:
        raise ValueError(f"No reference graphs found under {splits_dir}/split_*/kg/reference/data.nt")
    return sorted(splits, key=lambda item: item[0])


def _load_reference_graph(reference_path: Path) -> Graph:
    graph = Graph()
    graph.parse(reference_path, format="nt")
    return graph


def _entity_types(graph: Graph) -> dict[str, str]:
    types: dict[str, str] = {}
    for subject, _, obj in graph.triples((None, RDF.type, None)):
        types[str(subject)] = str(obj)
    return types


def _entity_labels(graph: Graph) -> dict[str, str]:
    labels: dict[str, str] = {}
    for subject, _, label in graph.triples((None, RDFS.label, None)):
        labels[str(subject)] = str(label)
    return labels


def _write_verified_entities(
    path: Path,
    graph: Graph,
    *,
    dataset: str,
) -> None:
    types = _entity_types(graph)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write("dataset\tentity_id\tentity_type\n")
        for entity_id in sorted(types):
            entity_type = types.get(entity_id, str(OWL.Thing))
            handle.write(f"{dataset}\t{entity_id}\t{entity_type}\n")


def _serialize_graph(graph: Graph, path: Path, *, overwrite: bool = False) -> None:
    if path.exists():
        if not overwrite:
            raise ValueError(f"output path already exists: {path}")
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    graph.serialize(path, format="nt")


def generate_sources_for_split(
    split_idx: int,
    reference_path: Path,
    *,
    overwrite: bool = False,
) -> dict[str, int | str]:
    split_dir = reference_path.parent.parent.parent
    seed_root = split_dir / "kg" / "seed"
    rdf_root = split_dir / "sources" / "rdf"
    seed_data = seed_root / "data.nt"
    rdf_data = rdf_root / "data.nt"

    if not overwrite and (seed_data.exists() or rdf_data.exists()):
        raise ValueError(
            f"seed or rdf source already exists for split_{split_idx}; "
            "remove outputs or pass --overwrite"
        )

    reference_graph = _load_reference_graph(reference_path)
    reference_entities = len({str(s) for s in reference_graph.subjects()})
    reference_triples = len(reference_graph)

    seed_graph = shade_reference_graph(
        reference_graph,
        resource_namespace=KG_RESOURCE_NS,
        ontology_target_ns=KG_ONTOLOGY_NS,
    )
    _serialize_graph(seed_graph, seed_data, overwrite=overwrite)
    _write_verified_entities(
        seed_root / "meta" / "verified_entities.csv",
        seed_graph,
        dataset=f"split_{split_idx}/kg/seed",
    )

    rdf_resource_ns = f"http://kg.org/rdf/{split_idx}/resource/"
    rdf_ontology_ns = f"http://kg.org/rdf/{split_idx}/ontology/"
    rdf_graph = shade_reference_graph(
        reference_graph,
        resource_namespace=rdf_resource_ns,
        ontology_target_ns=rdf_ontology_ns,
    )
    _serialize_graph(rdf_graph, rdf_data, overwrite=overwrite)
    _write_verified_entities(
        rdf_root / "meta" / "verified_entities.csv",
        rdf_graph,
        dataset=f"split_{split_idx}/sources/rdf",
    )

    return {
        "split": split_idx,
        "reference_entities": reference_entities,
        "reference_triples": reference_triples,
        "seed_entities": len(_entity_types(seed_graph)),
        "seed_triples": len(seed_graph),
        "rdf_entities": len(_entity_types(rdf_graph)),
        "rdf_triples": len(rdf_graph),
        "seed_path": str(seed_data),
        "rdf_path": str(rdf_data),
    }


def generate_sources(
    splits_dir: str,
    *,
    split_ids: list[int] | None = None,
    overwrite: bool = False,
) -> dict[str, int | str | list]:
    root = Path(splits_dir)
    if not root.is_dir():
        raise ValueError(f"splits directory not found: {splits_dir}")

    discovered = _discover_splits(root)
    if split_ids is not None:
        selected = {split_idx for split_idx, _ in discovered}
        missing = [split_id for split_id in split_ids if split_id not in selected]
        if missing:
            raise ValueError(f"requested splits not found: {missing}")
        discovered = [(idx, path) for idx, path in discovered if idx in split_ids]

    split_stats: list[dict[str, int | str]] = []
    for split_idx, reference_path in discovered:
        split_stats.append(
            generate_sources_for_split(split_idx, reference_path, overwrite=overwrite)
        )

    return {
        "splits_dir": splits_dir,
        "split_count": len(split_stats),
        "splits": split_stats,
    }


def _print_summary(stats: dict[str, int | str | list]) -> None:
    print(f"Splits directory: {stats['splits_dir']}")
    print(f"Processed splits: {stats['split_count']}")
    print()
    for split in stats["splits"]:
        print(
            f"split_{split['split']}: "
            f"reference {int(split['reference_entities']):,} entities / "
            f"{int(split['reference_triples']):,} triples -> "
            f"seed {int(split['seed_entities']):,} / {int(split['seed_triples']):,}, "
            f"rdf {int(split['rdf_entities']):,} / {int(split['rdf_triples']):,}"
        )
        print(f"  seed: {split['seed_path']}")
        print(f"  rdf:  {split['rdf_path']}")
    print()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create shaded seed and RDF source graphs from reference splits.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "splits_dir",
        nargs="?",
        help="Root directory written by splits.py",
    )
    parser.add_argument(
        "--split",
        dest="split_ids",
        type=int,
        action="append",
        help="Only process this split index (repeatable)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing seed/rdf outputs",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    args = _build_parser().parse_args(argv)

    if not args.splits_dir:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))

    splits_dir = args.splits_dir or os.getenv("SPLITS_DIR") or os.getenv("OUTPUT_PATH")
    if not splits_dir:
        raise ValueError("splits_dir not provided and SPLITS_DIR/OUTPUT_PATH not set")

    stats = generate_sources(
        splits_dir,
        split_ids=args.split_ids,
        overwrite=args.overwrite,
    )
    _print_summary(stats)


if __name__ == "__main__":
    main()
