#!/usr/bin/env python3
"""
Build overlapping entity splits from a class-reachable NT file and write reference subgraphs.

Starting from entities of a main class in the input graph, creates overlapping splits
(movie-kg style) and writes per-split reachable subgraphs as reference bundles.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from pathlib import Path
from typing import Literal

from pyodibel.datasets.mp_mf.overlap_util import (
    build_exact_subsets,
    pairwise_entity_overlaps,
    validate_overlaps,
)
from pyodibel.operations.rdf.nt_io import Triple, iter_nt_lines, load_nt
from pyodibel.operations.rdf.reachability import (
    NTripleGraph,
    MainClassScope,
    entities_of_class,
    filter_reachable_from_entities,
)
from pyodibel.operations.rdf.uris import is_rdf_type_predicate, normalize_class_uri

Engine = Literal["memory", "spark"]
TraversalDirection = Literal["forward", "backward", "both"]


def _subjects_from_triples(triples: list[Triple]) -> set[str]:
    return {subject for subject, _, _, _ in triples}


def _entity_types_from_triples(triples: list[Triple]) -> dict[str, str]:
    types: dict[str, str] = {}
    for subject, predicate, obj, is_literal in triples:
        if not is_literal and is_rdf_type_predicate(predicate):
            types[subject] = obj
    return types


def _write_entity_list(path: Path, entities: list[str], *, header: str = "entity_id") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f"{header}\n")
        for entity in entities:
            handle.write(f"{entity}\n")


def _write_verified_entities(
    path: Path,
    triples: list[Triple],
    *,
    dataset: str,
) -> None:
    types = _entity_types_from_triples(triples)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write("dataset\tentity_id\tentity_type\n")
        for entity_id, entity_type in sorted(types.items()):
            handle.write(f"{dataset}\t{entity_id}\t{entity_type}\n")


def _write_triples(path: Path, triples: list[Triple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.writelines(iter_nt_lines(iter(triples)))


def _merge_triples(existing: set[Triple], new: list[Triple]) -> set[Triple]:
    merged = set(existing)
    merged.update(new)
    return merged


def _split_reference_dir(output_dir: Path, split_idx: int) -> Path:
    return output_dir / f"split_{split_idx}" / "kg" / "reference"


def generate_splits_memory(
    input_path: str,
    output_dir: str,
    main_class: str,
    *,
    num_subsets: int = 4,
    overlap_ratio: float = 0.04,
    subset_size: int,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    parser: Literal["stream", "rdflib"] = "stream",
    seed: int = 42,
    main_class_scope: MainClassScope = "reachable",
) -> dict[str, int | str | dict]:
    triples = load_nt(input_path, parser=parser)
    graph = NTripleGraph.from_triples(triples)
    main_class_entities = entities_of_class(graph, main_class)
    if not main_class_entities:
        raise ValueError(f"No entities of class {normalize_class_uri(main_class)} in {input_path}")

    shuffled = main_class_entities.copy()
    random.Random(seed).shuffle(shuffled)
    subsets = build_exact_subsets(shuffled, num_subsets, overlap_ratio, subset_size)

    out = Path(output_dir)
    if out.exists():
        raise ValueError(f"output directory already exists: {output_dir}")
    out.mkdir(parents=True)

    _write_entity_list(out / "entities" / "master_entities.csv", main_class_entities)

    agg_triples: set[Triple] = set()
    split_stats: list[dict[str, int]] = []
    seed_entity_sets: list[set[str]] = []
    subgraph_entity_sets: list[set[str]] = []
    subgraph_main_class_sets: list[set[str]] = []
    normalized_main_class = normalize_class_uri(main_class)

    for idx, subset in enumerate(subsets):
        ref_dir = _split_reference_dir(out, idx)
        ref_dir.mkdir(parents=True, exist_ok=True)

        _write_entity_list(ref_dir.parent.parent / "index" / "entities.csv", subset)

        split_triples = filter_reachable_from_entities(
            triples,
            subset,
            max_hops=max_hops,
            direction=direction,
            main_class=main_class,
            main_class_scope=main_class_scope,
        )
        split_subjects = _subjects_from_triples(split_triples)
        split_entities = len(split_subjects)
        split_main_class_entities = {
            entity
            for entity in split_subjects
            if normalized_main_class in graph.types_by_entity.get(entity, set())
        }

        seed_entity_sets.append(set(subset))
        subgraph_entity_sets.append(split_subjects)
        subgraph_main_class_sets.append(split_main_class_entities)

        _write_triples(ref_dir / "data.nt", split_triples)
        agg_triples = _merge_triples(agg_triples, split_triples)
        _write_triples(ref_dir / "data_agg.nt", list(agg_triples))

        dataset_name = f"split_{idx}/kg/reference"
        _write_verified_entities(
            ref_dir / "meta" / "verified_entities.csv",
            split_triples,
            dataset=dataset_name,
        )

        split_stats.append(
            {
                "split": idx,
                "seed_entities": len(subset),
                "subgraph_entities": split_entities,
                "subgraph_main_class_entities": len(split_main_class_entities),
                "subgraph_triples": len(split_triples),
            }
        )

    seed_overlaps = validate_overlaps(subsets)
    subgraph_overlaps = pairwise_entity_overlaps(subgraph_entity_sets)
    subgraph_main_class_overlaps = pairwise_entity_overlaps(subgraph_main_class_sets)

    return {
        "engine": "memory",
        "main_class": normalized_main_class,
        "main_class_entities": len(main_class_entities),
        "num_subsets": num_subsets,
        "subset_size": subset_size,
        "overlap_ratio": overlap_ratio,
        "direction": direction,
        "max_hops": max_hops if max_hops is not None else "unlimited",
        "main_class_scope": main_class_scope,
        "splits": split_stats,
        "seed_overlaps": seed_overlaps,
        "subgraph_overlaps": subgraph_overlaps,
        "subgraph_main_class_overlaps": subgraph_main_class_overlaps,
        "output_dir": output_dir,
    }


def generate_splits_spark(
    input_path: str,
    output_dir: str,
    main_class: str,
    *,
    num_subsets: int = 4,
    overlap_ratio: float = 0.04,
    subset_size: int,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    seed: int = 42,
    master: str | None = None,
    main_class_scope: MainClassScope = "reachable",
) -> dict[str, int | str | dict]:
    from pyodibel.management.spark_mgr import get_spark_session
    from pyodibel.operations.rdf.rdf2 import rDF2

    normalized_class = rDF2.normalize_class_uri(main_class)
    spark = get_spark_session("Text2KGBenchSplits", master=master)
    try:
        rdf = rDF2.parse(spark, input_path)
        type_expr = rdf._type_filter_expr()
        main_class_rows = (
            rdf.df
            .filter(type_expr & (rdf.df["o"] == normalized_class))
            .select("s")
            .distinct()
            .collect()
        )
        main_class_entities = sorted(row.s for row in main_class_rows)
        if not main_class_entities:
            raise ValueError(f"No entities of class {normalized_class} in {input_path}")

        shuffled = main_class_entities.copy()
        random.Random(seed).shuffle(shuffled)
        subsets = build_exact_subsets(shuffled, num_subsets, overlap_ratio, subset_size)

        out = Path(output_dir)
        if out.exists():
            raise ValueError(f"output directory already exists: {output_dir}")
        out.mkdir(parents=True)

        _write_entity_list(out / "entities" / "master_entities.csv", main_class_entities)

        split_stats: list[dict[str, int]] = []
        seed_entity_sets: list[set[str]] = []
        subgraph_entity_sets: list[set[str]] = []
        subgraph_main_class_sets: list[set[str]] = []

        for idx, subset in enumerate(subsets):
            ref_dir = _split_reference_dir(out, idx)
            ref_dir.mkdir(parents=True, exist_ok=True)

            _write_entity_list(ref_dir.parent.parent / "index" / "entities.csv", subset)

            split_rdf = rdf.filter_reachable_from_entities(
                subset,
                max_hops=max_hops,
                direction=direction,
                main_class=main_class,
                main_class_scope=main_class_scope,
            )
            split_subjects = {row.s for row in split_rdf.df.select("s").distinct().collect()}
            split_entities = len(split_subjects)
            split_triples_count = split_rdf.df.count()
            split_main_class_entities = {
                row.s
                for row in (
                    split_rdf.df
                    .filter(type_expr & (split_rdf.df["o"] == normalized_class))
                    .select("s")
                    .distinct()
                    .collect()
                )
            }

            seed_entity_sets.append(set(subset))
            subgraph_entity_sets.append(split_subjects)
            subgraph_main_class_sets.append(split_main_class_entities)

            data_nt = ref_dir / "data.nt"
            if data_nt.exists():
                raise ValueError(f"output path already exists: {data_nt}")
            split_rdf.write_nt(str(data_nt))

            if idx == 0:
                agg_rdf = split_rdf
            else:
                prev_agg = rDF2.parse(spark, str(_split_reference_dir(out, idx - 1) / "data_agg.nt"))
                agg_rdf = rDF2(
                    prev_agg.df.unionByName(split_rdf.df).dropDuplicates(["s", "p", "o", "isLiteral"])
                )

            data_agg = ref_dir / "data_agg.nt"
            if data_agg.exists():
                raise ValueError(f"output path already exists: {data_agg}")
            agg_rdf.write_nt(str(data_agg))

            type_rows = (
                split_rdf.df
                .filter(type_expr)
                .select("s", "o")
                .dropDuplicates(["s", "o"])
                .collect()
            )
            dataset_name = f"split_{idx}/kg/reference"
            meta_path = ref_dir / "meta" / "verified_entities.csv"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            with meta_path.open("w", encoding="utf-8") as handle:
                handle.write("dataset\tentity_id\tentity_type\n")
                for row in sorted(type_rows, key=lambda value: value.s):
                    handle.write(f"{dataset_name}\t{row.s}\t{row.o}\n")

            split_stats.append(
                {
                    "split": idx,
                    "seed_entities": len(subset),
                    "subgraph_entities": split_entities,
                    "subgraph_main_class_entities": len(split_main_class_entities),
                    "subgraph_triples": split_triples_count,
                }
            )

        seed_overlaps = validate_overlaps(subsets)
        subgraph_overlaps = pairwise_entity_overlaps(subgraph_entity_sets)
        subgraph_main_class_overlaps = pairwise_entity_overlaps(subgraph_main_class_sets)

        return {
            "engine": "spark",
            "main_class": normalized_class,
            "main_class_entities": len(main_class_entities),
            "num_subsets": num_subsets,
            "subset_size": subset_size,
            "overlap_ratio": overlap_ratio,
            "direction": direction,
            "max_hops": max_hops if max_hops is not None else "unlimited",
            "main_class_scope": main_class_scope,
            "splits": split_stats,
            "seed_overlaps": seed_overlaps,
            "subgraph_overlaps": subgraph_overlaps,
            "subgraph_main_class_overlaps": subgraph_main_class_overlaps,
            "output_dir": output_dir,
        }
    finally:
        spark.stop()


def generate_splits(
    input_path: str,
    output_dir: str,
    main_class: str,
    *,
    engine: Engine = "memory",
    num_subsets: int = 4,
    overlap_ratio: float = 0.04,
    subset_size: int,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    parser: Literal["stream", "rdflib"] = "stream",
    seed: int = 42,
    master: str | None = None,
    main_class_scope: MainClassScope = "reachable",
) -> dict[str, int | str | dict]:
    if engine == "memory":
        return generate_splits_memory(
            input_path,
            output_dir,
            main_class,
            num_subsets=num_subsets,
            overlap_ratio=overlap_ratio,
            subset_size=subset_size,
            max_hops=max_hops,
            direction=direction,
            parser=parser,
            seed=seed,
            main_class_scope=main_class_scope,
        )
    return generate_splits_spark(
        input_path,
        output_dir,
        main_class,
        num_subsets=num_subsets,
        overlap_ratio=overlap_ratio,
        subset_size=subset_size,
        max_hops=max_hops,
        direction=direction,
        seed=seed,
        master=master,
        main_class_scope=main_class_scope,
    )


def _print_overlap_section(
    title: str,
    overlaps: dict[str, dict[str, int | float]] | dict[str, float],
) -> None:
    print(title)
    if not overlaps:
        print("  (none)")
        return
    for pair, value in overlaps.items():
        if isinstance(value, dict):
            print(
                f"  {pair}: {int(value['count']):,} shared "
                f"({value['pct_left']:.1%} of left, {value['pct_right']:.1%} of right)"
            )
        else:
            print(f"  {pair}: {value:.4f}")
    print()


def _print_summary(stats: dict[str, int | str | dict]) -> None:
    print(f"Engine:              {stats['engine']}")
    print(f"Main class:          {stats['main_class']}")
    print(f"Main class entities: {int(stats['main_class_entities']):,}")
    print(f"Subsets:             {stats['num_subsets']} x {stats['subset_size']}")
    print(f"Target seed overlap: {stats['overlap_ratio']}")
    print(f"Direction:           {stats['direction']}")
    print(f"Max hops:            {stats['max_hops']}")
    print(f"Main-class scope:    {stats['main_class_scope']}")
    print()
    if stats["main_class_scope"] == "seeds":
        print(
            "Subgraphs are seed-scoped induced subgraphs: all triples whose subject is "
            "reachable from the split seeds, but only seed main-class entities are kept."
        )
    else:
        print(
            "Subgraphs are complete induced subgraphs: all triples whose subject is "
            "reachable from the split seeds within the input NT."
        )
    print()
    for split in stats["splits"]:
        print(
            f"split_{split['split']}: "
            f"{split['seed_entities']} seeds -> "
            f"{split['subgraph_entities']:,} subgraph entities "
            f"({split['subgraph_main_class_entities']:,} main-class), "
            f"{split['subgraph_triples']:,} triples"
        )
    print()
    _print_overlap_section(
        "Seed overlap (main-class entities by design):",
        stats["seed_overlaps"],
    )
    _print_overlap_section(
        "Subgraph overlap (all entities in reference graphs):",
        stats["subgraph_overlaps"],
    )
    _print_overlap_section(
        "Subgraph overlap (main-class entities only):",
        stats["subgraph_main_class_overlaps"],
    )
    print(f"Wrote: {stats['output_dir']}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build overlapping splits and reference subgraphs from a reachable NT file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("input_path", nargs="?", help="Input NT file")
    parser.add_argument("output_dir", nargs="?", help="Output directory (must not exist)")
    parser.add_argument(
        "--main-class",
        required=False,
        help="Main class URI or dbo:Name shorthand",
    )
    parser.add_argument("--num-subsets", type=int, default=4)
    parser.add_argument("--overlap-ratio", type=float, default=0.04)
    parser.add_argument("--subset-size", type=int, required=False)
    parser.add_argument(
        "--engine",
        choices=("memory", "spark"),
        default="memory",
    )
    parser.add_argument(
        "--parser",
        choices=("stream", "rdflib"),
        default="stream",
        help="NT parser for --engine memory",
    )
    parser.add_argument("--max-hops", type=int, default=None)
    parser.add_argument(
        "--direction",
        choices=("forward", "backward", "both"),
        default="forward",
    )
    parser.add_argument(
        "--main-class-scope",
        choices=("reachable", "seeds"),
        default="reachable",
        help=(
            "Whether reachable main-class entities beyond the split seeds are included "
            "in reference subgraphs (reachable) or limited to seeds only (seeds)"
        ),
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--master", default=None, help="Optional Spark master URL")
    return parser


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    args = _build_parser().parse_args(argv)

    if not args.input_path or not args.output_dir or not args.main_class or args.subset_size is None:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))

    input_path = args.input_path or os.getenv("INPUT_PATH")
    output_dir = args.output_dir or os.getenv("OUTPUT_PATH")
    main_class = args.main_class or os.getenv("MAIN_CLASS")
    subset_size = args.subset_size
    if subset_size is None and os.getenv("SUBSET_SIZE"):
        subset_size = int(os.getenv("SUBSET_SIZE", ""))

    if not input_path:
        raise ValueError("input_path not provided and INPUT_PATH not set")
    if not output_dir:
        raise ValueError("output_dir not provided and OUTPUT_PATH not set")
    if not main_class:
        raise ValueError("--main-class not provided and MAIN_CLASS not set")
    if subset_size is None:
        raise ValueError("--subset-size not provided and SUBSET_SIZE not set")

    stats = generate_splits(
        input_path,
        output_dir,
        main_class,
        engine=args.engine,
        num_subsets=args.num_subsets,
        overlap_ratio=args.overlap_ratio,
        subset_size=subset_size,
        max_hops=args.max_hops,
        direction=args.direction,
        parser=args.parser,
        seed=args.seed,
        master=args.master,
        main_class_scope=args.main_class_scope,
    )
    _print_summary(stats)


if __name__ == "__main__":
    main()
