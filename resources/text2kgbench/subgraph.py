#!/usr/bin/env python3
"""
Extract a class-rooted reachable subgraph from a property-filtered DBpedia NT file.

Starting from entities of a root class (e.g. dbo:Food), follows resource edges
to include indirectly connected entities such as City or Country, then keeps all
triples for entities in that reachable set.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Literal

Engine = Literal["memory", "spark"]
TraversalDirection = Literal["forward", "backward", "both"]


def _print_stats(stats: dict[str, int | str], output_path: str) -> None:
    before_triples = int(stats["before_triples"])
    after_triples = int(stats["after_triples"])
    print(f"Engine:          {stats['engine']}")
    print(f"Root class:      {stats['root_class']}")
    print(f"Direction:       {stats['direction']}")
    print(f"Max hops:        {stats['max_hops']}")
    print(f"Input entities:  {int(stats['before_entities']):,}")
    print(f"Input triples:   {before_triples:,}")
    print(f"Output entities: {int(stats['after_entities']):,}")
    print(f"Output triples:  {after_triples:,}")
    if before_triples:
        print(f"Triple ratio:    {after_triples / before_triples:.1%}")
    print(f"Wrote:           {output_path}")


def extract_reachable_subgraph_memory(
    input_path: str,
    output_path: str,
    root_class: str,
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    parser: Literal["stream", "rdflib"] = "stream",
) -> None:
    from pyodibel.operations.rdf.reachability import filter_reachable_nt_file

    stats = filter_reachable_nt_file(
        input_path,
        output_path,
        root_class,
        max_hops=max_hops,
        direction=direction,
        parser=parser,
    )
    stats["engine"] = "memory"
    _print_stats(stats, output_path)


def extract_reachable_subgraph_spark(
    input_path: str,
    output_path: str,
    root_class: str,
    *,
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    master: str | None = None,
) -> None:
    from pyodibel.management.spark_mgr import get_spark_session
    from pyodibel.operations.rdf.rdf2 import rDF2

    spark = get_spark_session("Text2KGBenchReachableSubgraph", master=master)
    try:
        rdf = rDF2.parse(spark, input_path)
        before_entities = rdf.df.select("s").distinct().count()
        before_triples = rdf.df.count()

        subgraph = rdf.filter_reachable_from_classes(
            root_class,
            max_hops=max_hops,
            direction=direction,
        )

        after_entities = subgraph.df.select("s").distinct().count()
        after_triples = subgraph.df.count()

        if os.path.exists(output_path):
            raise ValueError(f"output path already exists: {output_path}")

        subgraph.write_nt(output_path)

        _print_stats(
            {
                "engine": "spark",
                "root_class": rDF2.normalize_class_uri(root_class),
                "direction": direction,
                "max_hops": max_hops if max_hops is not None else "unlimited",
                "before_entities": before_entities,
                "before_triples": before_triples,
                "after_entities": after_entities,
                "after_triples": after_triples,
            },
            output_path,
        )
    finally:
        spark.stop()


def extract_reachable_subgraph(
    input_path: str,
    output_path: str,
    root_class: str,
    *,
    engine: Engine = "memory",
    max_hops: int | None = None,
    direction: TraversalDirection = "forward",
    parser: Literal["stream", "rdflib"] = "stream",
    master: str | None = None,
) -> None:
    if engine == "memory":
        extract_reachable_subgraph_memory(
            input_path,
            output_path,
            root_class,
            max_hops=max_hops,
            direction=direction,
            parser=parser,
        )
        return

    extract_reachable_subgraph_spark(
        input_path,
        output_path,
        root_class,
        max_hops=max_hops,
        direction=direction,
        master=master,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract subgraph reachable from a root RDF class.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("input_path", nargs="?", help="Input NT file or directory")
    parser.add_argument("output_path", nargs="?", help="Output NT path (must not exist)")
    parser.add_argument(
        "--root-class",
        required=False,
        help="Root class URI or dbo:Name shorthand, e.g. dbo:Food",
    )
    parser.add_argument(
        "--engine",
        choices=("memory", "spark"),
        default="memory",
        help="memory uses in-RAM adjacency indexes; spark uses rDF2 on Spark",
    )
    parser.add_argument(
        "--parser",
        choices=("stream", "rdflib"),
        default="stream",
        help="NT parser for --engine memory (stream is faster for large files)",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=None,
        help="Maximum traversal hops from seed entities (default: unlimited)",
    )
    parser.add_argument(
        "--direction",
        choices=("forward", "backward", "both"),
        default="forward",
        help="Edge traversal direction for reachability expansion",
    )
    parser.add_argument("--master", default=None, help="Optional Spark master URL")
    return parser


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    args = _build_parser().parse_args(argv)

    if not args.input_path or not args.output_path or not args.root_class:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))

    input_path = args.input_path or os.getenv("INPUT_PATH")
    output_path = args.output_path or os.getenv("OUTPUT_PATH")
    root_class = args.root_class or os.getenv("ROOT_CLASS")
    if not input_path:
        raise ValueError("input_path not provided and INPUT_PATH not set")
    if not output_path:
        raise ValueError("output_path not provided and OUTPUT_PATH not set")
    if not root_class:
        raise ValueError("--root-class not provided and ROOT_CLASS not set")

    extract_reachable_subgraph(
        input_path,
        output_path,
        root_class,
        engine=args.engine,
        max_hops=args.max_hops,
        direction=args.direction,
        parser=args.parser,
        master=args.master,
    )


if __name__ == "__main__":
    main()
