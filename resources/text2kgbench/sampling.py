#!/usr/bin/env python3
"""CLI for Text2KGBench subgraph sampling via pyodibel.operations.sample."""

from __future__ import annotations

import argparse
import os
import sys

from pyodibel.operations.sample.io import sample_nt_file
from pyodibel.operations.sample.subgraph import DEFAULT_DEGREE_BINS


def _parse_degree_bins(raw: str) -> tuple[int, ...]:
    bins = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not bins or any(value <= 0 for value in bins):
        raise argparse.ArgumentTypeError("degree bins must be positive integers, e.g. 5,20,100")
    if sorted(bins) != list(bins):
        raise argparse.ArgumentTypeError("degree bins must be in ascending order")
    return bins


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Downsample RDF subgraphs while preserving key distributions.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("input_path", nargs="?", help="Input NT file or directory")
    parser.add_argument("output_path", nargs="?", help="Output NT path (must not exist)")
    parser.add_argument(
        "--method",
        choices=("subgraph", "stratified"),
        default="subgraph",
        help="subgraph preserves class/degree via entity sampling; "
        "stratified preserves relation counts at triple level",
    )
    size = parser.add_mutually_exclusive_group(required=False)
    size.add_argument(
        "--fraction",
        type=float,
        help="Fraction to keep (entities for subgraph, triples for stratified)",
    )
    size.add_argument("--n", type=int, help="Exact count to keep")
    parser.add_argument(
        "--stratify-by",
        choices=("relation", "composite"),
        default="relation",
        help="Only used with --method stratified",
    )
    parser.add_argument(
        "--degree-metric",
        choices=("out_triples", "out_predicates"),
        default="out_triples",
        help="Only used with --method subgraph",
    )
    parser.add_argument(
        "--degree-bins",
        type=_parse_degree_bins,
        default=DEFAULT_DEGREE_BINS,
        metavar="BIN,...",
        help="Upper bounds for degree bins, e.g. 5,20,100",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-report", action="store_true", help="Skip distribution comparison")
    parser.add_argument("--master", default=None, help="Optional Spark master URL")
    return parser


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    args = _build_parser().parse_args(argv)

    if not args.input_path or not args.output_path:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))

    input_path = args.input_path or os.getenv("INPUT_PATH")
    output_path = args.output_path or os.getenv("OUTPUT_PATH")
    if not input_path:
        raise ValueError("input_path not provided and INPUT_PATH not set")
    if not output_path:
        raise ValueError("output_path not provided and OUTPUT_PATH not set")
    if args.fraction is None and args.n is None:
        raise ValueError("Provide --fraction or --n")

    sample_nt_file(
        input_path,
        output_path,
        method=args.method,
        fraction=args.fraction,
        n=args.n,
        stratify_by=args.stratify_by,
        degree_metric=args.degree_metric,
        degree_bins=args.degree_bins,
        seed=args.seed,
        master=args.master,
        report=not args.no_report,
    )


if __name__ == "__main__":
    main()
