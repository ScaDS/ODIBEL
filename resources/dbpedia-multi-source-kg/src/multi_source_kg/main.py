from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _bootstrap_import_path() -> None:
    src_root = Path(__file__).resolve().parents[1]
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))


_bootstrap_import_path()

from multi_source_kg.llm_classifier import LLMClassifier, LLMSettings
from multi_source_kg.hierarchy_tree import export_hierarchy_tree_by_subdomain
from multi_source_kg.pipeline import PipelineOptions, run_pipeline


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Classify DBpedia ontology classes into fixed subdomains."
    )
    parser.add_argument("--input", type=Path, default=root / "dbo-snapshots.ttl")
    parser.add_argument("--output", type=Path, default=root / "dbpedia_class_subdomains.yaml")
    parser.add_argument(
        "--export-hierarchy-tree",
        action="store_true",
        help="Export per-subdomain class hierarchy tree from the YAML mapping.",
    )
    parser.add_argument(
        "--tree-output",
        type=Path,
        default=root / "dbpedia_class_hierarchy_by_subdomain.txt",
        help="Path for hierarchy tree text output.",
    )
    parser.add_argument(
        "--method",
        choices=["rules", "hybrid", "llm"],
        default="rules",
        help="Classification strategy.",
    )
    parser.add_argument("--max-classes", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.export_hierarchy_tree:
        domain_count, class_count = export_hierarchy_tree_by_subdomain(
            class_subdomains_yaml=args.output,
            ontology_ttl=args.input,
            output_tree_txt=args.tree_output,
        )
        print(f"Wrote hierarchy tree to {args.tree_output}")
        print(f"Domains: {domain_count}")
        print(f"Total mapped classes: {class_count}")
        return

    llm_classifier = None

    if args.method in {"hybrid", "llm"}:
        settings = LLMSettings.from_env()
        if settings is None:
            raise RuntimeError(
                "OPENAI_API_KEY is required for --method hybrid/llm. "
                "Optional: OPENAI_MODEL, OPENAI_BASE_URL, OPENAI_TIMEOUT_SECONDS."
            )
        llm_classifier = LLMClassifier(settings=settings)

    total, low_count, llm_attempted, llm_succeeded = run_pipeline(
        input_ttl=args.input,
        output_yaml=args.output,
        options=PipelineOptions(method=args.method, max_classes=args.max_classes),
        llm_classifier=llm_classifier,
    )
    print(f"Wrote {total} class mappings to {args.output}")
    print(f"Low-confidence/manual review classes: {low_count}")
    print(f"Method: {args.method}")
    if args.method in {"hybrid", "llm"}:
        print(f"LLM calls succeeded: {llm_succeeded}/{llm_attempted}")


if __name__ == "__main__":
    main()
