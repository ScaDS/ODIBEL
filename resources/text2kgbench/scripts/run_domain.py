#!/usr/bin/env python3
"""
Run the RDF-path KGI-Bench generation for a Text2KGBench domain.

Stages: property filter → reachable subgraph → optional sample → splits → shaded sources.
Emits generation_config.yaml, dataset_card.json, and updates registry.json.

Example:
  uv run resources/text2kgbench/run_domain.py airport --size 100 --skip-sample
  uv run resources/text2kgbench/run_domain.py celestialbody --size 100 --from-stage subgraph
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

# Scripts live under resources/ but that tree is not a Python package.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from pyodibel.datasets.kgi_provenance import (
    MeasuredOutcomes,
    SoftwareProvenance,
    build_dataset_card,
    build_dataset_id,
    build_generation_config,
    default_rdf_only_recipe,
    detect_git_commit,
    detect_pyodibel_version,
    register_dataset,
    save_dataset_card,
    save_generation_config,
)

DOMAINS = {
    "airport": {
        "ontology": "3_airport_ontology.ttl",
        "root_class": "dbo:Airport",
        "prefix": "ont_3",
        "motivation": (
            "Infrastructure/geo domain contrasting MovieKG: place hierarchy and "
            "facility attributes rather than person-role fusion."
        ),
    },
    "celestialbody": {
        "ontology": "8_celestialbody_ontology.ttl",
        "root_class": "dbo:CelestialBody",
        "prefix": "ont_8",
        "motivation": (
            "Scientific/numeric-heavy domain contrasting MovieKG: magnitudes and "
            "orbital attributes stress literal/datatype handling."
        ),
    },
    "food": {
        "ontology": "13_food_ontology.ttl",
        "root_class": "dbo:Food",
        "prefix": "ont_13",
        "motivation": (
            "Product/taxonomy domain (ingredients, genus/family, geographic origin) "
            "contrasting MovieKG creative-work hubs."
        ),
    },
}

STAGES = ("filter", "subgraph", "sample", "splits", "sources")


def _default_paths() -> dict[str, Path]:
    data_root = Path(os.getenv("KGI_DATA_ROOT", "/local/d1/datasets"))
    return {
        "dump": data_root / "dbpedia-multi-source-kg-data" / "selected.nt.bz2",
        "ontologies": data_root
        / "text2kgbench"
        / "data"
        / "kgpipe-ontologies"
        / "dbpedia_webnlg",
        "work": data_root / "kgi-bench-text2kgbench",
    }


def _size_params(size: int) -> tuple[int, int, float]:
    """Return (subset_size, num_subsets, overlap_ratio) for a target root-entity budget."""
    if size == 100:
        return 25, 4, 0.04
    if size == 1000:
        return 250, 4, 0.04
    if size == 10000:
        return 2500, 4, 0.04
    raise ValueError(f"Unsupported size {size}; use 100, 1000, or 10000")


def _patch_spark(driver_memory: str, master: str) -> None:
    import pyodibel.management.spark_mgr as sm

    orig = sm.get_spark_session

    def wrapped(app_name, master_arg=None, **kwargs):
        kwargs.setdefault("driver_memory", driver_memory)
        kwargs.setdefault("shuffle_partitions", 64)
        return orig(app_name, master=master_arg or master, **kwargs)

    sm.get_spark_session = wrapped


def run_filter(paths: dict[str, Path], domain: str, cfg: dict) -> Path:
    from generate_refac import generate

    out = paths["work"] / "subgraphs"
    out.mkdir(parents=True, exist_ok=True)
    ontology = paths["ontologies"] / cfg["ontology"]
    dump = f"file://{paths['dump']}"
    generate(dump, str(out), str(ontology), name=cfg["prefix"])
    return out / f"{cfg['prefix']}_subgraph" / "property_filter"


def run_subgraph(
    paths: dict[str, Path],
    cfg: dict,
    *,
    engine: str,
    max_hops: int | None,
    direction: str,
) -> Path:
    from subgraph import extract_reachable_subgraph

    subgraph_dir = paths["work"] / "subgraphs" / f"{cfg['prefix']}_subgraph"
    candidates = [
        subgraph_dir / "property_filter",
        subgraph_dir / "property_filter.nt",
    ]
    input_path = next((p for p in candidates if p.exists()), None)
    if input_path is None:
        raise FileNotFoundError(
            f"No property_filter output under {subgraph_dir}; run filter stage first"
        )

    output_path = subgraph_dir / "reachable.nt"
    if output_path.exists():
        print(f"Skipping existing reachable subgraph: {output_path}")
        return output_path

    extract_reachable_subgraph(
        str(input_path),
        str(output_path),
        cfg["root_class"],
        engine=engine,  # type: ignore[arg-type]
        max_hops=max_hops,
        direction=direction,  # type: ignore[arg-type]
    )
    return output_path


def run_sample(
    paths: dict[str, Path],
    cfg: dict,
    *,
    size: int,
    seed: int,
) -> Path:
    from pyodibel.operations.sample.io import sample_nt_file

    subgraph_dir = paths["work"] / "subgraphs" / f"{cfg['prefix']}_subgraph"
    input_path = subgraph_dir / "reachable.nt"
    if not input_path.exists():
        raise FileNotFoundError(f"Missing reachable graph: {input_path}")

    output_path = subgraph_dir / f"reachable_sample_{size}.nt"
    if output_path.exists():
        print(f"Skipping existing sample: {output_path}")
        return output_path

    sample_nt_file(
        str(input_path),
        str(output_path),
        method="subgraph",
        n=size,
        seed=seed,
        report=True,
    )
    return output_path


def run_splits(
    *,
    output_dir: Path,
    main_class: str,
    subset_size: int,
    num_subsets: int,
    overlap_ratio: float,
    seed: int,
    engine: str,
    input_nt: Path,
    max_hops: int | None,
    direction: str,
    main_class_scope: str,
) -> tuple[Path, dict]:
    from splits import generate_splits

    if output_dir.exists():
        print(f"Skipping existing splits: {output_dir}")
        return output_dir, {}

    stats = generate_splits(
        str(input_nt),
        str(output_dir),
        main_class,
        engine=engine,  # type: ignore[arg-type]
        num_subsets=num_subsets,
        overlap_ratio=overlap_ratio,
        subset_size=subset_size,
        seed=seed,
        max_hops=max_hops,
        direction=direction,  # type: ignore[arg-type]
        main_class_scope=main_class_scope,  # type: ignore[arg-type]
    )
    return output_dir, stats


def run_sources(splits_dir: Path) -> None:
    from sources import generate_sources

    generate_sources(str(splits_dir), overwrite=False)


def _emit_provenance(
    *,
    paths: dict[str, Path],
    domain: str,
    domain_cfg: dict,
    dataset_dir: Path,
    dataset_id: str,
    config,
    split_stats: dict,
    stages: list[str],
    engine: str,
    status: str,
) -> None:
    ontology_src = paths["ontologies"] / domain_cfg["ontology"]
    ontology_dst = dataset_dir / "ontology.ttl"
    if ontology_src.is_file() and not ontology_dst.exists():
        shutil.copy2(ontology_src, ontology_dst)

    config.software = SoftwareProvenance(
        pyodibel_version=detect_pyodibel_version(),
        git_commit=detect_git_commit(),
        entrypoint="resources/text2kgbench/run_domain.py",
        stages=tuple(stages),
        engine=engine,
    )

    measured = MeasuredOutcomes(
        seed_overlaps=dict(split_stats.get("seed_overlaps") or {}),
        subgraph_overlaps=dict(split_stats.get("subgraph_overlaps") or {}),
        extra={
            "splits": split_stats.get("splits"),
            "main_class_entities": split_stats.get("main_class_entities"),
            "subgraph_main_class_overlaps": split_stats.get(
                "subgraph_main_class_overlaps"
            ),
        },
        quality_gate={
            "passed": status == "published",
            "notes": "Automatic gates not fully wired; draft until evaluate_dataset lands",
        },
    )

    artifacts = {
        "bundle_root": str(dataset_dir),
        "splits_glob": "split_*/",
        "reference": "split_*/kg/reference/data.nt",
        "seed": "split_*/kg/seed/data.nt",
        "rdf_source": "split_*/sources/rdf/data.nt",
        "ontology": "ontology.ttl",
        "generation_config": "generation_config.yaml",
        "dataset_card": "dataset_card.json",
        "parent_subgraph": str(
            paths["work"] / "subgraphs" / f"{domain_cfg['prefix']}_subgraph"
        ),
    }

    card = build_dataset_card(
        config,
        dataset_id=dataset_id,
        measured=measured,
        artifacts=artifacts,
        motivation=domain_cfg.get("motivation"),
        status=status,
    )
    save_generation_config(config, dataset_dir / "generation_config.yaml")
    save_dataset_card(card, dataset_dir / "dataset_card.json")
    register_dataset(
        paths["work"] / "registry.json",
        card,
        dataset_path=dataset_dir,
    )
    print(f"dataset_id: {dataset_id}")
    print(f"display:    {card.display_name}")
    print(f"card:       {dataset_dir / 'dataset_card.json'}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("domain", choices=sorted(DOMAINS))
    parser.add_argument("--size", type=int, default=100, choices=(100, 1000, 10000))
    parser.add_argument(
        "--from-stage",
        choices=STAGES,
        default="filter",
        help="Skip earlier stages (e.g. reuse an existing property_filter)",
    )
    parser.add_argument("--to-stage", choices=STAGES, default="sources")
    parser.add_argument("--engine", choices=("memory", "spark"), default="spark")
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
    )
    parser.add_argument("--overlap-ratio", type=float, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--master", default=os.getenv("MASTER", "local[16]"))
    parser.add_argument("--driver-memory", default="64g")
    parser.add_argument("--skip-sample", action="store_true")
    parser.add_argument(
        "--status",
        choices=("draft", "published", "deprecated"),
        default="draft",
    )
    parser.add_argument(
        "--dump-sha256",
        default=None,
        help="Optional precomputed dump checksum (skips hashing large files)",
    )
    args = parser.parse_args(argv)

    paths = _default_paths()
    domain_cfg = DOMAINS[args.domain]
    stage_order = list(STAGES)
    start = stage_order.index(args.from_stage)
    end = stage_order.index(args.to_stage)
    if end < start:
        raise SystemExit("--to-stage must be after --from-stage")

    subset_size, num_subsets, default_overlap = _size_params(args.size)
    overlap_ratio = default_overlap if args.overlap_ratio is None else args.overlap_ratio

    recipe = default_rdf_only_recipe(
        target_root_budget=args.size,
        subset_size=subset_size,
        num_subsets=num_subsets,
        overlap_ratio=overlap_ratio,
        seed=args.seed,
        max_hops=args.max_hops,
        direction=args.direction,
        main_class_scope=args.main_class_scope,
    )
    if not args.skip_sample and "sample" in stage_order[start : end + 1]:
        # Record that sampling was requested; method details filled if sample runs.
        from dataclasses import replace

        recipe = replace(recipe, sample_method="subgraph", sample_n=args.size)

    config = build_generation_config(
        domain=args.domain,
        ontology_id=domain_cfg["ontology"].removesuffix(".ttl"),
        ontology_path=paths["ontologies"] / domain_cfg["ontology"],
        root_class=domain_cfg["root_class"],
        dump_path=paths["dump"],
        dump_content_sha256=args.dump_sha256,
        recipe=recipe,
        parent_subgraph_id=f"{domain_cfg['prefix']}_subgraph",
    )
    dataset_id = build_dataset_id(config)
    dataset_dir = paths["work"] / "datasets" / dataset_id

    os.environ.setdefault("JAVA_HOME", os.environ.get("JAVA_HOME", ""))
    os.environ["MASTER"] = args.master
    os.environ.setdefault(
        "SPARK_LOCAL_DIR",
        str(paths["work"] / "spark-local"),
    )
    _patch_spark(args.driver_memory, args.master)

    active = stage_order[start : end + 1]
    print(f"Domain={args.domain} size={args.size} dataset_id={dataset_id}")
    print(f"Stages={active}")

    sample_input: Path | None = None
    split_stats: dict = {}

    if "filter" in active:
        run_filter(paths, args.domain, domain_cfg)

    if "subgraph" in active:
        reachable = run_subgraph(
            paths,
            domain_cfg,
            engine=args.engine,
            max_hops=args.max_hops,
            direction=args.direction,
        )
        sample_input = reachable

    if "sample" in active and not args.skip_sample:
        sample_input = run_sample(
            paths, domain_cfg, size=args.size, seed=args.seed
        )
    elif sample_input is None:
        sample_input = (
            paths["work"]
            / "subgraphs"
            / f"{domain_cfg['prefix']}_subgraph"
            / "reachable.nt"
        )

    if "splits" in active:
        assert sample_input is not None
        dataset_dir, split_stats = run_splits(
            output_dir=dataset_dir,
            main_class=domain_cfg["root_class"],
            subset_size=subset_size,
            num_subsets=num_subsets,
            overlap_ratio=overlap_ratio,
            seed=args.seed,
            engine="memory" if args.size <= 1000 else args.engine,
            input_nt=sample_input,
            max_hops=args.max_hops,
            direction=args.direction,
            main_class_scope=args.main_class_scope,
        )

    if "sources" in active:
        if not dataset_dir.exists():
            raise FileNotFoundError(
                f"dataset bundle missing: {dataset_dir}; run splits stage first"
            )
        run_sources(dataset_dir)

    if dataset_dir.exists() and (
        "splits" in active or "sources" in active or (dataset_dir / "split_0").exists()
    ):
        _emit_provenance(
            paths=paths,
            domain=args.domain,
            domain_cfg=domain_cfg,
            dataset_dir=dataset_dir,
            dataset_id=dataset_id,
            config=config,
            split_stats=split_stats,
            stages=active,
            engine=args.engine,
            status=args.status,
        )

    print("Done.")


if __name__ == "__main__":
    main()
