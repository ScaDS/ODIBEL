"""
Validate a small Text2KGBench RDF run (generate_refac, subgraph, splits, sources).

Recomputes every step independently from its input and compares, measures seed and
subgraph overlap, and checks that shaded sources carry no DBpedia IRIs. Prints JSON;
intended for ``.odibel/stats.json`` of a run dataset.

Usage:
    uv run resources/text2kgbench/validate_run.py <run_dir> <input.nt[.bz2]> <ontology.ttl> \
        --main-class dbo:Food --subset-size 100 --num-subsets 4 --overlap-ratio 0.04

``run_dir`` holds ``subgraphs/<prefix>_subgraph/{property_filter.nt,reachable.nt}`` and ``splits/``.
"""

from __future__ import annotations

import argparse
import bz2
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict, deque
from itertools import combinations
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from generate_refac import RDF_TYPE_URI, RDFS_LABEL_URI, extract_from_ontology  # noqa: E402

DBR = "http://dbpedia.org/resource/"
DBO = "http://dbpedia.org/ontology/"
SKOS_ALT = "<http://www.w3.org/2004/02/skos/core#altLabel>"
SEED_NS = "http://kg.org/resource/"

Triple = tuple[str, str, str]


def read_nt(path: Path) -> list[Triple]:
    opener = bz2.open if path.suffix == ".bz2" else open
    triples = []
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                s, p, rest = line.split(" ", 2)
                triples.append((s, p, re.sub(r"\s*\.\s*$", "", rest)))
    return triples


def read_entities(path: Path) -> list[str]:
    """One IRI per line after the header; IRIs may contain commas and are not quoted."""
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines[1:] if line.strip()]


def iri(term: str) -> str:
    return f"<{term}>" if not term.startswith("<") else term


def digest(triples) -> str:
    h = hashlib.sha256()
    for line in sorted(f"{s} {p} {o} .\n" for s, p, o in triples):
        h.update(line.encode("utf-8"))
    return h.hexdigest()


def typed(triples, cls: str) -> set[str]:
    return {s for s, p, o in triples if p == RDF_TYPE_URI and o == cls}


def reachable(triples: list[Triple], seeds: set[str]) -> set[str]:
    """Forward reachability over IRI objects, as in rDF2/NTripleGraph (unlimited hops)."""
    out = defaultdict(set)
    for s, p, o in triples:
        if p != RDF_TYPE_URI and o.startswith("<"):
            out[s].add(o)
    seen, queue = set(seeds), deque(seeds)
    while queue:
        for nxt in out.get(queue.popleft(), ()):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return seen


def check_property_filter(input_triples, filtered, classes, properties) -> dict:
    allowed = set(properties) | {RDF_TYPE_URI, RDFS_LABEL_URI}
    entities = {s for s, p, o in input_triples if p == RDF_TYPE_URI and o in classes}
    expected = [
        (s, p, o) for s, p, o in input_triples
        if s in entities and p in allowed
        and (not o.startswith("<") or o in entities or (p == RDF_TYPE_URI and o in classes))
    ]
    return {
        "triples": len(filtered),
        "matches_recount": Counter(expected) == Counter(filtered),
        "predicates_outside_ontology": sorted({p for s, p, o in filtered} - allowed),
    }


def check_reachable(filtered, result, main_class) -> dict:
    keep = reachable(filtered, typed(filtered, main_class))
    expected = [t for t in filtered if t[0] in keep]
    return {
        "triples": len(result),
        "entities": len({s for s, p, o in result}),
        "matches_recount": Counter(expected) == Counter(result),
    }


def check_splits(splits_dir: Path, reach, main_class, args) -> tuple[dict, list[set[str]]]:
    master = set(read_entities(splits_dir / "entities" / "master_entities.csv"))
    report: dict = {"master_matches_main_class": master == typed(reach, main_class),
                    "master_entities": len(master), "splits": {}}
    split_dirs = sorted(splits_dir.glob("split_*"), key=lambda p: int(p.name.split("_")[1]))
    seeds_per_split, refs, agg = [], [], set()
    for split in split_dirs:
        seeds = set(read_entities(split / "index" / "entities.csv"))
        ref = read_nt(split / "kg" / "reference" / "data.nt")
        keep = reachable(reach, seeds)
        expected = {t for t in reach if t[0] in keep}
        agg |= set(ref)
        report["splits"][split.name] = {
            "seeds": len(seeds),
            "seeds_are_main_class": seeds <= master,
            "reference_triples": len(ref),
            "reference_entities": len({s for s, p, o in ref}),
            "reference_main_class_entities": len(typed(ref, main_class)),
            "reference_matches_reachability": expected == set(ref),
            "data_agg_is_union_so_far": set(read_nt(split / "kg" / "reference" / "data_agg.nt")) == agg,
            "reference_content_sha256": digest(ref),
        }
        seeds_per_split.append(seeds)
        refs.append(ref)

    expected_shared = round(args.overlap_ratio * args.subset_size)
    pairs = list(combinations(range(len(split_dirs)), 2))
    report["seed_overlap"] = {f"{a}-{b}": len(seeds_per_split[a] & seeds_per_split[b]) for a, b in pairs}
    report["seed_overlap_as_configured"] = all(n == expected_shared for n in report["seed_overlap"].values())
    entity_sets = [{s for s, p, o in r} for r in refs]
    main_sets = [typed(r, main_class) for r in refs]
    report["reference_overlap_entities"] = {
        f"{a}-{b}": round(len(entity_sets[a] & entity_sets[b]) / min(len(entity_sets[a]), len(entity_sets[b])), 4)
        for a, b in pairs
    }
    report["reference_overlap_main_class"] = {
        f"{a}-{b}": round(len(main_sets[a] & main_sets[b]) / min(len(main_sets[a]), len(main_sets[b])), 4)
        for a, b in pairs
    }
    return report, entity_sets


def expected_shading(ref: list[Triple], resource_ns: str, ontology_ns: str) -> set[Triple]:
    def term(t: str) -> str:
        if t.startswith(f"<{DBR}"):
            return f"<{resource_ns}{hashlib.md5(t[1:-1].encode()).hexdigest()}>"
        if t.startswith(f"<{DBO}"):
            return f"<{ontology_ns}{t[len(DBO) + 1:]}"
        return t

    shaded = {(term(s), term(p), term(o)) for s, p, o in ref}
    shaded |= {(s, SKOS_ALT, o) for s, p, o in shaded if p == RDFS_LABEL_URI}
    return shaded


def check_sources(splits_dir: Path) -> dict:
    report: dict = {}
    seed_ids, rdf_ids = {}, {}
    for split in sorted(splits_dir.glob("split_*"), key=lambda p: int(p.name.split("_")[1])):
        idx = split.name.split("_")[1]
        ref = read_nt(split / "kg" / "reference" / "data.nt")
        seed = read_nt(split / "kg" / "seed" / "data.nt")
        rdf = read_nt(split / "sources" / "rdf" / "data.nt")
        rdf_ns = f"http://kg.org/rdf/{idx}/resource/"
        leaks = lambda ts: sum(DBR in " ".join(t) or DBO in " ".join(t) for t in ts)
        report[split.name] = {
            "seed_triples": len(seed),
            "rdf_triples": len(rdf),
            "seed_matches_expected_shading": set(seed) == expected_shading(ref, SEED_NS, "http://kg.org/ontology/"),
            "rdf_matches_expected_shading": set(rdf) == expected_shading(ref, rdf_ns, f"http://kg.org/rdf/{idx}/ontology/"),
            "dbpedia_iris_in_seed": leaks(seed),
            "dbpedia_iris_in_rdf": leaks(rdf),
        }
        seed_ids[split.name] = {s[1:-1].rsplit("/", 1)[1] for s, p, o in seed if s.startswith(f"<{SEED_NS}")}
        rdf_ids[split.name] = {s[1:-1].rsplit("/", 1)[1] for s, p, o in rdf if s.startswith(f"<{rdf_ns}")}

    report["shared_local_names_seed_vs_rdf"] = {
        name: round(len(seed_ids[name] & rdf_ids[name]) / max(len(seed_ids[name]), 1), 4) for name in seed_ids
    }
    names = sorted(rdf_ids)
    report["shared_local_names_across_rdf_splits"] = {
        f"{a}-{b}": len(rdf_ids[a] & rdf_ids[b]) for a, b in combinations(names, 2)
    }
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("input", type=Path)
    parser.add_argument("ontology", type=Path)
    parser.add_argument("--main-class", required=True)
    parser.add_argument("--subset-size", type=int, required=True)
    parser.add_argument("--num-subsets", type=int, default=4)
    parser.add_argument("--overlap-ratio", type=float, default=0.04)
    args = parser.parse_args(argv)

    main_class = iri(args.main_class.replace("dbo:", DBO))
    classes, properties = extract_from_ontology(str(args.ontology))
    subgraph_dir = next((args.run_dir / "subgraphs").glob("*_subgraph"))
    input_triples = read_nt(args.input)
    filtered = read_nt(subgraph_dir / "property_filter.nt")
    reach = read_nt(subgraph_dir / "reachable.nt")
    splits_report, _ = check_splits(args.run_dir / "splits", reach, main_class, args)

    result = {
        "input_triples": len(input_triples),
        "ontology": {"classes": len(classes), "properties": len(properties)},
        "main_class": main_class,
        "property_filter": check_property_filter(input_triples, filtered, set(classes), properties),
        "reachable": check_reachable(filtered, reach, main_class),
        "splits": splits_report,
        "sources": check_sources(args.run_dir / "splits"),
    }
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
