"""
Validate a generate.py run on a small input without Spark.

Checks each step against an independent recomputation, classifies the triples
removed by ``clean_domain_range_violations``, and counts violations of ontology
axioms the pipeline does not check (``owl:disjointWith``, ``owl:FunctionalProperty``).
Prints JSON; intended for ``.odibel/stats.json`` of a run dataset.

Usage:
    uv run resources/dbpedia-multi-source-kg/validate_run.py \
        <run_dir> <input.nt[.bz2]> <dbo-snapshots.ttl> <generate_<domain>.py> > stats.json
"""

from __future__ import annotations

import bz2
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from rdflib import Graph
from rdflib.namespace import OWL, RDF, RDFS

RDF_TYPE = f"<{RDF.type}>"
STEPS = ["selected", "distinct", "consistent", "cleaned_types", "cleaned_subgraph"]

Triple = tuple[str, str, str]


def parse_line(line: str) -> Triple | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    s, p, rest = line.split(" ", 2)
    return s, p, re.sub(r"\s*\.\s*$", "", rest)


def read_nt(path: Path) -> list[Triple]:
    """Read an NT file (optionally .bz2) or a Spark part directory."""
    files = sorted(path.glob("part-*")) if path.is_dir() else [path]
    triples = []
    for file in files:
        # Spark part directories are named *.nt.bz2 but hold plain text.
        opener = bz2.open if file.suffix == ".bz2" and not path.is_dir() else open
        with opener(file, "rt", encoding="utf-8") as handle:
            triples.extend(t for line in handle if (t := parse_line(line)))
    return triples


def content_digest(triples: list[Triple]) -> str:
    """sha256 over sorted N-Triples lines; stable across Spark part-file names."""
    digest = hashlib.sha256()
    for line in sorted(f"{s} {p} {o} .\n" for s, p, o in triples):
        digest.update(line.encode("utf-8"))
    return digest.hexdigest()


def domain_classes(script: Path) -> set[str]:
    names = re.findall(r'"dbo:([A-Za-z0-9_]+)"', script.read_text(encoding="utf-8"))
    return {f"<http://dbpedia.org/ontology/{n}>" for n in names}


class Ontology:
    def __init__(self, path: Path):
        graph = Graph()
        graph.parse(path, format="turtle")
        self.parents = defaultdict(set)
        for s, o in graph.subject_objects(RDFS.subClassOf):
            self.parents[str(s)].add(str(o))
        self.domains = defaultdict(set)
        for s, o in graph.subject_objects(RDFS.domain):
            self.domains[str(s)].add(str(o))
        self.ranges = defaultdict(set)
        for s, o in graph.subject_objects(RDFS.range):
            self.ranges[str(s)].add(str(o))
        self.disjoint = {(str(a), str(b)) for a, b in graph.subject_objects(OWL.disjointWith)}
        self.functional = {str(s) for s in graph.subjects(RDF.type, OWL.FunctionalProperty)}
        self._ancestors: dict[str, set[str]] = {}

    def ancestors(self, cls: str) -> set[str]:
        if cls not in self._ancestors:
            seen, queue = set(), [cls]
            while queue:
                cur = queue.pop()
                if cur not in seen:
                    seen.add(cur)
                    queue.extend(self.parents.get(cur, ()))
            self._ancestors[cls] = seen
        return self._ancestors[cls]


def types_of(triples: list[Triple]) -> dict[str, set[str]]:
    types = defaultdict(set)
    for s, p, o in triples:
        if p == RDF_TYPE:
            types[s].add(o.strip("<>"))
    return types


def classify_domain_range_removals(removed, before_types, input_types, ont: Ontology) -> Counter:
    """Reason per removed triple, mirroring rDF2.clean_domain_range_violations."""
    reasons = Counter()
    for s, p, o in removed:
        pred = p.strip("<>")
        subject_ancestors = set().union(*(ont.ancestors(t) for t in before_types.get(s, ())))
        if ont.domains.get(pred) and not ont.domains[pred] & subject_ancestors:
            reasons["domain: subject type not in domain"] += 1
        elif o.startswith("<"):
            if o not in before_types:
                where = "typed in input" if o in input_types else "untyped in input"
                reasons[f"range: object has no type in step input ({where})"] += 1
            else:
                reasons["range: object typed, not in range"] += 1
        elif "^^<" in o:
            reasons["range: datatype not in range"] += 1
        else:
            reasons["range: untagged or language literal vs range"] += 1
    return reasons


def unchecked_axioms(triples: list[Triple], input_types, ont: Ontology) -> dict:
    disjoint_entities = 0
    for types in types_of(triples).values():
        closure = set().union(*(ont.ancestors(t) for t in types))
        if any(a in closure and b in closure for a, b in ont.disjoint):
            disjoint_entities += 1
    values = defaultdict(set)
    for s, p, o in triples:
        if p.strip("<>") in ont.functional:
            values[(s, p)].add(o)
    functional = Counter(p for (s, p), objs in values.items() if len(objs) > 1)
    return {
        "disjoint_class_entities": disjoint_entities,
        "functional_property_violations": dict(functional.most_common()),
    }


def validate_variant(variant: Path, input_triples, input_types, classes, ont: Ontology) -> dict:
    steps = {name: read_nt(variant / f"{name}.nt.bz2") for name in STEPS}
    report: dict = {
        "triples": {name: len(t) for name, t in steps.items()},
        "content_sha256": {name: content_digest(t) for name, t in steps.items()},
        "checks": {},
    }
    checks = report["checks"]

    selected_subjects = {s for s, p, o in input_triples if p == RDF_TYPE and o in classes}
    expected = [t for t in input_triples if t[0] in selected_subjects]
    checks["selected_matches_recount"] = Counter(expected) == Counter(steps["selected"])
    checks["distinct_has_no_duplicates"] = len(set(steps["distinct"])) == len(steps["distinct"])

    before, after = set(steps["distinct"]), set(steps["consistent"])
    checks["consistent_is_subset"] = after <= before
    reasons = classify_domain_range_removals(before - after, types_of(steps["distinct"]), input_types, ont)
    report["domain_range_removed"] = {"total": len(before - after), "by_reason": dict(reasons.most_common())}
    by_predicate = Counter(p for s, p, o in before - after)
    report["domain_range_removed"]["top_predicates"] = dict(by_predicate.most_common(15))

    non_type = lambda ts: {t for t in ts if t[1] != RDF_TYPE}
    checks["cleaned_types_keeps_non_type_triples"] = non_type(steps["consistent"]) == non_type(steps["cleaned_types"])
    checks["cleaned_types_only_domain_classes"] = all(
        o in classes for s, p, o in steps["cleaned_types"] if p == RDF_TYPE
    )

    final = steps["cleaned_subgraph"]
    subjects = {s for s, p, o in final}
    checks["cleaned_subgraph_no_dangling_objects"] = all(
        not o.startswith("<") or o in subjects or p == RDF_TYPE for s, p, o in final
    )
    report["final_types"] = dict(Counter(o for s, p, o in final if p == RDF_TYPE).most_common())
    report["unchecked_axioms"] = {
        "distinct": unchecked_axioms(steps["distinct"], input_types, ont),
        "cleaned_subgraph": unchecked_axioms(final, input_types, ont),
    }
    return report


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 4:
        sys.exit(__doc__)
    run_dir, input_path, ontology_path, script = map(Path, argv)
    input_triples = read_nt(input_path)
    input_types = types_of(input_triples)
    classes = domain_classes(script)
    ont = Ontology(ontology_path)

    result = {
        "input_triples": len(input_triples),
        "domain_classes": len(classes),
        "variants": {
            variant.name: validate_variant(variant, input_triples, input_types, classes, ont)
            for variant in sorted(run_dir.glob("dbpedia_*_subgraph"))
        },
    }
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
